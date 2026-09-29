"""`flow`: who pushed the price, hour by hour (Tier 16).

From the tape's top of book and trades for the hour that closed, per
instrument:

- **order-flow imbalance** (Cont, Kukanov and Stoikov 2014, `gr.liquidity.ofi`)
  over 10 s buckets: the hour's sum over its mean depth, and the linear fit of
  each bucket's mid return on its depth-normalised imbalance, β in bps and
  its R². β·depth is about constant in their data, so β moving says the book
  thinned or filled;
- **trade imbalance**: buy minus sell notional over their sum, by aggressor
  (`gr.liquidity.flow`), and the R² of the same 10 s returns on it. In crypto
  trade flow has explained prices as well as the book's (Silantyev 2019) where
  in equities the book dominated, so both are kept and the R²s say which;
- **queue imbalance**, (bid size − ask size)/(their sum) time-weighted, and
  the share of the hour it spent beyond ±0.5.

The R²s are **contemporaneous**: how much of the hour's 10 s moves the flow
accounts for, not a forecast (Cont, Cucuringu and Zhang 2023 find the
next-minute out-of-sample R² of the same regressor is below zero). A fit needs
100 of the hour's 360 buckets.
"""

from __future__ import annotations

import json
import math
from datetime import UTC, datetime
from pathlib import Path

import polars as pl

import galata_research as gr
from galata_research import Refused

from .liquidity import quotes, trades
from .frontier import frontier, whole
from .varcov import Run, stored_asof

SIGNAL = "flow"
HORIZON = "1h"
HOUR_US = 3_600_000_000
BUCKET = "10s"
MIN_BUCKETS = 100
EXTREME = 0.5


def _dt(us: int) -> datetime:
    return datetime.fromtimestamp(us / 1_000_000, tz=UTC)


def compute(tape: Path, run: Run) -> Run:
    # The last whole period on the tape as well as by the clock (frontier.py).
    asof = whole(run.computed_micros, HOUR_US, frontier(tape, ("quotes", "trades")))
    stored = stored_asof(tape, SIGNAL).get(HORIZON)
    if stored is not None and asof <= stored:
        run.said.append(f"flow: nothing new since {_dt(stored):%Y-%m-%d %H:%M}")
        return run
    lo = asof - HOUR_US
    q = quotes(lo - 60_000_000, asof)
    t = trades(lo, asof)
    tickers = sorted(set(q["ticker"].to_list()) | set(t["ticker"].to_list()))
    for ticker in tickers:
        run.rows.extend(_rows(ticker, asof, q.filter(pl.col("ticker") == ticker), t.filter(pl.col("ticker") == ticker), run))
    run.said.append(f"flow: {len(tickers)} instruments for the hour to {_dt(asof):%Y-%m-%d %H:%M}")
    return run


def _rows(ticker: str, asof: int, q: pl.DataFrame, t: pl.DataFrame, run: Run) -> list[dict]:
    lo = asof - HOUR_US
    base = {
        "signal": SIGNAL, "horizon": HORIZON, "ticker_i": ticker, "ticker_j": None, "h": 1, "asof_micros": asof,
        "target_micros": asof, "computed_micros": run.computed_micros, "fitted_through_micros": None, "fit_from_micros": None,
        "model": "cont-kukanov-stoikov/10s",
        "params": json.dumps({"bucket": BUCKET, "min_buckets": MIN_BUCKETS, "extreme": EXTREME}, sort_keys=True),
        "fitted": False, "after_gap": False, "code": run.code, "run_id": run.run_id,
    }  # fmt: skip
    rows = []

    def put(measure, value, reason=None, n=None):
        ok = value is not None and math.isfinite(value)
        rows.append({**base, "measure": measure, "value": float(value) if ok else None, "absent": None if ok else (reason or "not defined"), "n_eff": n})

    in_hour = (pl.col("ts").dt.epoch("us") >= lo) & (pl.col("ts").dt.epoch("us") < asof)
    buckets = gr.liquidity.ofi(q, BUCKET).filter(in_hour) if q.height else pl.DataFrame()
    if buckets.height:
        buckets = buckets.with_columns((pl.col("ofi") / pl.col("depth")).alias("ofi_norm"))
        depth = buckets["depth"].mean()
        put("ofi_norm_1h", buckets["ofi"].sum() / depth if depth else None, "no depth in the hour", buckets.height)
        _fit(put, "ofi", buckets, "ofi_norm")
    else:
        for m in ("ofi_norm_1h", "ofi_beta_bps", "ofi_r2"):
            put(m, None, "no quotes in the hour")

    if t.height:
        f = gr.liquidity.flow(t, BUCKET).filter(in_hour)
        buy, sell = f["buy_notional"].sum(), f["sell_notional"].sum()
        put("trade_imbalance_1h", (buy - sell) / (buy + sell) if buy + sell > 0 else None, "no notional traded", f.height)
        if buckets.height:
            joined = buckets.select("ts", "return_bps").join(f.select("ts", "imbalance"), on="ts", how="inner")
            _fit(put, "trade", joined, "imbalance", beta=False)
        else:
            put("trade_r2", None, "no quotes to take the mid's return from")
    else:
        put("trade_imbalance_1h", None, "no trades in the hour")
        put("trade_r2", None, "no trades in the hour")

    s = _queue_states(q, lo, asof) if q.height else pl.DataFrame()
    if s.height:
        w = s["stood_us"]
        put("queue_imbalance_twa", float((s["qi"] * w).sum() / w.sum()), n=s.height)
        put("queue_imbalance_extreme_share", float(w.filter(s["qi"].abs() > EXTREME).sum() / w.sum()), n=s.height)
    else:
        for m in ("queue_imbalance_twa", "queue_imbalance_extreme_share"):
            put(m, None, "no valid quote state in the hour")
    return rows


def _fit(put, name: str, frame: pl.DataFrame, x: str, *, beta: bool = True) -> None:
    try:
        fit = gr.liquidity.impact(frame, x=x, min_n=MIN_BUCKETS)
    except Refused as why:
        if beta:
            put(f"{name}_beta_bps", None, str(why))
        put(f"{name}_r2", None, str(why))
        return
    if beta:
        put(f"{name}_beta_bps", fit["beta"], n=fit["n"])
    put(f"{name}_r2", fit["r2"], n=fit["n"])


def _queue_states(q: pl.DataFrame, lo: int, hi: int) -> pl.DataFrame:
    """Each valid state's queue imbalance and how long it stood, as `liquidity.states` weighs its spread."""
    frame = q.sort("ts").with_columns(pl.col("ts").dt.epoch("us").alias("_t"))
    frame = frame.with_columns(pl.col("_t").shift(-1).fill_null(hi).alias("_next"))
    frame = frame.with_columns(pl.col("_t").clip(lo, hi).alias("_from"), pl.col("_next").clip(lo, hi).alias("_to"))
    total = pl.col("bid_sz") + pl.col("ask_sz")
    return frame.filter((pl.col("_to") > pl.col("_from")) & (pl.col("ask_px") > pl.col("bid_px")) & (total > 0)).select(
        ((pl.col("bid_sz") - pl.col("ask_sz")) / total).alias("qi"),
        (pl.col("_to") - pl.col("_from")).alias("stood_us"),
    )

