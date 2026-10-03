"""`liquidity`: what it costs to trade each instrument, hour by hour (Tier 16).

From the tape's top of book and trades for the hour that closed:

- the **time-weighted** quoted spread and touch depth: each quote state weighs
  as long as it stood (Holden and Jacobsen 2014); on an event-driven feed an
  average over updates weights churn, and a long, wide stretch counts once.
  Crossed and locked states are dropped, and an hour with under 48 valid
  minutes has no figure (Brauneis, Mestel, Riordan and Theissen 2021);
- the effective spread, dollar-volume-weighted (the cost paid) and
  equal-weighted (the typical small taker's), and the price impact 5 s after
  each trade (`gr.liquidity.effective`), over trades whose 5 s had passed by
  the asof, so nothing after it is read;
- Amihud's illiquidity on 1m flow over 24 hours (`gr.liquidity.amihud`),
  which Brauneis et al. find ranks levels best;
- a robust z of the log spread against this signal's own stored hours: the
  tape is the history, and re-reading a week of raw quotes each hour is not.
"""

from __future__ import annotations

import json
import math
from datetime import UTC, datetime
from pathlib import Path

import polars as pl

import galata_research as gr

from .frontier import frontier, period, signal_files
from .varcov import Run, micros, stored_asof

SIGNAL = "liquidity"
HORIZON = "1h"
HOUR_US = 3_600_000_000
MIN_VALID_US = 48 * 60_000_000
Z_DAYS = 7
Z_MIN = 72
MAD_SCALE = 1.4826
IMPACT = "5s"


def _dt(us: int) -> datetime:
    return datetime.fromtimestamp(us / 1_000_000, tz=UTC)


# The record, behind names a test can replace.
def quotes(lo: int, hi: int) -> pl.DataFrame:
    return gr.market.quotes(None, _dt(lo), _dt(hi)).collect()


def trades(lo: int, hi: int) -> pl.DataFrame:
    return gr.market.trades(None, _dt(lo), _dt(hi)).collect()


def history(tape: Path, lo: int, hi: int) -> pl.DataFrame:
    """This signal's stored `quoted_spread_bps` with asof in [lo, hi): its own history."""
    files = signal_files(tape, lo, hi)
    if not files:
        return pl.DataFrame(schema={"ticker_i": pl.String, "asof_micros": pl.Int64, "value": pl.Float64})
    # The newest computation of each hour, as `activity.history` and `basis.history`.
    return (
        pl.scan_parquet(files, hive_partitioning=False)
        .filter((pl.col("signal") == SIGNAL) & (pl.col("measure") == "quoted_spread_bps") & (pl.col("asof_micros") >= lo) & (pl.col("asof_micros") < hi))
        .group_by("ticker_i", "asof_micros")
        .agg(pl.col("value").sort_by("computed_micros").last())
        .select("ticker_i", "asof_micros", "value")
        .collect()
    )


def weighted_quantile(values: list[float], weights: list[float], q: float) -> float:
    """The value at cumulative weight share q, values sorted ascending."""
    pairs = sorted(zip(values, weights))
    total = sum(w for _, w in pairs)
    running = 0.0
    for v, w in pairs:
        running += w
        if running >= q * total:
            return v
    return pairs[-1][0]


def states(quotes_: pl.DataFrame, lo: int, hi: int) -> pl.DataFrame:
    """Each valid quote state within [lo, hi): its spread, touch depth and how long it stood.

    A state runs from its quote to the next (or to `hi`); one that began before
    `lo` counts from `lo`. Crossed and locked states (ask ≤ bid) are dropped.
    """
    frame = quotes_.sort("ts").with_columns(pl.col("ts").dt.epoch("us").alias("_t"))
    frame = frame.with_columns(pl.col("_t").shift(-1).fill_null(hi).alias("_next"))
    frame = frame.with_columns(pl.col("_t").clip(lo, hi).alias("_from"), pl.col("_next").clip(lo, hi).alias("_to"))
    mid = (pl.col("bid_px") + pl.col("ask_px")) / 2
    return (
        frame.filter((pl.col("_to") > pl.col("_from")) & (pl.col("ask_px") > pl.col("bid_px")))
        .select(
            ((pl.col("ask_px") - pl.col("bid_px")) / mid * 1e4).alias("spread_bps"),
            pl.min_horizontal(pl.col("bid_px") * pl.col("bid_sz"), pl.col("ask_px") * pl.col("ask_sz")).alias("depth_usd"),
            (pl.col("_to") - pl.col("_from")).alias("stood_us"),
        )
    )


def compute(tape: Path, run: Run) -> Run:
    # The last whole period on the tape as well as by the clock (frontier.py).
    asof = period(run, HOUR_US, frontier(tape, ("quotes", "trades")))
    stored = stored_asof(tape, SIGNAL).get(HORIZON)
    if not run.redo and stored is not None and asof <= stored:
        run.said.append(f"liquidity: nothing new since {_dt(stored):%Y-%m-%d %H:%M}")
        return run
    lo = asof - HOUR_US
    # The state standing when the hour opened began before it: read a minute back.
    q = quotes(lo - 60_000_000, asof)
    day = trades(asof - 24 * HOUR_US, asof)
    past = history(tape, asof - Z_DAYS * 24 * HOUR_US, asof)
    tickers = sorted(set(q["ticker"].to_list()) | set(day["ticker"].to_list()))
    for t in tickers:
        run.rows.extend(_rows(t, asof, q.filter(pl.col("ticker") == t), day.filter(pl.col("ticker") == t), past.filter(pl.col("ticker_i") == t), run))
    run.said.append(f"liquidity: {len(tickers)} instruments for the hour to {_dt(asof):%Y-%m-%d %H:%M}")
    return run


def _rows(ticker: str, asof: int, q: pl.DataFrame, day: pl.DataFrame, past: pl.DataFrame, run: Run) -> list[dict]:
    lo = asof - HOUR_US
    base = {
        "signal": SIGNAL, "horizon": HORIZON, "ticker_i": ticker, "ticker_j": None, "h": 1, "asof_micros": asof,
        "target_micros": asof, "computed_micros": run.computed_micros, "fitted_through_micros": None, "fit_from_micros": None,
        "model": "time-weighted-book/effective-spread",
        "params": json.dumps({"min_valid_minutes": 48, "impact_horizon": IMPACT, "z_days": Z_DAYS, "z_min": Z_MIN}, sort_keys=True),
        "fitted": False, "after_gap": False, "code": run.code, "run_id": run.run_id,
    }  # fmt: skip
    rows = []

    def put(measure, value, reason=None, n=None):
        ok = value is not None and math.isfinite(value)
        rows.append({**base, "measure": measure, "value": float(value) if ok else None, "absent": None if ok else (reason or "not defined"), "n_eff": n})

    s = states(q, lo, asof) if q.height else pl.DataFrame(schema={"spread_bps": pl.Float64, "depth_usd": pl.Float64, "stood_us": pl.Int64})
    covered = int(s["stood_us"].sum()) if s.height else 0
    spread = None
    if covered < MIN_VALID_US:
        why = f"valid quote states cover {covered // 60_000_000} of 60 minutes, under 48"
        for m in ("quoted_spread_bps", "depth_usd_median", "depth_usd_p10"):
            put(m, None, why)
    else:
        w = s["stood_us"].to_list()
        spread = float((s["spread_bps"] * s["stood_us"]).sum() / covered)
        put("quoted_spread_bps", spread, n=s.height)
        put("depth_usd_median", weighted_quantile(s["depth_usd"].to_list(), w, 0.5), n=s.height)
        put("depth_usd_p10", weighted_quantile(s["depth_usd"].to_list(), w, 0.1), n=s.height)

    hour = day.filter(pl.col("ts").dt.epoch("us") >= lo)
    if hour.height and q.height:
        eff = gr.liquidity.effective(hour, q, horizons=(IMPACT,)).drop_nulls("effective_bps")
        eff = eff.with_columns((pl.col("price") * pl.col("size")).alias("_usd"))
        if eff.height:
            put("effective_spread_bps_vw", float((eff["effective_bps"] * eff["_usd"]).sum() / eff["_usd"].sum()), n=eff.height)
            put("effective_spread_bps_ew", float(eff["effective_bps"].mean()), n=eff.height)
        else:
            put("effective_spread_bps_vw", None, "no trade had a quote before it")
            put("effective_spread_bps_ew", None, "no trade had a quote before it")
        known = eff.filter(pl.col(f"known_ts_{IMPACT}").dt.epoch("us") <= asof).drop_nulls(f"impact_bps_{IMPACT}")
        put("impact_bps_5s_vw", float((known[f"impact_bps_{IMPACT}"] * known["_usd"]).sum() / known["_usd"].sum()) if known.height else None, "no trade's 5 s had passed by the asof", known.height)
    else:
        for m in ("effective_spread_bps_vw", "effective_spread_bps_ew", "impact_bps_5s_vw"):
            put(m, None, "no trades in the hour" if not hour.height else "no quotes in the hour")

    if day.height:
        a = gr.liquidity.amihud(gr.liquidity.flow(day, "1m"))
        value = a["amihud_bps_per_m"][0] if a.height else None
        put("amihud_24h", value, "no 1m bucket with a return and notional", int(a["n"][0]) if value is not None else None)
    else:
        put("amihud_24h", None, "no trades in the 24 hours")

    values = [v for v in past["value"].to_list() if v is not None and v > 0]
    if spread is None:
        put("spread_z_7d", None, "no quoted spread this hour")
    elif len(values) < Z_MIN:
        put("spread_z_7d", None, f"{len(values)} stored hours in the {Z_DAYS} days before, under {Z_MIN}")
    else:
        logs = sorted(math.log(v) for v in values)
        med = logs[len(logs) // 2] if len(logs) % 2 else (logs[len(logs) // 2 - 1] + logs[len(logs) // 2]) / 2
        dev = sorted(abs(x - med) for x in logs)
        mad = dev[len(dev) // 2] if len(dev) % 2 else (dev[len(dev) // 2 - 1] + dev[len(dev) // 2]) / 2
        put("spread_z_7d", (math.log(spread) - med) / (MAD_SCALE * mad) if mad > 0 else None, "the stored spreads do not vary", len(values))
    return rows
