"""`backtest`: whether each stored tail held, day by day (Tier 16).

Once a UTC day has closed, for each fitted horizon and instrument, the stored
`tail` rows of the last 90 days are matched with the return of the bar each
one forecast (the bar closing one width after its asof), and judged:

- at 97.5%, the level Basel's FRTB sets ES at: the hit rate, Kupiec's
  unconditional coverage, Christoffersen's conditional coverage (hits that
  cluster fail it) and Engle and Manganelli's DQ (`gr.models.evaluate
  .var_backtest`), and Acerbi and Székely's (2014) Z2 for the ES itself:
  Z2 = Σ rₜ·Iₜ/(T·α·ESₜ) + 1, zero when ES is right and negative when it
  understates, with the traffic light at −0.7 and −1.8;
- at 99%: the hit rate, Kupiec and conditional coverage.

It needs 250 matched forecasts: the 1% tail then expects 2.5 hits, which is
about the least a coverage test can say anything with.
"""

from __future__ import annotations

import json
import math
from pathlib import Path

import numpy as np
import polars as pl

import galata_research as gr
from galata_research import Refused

from .bars import WIDTH_US, bars
from .basis import _dt
from .frontier import frontier, whole
from .varcov import Horizon, Run, stored_asof

SIGNAL = "backtest"
DAY_US = 86_400_000_000
WINDOW_DAYS = 90
MIN_N = 250
MEASURES = ("n", "hit_rate_975", "kupiec_p_975", "cc_p_975", "dq_p_975", "z2_975", "hit_rate_99", "kupiec_p_99", "cc_p_99")


def stored_tails(tape: Path, lo: int, hi: int) -> pl.DataFrame:
    """The stored `tail` rows with asof in [lo, hi), the newest computation of each: `horizon, ticker, asof, measure, value`."""
    root = tape / "kind=signals"
    empty = pl.DataFrame(schema={"horizon": pl.String, "ticker": pl.String, "asof": pl.Int64, "measure": pl.String, "value": pl.Float64})
    if not root.is_dir() or not any(root.rglob("*.parquet")):
        return empty
    return (
        pl.scan_parquet(str(root / "**" / "*.parquet"), hive_partitioning=False)
        .filter((pl.col("signal") == "tail") & pl.col("measure").is_in(["var_99", "var_975", "es_975"]) & (pl.col("asof_micros") >= lo) & (pl.col("asof_micros") < hi))
        .sort("computed_micros")
        .group_by("horizon", "ticker_i", "asof_micros", "measure")
        .agg(pl.col("value").last())
        .select("horizon", pl.col("ticker_i").alias("ticker"), pl.col("asof_micros").alias("asof"), "measure", "value")
        .collect()
    )


def compute(horizons: list[Horizon], tape: Path, run: Run) -> Run:
    asof = whole(run.computed_micros, DAY_US, frontier(tape, ("candles",)))
    stored = stored_asof(tape, SIGNAL)
    tails = stored_tails(tape, asof - WINDOW_DAYS * DAY_US, asof)
    for hz in (h for h in horizons if h.fitted):
        if hz.name in stored and asof <= stored[hz.name]:
            run.said.append(f"backtest {hz.name}: nothing new since {_dt(stored[hz.name]):%Y-%m-%d}")
            continue
        mine = tails.filter(pl.col("horizon") == hz.name)
        if not mine.height:
            run.said.append(f"backtest {hz.name}: no stored tail yet")
            continue
        wide = mine.pivot(on="measure", index=["ticker", "asof"], values="value")
        r = gr.timeseries.returns(bars(hz.name, hz.bars), kind="log").drop_nulls("return").select(
            "ticker", (pl.col("close_ts").dt.epoch("us") - WIDTH_US[hz.name]).alias("asof"), "return"
        )
        # A tail at asof forecast the bar that closes one width later: its return is keyed by that bar's start.
        matched = wide.join(r, on=["ticker", "asof"], how="inner").filter(pl.col("asof") + WIDTH_US[hz.name] <= asof)
        for t in sorted(set(wide["ticker"].to_list())):
            run.rows.extend(_rows(hz, t, asof, matched.filter(pl.col("ticker") == t).sort("asof"), run))
        run.said.append(f"backtest {hz.name}: {len(set(wide['ticker'].to_list()))} instruments to {_dt(asof):%Y-%m-%d}")
    return run


def z2(returns: np.ndarray, var: np.ndarray, es: np.ndarray, alpha: float) -> float:
    """Acerbi and Székely's Z2, VaR and ES as positive losses: Σ rₜ·1{rₜ < −VaRₜ}/(T·α·ESₜ) + 1."""
    hit = returns < -var
    return float((returns * hit / es).sum() / (len(returns) * alpha) + 1)


def _rows(hz: Horizon, ticker: str, asof: int, m: pl.DataFrame, run: Run) -> list[dict]:
    base = {
        "signal": SIGNAL, "horizon": hz.name, "ticker_i": ticker, "ticker_j": None, "h": 1, "asof_micros": asof,
        "target_micros": asof, "computed_micros": run.computed_micros, "fitted_through_micros": None, "fit_from_micros": None,
        "model": "kupiec/christoffersen/dq/acerbi-szekely",
        "params": json.dumps({"window_days": WINDOW_DAYS, "min_n": MIN_N, "z2_yellow": -0.7, "z2_red": -1.8}, sort_keys=True),
        "fitted": False, "after_gap": False, "code": run.code, "run_id": run.run_id,
    }  # fmt: skip
    rows = []

    def put(measure, value, reason=None, n=None):
        ok = value is not None and math.isfinite(value)
        rows.append({**base, "measure": measure, "value": float(value) if ok else None, "absent": None if ok else (reason or "not defined"), "n_eff": n})

    m = m.drop_nulls(["var_99", "var_975", "es_975", "return"]).filter((pl.col("var_975") > 0) & (pl.col("es_975") >= pl.col("var_975")))
    n = m.height
    if n < MIN_N:
        for measure in MEASURES:
            put(measure, None, f"{n} forecasts matched with their bar in the {WINDOW_DAYS} days, under {MIN_N}")
        return rows
    r, v99, v975, e975 = (m[c].to_numpy() for c in ("return", "var_99", "var_975", "es_975"))
    put("n", n, n=n)
    try:
        b975 = gr.models.evaluate.var_backtest(r, -v975, -e975, 0.025)
        put("hit_rate_975", b975["hits"] / n, n=n)
        put("kupiec_p_975", b975["kupiec_p"], "no hit: coverage is undefined", n)
        put("cc_p_975", b975["conditional_coverage_p"], "no hit: coverage is undefined", n)
        put("dq_p_975", b975["dq_p"], n=n)
    except Refused as why:
        for measure in ("hit_rate_975", "kupiec_p_975", "cc_p_975", "dq_p_975"):
            put(measure, None, str(why))
    put("z2_975", z2(r, v975, e975, 0.025), n=n)
    # At 99% there is no ES: hand var_backtest an ES just beyond the VaR; only the hits and coverage are read.
    b99 = gr.models.evaluate.var_backtest(r, -v99, -v99 * (1 + 1e-9), 0.01)
    put("hit_rate_99", b99["hits"] / n, n=n)
    put("kupiec_p_99", b99["kupiec_p"], "no hit: coverage is undefined", n)
    put("cc_p_99", b99["conditional_coverage_p"], "no hit: coverage is undefined", n)
    return rows
