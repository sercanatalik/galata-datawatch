"""`realvol`: each instrument's volatility tomorrow and over the next week, from its realized variance (Tier 16).

HARQ (Bollerslev, Patton and Quaedvlieg 2016; `gr.models.vol.har`): the next
day's realized variance regressed on the last day's, the last 7 days' and the
last 30 days' means, the day's weight shrunk when its realized quarticity says
it was noisily measured. Each UTC day's RV is Σr² over its 24 hourly log
returns (`gr.timeseries.realized_from`), whole days only. galata-research's
registered study (item 24, `har_long.py`, Binance BTC and ETH 2020–2026)
found HARQ beat GARCH at 1d on both, significantly (uSPA p 0.016, 0.000), and
the record's coarser RV forecast better than 5-minute RV. Once a day has closed:

- `sigma_1d`, √ of the forecast variance of the day now opening;
- `sigma_7d`, √ of the forecast sum over the next 7 days (a direct regression
  per horizon, not an iterated one);
- `rv_sigma_1d`, √RV of the day just closed, what the forecast is judged by;
- `vol_term`, that day's RV over the mean of the last 30: above 1, volatility
  is high against its month;
- `qlike_harq`, `qlike_naive` and `qlike_mean30`: over the last 60 days, the
  loss of HARQ's walk-forward forecast (refitted each day on the days before)
  against yesterday's RV and the 30-day mean. QLIKE is robust to the noise in
  RV as a proxy (Patton 2011). On the record to 29 September, HARQ's was
  0.46–0.81 against 0.70–2.50 for yesterday's RV and 0.52–0.82 for the 30-day
  mean, which beat it on CL alone (0.54 against 0.58); HAR's was within 0.04
  of HARQ's. The model is stored beside its own evidence.

An xyz perp's day holds all 24 hours, its closed session's included: the
position is held through them. A forecast outside its training targets'
range is replaced by their mean, and its params say `"filtered": true`.
"""

from __future__ import annotations

import json
import math
from datetime import UTC, datetime
from pathlib import Path

import polars as pl

import galata_research as gr
from galata_research.models.vol.har import har

from .bars import bars
from .frontier import frontier, period
from .varcov import Run, stored_asof

SIGNAL = "realvol"
HORIZON = "1d"
DAY_US = 86_400_000_000
MODEL = "harq"
MIN_TRAIN = 100  # training days at the first refit; the library's 250 assumes years of history
EVAL_DAYS = 60
WEEK = 7
MONTH = 30


def hourly() -> pl.DataFrame:
    """The tape's 1h candles. A name a test can replace."""
    return bars("1h", "1h")


def _dt(us: int) -> datetime:
    return datetime.fromtimestamp(us / 1_000_000, tz=UTC)


def compute(tape: Path, run: Run) -> Run:
    # The last whole period on the tape as well as by the clock (frontier.py).
    asof = period(run, DAY_US, frontier(tape, ("candles",)))
    stored = stored_asof(tape, SIGNAL).get(HORIZON)
    if not run.redo and stored is not None and asof <= stored:
        run.said.append(f"realvol: nothing new since {_dt(stored):%Y-%m-%d}")
        return run
    fine = hourly()
    fine = fine.filter(pl.col("close_ts").dt.epoch("us") <= asof) if fine.height else fine
    days = gr.timeseries.realized_from(fine, "1d").filter(pl.col("close_ts").dt.epoch("us") <= asof) if fine.height else pl.DataFrame()
    tickers = sorted(set(days["ticker"].to_list())) if days.height else []
    for t in tickers:
        run.rows.extend(_rows(t, asof, days.filter(pl.col("ticker") == t).sort("ts"), run))
    run.said.append(f"realvol: {len(tickers)} instruments for the day to {_dt(asof):%Y-%m-%d}")
    return run


def _rows(ticker: str, asof: int, days: pl.DataFrame, run: Run) -> list[dict]:
    params = {"model": MODEL, "rv": "1h log returns, whole UTC days", "lags": [1, WEEK, MONTH], "min_train": MIN_TRAIN, "eval_days": EVAL_DAYS}
    base = {
        "signal": SIGNAL, "horizon": HORIZON, "ticker_i": ticker, "ticker_j": None, "h": 1, "asof_micros": asof,
        "target_micros": asof + DAY_US, "computed_micros": run.computed_micros, "fitted_through_micros": asof, "fit_from_micros": None,
        "model": f"{MODEL}/1d", "fitted": True, "after_gap": False, "code": run.code, "run_id": run.run_id,
    }  # fmt: skip
    rows = []

    def put(measure, value, reason=None, n=None, **over):
        ok = value is not None and math.isfinite(value)
        row = {**base, "params": json.dumps(params, sort_keys=True), **over}
        rows.append({**row, "measure": measure, "value": float(value) if ok else None, "absent": None if ok else (reason or "not defined"), "n_eff": n})

    def absent_all(why):
        for m in ("sigma_1d", "sigma_7d", "rv_sigma_1d", "vol_term", "qlike_harq", "qlike_naive", "qlike_mean30"):
            put(m, None, why, fitted=False, fitted_through_micros=None)
        return rows

    last = days.filter(pl.col("close_ts").dt.epoch("us") == asof)
    if not last.height or last["rv"][0] is None:
        return absent_all("the day to the asof is not whole on the tape")
    whole = days.drop_nulls("rv")
    need = MIN_TRAIN + MONTH + EVAL_DAYS
    if whole.height < need:
        return absent_all(f"{whole.height} whole days, under {need}: {MIN_TRAIN} to train, {MONTH} for the monthly mean, {EVAL_DAYS} to judge")

    rv = whole["rv"]
    today = float(last["rv"][0])
    put("rv_sigma_1d", math.sqrt(today), n=24)
    put("vol_term", today / float(rv.tail(MONTH).mean()), n=MONTH)

    split = whole["close_ts"][whole.height - EVAL_DAYS - 1]
    f = har(whole, model=MODEL, split=split, horizons=(1, WEEK), min_obs=MIN_TRAIN)
    first = int(f["fit_from"].min().timestamp() * 1_000_000) if f.height else None
    now = f.filter(pl.col("close_ts").dt.epoch("us") == asof)
    one, week = now.filter(pl.col("h") == 1), now.filter(pl.col("h") == WEEK)
    if one["filtered"][0]:
        params["filtered"] = True
    put("sigma_1d", math.sqrt(one["variance"][0]), n=whole.height, fit_from_micros=first)
    put("sigma_7d", math.sqrt(week["cum_variance"][0]), n=whole.height, fit_from_micros=first)
    params.pop("filtered", None)

    # Each earlier origin's day-ahead forecast against the day that followed it.
    target = whole.select(pl.col("ts").alias("target_ts"), pl.col("rv").alias("y"))
    naive = whole.select("close_ts", pl.col("rv").alias("naive"), pl.col("rv").rolling_mean(MONTH).alias("mean30"))
    judged = f.filter(pl.col("h") == 1).join(target, on="target_ts").join(naive, on="close_ts").drop_nulls(["y", "naive", "mean30"])
    n = judged.height
    for measure, column in (("qlike_harq", "variance"), ("qlike_naive", "naive"), ("qlike_mean30", "mean30")):
        put(measure, qlike(judged[column], judged["y"]) if n else None, "no earlier forecast has its day closed", n)
    return rows


def qlike(forecast: pl.Series, realized: pl.Series) -> float:
    """Mean QLIKE, y/f − ln(y/f) − 1: zero for a perfect forecast (Patton 2011)."""
    ratio = realized / forecast
    return float((ratio - ratio.log() - 1).mean())
