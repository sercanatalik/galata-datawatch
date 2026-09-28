"""`moments`: each instrument's realized skewness and kurtosis, day by day (Tier 16).

Amaya, Christoffersen, Jacobs and Vasquez (2015) from 5-minute log returns
(`gr.timeseries.realized_moments`), once a UTC day has closed:

- `realized_vol_1d`, √RV over the day's returns;
- `realized_skew_1d` = √N·Σr³/RV^{3/2} and `realized_kurt_1d` = N·Σr⁴/RV²;
- `realized_skew_7d` and `realized_kurt_7d`, the mean of the last 7 days'
  daily values, as they average their weekly measure, with 4 valid days
  required.

In their equity cross-section, and in crypto (Jia, Liu and Yan 2021), low
realized skewness is followed by higher returns; kurtosis is the weaker
signal. **N is the returns actually there**: a 5-minute bar exists only when
all its minutes traded, and a return across a hole is dropped, so a session
instrument's closed hours and reopening jump neither inflate N nor land in
r³ and r⁴. A day under 50 returns has no moments.
"""

from __future__ import annotations

import json
import math
from datetime import UTC, datetime
from pathlib import Path

import polars as pl

import galata_research as gr

from .bars import bars
from .varcov import Run, stored_asof

SIGNAL = "moments"
HORIZON = "1d"
DAY_US = 86_400_000_000
MIN_N = 50
WEEK_DAYS = 7
MIN_DAYS = 4


def fivemin() -> pl.DataFrame:
    """5-minute bars from the tape's 1m, whole buckets only. A name a test can replace."""
    return bars("5m", "1m")


def _dt(us: int) -> datetime:
    return datetime.fromtimestamp(us / 1_000_000, tz=UTC)


def compute(tape: Path, run: Run) -> Run:
    asof = run.computed_micros // DAY_US * DAY_US
    stored = stored_asof(tape, SIGNAL).get(HORIZON)
    if stored is not None and asof <= stored:
        run.said.append(f"moments: nothing new since {_dt(stored):%Y-%m-%d}")
        return run
    lo = asof - WEEK_DAYS * DAY_US
    b = fivemin().filter((pl.col("close_ts").dt.epoch("us") > lo - DAY_US) & (pl.col("close_ts").dt.epoch("us") <= asof))
    r = gr.timeseries.returns(b, kind="log").filter(pl.col("close_ts").dt.epoch("us") > lo)
    days = gr.timeseries.realized_moments(r, "1d", min_n=MIN_N) if r.height else pl.DataFrame()
    tickers = sorted(set(b["ticker"].to_list())) if b.height else []
    for t in tickers:
        mine = days.filter(pl.col("ticker") == t) if days.height else days
        run.rows.extend(_rows(t, asof, mine, run))
    run.said.append(f"moments: {len(tickers)} instruments for the day to {_dt(asof):%Y-%m-%d}")
    return run


def _rows(ticker: str, asof: int, days: pl.DataFrame, run: Run) -> list[dict]:
    base = {
        "signal": SIGNAL, "horizon": HORIZON, "ticker_i": ticker, "ticker_j": None, "h": 1, "asof_micros": asof,
        "target_micros": asof, "computed_micros": run.computed_micros, "fitted_through_micros": None, "fit_from_micros": None,
        "model": "realized-moments/5m",
        "params": json.dumps({"returns": "5m log", "min_n": MIN_N, "week_days": WEEK_DAYS, "min_days": MIN_DAYS}, sort_keys=True),
        "fitted": False, "after_gap": False, "code": run.code, "run_id": run.run_id,
    }  # fmt: skip
    rows = []

    def put(measure, value, reason=None, n=None):
        ok = value is not None and math.isfinite(value)
        rows.append({**base, "measure": measure, "value": float(value) if ok else None, "absent": None if ok else (reason or "not defined"), "n_eff": n})

    day_start = asof - DAY_US
    today = days.filter(pl.col("ts").dt.epoch("us") == day_start) if days.height else days
    if not today.height or today["skew"][0] is None:
        n = int(today["n"][0]) if today.height else 0
        why = f"{n} whole 5-minute returns in the day, under {MIN_N}"
        for m in ("realized_vol_1d", "realized_skew_1d", "realized_kurt_1d"):
            put(m, None, why)
    else:
        row = today.row(0, named=True)
        put("realized_vol_1d", math.sqrt(row["rv"]), n=row["n"])
        put("realized_skew_1d", row["skew"], n=row["n"])
        put("realized_kurt_1d", row["kurt"], n=row["n"])

    valid = days.filter(pl.col("skew").is_not_null()) if days.height else days
    if valid.height < MIN_DAYS:
        why = f"{valid.height} days with moments in the last {WEEK_DAYS}, under {MIN_DAYS}"
        put("realized_skew_7d", None, why)
        put("realized_kurt_7d", None, why)
    else:
        put("realized_skew_7d", valid["skew"].mean(), n=valid.height)
        put("realized_kurt_7d", valid["kurt"].mean(), n=valid.height)
    return rows
