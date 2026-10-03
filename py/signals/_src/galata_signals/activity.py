"""`activity`: how unusual each instrument's trading was, hour by hour (Tier 16).

Per whole closed hour and instrument, from the tape's trades:

- `notional_usd`, `trade_count` and `avg_trade_usd`, the hour as it traded:
  stored, so they are the history the rest is judged against;
- `volume_z`, `count_z` and `size_z`: robust z (median and MAD) of the log
  of each against **the same hour of day, weekdays against weekdays and
  weekends against weekends**, over this signal's stored 28 days. Crypto's
  activity follows the stock markets' sessions within the day, and a weekend
  trades 20–40% below a weekday, so the hour alone would mistake a Saturday
  for a lull;
- `volume_pct`, the hour's percentile among those same hours: Gervais, Kaniel
  and Mingelgrin's (2001) volume shock is a rank against the stock's own
  trailing days, not a z;
- `large_share`, the share of the hour's notional in trades larger than the
  95th percentile of the previous 24 hours' trade sizes, per instrument (a
  fixed dollar cut does not carry across instruments): the whale share
  Scaillet, Treccani and Trevisan find ahead of Bitcoin's jumps.

A seasonal figure needs 5 stored hours in its bucket, so it builds for about a
week (weekends longer).
"""

from __future__ import annotations

import json
import math
from datetime import UTC, datetime
from pathlib import Path

import polars as pl

from .basis import _dt
from .frontier import frontier, period, signal_files
from .liquidity import trades
from .varcov import Run, stored_asof

SIGNAL = "activity"
HORIZON = "1h"
HOUR_US = 3_600_000_000
DAY_US = 24 * HOUR_US
HISTORY_DAYS = 28
MIN_BUCKET = 5
LARGE_Q = 0.95
MAD_SCALE = 1.4826
RAW = ("notional_usd", "trade_count", "avg_trade_usd")


def history(tape: Path, lo: int, hi: int) -> pl.DataFrame:
    """This signal's stored raw figures with asof in [lo, hi): `ticker_i, asof_micros, measure, value`."""
    empty = pl.DataFrame(schema={"ticker_i": pl.String, "asof_micros": pl.Int64, "measure": pl.String, "value": pl.Float64})
    files = signal_files(tape, lo, hi)
    if not files:
        return empty
    # The newest computation of each hour: a `--redo` adds a row beside the one
    # it repairs (the dataset is additive-only), and both in the baseline would
    # keep the wrong figure in the median it was redone to replace.
    return (
        pl.scan_parquet(files, hive_partitioning=False)
        .filter((pl.col("signal") == SIGNAL) & pl.col("measure").is_in(list(RAW)) & (pl.col("asof_micros") >= lo) & (pl.col("asof_micros") < hi))
        .group_by("ticker_i", "asof_micros", "measure")
        .agg(pl.col("value").sort_by("computed_micros").last())
        .select("ticker_i", "asof_micros", "measure", "value")
        .collect()
    )


def _bucket(asof: int) -> tuple[int, bool]:
    """The hour of day an hour *started* at, and whether it was a weekend (UTC)."""
    start = datetime.fromtimestamp((asof - HOUR_US) / 1e6, tz=UTC)
    return start.hour, start.weekday() >= 5


def compute(tape: Path, run: Run) -> Run:
    asof = period(run, HOUR_US, frontier(tape, ("trades",)))
    stored = stored_asof(tape, SIGNAL).get(HORIZON)
    if not run.redo and stored is not None and asof <= stored:
        run.said.append(f"activity: nothing new since {_dt(stored):%Y-%m-%d %H:%M}")
        return run
    lo = asof - HOUR_US
    t = trades(asof - DAY_US - HOUR_US, asof)
    if t.height:
        t = t.with_columns((pl.col("price") * pl.col("size")).alias("usd"), pl.col("ts").dt.epoch("us").alias("_t"))
    past = history(tape, asof - HISTORY_DAYS * DAY_US, asof)
    tickers = sorted(set(t["ticker"].to_list())) if t.height else []
    for ticker in tickers:
        mine = t.filter(pl.col("ticker") == ticker)
        run.rows.extend(_rows(ticker, asof, mine.filter(pl.col("_t") >= lo), mine.filter(pl.col("_t") < lo), past.filter(pl.col("ticker_i") == ticker), run))
    run.said.append(f"activity: {len(tickers)} instruments for the hour to {_dt(asof):%Y-%m-%d %H:%M}")
    return run


def _robust(values: list[float]) -> tuple[float, float] | None:
    v = sorted(values)
    n = len(v)
    med = v[n // 2] if n % 2 else (v[n // 2 - 1] + v[n // 2]) / 2
    dev = sorted(abs(x - med) for x in v)
    mad = dev[n // 2] if n % 2 else (dev[n // 2 - 1] + dev[n // 2]) / 2
    return (med, MAD_SCALE * mad) if mad > 0 else None


def _rows(ticker: str, asof: int, hour: pl.DataFrame, before: pl.DataFrame, past: pl.DataFrame, run: Run) -> list[dict]:
    hod, weekend = _bucket(asof)
    base = {
        "signal": SIGNAL, "horizon": HORIZON, "ticker_i": ticker, "ticker_j": None, "h": 1, "asof_micros": asof,
        "target_micros": asof, "computed_micros": run.computed_micros, "fitted_through_micros": None, "fit_from_micros": None,
        "model": "seasonal-robust-z",
        "params": json.dumps({"hour_of_day": hod, "weekend": weekend, "history_days": HISTORY_DAYS, "min_bucket": MIN_BUCKET, "large_q": LARGE_Q}, sort_keys=True),
        "fitted": False, "after_gap": False, "code": run.code, "run_id": run.run_id,
    }  # fmt: skip
    rows = []

    def put(measure, value, reason=None, n=None):
        ok = value is not None and math.isfinite(value)
        rows.append({**base, "measure": measure, "value": float(value) if ok else None, "absent": None if ok else (reason or "not defined"), "n_eff": n})

    if not hour.height:
        for m in (*RAW, "volume_z", "count_z", "size_z", "volume_pct", "large_share"):
            put(m, None, "no trades in the hour")
        return rows
    usd, count = float(hour["usd"].sum()), hour.height
    now = {"notional_usd": usd, "trade_count": float(count), "avg_trade_usd": usd / count}
    for m in RAW:
        put(m, now[m], n=count)

    # The same bucket: hours that started at this hour of day, on the same kind of day.
    same = past.with_columns(pl.col("asof_micros").map_elements(lambda a: _bucket(a) == (hod, weekend), return_dtype=pl.Boolean).alias("_same")).filter(pl.col("_same")) if past.height else past
    for measure, name in (("notional_usd", "volume_z"), ("trade_count", "count_z"), ("avg_trade_usd", "size_z")):
        values = [math.log(v) for v in same.filter(pl.col("measure") == measure)["value"].to_list() if v and v > 0] if same.height else []
        if len(values) < MIN_BUCKET:
            put(name, None, f"{len(values)} stored {'weekend' if weekend else 'weekday'} hours at {hod:02d}:00 in the {HISTORY_DAYS} days before, under {MIN_BUCKET}")
            continue
        fit = _robust(values)
        put(name, (math.log(now[measure]) - fit[0]) / fit[1] if fit else None, "the stored hours do not vary", len(values))
        if measure == "notional_usd":
            put("volume_pct", sum(1 for v in values if v < math.log(usd)) / len(values), n=len(values))
    if not any(r["measure"] == "volume_pct" for r in rows):
        put("volume_pct", None, f"under {MIN_BUCKET} stored hours in the bucket")

    if before.height >= 100:
        cut = float(before["usd"].quantile(LARGE_Q))
        put("large_share", float(hour.filter(pl.col("usd") > cut)["usd"].sum()) / usd, n=before.height)
    else:
        put("large_share", None, f"{before.height} trades in the 24 hours before, under 100")
    return rows
