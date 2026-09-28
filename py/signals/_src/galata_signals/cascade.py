"""`cascade`: forced closures inferred from open interest, hour by hour (Tier 16).

Hyperliquid's public trades carry no liquidation flag, so, as the research on
cascades does (observing "consequences of forced liquidation, open interest
and aggressor flow, rather than liquidations themselves"), each minute is
judged from the tape's marks:

- r, the mark's log return over the minute, and d, the log change of open
  interest; **consecutive minutes only**, so an outage is not a crash;
- each as a robust z (median and MAD) against the 24 hours of minutes before
  the hour;
- a **long liquidation** minute has z_r ≤ −4 and z_d ≤ −4, a **short squeeze**
  minute z_r ≥ +4 and z_d ≤ −4, and in both the closed notional
  (−ΔOI × mark) at least max($250k, 0.25% of open interest);
- same-sign flagged minutes at most 2 minutes apart are one event, and an
  event counts only when it closed at least 1% of the open interest standing
  when it began (the cumulative condition the cascade studies apply): a
  single tail minute is not a cascade.

The notional is an **upper bound** on what was forced: open interest also falls
on voluntary closes and on auto-deleveraging.
"""

from __future__ import annotations

import json
import math
from pathlib import Path

import polars as pl

from .basis import _dt, marks
from .carry import Declared
from .varcov import Run, stored_asof

SIGNAL = "cascade"
HORIZON = "1h"
HOUR_US = 3_600_000_000
MIN_US = 60_000_000
TRAIL_MIN = 1440
TRAIL_FLOOR = 720
Z = 4.0
FLOOR_USD = 250_000.0
FLOOR_SHARE = 0.0025
MERGE_GAP = 2
EVENT_SHARE = 0.01
MAD_SCALE = 1.4826


def minutes(m: pl.DataFrame) -> pl.DataFrame:
    """Per ticker and minute (by its start): the last mark and open interest, with r and d against the minute before, when it is there."""
    return (
        m.drop_nulls(["mark", "open_interest"])
        .filter((pl.col("mark") > 0) & (pl.col("open_interest") > 0))
        .sort("ticker", "t")
        .group_by("ticker", (pl.col("t") // MIN_US * MIN_US).alias("minute"), maintain_order=True)
        .agg(pl.col("mark").last(), pl.col("open_interest").last())
        .sort("ticker", "minute")
        .with_columns(
            pl.when(pl.col("minute") - pl.col("minute").shift(1).over("ticker") == MIN_US)
            .then(pl.col("mark").log() - pl.col("mark").shift(1).over("ticker").log())
            .alias("r"),
            pl.when(pl.col("minute") - pl.col("minute").shift(1).over("ticker") == MIN_US)
            .then(pl.col("open_interest").log() - pl.col("open_interest").shift(1).over("ticker").log())
            .alias("d"),
            (pl.col("open_interest").shift(1).over("ticker") - pl.col("open_interest")).alias("closed"),
        )
    )


def _robust(values: list[float]) -> tuple[float, float] | None:
    v = sorted(values)
    n = len(v)
    med = v[n // 2] if n % 2 else (v[n // 2 - 1] + v[n // 2]) / 2
    dev = sorted(abs(x - med) for x in v)
    mad = dev[n // 2] if n % 2 else (dev[n // 2 - 1] + dev[n // 2]) / 2
    return (med, MAD_SCALE * mad) if mad > 0 else None


def compute(declared: Declared, tape: Path, run: Run) -> Run:
    asof = run.computed_micros // HOUR_US * HOUR_US
    stored = stored_asof(tape, SIGNAL).get(HORIZON)
    if stored is not None and asof <= stored:
        run.said.append(f"cascade: nothing new since {_dt(stored):%Y-%m-%d %H:%M}")
        return run
    lo = asof - HOUR_US
    mins = minutes(marks(tape, lo - (TRAIL_MIN + 2) * MIN_US, asof))
    tickers = sorted(set(mins["ticker"].to_list()))
    for t in tickers:
        run.rows.extend(_rows(declared.dex(t), t, asof, mins.filter(pl.col("ticker") == t), run))
    run.said.append(f"cascade: {len(tickers)} instruments for the hour to {_dt(asof):%Y-%m-%d %H:%M}")
    return run


def _rows(dex: str, ticker: str, asof: int, mins: pl.DataFrame, run: Run) -> list[dict]:
    lo = asof - HOUR_US
    base = {
        "signal": SIGNAL, "horizon": HORIZON, "ticker_i": ticker, "ticker_j": None, "h": 1, "asof_micros": asof,
        "target_micros": asof, "computed_micros": run.computed_micros, "fitted_through_micros": None, "fit_from_micros": None,
        "model": "oi-drop/robust-z",
        "params": json.dumps({"dex": dex, "z": Z, "trail_minutes": TRAIL_MIN, "floor_usd": FLOOR_USD, "floor_share": FLOOR_SHARE, "merge_gap_minutes": MERGE_GAP, "event_share": EVENT_SHARE}, sort_keys=True),
        "fitted": False, "after_gap": False, "code": run.code, "run_id": run.run_id,
    }  # fmt: skip
    rows = []
    measures = ("liq_long_usd", "liq_short_usd", "liq_intensity", "cascade_events", "largest_event_usd", "max_joint_z")

    def put(measure, value, reason=None, n=None):
        ok = value is not None and math.isfinite(value)
        rows.append({**base, "measure": measure, "value": float(value) if ok else None, "absent": None if ok else (reason or "not defined"), "n_eff": n})

    trail = mins.filter((pl.col("minute") < lo) & pl.col("r").is_not_null())
    hour = mins.filter((pl.col("minute") >= lo) & (pl.col("minute") < asof))
    zr, zd = _robust(trail["r"].to_list()) if trail.height >= TRAIL_FLOOR else None, _robust(trail["d"].to_list()) if trail.height >= TRAIL_FLOOR else None
    if zr is None or zd is None:
        why = f"{trail.height} consecutive minutes in the 24 hours before, under {TRAIL_FLOOR}" if trail.height < TRAIL_FLOOR else "the trailing minutes do not vary"
        for m in measures:
            put(m, None, why)
        return rows
    if not hour.filter(pl.col("r").is_not_null()).height:
        for m in measures:
            put(m, None, "no consecutive minutes in the hour")
        return rows

    flagged, joint = [], 0.0
    for row in hour.filter(pl.col("r").is_not_null()).iter_rows(named=True):
        z_r = (row["r"] - zr[0]) / zr[1]
        z_d = (row["d"] - zd[0]) / zd[1]
        if z_d < 0:
            joint = max(joint, min(abs(z_r), -z_d))
        notional = max(row["closed"], 0.0) * row["mark"]
        floor = max(FLOOR_USD, FLOOR_SHARE * row["open_interest"] * row["mark"])
        if z_d <= -Z and notional >= floor and abs(z_r) >= Z:
            # The open interest standing before the minute, for the event's share.
            flagged.append((row["minute"], "long" if z_r < 0 else "short", notional, row["closed"], row["open_interest"] + row["closed"]))

    events: list[list] = []  # side, notional, last minute, coins closed, coins standing at the start
    for minute, side, notional, coins, standing in flagged:
        if events and events[-1][0] == side and minute - events[-1][2] <= (MERGE_GAP + 1) * MIN_US:
            events[-1][1] += notional
            events[-1][2] = minute
            events[-1][3] += coins
        else:
            events.append([side, notional, minute, coins, standing])
    events = [e for e in events if e[3] >= EVENT_SHARE * e[4]]
    long_usd = sum(e[1] for e in events if e[0] == "long")
    short_usd = sum(e[1] for e in events if e[0] == "short")
    oi_usd = float((hour["open_interest"] * hour["mark"]).mean())
    n = hour.height
    put("liq_long_usd", long_usd, n=n)
    put("liq_short_usd", short_usd, n=n)
    put("liq_intensity", (long_usd + short_usd) / oi_usd if oi_usd > 0 else None, "no open interest", n)
    put("cascade_events", float(len(events)), n=n)
    put("largest_event_usd", max((e[1] for e in events), default=0.0), n=n)
    put("max_joint_z", joint, n=n)
    return rows
