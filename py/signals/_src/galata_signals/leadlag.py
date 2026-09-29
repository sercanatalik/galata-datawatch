"""`leadlag`: whether BTC moves first, and by how long, hour by hour (Tier 16).

Per closed hour, for BTC against each other instrument, from the tape's top
of book (mid prices, on each instrument's own clock): Hoffmann, Rosenbaum and
Yoshida's (2013) shifted Hayashi–Yoshida correlation (`gr.leadlag.lead_lag`)
over Huth and Abergel's (2014) lag grid to ±30 s:

- `lead_ms`, the shift that maximises |ρ|: positive, BTC leads;
- `rho_lead` and `rho_0`, the correlation there and without a shift;
- `llr`, Σ_{lag>0} ρ² / Σ_{lag<0} ρ²: above 1, BTC leads.

No resampling: a previous-tick grid would make the more often quoted price
look like the leader whatever the truth. A lead on the grid's edge (±30 s) is
reported but flagged in params, and an hour with under 300 quote updates on
either side is absent.

**One venue's blocks set the resolution.** Every instrument is stamped with the
venue's block time (about every 84 ms), and 71% of BTC's and ETH's update
times are the same block. Over 13 whole hours on 2026-09-28 every other
instrument peaked at −100 or −200 ms with an LLR below 1, one block ahead of
BTC: at the resolution limit, and unexplained (it may be how the venue emits
top-of-book within a block). A lead within one block is flagged
`within_one_block` in the row's params and is not a lead to act on.
"""

from __future__ import annotations

import json
import math
from pathlib import Path

import polars as pl

import galata_research as gr

from .basis import _dt
from .frontier import frontier, period
from .liquidity import quotes
from .varcov import Run, stored_asof

SIGNAL = "leadlag"
HORIZON = "1h"
HOUR_US = 3_600_000_000
LEADER = "BTC"
MIN_TICKS = 300
BLOCK_MS = 100
_GRID = (0, 100, 200, 300, 400, 500, 600, 700, 800, 900, 1000, 2000, 3000, 4000, 5000, 6000, 7000, 8000, 9000, 10000, 15000, 20000, 30000)
LAGS_MS = tuple(sorted({g for g in _GRID} | {-g for g in _GRID}))
MEASURES = ("lead_ms", "rho_lead", "rho_0", "llr")


def compute(tape: Path, run: Run) -> Run:
    # The last whole hour on the tape as well as by the clock (frontier.py).
    asof = period(run, HOUR_US, frontier(tape, ("quotes",)))
    stored = stored_asof(tape, SIGNAL).get(HORIZON)
    if not run.redo and stored is not None and asof <= stored:
        run.said.append(f"leadlag: nothing new since {_dt(stored):%Y-%m-%d %H:%M}")
        return run
    lo = asof - HOUR_US
    q = quotes(lo, asof)
    if q.height:
        q = q.filter((pl.col("ask_px") > pl.col("bid_px")) & (pl.col("ts").dt.epoch("us") >= lo)).with_columns(((pl.col("bid_px") + pl.col("ask_px")) / 2).alias("price"))
    tickers = sorted(set(q["ticker"].to_list())) if q.height else []
    lead = q.filter(pl.col("ticker") == LEADER).select("ts", "price") if q.height else q
    for t in tickers:
        if t != LEADER:
            run.rows.extend(_rows(t, asof, lead, q.filter(pl.col("ticker") == t).select("ts", "price"), run))
    run.said.append(f"leadlag: {LEADER} against {max(len(tickers) - 1, 0)} for the hour to {_dt(asof):%Y-%m-%d %H:%M}")
    return run


def _rows(follower: str, asof: int, lead: pl.DataFrame, other: pl.DataFrame, run: Run) -> list[dict]:
    base = {
        "signal": SIGNAL, "horizon": HORIZON, "ticker_i": LEADER, "ticker_j": follower, "h": 1, "asof_micros": asof,
        "target_micros": asof, "computed_micros": run.computed_micros, "fitted_through_micros": None, "fit_from_micros": None,
        "model": "shifted-hayashi-yoshida/mid",
        "fitted": False, "after_gap": False, "code": run.code, "run_id": run.run_id,
    }  # fmt: skip
    params = {"grid_ms": max(_GRID), "min_ticks": MIN_TICKS}
    rows = []

    def put(measure, value, reason=None, n=None):
        ok = value is not None and math.isfinite(value)
        rows.append({**base, "params": json.dumps(params, sort_keys=True), "measure": measure, "value": float(value) if ok else None, "absent": None if ok else (reason or "not defined"), "n_eff": n})

    ticks = (lead["ts"].n_unique() if lead.height else 0, other["ts"].n_unique() if other.height else 0)
    if min(ticks) < MIN_TICKS:
        for m in MEASURES:
            put(m, None, f"{LEADER} has {ticks[0]} and {follower} {ticks[1]} quote updates in the hour, under {MIN_TICKS}")
        return rows
    found = gr.leadlag.lead_lag(lead, other, LAGS_MS)
    if not found.height:
        for m in MEASURES:
            put(m, None, "no overlap between the two")
        return rows
    row = found.row(0, named=True)
    params["edge"] = row["lead_ms"] is not None and abs(row["lead_ms"]) == max(_GRID)
    params["within_one_block"] = row["lead_ms"] is not None and abs(row["lead_ms"]) <= BLOCK_MS
    n = min(row["x_ticks"], row["y_ticks"])
    for m in MEASURES:
        put(m, row[m], "no correlation on either side of zero" if m == "llr" else None, n)
    return rows
