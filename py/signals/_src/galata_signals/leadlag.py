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
venue's block time (about every 84 ms), and about 80% of each instrument's
update times are one of BTC's. With tied stamps, Hayashi–Yoshida at a shift
of exactly 0 pairs each interval only with its twin, while any shift, however
small, also takes in the adjacent block on one side: on 2026-09-28 13:00–14:00
ETH's ρ was 0.24 at 0 and 0.40 at −25 ms, and a simulation of two prices with
no lead on a shared 84 ms grid is flat within a block. With 0 and then ±100 ms
on the grid, the peak fell to −100 ms for every instrument: the notch, not a
lead (`resolve-the-block`, 2026-09-29). So:

- the grid holds ±1 ms, a shift that takes in exactly one adjacent block;
- `rho_btc_first` is ρ at +1 ms (the other's move paired with BTC's in the
  block before, too), `rho_other_first` at −1 ms, and `block_asymmetry` their
  difference: positive, BTC's quote more often moves a block first;
- a peak within one block (|θ| ≤ 100 ms) is stored as `lead_ms` 0, flagged
  `within_one_block`, with `rho_lead` the correlation there;
- a peak beyond the block that beats the block's best |ρ| by under 2/√n
  (n the fewer updates; about a correlation's standard error under
  independence, a rule of thumb for HY) is flagged `indistinct` and stored as
  0 too: on the xyz perps the curve is flat to ±0.01 over ±300 ms, and its
  argmax there is noise;
- `rho_0` stays the correlation at exactly 0, which ties understate.
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
from .split import by
from .varcov import Run, stored_asof

SIGNAL = "leadlag"
HORIZON = "1h"
HOUR_US = 3_600_000_000
LEADER = "BTC"
MIN_TICKS = 300
BLOCK_MS = 100
_GRID = (0, 1, 100, 200, 300, 400, 500, 600, 700, 800, 900, 1000, 2000, 3000, 4000, 5000, 6000, 7000, 8000, 9000, 10000, 15000, 20000, 30000)
LAGS_MS = tuple(sorted({g for g in _GRID} | {-g for g in _GRID}))
MEASURES = ("lead_ms", "rho_lead", "rho_0", "llr", "rho_btc_first", "rho_other_first", "block_asymmetry")


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
    quotes_of = by(q)
    for t in tickers:
        if t != LEADER:
            run.rows.extend(_rows(t, asof, lead, quotes_of(t).select("ts", "price"), run))
    run.said.append(f"leadlag: {LEADER} against {max(len(tickers) - 1, 0)} for the hour to {_dt(asof):%Y-%m-%d %H:%M}")
    return run


def _rows(follower: str, asof: int, lead: pl.DataFrame, other: pl.DataFrame, run: Run) -> list[dict]:
    base = {
        "signal": SIGNAL, "horizon": HORIZON, "ticker_i": LEADER, "ticker_j": follower, "h": 1, "asof_micros": asof,
        "target_micros": asof, "computed_micros": run.computed_micros, "fitted_through_micros": None, "fit_from_micros": None,
        "model": "shifted-hayashi-yoshida/mid/block",
        "fitted": False, "after_gap": False, "code": run.code, "run_id": run.run_id,
    }  # fmt: skip
    params = {"grid_ms": max(_GRID), "min_ticks": MIN_TICKS, "block_ms": BLOCK_MS, "sub_block_ms": 1}
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
    curve = gr.leadlag.hayashi_yoshida(lead, other, (-BLOCK_MS, -1, 0, 1, BLOCK_MS))
    first = dict(zip(curve["lag_ms"].to_list(), curve["rho"].to_list(), strict=True))
    row["rho_btc_first"], row["rho_other_first"] = first.get(1.0), first.get(-1.0)
    in_block = [(lag, rho) for lag, rho in first.items() if rho is not None]
    block_lag, block_rho = max(in_block, key=lambda lr: abs(lr[1])) if in_block else (0.0, None)
    params["indistinct"] = (
        not params["within_one_block"] and row["rho_lead"] is not None and block_rho is not None
        and abs(row["rho_lead"]) - abs(block_rho) < 2 / math.sqrt(n)
    )  # fmt: skip
    if params["within_one_block"] or params["indistinct"]:
        row["lead_ms"], row["rho_lead"] = 0.0, block_rho
    both = row["rho_btc_first"] is not None and row["rho_other_first"] is not None
    row["block_asymmetry"] = row["rho_btc_first"] - row["rho_other_first"] if both else None
    for m in MEASURES:
        put(m, row[m], "no correlation on either side of zero" if m == "llr" else None, n)
    return rows
