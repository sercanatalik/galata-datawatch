"""`jumps`: how much of each instrument's last day was discontinuous (Tier 16).

Lee and Mykland's (2008) test judges each 5-minute return against a local
bipower scale made only of the returns before it (`gr.jumps.lee_mykland`), after
Boudt, Croux and Laurent's (2011) robust hour-of-day factor, fitted here on the
days **before** the 24-hour window so that it never sees the returns it scales.
Over the window:

- Huang and Tauchen's (2005) relative jump, RJ = (RV − BV)/RV, reported as the
  jump share max(RJ, 0), and its ratio statistic
  z = RJ / √(θ M⁻¹ max(1, TQ/BV²)), θ = (π/2)² + π − 5, one-sided N(0, 1)
  under no jump (ABD 2007 eq. 19–20; Dumitru and Urga for the small-sample
  factors M/(M − 1) on BV and M/(M − 2) on TQ);
- the flagged bars counted and their squared returns shared by sign: Lee and
  Wang (2024, crypto) find positive-jump variance predicts negative returns;
- a jump intensity Σ exp(−Δt/6 h) over 7 days, a Hawkes intensity with fixed
  parameters, by sign.

A 24-hour window recomputed at every 5m close overlaps the last almost
entirely: `rj_z_24h` is a smoothed state, not a fresh test each run.
"""

from __future__ import annotations

import json
import math
from pathlib import Path

import polars as pl

import galata_research as gr
from galata_research import Refused

from .bars import bars
from .varcov import Run, micros, stored_asof

SIGNAL = "jumps"
HORIZON = "5m"
M = 288  # 5-minute returns in 24 hours
COVERAGE = 0.9
ALPHA = 0.01
TAU_US = 6 * 3_600_000_000
INTENSITY_DAYS = 7
THETA = (math.pi / 2) ** 2 + math.pi - 5
MU_43 = 2 ** (2 / 3) * math.gamma(7 / 6) / math.gamma(0.5)


def fivemin() -> pl.DataFrame:
    """5-minute bars from the tape's 1m, whole buckets only. A name a test can replace."""
    return bars("5m", "1m")


def ratio(r: list[float]) -> dict:
    """RV, BV, TQ, RJ, the jump share and Huang and Tauchen's z over one window of returns."""
    m = len(r)
    if m < 3:
        raise Refused(f"{m} returns cannot make bipower and tripower variation")
    a = [abs(x) for x in r]
    rv = sum(x * x for x in r)
    bv = (math.pi / 2) * (m / (m - 1)) * sum(a[i] * a[i - 1] for i in range(1, m))
    tq = m * MU_43**-3 * (m / (m - 2)) * sum((a[i] * a[i - 1] * a[i - 2]) ** (4 / 3) for i in range(2, m))
    if rv <= 0 or bv <= 0:
        raise Refused("no variation in the window")
    rj = (rv - bv) / rv
    z = rj / math.sqrt(THETA / m * max(1.0, tq / bv**2))
    return {"rv": rv, "bv": bv, "tq": tq, "rj": rj, "share": max(rj, 0.0), "z": z}


def intensity(jump_times: list[int], asof: int, tau: int = TAU_US) -> float:
    """Σ exp(−(asof − t)/τ) over jumps at or before the asof."""
    return sum(math.exp(-(asof - t) / tau) for t in jump_times if t <= asof)


def compute(tape: Path, run: Run) -> Run:
    frame = fivemin()
    if frame.height == 0:
        run.said.append("jumps: no 5m bars")
        return run
    asof_ts = frame["close_ts"].max()
    asof = micros(asof_ts)
    stored = stored_asof(tape, SIGNAL).get(HORIZON)
    if stored is not None and asof <= stored:
        run.said.append(f"jumps: nothing new since {asof_ts:%Y-%m-%d %H:%M}")
        return run
    returns = gr.timeseries.returns(frame, kind="log").drop_nulls("return")
    start = returns["ts"].min()
    cut = asof - 24 * 3_600_000_000
    try:
        factors = gr.jumps.periodicity(returns, slot="1h", by="time_of_day", fit=(start, _dt(cut)))
        judged = gr.jumps.lee_mykland(returns, alpha=ALPHA, periodicity=factors)
    except Refused as why:
        for t in sorted(returns["ticker"].unique().to_list()):
            run.rows.extend(_absent(t, asof, str(why), run))
        run.said.append(f"jumps: absent, {why}")
        return run
    for t in sorted(judged["ticker"].unique().to_list()):
        run.rows.extend(_rows(t, asof, judged.filter(pl.col("ticker") == t).sort("ts"), run))
    run.said.append(f"jumps: {judged['ticker'].n_unique()} instruments at {asof_ts:%Y-%m-%d %H:%M}")
    return run


def _dt(us: int):
    from datetime import UTC, datetime

    return datetime.fromtimestamp(us / 1_000_000, tz=UTC)


def _base(ticker: str, asof: int, last_bar: int | None, run: Run) -> dict:
    return {
        "signal": SIGNAL, "horizon": HORIZON, "ticker_i": ticker, "ticker_j": None, "h": 1, "asof_micros": asof,
        "target_micros": asof, "computed_micros": run.computed_micros, "fitted_through_micros": None, "fit_from_micros": None,
        "model": "lee-mykland/huang-tauchen",
        "params": json.dumps({"alpha": ALPHA, "rule": "gumbel", "window_bars": M, "tau_hours": 6, "last_bar_micros": last_bar}, sort_keys=True),
        "fitted": False, "after_gap": False, "code": run.code, "run_id": run.run_id,
    }  # fmt: skip


MEASURES = ("L_last", "jump_last", "jumps_up_24h", "jumps_down_24h", "jump_share_24h", "rj_z_24h", "jump_var_up_24h", "jump_var_down_24h", "intensity_up", "intensity_down")


def _absent(ticker: str, asof: int, reason: str, run: Run) -> list[dict]:
    base = _base(ticker, asof, None, run)
    return [{**base, "measure": m, "value": None, "absent": reason, "n_eff": None} for m in MEASURES]


def _rows(ticker: str, asof: int, judged: pl.DataFrame, run: Run) -> list[dict]:
    last = judged.tail(1).row(0, named=True)
    base = _base(ticker, asof, micros(last["close_ts"]), run)
    rows = []

    def put(measure, value, reason=None, n=None):
        ok = value is not None and math.isfinite(value)
        rows.append({**base, "measure": measure, "value": float(value) if ok else None, "absent": None if ok else (reason or "not defined"), "n_eff": n})

    put("L_last", last["L"], "the local scale needs 270 returns before the bar")
    put("jump_last", float(bool(last["jump"])) if last["L"] is not None else None, "the local scale needs 270 returns before the bar")

    since = micros(last["close_ts"]) - 24 * 3_600_000_000
    window = judged.filter(pl.col("close_ts").dt.epoch("us") > since)
    if window.height < COVERAGE * M:
        why = f"{window.height} 5m returns in the 24 hours to the last bar, under {int(COVERAGE * M)}"
        for m in MEASURES[2:8]:
            put(m, None, why, window.height)
    else:
        r = window["return"].to_list()
        found = ratio(r)
        up = window.filter(pl.col("jump") & (pl.col("return") > 0))
        down = window.filter(pl.col("jump") & (pl.col("return") < 0))
        put("jumps_up_24h", up.height, n=window.height)
        put("jumps_down_24h", down.height, n=window.height)
        put("jump_share_24h", found["share"], n=window.height)
        put("rj_z_24h", found["z"], n=window.height)
        put("jump_var_up_24h", float((up["return"] ** 2).sum()) / found["rv"], n=window.height)
        put("jump_var_down_24h", float((down["return"] ** 2).sum()) / found["rv"], n=window.height)
    recent = judged.filter(pl.col("jump") & (pl.col("close_ts").dt.epoch("us") > asof - INTENSITY_DAYS * 86_400_000_000))
    for sign, name in ((1, "intensity_up"), (-1, "intensity_down")):
        times = [micros(t) for t, x in zip(recent["close_ts"], recent["return"]) if (x > 0) == (sign > 0)]
        put(name, intensity(times, asof))
    return rows
