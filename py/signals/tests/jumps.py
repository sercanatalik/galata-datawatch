"""Jumps on 5m bars made in the test."""

from __future__ import annotations

import math
from datetime import UTC, datetime, timedelta

import numpy as np
import polars as pl
import pytest

from galata_signals import jumps, varcov

T0 = datetime(2026, 9, 21, tzinfo=UTC)
FIVE = timedelta(minutes=5)


def _bars(returns_by_ticker: dict[str, np.ndarray]) -> pl.DataFrame:
    frames = []
    for t, r in returns_by_ticker.items():
        close = 100 * np.exp(np.cumsum(np.concatenate([[0.0], r])))
        n = len(close)
        frames.append(pl.DataFrame({"ticker": t, "ts": [T0 + i * FIVE for i in range(n)], "close_ts": [T0 + (i + 1) * FIVE for i in range(n)],
                                    "open": close, "high": close, "low": close, "close": close}))  # fmt: skip
    return pl.concat(frames)


def _run(frame, tmp_path, monkeypatch):
    monkeypatch.setattr(jumps, "fivemin", lambda: frame)
    run = varcov.Run(computed_micros=varcov.micros(frame["close_ts"].max()) + 1, code="abc", signal="jumps")
    return {(r["ticker_i"], r["measure"]): r for r in jumps.compute(tmp_path, run).rows}


def a_planted_jump_is_flagged(tmp_path, monkeypatch):
    r = np.random.default_rng(1).normal(0, 0.001, 7 * 288)
    r[-1] = 0.04  # 40 times the local scale, on the last bar
    got = _run(_bars({"BTC": r}), tmp_path, monkeypatch)
    assert got[("BTC", "jump_last")]["value"] == 1.0
    assert got[("BTC", "jumps_up_24h")]["value"] >= 1
    window = r[-288:]
    assert got[("BTC", "jump_var_up_24h")]["value"] >= 0.04**2 / float((window**2).sum()) - 1e-12
    assert got[("BTC", "intensity_up")]["value"] == pytest.approx(1.0, abs=0.05)  # at the asof


def a_diffusion_has_no_jump_share_to_speak_of(tmp_path, monkeypatch):
    r = np.random.default_rng(2).normal(0, 0.001, 7 * 288)
    got = _run(_bars({"ETH": r}), tmp_path, monkeypatch)
    assert got[("ETH", "rj_z_24h")]["value"] < 3.09


def the_ratio_statistic_is_its_formula():
    r = list(np.random.default_rng(3).normal(0, 1, 288))
    m = len(r)
    a = np.abs(r)
    rv = float(np.sum(np.square(r)))
    bv = math.pi / 2 * m / (m - 1) * float(np.sum(a[1:] * a[:-1]))
    mu43 = 2 ** (2 / 3) * math.gamma(7 / 6) / math.gamma(0.5)
    tq = m * mu43**-3 * m / (m - 2) * float(np.sum((a[2:] * a[1:-1] * a[:-2]) ** (4 / 3)))
    rj = (rv - bv) / rv
    theta = (math.pi / 2) ** 2 + math.pi - 5
    z = rj / math.sqrt(theta / m * max(1, tq / bv**2))
    got = jumps.ratio(r)
    assert got["rj"] == pytest.approx(rj) and got["z"] == pytest.approx(z) and got["share"] == pytest.approx(max(rj, 0))
    assert theta == pytest.approx(0.6090, abs=1e-4)


def an_old_jump_decays():
    asof = 1_000_000_000_000
    assert jumps.intensity([asof - 6 * 3_600_000_000], asof) == pytest.approx(math.exp(-1))
    assert jumps.intensity([asof + 1], asof) == 0.0


def a_thin_window_is_absent(tmp_path, monkeypatch):
    r = np.random.default_rng(4).normal(0, 0.001, 7 * 288)
    frame = _bars({"GOLD": r})
    last = frame["close_ts"].max()
    frame = frame.filter(~((pl.col("close_ts") > last - timedelta(hours=12)) & (pl.col("close_ts") < last - timedelta(hours=2))))
    got = _run(frame, tmp_path, monkeypatch)
    share = got[("GOLD", "jump_share_24h")]
    assert share["value"] is None and "under 259" in share["absent"]
