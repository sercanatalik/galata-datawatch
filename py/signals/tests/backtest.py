"""Backtests of stored tails against returns made in the test."""

from __future__ import annotations

import math
from datetime import UTC, datetime, timedelta

import numpy as np
import polars as pl
import pytest
from scipy import stats

from galata_signals import backtest, varcov

ASOF = datetime(2026, 12, 1, tzinfo=UTC)
A = varcov.micros(ASOF)
H = 3_600_000_000
HZ = varcov.Horizon.declared("1h", {"bars": "1h", "model": "garch", "dist": "t", "corr": "ccc"})


def _true_tail(nu=4.0, scale=0.01):
    """VaR and ES of a scaled t(ν) with unit variance, as positive losses."""
    s = scale * math.sqrt((nu - 2) / nu)
    q99, q975 = stats.t.ppf(0.01, nu), stats.t.ppf(0.025, nu)
    es = s * stats.t.pdf(q975, nu) / 0.025 * (nu + q975**2) / (nu - 1)
    return -s * q99, -s * q975, es


def a_right_es_has_z2_near_zero_and_an_understated_one_is_negative():
    rng = np.random.default_rng(3)
    r = 0.01 * math.sqrt(2 / 4) * rng.standard_t(4, 200_000)
    v99, v975, es = _true_tail()
    ones = np.ones_like(r)
    assert abs(backtest.z2(r, v975 * ones, es * ones, 0.025)) < 0.05
    assert backtest.z2(r, v975 * ones, v975 * ones, 0.025) < -0.2  # ES taken as the VaR: understated


def _served(monkeypatch, n, var_scale=1.0, seed=4):
    rng = np.random.default_rng(seed)
    v99, v975, es = _true_tail()
    starts = [A - (n + 1 - k) * H for k in range(n)]
    tails = pl.DataFrame(
        [{"horizon": "1h", "ticker": "BTC", "asof": s, "measure": m, "value": v * var_scale} for s in starts for m, v in (("var_99", v99), ("var_975", v975), ("es_975", es))],
        schema={"horizon": pl.String, "ticker": pl.String, "asof": pl.Int64, "measure": pl.String, "value": pl.Float64},
    )
    # The bar each tail forecast starts at its asof; prices whose log returns are t(4).
    rets = 0.01 * math.sqrt(2 / 4) * rng.standard_t(4, n + 1)
    closes = 100 * np.exp(np.cumsum(rets))
    t0 = datetime.fromtimestamp((starts[0] - H) / 1e6, tz=UTC)
    bars = pl.DataFrame(
        {"ticker": "BTC", "ts": [t0 + timedelta(hours=k) for k in range(n + 1)], "close_ts": [t0 + timedelta(hours=k + 1) for k in range(n + 1)], "open": closes, "high": closes, "low": closes, "close": closes}
    )
    monkeypatch.setattr(backtest, "stored_tails", lambda tape, lo, hi: tails)
    monkeypatch.setattr(backtest, "bars", lambda h, s, start=None: bars)


def _got(tmp_path):
    run = varcov.Run(computed_micros=A + 45 * 60_000_000, code="abc", signal="backtest")
    return {r["measure"]: r for r in backtest.compute([HZ], tmp_path, run).rows}


def a_right_tail_passes_its_coverage(tmp_path, monkeypatch):
    _served(monkeypatch, 2000)
    got = _got(tmp_path)
    assert got["n"]["value"] == 2000  # every forecast met its bar
    assert 0.01 < got["hit_rate_975"]["value"] < 0.04 and got["kupiec_p_975"]["value"] > 0.01
    assert abs(got["z2_975"]["value"]) < 0.5


def a_tail_half_as_wide_fails_it(tmp_path, monkeypatch):
    _served(monkeypatch, 2000, var_scale=0.5)
    got = _got(tmp_path)
    assert got["hit_rate_99"]["value"] > 0.03 and got["kupiec_p_99"]["value"] < 1e-6
    assert got["z2_975"]["value"] < -1.8  # red


def a_short_record_is_absent(tmp_path, monkeypatch):
    _served(monkeypatch, 100)
    got = _got(tmp_path)
    assert got["n"]["value"] is None and "under 250" in got["n"]["absent"]
