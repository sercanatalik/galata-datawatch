"""HARQ on hourly bars made in the test."""

from __future__ import annotations

import math
import random
from datetime import UTC, datetime, timedelta

import polars as pl
import pytest

from galata_signals import realvol, varcov

ASOF = datetime(2026, 9, 29, tzinfo=UTC)
HOUR = timedelta(hours=1)


def _bars(days=220, ticker="BTC", seed=3, drop=None):
    """`days` of hourly bars to ASOF, each day's σ a slow AR(1) in logs; `drop` is an hour to leave out."""
    rng = random.Random(seed)
    rows, price, log_sigma = [], 100.0, math.log(0.01)
    for d in range(days):
        log_sigma = math.log(0.01) + 0.98 * (log_sigma - math.log(0.01)) + rng.gauss(0, 0.1)
        hourly = math.exp(log_sigma) / math.sqrt(24)
        for i in range(24):
            ts = ASOF - timedelta(days=days - d) + i * HOUR
            price *= math.exp(rng.gauss(0, hourly))
            if ts != drop:
                rows.append({"ticker": ticker, "ts": ts, "close_ts": ts + HOUR, "open": price, "high": price, "low": price, "close": price})
    return pl.DataFrame(rows, schema={"ticker": pl.String, "ts": pl.Datetime("us", "UTC"), "close_ts": pl.Datetime("us", "UTC"), "open": pl.Float64, "high": pl.Float64, "low": pl.Float64, "close": pl.Float64})


@pytest.fixture(name="served")
def _served(monkeypatch):
    held = {"bars": _bars()}
    monkeypatch.setattr(realvol, "hourly", lambda: held["bars"])
    return held


def _got(tmp_path):
    run = varcov.Run(computed_micros=varcov.micros(ASOF + timedelta(minutes=15)), code="abc", signal="realvol")
    return {r["measure"]: r for r in realvol.compute(tmp_path, run).rows}


def a_long_history_forecasts_tomorrow_and_the_week(served, tmp_path):
    got = _got(tmp_path)
    assert all(got[m]["value"] is not None for m in ("sigma_1d", "sigma_7d", "rv_sigma_1d", "vol_term", "qlike_harq", "qlike_naive", "qlike_mean30"))
    assert got["sigma_7d"]["value"] > got["sigma_1d"]["value"] > 0
    assert got["sigma_1d"]["target_micros"] == varcov.micros(ASOF + timedelta(days=1))
    last = served["bars"].filter(pl.col("ts") >= ASOF - timedelta(days=1))
    r = last["close"].log().diff().drop_nulls()
    prior = served["bars"].filter(pl.col("close_ts") == ASOF - timedelta(days=1))["close"][0]
    rv = float((r**2).sum()) + math.log(last["close"][0] / prior) ** 2
    assert got["rv_sigma_1d"]["value"] == pytest.approx(math.sqrt(rv), rel=1e-9)
    # A slow σ measured from 24 returns a day: yesterday's RV alone is the noisier forecast. (With σ moving
    # faster than one day's RV mis-measures it, the naive forecast wins: the stored losses say which holds.)
    assert got["qlike_harq"]["value"] < got["qlike_naive"]["value"] and got["qlike_harq"]["n_eff"] == 60


def a_short_history_is_absent(served, tmp_path):
    served["bars"] = _bars(days=150)
    got = _got(tmp_path)
    assert got["sigma_1d"]["value"] is None and "149 whole days, under 190" in got["sigma_1d"]["absent"]


def a_day_missing_an_hour_is_not_whole(served, tmp_path):
    served["bars"] = _bars(drop=ASOF - 5 * HOUR)
    got = _got(tmp_path)
    assert got["sigma_1d"]["value"] is None and "not whole" in got["sigma_1d"]["absent"]


def the_loss_of_a_perfect_forecast_is_zero():
    y = pl.Series([0.5, 2.0, 1.0])
    assert realvol.qlike(y, y) == pytest.approx(0.0)
    assert realvol.qlike(y * 2, y) > 0 and realvol.qlike(y / 2, y) > 0
