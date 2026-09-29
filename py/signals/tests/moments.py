"""Realized moments on 5-minute bars made in the test."""

from __future__ import annotations

import math
import random
from datetime import UTC, datetime, timedelta

import polars as pl
import pytest

from galata_signals import moments, varcov
from galata_signals.carry import Declared

ASOF = datetime(2026, 9, 28, tzinfo=UTC)
FIVE = timedelta(minutes=5)


def _bars(ticker, days=8, every_day=288, seed=1, shock=None):
    """`days` of 5m bars to ASOF, the first `every_day` of each day's slots; `shock` is (day, slot, log return)."""
    rng = random.Random(seed)
    rows, price = [], 100.0
    for d in range(days):
        day = ASOF - timedelta(days=days - d)
        for i in range(every_day):
            r = rng.gauss(0, 0.001)
            if shock and (d, i) == shock[:2]:
                r = shock[2]
            price *= math.exp(r)
            ts = day + i * FIVE
            rows.append({"ticker": ticker, "ts": ts, "close_ts": ts + FIVE, "open": price, "high": price, "low": price, "close": price})
    return pl.DataFrame(rows, schema={"ticker": pl.String, "ts": pl.Datetime("us", "UTC"), "close_ts": pl.Datetime("us", "UTC"), "open": pl.Float64, "high": pl.Float64, "low": pl.Float64, "close": pl.Float64})


@pytest.fixture(name="served")
def _served(monkeypatch):
    held = {"bars": _bars("BTC")}
    monkeypatch.setattr(moments, "fivemin", lambda: held["bars"])
    return held


def _got(tmp_path, ticker="BTC"):
    run = varcov.Run(computed_micros=varcov.micros(ASOF + timedelta(minutes=15)), code="abc", signal="moments")
    return {r["measure"]: r for r in moments.compute(tmp_path, run).rows if r["ticker_i"] == ticker}


def a_gaussian_day_has_little_skew_and_a_kurtosis_near_three(served, tmp_path):
    got = _got(tmp_path)
    assert abs(got["realized_skew_1d"]["value"]) < 0.5
    assert got["realized_kurt_1d"]["value"] == pytest.approx(3.0, abs=0.8)
    assert got["realized_vol_1d"]["value"] == pytest.approx(0.001 * math.sqrt(288), rel=0.15)
    assert got["realized_skew_1d"]["n_eff"] == 288  # the day's first return is from the day before's last bar, which was read
    assert got["realized_skew_7d"]["n_eff"] == 7


def a_crash_bar_makes_the_day_skew_left(served, tmp_path):
    served["bars"] = _bars("BTC", shock=(7, 100, -0.03))
    got = _got(tmp_path)
    assert got["realized_skew_1d"]["value"] < -5 and got["realized_kurt_1d"]["value"] > 50


def a_thin_day_has_no_moments(served, tmp_path):
    served["bars"] = pl.concat([_bars("BTC"), _bars("GOLD", every_day=40, seed=2)])
    gold = _got(tmp_path, "GOLD")
    assert gold["realized_skew_1d"]["value"] is None and "under 50" in gold["realized_skew_1d"]["absent"]
    assert gold["realized_skew_7d"]["value"] is None and "0 days with moments" in gold["realized_skew_7d"]["absent"]


def only_a_new_day_is_written(served, tmp_path):
    rows = moments.compute(tmp_path, varcov.Run(computed_micros=varcov.micros(ASOF + timedelta(hours=5)), code="abc", signal="moments")).rows
    assert {r["asof_micros"] for r in rows} == {varcov.micros(ASOF)}


def an_xyz_perp_keeps_to_its_session(served, tmp_path):
    # 27 September is a Sunday: the session opens at 22:00 UTC, and the jump into it is the first bar after.
    served["bars"] = pl.concat([_bars("BTC", shock=(7, 264, 0.05)), _bars("GOLD", shock=(7, 264, 0.05))])
    declared = Declared.declared({"baseline": {"main": 0.0000125, "xyz": 0.00000625}, "dex": {"xyz": ["GOLD"]}})
    run = varcov.Run(computed_micros=varcov.micros(ASOF + timedelta(minutes=15)), code="abc", signal="moments")
    rows = moments.compute(tmp_path, run, declared).rows
    btc = {r["measure"]: r for r in rows if r["ticker_i"] == "BTC"}
    gold = {r["measure"]: r for r in rows if r["ticker_i"] == "GOLD"}
    assert btc["realized_kurt_1d"]["value"] > 100  # the jump dominates a day with no session
    # 22:00 to 24:00 holds 24 bars, so 23 returns between them; the jump into 22:05 is not one.
    assert gold["realized_kurt_1d"]["value"] is None and "23 whole 5-minute returns in the day in the external session" in gold["realized_kurt_1d"]["absent"]
    assert '"session": "external"' in gold["realized_kurt_1d"]["params"] and "session" not in btc["realized_kurt_1d"]["params"]


def a_session_return_spans_two_bars_in_session():
    monday_3am_ny = varcov.micros(datetime(2026, 9, 28, 7, 0, tzinfo=UTC))
    sunday_open = varcov.micros(datetime(2026, 9, 27, 22, 0, tzinfo=UTC))
    assert moments.in_session([monday_3am_ny, sunday_open + 5 * 60_000_000, sunday_open + 10 * 60_000_000]) == [True, False, True]
