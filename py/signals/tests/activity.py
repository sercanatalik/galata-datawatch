"""Abnormal activity on trades and history made in the test."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import polars as pl
import pytest

from galata_signals import activity, varcov

ASOF = datetime(2026, 9, 29, 15, 0, tzinfo=UTC)  # a Tuesday: the hour 14:00–15:00
A = varcov.micros(ASOF)
HOUR = 3_600_000_000
TSCHEMA = {"venue": pl.String, "ticker": pl.String, "ts": pl.Datetime("us", "UTC"), "price": pl.Float64, "size": pl.Float64, "aggressor": pl.String}


def _trades(hour_sizes, before_sizes=(1.0,) * 200):
    rows = [{"venue": "hyperliquid", "ticker": "BTC", "ts": ASOF - timedelta(hours=1) + timedelta(seconds=i * 10), "price": 100.0, "size": s, "aggressor": "bid"} for i, s in enumerate(hour_sizes)]
    rows += [{"venue": "hyperliquid", "ticker": "BTC", "ts": ASOF - timedelta(hours=20) + timedelta(seconds=i * 60), "price": 100.0, "size": s, "aggressor": "ask"} for i, s in enumerate(before_sizes)]
    return pl.DataFrame(rows, schema=TSCHEMA)


def _history(days, notional=10_000.0, count=100.0, weekend_too=False):
    """Stored raw figures for the same hour on the `days` before (Tuesday back), plus the hour before each."""
    rows = []
    for d in range(1, days + 1):
        asof = A - d * 24 * HOUR
        for m, v in (("notional_usd", notional * (1 + 0.05 * (d % 3))), ("trade_count", count * (1 + 0.04 * (d % 4))), ("avg_trade_usd", notional / count * (1 + 0.03 * (d % 5)))):
            rows.append({"ticker_i": "BTC", "asof_micros": asof, "measure": m, "value": v})
    return pl.DataFrame(rows, schema={"ticker_i": pl.String, "asof_micros": pl.Int64, "measure": pl.String, "value": pl.Float64})


@pytest.fixture(name="record")
def _record(monkeypatch):
    held = {"trades": _trades([1.0] * 100), "history": _history(0)}
    monkeypatch.setattr(activity, "trades", lambda lo, hi: held["trades"])
    monkeypatch.setattr(activity, "history", lambda tape, lo, hi: held["history"])
    return held


def _got(tmp_path):
    run = varcov.Run(computed_micros=A + 45 * 60_000_000, code="abc", signal="activity")
    return {r["measure"]: r for r in activity.compute(tmp_path, run).rows}


def the_hour_is_its_notional_count_and_size(record, tmp_path):
    got = _got(tmp_path)
    assert got["notional_usd"]["value"] == 10_000.0 and got["trade_count"]["value"] == 100 and got["avg_trade_usd"]["value"] == 100.0
    assert "0 stored weekday hours at 14:00" in got["volume_z"]["absent"]


def a_surge_against_its_own_hours_is_a_large_z(record, tmp_path):
    record["history"] = _history(20)  # 20 days back; 14 weekdays land on a weekday
    record["trades"] = _trades([5.0] * 300)  # 15x the notional, 3x the trades
    got = _got(tmp_path)
    assert got["volume_z"]["value"] > 10 and got["count_z"]["value"] > 1 and got["volume_pct"]["value"] == 1.0
    assert got["volume_z"]["n_eff"] == 14  # weekdays only: the six weekend days are another bucket


def the_large_trade_share_uses_the_day_before(record, tmp_path):
    # The day before: 200 trades of $100; this hour: 90 of $100 and 10 of $1,000.
    record["trades"] = _trades([1.0] * 90 + [10.0] * 10)
    got = _got(tmp_path)
    assert got["large_share"]["value"] == pytest.approx(10_000 / 19_000)


def a_saturday_is_judged_against_saturdays_and_sundays():
    assert activity._bucket(varcov.micros(datetime(2026, 10, 3, 15, 0, tzinfo=UTC))) == (14, True)
    assert activity._bucket(varcov.micros(datetime(2026, 10, 5, 0, 0, tzinfo=UTC))) == (23, True)  # Sunday 23:00–00:00
