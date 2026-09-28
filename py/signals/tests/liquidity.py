"""Liquidity on quotes and trades made in the test."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import polars as pl
import pytest

from galata_signals import liquidity, varcov

ASOF = datetime(2026, 9, 28, 7, 0, tzinfo=UTC)
MIN = timedelta(minutes=1)


def _q(rows):
    """(ts, bid, ask) with 1 unit a side, for BTC."""
    return pl.DataFrame(
        [{"venue": "hyperliquid", "ticker": "BTC", "ts": ts, "bid_px": b, "ask_px": a, "bid_sz": 1.0, "ask_sz": 1.0} for ts, b, a in rows],
        schema={"venue": pl.String, "ticker": pl.String, "ts": pl.Datetime("us", "UTC"), "bid_px": pl.Float64, "ask_px": pl.Float64, "bid_sz": pl.Float64, "ask_sz": pl.Float64},
    )


def _t(rows=()):
    return pl.DataFrame(
        [{"venue": "hyperliquid", "ticker": "BTC", "ts": ts, "price": p, "size": s, "aggressor": ag} for ts, p, s, ag in rows],
        schema={"venue": pl.String, "ticker": pl.String, "ts": pl.Datetime("us", "UTC"), "price": pl.Float64, "size": pl.Float64, "aggressor": pl.String},
    )


@pytest.fixture(name="record")
def _record(monkeypatch):
    held = {"trades": _t(), "history": pl.DataFrame(schema={"ticker_i": pl.String, "asof_micros": pl.Int64, "value": pl.Float64})}
    monkeypatch.setattr(liquidity, "quotes", lambda lo, hi: held["quotes"])
    monkeypatch.setattr(liquidity, "trades", lambda lo, hi: held["trades"])
    monkeypatch.setattr(liquidity, "history", lambda tape, lo, hi: held["history"])
    return held


def _got(tmp_path):
    run = varcov.Run(computed_micros=varcov.micros(ASOF + 15 * MIN), code="abc", signal="liquidity")
    return {r["measure"]: r for r in liquidity.compute(tmp_path, run).rows}


def a_quote_that_stood_longer_weighs_more(record, tmp_path):
    start = ASOF - 60 * MIN
    # 2 bps for 59 minutes, 20 bps for the last one.
    record["quotes"] = _q([(start, 99.99, 100.01), (ASOF - MIN, 99.9, 100.1)])
    got = _got(tmp_path)
    assert got["quoted_spread_bps"]["value"] == pytest.approx((59 * 2 + 20) / 60, rel=1e-6)


def a_crossed_book_is_not_a_spread(record, tmp_path):
    start = ASOF - 60 * MIN
    record["quotes"] = _q([(start, 99.99, 100.01), (start + 30 * MIN, 100.02, 100.01), (start + 31 * MIN, 99.99, 100.01)])
    got = _got(tmp_path)
    assert got["quoted_spread_bps"]["value"] == pytest.approx(2.0, rel=1e-6)  # the crossed minute is simply gone
    assert got["depth_usd_median"]["value"] == pytest.approx(99.99)


def a_thin_hour_is_absent(record, tmp_path):
    start = ASOF - 60 * MIN
    # Valid for 40 minutes, crossed for the last 20.
    record["quotes"] = _q([(start, 99.99, 100.01), (start + 40 * MIN, 100.02, 100.01)])
    got = _got(tmp_path)
    assert got["quoted_spread_bps"]["value"] is None and "cover 40 of 60 minutes, under 48" in got["quoted_spread_bps"]["absent"]


def the_spread_is_judged_against_its_own_history(record, tmp_path):
    start = ASOF - 60 * MIN
    record["quotes"] = _q([(start, 99.97, 100.03)])  # 6 bps
    hours = [2.0 * (1 + 0.05 * ((k % 5) - 2)) for k in range(72)]
    record["history"] = pl.DataFrame({"ticker_i": ["BTC"] * 72, "asof_micros": list(range(72)), "value": hours})
    assert _got(tmp_path)["spread_z_7d"]["value"] > 5
    record["history"] = record["history"].head(50)
    assert "50 stored hours" in _got(tmp_path)["spread_z_7d"]["absent"]


def an_impact_never_reads_past_the_asof(record, tmp_path):
    start = ASOF - 60 * MIN
    # The tape's top of book pushes every ~57 ms; the library's 2 s tolerance needs a quote near each mid it reads.
    trade = start + 10 * MIN
    record["quotes"] = _q(
        [(start, 99.99, 100.01), (trade - timedelta(seconds=1), 99.99, 100.01), (trade + timedelta(seconds=4), 100.0, 100.02), (ASOF - timedelta(seconds=3), 100.09, 100.11)]
    )
    # One trade early in the hour, one 2 s before the asof: its 5 s mid would be after it.
    record["trades"] = _t([(trade, 100.01, 1.0, "bid"), (ASOF - timedelta(seconds=2), 100.11, 1.0, "bid")])
    got = _got(tmp_path)
    assert got["impact_bps_5s_vw"]["n_eff"] == 1
    assert got["effective_spread_bps_vw"]["n_eff"] == 2
