"""BTC against the rest, on mid prices made in the test."""

from __future__ import annotations

import json
import random
from datetime import UTC, datetime, timedelta

import polars as pl
import pytest

from galata_signals import leadlag, varcov

ASOF = datetime(2026, 9, 28, 7, 0, tzinfo=UTC)
START = ASOF - timedelta(hours=1)
QSCHEMA = {"venue": pl.String, "ticker": pl.String, "ts": pl.Datetime("us", "UTC"), "bid_px": pl.Float64, "ask_px": pl.Float64, "bid_sz": pl.Float64, "ask_sz": pl.Float64}


def _book(lag_s: float, follower="ETH", every_ms=(200, 450), seed=2):
    """BTC's mid a random walk; the follower's shows BTC's log price `lag_s` later, each quoted at its own random times."""
    rng = random.Random(seed)
    steps = [0.0]
    for _ in range(3600 * 20):  # a 50 ms path
        steps.append(steps[-1] + rng.gauss(0, 2e-5))
    rows = []
    for ticker, gap, lag, scale in (("BTC", every_ms[0], 0.0, 100_000.0), (follower, every_ms[1], lag_s, 3_000.0)):
        t = 0.0
        while t < 3600:
            k = max(0, int((t - lag) / 0.05))
            mid = scale * pow(2.718281828, steps[min(k, len(steps) - 1)])
            rows.append({"venue": "hyperliquid", "ticker": ticker, "ts": START + timedelta(seconds=t), "bid_px": mid * 0.9999, "ask_px": mid * 1.0001, "bid_sz": 1.0, "ask_sz": 1.0})
            t += rng.expovariate(1000 / gap)
    return pl.DataFrame(rows, schema=QSCHEMA)


@pytest.fixture(name="record")
def _record(monkeypatch):
    held = {"quotes": _book(2.0)}
    monkeypatch.setattr(leadlag, "quotes", lambda lo, hi: held["quotes"])
    return held


def _got(tmp_path):
    run = varcov.Run(computed_micros=varcov.micros(ASOF + timedelta(minutes=45)), code="abc", signal="leadlag")
    return {r["measure"]: r for r in leadlag.compute(tmp_path, run).rows}


def a_follower_two_seconds_behind_is_found(record, tmp_path):
    got = _got(tmp_path)
    assert got["lead_ms"]["value"] == 2000 and got["llr"]["value"] > 1.5
    assert got["lead_ms"]["ticker_i"] == "BTC" and got["lead_ms"]["ticker_j"] == "ETH"
    params = json.loads(got["lead_ms"]["params"])
    assert params["within_one_block"] is False and params["edge"] is False


def a_lead_within_a_block_is_flagged(record, tmp_path):
    record["quotes"] = _book(0.0)
    params = json.loads(_got(tmp_path)["lead_ms"]["params"])
    assert params["within_one_block"] is True


def a_thin_hour_is_absent(record, tmp_path):
    record["quotes"] = _book(2.0, every_ms=(200, 20_000))
    got = _got(tmp_path)
    assert got["lead_ms"]["value"] is None and "under 300" in got["lead_ms"]["absent"]
