"""Cascades inferred on marks made in the test."""

from __future__ import annotations

import random
from datetime import UTC, datetime

import polars as pl
import pytest

from galata_signals import cascade, varcov
from galata_signals.carry import Declared

ASOF = datetime(2026, 9, 28, 7, 0, tzinfo=UTC)
A = varcov.micros(ASOF)
MIN = 60_000_000
DECLARED = Declared.declared({"baseline": {"main": 0.0000125}})


def _marks(shocks=None, minutes=1500, skip=(), seed=5):
    """One sample a minute for BTC up to ASOF: mark ~ random walk, OI ~ 10,000 coins wandering; `shocks` maps minutes-before-ASOF to (return, OI log change)."""
    rng = random.Random(seed)
    shocks = shocks or {}
    mark, oi, rows = 100_000.0, 10_000.0, []
    for k in range(minutes, 0, -1):
        r, d = shocks.get(k, (rng.gauss(0, 0.0005), rng.gauss(0, 0.0005)))
        mark *= pow(2.718281828, r)
        oi *= pow(2.718281828, d)
        if k in skip:
            continue
        rows.append({"ticker": "BTC", "t": A - k * MIN + 30_000_000, "mark": mark, "oracle": mark, "premium": 0.0, "open_interest": oi})
    return pl.DataFrame(rows, schema={"ticker": pl.String, "t": pl.Int64, "mark": pl.Float64, "oracle": pl.Float64, "premium": pl.Float64, "open_interest": pl.Float64})


@pytest.fixture(name="record")
def _record(monkeypatch):
    held = {"marks": _marks()}
    monkeypatch.setattr(cascade, "marks", lambda tape, lo, hi: held["marks"])
    return held


def _got(tmp_path):
    run = varcov.Run(computed_micros=A + 15 * MIN, code="abc", signal="cascade")
    return {r["measure"]: r for r in cascade.compute(DECLARED, tmp_path, run).rows}


def a_quiet_hour_has_no_cascade(record, tmp_path):
    got = _got(tmp_path)
    assert got["cascade_events"]["value"] == 0 and got["liq_long_usd"]["value"] == 0 and got["liq_short_usd"]["value"] == 0
    assert 0 < got["max_joint_z"]["value"] < 4


def a_crash_with_open_interest_falling_is_a_long_liquidation(record, tmp_path):
    # Three minutes of −1% with OI −2% each, one minute apart: one event.
    record["marks"] = _marks({30: (-0.01, -0.02), 29: (-0.01, -0.02), 27: (-0.01, -0.02)})
    got = _got(tmp_path)
    assert got["cascade_events"]["value"] == 1
    assert got["liq_long_usd"]["value"] > 0 and got["liq_short_usd"]["value"] == 0
    assert got["largest_event_usd"]["value"] == pytest.approx(got["liq_long_usd"]["value"])
    assert got["liq_intensity"]["value"] == pytest.approx(0.06, abs=0.01)  # about 6% of open interest closed


def a_squeeze_is_short_and_a_separate_event(record, tmp_path):
    record["marks"] = _marks({40: (-0.01, -0.02), 20: (0.01, -0.02)})
    got = _got(tmp_path)
    assert got["cascade_events"]["value"] == 2
    assert got["liq_long_usd"]["value"] > 0 and got["liq_short_usd"]["value"] > 0


def a_price_move_without_open_interest_falling_is_no_cascade(record, tmp_path):
    record["marks"] = _marks({30: (-0.02, 0.0)})
    assert _got(tmp_path)["cascade_events"]["value"] == 0


def an_outage_is_not_a_crash(record, tmp_path):
    # The OI drop falls inside a hole: the minute after it has no minute before it to differ from.
    record["marks"] = _marks({30: (-0.01, -0.05)}, skip=(31,))
    assert _got(tmp_path)["cascade_events"]["value"] == 0


def no_cascade_without_a_trailing_day(record, tmp_path):
    record["marks"] = _marks(minutes=300)
    got = _got(tmp_path)
    assert got["cascade_events"]["value"] is None and "under 720" in got["cascade_events"]["absent"]
