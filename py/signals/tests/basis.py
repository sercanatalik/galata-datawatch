"""The basis on marks made in the test."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal

import polars as pl
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from galata_signals import basis, varcov
from galata_signals.carry import Declared

ASOF = datetime(2026, 9, 28, 7, 0, tzinfo=UTC)
SEC = 1_000_000
HOUR = 3600 * SEC
A = varcov.micros(ASOF)
DECLARED = Declared.declared({"baseline": {"main": 0.0000125, "xyz": 0.00000625}, "dex": {"xyz": ["GOLD"]}})


def _m(ticker, samples):
    """(seconds after the hour opened, premium, mark, oracle, open interest) per sample."""
    return pl.DataFrame(
        [{"ticker": ticker, "t": A - HOUR + s * SEC, "mark": mk, "oracle": o, "premium": p, "open_interest": oi} for s, p, mk, o, oi in samples],
        schema={"ticker": pl.String, "t": pl.Int64, "mark": pl.Float64, "oracle": pl.Float64, "premium": pl.Float64, "open_interest": pl.Float64},
    )


def _every_second(ticker="BTC", premium=lambda s: 0.0001, mark=100.0, oracle=100.0, oi=lambda s: 1000.0):
    return _m(ticker, [(s, premium(s), mark, oracle, oi(s)) for s in range(3600)])


def _history(rows):
    """(ticker, hours before the asof, premium) per stored hour."""
    return pl.DataFrame(
        [{"ticker_i": t, "asof_micros": A - h * HOUR, "value": v} for t, h, v in rows],
        schema={"ticker_i": pl.String, "asof_micros": pl.Int64, "value": pl.Float64},
    )


@pytest.fixture(name="record")
def _record(monkeypatch):
    held = {"marks": _every_second(), "history": _history([])}
    monkeypatch.setattr(basis, "marks", lambda tape, lo, hi: held["marks"])
    monkeypatch.setattr(basis, "history", lambda tape, lo, hi: held["history"])
    return held


def _got(tmp_path, ticker="BTC"):
    run = varcov.Run(computed_micros=A + 15 * 60 * SEC, code="abc", signal="basis")
    return {r["measure"]: r for r in basis.compute(DECLARED, tmp_path, run).rows if r["ticker_i"] == ticker}


def a_premium_that_stood_longer_weighs_more(record, tmp_path):
    # 1 bp for 59 minutes, 20 bp for the last one: the mean moves, the median does not.
    record["marks"] = _every_second(premium=lambda s: 0.0001 if s < 3540 else 0.0020)
    got = _got(tmp_path)
    assert got["premium_twa_bps"]["value"] == pytest.approx((59 * 1 + 20) / 60, rel=1e-6)
    assert got["premium_median_bps"]["value"] == pytest.approx(1.0)


def a_mark_against_its_oracle_is_in_bps(record, tmp_path):
    record["marks"] = _every_second(mark=100.05, oracle=100.0)
    assert _got(tmp_path)["mark_oracle_bps"]["value"] == pytest.approx(5.0)


def the_open_interest_is_its_change_and_its_dollars(record, tmp_path):
    record["marks"] = _every_second(oi=lambda s: 1000.0 if s < 1800 else 1100.0, mark=50.0, oracle=50.0)
    got = _got(tmp_path)
    assert got["open_interest_log_change"]["value"] == pytest.approx(0.0953101798)
    assert got["open_interest_usd"]["value"] == pytest.approx(55_000.0)


def an_outage_is_not_a_price_that_stood(record, tmp_path):
    # One sample every ten minutes: each stands 30 s at most, 3 minutes in all.
    record["marks"] = _m("BTC", [(s, 0.0001, 100.0, 100.0, 1000.0) for s in range(0, 3600, 600)])
    got = _got(tmp_path)
    assert got["premium_twa_bps"]["value"] is None
    assert "cover 3 of 60 minutes" in got["premium_twa_bps"]["absent"]


def the_z_needs_its_own_history(record, tmp_path):
    record["marks"] = _every_second("BTC", premium=lambda s: 0.0005)
    got = _got(tmp_path)
    assert "0 stored hours" in got["premium_z_30d"]["absent"]
    assert "external_share" not in got  # the main dex has no session
    record["history"] = _history([("BTC", i + 1, 1.0 + (i % 5) * 0.5) for i in range(100)])
    z = _got(tmp_path)["premium_z_30d"]
    assert z["value"] == pytest.approx((5.0 - 2.0) / (1.4826 * 0.5)) and z["n_eff"] == 100


def an_xyz_hour_is_compared_only_with_wholly_external_hours(record, tmp_path):
    # 07:00 UTC on Monday 28 September is 03:00 in New York: inside the Globex session.
    record["marks"] = _every_second("GOLD", premium=lambda s: 0.0005)
    # Of the 59 hours before it, 8 are wholly in Sunday's and Monday's session and 2 in Friday's before
    # its 17:00 close; the weekend between is closed, and 50 bp there would move the median if counted.
    hours = [(h, 1.0 + (h % 5) * 0.5) for h in range(1, 12)] + [(h, 50.0) for h in range(12, 60)]
    record["history"] = _history([("GOLD", h, v) for h, v in hours])
    got = _got(tmp_path, "GOLD")
    assert got["external_share"]["value"] == 1.0
    z = got["premium_z_30d"]
    assert "10 stored hours" in z["absent"]
    assert '"dex": "xyz"' in got["premium_twa_bps"]["params"]


def an_xyz_hour_in_the_daily_break_has_no_z(record, tmp_path, monkeypatch):
    # The hour to 22:00 UTC on a Monday is 17:00–18:00 in New York: the maintenance hour.
    monkeypatch.setattr(basis, "period", lambda run, width, end: A + 15 * HOUR)
    record["marks"] = _m("GOLD", [(s + 15 * 3600, 0.0001, 100.0, 100.0, 1000.0) for s in range(3600)])
    got = {r["measure"]: r for r in basis.compute(DECLARED, tmp_path, varcov.Run(computed_micros=A + 16 * HOUR, code="abc", signal="basis")).rows}
    assert got["external_share"]["value"] == 0.0
    assert got["premium_z_30d"]["value"] is None and "0% of the hour in the external session" in got["premium_z_30d"]["absent"]


def the_stored_history_keeps_each_hours_latest(tmp_path):
    def row(asof, value, computed):
        return {"signal": "basis", "measure": "premium_twa_bps", "ticker_i": "GOLD", "asof_micros": asof, "value": value, "computed_micros": computed}

    (tmp_path / "kind=signals" / "date=2026-09-28").mkdir(parents=True)
    pl.DataFrame([row(A - HOUR, 1.0, 1), row(A - HOUR, 2.0, 2), row(A - 2 * HOUR, 3.0, 1)]).write_parquet(tmp_path / "kind=signals" / "date=2026-09-28" / "s.parquet")
    got = basis.history(tmp_path, A - 3 * HOUR, A).sort("asof_micros")
    assert got.rows() == [("GOLD", A - 2 * HOUR, 3.0), ("GOLD", A - HOUR, 2.0)]  # a re-stored hour: its latest
