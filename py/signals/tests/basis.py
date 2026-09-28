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


@pytest.fixture(name="record")
def _record(monkeypatch):
    held = {"marks": _every_second(), "history": pl.DataFrame(schema={"ticker_i": pl.String, "asof_micros": pl.Int64, "value": pl.Float64})}
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


def the_z_needs_its_own_history_and_the_main_dex(record, tmp_path):
    record["marks"] = pl.concat([_every_second("BTC", premium=lambda s: 0.0005), _every_second("GOLD")])
    got = _got(tmp_path)
    assert "0 stored hours" in got["premium_z_30d"]["absent"]
    record["history"] = pl.DataFrame({"ticker_i": ["BTC"] * 100, "asof_micros": [A - (i + 1) * HOUR for i in range(100)], "value": [1.0 + (i % 5) * 0.5 for i in range(100)]})
    z = _got(tmp_path)["premium_z_30d"]
    assert z["value"] == pytest.approx((5.0 - 2.0) / (1.4826 * 0.5)) and z["n_eff"] == 100
    gold = _got(tmp_path, "GOLD")["premium_z_30d"]
    assert gold["value"] is None and "xyz dex's oracle follows its own book" in gold["absent"]
    assert '"dex": "xyz"' in _got(tmp_path, "GOLD")["premium_twa_bps"]["params"]


def only_a_new_hour_is_written(record, tmp_path):
    run = varcov.Run(computed_micros=A + 15 * 60 * SEC, code="abc", signal="basis")
    rows = basis.compute(DECLARED, tmp_path, run).rows
    assert {r["asof_micros"] for r in rows} == {A}


def the_tapes_decimals_are_read_as_floats(tmp_path):
    d = pa.decimal128(38, 18)
    table = pa.table(
        {
            "venue": ["hyperliquid"] * 2, "ticker": ["BTC"] * 2, "at_micros": pa.array([None, None], pa.int64()),
            "recv_micros": [A - HOUR + SEC, A + SEC], "stream_seq": pa.array([1, 2], pa.uint64()),
            "mark": pa.array([Decimal("100.5")] * 2, d), "index": pa.array([None, None], d), "oracle": pa.array([Decimal("100")] * 2, d),
            "open_interest": pa.array([Decimal("10")] * 2, d), "mid": pa.array([Decimal("100.4")] * 2, d), "premium": pa.array([Decimal("0.0003")] * 2, d),
        }
    )  # fmt: skip
    (tmp_path / "kind=marks" / "date=2026-09-28").mkdir(parents=True)
    pq.write_table(table, tmp_path / "kind=marks" / "date=2026-09-28" / "s-1_2.parquet")
    got = basis.marks(tmp_path, A - HOUR, A)
    assert got.height == 1  # the one inside [lo, hi)
    assert got.row(0, named=True) == {"ticker": "BTC", "t": A - HOUR + SEC, "mark": 100.5, "oracle": 100.0, "premium": 0.0003, "open_interest": 10.0}
