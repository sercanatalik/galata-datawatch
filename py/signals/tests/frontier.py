"""The tape's frontier, and the whole period a calculator may take."""

from __future__ import annotations

from datetime import UTC, datetime

import pyarrow as pa
import pyarrow.parquet as pq

from galata_signals import basis, varcov
from galata_signals.carry import Declared
from galata_signals.frontier import frontier, whole

HOUR = 3_600_000_000
T = varcov.micros(datetime(2026, 9, 28, 23, 0, tzinfo=UTC))


def _segment(tape, kind, date, name, recv):
    d = tape / f"kind={kind}" / f"date={date}"
    d.mkdir(parents=True, exist_ok=True)
    pq.write_table(pa.table({"recv_micros": pa.array(recv, pa.int64())}), d / name)


def a_frontier_is_the_newest_receipt_of_the_slowest_dataset(tmp_path):
    _segment(tmp_path, "quotes", "2026-09-28", "s-1_2.parquet", [T - HOUR, T - 20 * 60_000_000])
    _segment(tmp_path, "trades", "2026-09-28", "s-1_2.parquet", [T - 25 * 60_000_000])
    assert frontier(tmp_path, ("quotes",)) == T - 20 * 60_000_000
    assert frontier(tmp_path, ("quotes", "trades")) == T - 25 * 60_000_000


def no_frontier_without_the_dataset(tmp_path):
    assert frontier(tmp_path, ("marks",)) is None


def the_whole_period_waits_for_the_tape():
    now = T + 15 * 60_000_000  # 23:15
    assert whole(now, HOUR, T - 20 * 60_000_000) == T - HOUR  # the tape ends 22:40: the hour to 22:00
    assert whole(now, HOUR, T + 40 * 60_000_000) == T  # after the :40 projection: the hour to 23:00
    assert whole(now, HOUR, None) == T


def a_basis_run_before_the_projection_takes_the_hour_before(tmp_path, monkeypatch):
    _segment(tmp_path, "marks", "2026-09-28", "s-1_2.parquet", [T - 20 * 60_000_000])
    seen = []
    monkeypatch.setattr(basis, "marks", lambda tape, lo, hi: seen.append(hi) or basis.pl.DataFrame(schema={"ticker": basis.pl.String, "t": basis.pl.Int64, "mark": basis.pl.Float64, "oracle": basis.pl.Float64, "premium": basis.pl.Float64, "open_interest": basis.pl.Float64}))
    monkeypatch.setattr(basis, "history", lambda tape, lo, hi: basis.pl.DataFrame(schema={"ticker_i": basis.pl.String, "asof_micros": basis.pl.Int64, "value": basis.pl.Float64}))
    decl = Declared.declared({"baseline": {"main": 0.0000125}})
    basis.compute(decl, tmp_path, varcov.Run(computed_micros=T + 15 * 60_000_000, code="abc", signal="basis"))
    assert seen == [T - HOUR]  # it read the hour to 22:00, not the one to 23:00 the tape does not hold
