"""trade.xyz's external session: CME Globex hours in New York time."""

from __future__ import annotations

from datetime import UTC, date, datetime

import pytest

from galata_signals import sessions

NONE: frozenset[date] = frozenset()


def _utc(*args):
    return datetime(*args, tzinfo=UTC)


@pytest.mark.parametrize(
    ("at", "open_"),
    [
        (_utc(2026, 9, 28, 7, 0), True),  # Monday 03:00 New York
        (_utc(2026, 9, 28, 21, 0), False),  # Monday 17:00: the daily break
        (_utc(2026, 9, 28, 21, 59), False),  # Monday 17:59
        (_utc(2026, 9, 28, 22, 0), True),  # Monday 18:00: the next session opens
        (_utc(2026, 10, 2, 20, 59), True),  # Friday 16:59
        (_utc(2026, 10, 2, 21, 0), False),  # Friday 17:00: the week closes
        (_utc(2026, 10, 3, 15, 0), False),  # Saturday
        (_utc(2026, 10, 4, 21, 59), False),  # Sunday 17:59
        (_utc(2026, 10, 4, 22, 0), True),  # Sunday 18:00: the week opens
        (_utc(2026, 11, 2, 22, 0), False),  # Monday 17:00 after daylight saving ends (EST)
        (_utc(2026, 11, 2, 23, 0), True),  # Monday 18:00 EST
    ],
)
def every_minute_is_in_or_out_of_the_session(at, open_):
    assert sessions.external(at, NONE) is open_


def a_futures_holiday_is_internal_from_the_evening_before():
    good_friday = frozenset({date(2027, 3, 26)})
    assert not sessions.external(_utc(2027, 3, 25, 21, 0), good_friday)  # Thursday 17:00 EDT: the break
    assert sessions.external(_utc(2027, 3, 25, 20, 0), good_friday)  # Thursday 16:00: Thursday's session
    assert not sessions.external(_utc(2027, 3, 25, 22, 30), good_friday)  # Thursday 18:30: Friday's session, closed
    assert not sessions.external(_utc(2027, 3, 26, 14, 0), good_friday)


def the_calendar_supplies_the_closures():
    assert {date(2026, 12, 25), date(2027, 3, 26), date(2027, 1, 1)} <= sessions.closures(2026)


def an_hours_share_counts_its_minutes():
    start = int(_utc(2026, 9, 28, 21, 30).timestamp() * 1e6)
    assert sessions.external_share(start, start + 3600 * 1_000_000, NONE) == pytest.approx(0.5)
