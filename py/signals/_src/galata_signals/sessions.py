"""When an xyz perp's oracle follows its external market, and when its own book.

trade.xyz's GOLD, CL (WTIOIL) and XYZ100 follow **CME Globex hours**
(docs.trade.xyz, Specification Index): external from Sunday 18:00 to Friday
17:00 New York time, internal during the daily maintenance hour 17:00–18:00
Monday to Thursday, and internal on futures holiday closures. In the internal
session the oracle moves by an EMA toward the venue's own impact prices, so a
premium there is pulled to zero by construction.

The rule is written here from those hours. `exchange_calendars`' CMES models
the weekly open but has no daily maintenance gap, so it supplies only the
closures (Good Friday, Christmas, New Year), fetched
from the calendar and never kept in a file here. An early close is not
modelled: trade.xyz does not say whether one makes the rest of the day internal.
"""

from __future__ import annotations

from datetime import UTC, date, datetime, timedelta
from functools import lru_cache
from zoneinfo import ZoneInfo

import galata_research as gr

NEW_YORK = ZoneInfo("America/New_York")
MIN_US = 60_000_000


@lru_cache(maxsize=4)
def closures(year: int) -> frozenset[date]:
    """CME Globex's weekday closures in `year` and the next (a session dated D is closed)."""
    frame = gr.calendar.closures("CMES", f"{year}-01-01T00:00Z", f"{year + 2}-01-01T00:00Z")
    return frozenset(frame["date"].to_list())


def external(at: datetime, closed: frozenset[date] | None = None) -> bool:
    """Whether the Globex session is open at `at` (UTC): Sunday 18:00 to Friday 17:00 New York, less the daily break and closures."""
    ny = at.astimezone(NEW_YORK)
    wd, hour = ny.weekday(), ny.hour + ny.minute / 60
    if wd == 5 or (wd == 6 and hour < 18) or (wd == 4 and hour >= 17):
        return False
    if 17 <= hour < 18:
        return False  # the daily maintenance hour, Monday to Thursday
    # A Globex session is dated by the day it closes: 18:00 onwards belongs to the next day's.
    trade_date = ny.date() + timedelta(days=1) if hour >= 18 else ny.date()
    closed = closures(at.year) if closed is None else closed
    return trade_date not in closed


HOUR_US = 60 * MIN_US


@lru_cache(maxsize=65_536)
def _external_hour(hour: int, closed: frozenset[date] | None) -> bool:
    """`external` for every instant of one UTC hour, which it is constant over.

    New York is a whole number of hours from UTC, in both seasons, and every
    threshold above falls on a New York hour (17:00, 18:00, midnight, and the
    2:00 clock change). So within a UTC hour nothing `external` reads changes.
    """
    return external(datetime.fromtimestamp(hour * HOUR_US / 1e6, tz=UTC), closed)


def external_share(start_micros: int, end_micros: int, closed: frozenset[date] | None = None) -> float:
    """The share of [start, end) in the external session, minute by minute.

    **Counted per UTC hour**, not per minute: the session is constant over an
    hour, so each hour's minutes on the grid from `start` are counted and the
    session asked once — and remembered, since every calculator asks about the
    same hours. It was 43,200 timezone conversions per instrument for a
    month's baseline.
    """
    n = len(range(start_micros, end_micros, MIN_US))
    if n == 0:
        raise ZeroDivisionError("an empty interval has no share")
    count = 0
    for hour in range(start_micros // HOUR_US, (end_micros - 1) // HOUR_US + 1):
        # The grid's minutes k with hour start ≤ start + k·MIN < hour end.
        k_lo = max(0, -(-(hour * HOUR_US - start_micros) // MIN_US))
        k_hi = min(n, -(-((hour + 1) * HOUR_US - start_micros) // MIN_US))
        if k_hi > k_lo and _external_hour(hour, closed):
            count += k_hi - k_lo
    return count / n
