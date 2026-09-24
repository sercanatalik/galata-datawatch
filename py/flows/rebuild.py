"""Rebuild one day of one venue's tape. For history; never scheduled.

The nightly projection keeps recent days; this is how an older range is
re-derived — a backfill over ``date``, one run per day, so a failed day
retries alone rather than dragging the range through again.

**The one flow that retries, and only when broken.** A day far outside the
nightly window is not repaired by any later schedule, so a transient failure
here would be a hole nothing fills. ``retry_on=(Broken,)``: a bad argument
fails once and stays failed, which is the retry debt legacy recorded as
owed — *"a scheduler retrying on exit code alone would retry a bad argument
forever."* The delays are legacy's, carried unchanged.
"""

from __future__ import annotations

from datetime import date

from cereyan import flow, task

from . import _runner

# Seconds before each retry: legacy's figures (`py/flows/rebuild.py`), long
# enough that a second attempt is not the same transient as the first.
RETRY_DELAYS = [30, 120, 600]


@task(retries=len(RETRY_DELAYS), retry_delay=RETRY_DELAYS, retry_on=(_runner.Broken,))
def rebuild_day(config: str, venue: str, day: str) -> str:
    return _runner.run("galata-tape-rebuild", ["--replace", venue, day], config)


@flow(name="rebuild-one-day", resources={"galata-record": 1})
def rebuild_one_day(venue: str, day: date, config: str = str(_runner.CONFIG)) -> str:
    return rebuild_day(config, venue, day.isoformat())
