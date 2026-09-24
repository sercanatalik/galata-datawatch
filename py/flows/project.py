"""Project the last closed days of the archive onto the tape. Nightly.

**The tape exists only when this runs.** Capture writes the archive and never
the tape; ``galata-tape-rebuild`` is the tape's one writer. Without a cadence
the product stops at whatever date somebody last rebuilt by hand.

**A fixed trailing window, replaced, remembered nowhere.** Each night rebuilds
the three closed days before today, half-open, with ``--replace`` — so a run
is idempotent, a missed night is re-projected by the next, and nothing about
*which days are done* lives in cereyan's store (invariant 1). This departs
from legacy, where the rebuild was the one flow that retried: it had no
``--replace``, so a range could not be redone and a missed date was a hole.

**Venues come from the configuration**, by table name only, so declaring a
venue projects its tape without a second edit here. Every venue is attempted
before any failure is raised: one venue's broken day must not cost another
venue its tape.
"""

from __future__ import annotations

import tomllib
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

from cereyan import Cron, flow, task

from . import _runner

# 00:40 UTC: thirty minutes after compaction starts. The shared resource is
# what actually orders them; the gap only makes waiting the unusual case.
SCHEDULE = Cron("40 0 * * *", timezone="UTC")

# Two consecutive missed nights still self-heal, and a late arrival for
# yesterday is picked up on each of the next two nights. The cost is measured
# in design/measured.md ("projecting the trailing window"); the number moves
# when that measurement says so.
TRAILING_DAYS = 3


def window(now: datetime) -> tuple[date, date]:
    """The closed days to project, half-open: ``[today - 3, today)``.

    The moment is a parameter so the boundary is visible and testable.
    Today is not closed, so it is the exclusive end.
    """
    today = now.astimezone(timezone.utc).date()
    return today - timedelta(days=TRAILING_DAYS), today


def declared_venues(config: str) -> list[str]:
    """The ``[venue.*]`` table names, in the file's order. Names only."""
    path = Path(config)
    with path.open("rb") as fh:
        venues = list(tomllib.load(fh).get("venue", {}))
    if not venues:
        raise _runner.BadArgument(f"{path.resolve()} declares no [venue.*] table")
    return venues


@task(retries=0)
def project_venue(config: str, venue: str, start: str, end: str) -> str:
    return _runner.run("galata-tape-rebuild", ["--replace", venue, start, end], config)


@flow(
    name="project-the-closed-days",
    schedule=SCHEDULE,
    max_concurrent=1,
    on_overlap="skip",
    resources={"galata-record": 1},
)
def project_the_closed_days(config: str = str(_runner.CONFIG)) -> dict[str, str]:
    start, end = window(datetime.now(timezone.utc))
    said: dict[str, str] = {}
    refused: list[str] = []
    for venue in declared_venues(config):
        try:
            said[venue] = project_venue(config, venue, start.isoformat(), end.isoformat())
        except _runner.Refused as refusal:
            refused.append(f"{venue}: {refusal}")
    if refused:
        raise _runner.Broken("\n".join(refused))
    return said
