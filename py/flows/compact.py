"""Fold every closed archive partition. Nightly.

**No retries, and that is the rule rather than an omission.**
``galata-compact`` folds *every* closed partition, not yesterday's, so a run
that fails tonight is repaired by tomorrow's compacting two. For work that
catches up by construction, the next scheduled run is the retry.

Two runs at once are refused by the binary's own exclusive hold; the
``galata-record`` resource keeps the lane from asking for that in the first
place, and keeps the projection from reading what this is rewriting.
"""

from __future__ import annotations

from cereyan import Cron, flow, task

from . import _runner

# 00:10 UTC: the archive's day closes at midnight and this clears the
# writer's last flush. The time legacy's launchd agent used from 2026-09-06.
SCHEDULE = Cron("10 0 * * *", timezone="UTC")


@task(retries=0)
def compact(config: str) -> str:
    return _runner.run("galata-compact", [], config)


@flow(
    name="compact-the-archive",
    schedule=SCHEDULE,
    max_concurrent=1,
    on_overlap="skip",
    resources={"galata-record": 1},
)
def compact_the_archive(config: str = str(_runner.CONFIG)) -> str:
    return compact(config)
