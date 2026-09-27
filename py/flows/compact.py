"""Fold every closed archive partition, and today's closed hours. Hourly.

**No retries, and that is the rule rather than an omission.**
``galata-compact`` folds *every* closed partition, not yesterday's, so a run
that fails is repaired by the next hour's. For work that
catches up by construction, the next scheduled run is the retry.

Two runs at once are refused by the binary's own exclusive hold; the
``galata-record`` resource keeps the lane from asking for that in the first
place, and keeps the projection from reading what this is rewriting.
"""

from __future__ import annotations

from cereyan import Cron, flow, task

from . import _runner

# :20 UTC hourly, with ``--closed-hours``: each run folds every closed day
# (the 00:20 run folds yesterday) and today's hours that ended at least five
# minutes ago. Nightly left today's partitions at ~43,000 two-second flushes
# per kind by midnight, and every reader of today listed them
# (``compact-closed-hours``). Clear of the projection at :40 and the judge at
# :05; the ``galata-record`` resource and the binary's hold order the rest.
SCHEDULE = Cron("20 * * * *", timezone="UTC")


@task(retries=0)
def compact(config: str) -> str:
    return _runner.run("galata-compact", ["--closed-hours"], config)


@flow(
    name="compact-the-archive",
    schedule=SCHEDULE,
    max_concurrent=1,
    on_overlap="skip",
    resources={"galata-record": 1},
)
def compact_the_archive(config: str = str(_runner.CONFIG)) -> str:
    return compact(config)
