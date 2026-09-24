"""Report what a declared retention horizon would expire. Weekly.

**The delete is not a flow, and that is the security boundary.** cereyan's
``run_flow`` starts any registered flow, so the only exposure control that
provably exists is registration: if it must not be triggerable, it is not
registered. ``galata-retain`` with no argument reports and removes nothing;
``--delete`` stays the operator's hand.

It will report that nothing is declared for as long as no ``[retention]``
block exists — *keep everything* is the operator's standing answer — and it
runs anyway, because a job that has never been run is one that does not work
the first time it is wanted.
"""

from __future__ import annotations

from cereyan import Cron, flow, task

from . import _runner

# Sundays 01:30 UTC, after the nightly compaction and projection have settled.
SCHEDULE = Cron("30 1 * * 0", timezone="UTC")


@task(retries=0)
def report(config: str) -> str:
    return _runner.run("galata-retain", [], config)


@flow(name="report-what-retention-would-expire", schedule=SCHEDULE)
def report_what_retention_would_expire(config: str = str(_runner.CONFIG)) -> str:
    return report(config)
