"""Judge the record. Hourly.

**It watches the record, not the scheduler.** A missed compaction shows up as
a closed partition still holding its segments; a stopped capture shows up as
a record older than its bound. Both are facts on disk, and neither needs
cereyan to be trusted — this flow is only how often somebody asks.

Findings fail the run, and so does *nothing to check*: the binary names that
as what an empty archive looks like when capture has silently stopped, and a
watcher that reported it as success would hide the one thing it is for.
"""

from __future__ import annotations

from cereyan import Cron, flow, task

from . import _runner

# :05 past the hour, clear of the nightly jobs' :10 and :40.
SCHEDULE = Cron("5 * * * *", timezone="UTC")


@task(retries=0)
def judge(config: str) -> str:
    return _runner.run("galata-watch", [], config)


@flow(name="judge-the-record", schedule=SCHEDULE, max_concurrent=1, on_overlap="skip")
def judge_the_record(config: str = str(_runner.CONFIG)) -> str:
    return judge(config)
