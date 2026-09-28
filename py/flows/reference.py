"""Keep galata-research's reference store current. Daily at 07:30 UTC.

**One tool, and no figure here.** ``galata-fetch update`` (galata-research,
at the commit ``py/signals`` pins) brings each declared series of published
archives from the day after its last good day through yesterday, and the
FOMC and BLS calendars. The archives are the venues' public daily files
(settled point 8 of galata-research: never a live API, never an account).
It reads and writes galata-research's store, never the record, so it holds
no ``galata-record``.

**The store is the cursor.** Its manifest says which days are held, so a
missed run is caught up by the next, and a day asked before the venue
published it is asked again. 07:30 is after Binance publishes the previous
UTC day, and clear of the lane's :05, :15, :20, :40 and :45.

**No contact is passed.** bls.gov is asked only with ``GALATA_CONTACT``,
which this job never receives, so the BLS events already held are kept and
not refreshed.
"""

from __future__ import annotations

from cereyan import Cron, flow, task

from . import _runner

SCHEDULE = Cron("30 7 * * *", timezone="UTC")

# galata-research's store: the sibling checkout, as py/signals' pin names it.
REFERENCE = _runner.REPO.parent / "galata-research" / "var" / "reference"


@task(retries=0)
def fetch(config: str) -> str:
    if not REFERENCE.is_dir():
        raise _runner.Broken(f"no reference store at {REFERENCE}: seed it once with galata-fetch in galata-research")
    return _runner.run("galata-fetch", ["update"], config, paths={"GALATA_REFERENCE": REFERENCE})


@flow(name="update-the-reference", schedule=SCHEDULE, max_concurrent=1, on_overlap="skip")
def update_the_reference(config: str = str(_runner.CONFIG)) -> str:
    """Every declared series up to yesterday; a failed day fails the run, and tomorrow's asks again."""
    return fetch(config)
