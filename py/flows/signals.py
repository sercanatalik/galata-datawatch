"""Derive the market-data signals. Every 30 minutes, at :15 and :45.

**Two tools, and no figure here** (design/roadmap.md, Tier 16). Each
calculator — ``galata-signals varcov``, ``carry``, ``jumps`` and ``liquidity``, in that order
(``py/signals``) — reads the tape through galata-research and writes one run's
rows as an Arrow IPC file; the commit,
``galata-signals-commit``, checks them against the dataset's schema and writes
them to the tape. The arithmetic is galata-research's and the tape's contract
is Rust's; this only gives them a cadence.

**The tape is the cursor.** The calculator computes only the horizons whose
bar has closed since the newest stored figure, and says ``nothing to do``
(exit 3) when none has; then there is nothing to commit. A missed run is
caught up by the next, for the newest bar: a signal is what was known at the
time, so history is not backfilled.

**One writer of the record at a time.** It holds ``galata-record`` like the
other writers, so it never reads a tape a projection is replacing. :15 and
:45 keep it clear of the projection at :40 and the watch at :05.
"""

from __future__ import annotations

from cereyan import Cron, flow, task

from . import _runner

SCHEDULE = Cron("15,45 * * * *", timezone="UTC")

# The calculators, in order, each with its own hand-off inside the record's
# root. A commit removes its hand-off; a commit that failed leaves it, and that
# calculator's next run writes over it.
CALCULATORS = ("varcov", "carry", "jumps", "liquidity", "basis", "flow", "moments", "cascade", "leadlag", "activity")
STAGING = _runner.REPO / "var" / "signals-staging"
HANDOFF = STAGING / "varcov.arrow"
# The declared horizons, models and baselines: a committed file, named here
# rather than found by the calculator relative to wherever it is installed.
DECLARED = _runner.REPO / "py" / "signals" / "signals.toml"


@task(retries=0)
def compute(config: str, signal: str, var: str, out: str) -> str:
    return _runner.run("galata-signals", [signal, "--var", var, "--out", out, "--config", str(DECLARED)], config)


@task(retries=0)
def commit_signals(config: str, out: str) -> str:
    return _runner.run("galata-signals-commit", [out], config)


@flow(
    name="derive-the-signals",
    schedule=SCHEDULE,
    max_concurrent=1,
    on_overlap="skip",
    resources={"galata-record": 1},
)
def derive_the_signals(config: str = str(_runner.CONFIG)) -> dict[str, str]:
    """Every calculator, then its commit. One that refuses does not stop the
    next; the run fails after all of them, naming each refusal, as
    `project-the-recent-days` does across venues."""
    said: dict[str, str] = {}
    refused: list[str] = []
    for signal in CALCULATORS:
        out = str(STAGING / f"{signal}.arrow")
        try:
            said[signal] = compute(config, signal, str(_runner.REPO / "var"), out)
            if not said[signal].startswith("nothing to do"):
                said[f"{signal} commit"] = commit_signals(config, out)
        except _runner.Refused as refusal:
            refused.append(f"{signal}: {refusal}")
    if refused:
        raise _runner.Broken("\n".join(refused))
    return said
