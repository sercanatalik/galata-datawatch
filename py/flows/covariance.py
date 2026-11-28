"""Derive the EWMA covariance signals in Rust. Every 30 minutes, at :15 and :45.

**One tool, and no figure here** (`add-galata-quant`). ``galata-quant-signals
varcov`` reads the tape's candles, computes `varcov`'s EWMA horizons (30m, 1d,
1w) and what is derived from their Σ with galata-quant — the arithmetic
galata-research's ``gr.models.corr`` calls too — and commits the rows to the
tape itself. This only gives it a cadence.

**The tape is the cursor**, as for every calculator: a horizon whose bar has
not closed since the newest stored figure is skipped, and ``nothing to do``
(exit 3) is not a failure. A missed run is caught up by the next, for the
newest bar; history is not backfilled.

**One writer of the record at a time.** It holds ``galata-record`` like the
other writers; :15 and :45 keep it clear of the projection at :40 and the
watch at :05. ``derive-the-signals``, py/signals' flow, is not in the image
while galata-research is set aside, so this is the one signals writer.
"""

from __future__ import annotations

from cereyan import Cron, flow, task

from . import _runner

SCHEDULE = Cron("15,45 * * * *", timezone="UTC")
# The declared horizons: the committed file both calculators read.
DECLARED = _runner.REPO / "py" / "signals" / "signals.toml"


@task(retries=0)
def compute_and_commit(config: str, var: str) -> str:
    return _runner.run("galata-quant-signals", ["varcov", "--var", var, "--config", str(DECLARED)], config)


@flow(
    name="derive-the-covariance",
    schedule=SCHEDULE,
    max_concurrent=1,
    on_overlap="skip",
    resources={"galata-record": 1},
)
def derive_the_covariance(config: str = str(_runner.CONFIG)) -> str:
    """The EWMA covariance, computed and committed by one tool."""
    return compute_and_commit(config, str(_runner.REPO / "var"))
