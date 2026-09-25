"""How a flow runs one of the record's tools: as a subprocess, and nothing else.

**The only module that imports ``subprocess``**, which
``scripts/check-python-flows.sh`` asserts. The leading underscore keeps it
out of ``cereyan serve``'s flow discovery and leaves it importable.

**A scheduler's environment is empty.** No PATH, no cargo, none of an
operator's variables. So the release directory is named from this file's own
location, and the working directory is the repository root, so a
configuration's relative ``var/…`` paths mean what they mean from a shell.

**And a scheduler's environment may hold anything.** The job gets exactly
the four variables in :func:`job_env` and inherits nothing, so a vault token
or a broker password in cereyan's own environment cannot reach a tool. The
jobs need none (the tools read a file configuration and open no broker); this
makes that true whatever the environment holds, not only true today.

**The exit code is the tools' interface** (each binary's own doc says so),
and it means different things to different tools, so it is read by table:

    tool                 0     1          2            3
    galata-compact       ok    Broken     BadArgument  ok, nothing to do
    galata-tape-rebuild  ok    Broken     BadArgument  ok, nothing to do
    galata-retain        ok    Broken     BadArgument  ok, nothing to do
    galata-watch         ok    Findings   BadArgument  NothingToCheck
    anything else        Broken

``3`` fails the watch because the binary names what it means there: *"what
an empty archive looks like when capture has silently stopped."*
"""

from __future__ import annotations

import subprocess
from pathlib import Path

# py/flows/_runner.py -> py/flows -> py -> the checkout.
REPO = Path(__file__).resolve().parents[2]
RELEASE = REPO / "target" / "release"
COMMITTED = REPO / "config" / "datawatch.toml"
LOCAL = REPO / "var" / "datawatch.local.toml"


def deployment_config() -> Path:
    """The configuration this deployment runs from — the same file capture does.

    ``var/datawatch.local.toml`` when it exists (the committed file plus this
    machine's ``[broker]``, ``[watch]`` and whatever else it declares), and
    otherwise the committed ``config/datawatch.toml``. The test is the one
    ``scripts/run-service.sh`` applies to capture.

    **Whole-file, never merged.** "One file, one type, one load": a merged
    override would be a second definition of what capture was told, and the
    defect this replaced was two — capture read the local file while every
    flow read the committed one, so a ``[watch]`` bound or a second venue
    declared for the deployment never reached the lane.
    """
    return LOCAL if LOCAL.is_file() else COMMITTED


# Resolved at import; the lane is restarted to pick up a new local file.
CONFIG = deployment_config()

# The whole environment a job receives. `/usr/bin:/bin` because a tool that
# shells out to nothing still gets a PATH that resolves `sh` rather than an
# empty one; `RUST_LOG=info` because the tools log through `tracing`, and the
# run's log is where their refusals are read; `NO_COLOR=1` because
# tracing-subscriber 0.3 colours its output whether or not anyone is at a
# terminal, and the first real run put escape codes in every run's result.
PATH = "/usr/bin:/bin"
RUST_LOG = "info"
NO_COLOR = "1"


class Refused(RuntimeError):
    """A tool did not do its work. Carries the tool's own output."""


class Broken(Refused):
    """Exit 1 from a maintenance tool, or a code the taxonomy does not name."""


class BadArgument(Refused):
    """Exit 2. The invocation is wrong, and repeating it will not help."""


class Findings(Refused):
    """Exit 1 from ``galata-watch``: the record breaks a declared bound."""


class NothingToCheck(Refused):
    """Exit 3 from ``galata-watch``: there was no record to judge."""


MAINTENANCE = {1: Broken, 2: BadArgument}
WATCH = {1: Findings, 2: BadArgument, 3: NothingToCheck}
TABLES = {
    "galata-compact": MAINTENANCE,
    "galata-tape-rebuild": MAINTENANCE,
    "galata-retain": MAINTENANCE,
    "galata-watch": WATCH,
}

# What `3` means where it is not a failure — said in the run's result, so
# "did nothing" and "did something" do not read the same in the history.
NOTHING = 3


def binary(name: str) -> Path:
    """A release binary, refused by name when it is absent."""
    path = RELEASE / name
    if not path.exists():
        raise Broken(f"no release binary at {path} — cargo build --release")
    return path


def job_env(config: Path) -> dict[str, str]:
    """Exactly what a job receives. Nothing is inherited."""
    return {"GALATA_CONFIG": str(config), "PATH": PATH, "RUST_LOG": RUST_LOG, "NO_COLOR": NO_COLOR}


def run(tool: str, args: list[str], config: Path = CONFIG) -> str:
    """Run one tool to completion and return what it said.

    Raises a :class:`Refused` chosen by the tool's table. The configuration
    path is made absolute before it is handed over, so it means the same
    thing whatever directory the scheduler started in.
    """
    table = TABLES[tool]
    config = Path(config).resolve()
    if not config.is_file():
        raise BadArgument(f"no configuration at {config}")
    finished = subprocess.run(
        [str(binary(tool)), *args],
        cwd=str(REPO),
        env=job_env(config),
        capture_output=True,
        text=True,
    )
    output = ((finished.stdout or "") + (finished.stderr or "")).strip()
    code = finished.returncode
    if code == 0:
        return output
    if code == NOTHING and tool != "galata-watch":
        return f"nothing to do\n{output}".strip()
    refusal = table.get(code, Broken)
    raise refusal(f"{tool} exited {code}\n{output}")
