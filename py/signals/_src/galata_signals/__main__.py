"""`galata-signals varcov --var <root> --out <file.arrow> [--config signals.toml]`.

Exit codes, as the maintenance tools use them and the lane's runner reads them:

    0   wrote the rows to --out     1   broken
    2   bad argument                3   nothing new: no file written
"""

from __future__ import annotations

import argparse
import importlib.metadata
import json
import os
import sys
import tomllib
from datetime import datetime, timezone
from pathlib import Path

DONE, BROKEN, BAD_ARGUMENT, NOTHING = 0, 1, 2, 3
HERE = Path(__file__).resolve().parents[2]  # py/signals, beside signals.toml


def code() -> str:
    """galata-research's commit, from its install record (PEP 610), or `unpinned`."""
    try:
        record = json.loads(importlib.metadata.distribution("galata-research").read_text("direct_url.json") or "{}")
    except importlib.metadata.PackageNotFoundError:
        return "unpinned"
    return record.get("vcs_info", {}).get("commit_id", "unpinned")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="galata-signals")
    parser.add_argument("signal", choices=["varcov"])
    parser.add_argument("--var", required=True, type=Path, help="the record's root, holding tape/")
    parser.add_argument("--out", required=True, type=Path, help="the Arrow IPC file to hand to galata-signals-commit")
    parser.add_argument("--config", type=Path, default=HERE / "signals.toml")
    parser.add_argument("--now", help="the run's computed instant, ISO 8601 with a zone (default: the clock)")
    try:
        args = parser.parse_args(argv)
    except SystemExit as exit_:
        return BAD_ARGUMENT if exit_.code else DONE
    tape = args.var / "tape"
    if not tape.is_dir():
        print(f"no tape at {tape}", file=sys.stderr)
        return BAD_ARGUMENT
    # galata-research reads the record from GALATA_VAR; set before it loads.
    os.environ["GALATA_VAR"] = str(args.var.resolve())
    from . import schema, varcov

    try:
        with args.config.open("rb") as fh:
            declared = tomllib.load(fh).get("varcov", {})
        horizons = [varcov.Horizon.declared(name, table) for name, table in declared.items()]
    except (OSError, ValueError, TypeError, tomllib.TOMLDecodeError) as error:
        print(f"{args.config}: {error}", file=sys.stderr)
        return BAD_ARGUMENT
    if not horizons:
        print(f"{args.config} declares no [varcov.*] horizon", file=sys.stderr)
        return BAD_ARGUMENT
    now = datetime.fromisoformat(args.now) if args.now else datetime.now(timezone.utc)
    if now.tzinfo is None:
        print("--now needs a zone", file=sys.stderr)
        return BAD_ARGUMENT
    run = varcov.Run(computed_micros=varcov.micros(now), code=code())
    try:
        varcov.compute(horizons, tape, run)
    except Exception as error:  # the tool's own failure, reported as broken
        print(f"broken: {type(error).__name__}: {error}", file=sys.stderr)
        return BROKEN
    for line in run.said:
        print(line)
    if not run.rows:
        return NOTHING
    schema.write(run.rows, args.out)
    print(f"{len(run.rows)} rows, run {run.run_id}, to {args.out}")
    return DONE


if __name__ == "__main__":
    sys.exit(main())
