"""Stand-in tools, so a flow runs end to end without the record.

Each stand-in is a shell script at ``<release>/<tool>`` that appends its argv,
working directory and whole environment to a log, then exits with the next
code from a per-tool queue (the last code repeats). The flows run as plain
functions against a temporary ``CEREYAN_HOME``, which is cereyan's own
offline mode — no server, no mocks of the orchestrator.
"""

from __future__ import annotations

import json
import os
import stat
from pathlib import Path

import pytest

from flows import _runner

STAND_IN = """#!/bin/sh
here=$(dirname "$0")
{{
  printf '%s\\n' "$*"
  pwd
  env
  printf '%s\\n' '---'
}} >> "$here/{tool}.log"
codes="$here/{tool}.codes"
code=$(head -n 1 "$codes")
if [ "$(wc -l < "$codes")" -gt 1 ]; then tail -n +2 "$codes" > "$codes.next" && mv "$codes.next" "$codes"; fi
echo "{tool} said its piece"
exit "$code"
"""


class Tools:
    """The stand-in release directory, and what each tool was asked."""

    def __init__(self, release: Path):
        self.release = release

    def exits(self, tool: str, *codes: int) -> None:
        script = self.release / tool
        script.write_text(STAND_IN.format(tool=tool))
        script.chmod(script.stat().st_mode | stat.S_IXUSR)
        (self.release / f"{tool}.codes").write_text("".join(f"{c}\n" for c in codes))

    def calls(self, tool: str) -> list[dict]:
        log = self.release / f"{tool}.log"
        if not log.exists():
            return []
        calls = []
        for block in log.read_text().split("---\n"):
            lines = block.splitlines()
            if not lines:
                continue
            env = dict(line.split("=", 1) for line in lines[2:] if "=" in line)
            calls.append({"argv": lines[0].split(), "cwd": lines[1], "env": env})
        return calls


@pytest.fixture(autouse=True)
def cereyan_home(tmp_path, monkeypatch):
    monkeypatch.setenv("CEREYAN_HOME", str(tmp_path / "home"))


@pytest.fixture
def tools(tmp_path, monkeypatch) -> Tools:
    release = tmp_path / "release"
    release.mkdir()
    monkeypatch.setattr(_runner, "RELEASE", release)
    return Tools(release)


@pytest.fixture
def config(tmp_path) -> Path:
    path = tmp_path / "datawatch.toml"
    path.write_text('[paths]\narchive = "var/archive"\n\n[venue.hyperliquid]\n')
    return path
