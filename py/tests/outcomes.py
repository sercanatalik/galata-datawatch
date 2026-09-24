"""The exit taxonomy, read per tool."""

from __future__ import annotations

import pytest

from flows import _runner
from flows.compact import compact_the_archive
from flows.retain import report_what_retention_would_expire
from flows.watch import judge_the_record


def a_compaction_with_nothing_to_do_completes(tools, config):
    tools.exits("galata-compact", 3)
    said = compact_the_archive(str(config))
    assert said.startswith("nothing to do")


def a_compaction_that_is_broken_fails(tools, config):
    tools.exits("galata-compact", 1)
    with pytest.raises(_runner.Broken, match="galata-compact exited 1"):
        compact_the_archive(str(config))


def a_watch_with_nothing_to_check_fails(tools, config):
    tools.exits("galata-watch", 3)
    with pytest.raises(_runner.NothingToCheck):
        judge_the_record(str(config))


def a_watch_with_findings_fails_carrying_them(tools, config):
    tools.exits("galata-watch", 1)
    with pytest.raises(_runner.Findings, match="galata-watch said its piece"):
        judge_the_record(str(config))


def a_clean_watch_completes(tools, config):
    tools.exits("galata-watch", 0)
    assert "said its piece" in judge_the_record(str(config))


@pytest.mark.parametrize("code", [4, 101, 137])
def an_unnamed_exit_code_is_broken(tools, config, code):
    tools.exits("galata-compact", code)
    with pytest.raises(_runner.Broken, match=f"exited {code}"):
        compact_the_archive(str(config))


def a_missing_binary_names_the_build(tools, config):
    with pytest.raises(_runner.Broken, match="cargo build --release"):
        compact_the_archive(str(config))


def a_missing_configuration_is_a_bad_argument(tools, tmp_path):
    tools.exits("galata-compact", 0)
    with pytest.raises(_runner.BadArgument, match="no configuration"):
        compact_the_archive(str(tmp_path / "absent.toml"))
    assert tools.calls("galata-compact") == []


def the_retention_report_passes_no_argument(tools, config):
    tools.exits("galata-retain", 3)
    report_what_retention_would_expire(str(config))
    [call] = tools.calls("galata-retain")
    assert call["argv"] == []
