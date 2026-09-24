"""Only history retries, and only when broken."""

from __future__ import annotations

from datetime import date

import pytest

from flows import _runner, rebuild
from flows.rebuild import rebuild_one_day


@pytest.fixture(autouse=True)
def no_waiting(monkeypatch):
    # The delays are the claim of another test; this one is about which
    # failures are retried, and should not spend 150 seconds proving it.
    monkeypatch.setattr(rebuild.rebuild_day, "retry_delay", 0)


def the_history_rebuild_retries_three_times_with_legacys_delays():
    assert rebuild.RETRY_DELAYS == [30, 120, 600]
    assert rebuild.rebuild_day.retries == 3
    assert rebuild.rebuild_day.retry_on == (_runner.Broken,)


def a_bad_argument_is_never_retried(tools, config):
    tools.exits("galata-tape-rebuild", 2)
    with pytest.raises(_runner.BadArgument):
        rebuild_one_day("hyperliquid", date(2026, 1, 5), str(config))
    assert len(tools.calls("galata-tape-rebuild")) == 1


def a_broken_history_rebuild_is_retried(tools, config):
    tools.exits("galata-tape-rebuild", 1, 0)
    rebuild_one_day("hyperliquid", date(2026, 1, 5), str(config))
    calls = tools.calls("galata-tape-rebuild")
    assert len(calls) == 2
    assert calls[0]["argv"] == ["--replace", "hyperliquid", "2026-01-05"]


def no_scheduled_flow_retries():
    from flows import compact, project, retain, watch
    for task in (compact.compact, project.project_venue, retain.report, watch.judge):
        assert task.retries == 0, task
