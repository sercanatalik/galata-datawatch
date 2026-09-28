"""The signal flow: two tools in order, and the second only when the first wrote."""

from __future__ import annotations

import pytest

from flows import _runner
from flows import signals
from flows.signals import derive_the_signals


def no_commit_follows_nothing_new(tools, config):
    tools.exits("galata-signals", 3)
    tools.exits("galata-signals-commit", 0)
    said = derive_the_signals(str(config))
    assert said["varcov"].startswith("nothing to do")
    assert tools.calls("galata-signals-commit") == []


def a_written_file_is_committed(tools, config):
    tools.exits("galata-signals", 0)
    tools.exits("galata-signals-commit", 0)
    said = derive_the_signals(str(config))
    assert set(said) == {"varcov", "commit"}
    (compute,) = tools.calls("galata-signals")
    (commit,) = tools.calls("galata-signals-commit")
    assert compute["argv"][:2] == ["varcov", "--var"]
    assert compute["argv"][compute["argv"].index("--out") + 1] == commit["argv"][0] == str(signals.HANDOFF)
    assert compute["argv"][-1] == str(signals.DECLARED)
    # A built environment, nothing inherited, for both.
    assert set(commit["env"]) <= {"GALATA_CONFIG", "PATH", "RUST_LOG", "NO_COLOR", "PWD", "SHLVL", "_"}


def a_refused_commit_fails_the_run(tools, config):
    tools.exits("galata-signals", 0)
    tools.exits("galata-signals-commit", 2)
    with pytest.raises(_runner.BadArgument, match="galata-signals-commit exited 2"):
        derive_the_signals(str(config))


def a_missing_calculator_names_its_sync(tools, config, tmp_path, monkeypatch):
    monkeypatch.setattr(_runner, "SIGNALS", tmp_path / "nowhere")
    with pytest.raises(_runner.Broken, match="uv sync --project py/signals"):
        derive_the_signals(str(config))


def the_signal_flow_holds_the_record():
    assert derive_the_signals.options["resources"] == {"galata-record": 1}
