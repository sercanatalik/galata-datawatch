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
    assert all(said[s].startswith("nothing to do") for s in ("varcov", "carry", "jumps"))
    assert tools.calls("galata-signals-commit") == []


def a_written_file_is_committed(tools, config):
    tools.exits("galata-signals", 0)
    tools.exits("galata-signals-commit", 0)
    said = derive_the_signals(str(config))
    assert set(said) == {"varcov", "varcov commit", "carry", "carry commit", "jumps", "jumps commit"}
    first, second, third = tools.calls("galata-signals")
    commits = tools.calls("galata-signals-commit")
    assert [first["argv"][0], second["argv"][0], third["argv"][0]] == ["varcov", "carry", "jumps"]
    assert [c["argv"][0] for c in commits] == [str(signals.STAGING / f"{s}.arrow") for s in ("varcov", "carry", "jumps")]
    assert first["argv"][first["argv"].index("--out") + 1] == str(signals.HANDOFF)
    assert first["argv"][-1] == str(signals.DECLARED)
    commit = commits[0]
    # A built environment, nothing inherited, for both.
    assert set(commit["env"]) <= {"GALATA_CONFIG", "PATH", "RUST_LOG", "NO_COLOR", "PWD", "SHLVL", "_"}


def a_refused_commit_fails_the_run(tools, config):
    tools.exits("galata-signals", 0)
    tools.exits("galata-signals-commit", 2)
    with pytest.raises(_runner.Broken, match="galata-signals-commit exited 2"):
        derive_the_signals(str(config))


def a_broken_varcov_does_not_cost_the_carry(tools, config):
    tools.exits("galata-signals", 1, 0)  # varcov broken, the rest fine
    tools.exits("galata-signals-commit", 0)
    with pytest.raises(_runner.Broken, match="varcov: galata-signals exited 1"):
        derive_the_signals(str(config))
    assert [c["argv"][0] for c in tools.calls("galata-signals-commit")] == [str(signals.STAGING / f"{s}.arrow") for s in ("carry", "jumps")]


def a_missing_calculator_names_its_sync(tools, config, tmp_path, monkeypatch):
    monkeypatch.setattr(_runner, "SIGNALS", tmp_path / "nowhere")
    with pytest.raises(_runner.Broken, match="uv sync --project py/signals"):
        derive_the_signals(str(config))


def the_signal_flow_holds_the_record():
    assert derive_the_signals.options["resources"] == {"galata-record": 1}
