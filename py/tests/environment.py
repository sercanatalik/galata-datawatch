"""What a job receives, whatever the scheduler holds."""

from __future__ import annotations

from flows import _runner
from flows.compact import compact_the_archive


def a_scheduler_credential_never_reaches_a_job(tools, config, monkeypatch):
    monkeypatch.setenv("GV_TOKEN", "gvt_should_not_travel")
    monkeypatch.setenv("GALATA_DATAWATCH_PASSWORD", "hunter2")
    monkeypatch.setenv("CEREYAN_TOKEN", "also-not")
    tools.exits("galata-compact", 0)
    compact_the_archive(str(config))
    [call] = tools.calls("galata-compact")
    env = {k: v for k, v in call["env"].items() if k not in {"PWD", "SHLVL", "_", "OLDPWD"}}
    assert env == {
        "GALATA_CONFIG": str(config.resolve()),
        "PATH": "/usr/bin:/bin",
        "RUST_LOG": "info",
        "NO_COLOR": "1",
    }


def the_job_runs_from_the_repository_root(tools, config):
    tools.exits("galata-compact", 0)
    compact_the_archive(str(config))
    [call] = tools.calls("galata-compact")
    assert call["cwd"] == str(_runner.REPO)
