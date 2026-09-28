"""The reference flow: one galata-fetch, pointed at galata-research's store, and nothing inherited."""

from __future__ import annotations

import pytest

from flows import _runner, reference
from flows.reference import update_the_reference


@pytest.fixture
def store(tmp_path, monkeypatch):
    path = tmp_path / "reference"
    path.mkdir()
    monkeypatch.setattr(reference, "REFERENCE", path)
    return path


def the_update_is_asked_with_the_store_named(tools, config, store):
    tools.exits("galata-fetch", 0)
    update_the_reference(str(config))
    [call] = tools.calls("galata-fetch")
    assert call["argv"] == ["update"]
    env = {k: v for k, v in call["env"].items() if k not in {"PWD", "SHLVL", "_", "OLDPWD"}}
    assert env == {"GALATA_CONFIG": str(config.resolve()), "PATH": "/usr/bin:/bin", "RUST_LOG": "info", "NO_COLOR": "1",
                   "GALATA_REFERENCE": str(store.resolve())}  # fmt: skip


def no_contact_reaches_the_fetch(tools, config, store, monkeypatch):
    monkeypatch.setenv("GALATA_CONTACT", "someone@example.com")
    tools.exits("galata-fetch", 0)
    update_the_reference(str(config))
    [call] = tools.calls("galata-fetch")
    assert "GALATA_CONTACT" not in call["env"]


def a_failed_day_fails_the_run(tools, config, store):
    tools.exits("galata-fetch", 1)
    with pytest.raises(_runner.Broken, match="galata-fetch exited 1"):
        update_the_reference(str(config))


def a_missing_store_is_refused_before_any_fetch(tools, config, tmp_path, monkeypatch):
    monkeypatch.setattr(reference, "REFERENCE", tmp_path / "nowhere")
    tools.exits("galata-fetch", 0)
    with pytest.raises(_runner.Broken, match="no reference store"):
        update_the_reference(str(config))
    assert tools.calls("galata-fetch") == []


def the_reference_flow_never_holds_the_record():
    assert "galata-record" not in (update_the_reference.options.get("resources") or {})
