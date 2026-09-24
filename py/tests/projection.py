"""The trailing window, and the venues it is projected for."""

from __future__ import annotations

from datetime import date, datetime, timezone

import pytest

from flows import _runner, project
from flows.project import project_the_closed_days, window


def the_window_is_three_closed_days_half_open():
    start, end = window(datetime(2026, 9, 24, 0, 40, tzinfo=timezone.utc))
    assert (start, end) == (date(2026, 9, 21), date(2026, 9, 24))


def the_window_is_taken_in_utc():
    # 23:30 on the 23rd in New York is already the 24th in UTC.
    from datetime import timedelta
    new_york = timezone(timedelta(hours=-4))
    start, end = window(datetime(2026, 9, 23, 23, 30, tzinfo=new_york))
    assert end == date(2026, 9, 24)


def the_projection_replaces_its_window(tools, config, monkeypatch):
    monkeypatch.setattr(project, "window", lambda now: (date(2026, 9, 21), date(2026, 9, 24)))
    tools.exits("galata-tape-rebuild", 0)
    project_the_closed_days(str(config))
    [call] = tools.calls("galata-tape-rebuild")
    assert call["argv"] == ["--replace", "hyperliquid", "2026-09-21", "2026-09-24"]


def every_declared_venue_is_projected_even_after_one_fails(tools, tmp_path):
    config = tmp_path / "two.toml"
    config.write_text("[venue.hyperliquid]\n\n[venue.rh-chain]\n")
    tools.exits("galata-tape-rebuild", 1, 0)
    with pytest.raises(_runner.Broken, match="hyperliquid"):
        project_the_closed_days(str(config))
    venues = [call["argv"][1] for call in tools.calls("galata-tape-rebuild")]
    assert venues == ["hyperliquid", "rh-chain"]


def no_declared_venue_is_a_refusal(tools, tmp_path):
    config = tmp_path / "none.toml"
    config.write_text('[paths]\narchive = "var/archive"\n')
    tools.exits("galata-tape-rebuild", 0)
    with pytest.raises(_runner.BadArgument, match=r"declares no \[venue"):
        project_the_closed_days(str(config))
    assert tools.calls("galata-tape-rebuild") == []
