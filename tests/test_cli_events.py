"""Tests for pipewatch.cli_events."""

from __future__ import annotations

import argparse
import pytest

import pipewatch.run_events as re_mod
from pipewatch.cli_events import cmd_add, cmd_clear, cmd_list, build_parser


@pytest.fixture(autouse=True)
def _tmp_events(tmp_path, monkeypatch):
    monkeypatch.setattr(re_mod, "_EVENTS_DIR", tmp_path / "events")


def make_args(**kwargs) -> argparse.Namespace:
    defaults = dict(run_id="run-1", event_type="test_event", message="hello", level="info")
    defaults.update(kwargs)
    return argparse.Namespace(**defaults)


def test_cmd_add_records_event(capsys):
    cmd_add(make_args())
    out = capsys.readouterr().out
    assert "Event recorded" in out
    assert "INFO" in out


def test_cmd_add_records_warning_level(capsys):
    cmd_add(make_args(level="warning"))
    out = capsys.readouterr().out
    assert "WARNING" in out


def test_cmd_list_empty(capsys):
    cmd_list(make_args(event_type=None))
    out = capsys.readouterr().out
    assert "(no events)" in out


def test_cmd_list_shows_events(capsys):
    re_mod.record_event("run-1", "test_event", "hello")
    cmd_list(make_args(event_type=None))
    out = capsys.readouterr().out
    assert "test_event" in out
    assert "hello" in out


def test_cmd_list_filters_by_level(capsys):
    re_mod.record_event("run-1", "a", "info msg", level="info")
    re_mod.record_event("run-1", "b", "err msg", level="error")
    cmd_list(make_args(level="error", event_type=None))
    out = capsys.readouterr().out
    assert "err msg" in out
    assert "info msg" not in out


def test_cmd_clear_prints_count(capsys):
    re_mod.record_event("run-1", "x", "y")
    re_mod.record_event("run-1", "x", "z")
    cmd_clear(make_args())
    out = capsys.readouterr().out
    assert "2" in out
    assert "Cleared" in out


def test_cmd_clear_missing_run(capsys):
    cmd_clear(make_args(run_id="ghost"))
    out = capsys.readouterr().out
    assert "0" in out


def test_build_parser_standalone():
    p = build_parser()
    assert p is not None


def test_build_parser_add_subcommand():
    p = build_parser()
    args = p.parse_args(["add", "run-x", "stage_start", "Started"])
    assert args.run_id == "run-x"
    assert args.event_type == "stage_start"
    assert args.level == "info"
