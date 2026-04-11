"""Tests for pipewatch.cli_annotations CLI commands."""

from __future__ import annotations

import types

import pytest

from pipewatch.cli_annotations import cmd_delete, cmd_filter, cmd_get, cmd_set
from pipewatch.run_annotations import get_annotations, set_annotation


def make_args(base_dir, **kwargs) -> types.SimpleNamespace:
    return types.SimpleNamespace(base_dir=str(base_dir), **kwargs)


def test_cmd_set_creates_annotation(tmp_path, capsys):
    args = make_args(tmp_path, run_id="run-1", key="env", value="prod")
    cmd_set(args)
    captured = capsys.readouterr()
    assert "env" in captured.out
    assert get_annotations(str(tmp_path), "run-1")["env"] == "prod"


def test_cmd_get_shows_annotations(tmp_path, capsys):
    set_annotation(str(tmp_path), "run-1", "env", "prod")
    args = make_args(tmp_path, run_id="run-1")
    cmd_get(args)
    captured = capsys.readouterr()
    assert "env" in captured.out
    assert "prod" in captured.out


def test_cmd_get_no_annotations(tmp_path, capsys):
    args = make_args(tmp_path, run_id="run-99")
    cmd_get(args)
    captured = capsys.readouterr()
    assert "No annotations" in captured.out


def test_cmd_delete_existing_key(tmp_path, capsys):
    set_annotation(str(tmp_path), "run-1", "env", "prod")
    args = make_args(tmp_path, run_id="run-1", key="env")
    cmd_delete(args)
    captured = capsys.readouterr()
    assert "removed" in captured.out
    assert get_annotations(str(tmp_path), "run-1") == {}


def test_cmd_delete_missing_key_exits(tmp_path):
    args = make_args(tmp_path, run_id="run-1", key="env")
    with pytest.raises(SystemExit):
        cmd_delete(args)


def test_cmd_filter_shows_matching_runs(tmp_path, capsys):
    set_annotation(str(tmp_path), "run-1", "env", "prod")
    set_annotation(str(tmp_path), "run-2", "env", "staging")
    args = make_args(tmp_path, key="env", value="prod")
    cmd_filter(args)
    captured = capsys.readouterr()
    assert "run-1" in captured.out
    assert "run-2" not in captured.out


def test_cmd_filter_no_match_message(tmp_path, capsys):
    args = make_args(tmp_path, key="env", value="prod")
    cmd_filter(args)
    captured = capsys.readouterr()
    assert "No runs matched" in captured.out
