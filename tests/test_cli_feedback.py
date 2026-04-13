"""Tests for pipewatch.cli_feedback."""

import json
import pytest
from pipewatch.cli_feedback import cmd_add, cmd_get, cmd_remove, cmd_list, cmd_average, build_parser
from pipewatch.run_feedback import add_feedback


class Args:
    def __init__(self, **kwargs):
        for k, v in kwargs.items():
            setattr(self, k, v)


def make_args(base_dir, **kwargs):
    defaults = {"base_dir": str(base_dir), "comment": None, "author": None, "json": False}
    defaults.update(kwargs)
    return Args(**defaults)


def test_cmd_add_creates_feedback(tmp_path, capsys):
    args = make_args(tmp_path, run_id="run-1", rating=5)
    cmd_add(args)
    out = capsys.readouterr().out
    assert "run-1" in out
    assert "rating=5" in out


def test_cmd_add_invalid_rating_exits(tmp_path, capsys):
    args = make_args(tmp_path, run_id="run-1", rating=9)
    with pytest.raises(SystemExit):
        cmd_add(args)


def test_cmd_get_shows_entry(tmp_path, capsys):
    add_feedback(str(tmp_path), "run-1", 3, comment="ok", author="bob")
    args = make_args(tmp_path, run_id="run-1")
    cmd_get(args)
    out = capsys.readouterr().out
    assert "3/5" in out
    assert "bob" in out
    assert "ok" in out


def test_cmd_get_json_output(tmp_path, capsys):
    add_feedback(str(tmp_path), "run-1", 4, comment="nice")
    args = make_args(tmp_path, run_id="run-1", **{"json": True})
    cmd_get(args)
    out = capsys.readouterr().out
    parsed = json.loads(out)
    assert parsed["rating"] == 4


def test_cmd_get_missing_run(tmp_path, capsys):
    args = make_args(tmp_path, run_id="ghost")
    cmd_get(args)
    out = capsys.readouterr().out
    assert "No feedback" in out


def test_cmd_remove_existing(tmp_path, capsys):
    add_feedback(str(tmp_path), "run-1", 2)
    args = make_args(tmp_path, run_id="run-1")
    cmd_remove(args)
    out = capsys.readouterr().out
    assert "removed" in out.lower()


def test_cmd_remove_nonexistent(tmp_path, capsys):
    args = make_args(tmp_path, run_id="ghost")
    cmd_remove(args)
    out = capsys.readouterr().out
    assert "No feedback" in out


def test_cmd_list_empty(tmp_path, capsys):
    args = make_args(tmp_path)
    cmd_list(args)
    out = capsys.readouterr().out
    assert "No feedback" in out


def test_cmd_list_shows_entries(tmp_path, capsys):
    add_feedback(str(tmp_path), "run-1", 5, comment="excellent")
    args = make_args(tmp_path)
    cmd_list(args)
    out = capsys.readouterr().out
    assert "run-1" in out
    assert "*****" in out


def test_cmd_average_no_data(tmp_path, capsys):
    args = make_args(tmp_path)
    cmd_average(args)
    out = capsys.readouterr().out
    assert "No feedback" in out


def test_cmd_average_with_data(tmp_path, capsys):
    add_feedback(str(tmp_path), "run-1", 4)
    add_feedback(str(tmp_path), "run-2", 2)
    args = make_args(tmp_path)
    cmd_average(args)
    out = capsys.readouterr().out
    assert "3.0" in out


def test_build_parser_returns_parser(tmp_path):
    p = build_parser()
    assert p is not None
