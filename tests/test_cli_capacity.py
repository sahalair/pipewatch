"""Tests for pipewatch.cli_capacity."""

import sys
import types
import pytest

from pipewatch.cli_capacity import cmd_check, cmd_list, cmd_remove, cmd_set
from pipewatch.run_capacity import set_capacity


def make_args(base_dir, **kwargs):
    ns = types.SimpleNamespace(base_dir=str(base_dir))
    for k, v in kwargs.items():
        setattr(ns, k, v)
    return ns


def test_cmd_set_creates_rule(tmp_path, capsys):
    args = make_args(tmp_path, pipeline="etl", max_concurrent=4)
    cmd_set(args)
    out = capsys.readouterr().out
    assert "etl" in out
    assert "4" in out


def test_cmd_remove_existing(tmp_path, capsys):
    set_capacity(str(tmp_path), "etl", 3)
    args = make_args(tmp_path, pipeline="etl")
    cmd_remove(args)
    out = capsys.readouterr().out
    assert "removed" in out.lower()


def test_cmd_remove_nonexistent_prints_error(tmp_path, capsys):
    args = make_args(tmp_path, pipeline="ghost")
    cmd_remove(args)
    err = capsys.readouterr().err
    assert "ghost" in err


def test_cmd_list_empty(tmp_path, capsys):
    args = make_args(tmp_path)
    cmd_list(args)
    out = capsys.readouterr().out
    assert "no" in out.lower()


def test_cmd_list_shows_rules(tmp_path, capsys):
    set_capacity(str(tmp_path), "pipe_a", 2)
    set_capacity(str(tmp_path), "pipe_b", 5)
    args = make_args(tmp_path)
    cmd_list(args)
    out = capsys.readouterr().out
    assert "pipe_a" in out
    assert "pipe_b" in out


def test_cmd_check_no_rule(tmp_path, capsys):
    args = make_args(tmp_path, pipeline="pipe", active=3)
    cmd_check(args)
    out = capsys.readouterr().out
    assert "no capacity limit" in out


def test_cmd_check_under_capacity(tmp_path, capsys):
    set_capacity(str(tmp_path), "pipe", 5)
    args = make_args(tmp_path, pipeline="pipe", active=2)
    cmd_check(args)
    out = capsys.readouterr().out
    assert "OK" in out


def test_cmd_check_at_capacity_exits_1(tmp_path):
    set_capacity(str(tmp_path), "pipe", 3)
    args = make_args(tmp_path, pipeline="pipe", active=3)
    with pytest.raises(SystemExit) as exc:
        cmd_check(args)
    assert exc.value.code == 1
