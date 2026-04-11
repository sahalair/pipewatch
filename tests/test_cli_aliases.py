"""Tests for pipewatch.cli_aliases."""

import sys
import pytest

from pipewatch.cli_aliases import (
    cmd_set,
    cmd_remove,
    cmd_resolve,
    cmd_list,
    cmd_find,
    build_parser,
)
from pipewatch.run_aliases import set_alias


def make_args(base_dir, **kwargs):
    class Args:
        pass
    a = Args()
    a.base_dir = str(base_dir)
    for k, v in kwargs.items():
        setattr(a, k, v)
    return a


def test_cmd_set_creates_alias(tmp_path):
    args = make_args(tmp_path, alias="prod", run_id="run-abc")
    cmd_set(args)
    from pipewatch.run_aliases import resolve_alias
    assert resolve_alias(str(tmp_path), "prod") == "run-abc"


def test_cmd_set_prints_confirmation(tmp_path, capsys):
    args = make_args(tmp_path, alias="latest", run_id="run-xyz")
    cmd_set(args)
    out = capsys.readouterr().out
    assert "latest" in out
    assert "run-xyz" in out


def test_cmd_remove_existing(tmp_path, capsys):
    set_alias(str(tmp_path), "old", "run-1")
    args = make_args(tmp_path, alias="old")
    cmd_remove(args)
    out = capsys.readouterr().out
    assert "old" in out


def test_cmd_remove_nonexistent_exits(tmp_path):
    args = make_args(tmp_path, alias="ghost")
    with pytest.raises(SystemExit):
        cmd_remove(args)


def test_cmd_resolve_found(tmp_path, capsys):
    set_alias(str(tmp_path), "stable", "run-55")
    args = make_args(tmp_path, alias="stable")
    cmd_resolve(args)
    out = capsys.readouterr().out
    assert "run-55" in out


def test_cmd_resolve_not_found_exits(tmp_path):
    args = make_args(tmp_path, alias="missing")
    with pytest.raises(SystemExit):
        cmd_resolve(args)


def test_cmd_list_empty(tmp_path, capsys):
    args = make_args(tmp_path)
    cmd_list(args)
    out = capsys.readouterr().out
    assert "No aliases" in out


def test_cmd_list_shows_aliases(tmp_path, capsys):
    set_alias(str(tmp_path), "a", "run-1")
    set_alias(str(tmp_path), "b", "run-2")
    args = make_args(tmp_path)
    cmd_list(args)
    out = capsys.readouterr().out
    assert "a" in out
    assert "run-1" in out


def test_cmd_find_shows_aliases(tmp_path, capsys):
    set_alias(str(tmp_path), "x", "run-99")
    set_alias(str(tmp_path), "y", "run-99")
    args = make_args(tmp_path, run_id="run-99")
    cmd_find(args)
    out = capsys.readouterr().out
    assert "x" in out
    assert "y" in out


def test_build_parser_standalone():
    parser = build_parser()
    assert parser is not None
