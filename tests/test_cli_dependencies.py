"""Tests for pipewatch.cli_dependencies."""

from __future__ import annotations

import argparse

import pytest

from pipewatch.cli_dependencies import cmd_add, cmd_dependents, cmd_list, cmd_remove
from pipewatch.run_dependencies import get_dependencies


def make_args(base_dir, **kwargs) -> argparse.Namespace:
    defaults = {"base_dir": str(base_dir)}
    defaults.update(kwargs)
    return argparse.Namespace(**defaults)


def test_cmd_add_creates_dependency(tmp_path, capsys):
    args = make_args(tmp_path, run_id="run-b", upstream_id="run-a")
    cmd_add(args)
    assert "run-a" in get_dependencies("run-b", base_dir=str(tmp_path))


def test_cmd_add_prints_confirmation(tmp_path, capsys):
    args = make_args(tmp_path, run_id="run-b", upstream_id="run-a")
    cmd_add(args)
    out = capsys.readouterr().out
    assert "run-b" in out
    assert "run-a" in out


def test_cmd_remove_existing(tmp_path, capsys):
    add_args = make_args(tmp_path, run_id="run-b", upstream_id="run-a")
    cmd_add(add_args)
    rm_args = make_args(tmp_path, run_id="run-b", upstream_id="run-a")
    cmd_remove(rm_args)
    out = capsys.readouterr().out
    assert "removed" in out.lower()
    assert get_dependencies("run-b", base_dir=str(tmp_path)) == []


def test_cmd_remove_nonexistent_prints_error(tmp_path, capsys):
    args = make_args(tmp_path, run_id="run-b", upstream_id="run-a")
    cmd_remove(args)
    err = capsys.readouterr().err
    assert "No such dependency" in err


def test_cmd_list_shows_upstreams(tmp_path, capsys):
    cmd_add(make_args(tmp_path, run_id="run-c", upstream_id="run-a"))
    cmd_add(make_args(tmp_path, run_id="run-c", upstream_id="run-b"))
    capsys.readouterr()
    cmd_list(make_args(tmp_path, run_id="run-c"))
    out = capsys.readouterr().out
    assert "run-a" in out
    assert "run-b" in out


def test_cmd_list_empty(tmp_path, capsys):
    cmd_list(make_args(tmp_path, run_id="run-x"))
    out = capsys.readouterr().out
    assert "No dependencies" in out


def test_cmd_dependents_shows_downstream(tmp_path, capsys):
    cmd_add(make_args(tmp_path, run_id="run-b", upstream_id="run-a"))
    cmd_add(make_args(tmp_path, run_id="run-c", upstream_id="run-a"))
    capsys.readouterr()
    cmd_dependents(make_args(tmp_path, run_id="run-a"))
    out = capsys.readouterr().out
    assert "run-b" in out
    assert "run-c" in out


def test_cmd_dependents_none(tmp_path, capsys):
    cmd_dependents(make_args(tmp_path, run_id="run-leaf"))
    out = capsys.readouterr().out
    assert "No runs depend" in out
