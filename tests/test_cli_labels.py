"""Tests for pipewatch.cli_labels commands."""

from __future__ import annotations

import argparse

import pytest

from pipewatch.cli_labels import cmd_filter, cmd_list, cmd_remove, cmd_set
from pipewatch.run_labels import get_labels, load_labels


def make_args(base_dir: str, **kwargs) -> argparse.Namespace:
    defaults = {"base_dir": base_dir}
    defaults.update(kwargs)
    return argparse.Namespace(**defaults)


def test_cmd_set_creates_label(tmp_path, capsys):
    args = make_args(str(tmp_path), run_id="run-1", key="env", value="prod")
    cmd_set(args)
    labels = get_labels(str(tmp_path), "run-1")
    assert labels["env"] == "prod"


def test_cmd_set_prints_confirmation(tmp_path, capsys):
    args = make_args(str(tmp_path), run_id="run-1", key="env", value="prod")
    cmd_set(args)
    out = capsys.readouterr().out
    assert "env=prod" in out


def test_cmd_remove_deletes_label(tmp_path, capsys):
    args_set = make_args(str(tmp_path), run_id="run-1", key="env", value="prod")
    cmd_set(args_set)
    args_rm = make_args(str(tmp_path), run_id="run-1", key="env")
    cmd_remove(args_rm)
    labels = get_labels(str(tmp_path), "run-1")
    assert "env" not in labels


def test_cmd_remove_prints_message(tmp_path, capsys):
    args_set = make_args(str(tmp_path), run_id="run-1", key="env", value="prod")
    cmd_set(args_set)
    args_rm = make_args(str(tmp_path), run_id="run-1", key="env")
    cmd_remove(args_rm)
    out = capsys.readouterr().out
    assert "env" in out


def test_cmd_list_shows_labels(tmp_path, capsys):
    args_set = make_args(str(tmp_path), run_id="run-1", key="team", value="data")
    cmd_set(args_set)
    capsys.readouterr()  # flush
    args_ls = make_args(str(tmp_path), run_id="run-1")
    cmd_list(args_ls)
    out = capsys.readouterr().out
    assert "team" in out
    assert "data" in out


def test_cmd_list_no_labels(tmp_path, capsys):
    args = make_args(str(tmp_path), run_id="run-missing")
    cmd_list(args)
    out = capsys.readouterr().out
    assert "No labels" in out


def test_cmd_filter_by_key(tmp_path, capsys):
    for rid in ["run-1", "run-2"]:
        cmd_set(make_args(str(tmp_path), run_id=rid, key="env", value="prod"))
    capsys.readouterr()
    args = make_args(str(tmp_path), key="env", value=None)
    cmd_filter(args)
    out = capsys.readouterr().out
    assert "run-1" in out
    assert "run-2" in out


def test_cmd_filter_no_match(tmp_path, capsys):
    args = make_args(str(tmp_path), key="nonexistent", value=None)
    cmd_filter(args)
    out = capsys.readouterr().out
    assert "No runs" in out
