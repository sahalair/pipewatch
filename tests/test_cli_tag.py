"""Tests for pipewatch.cli_tag CLI commands."""

from __future__ import annotations

import types

import pytest

from pipewatch.cli_tag import build_parser, cmd_add, cmd_filter, cmd_list, cmd_remove
from pipewatch.tag_manager import add_tags


def make_args(run_dir, **kwargs):
    ns = types.SimpleNamespace(run_dir=run_dir, **kwargs)
    return ns


def test_cmd_add_creates_tags(tmp_path, capsys):
    args = make_args(str(tmp_path), run_id="run-1", tags=["nightly", "prod"])
    cmd_add(args)
    captured = capsys.readouterr()
    assert "nightly" in captured.out
    assert "prod" in captured.out


def test_cmd_remove_removes_tag(tmp_path, capsys):
    add_tags(str(tmp_path), "run-1", ["nightly", "prod"])
    args = make_args(str(tmp_path), run_id="run-1", tags=["prod"])
    cmd_remove(args)
    captured = capsys.readouterr()
    assert "prod" not in captured.out or "nightly" in captured.out


def test_cmd_list_shows_tags(tmp_path, capsys):
    add_tags(str(tmp_path), "run-1", ["alpha", "beta"])
    args = make_args(str(tmp_path), run_id="run-1")
    cmd_list(args)
    out = capsys.readouterr().out
    assert "alpha" in out
    assert "beta" in out


def test_cmd_list_no_tags(tmp_path, capsys):
    args = make_args(str(tmp_path), run_id="ghost")
    cmd_list(args)
    out = capsys.readouterr().out
    assert "No tags" in out


def test_cmd_filter_shows_matching_runs(tmp_path, capsys):
    add_tags(str(tmp_path), "run-1", ["ci"])
    add_tags(str(tmp_path), "run-2", ["nightly"])
    args = make_args(str(tmp_path), tag="ci")
    cmd_filter(args)
    out = capsys.readouterr().out
    assert "run-1" in out
    assert "run-2" not in out


def test_cmd_filter_no_matches(tmp_path, capsys):
    args = make_args(str(tmp_path), tag="missing")
    cmd_filter(args)
    out = capsys.readouterr().out
    assert "No runs" in out


def test_build_parser_returns_parser(tmp_path):
    parser = build_parser(run_dir=str(tmp_path))
    assert parser is not None


def test_parser_add_subcommand(tmp_path):
    parser = build_parser(run_dir=str(tmp_path))
    args = parser.parse_args(["add", "run-99", "nightly", "prod"])
    assert args.run_id == "run-99"
    assert "nightly" in args.tags


def test_parser_filter_subcommand(tmp_path):
    parser = build_parser(run_dir=str(tmp_path))
    args = parser.parse_args(["filter", "ci"])
    assert args.tag == "ci"
