"""Tests for pipewatch/cli_sla.py"""

import pytest
from unittest.mock import patch
from pathlib import Path
from pipewatch.cli_sla import cmd_set, cmd_remove, cmd_list, cmd_check, build_parser
from pipewatch.run_sla import set_sla, load_sla_rules


def make_args(**kwargs):
    class Args:
        pass
    a = Args()
    for k, v in kwargs.items():
        setattr(a, k, v)
    return a


@pytest.fixture
def sla_dir(tmp_path):
    return tmp_path / "sla"


def test_cmd_set_creates_rule(capsys, sla_dir):
    args = make_args(pipeline="etl", max_seconds=120.0, description="")
    cmd_set(args, base_dir=sla_dir)
    rules = load_sla_rules(base_dir=sla_dir)
    assert "etl" in rules
    out = capsys.readouterr().out
    assert "etl" in out


def test_cmd_set_prints_confirmation(capsys, sla_dir):
    args = make_args(pipeline="ingest", max_seconds=60.0, description="fast")
    cmd_set(args, base_dir=sla_dir)
    out = capsys.readouterr().out
    assert "60.0" in out


def test_cmd_remove_existing(capsys, sla_dir):
    set_sla("etl", 90.0, base_dir=sla_dir)
    args = make_args(pipeline="etl")
    cmd_remove(args, base_dir=sla_dir)
    out = capsys.readouterr().out
    assert "removed" in out
    assert load_sla_rules(base_dir=sla_dir) == {}


def test_cmd_remove_nonexistent(capsys, sla_dir):
    args = make_args(pipeline="ghost")
    cmd_remove(args, base_dir=sla_dir)
    out = capsys.readouterr().out
    assert "No SLA rule found" in out


def test_cmd_list_empty(capsys, sla_dir):
    args = make_args()
    cmd_list(args, base_dir=sla_dir)
    out = capsys.readouterr().out
    assert "No SLA rules" in out


def test_cmd_list_shows_rules(capsys, sla_dir):
    set_sla("etl", 120.0, description="nightly", base_dir=sla_dir)
    args = make_args()
    cmd_list(args, base_dir=sla_dir)
    out = capsys.readouterr().out
    assert "etl" in out
    assert "120" in out


def test_cmd_check_no_runs(capsys, sla_dir):
    with patch("pipewatch.cli_sla.get_durations_for_pipeline", return_value=[]):
        args = make_args(pipeline="etl", last=5, json=False)
        cmd_check(args, base_dir=sla_dir)
    out = capsys.readouterr().out
    assert "No completed runs" in out


def test_cmd_check_no_violation(capsys, sla_dir):
    set_sla("etl", 200.0, base_dir=sla_dir)
    fake_durations = [{"run_id": "abc123xyz", "duration_seconds": 90.0}]
    with patch("pipewatch.cli_sla.get_durations_for_pipeline", return_value=fake_durations):
        args = make_args(pipeline="etl", last=5, json=False)
        cmd_check(args, base_dir=sla_dir)
    out = capsys.readouterr().out
    assert "0/1" in out


def test_build_parser_returns_parser():
    parser = build_parser()
    assert parser is not None
    args = parser.parse_args(["set", "etl", "120"])
    assert args.pipeline == "etl"
    assert args.max_seconds == 120.0
