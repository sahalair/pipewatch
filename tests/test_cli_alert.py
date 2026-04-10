"""Tests for pipewatch.cli_alert CLI commands."""

from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import patch

import pytest

from pipewatch.cli_alert import build_parser, cmd_add, cmd_list, cmd_check
from pipewatch.alert import save_alert_rules, create_alert_rule


def make_args(**kwargs):
    """Build a simple namespace for CLI args."""
    import argparse
    defaults = {
        "alerts_file": ".pipewatch_alerts.json",
        "snapshots_dir": ".pipewatch_snapshots",
    }
    defaults.update(kwargs)
    return argparse.Namespace(**defaults)


def test_cmd_add_creates_rule(tmp_path):
    alerts_file = str(tmp_path / "alerts.json")
    args = make_args(
        alerts_file=alerts_file,
        name="test-rule",
        snapshot_key="output.txt",
        threshold=0.1,
        notify="stdout",
    )
    cmd_add(args)
    rules = json.loads(Path(alerts_file).read_text())
    assert len(rules) == 1
    assert rules[0]["name"] == "test-rule"
    assert rules[0]["snapshot_key"] == "output.txt"


def test_cmd_list_empty(tmp_path, capsys):
    alerts_file = str(tmp_path / "alerts.json")
    args = make_args(alerts_file=alerts_file)
    cmd_list(args)
    captured = capsys.readouterr()
    assert "No alert rules" in captured.out


def test_cmd_list_shows_rules(tmp_path, capsys):
    alerts_file = str(tmp_path / "alerts.json")
    rules = [create_alert_rule("rule-a", "snap-a", threshold=0.3)]
    save_alert_rules(rules, alerts_file)
    args = make_args(alerts_file=alerts_file)
    cmd_list(args)
    captured = capsys.readouterr()
    assert "rule-a" in captured.out
    assert "snap-a" in captured.out


def test_cmd_check_no_rules(tmp_path, capsys):
    alerts_file = str(tmp_path / "alerts.json")
    args = make_args(alerts_file=alerts_file)
    cmd_check(args)
    captured = capsys.readouterr()
    assert "No alert rules" in captured.out


def test_cmd_check_skips_missing_snapshot(tmp_path, capsys):
    alerts_file = str(tmp_path / "alerts.json")
    rules = [create_alert_rule("r", "missing-key")]
    save_alert_rules(rules, alerts_file)
    snapshots_dir = str(tmp_path / "snaps")
    args = make_args(alerts_file=alerts_file, snapshots_dir=snapshots_dir)
    cmd_check(args)
    captured = capsys.readouterr()
    assert "SKIP" in captured.out


def test_build_parser_returns_parser():
    parser = build_parser()
    assert parser is not None
    args = parser.parse_args(["add", "my-rule", "snap-key"])
    assert args.name == "my-rule"
    assert args.snapshot_key == "snap-key"
    assert args.threshold == 0.0
