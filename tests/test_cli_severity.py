"""Tests for pipewatch.cli_severity."""

import json
import pytest
from unittest.mock import patch
from pipewatch.cli_severity import cmd_show, cmd_list, cmd_summary, build_parser


def make_args(**kwargs):
    class Args:
        pass
    a = Args()
    defaults = {"run_id": None, "pipeline": None, "level": None, "json": False}
    defaults.update(kwargs)
    for k, v in defaults.items():
        setattr(a, k, v)
    return a


FAKE_RUNS = [
    {"run_id": "run-001", "pipeline": "etl", "status": "ok", "exit_code": 0},
    {"run_id": "run-002", "pipeline": "etl", "status": "failed", "exit_code": 1},
    {"run_id": "run-003", "pipeline": "ingest", "status": "failed", "exit_code": 2},
]


@patch("pipewatch.cli_severity.list_run_records", return_value=FAKE_RUNS)
def test_cmd_show_prints_severity(mock_list, capsys):
    cmd_show(make_args(run_id="run-001"))
    out = capsys.readouterr().out
    assert "run-001" in out
    assert "ok" in out


@patch("pipewatch.cli_severity.list_run_records", return_value=FAKE_RUNS)
def test_cmd_show_json(mock_list, capsys):
    cmd_show(make_args(run_id="run-002", json=True))
    out = capsys.readouterr().out
    data = json.loads(out)
    assert data["run_id"] == "run-002"
    assert data["severity"] == "high"


@patch("pipewatch.cli_severity.list_run_records", return_value=FAKE_RUNS)
def test_cmd_show_not_found_exits(mock_list):
    with pytest.raises(SystemExit):
        cmd_show(make_args(run_id="nonexistent"))


@patch("pipewatch.cli_severity.list_run_records", return_value=FAKE_RUNS)
def test_cmd_list_all(mock_list, capsys):
    cmd_list(make_args())
    out = capsys.readouterr().out
    assert "run-001" in out
    assert "run-002" in out
    assert "run-003" in out


@patch("pipewatch.cli_severity.list_run_records", return_value=FAKE_RUNS)
def test_cmd_list_filter_pipeline(mock_list, capsys):
    cmd_list(make_args(pipeline="ingest"))
    out = capsys.readouterr().out
    assert "run-003" in out
    assert "run-001" not in out


@patch("pipewatch.cli_severity.list_run_records", return_value=FAKE_RUNS)
def test_cmd_list_filter_level(mock_list, capsys):
    cmd_list(make_args(level="critical"))
    out = capsys.readouterr().out
    assert "run-003" in out
    assert "run-001" not in out


@patch("pipewatch.cli_severity.list_run_records", return_value=FAKE_RUNS)
def test_cmd_list_json(mock_list, capsys):
    cmd_list(make_args(json=True))
    out = capsys.readouterr().out
    data = json.loads(out)
    assert isinstance(data, list)
    assert len(data) == 3


@patch("pipewatch.cli_severity.list_run_records", return_value=FAKE_RUNS)
def test_cmd_summary_prints_counts(mock_list, capsys):
    cmd_summary(make_args())
    out = capsys.readouterr().out
    assert "Severity" in out or "ok" in out.lower() or "CRIT" in out


@patch("pipewatch.cli_severity.list_run_records", return_value=FAKE_RUNS)
def test_cmd_summary_json(mock_list, capsys):
    cmd_summary(make_args(json=True))
    out = capsys.readouterr().out
    data = json.loads(out)
    assert "ok" in data
    assert "critical" in data


def test_build_parser_returns_parser():
    parser = build_parser()
    assert parser is not None
