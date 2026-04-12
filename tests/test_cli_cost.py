"""Tests for pipewatch.cli_cost."""

import sys
import pytest
from unittest.mock import patch, MagicMock

from pipewatch import run_cost
import pipewatch.cli_cost as cli_cost


def make_args(**kwargs):
    class Args:
        pass
    a = Args()
    defaults = {"run_id": "r1", "pipeline": "etl", "amount": 1.0,
                "currency": "USD", "unit": None, "notes": None, "json": False}
    defaults.update(kwargs)
    for k, v in defaults.items():
        setattr(a, k, v)
    return a


@pytest.fixture(autouse=True)
def _tmp_costs(tmp_path, monkeypatch):
    costs_file = tmp_path / ".pipewatch" / "run_costs.json"
    monkeypatch.setattr(run_cost, "_COSTS_FILE", str(costs_file))
    yield


def test_cmd_record_prints_confirmation(capsys):
    args = make_args(run_id="r1", pipeline="etl", amount=0.5)
    cli_cost.cmd_record(args)
    out = capsys.readouterr().out
    assert "r1" in out
    assert "0.5" in out


def test_cmd_get_prints_details(capsys):
    run_cost.record_cost("r2", "etl", 2.5, unit="cpu-hour")
    args = make_args(run_id="r2")
    cli_cost.cmd_get(args)
    out = capsys.readouterr().out
    assert "r2" in out
    assert "2.5" in out
    assert "cpu-hour" in out


def test_cmd_get_missing_exits(capsys):
    args = make_args(run_id="missing")
    with pytest.raises(SystemExit):
        cli_cost.cmd_get(args)


def test_cmd_get_json_output(capsys):
    import json
    run_cost.record_cost("r3", "etl", 1.1)
    args = make_args(run_id="r3", json=True)
    cli_cost.cmd_get(args)
    out = capsys.readouterr().out
    data = json.loads(out)
    assert data["run_id"] == "r3"


def test_cmd_list_empty(capsys):
    args = make_args(pipeline="nopipe")
    cli_cost.cmd_list(args)
    out = capsys.readouterr().out
    assert "No cost records" in out


def test_cmd_list_shows_entries(capsys):
    run_cost.record_cost("r4", "etl", 0.99)
    args = make_args(pipeline="etl")
    cli_cost.cmd_list(args)
    out = capsys.readouterr().out
    assert "r4" in out
    assert "0.99" in out


def test_cmd_summary_empty(capsys):
    args = make_args(pipeline="ghost")
    cli_cost.cmd_summary(args)
    out = capsys.readouterr().out
    assert "No cost records" in out


def test_cmd_summary_shows_totals(capsys):
    run_cost.record_cost("r5", "etl", 1.0)
    run_cost.record_cost("r6", "etl", 3.0)
    args = make_args(pipeline="etl")
    cli_cost.cmd_summary(args)
    out = capsys.readouterr().out
    assert "4.0" in out
    assert "2.0" in out
