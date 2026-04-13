"""Tests for pipewatch.cli_scoring."""

from __future__ import annotations

import json
import sys
from types import SimpleNamespace
from unittest.mock import patch

import pytest

from pipewatch.cli_scoring import cmd_show, cmd_list, build_parser


_RUN = {
    "run_id": "abc-123",
    "pipeline": "etl",
    "status": "success",
    "exit_code": 0,
}

_SCORE = {
    "run_id": "abc-123",
    "pipeline": "etl",
    "score": 95.0,
    "grade": "A",
    "components": {"severity": 100.0, "flakiness": 100.0, "duration": 100.0, "sla": 100.0},
}


def make_args(**kwargs):
    defaults = {"base_dir": ".", "json": False, "pipeline": None}
    defaults.update(kwargs)
    return SimpleNamespace(**defaults)


def test_cmd_show_prints_formatted(capsys):
    args = make_args(run_id="abc-123")
    with patch("pipewatch.cli_scoring.load_run_record", return_value=_RUN), \
         patch("pipewatch.cli_scoring.compute_score", return_value=_SCORE):
        cmd_show(args)
    out = capsys.readouterr().out
    assert "abc-123" in out


def test_cmd_show_json(capsys):
    args = make_args(run_id="abc-123", json=True)
    with patch("pipewatch.cli_scoring.load_run_record", return_value=_RUN), \
         patch("pipewatch.cli_scoring.compute_score", return_value=_SCORE):
        cmd_show(args)
    out = capsys.readouterr().out
    data = json.loads(out)
    assert data["run_id"] == "abc-123"


def test_cmd_show_not_found_exits():
    args = make_args(run_id="missing")
    with patch("pipewatch.cli_scoring.load_run_record", return_value=None):
        with pytest.raises(SystemExit):
            cmd_show(args)


def test_cmd_list_empty(capsys):
    args = make_args()
    with patch("pipewatch.cli_scoring.list_run_records", return_value=[]):
        cmd_list(args)
    out = capsys.readouterr().out
    assert "No runs" in out


def test_cmd_list_shows_runs(capsys):
    args = make_args()
    with patch("pipewatch.cli_scoring.list_run_records", return_value=[_RUN]), \
         patch("pipewatch.cli_scoring.compute_score", return_value=_SCORE):
        cmd_list(args)
    out = capsys.readouterr().out
    assert "abc-123" in out
    assert "etl" in out


def test_cmd_list_json(capsys):
    args = make_args(json=True)
    with patch("pipewatch.cli_scoring.list_run_records", return_value=[_RUN]), \
         patch("pipewatch.cli_scoring.compute_score", return_value=_SCORE):
        cmd_list(args)
    out = capsys.readouterr().out
    data = json.loads(out)
    assert isinstance(data, list)
    assert data[0]["run_id"] == "abc-123"


def test_cmd_list_filters_by_pipeline(capsys):
    run2 = {**_RUN, "run_id": "xyz-999", "pipeline": "other"}
    args = make_args(pipeline="etl")
    with patch("pipewatch.cli_scoring.list_run_records", return_value=[_RUN, run2]), \
         patch("pipewatch.cli_scoring.compute_score", return_value=_SCORE):
        cmd_list(args)
    out = capsys.readouterr().out
    assert "xyz-999" not in out


def test_build_parser_standalone():
    parser = build_parser()
    args = parser.parse_args(["show", "abc-123"])
    assert args.run_id == "abc-123"
    assert args.command == "show"
