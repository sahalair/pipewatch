"""Tests for pipewatch.cli_reliability."""

import json
from unittest.mock import patch
import pytest

from pipewatch.cli_reliability import cmd_show, cmd_list, build_parser


class Args:
    def __init__(self, **kwargs):
        self.__dict__.update(kwargs)


def make_args(**kwargs):
    defaults = dict(base_dir=".pipewatch", limit=50, json=False)
    defaults.update(kwargs)
    return Args(**defaults)


_SAMPLE_REPORT = {
    "pipeline": "etl",
    "score": 0.88,
    "grade": "B",
    "uptime_component": 0.90,
    "flakiness_component": 0.85,
    "sla_component": 1.0,
}


def test_cmd_show_prints_formatted(capsys):
    with patch("pipewatch.cli_reliability.compute_reliability", return_value=_SAMPLE_REPORT):
        with patch("pipewatch.cli_reliability.format_reliability_report", return_value="REPORT"):
            cmd_show(make_args(pipeline="etl"))
    out = capsys.readouterr().out
    assert "REPORT" in out


def test_cmd_show_json_output(capsys):
    with patch("pipewatch.cli_reliability.compute_reliability", return_value=_SAMPLE_REPORT):
        cmd_show(make_args(pipeline="etl", json=True))
    out = capsys.readouterr().out
    data = json.loads(out)
    assert data["pipeline"] == "etl"


def test_cmd_show_error_exits(capsys):
    with patch("pipewatch.cli_reliability.compute_reliability", side_effect=ValueError("boom")):
        with pytest.raises(SystemExit):
            cmd_show(make_args(pipeline="etl"))


def test_cmd_list_empty_pipelines(capsys):
    with patch("pipewatch.cli_reliability.list_run_records", return_value=[]):
        cmd_list(make_args())
    out = capsys.readouterr().out
    assert "No pipelines" in out


def test_cmd_list_shows_pipelines(capsys):
    records = [{"pipeline": "etl"}, {"pipeline": "ingest"}]
    reports = [
        {"pipeline": "etl", "score": 0.90, "grade": "A",
         "uptime_component": 1.0, "flakiness_component": 1.0, "sla_component": 1.0},
        {"pipeline": "ingest", "score": 0.70, "grade": "C",
         "uptime_component": 0.7, "flakiness_component": 0.7, "sla_component": 0.7},
    ]
    with patch("pipewatch.cli_reliability.list_run_records", return_value=records):
        with patch("pipewatch.cli_reliability.compute_reliability", side_effect=reports):
            cmd_list(make_args())
    out = capsys.readouterr().out
    assert "etl" in out
    assert "ingest" in out


def test_build_parser_standalone():
    parser = build_parser()
    assert parser is not None
