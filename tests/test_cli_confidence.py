"""Tests for pipewatch.cli_confidence."""

import json
import pytest
from unittest.mock import patch

from pipewatch.cli_confidence import cmd_show, cmd_list, build_parser


class Args:
    def __init__(self, **kwargs):
        self.__dict__.update(kwargs)


def make_args(**kwargs):
    defaults = {"base_dir": ".pipewatch", "json": False}
    defaults.update(kwargs)
    return Args(**defaults)


_FAKE_RESULT = {
    "pipeline": "etl",
    "score": 88.5,
    "grade": "B",
    "components": {"uptime": 90.0, "flakiness": 85.0, "volume": 80.0},
}


def test_cmd_show_prints_formatted(capsys):
    with patch(
        "pipewatch.cli_confidence.compute_confidence",
        return_value=_FAKE_RESULT,
    ):
        cmd_show(make_args(pipeline="etl"))
    out = capsys.readouterr().out
    assert "etl" in out
    assert "88.5" in out


def test_cmd_show_json_output(capsys):
    with patch(
        "pipewatch.cli_confidence.compute_confidence",
        return_value=_FAKE_RESULT,
    ):
        cmd_show(make_args(pipeline="etl", json=True))
    out = capsys.readouterr().out
    data = json.loads(out)
    assert data["pipeline"] == "etl"
    assert data["grade"] == "B"


def test_cmd_list_empty_prints_message(capsys):
    with patch(
        "pipewatch.cli_confidence.list_run_records", return_value=[]
    ):
        cmd_list(make_args())
    out = capsys.readouterr().out
    assert "No pipelines" in out


def test_cmd_list_shows_pipelines(capsys):
    fake_records = [{"pipeline": "etl"}, {"pipeline": "ingest"}]
    with patch(
        "pipewatch.cli_confidence.list_run_records",
        return_value=fake_records,
    ), patch(
        "pipewatch.cli_confidence.compute_confidence",
        side_effect=lambda p, **kw: {
            "pipeline": p,
            "score": 75.0,
            "grade": "B",
            "components": {},
        },
    ):
        cmd_list(make_args())
    out = capsys.readouterr().out
    assert "etl" in out
    assert "ingest" in out


def test_cmd_list_json_output(capsys):
    fake_records = [{"pipeline": "etl"}]
    with patch(
        "pipewatch.cli_confidence.list_run_records",
        return_value=fake_records,
    ), patch(
        "pipewatch.cli_confidence.compute_confidence",
        return_value=_FAKE_RESULT,
    ):
        cmd_list(make_args(json=True))
    out = capsys.readouterr().out
    data = json.loads(out)
    assert isinstance(data, list)
    assert data[0]["pipeline"] == "etl"


def test_build_parser_standalone():
    parser = build_parser()
    assert parser is not None
