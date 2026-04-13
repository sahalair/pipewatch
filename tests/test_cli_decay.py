"""Tests for pipewatch/cli_decay.py"""

import json
from unittest.mock import patch

import pytest

from pipewatch.cli_decay import build_parser, cmd_list, cmd_show

_DECAY = "pipewatch.cli_decay.compute_decay_score"
_LIST_RECORDS = "pipewatch.cli_decay.list_run_records"
_FORMAT = "pipewatch.cli_decay.format_decay_report"


class Args:
    def __init__(self, **kwargs):
        self.__dict__.update(kwargs)


def make_args(**kwargs):
    defaults = {"pipeline": "pipe", "half_life": 24.0, "json": False}
    defaults.update(kwargs)
    return Args(**defaults)


_SAMPLE = {
    "pipeline": "pipe",
    "score": 0.9,
    "grade": "fresh",
    "last_success_iso": "2024-01-01T12:00:00",
    "hours_since_success": 2.0,
}


def test_cmd_show_prints_formatted(capsys):
    with patch(_DECAY, return_value=_SAMPLE), patch(_FORMAT, return_value="REPORT"):
        cmd_show(make_args())
    out = capsys.readouterr().out
    assert "REPORT" in out


def test_cmd_show_json_output(capsys):
    with patch(_DECAY, return_value=_SAMPLE):
        cmd_show(make_args(json=True))
    out = capsys.readouterr().out
    data = json.loads(out)
    assert data["pipeline"] == "pipe"


def test_cmd_show_invalid_half_life_exits(capsys):
    with patch(_DECAY, side_effect=ValueError("bad")):
        with pytest.raises(SystemExit) as exc:
            cmd_show(make_args(half_life=-1))
    assert exc.value.code == 1


def test_cmd_list_empty(capsys):
    with patch(_LIST_RECORDS, return_value=[]):
        cmd_list(make_args())
    out = capsys.readouterr().out
    assert "No pipelines" in out


def test_cmd_list_shows_pipelines(capsys):
    runs = [{"pipeline": "etl", "status": "ok"}]
    with patch(_LIST_RECORDS, return_value=runs), patch(_DECAY, return_value=_SAMPLE):
        cmd_list(make_args())
    out = capsys.readouterr().out
    assert "etl" in out


def test_cmd_list_json_output(capsys):
    runs = [{"pipeline": "etl", "status": "ok"}]
    with patch(_LIST_RECORDS, return_value=runs), patch(_DECAY, return_value={**_SAMPLE, "pipeline": "etl"}):
        cmd_list(make_args(json=True))
    out = capsys.readouterr().out
    data = json.loads(out)
    assert isinstance(data, list)
    assert data[0]["pipeline"] == "etl"


def test_build_parser_show_subcommand():
    parser = build_parser()
    args = parser.parse_args(["show", "my_pipe"])
    assert args.pipeline == "my_pipe"
    assert args.half_life == 24.0


def test_build_parser_list_subcommand():
    parser = build_parser()
    args = parser.parse_args(["--half-life", "12", "list"])
    assert args.half_life == 12.0
