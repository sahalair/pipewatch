"""Tests for cli_longevity."""
import json
import pytest
from unittest.mock import patch, MagicMock
from pipewatch.cli_longevity import cmd_show, cmd_list, build_parser


class Args:
    def __init__(self, **kwargs):
        self.__dict__.update(kwargs)


def make_args(**kwargs):
    defaults = {"base_dir": ".", "json": False}
    defaults.update(kwargs)
    return Args(**defaults)


_LONGEVITY = "pipewatch.cli_longevity.compute_longevity"
_LIST = "pipewatch.cli_longevity.list_run_records"
_FORMAT = "pipewatch.cli_longevity.format_longevity_report"

_SAMPLE = {
    "pipeline": "p",
    "run_count": 5,
    "first_run": "2024-01-01T00:00:00+00:00",
    "last_run": "2024-06-01T00:00:00+00:00",
    "age_days": 152,
    "active_days": 30,
    "activity_ratio": 0.197,
    "grade": "D",
}


def test_cmd_show_prints_formatted(capsys):
    with patch(_LONGEVITY, return_value=_SAMPLE), patch(_FORMAT, return_value="REPORT"):
        cmd_show(make_args(pipeline="p"))
    out = capsys.readouterr().out
    assert "REPORT" in out


def test_cmd_show_json_output(capsys):
    with patch(_LONGEVITY, return_value=_SAMPLE):
        cmd_show(make_args(pipeline="p", json=True))
    out = capsys.readouterr().out
    data = json.loads(out)
    assert data["pipeline"] == "p"


def test_cmd_list_empty(capsys):
    with patch(_LIST, return_value=[]):
        cmd_list(make_args())
    out = capsys.readouterr().out
    assert "No pipelines" in out


def test_cmd_list_shows_all_pipelines(capsys):
    runs = [
        {"pipeline": "a", "started": "2024-01-01T00:00:00"},
        {"pipeline": "b", "started": "2024-01-02T00:00:00"},
    ]
    with patch(_LIST, return_value=runs), patch(_LONGEVITY, return_value=_SAMPLE), patch(_FORMAT, return_value="REPORT"):
        cmd_list(make_args())
    out = capsys.readouterr().out
    assert out.count("REPORT") == 2


def test_cmd_list_json_output(capsys):
    runs = [{"pipeline": "a", "started": "2024-01-01T00:00:00"}]
    with patch(_LIST, return_value=runs), patch(_LONGEVITY, return_value=_SAMPLE):
        cmd_list(make_args(json=True))
    out = capsys.readouterr().out
    data = json.loads(out)
    assert isinstance(data, list)


def test_build_parser_returns_parser():
    parser = build_parser()
    assert parser is not None


def test_build_parser_show_subcommand():
    parser = build_parser()
    args = parser.parse_args(["show", "mypipe"])
    assert args.pipeline == "mypipe"
