"""Tests for pipewatch.cli_saturation."""

import json
from unittest.mock import patch, MagicMock

from pipewatch.cli_saturation import cmd_show, cmd_list, build_parser


class Args:
    def __init__(self, **kwargs):
        self.__dict__.update(kwargs)


def make_args(**kwargs):
    defaults = {"base_dir": ".", "window": "day", "json": False}
    defaults.update(kwargs)
    return Args(**defaults)


FAKE_RESULT = {
    "pipeline": "pipe-a",
    "actual": 8,
    "capacity": 10,
    "saturation": 0.8,
    "grade": "HIGH",
    "warning": None,
}

FAKE_LIST = [
    {"pipeline": "pipe-a", "actual": 8, "capacity": 10, "saturation": 0.8, "grade": "HIGH", "warning": None},
    {"pipeline": "pipe-b", "actual": 6, "capacity": 5, "saturation": 1.2, "grade": "OVER_CAPACITY", "warning": "Over capacity!"},
]


def test_cmd_show_prints_formatted(capsys):
    with patch("pipewatch.cli_saturation.compute_saturation", return_value=FAKE_RESULT):
        with patch("pipewatch.cli_saturation.format_saturation_report", return_value="report text"):
            cmd_show(make_args(pipeline="pipe-a"))
    out = capsys.readouterr().out
    assert "report text" in out


def test_cmd_show_json_output(capsys):
    with patch("pipewatch.cli_saturation.compute_saturation", return_value=FAKE_RESULT):
        cmd_show(make_args(pipeline="pipe-a", json=True))
    out = capsys.readouterr().out
    data = json.loads(out)
    assert data["pipeline"] == "pipe-a"
    assert data["grade"] == "HIGH"


def test_cmd_list_empty(capsys):
    with patch("pipewatch.cli_saturation.saturation_for_all_pipelines", return_value=[]):
        cmd_list(make_args())
    out = capsys.readouterr().out
    assert "No capacity rules" in out


def test_cmd_list_shows_pipelines(capsys):
    with patch("pipewatch.cli_saturation.saturation_for_all_pipelines", return_value=FAKE_LIST):
        cmd_list(make_args())
    out = capsys.readouterr().out
    assert "pipe-a" in out
    assert "pipe-b" in out
    assert "OVER_CAPACITY" in out


def test_cmd_list_json_output(capsys):
    with patch("pipewatch.cli_saturation.saturation_for_all_pipelines", return_value=FAKE_LIST):
        cmd_list(make_args(json=True))
    out = capsys.readouterr().out
    data = json.loads(out)
    assert len(data) == 2


def test_build_parser_standalone():
    parser = build_parser()
    assert parser is not None


def test_build_parser_subcommands():
    parser = build_parser()
    args = parser.parse_args(["show", "pipe-a"])
    assert args.pipeline == "pipe-a"
