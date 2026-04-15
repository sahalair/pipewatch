"""Tests for pipewatch.cli_volatility."""

import json
from unittest.mock import patch

import pytest

from pipewatch.cli_volatility import cmd_show, cmd_list, build_parser


class Args:
    def __init__(self, **kwargs):
        self.__dict__.update(kwargs)


def make_args(**kwargs):
    defaults = {"base_dir": ".", "limit": 20, "json": False}
    defaults.update(kwargs)
    return Args(**defaults)


_SAMPLE_RESULT = {
    "pipeline": "my_pipe",
    "sample_count": 5,
    "mean_duration": 10.0,
    "stddev_duration": 1.0,
    "coefficient_of_variation": 0.1,
    "grade": "stable",
}


def test_cmd_show_prints_formatted(capsys):
    args = make_args(pipeline="my_pipe")
    with patch("pipewatch.cli_volatility.compute_volatility", return_value=_SAMPLE_RESULT):
        with patch("pipewatch.cli_volatility.format_volatility_report", return_value="REPORT"):
            cmd_show(args)
    out = capsys.readouterr().out
    assert "REPORT" in out


def test_cmd_show_json_output(capsys):
    args = make_args(pipeline="my_pipe", json=True)
    with patch("pipewatch.cli_volatility.compute_volatility", return_value=_SAMPLE_RESULT):
        cmd_show(args)
    out = capsys.readouterr().out
    data = json.loads(out)
    assert data["pipeline"] == "my_pipe"


def test_cmd_list_empty(capsys):
    args = make_args()
    with patch("pipewatch.cli_volatility.volatility_for_all_pipelines", return_value=[]):
        cmd_list(args)
    out = capsys.readouterr().out
    assert "No pipelines" in out


def test_cmd_list_prints_table(capsys):
    results = [_SAMPLE_RESULT]
    args = make_args()
    with patch("pipewatch.cli_volatility.volatility_for_all_pipelines", return_value=results):
        cmd_list(args)
    out = capsys.readouterr().out
    assert "my_pipe" in out
    assert "stable" in out


def test_cmd_list_json(capsys):
    results = [_SAMPLE_RESULT]
    args = make_args(json=True)
    with patch("pipewatch.cli_volatility.volatility_for_all_pipelines", return_value=results):
        cmd_list(args)
    data = json.loads(capsys.readouterr().out)
    assert isinstance(data, list)
    assert data[0]["pipeline"] == "my_pipe"


def test_build_parser_show_subcommand():
    parser = build_parser()
    args = parser.parse_args(["show", "pipe_x"])
    assert args.pipeline == "pipe_x"
    assert args.func == cmd_show


def test_build_parser_list_subcommand():
    parser = build_parser()
    args = parser.parse_args(["list"])
    assert args.func == cmd_list


def test_main_no_subcommand_exits():
    import sys
    from pipewatch.cli_volatility import main
    with patch("sys.argv", ["cli_volatility"]):
        with pytest.raises(SystemExit):
            main()
