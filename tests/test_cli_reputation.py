"""Tests for pipewatch.cli_reputation."""

import json
import pytest
from unittest.mock import patch, MagicMock

from pipewatch.cli_reputation import cmd_show, cmd_list, build_parser


_SAMPLE_REP = {
    "pipeline": "alpha",
    "score": 92.0,
    "grade": "excellent",
    "flakiness_score": 0.0,
    "flakiness_label": "stable",
    "stability_score": 100.0,
    "stability_grade": "A",
    "run_count": 10,
}


def make_args(**kwargs):
    class Args:
        pass
    a = Args()
    defaults = {"pipeline": "alpha", "pipelines": ["alpha"], "limit": 20,
                "json": False, "base_dir": "."}
    defaults.update(kwargs)
    for k, v in defaults.items():
        setattr(a, k, v)
    return a


def test_cmd_show_prints_formatted(capsys):
    with patch("pipewatch.cli_reputation.compute_reputation", return_value=_SAMPLE_REP):
        with patch("pipewatch.cli_reputation.format_reputation_report", return_value="REPORT") as fmt:
            cmd_show(make_args())
            fmt.assert_called_once()
    out = capsys.readouterr().out
    assert "REPORT" in out


def test_cmd_show_json(capsys):
    with patch("pipewatch.cli_reputation.compute_reputation", return_value=_SAMPLE_REP):
        cmd_show(make_args(json=True))
    out = capsys.readouterr().out
    data = json.loads(out)
    assert data["pipeline"] == "alpha"
    assert data["grade"] == "excellent"


def test_cmd_list_empty_pipelines(capsys):
    cmd_list(make_args(pipelines=[]))
    out = capsys.readouterr().out
    assert "No pipelines" in out


def test_cmd_list_prints_table(capsys):
    with patch("pipewatch.cli_reputation.compute_reputation", return_value=_SAMPLE_REP):
        cmd_list(make_args(pipelines=["alpha", "beta"]))
    out = capsys.readouterr().out
    assert "alpha" in out


def test_cmd_list_json(capsys):
    rep_beta = dict(_SAMPLE_REP, pipeline="beta")
    side = [_SAMPLE_REP, rep_beta]
    with patch("pipewatch.cli_reputation.compute_reputation", side_effect=side):
        cmd_list(make_args(pipelines=["alpha", "beta"], json=True))
    out = capsys.readouterr().out
    data = json.loads(out)
    assert len(data) == 2


def test_build_parser_returns_parser():
    parser = build_parser()
    assert parser is not None


def test_build_parser_show_subcommand():
    parser = build_parser()
    args = parser.parse_args(["show", "my_pipeline", "--limit", "5"])
    assert args.pipeline == "my_pipeline"
    assert args.limit == 5
