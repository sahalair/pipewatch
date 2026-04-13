"""Tests for pipewatch.cli_badges."""

from __future__ import annotations

import json
from types import SimpleNamespace
from unittest.mock import patch

import pytest

from pipewatch.cli_badges import cmd_score, cmd_status, cmd_flakiness, cmd_all, build_parser


def make_args(**kwargs) -> SimpleNamespace:
    defaults = {"pipeline": "etl", "base_dir": ".", "json": False}
    defaults.update(kwargs)
    return SimpleNamespace(**defaults)


_SCORE_BADGE = {"label": "score", "message": "88 (B)", "color": "green"}
_STATUS_BADGE = {"label": "status", "message": "ok", "color": "brightgreen"}
_FLAKY_BADGE = {"label": "flakiness", "message": "stable (0.02)", "color": "brightgreen"}


def test_cmd_score_prints_text(capsys):
    with patch("pipewatch.cli_badges.build_score_badge", return_value=_SCORE_BADGE):
        cmd_score(make_args())
    out = capsys.readouterr().out
    assert "score" in out
    assert "88" in out


def test_cmd_score_json_output(capsys):
    with patch("pipewatch.cli_badges.build_score_badge", return_value=_SCORE_BADGE):
        cmd_score(make_args(json=True))
    out = capsys.readouterr().out
    data = json.loads(out)
    assert data["label"] == "score"


def test_cmd_status_prints_text(capsys):
    with patch("pipewatch.cli_badges.build_status_badge", return_value=_STATUS_BADGE):
        cmd_status(make_args())
    out = capsys.readouterr().out
    assert "status" in out
    assert "ok" in out


def test_cmd_flakiness_prints_text(capsys):
    with patch("pipewatch.cli_badges.build_flakiness_badge", return_value=_FLAKY_BADGE):
        cmd_flakiness(make_args())
    out = capsys.readouterr().out
    assert "flakiness" in out


def test_cmd_all_prints_three_lines(capsys):
    with patch("pipewatch.cli_badges.build_score_badge", return_value=_SCORE_BADGE), \
         patch("pipewatch.cli_badges.build_status_badge", return_value=_STATUS_BADGE), \
         patch("pipewatch.cli_badges.build_flakiness_badge", return_value=_FLAKY_BADGE):
        cmd_all(make_args())
    lines = [l for l in capsys.readouterr().out.strip().splitlines() if l]
    assert len(lines) == 3


def test_cmd_all_json_output(capsys):
    with patch("pipewatch.cli_badges.build_score_badge", return_value=_SCORE_BADGE), \
         patch("pipewatch.cli_badges.build_status_badge", return_value=_STATUS_BADGE), \
         patch("pipewatch.cli_badges.build_flakiness_badge", return_value=_FLAKY_BADGE):
        cmd_all(make_args(json=True))
    data = json.loads(capsys.readouterr().out)
    assert set(data.keys()) == {"score", "status", "flakiness"}


def test_build_parser_returns_parser():
    parser = build_parser()
    assert parser is not None


def test_build_parser_subcommands_exist():
    parser = build_parser()
    args = parser.parse_args(["score", "my-pipeline"])
    assert args.pipeline == "my-pipeline"
