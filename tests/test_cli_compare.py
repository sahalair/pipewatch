"""Tests for pipewatch.cli_compare."""

from __future__ import annotations

import json
import sys
import pytest
from unittest.mock import patch, MagicMock

from pipewatch.cli_compare import cmd_compare, build_parser


RUN_A = "run-aaa"
RUN_B = "run-bbb"

_COMPARISON = {
    "run_a": RUN_A,
    "run_b": RUN_B,
    "status": {"run_a": "success", "run_b": "success", "changed": False},
    "pipeline": {"run_a": "etl", "run_b": "etl"},
    "metrics": {},
    "tags": {"only_in_a": [], "only_in_b": [], "common": []},
    "annotations": {"run_a": {}, "run_b": {}},
}


def make_args(**kwargs):
    defaults = {"run_a": RUN_A, "run_b": RUN_B, "data_dir": ".", "json": False}
    defaults.update(kwargs)
    ns = MagicMock()
    for k, v in defaults.items():
        setattr(ns, k, v)
    return ns


def test_cmd_compare_prints_formatted(capsys):
    with patch("pipewatch.cli_compare.compare_runs", return_value=_COMPARISON), \
         patch("pipewatch.cli_compare.format_run_comparison", return_value="REPORT"):
        cmd_compare(make_args())
    out = capsys.readouterr().out
    assert "REPORT" in out


def test_cmd_compare_json_output(capsys):
    with patch("pipewatch.cli_compare.compare_runs", return_value=_COMPARISON):
        cmd_compare(make_args(json=True))
    out = capsys.readouterr().out
    parsed = json.loads(out)
    assert parsed["run_a"] == RUN_A


def test_cmd_compare_not_found_exits(capsys):
    with patch("pipewatch.cli_compare.compare_runs",
               side_effect=FileNotFoundError("missing")):
        with pytest.raises(SystemExit) as exc_info:
            cmd_compare(make_args())
    assert exc_info.value.code == 1
    err = capsys.readouterr().err
    assert "Error" in err


def test_build_parser_standalone():
    parser = build_parser()
    args = parser.parse_args([RUN_A, RUN_B])
    assert args.run_a == RUN_A
    assert args.run_b == RUN_B
    assert args.json is False


def test_build_parser_json_flag():
    parser = build_parser()
    args = parser.parse_args([RUN_A, RUN_B, "--json"])
    assert args.json is True


def test_build_parser_data_dir():
    parser = build_parser()
    args = parser.parse_args([RUN_A, RUN_B, "--data-dir", "/tmp/pw"])
    assert args.data_dir == "/tmp/pw"
