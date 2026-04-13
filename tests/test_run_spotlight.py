"""Tests for run_spotlight.py and cli_spotlight.py."""

import json
import pytest
from unittest.mock import patch, MagicMock

from pipewatch.run_spotlight import spotlight_runs, format_spotlight_report
from pipewatch.cli_spotlight import cmd_show, build_parser


FAKE_RUNS = [
    {"run_id": "aaa111", "pipeline": "etl", "status": "success"},
    {"run_id": "bbb222", "pipeline": "etl", "status": "failed"},
    {"run_id": "ccc333", "pipeline": "ingest", "status": "success"},
]


def _patch_all(monkeypatch, runs=None):
    if runs is None:
        runs = FAKE_RUNS
    monkeypatch.setattr("pipewatch.run_spotlight.list_run_records", lambda **kw: runs)
    monkeypatch.setattr(
        "pipewatch.run_spotlight.compute_score",
        lambda run_id, **kw: {"score": 80.0, "grade": "B"},
    )
    monkeypatch.setattr(
        "pipewatch.run_spotlight.classify_severity",
        lambda run: "low",
    )
    monkeypatch.setattr(
        "pipewatch.run_spotlight.detect_anomaly",
        lambda run_id, metric, **kw: None,
    )


def test_spotlight_runs_returns_list(monkeypatch):
    _patch_all(monkeypatch)
    result = spotlight_runs(base_dir="/tmp")
    assert isinstance(result, list)


def test_spotlight_runs_respects_limit(monkeypatch):
    _patch_all(monkeypatch)
    result = spotlight_runs(limit=2, base_dir="/tmp")
    assert len(result) <= 2


def test_spotlight_runs_filters_by_pipeline(monkeypatch):
    _patch_all(monkeypatch)
    result = spotlight_runs(pipeline="etl", base_dir="/tmp")
    assert all(r["pipeline"] == "etl" for r in result)


def test_spotlight_runs_required_keys(monkeypatch):
    _patch_all(monkeypatch)
    result = spotlight_runs(base_dir="/tmp")
    for entry in result:
        for key in ("run_id", "pipeline", "status", "severity", "score", "grade", "anomaly_flag"):
            assert key in entry


def test_spotlight_runs_anomaly_flag_true(monkeypatch):
    _patch_all(monkeypatch)
    monkeypatch.setattr(
        "pipewatch.run_spotlight.detect_anomaly",
        lambda run_id, metric, **kw: {"is_anomaly": True, "z_score": 3.5},
    )
    result = spotlight_runs(base_dir="/tmp")
    assert all(r["anomaly_flag"] is True for r in result)


def test_spotlight_runs_sorted_by_score_desc(monkeypatch):
    scores = [60.0, 90.0, 75.0]
    call_count = {"n": 0}

    def fake_score(run_id, **kw):
        s = scores[call_count["n"] % len(scores)]
        call_count["n"] += 1
        return {"score": s, "grade": "B"}

    _patch_all(monkeypatch)
    monkeypatch.setattr("pipewatch.run_spotlight.compute_score", fake_score)
    result = spotlight_runs(base_dir="/tmp")
    score_vals = [r["score"] for r in result]
    assert score_vals == sorted(score_vals, reverse=True)


def test_format_spotlight_report_empty():
    assert format_spotlight_report([]) == "No spotlight runs found."


def test_format_spotlight_report_contains_run_id(monkeypatch):
    entries = [
        {"run_id": "abc123xyz", "pipeline": "etl", "status": "success",
         "severity": "low", "score": 88.0, "grade": "B", "anomaly_flag": False}
    ]
    report = format_spotlight_report(entries)
    assert "abc123xyz" in report
    assert "etl" in report


def test_format_spotlight_report_anomaly_flag(monkeypatch):
    entries = [
        {"run_id": "abc123xyz", "pipeline": "etl", "status": "success",
         "severity": "low", "score": 88.0, "grade": "B", "anomaly_flag": True}
    ]
    report = format_spotlight_report(entries)
    assert "ANOMALY" in report


def test_cmd_show_prints_output(monkeypatch, capsys):
    _patch_all(monkeypatch)

    class Args:
        pipeline = None
        limit = 5
        json = False
        base_dir = "/tmp"

    cmd_show(Args())
    captured = capsys.readouterr()
    assert "Spotlight" in captured.out or len(captured.out) > 0


def test_cmd_show_json_output(monkeypatch, capsys):
    _patch_all(monkeypatch)

    class Args:
        pipeline = None
        limit = 5
        json = True
        base_dir = "/tmp"

    cmd_show(Args())
    captured = capsys.readouterr()
    parsed = json.loads(captured.out)
    assert isinstance(parsed, list)


def test_build_parser_defaults():
    parser = build_parser()
    args = parser.parse_args([])
    assert args.limit == 5
    assert args.pipeline is None
    assert args.json is False
