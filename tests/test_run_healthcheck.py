"""Tests for pipewatch.run_healthcheck."""

from __future__ import annotations

from unittest.mock import patch

import pytest

from pipewatch.run_healthcheck import (
    evaluate_health,
    format_health_report,
    HEALTH_OK,
    HEALTH_WARN,
    HEALTH_FAIL,
)


def _make_run(pipeline: str, status: str = "success", exit_code: int = 0) -> dict:
    return {
        "run_id": "abc123",
        "pipeline": pipeline,
        "status": status,
        "exit_code": exit_code,
        "started": "2024-01-01T10:00:00",
        "finished": "2024-01-01T10:01:00",
    }


def _patch_all(runs, flakiness=None, sla_rules=None):
    flakiness = flakiness or {"classification": "stable", "score": 0.0}
    sla_rules = sla_rules or {}
    return [
        patch("pipewatch.run_healthcheck.list_run_records", return_value=runs),
        patch("pipewatch.run_healthcheck.analyze_pipeline_flakiness", return_value=flakiness),
        patch("pipewatch.run_healthcheck.load_sla_rules", return_value=sla_rules),
    ]


def test_evaluate_health_no_runs():
    with patch("pipewatch.run_healthcheck.list_run_records", return_value=[]):
        result = evaluate_health("my_pipeline")
    assert result["status"] == HEALTH_WARN
    assert "no runs found" in result["reason"]


def test_evaluate_health_ok():
    runs = [_make_run("pipe_a")]
    patches = _patch_all(runs)
    with patches[0], patches[1], patches[2]:
        result = evaluate_health("pipe_a")
    assert result["status"] == HEALTH_OK
    assert result["pipeline"] == "pipe_a"
    assert "details" in result


def test_evaluate_health_fail_on_critical_severity():
    runs = [_make_run("pipe_b", status="failed", exit_code=2)]
    flakiness = {"classification": "stable", "score": 0.0}
    patches = _patch_all(runs, flakiness=flakiness)
    with patches[0], patches[1], patches[2]:
        with patch("pipewatch.run_healthcheck.classify_severity", return_value="critical"):
            result = evaluate_health("pipe_b")
    assert result["status"] == HEALTH_FAIL
    assert "critical" in result["reason"]


def test_evaluate_health_warn_on_flaky():
    runs = [_make_run("pipe_c")]
    flakiness = {"classification": "flaky", "score": 0.6}
    patches = _patch_all(runs, flakiness=flakiness)
    with patches[0], patches[1], patches[2]:
        with patch("pipewatch.run_healthcheck.classify_severity", return_value="low"):
            result = evaluate_health("pipe_c")
    assert result["status"] == HEALTH_WARN
    assert "flaky" in result["reason"]


def test_evaluate_health_details_keys():
    runs = [_make_run("pipe_d")]
    patches = _patch_all(runs)
    with patches[0], patches[1], patches[2]:
        result = evaluate_health("pipe_d")
    for key in ("latest_run_id", "severity", "flakiness", "sla"):
        assert key in result["details"]


def test_format_health_report_contains_pipeline():
    result = {
        "pipeline": "my_pipe",
        "status": HEALTH_OK,
        "reason": "all checks passed",
        "details": {"severity": "low", "flakiness": "stable"},
    }
    report = format_health_report(result)
    assert "my_pipe" in report
    assert "HEALTHY" in report
    assert "all checks passed" in report
    assert "severity" in report


def test_evaluate_health_filters_by_pipeline():
    runs = [
        _make_run("pipe_x"),
        _make_run("pipe_y", status="failed", exit_code=1),
    ]
    patches = _patch_all(runs)
    with patches[0], patches[1], patches[2]:
        result = evaluate_health("pipe_x")
    assert result["details"]["latest_run_id"] == "abc123"
