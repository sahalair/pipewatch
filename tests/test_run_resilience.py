"""Tests for pipewatch.run_resilience."""

import pytest
from unittest.mock import patch

from pipewatch.run_resilience import compute_resilience, format_resilience_report, _mean_recovery_time


BASE = "."


def _make_run(pipeline, exit_code=0, started="2024-01-01T00:00:00", finished="2024-01-01T00:01:00"):
    return {"pipeline": pipeline, "exit_code": exit_code, "started": started, "finished": finished}


def _patch_all(runs, uptime_ratio=1.0, flakiness_score=0.0):
    uptime_data = {"ratio": uptime_ratio}
    flakiness_data = {"score": flakiness_score}
    p1 = patch("pipewatch.run_resilience.list_run_records", return_value=runs)
    p2 = patch("pipewatch.run_resilience.compute_uptime", return_value=uptime_data)
    p3 = patch("pipewatch.run_resilience.analyze_pipeline_flakiness", return_value=flakiness_data)
    return p1, p2, p3


def test_compute_resilience_returns_required_keys():
    runs = [_make_run("etl")]
    p1, p2, p3 = _patch_all(runs)
    with p1, p2, p3:
        result = compute_resilience("etl")
    for key in ("pipeline", "score", "grade", "uptime_ratio", "flakiness_score",
                "mean_recovery_seconds", "recovery_component"):
        assert key in result


def test_compute_resilience_perfect_score():
    runs = [_make_run("etl")]
    p1, p2, p3 = _patch_all(runs, uptime_ratio=1.0, flakiness_score=0.0)
    with p1, p2, p3:
        result = compute_resilience("etl")
    assert result["score"] == 100.0
    assert result["grade"] == "A"


def test_compute_resilience_low_uptime_lowers_score():
    runs = [_make_run("etl")]
    p1, p2, p3 = _patch_all(runs, uptime_ratio=0.5, flakiness_score=0.0)
    with p1, p2, p3:
        result = compute_resilience("etl")
    assert result["score"] < 100.0


def test_compute_resilience_high_flakiness_lowers_score():
    runs = [_make_run("etl")]
    p1, p2, p3 = _patch_all(runs, uptime_ratio=1.0, flakiness_score=0.9)
    with p1, p2, p3:
        result = compute_resilience("etl")
    assert result["score"] < 100.0


def test_compute_resilience_grade_f_for_low_score():
    runs = [_make_run("etl")]
    p1, p2, p3 = _patch_all(runs, uptime_ratio=0.0, flakiness_score=1.0)
    with p1, p2, p3:
        result = compute_resilience("etl")
    assert result["grade"] == "F"


def test_mean_recovery_time_no_failures_returns_none():
    runs = [
        _make_run("etl", exit_code=0, started="2024-01-01T00:00:00"),
        _make_run("etl", exit_code=0, started="2024-01-01T01:00:00"),
    ]
    with patch("pipewatch.run_resilience.list_run_records", return_value=runs):
        result = _mean_recovery_time("etl")
    assert result is None


def test_mean_recovery_time_calculates_correctly():
    runs = [
        _make_run("etl", exit_code=1, started="2024-01-01T00:00:00", finished="2024-01-01T00:10:00"),
        _make_run("etl", exit_code=0, started="2024-01-01T00:20:00", finished="2024-01-01T00:21:00"),
    ]
    with patch("pipewatch.run_resilience.list_run_records", return_value=runs):
        result = _mean_recovery_time("etl")
    # recovery = started_of_success - finished_of_failure = 00:20 - 00:10 = 600s
    assert result == pytest.approx(600.0, abs=1.0)


def test_format_resilience_report_contains_pipeline():
    report = {
        "pipeline": "my_pipe",
        "score": 88.5,
        "grade": "B",
        "uptime_ratio": 0.95,
        "flakiness_score": 0.1,
        "mean_recovery_seconds": 120.0,
        "recovery_component": 0.97,
    }
    text = format_resilience_report(report)
    assert "my_pipe" in text
    assert "88.5" in text
    assert "B" in text
