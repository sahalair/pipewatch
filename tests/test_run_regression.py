"""Tests for run_regression module."""

import pytest
from unittest.mock import patch

from pipewatch.run_regression import (
    detect_regression,
    analyze_regression,
    format_regression_report,
    get_metric_values,
)


def _make_metrics(pipeline, name, values):
    """Build fake metric records for a list of values."""
    return [
        {"pipeline": pipeline, "name": name, "value": v, "unit": ""}
        for v in values
    ]


def _patch(runs, metrics_by_run):
    """Patch list_runs and load_metrics."""
    return [
        patch("pipewatch.run_regression.list_runs", return_value=runs),
        patch(
            "pipewatch.run_regression.load_metrics",
            side_effect=lambda run_id: metrics_by_run.get(run_id, []),
        ),
    ]


# --- detect_regression ---

def test_detect_regression_no_regression():
    values = [(f"r{i}", 10.0) for i in range(5)]
    assert detect_regression(values) is None


def test_detect_regression_insufficient_data():
    values = [("r1", 5.0), ("r2", 6.0)]
    assert detect_regression(values, window=3) is None


def test_detect_regression_detects_increase():
    values = [(f"r{i}", 10.0) for i in range(4)] + [("r4", 25.0)]
    result = detect_regression(values, threshold_pct=10.0, window=3)
    assert result is not None
    assert result["regressed"] is True
    assert result["direction"] == "increase"
    assert result["change_pct"] > 0


def test_detect_regression_detects_decrease():
    values = [(f"r{i}", 100.0) for i in range(4)] + [("r4", 50.0)]
    result = detect_regression(values, threshold_pct=10.0, window=3)
    assert result is not None
    assert result["direction"] == "decrease"
    assert result["change_pct"] < 0


def test_detect_regression_below_threshold_returns_none():
    values = [(f"r{i}", 10.0) for i in range(4)] + [("r4", 10.5)]
    assert detect_regression(values, threshold_pct=10.0, window=3) is None


def test_detect_regression_zero_baseline_returns_none():
    values = [(f"r{i}", 0.0) for i in range(4)] + [("r4", 5.0)]
    assert detect_regression(values, threshold_pct=10.0, window=3) is None


def test_detect_regression_result_keys():
    values = [(f"r{i}", 10.0) for i in range(4)] + [("r4", 30.0)]
    result = detect_regression(values, threshold_pct=10.0, window=3)
    for key in ("run_id", "latest_value", "baseline", "change_pct", "direction", "regressed"):
        assert key in result


# --- analyze_regression ---

def test_analyze_regression_returns_required_keys():
    runs = ["r1", "r2", "r3", "r4", "r5"]
    metrics = {r: _make_metrics("pipe", "duration", [10.0]) for r in runs}
    patches = _patch(runs, metrics)
    for p in patches:
        p.start()
    try:
        result = analyze_regression("pipe", "duration")
        for key in ("pipeline", "metric", "data_points", "regression"):
            assert key in result
    finally:
        for p in patches:
            p.stop()


def test_analyze_regression_no_data():
    with patch("pipewatch.run_regression.list_runs", return_value=[]), \
         patch("pipewatch.run_regression.load_metrics", return_value=[]):
        result = analyze_regression("pipe", "missing_metric")
        assert result["data_points"] == 0
        assert result["regression"] is None


# --- format_regression_report ---

def test_format_regression_report_no_regression():
    result = {"pipeline": "etl", "metric": "rows", "data_points": 5, "regression": None}
    report = format_regression_report(result)
    assert "OK" in report
    assert "etl" in report
    assert "rows" in report


def test_format_regression_report_with_regression():
    reg = {
        "run_id": "abc123",
        "latest_value": 50.0,
        "baseline": 10.0,
        "change_pct": 400.0,
        "direction": "increase",
        "regressed": True,
    }
    result = {"pipeline": "etl", "metric": "duration", "data_points": 5, "regression": reg}
    report = format_regression_report(result)
    assert "REGRESSION" in report
    assert "abc123" in report
    assert "+400.00%" in report
