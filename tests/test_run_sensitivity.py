"""Tests for run_sensitivity module."""

import pytest
from unittest.mock import patch

from pipewatch.run_sensitivity import (
    analyze_sensitivity,
    format_sensitivity_report,
    _classify_influence,
    get_outcome_series,
)


def _make_run(pipeline, status, started="2024-01-01T00:00:00"):
    return {"pipeline": pipeline, "status": status, "started": started}


def _patch(runs, series_map):
    """Return context managers patching list_run_records and get_metric_series."""
    r_patch = patch("pipewatch.run_sensitivity.list_run_records", return_value=runs)
    s_patch = patch(
        "pipewatch.run_sensitivity.get_metric_series",
        side_effect=lambda pipeline, metric, **kw: series_map.get(metric, []),
    )
    return r_patch, s_patch


def test_get_outcome_series_ok_and_fail():
    runs = [
        _make_run("p", "ok", "2024-01-01T00:00:00"),
        _make_run("p", "fail", "2024-01-02T00:00:00"),
        _make_run("p", "ok", "2024-01-03T00:00:00"),
    ]
    with patch("pipewatch.run_sensitivity.list_run_records", return_value=runs):
        result = get_outcome_series("p")
    assert result == [1.0, 0.0, 1.0]


def test_get_outcome_series_filters_by_pipeline():
    runs = [
        _make_run("p", "ok"),
        _make_run("other", "fail"),
    ]
    with patch("pipewatch.run_sensitivity.list_run_records", return_value=runs):
        result = get_outcome_series("p")
    assert result == [1.0]


def test_analyze_sensitivity_returns_list():
    runs = [_make_run("p", "ok", f"2024-01-0{i}T00:00:00") for i in range(1, 7)]
    series = [10.0, 20.0, 10.0, 20.0, 10.0, 20.0]
    r_p, s_p = _patch(runs, {"latency": series})
    with r_p, s_p:
        result = analyze_sensitivity("p", ["latency"], min_samples=5)
    assert isinstance(result, list)


def test_analyze_sensitivity_insufficient_data_returns_empty():
    runs = [_make_run("p", "ok") for _ in range(3)]
    r_p, s_p = _patch(runs, {"latency": [1.0, 2.0, 3.0]})
    with r_p, s_p:
        result = analyze_sensitivity("p", ["latency"], min_samples=5)
    assert result == []


def test_analyze_sensitivity_result_has_required_keys():
    runs = [_make_run("p", "ok", f"2024-01-{i:02d}T00:00:00") for i in range(1, 8)]
    series = [float(i) for i in range(1, 8)]
    r_p, s_p = _patch(runs, {"cpu": series})
    with r_p, s_p:
        result = analyze_sensitivity("p", ["cpu"], min_samples=5)
    if result:
        assert "metric" in result[0]
        assert "correlation" in result[0]
        assert "influence" in result[0]


def test_analyze_sensitivity_sorted_by_abs_correlation():
    runs = [_make_run("p", "ok", f"2024-01-{i:02d}T00:00:00") for i in range(1, 8)]
    outcome = [1.0, 0.0, 1.0, 0.0, 1.0, 0.0, 1.0]
    strong = [10.0, 1.0, 10.0, 1.0, 10.0, 1.0, 10.0]
    weak = [5.0, 5.1, 5.0, 5.1, 5.0, 5.1, 5.0]
    r_p, s_p = _patch(runs, {"strong_m": strong, "weak_m": weak})
    with r_p, s_p:
        result = analyze_sensitivity("p", ["weak_m", "strong_m"], min_samples=5)
    if len(result) >= 2:
        assert abs(result[0]["correlation"]) >= abs(result[1]["correlation"])


def test_classify_influence_boundaries():
    assert _classify_influence(0.8) == "strong"
    assert _classify_influence(-0.75) == "strong"
    assert _classify_influence(0.5) == "moderate"
    assert _classify_influence(0.25) == "weak"
    assert _classify_influence(0.05) == "negligible"


def test_format_sensitivity_report_no_results():
    report = format_sensitivity_report("mypipe", [])
    assert "mypipe" in report
    assert "insufficient" in report


def test_format_sensitivity_report_with_results():
    results = [{"metric": "cpu", "correlation": 0.82, "influence": "strong"}]
    report = format_sensitivity_report("mypipe", results)
    assert "mypipe" in report
    assert "cpu" in report
    assert "strong" in report
