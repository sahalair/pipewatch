"""Tests for pipewatch.run_volatility."""

import pytest
from unittest.mock import patch

from pipewatch.run_volatility import (
    _mean,
    _stddev,
    _coefficient_of_variation,
    _grade_volatility,
    compute_volatility,
    format_volatility_report,
    volatility_for_all_pipelines,
)


def _patch(durations, runs=None):
    runs = runs or []
    return (
        patch("pipewatch.run_volatility.get_durations_for_pipeline", return_value=durations),
        patch("pipewatch.run_volatility.list_run_records", return_value=runs),
    )


def test_mean_basic():
    assert _mean([1.0, 3.0, 5.0]) == pytest.approx(3.0)


def test_mean_empty():
    assert _mean([]) == 0.0


def test_stddev_single_element():
    assert _stddev([42.0]) == 0.0


def test_stddev_basic():
    assert _stddev([2.0, 4.0, 4.0, 4.0, 5.0, 5.0, 7.0, 9.0]) == pytest.approx(2.0)


def test_cv_none_when_mean_zero():
    assert _coefficient_of_variation([0.0, 0.0]) is None


def test_cv_none_when_empty():
    assert _coefficient_of_variation([]) is None


def test_cv_basic():
    cv = _coefficient_of_variation([10.0, 20.0])
    assert cv is not None
    assert cv > 0


def test_grade_volatility_stable():
    assert _grade_volatility(0.05) == "stable"


def test_grade_volatility_low():
    assert _grade_volatility(0.15) == "low"


def test_grade_volatility_moderate():
    assert _grade_volatility(0.35) == "moderate"


def test_grade_volatility_high():
    assert _grade_volatility(0.75) == "high"


def test_grade_volatility_extreme():
    assert _grade_volatility(1.5) == "extreme"


def test_grade_volatility_unknown():
    assert _grade_volatility(None) == "unknown"


def test_compute_volatility_returns_required_keys():
    p1, p2 = _patch([10.0, 20.0, 30.0])
    with p1, p2:
        result = compute_volatility("my_pipeline")
    for key in ("pipeline", "sample_count", "mean_duration", "stddev_duration",
                "coefficient_of_variation", "grade"):
        assert key in result


def test_compute_volatility_no_durations():
    p1, p2 = _patch([])
    with p1, p2:
        result = compute_volatility("empty_pipeline")
    assert result["sample_count"] == 0
    assert result["mean_duration"] is None
    assert result["grade"] == "unknown"


def test_compute_volatility_stable_pipeline():
    p1, p2 = _patch([100.0, 101.0, 99.0, 100.5])
    with p1, p2:
        result = compute_volatility("stable")
    assert result["grade"] == "stable"


def test_format_volatility_report_contains_pipeline():
    p1, p2 = _patch([10.0, 20.0])
    with p1, p2:
        result = compute_volatility("my_pipe")
    report = format_volatility_report(result)
    assert "my_pipe" in report
    assert "Grade" in report


def test_volatility_for_all_pipelines_empty():
    p1, p2 = _patch([], runs=[])
    with p1, p2:
        results = volatility_for_all_pipelines()
    assert results == []


def test_volatility_for_all_pipelines_deduplicates():
    runs = [
        {"pipeline": "pipe_a"},
        {"pipeline": "pipe_a"},
        {"pipeline": "pipe_b"},
    ]
    p1, p2 = _patch([50.0, 60.0], runs=runs)
    with p1, p2:
        results = volatility_for_all_pipelines()
    pipelines = [r["pipeline"] for r in results]
    assert pipelines == ["pipe_a", "pipe_b"]
