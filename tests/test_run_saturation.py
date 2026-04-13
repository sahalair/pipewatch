"""Tests for pipewatch.run_saturation."""

import pytest
from unittest.mock import patch

from pipewatch.run_saturation import (
    compute_saturation,
    _grade_saturation,
    format_saturation_report,
    saturation_for_all_pipelines,
)


DEFAULT_CAPACITY = {
    "pipe-a": {"max_runs": 10},
    "pipe-b": {"max_runs": 5},
}

DEFAULT_THROUGHPUT = [
    {"pipeline": "pipe-a", "count": 8},
    {"pipeline": "pipe-b", "count": 6},
]


def _patch(capacity=None, throughput=None):
    cap = capacity if capacity is not None else DEFAULT_CAPACITY
    tp = throughput if throughput is not None else DEFAULT_THROUGHPUT
    return (
        patch("pipewatch.run_saturation.load_capacity", return_value=cap),
        patch("pipewatch.run_saturation.compute_throughput", return_value=tp),
    )


def test_compute_saturation_returns_required_keys():
    p1, p2 = _patch()
    with p1, p2:
        result = compute_saturation("pipe-a")
    for key in ("pipeline", "actual", "capacity", "saturation", "grade"):
        assert key in result


def test_compute_saturation_correct_ratio():
    p1, p2 = _patch()
    with p1, p2:
        result = compute_saturation("pipe-a")
    assert result["saturation"] == pytest.approx(0.8, rel=1e-3)
    assert result["actual"] == 8
    assert result["capacity"] == 10


def test_compute_saturation_over_capacity():
    p1, p2 = _patch()
    with p1, p2:
        result = compute_saturation("pipe-b")
    assert result["saturation"] > 1.0
    assert result["grade"] == "OVER_CAPACITY"
    assert result["warning"] is not None


def test_compute_saturation_no_capacity_rule():
    p1, p2 = _patch(capacity={}, throughput=DEFAULT_THROUGHPUT)
    with p1, p2:
        result = compute_saturation("pipe-a")
    assert result["capacity"] is None
    assert result["saturation"] is None
    assert result["grade"] == "N/A"


def test_compute_saturation_no_throughput_data():
    p1, p2 = _patch(throughput=[])
    with p1, p2:
        result = compute_saturation("pipe-a")
    assert result["actual"] == 0
    assert result["saturation"] == 0.0


def test_grade_saturation_boundaries():
    assert _grade_saturation(0.3) == "LOW"
    assert _grade_saturation(0.6) == "MODERATE"
    assert _grade_saturation(0.85) == "HIGH"
    assert _grade_saturation(1.0) == "NEAR_LIMIT"
    assert _grade_saturation(1.1) == "OVER_CAPACITY"
    assert _grade_saturation(None) == "N/A"


def test_format_saturation_report_contains_fields():
    p1, p2 = _patch()
    with p1, p2:
        result = compute_saturation("pipe-a")
    report = format_saturation_report(result)
    assert "pipe-a" in report
    assert "Saturation" in report
    assert "Grade" in report


def test_saturation_for_all_pipelines_returns_list():
    p1, p2 = _patch()
    with p1, p2:
        results = saturation_for_all_pipelines()
    assert isinstance(results, list)
    assert len(results) == 2
    pipelines = {r["pipeline"] for r in results}
    assert pipelines == {"pipe-a", "pipe-b"}


def test_saturation_for_all_pipelines_empty():
    p1, p2 = _patch(capacity={}, throughput=[])
    with p1, p2:
        results = saturation_for_all_pipelines()
    assert results == []
