"""Tests for pipewatch.run_confidence."""

import pytest
from unittest.mock import patch

from pipewatch.run_confidence import (
    _uptime_component,
    _flakiness_component,
    _volume_component,
    compute_confidence,
    format_confidence_report,
)


def _patch_all(uptime_ratio=1.0, flakiness_score=0.0, run_count=20):
    """Return a context manager tuple that patches all three data sources."""
    fake_runs = [
        {"pipeline": "my_pipe", "run_id": str(i)}
        for i in range(run_count)
    ]
    return (
        patch(
            "pipewatch.run_confidence.compute_uptime",
            return_value={"ratio": uptime_ratio},
        ),
        patch(
            "pipewatch.run_confidence.analyze_pipeline_flakiness",
            return_value={"flakiness_score": flakiness_score},
        ),
        patch(
            "pipewatch.run_confidence.list_run_records",
            return_value=fake_runs,
        ),
    )


def test_compute_confidence_returns_required_keys():
    with _patch_all()[0], _patch_all()[1], _patch_all()[2]:
        p0, p1, p2 = _patch_all()
        with p0, p1, p2:
            result = compute_confidence("my_pipe")
    assert "pipeline" in result
    assert "score" in result
    assert "grade" in result
    assert "components" in result


def test_compute_confidence_perfect_score():
    p0, p1, p2 = _patch_all(uptime_ratio=1.0, flakiness_score=0.0, run_count=20)
    with p0, p1, p2:
        result = compute_confidence("my_pipe")
    assert result["score"] == 100.0
    assert result["grade"] == "A"


def test_compute_confidence_low_volume_lowers_score():
    p0, p1, p2 = _patch_all(uptime_ratio=1.0, flakiness_score=0.0, run_count=0)
    with p0, p1, p2:
        result = compute_confidence("my_pipe")
    assert result["score"] < 100.0
    assert result["components"]["volume"] == 0.0


def test_compute_confidence_flaky_pipeline_lowers_score():
    p0, p1, p2 = _patch_all(uptime_ratio=1.0, flakiness_score=1.0, run_count=20)
    with p0, p1, p2:
        result = compute_confidence("my_pipe")
    assert result["components"]["flakiness"] == 0.0
    assert result["score"] < 100.0


def test_compute_confidence_low_uptime_lowers_score():
    p0, p1, p2 = _patch_all(uptime_ratio=0.0, flakiness_score=0.0, run_count=20)
    with p0, p1, p2:
        result = compute_confidence("my_pipe")
    assert result["components"]["uptime"] == 0.0
    assert result["score"] < 100.0


def test_compute_confidence_grade_f_when_very_low():
    p0, p1, p2 = _patch_all(uptime_ratio=0.0, flakiness_score=1.0, run_count=0)
    with p0, p1, p2:
        result = compute_confidence("my_pipe")
    assert result["grade"] == "F"


def test_format_confidence_report_contains_pipeline():
    p0, p1, p2 = _patch_all()
    with p0, p1, p2:
        result = compute_confidence("my_pipe")
    report = format_confidence_report(result)
    assert "my_pipe" in report
    assert "Confidence" in report
    assert "Components" in report


def test_volume_component_capped_at_one():
    fake_runs = [{"pipeline": "p", "run_id": str(i)} for i in range(100)]
    with patch(
        "pipewatch.run_confidence.list_run_records", return_value=fake_runs
    ):
        val = _volume_component("p", ".pipewatch", min_runs=10)
    assert val == 1.0


def test_uptime_component_none_ratio_returns_half():
    with patch(
        "pipewatch.run_confidence.compute_uptime",
        return_value={"ratio": None},
    ):
        val = _uptime_component("p", ".pipewatch")
    assert val == 0.5
