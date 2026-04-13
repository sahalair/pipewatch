"""Tests for pipewatch.run_scoring."""

from __future__ import annotations

from unittest.mock import patch

import pytest

from pipewatch.run_scoring import (
    _grade,
    _severity_component,
    compute_score,
    format_score_report,
)


_BASE_RUN = {
    "run_id": "abc-123",
    "pipeline": "etl",
    "status": "success",
    "exit_code": 0,
    "started": "2024-01-01T10:00:00",
    "finished": "2024-01-01T10:05:00",
}


def _patch_all(
    flakiness_score=0.0,
    durations=None,
    sla_met=True,
):
    durations = durations or [60.0, 60.0, 62.0]
    return [
        patch(
            "pipewatch.run_scoring.analyze_pipeline_flakiness",
            return_value={"flakiness_score": flakiness_score},
        ),
        patch(
            "pipewatch.run_scoring.get_durations_for_pipeline",
            return_value=durations,
        ),
        patch(
            "pipewatch.run_scoring.summarize_durations",
            return_value={"median": 60.0},
        ),
        patch(
            "pipewatch.run_scoring.check_sla",
            return_value={"met": sla_met},
        ),
    ]


def test_grade_boundaries():
    assert _grade(95) == "A"
    assert _grade(80) == "B"
    assert _grade(65) == "C"
    assert _grade(50) == "D"
    assert _grade(30) == "F"


def test_severity_component_ok_run():
    score = _severity_component({"status": "success", "exit_code": 0})
    assert score == 100.0


def test_severity_component_failed_run():
    score = _severity_component({"status": "failed", "exit_code": 1})
    assert score < 100.0


def test_compute_score_returns_required_keys():
    patches = _patch_all()
    with patches[0], patches[1], patches[2], patches[3]:
        result = compute_score(_BASE_RUN)
    assert "run_id" in result
    assert "pipeline" in result
    assert "score" in result
    assert "grade" in result
    assert "components" in result
    assert set(result["components"]) == {"severity", "flakiness", "duration", "sla"}


def test_compute_score_perfect_run():
    patches = _patch_all(flakiness_score=0.0, durations=[60.0, 60.0], sla_met=True)
    with patches[0], patches[1], patches[2], patches[3]:
        result = compute_score(_BASE_RUN)
    assert result["score"] == 100.0
    assert result["grade"] == "A"


def test_compute_score_flaky_pipeline_lowers_score():
    patches = _patch_all(flakiness_score=1.0, sla_met=True)
    with patches[0], patches[1], patches[2], patches[3]:
        result = compute_score(_BASE_RUN)
    assert result["components"]["flakiness"] == 0.0
    assert result["score"] < 100.0


def test_compute_score_sla_breach_lowers_score():
    patches = _patch_all(sla_met=False)
    with patches[0], patches[1], patches[2], patches[3]:
        result = compute_score(_BASE_RUN)
    assert result["components"]["sla"] == 0.0


def test_compute_score_slow_run_lowers_duration_component():
    slow = [60.0] * 10 + [200.0]  # latest is much slower
    patches = _patch_all(durations=slow)
    with patches[0], patches[1], patches[2], patches[3]:
        result = compute_score(_BASE_RUN)
    assert result["components"]["duration"] < 100.0


def test_format_score_report_contains_run_id():
    patches = _patch_all()
    with patches[0], patches[1], patches[2], patches[3]:
        result = compute_score(_BASE_RUN)
    report = format_score_report(result)
    assert "abc-123" in report
    assert "etl" in report
    assert "Score" in report
