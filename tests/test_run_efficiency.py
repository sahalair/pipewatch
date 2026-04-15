"""Tests for pipewatch.run_efficiency."""

from __future__ import annotations

from unittest.mock import patch

import pytest

from pipewatch.run_efficiency import (
    _grade_efficiency,
    _duration_component,
    _cost_component,
    compute_efficiency,
    format_efficiency_report,
)


# ---------------------------------------------------------------------------
# Grade helper
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("score,expected", [
    (95, "A"),
    (90, "A"),
    (80, "B"),
    (75, "B"),
    (65, "C"),
    (60, "C"),
    (50, "D"),
    (40, "D"),
    (39, "F"),
    (0,  "F"),
])
def test_grade_efficiency_boundaries(score, expected):
    assert _grade_efficiency(score) == expected


# ---------------------------------------------------------------------------
# Duration component
# ---------------------------------------------------------------------------

def _patch_dur(durations, run_duration):
    return [
        patch("pipewatch.run_efficiency.get_durations_for_pipeline", return_value=durations),
        patch("pipewatch.run_efficiency.summarize_durations", return_value={"median": 100.0}),
        patch("pipewatch.run_efficiency.get_run_duration", return_value=run_duration),
    ]


def test_duration_component_no_history_returns_50():
    with patch("pipewatch.run_efficiency.get_durations_for_pipeline", return_value=[]):
        score = _duration_component("pipe", "run1")
    assert score == 50.0


def test_duration_component_fast_run_returns_100():
    patches = _patch_dur([90.0], 70.0)  # ratio = 0.7 < 0.8
    with patches[0], patches[1], patches[2]:
        score = _duration_component("pipe", "run1")
    assert score == 100.0


def test_duration_component_slow_run_returns_low_score():
    patches = _patch_dur([100.0], 250.0)  # ratio = 2.5 > 2.0
    with patches[0], patches[1], patches[2]:
        score = _duration_component("pipe", "run1")
    assert score == 10.0


# ---------------------------------------------------------------------------
# Cost component
# ---------------------------------------------------------------------------

def test_cost_component_no_costs_returns_80():
    with patch("pipewatch.run_efficiency.load_costs", return_value=[]):
        score = _cost_component("run1")
    assert score == 80.0


def test_cost_component_high_cost_returns_low_score():
    costs = [{"amount": 500.0}]
    with patch("pipewatch.run_efficiency.load_costs", return_value=costs):
        score = _cost_component("run1")
    assert score == 10.0


# ---------------------------------------------------------------------------
# compute_efficiency
# ---------------------------------------------------------------------------

def _patch_all(dur_score=85.0, cost_score=80.0):
    return [
        patch("pipewatch.run_efficiency._duration_component", return_value=dur_score),
        patch("pipewatch.run_efficiency._cost_component", return_value=cost_score),
    ]


def test_compute_efficiency_returns_required_keys():
    with _patch_all()[0], _patch_all()[1]:
        result = compute_efficiency("my_pipe", "run-abc")
    assert {"pipeline", "run_id", "score", "grade", "duration_component", "cost_component"} <= result.keys()


def test_compute_efficiency_score_calculation():
    with _patch_all(dur_score=100.0, cost_score=100.0)[0], _patch_all(dur_score=100.0, cost_score=100.0)[1]:
        result = compute_efficiency("pipe", "run1")
    assert result["score"] == 100.0
    assert result["grade"] == "A"


# ---------------------------------------------------------------------------
# format_efficiency_report
# ---------------------------------------------------------------------------

def test_format_efficiency_report_contains_pipeline():
    data = {
        "pipeline": "etl_pipeline",
        "run_id": "run-xyz",
        "score": 72.5,
        "grade": "C",
        "duration_component": 65.0,
        "cost_component": 83.5,
    }
    report = format_efficiency_report(data)
    assert "etl_pipeline" in report
    assert "72.5" in report
    assert "C" in report
