"""Focused tests for scoring edge cases and grade transitions."""

from __future__ import annotations

from unittest.mock import patch

import pytest

from pipewatch.run_scoring import (
    _duration_component,
    _flakiness_component,
    _sla_component,
    _grade,
    compute_score,
)


def test_grade_exact_boundary_90():
    assert _grade(90.0) == "A"


def test_grade_exact_boundary_75():
    assert _grade(75.0) == "B"


def test_grade_exact_boundary_60():
    assert _grade(60.0) == "C"


def test_grade_exact_boundary_40():
    assert _grade(40.0) == "D"


def test_grade_below_40_is_f():
    assert _grade(39.9) == "F"
    assert _grade(0.0) == "F"


def test_duration_component_no_durations():
    with patch("pipewatch.run_scoring.get_durations_for_pipeline", return_value=[]), \
         patch("pipewatch.run_scoring.summarize_durations", return_value={"median": 0.0}):
        score = _duration_component("pipe")
    assert score == 100.0


def test_duration_component_zero_median():
    with patch("pipewatch.run_scoring.get_durations_for_pipeline", return_value=[0.0, 0.0]), \
         patch("pipewatch.run_scoring.summarize_durations", return_value={"median": 0.0}):
        score = _duration_component("pipe")
    assert score == 100.0


def test_duration_component_3x_slower_is_zero():
    with patch("pipewatch.run_scoring.get_durations_for_pipeline", return_value=[60.0, 180.0]), \
         patch("pipewatch.run_scoring.summarize_durations", return_value={"median": 60.0}):
        score = _duration_component("pipe")
    assert score == 0.0


def test_flakiness_component_zero_flakiness():
    with patch("pipewatch.run_scoring.analyze_pipeline_flakiness",
               return_value={"flakiness_score": 0.0}):
        score = _flakiness_component("pipe")
    assert score == 100.0


def test_flakiness_component_full_flakiness():
    with patch("pipewatch.run_scoring.analyze_pipeline_flakiness",
               return_value={"flakiness_score": 1.0}):
        score = _flakiness_component("pipe")
    assert score == 0.0


def test_sla_component_met():
    with patch("pipewatch.run_scoring.check_sla", return_value={"met": True}):
        score = _sla_component({"run_id": "x"})
    assert score == 100.0


def test_sla_component_not_met():
    with patch("pipewatch.run_scoring.check_sla", return_value={"met": False}):
        score = _sla_component({"run_id": "x"})
    assert score == 0.0


def test_sla_component_exception_defaults_to_100():
    with patch("pipewatch.run_scoring.check_sla", side_effect=Exception("oops")):
        score = _sla_component({"run_id": "x"})
    assert score == 100.0


def test_compute_score_score_in_range():
    run = {"run_id": "r1", "pipeline": "p", "status": "success", "exit_code": 0}
    with patch("pipewatch.run_scoring.analyze_pipeline_flakiness",
               return_value={"flakiness_score": 0.5}), \
         patch("pipewatch.run_scoring.get_durations_for_pipeline",
               return_value=[60.0, 90.0]), \
         patch("pipewatch.run_scoring.summarize_durations",
               return_value={"median": 60.0}), \
         patch("pipewatch.run_scoring.check_sla", return_value={"met": True}):
        result = compute_score(run)
    assert 0.0 <= result["score"] <= 100.0
