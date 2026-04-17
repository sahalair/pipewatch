"""Tests for run_pulse.py"""
from unittest.mock import patch
import pytest

from pipewatch.run_pulse import compute_pulse, format_pulse_report, _grade_pulse


MOD = "pipewatch.run_pulse"


def _patch_all(uptime_ratio=1.0, run_count=10, flakiness_score=0.0):
    uptime_data = {"ratio": uptime_ratio}
    velocity_data = {"run_count": run_count}
    flakiness_data = {"score": flakiness_score}
    patches = [
        patch(f"{MOD}.compute_uptime", return_value=uptime_data),
        patch(f"{MOD}.compute_velocity", return_value=velocity_data),
        patch(f"{MOD}.analyze_pipeline_flakiness", return_value=flakiness_data),
    ]
    return patches


def test_compute_pulse_returns_required_keys():
    with _patch_all()[0], _patch_all()[1], _patch_all()[2]:
        ps = [p.start() for p in _patch_all()]
        result = compute_pulse("my_pipeline")
        for p in ps:
            p.stop()
    required = {"pipeline", "score", "grade", "uptime_ratio", "velocity_component", "stability_component"}
    assert required <= result.keys()


def test_compute_pulse_perfect_score():
    ps = [p.start() for p in _patch_all(uptime_ratio=1.0, run_count=10, flakiness_score=0.0)]
    result = compute_pulse("pipe")
    for p in ps:
        p.stop()
    assert result["score"] == pytest.approx(1.0, abs=1e-4)
    assert result["grade"] == "A"


def test_compute_pulse_low_uptime_lowers_score():
    ps = [p.start() for p in _patch_all(uptime_ratio=0.0, run_count=0, flakiness_score=1.0)]
    result = compute_pulse("pipe")
    for p in ps:
        p.stop()
    assert result["score"] == pytest.approx(0.0, abs=1e-4)
    assert result["grade"] == "F"


def test_compute_pulse_velocity_capped_at_1():
    ps = [p.start() for p in _patch_all(uptime_ratio=1.0, run_count=999, flakiness_score=0.0)]
    result = compute_pulse("pipe")
    for p in ps:
        p.stop()
    assert result["velocity_component"] == pytest.approx(1.0)


def test_grade_pulse_boundaries():
    assert _grade_pulse(1.0) == "A"
    assert _grade_pulse(0.85) == "A"
    assert _grade_pulse(0.84) == "B"
    assert _grade_pulse(0.70) == "B"
    assert _grade_pulse(0.69) == "C"
    assert _grade_pulse(0.55) == "C"
    assert _grade_pulse(0.54) == "D"
    assert _grade_pulse(0.40) == "D"
    assert _grade_pulse(0.39) == "F"


def test_format_pulse_report_contains_pipeline():
    ps = [p.start() for p in _patch_all()]
    result = compute_pulse("my_pipe")
    for p in ps:
        p.stop()
    report = format_pulse_report(result)
    assert "my_pipe" in report
    assert "Score" in report
    assert "Uptime" in report
