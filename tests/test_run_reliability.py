"""Tests for pipewatch.run_reliability."""

from unittest.mock import patch
import pytest

from pipewatch.run_reliability import (
    _grade_reliability,
    compute_reliability,
    format_reliability_report,
)


def _patch_all(uptime=1.0, flakiness_score=0.0, sla_ok=True):
    """Return a context-manager stack that patches all three components."""
    patches = [
        patch(
            "pipewatch.run_reliability.compute_uptime",
            return_value={"ratio": uptime},
        ),
        patch(
            "pipewatch.run_reliability.analyze_pipeline_flakiness",
            return_value={"flakiness_score": flakiness_score},
        ),
        patch(
            "pipewatch.run_reliability.load_sla_rules",
            return_value={},
        ),
        patch(
            "pipewatch.run_reliability.check_sla",
            return_value={"ok": sla_ok},
        ),
    ]
    return patches


def _apply(patches):
    started = [p.start() for p in patches]
    return started


def _stop(patches):
    for p in patches:
        p.stop()


# --- grade helper ---

@pytest.mark.parametrize("score,expected", [
    (1.00, "A"), (0.90, "A"), (0.89, "B"), (0.75, "B"),
    (0.74, "C"), (0.60, "C"), (0.59, "D"), (0.40, "D"),
    (0.39, "F"), (0.00, "F"),
])
def test_grade_reliability(score, expected):
    assert _grade_reliability(score) == expected


# --- compute_reliability ---

def test_compute_reliability_returns_required_keys():
    patches = _patch_all()
    _apply(patches)
    try:
        result = compute_reliability("my_pipe")
        for key in ("pipeline", "score", "grade", "uptime_component",
                    "flakiness_component", "sla_component"):
            assert key in result
    finally:
        _stop(patches)


def test_compute_reliability_perfect_score():
    patches = _patch_all(uptime=1.0, flakiness_score=0.0, sla_ok=True)
    _apply(patches)
    try:
        result = compute_reliability("pipe")
        assert result["score"] == pytest.approx(1.0)
        assert result["grade"] == "A"
    finally:
        _stop(patches)


def test_compute_reliability_high_flakiness_lowers_score():
    patches = _patch_all(uptime=1.0, flakiness_score=1.0, sla_ok=True)
    _apply(patches)
    try:
        result = compute_reliability("pipe")
        # flakiness_component = 0.0 => score = 0.50*1 + 0.30*0 + 0.20*1 = 0.70
        assert result["score"] == pytest.approx(0.70)
    finally:
        _stop(patches)


def test_compute_reliability_sla_breach_lowers_score():
    patches = _patch_all(uptime=1.0, flakiness_score=0.0, sla_ok=False)
    # override load_sla_rules so the rule IS present
    patches[2] = patch(
        "pipewatch.run_reliability.load_sla_rules",
        return_value={"pipe": {"max_duration": 60}},
    )
    _apply(patches)
    try:
        result = compute_reliability("pipe")
        # sla_component = 0.0 => score = 0.50 + 0.30 + 0 = 0.80
        assert result["score"] == pytest.approx(0.80)
    finally:
        _stop(patches)


# --- format_reliability_report ---

def test_format_reliability_report_contains_pipeline():
    report = {
        "pipeline": "alpha",
        "score": 0.95,
        "grade": "A",
        "uptime_component": 1.0,
        "flakiness_component": 0.9,
        "sla_component": 1.0,
    }
    text = format_reliability_report(report)
    assert "alpha" in text
    assert "A" in text
