"""Tests for pipewatch.run_reputation."""

import pytest
from unittest.mock import patch

from pipewatch.run_reputation import compute_reputation, format_reputation_report


FLAKINESS_STABLE = {"score": 0.0, "label": "stable", "run_count": 10}
FLAKINESS_FLAKY  = {"score": 0.8, "label": "very flaky", "run_count": 10}
STABILITY_PERFECT = {"score": 100.0, "grade": "A"}
STABILITY_LOW     = {"score": 40.0, "grade": "D"}


def _patch_all(flakiness=None, stability=None):
    flakiness = flakiness or FLAKINESS_STABLE
    stability = stability or STABILITY_PERFECT
    return [
        patch("pipewatch.run_reputation.analyze_pipeline_flakiness", return_value=flakiness),
        patch("pipewatch.run_reputation.compute_stability_score", return_value=stability),
    ]


def _run_compute_reputation(pipeline_name, flakiness=None, stability=None):
    """Helper to run compute_reputation with patched dependencies."""
    patches = _patch_all(flakiness=flakiness, stability=stability)
    for p in patches:
        p.start()
    try:
        return compute_reputation(pipeline_name)
    finally:
        for p in patches:
            p.stop()


def test_compute_reputation_returns_required_keys():
    rep = _run_compute_reputation("my_pipeline")
    for key in ("pipeline", "score", "grade", "flakiness_score",
                "flakiness_label", "stability_score", "stability_grade", "run_count"):
        assert key in rep


def test_compute_reputation_perfect_score():
    rep = _run_compute_reputation("pipeline_a")
    assert rep["score"] == 100.0
    assert rep["grade"] == "excellent"


def test_compute_reputation_flaky_lowers_score():
    rep = _run_compute_reputation("pipeline_b", flakiness=FLAKINESS_FLAKY, stability=STABILITY_PERFECT)
    assert rep["score"] < 100.0


def test_compute_reputation_low_stability():
    rep = _run_compute_reputation("pipeline_c", flakiness=FLAKINESS_STABLE, stability=STABILITY_LOW)
    assert rep["score"] <= 40.0
    assert rep["grade"] in ("poor", "critical", "fair")


def test_compute_reputation_score_never_negative():
    rep = _run_compute_reputation(
        "pipeline_d",
        flakiness={"score": 1.0, "label": "very flaky", "run_count": 5},
        stability={"score": 0.0, "grade": "F"},
    )
    assert rep["score"] >= 0.0


def test_format_reputation_report_contains_pipeline_name():
    rep = {
        "pipeline": "my_pipe",
        "score": 85.0,
        "grade": "good",
        "stability_score": 90.0,
        "stability_grade": "A",
        "flakiness_score": 0.1,
        "flakiness_label": "mostly stable",
        "run_count": 15,
    }
    report = format_reputation_report(rep)
    assert "my_pipe" in report
    assert "85.0" in report
