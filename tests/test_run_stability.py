"""Tests for pipewatch.run_stability."""

import pytest
from unittest.mock import patch
from pipewatch.run_stability import (
    compute_stability_score,
    format_stability_report,
    _grade,
)

_BASE = ".pipewatch"


def _patch_all(flakiness_cls="stable", is_anomaly=False, sla_rules=None, sla_passed=True):
    """Return a context-manager stack that stubs all three sub-components."""
    sla_rules = sla_rules or []

    def _check_sla(rule, base_dir=_BASE):
        return {"passed": sla_passed}

    return (
        patch(
            "pipewatch.run_stability.analyze_pipeline_flakiness",
            return_value={"classification": flakiness_cls},
        ),
        patch(
            "pipewatch.run_stability.detect_anomaly",
            return_value={"is_anomaly": is_anomaly},
        ),
        patch("pipewatch.run_stability.load_sla_rules", return_value=sla_rules),
        patch("pipewatch.run_stability.check_sla", side_effect=_check_sla),
    )


def test_compute_stability_score_returns_required_keys():
    patches = _patch_all()
    with patches[0], patches[1], patches[2], patches[3]:
        result = compute_stability_score("my_pipeline")
    assert "pipeline" in result
    assert "score" in result
    assert "components" in result
    assert "grade" in result


def test_compute_stability_score_perfect():
    patches = _patch_all(flakiness_cls="stable", is_anomaly=False, sla_rules=[])
    with patches[0], patches[1], patches[2], patches[3]:
        result = compute_stability_score("p")
    # flakiness=1.0, anomaly=1.0, sla=1.0 (no rules) => 100
    assert result["score"] == 100.0
    assert result["grade"] == "A"


def test_compute_stability_score_very_flaky_lowers_score():
    patches = _patch_all(flakiness_cls="very_flaky", is_anomaly=False, sla_rules=[])
    with patches[0], patches[1], patches[2], patches[3]:
        result = compute_stability_score("p")
    assert result["score"] < 80


def test_compute_stability_score_anomaly_detected_lowers_score():
    patches = _patch_all(flakiness_cls="stable", is_anomaly=True, sla_rules=[])
    with patches[0], patches[1], patches[2], patches[3]:
        result = compute_stability_score("p")
    assert result["score"] < 100


def test_compute_stability_score_sla_failure_lowers_score():
    rules = [{"pipeline": "p", "max_duration": 60}]
    patches = _patch_all(flakiness_cls="stable", is_anomaly=False, sla_rules=rules, sla_passed=False)
    with patches[0], patches[1], patches[2], patches[3]:
        result = compute_stability_score("p")
    assert result["components"]["sla"] == 0.0


def test_compute_stability_score_pipeline_name_preserved():
    patches = _patch_all()
    with patches[0], patches[1], patches[2], patches[3]:
        result = compute_stability_score("etl_pipeline")
    assert result["pipeline"] == "etl_pipeline"


@pytest.mark.parametrize(
    "score,expected",
    [(95, "A"), (80, "B"), (60, "C"), (40, "D"), (20, "F")],
)
def test_grade_thresholds(score, expected):
    assert _grade(score) == expected


def test_format_stability_report_contains_pipeline_name():
    result = {
        "pipeline": "my_pipe",
        "score": 82.5,
        "grade": "B",
        "components": {"flakiness": 90.0, "anomaly": 70.0, "sla": 80.0},
    }
    report = format_stability_report(result)
    assert "my_pipe" in report
    assert "82.5" in report
    assert "Grade: B" in report
