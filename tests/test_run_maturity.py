"""Tests for pipewatch.run_maturity."""

import pytest
from unittest.mock import patch, MagicMock

from pipewatch.run_maturity import compute_maturity, format_maturity_report, _grade


PIPELINE = "alpha"
BASE = ".pipewatch"


def _patch_all(
    records=None,
    stability=None,
    baseline=None,
    owner=None,
    annotations=None,
):
    records = records or []
    stability = stability or {"score": 0.0}
    return [
        patch("pipewatch.run_maturity.list_run_records", return_value=records),
        patch("pipewatch.run_maturity.compute_stability_score", return_value=stability),
        patch("pipewatch.run_maturity.get_baseline", return_value=baseline),
        patch("pipewatch.run_maturity.get_owner", return_value=owner),
        patch("pipewatch.run_maturity.get_annotations", return_value=annotations),
    ]


def _apply(patches):
    for p in patches:
        p.start()
    return patches


def _stop(patches):
    for p in patches:
        p.stop()


def test_compute_maturity_returns_required_keys():
    patches = _apply(_patch_all())
    try:
        result = compute_maturity(PIPELINE, base_dir=BASE)
        assert "pipeline" in result
        assert "score" in result
        assert "grade" in result
        assert "run_count" in result
        assert "components" in result
    finally:
        _stop(patches)


def test_compute_maturity_no_runs_low_score():
    patches = _apply(_patch_all(records=[]))
    try:
        result = compute_maturity(PIPELINE, base_dir=BASE)
        assert result["score"] < 30
        assert result["run_count"] == 0
    finally:
        _stop(patches)


def test_compute_maturity_perfect_score():
    runs = [{"pipeline": PIPELINE, "status": "ok"} for _ in range(50)]
    patches = _apply(_patch_all(
        records=runs,
        stability={"score": 100.0},
        baseline={"value": 1.0},
        owner={"owner": "alice"},
        annotations={"note": "important"},
    ))
    try:
        result = compute_maturity(PIPELINE, base_dir=BASE)
        assert result["score"] == 100.0
        assert result["grade"] == "platinum"
    finally:
        _stop(patches)


def test_compute_maturity_filters_by_pipeline():
    runs = [
        {"pipeline": PIPELINE},
        {"pipeline": "other"},
        {"pipeline": PIPELINE},
    ]
    patches = _apply(_patch_all(records=runs))
    try:
        result = compute_maturity(PIPELINE, base_dir=BASE)
        assert result["run_count"] == 2
    finally:
        _stop(patches)


def test_grade_thresholds():
    assert _grade(95) == "platinum"
    assert _grade(75) == "gold"
    assert _grade(55) == "silver"
    assert _grade(35) == "bronze"
    assert _grade(10) == "emerging"
    assert _grade(0) == "emerging"


def test_format_maturity_report_contains_pipeline():
    report = {
        "pipeline": PIPELINE,
        "score": 72.5,
        "grade": "gold",
        "run_count": 20,
        "components": {
            "volume": 10.0,
            "stability": 40.0,
            "baseline": 10.0,
            "ownership": 10.0,
            "annotations": 5.0,
        },
    }
    text = format_maturity_report(report)
    assert PIPELINE in text
    assert "72.5" in text
    assert "GOLD" in text
    assert "volume" in text
