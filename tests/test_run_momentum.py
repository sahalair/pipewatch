"""Tests for pipewatch.run_momentum."""

from __future__ import annotations

from unittest.mock import patch

import pytest

from pipewatch.run_momentum import compute_momentum, format_momentum_report


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------

def _make_run(pipeline: str, status: str = "ok", started: str = "2024-01-01T00:00:00") -> dict:
    return {
        "run_id": f"r-{pipeline}-{started}",
        "pipeline": pipeline,
        "status": status,
        "started": started,
        "finished": started,
    }


def _patch_all(runs, flakiness="stable", velocity_count=5):
    """Return a context manager that patches all external calls."""
    import contextlib

    @contextlib.contextmanager
    def _ctx():
        with patch(
            "pipewatch.run_momentum.list_run_records", return_value=runs
        ), patch(
            "pipewatch.run_momentum.analyze_pipeline_flakiness",
            return_value={"classification": flakiness},
        ), patch(
            "pipewatch.run_momentum.compute_velocity",
            return_value={"count": velocity_count},
        ):
            yield

    return _ctx()


# ---------------------------------------------------------------------------
# compute_momentum
# ---------------------------------------------------------------------------

def test_compute_momentum_returns_required_keys():
    runs = [_make_run("etl") for _ in range(5)]
    with _patch_all(runs):
        result = compute_momentum("etl")
    required = {"pipeline", "momentum", "grade", "success_rate", "velocity", "flakiness", "components"}
    assert required.issubset(result.keys())


def test_compute_momentum_no_runs_returns_zero():
    with _patch_all([]):
        result = compute_momentum("ghost")
    assert result["momentum"] == 0.0
    assert result["grade"] == "F"


def test_compute_momentum_perfect_score():
    runs = [_make_run("etl") for _ in range(20)]
    with _patch_all(runs, flakiness="stable", velocity_count=20):
        result = compute_momentum("etl", limit=20)
    # success=50, velocity=25, flakiness=25 => 100
    assert result["momentum"] == 100.0
    assert result["grade"] == "A"


def test_compute_momentum_all_failed_lowers_score():
    runs = [_make_run("etl", status="failed") for _ in range(10)]
    with _patch_all(runs, flakiness="high", velocity_count=2):
        result = compute_momentum("etl")
    assert result["momentum"] < 20.0


def test_compute_momentum_grade_b():
    runs = [_make_run("etl") for _ in range(10)]
    # success=50, velocity=12.5, flakiness=20 => 82.5
    with _patch_all(runs, flakiness="low", velocity_count=10):
        result = compute_momentum("etl", limit=20)
    assert result["grade"] in {"A", "B"}


def test_compute_momentum_flakiness_very_high_penalises():
    runs = [_make_run("etl") for _ in range(10)]
    with _patch_all(runs, flakiness="very_high", velocity_count=5):
        result = compute_momentum("etl")
    assert result["components"]["flakiness"] == 0.0


def test_compute_momentum_pipeline_field_matches():
    runs = [_make_run("ingest")]
    with _patch_all(runs):
        result = compute_momentum("ingest")
    assert result["pipeline"] == "ingest"


# ---------------------------------------------------------------------------
# format_momentum_report
# ---------------------------------------------------------------------------

def test_format_momentum_report_contains_pipeline():
    runs = [_make_run("etl")]
    with _patch_all(runs):
        result = compute_momentum("etl")
    report = format_momentum_report(result)
    assert "etl" in report


def test_format_momentum_report_contains_grade():
    runs = [_make_run("etl") for _ in range(20)]
    with _patch_all(runs, flakiness="stable", velocity_count=20):
        result = compute_momentum("etl", limit=20)
    report = format_momentum_report(result)
    assert result["grade"] in report


def test_format_momentum_report_contains_components():
    runs = [_make_run("etl")]
    with _patch_all(runs):
        result = compute_momentum("etl")
    report = format_momentum_report(result)
    assert "Components" in report
    assert "success" in report
    assert "velocity" in report
    assert "flakiness" in report
