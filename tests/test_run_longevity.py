"""Tests for run_longevity."""
import pytest
from unittest.mock import patch
from datetime import datetime, timezone
from pipewatch.run_longevity import (
    compute_longevity,
    format_longevity_report,
    _grade_longevity,
)


def _iso(dt: datetime) -> str:
    return dt.isoformat()


def _make_run(pipeline: str, started: str, status: str = "ok") -> dict:
    return {"pipeline": pipeline, "started": started, "status": status, "run_id": started}


_PATCH = "pipewatch.run_longevity.list_run_records"
_NOW = "pipewatch.run_longevity._now_utc"
_FIXED_NOW = datetime(2024, 6, 15, 12, 0, 0, tzinfo=timezone.utc)


def test_compute_longevity_no_runs():
    with patch(_PATCH, return_value=[]):
        result = compute_longevity("pipe_x")
    assert result["run_count"] == 0
    assert result["grade"] == "N/A"
    assert result["first_run"] is None


def test_compute_longevity_returns_required_keys():
    runs = [_make_run("p", "2024-06-01T10:00:00")]
    with patch(_PATCH, return_value=runs), patch(_NOW, return_value=_FIXED_NOW):
        result = compute_longevity("p")
    for key in ("pipeline", "run_count", "first_run", "last_run", "age_days", "active_days", "activity_ratio", "grade"):
        assert key in result


def test_compute_longevity_single_run_is_new():
    runs = [_make_run("p", "2024-06-14T10:00:00")]
    with patch(_PATCH, return_value=runs), patch(_NOW, return_value=_FIXED_NOW):
        result = compute_longevity("p")
    assert result["grade"] == "New"
    assert result["run_count"] == 1


def test_compute_longevity_high_activity_ratio_is_grade_a():
    # 10 unique days over 11 days = ~0.91
    runs = [_make_run("p", f"2024-06-0{i}T10:00:00") for i in range(1, 10)]
    runs += [_make_run("p", "2024-06-11T10:00:00")]
    with patch(_PATCH, return_value=runs), patch(_NOW, return_value=_FIXED_NOW):
        result = compute_longevity("p")
    assert result["grade"] == "A"


def test_compute_longevity_filters_by_pipeline():
    runs = [
        _make_run("pipe_a", "2024-06-01T10:00:00"),
        _make_run("pipe_b", "2024-06-02T10:00:00"),
    ]
    with patch(_PATCH, return_value=runs), patch(_NOW, return_value=_FIXED_NOW):
        result = compute_longevity("pipe_a")
    assert result["run_count"] == 1


def test_compute_longevity_active_days_counts_unique():
    runs = [
        _make_run("p", "2024-06-01T08:00:00"),
        _make_run("p", "2024-06-01T20:00:00"),
        _make_run("p", "2024-06-02T10:00:00"),
    ]
    with patch(_PATCH, return_value=runs), patch(_NOW, return_value=_FIXED_NOW):
        result = compute_longevity("p")
    assert result["active_days"] == 2
    assert result["run_count"] == 3


def test_grade_longevity_new():
    assert _grade_longevity(0.9, 3) == "New"


def test_grade_longevity_boundaries():
    assert _grade_longevity(1.0, 30) == "A"
    assert _grade_longevity(0.6, 30) == "B"
    assert _grade_longevity(0.4, 30) == "C"
    assert _grade_longevity(0.2, 30) == "D"
    assert _grade_longevity(0.1, 30) == "F"


def test_format_longevity_report_no_runs():
    result = {"pipeline": "p", "run_count": 0}
    out = format_longevity_report(result)
    assert "no runs" in out


def test_format_longevity_report_has_pipeline():
    runs = [_make_run("mypipe", "2024-01-01T00:00:00")]
    with patch(_PATCH, return_value=runs), patch(_NOW, return_value=_FIXED_NOW):
        result = compute_longevity("mypipe")
    out = format_longevity_report(result)
    assert "mypipe" in out
    assert "Grade" in out
