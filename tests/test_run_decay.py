"""Tests for pipewatch/run_decay.py"""

import math
from datetime import datetime, timedelta, timezone
from unittest.mock import patch

import pytest

from pipewatch.run_decay import (
    _grade_decay,
    compute_decay_score,
    format_decay_report,
)


def _iso(dt: datetime) -> str:
    return dt.isoformat()


def _make_run(pipeline, status, finished_offset_hours):
    now = datetime.now(timezone.utc)
    finished = now - timedelta(hours=finished_offset_hours)
    return {"pipeline": pipeline, "status": status, "finished": _iso(finished)}


_PATCH_RECORDS = "pipewatch.run_decay.list_run_records"
_PATCH_NOW = "pipewatch.run_decay._now_utc"


def test_compute_decay_score_no_runs_returns_zero():
    with patch(_PATCH_RECORDS, return_value=[]):
        result = compute_decay_score("my_pipe")
    assert result["score"] == 0.0
    assert result["grade"] == "stale"
    assert result["last_success_iso"] is None


def test_compute_decay_score_returns_required_keys():
    with patch(_PATCH_RECORDS, return_value=[]):
        result = compute_decay_score("pipe")
    for key in ("pipeline", "score", "grade", "last_success_iso", "hours_since_success"):
        assert key in result


def test_compute_decay_score_recent_run_is_fresh():
    runs = [_make_run("pipe", "ok", 0.1)]
    now = datetime.now(timezone.utc)
    with patch(_PATCH_RECORDS, return_value=runs), patch(_PATCH_NOW, return_value=now):
        result = compute_decay_score("pipe", half_life_hours=24.0)
    assert result["score"] > 0.95
    assert result["grade"] == "fresh"


def test_compute_decay_score_old_run_is_stale():
    runs = [_make_run("pipe", "ok", 120)]
    now = datetime.now(timezone.utc)
    with patch(_PATCH_RECORDS, return_value=runs), patch(_PATCH_NOW, return_value=now):
        result = compute_decay_score("pipe", half_life_hours=24.0)
    assert result["score"] < 0.1


def test_compute_decay_score_ignores_failed_runs():
    runs = [
        _make_run("pipe", "failed", 0.5),
        _make_run("pipe", "ok", 50),
    ]
    now = datetime.now(timezone.utc)
    with patch(_PATCH_RECORDS, return_value=runs), patch(_PATCH_NOW, return_value=now):
        result = compute_decay_score("pipe", half_life_hours=24.0)
    assert result["hours_since_success"] >= 50


def test_compute_decay_score_uses_most_recent_success():
    runs = [
        _make_run("pipe", "ok", 48),
        _make_run("pipe", "ok", 2),
    ]
    now = datetime.now(timezone.utc)
    with patch(_PATCH_RECORDS, return_value=runs), patch(_PATCH_NOW, return_value=now):
        result = compute_decay_score("pipe", half_life_hours=24.0)
    assert result["hours_since_success"] < 10


def test_compute_decay_score_invalid_half_life_raises():
    with pytest.raises(ValueError):
        compute_decay_score("pipe", half_life_hours=0)


def test_grade_decay_boundaries():
    assert _grade_decay(1.0) == "fresh"
    assert _grade_decay(0.75) == "fresh"
    assert _grade_decay(0.74) == "aging"
    assert _grade_decay(0.40) == "aging"
    assert _grade_decay(0.39) == "stale"
    assert _grade_decay(0.10) == "stale"
    assert _grade_decay(0.09) == "expired"
    assert _grade_decay(0.0) == "expired"


def test_format_decay_report_contains_pipeline():
    result = {"pipeline": "etl", "score": 0.85, "grade": "fresh",
              "last_success_iso": "2024-01-01T00:00:00", "hours_since_success": 3.5}
    report = format_decay_report(result)
    assert "etl" in report
    assert "FRESH" in report
    assert "3.5h" in report


def test_format_decay_report_no_success():
    result = {"pipeline": "etl", "score": 0.0, "grade": "stale",
              "last_success_iso": None, "hours_since_success": None}
    report = format_decay_report(result)
    assert "never" in report
