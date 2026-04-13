"""Tests for run_stale_detector."""

import json
import os
import pytest
from datetime import datetime, timezone, timedelta
from unittest.mock import patch
from pipewatch.run_stale_detector import detect_stuck_runs, format_stale_report


def _iso(dt: datetime) -> str:
    return dt.isoformat()


def _make_run(run_id, pipeline, started_delta_minutes, status=None):
    started = datetime.now(timezone.utc) - timedelta(minutes=started_delta_minutes)
    run = {
        "run_id": run_id,
        "pipeline": pipeline,
        "started_at": _iso(started),
        "status": status,
    }
    return run


def _patch(runs):
    return patch("pipewatch.run_stale_detector.list_run_records", return_value=runs)


def test_detect_stuck_runs_finds_long_running():
    runs = [_make_run("r1", "etl", 120, status=None)]
    with _patch(runs):
        result = detect_stuck_runs(timeout_minutes=60)
    assert len(result) == 1
    assert result[0]["run_id"] == "r1"


def test_detect_stuck_runs_ignores_recent():
    runs = [_make_run("r2", "etl", 10, status=None)]
    with _patch(runs):
        result = detect_stuck_runs(timeout_minutes=60)
    assert result == []


def test_detect_stuck_runs_ignores_finished():
    runs = [_make_run("r3", "etl", 120, status="success")]
    with _patch(runs):
        result = detect_stuck_runs(timeout_minutes=60)
    assert result == []


def test_detect_stuck_runs_ignores_failed():
    runs = [_make_run("r4", "etl", 120, status="failed")]
    with _patch(runs):
        result = detect_stuck_runs(timeout_minutes=60)
    assert result == []


def test_detect_stuck_runs_filters_by_pipeline():
    runs = [
        _make_run("r5", "etl", 120, status=None),
        _make_run("r6", "ml", 120, status=None),
    ]
    with _patch(runs):
        result = detect_stuck_runs(pipeline="etl", timeout_minutes=60)
    assert len(result) == 1
    assert result[0]["pipeline"] == "etl"


def test_detect_stuck_runs_attaches_stuck_minutes():
    runs = [_make_run("r7", "etl", 90, status=None)]
    with _patch(runs):
        result = detect_stuck_runs(timeout_minutes=60)
    assert "stuck_for_minutes" in result[0]
    assert result[0]["stuck_for_minutes"] >= 90


def test_detect_stuck_runs_running_status_counts():
    runs = [_make_run("r8", "etl", 120, status="running")]
    with _patch(runs):
        result = detect_stuck_runs(timeout_minutes=60)
    assert len(result) == 1


def test_format_stale_report_empty():
    assert format_stale_report([]) == "No stuck runs detected."


def test_format_stale_report_shows_run_info():
    runs = [_make_run("r9", "etl", 90, status=None)]
    with _patch(runs):
        stuck = detect_stuck_runs(timeout_minutes=60)
    report = format_stale_report(stuck)
    assert "r9" in report
    assert "etl" in report
    assert "Stuck" in report
