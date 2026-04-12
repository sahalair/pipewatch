"""Tests for pipewatch.run_velocity."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from unittest.mock import patch

import pytest

from pipewatch.run_velocity import (
    compute_velocity,
    format_velocity_report,
    velocity_for_all_pipelines,
)

_NOW = datetime(2024, 6, 1, 12, 0, 0, tzinfo=timezone.utc)


def _iso(dt: datetime) -> str:
    return dt.isoformat()


def _make_run(pipeline: str, finished_offset_hours: float) -> dict:
    finished = _NOW - timedelta(hours=finished_offset_hours)
    return {
        "run_id": f"r-{pipeline}-{finished_offset_hours}",
        "pipeline": pipeline,
        "finished": _iso(finished),
    }


def _patch(runs):
    return patch("pipewatch.run_velocity.list_run_records", return_value=runs)


def _patch_now():
    return patch("pipewatch.run_velocity._now_utc", return_value=_NOW)


def test_compute_velocity_counts_within_window():
    runs = [
        _make_run("etl", 0.5),   # 30 min ago – inside day window
        _make_run("etl", 12),    # 12 h ago  – inside day window
        _make_run("etl", 25),    # 25 h ago  – outside day window
    ]
    with _patch(runs), _patch_now():
        result = compute_velocity("etl", window="day")
    assert result["count"] == 2
    assert result["pipeline"] == "etl"
    assert result["window"] == "day"


def test_compute_velocity_rate_per_hour():
    runs = [_make_run("etl", h) for h in [1, 2, 3, 4]]
    with _patch(runs), _patch_now():
        result = compute_velocity("etl", window="day")
    # 4 runs / 24 hours
    assert abs(result["rate_per_hour"] - 4 / 24) < 1e-6


def test_compute_velocity_hour_window():
    runs = [
        _make_run("pipe", 0.3),   # inside 1-hour window
        _make_run("pipe", 0.9),   # inside 1-hour window
        _make_run("pipe", 1.5),   # outside 1-hour window
    ]
    with _patch(runs), _patch_now():
        result = compute_velocity("pipe", window="hour")
    assert result["count"] == 2


def test_compute_velocity_excludes_unfinished():
    runs = [
        {"run_id": "r1", "pipeline": "etl", "finished": None},
        {"run_id": "r2", "pipeline": "etl"},  # no 'finished' key
        _make_run("etl", 1),
    ]
    with _patch(runs), _patch_now():
        result = compute_velocity("etl", window="day")
    assert result["count"] == 1


def test_compute_velocity_unknown_window_raises():
    with _patch([]), _patch_now():
        with pytest.raises(ValueError, match="Unknown window"):
            compute_velocity("etl", window="month")


def test_velocity_for_all_pipelines_groups_correctly():
    runs = [
        _make_run("etl", 1),
        _make_run("etl", 2),
        _make_run("ml", 3),
    ]
    with _patch(runs), _patch_now():
        results = velocity_for_all_pipelines(window="day")
    pipelines = [r["pipeline"] for r in results]
    assert "etl" in pipelines
    assert "ml" in pipelines
    etl = next(r for r in results if r["pipeline"] == "etl")
    assert etl["count"] == 2


def test_format_velocity_report_empty():
    assert "No velocity" in format_velocity_report([])


def test_format_velocity_report_contains_pipeline_name():
    stats = [{"pipeline": "etl", "window": "day", "count": 5, "rate_per_hour": 0.2083}]
    report = format_velocity_report(stats)
    assert "etl" in report
    assert "day" in report
    assert "5" in report
