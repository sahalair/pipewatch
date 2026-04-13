"""Tests for pipewatch.run_freshness."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from unittest.mock import patch

import pytest

from pipewatch.run_freshness import compute_freshness, format_freshness_report


def _iso(dt: datetime) -> str:
    return dt.isoformat()


def _make_run(pipeline: str, hours_ago: float, finished: bool = True) -> dict:
    now = datetime.now(timezone.utc)
    ts = _iso(now - timedelta(hours=hours_ago))
    run = {"pipeline": pipeline, "started_at": ts}
    if finished:
        run["finished_at"] = ts
    return run


def _patch(runs):
    return patch("pipewatch.run_freshness.list_run_records", return_value=runs)


def _patch_now(dt: datetime):
    return patch("pipewatch.run_freshness._now_utc", return_value=dt)


# ---------------------------------------------------------------------------
# compute_freshness
# ---------------------------------------------------------------------------

def test_compute_freshness_returns_required_keys():
    with _patch([_make_run("etl", 1.0)]):
        result = compute_freshness("etl")
    for key in ("pipeline", "last_run_at", "age_hours", "is_fresh", "max_age_hours", "grade"):
        assert key in result


def test_compute_freshness_no_runs_is_unknown():
    with _patch([]):
        result = compute_freshness("etl")
    assert result["grade"] == "unknown"
    assert result["last_run_at"] is None
    assert result["is_fresh"] is False


def test_compute_freshness_fresh_run():
    now = datetime.now(timezone.utc)
    runs = [_make_run("etl", 2.0)]
    with _patch(runs), _patch_now(now):
        result = compute_freshness("etl", max_age_hours=24.0)
    assert result["is_fresh"] is True
    assert result["grade"] == "fresh"
    assert result["age_hours"] is not None
    assert result["age_hours"] < 24.0


def test_compute_freshness_stale_run():
    now = datetime.now(timezone.utc)
    runs = [_make_run("etl", 50.0)]
    with _patch(runs), _patch_now(now):
        result = compute_freshness("etl", max_age_hours=24.0)
    assert result["is_fresh"] is False
    assert result["grade"] == "stale"


def test_compute_freshness_picks_most_recent():
    now = datetime.now(timezone.utc)
    runs = [_make_run("etl", 30.0), _make_run("etl", 1.0)]
    with _patch(runs), _patch_now(now):
        result = compute_freshness("etl", max_age_hours=24.0)
    assert result["is_fresh"] is True


def test_compute_freshness_filters_by_pipeline():
    runs = [_make_run("other", 1.0)]
    with _patch(runs):
        result = compute_freshness("etl")
    assert result["grade"] == "unknown"


def test_compute_freshness_uses_started_at_fallback():
    now = datetime.now(timezone.utc)
    run = _make_run("etl", 2.0, finished=False)
    with _patch([run]), _patch_now(now):
        result = compute_freshness("etl", max_age_hours=24.0)
    assert result["is_fresh"] is True


# ---------------------------------------------------------------------------
# format_freshness_report
# ---------------------------------------------------------------------------

def test_format_freshness_report_contains_pipeline():
    with _patch([_make_run("etl", 1.0)]):
        result = compute_freshness("etl")
    report = format_freshness_report(result)
    assert "etl" in report


def test_format_freshness_report_shows_grade():
    with _patch([_make_run("etl", 1.0)]):
        result = compute_freshness("etl")
    report = format_freshness_report(result)
    assert result["grade"].upper() in report


def test_format_freshness_report_unknown_shows_never():
    with _patch([]):
        result = compute_freshness("etl")
    report = format_freshness_report(result)
    assert "never" in report
