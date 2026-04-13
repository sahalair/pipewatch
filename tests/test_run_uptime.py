"""Tests for pipewatch.run_uptime."""

from __future__ import annotations

from unittest.mock import patch

import pytest

from pipewatch.run_uptime import compute_uptime, format_uptime_report

_MODULE = "pipewatch.run_uptime.list_run_records"


def _make_run(pipeline: str, status: str, exit_code: int) -> dict:
    return {"pipeline": pipeline, "status": status, "exit_code": exit_code}


def _patch(runs):
    return patch(_MODULE, return_value=runs)


def test_compute_uptime_no_runs_returns_none_ratio():
    with _patch([]):
        result = compute_uptime(pipeline="etl")
    assert result["total"] == 0
    assert result["uptime_ratio"] is None
    assert result["grade"] == "N/A"


def test_compute_uptime_all_successful():
    runs = [_make_run("etl", "ok", 0) for _ in range(10)]
    with _patch(runs):
        result = compute_uptime(pipeline="etl")
    assert result["total"] == 10
    assert result["successful"] == 10
    assert result["failed"] == 0
    assert result["uptime_ratio"] == 1.0
    assert result["grade"] == "A"


def test_compute_uptime_all_failed():
    runs = [_make_run("etl", "failed", 1) for _ in range(5)]
    with _patch(runs):
        result = compute_uptime(pipeline="etl")
    assert result["successful"] == 0
    assert result["failed"] == 5
    assert result["uptime_ratio"] == 0.0
    assert result["grade"] == "F"


def test_compute_uptime_filters_by_pipeline():
    runs = [
        _make_run("etl", "ok", 0),
        _make_run("etl", "ok", 0),
        _make_run("other", "failed", 1),
    ]
    with _patch(runs):
        result = compute_uptime(pipeline="etl")
    assert result["total"] == 2
    assert result["successful"] == 2


def test_compute_uptime_no_pipeline_filter_uses_all():
    runs = [
        _make_run("etl", "ok", 0),
        _make_run("other", "failed", 1),
    ]
    with _patch(runs):
        result = compute_uptime(pipeline=None)
    assert result["total"] == 2
    assert result["successful"] == 1


def test_compute_uptime_respects_limit():
    runs = [_make_run("etl", "ok", 0) for _ in range(50)] + [
        _make_run("etl", "failed", 1) for _ in range(50)
    ]
    with _patch(runs):
        result = compute_uptime(pipeline="etl", limit=10)
    assert result["total"] == 10


def test_compute_uptime_grade_b():
    ok = [_make_run("p", "ok", 0)] * 96
    fail = [_make_run("p", "failed", 1)] * 4
    with _patch(ok + fail):
        result = compute_uptime(pipeline="p")
    assert result["grade"] == "B"


def test_compute_uptime_returns_required_keys():
    with _patch([_make_run("p", "ok", 0)]):
        result = compute_uptime(pipeline="p")
    for key in ("pipeline", "total", "successful", "failed", "uptime_ratio", "grade"):
        assert key in result


def test_format_uptime_report_no_runs():
    result = {"pipeline": "etl", "total": 0, "uptime_ratio": None, "grade": "N/A",
              "successful": 0, "failed": 0}
    text = format_uptime_report(result)
    assert "no runs" in text


def test_format_uptime_report_shows_percentage():
    result = {"pipeline": "etl", "total": 10, "successful": 9, "failed": 1,
              "uptime_ratio": 0.9, "grade": "C"}
    text = format_uptime_report(result)
    assert "90.0%" in text
    assert "Grade: C" in text
    assert "etl" in text
