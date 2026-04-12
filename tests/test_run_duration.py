"""Tests for pipewatch.run_duration."""

from unittest.mock import patch

import pytest

from pipewatch.run_duration import (
    format_duration_report,
    get_durations_for_pipeline,
    get_run_duration,
    summarize_durations,
)

_REC_A = {
    "run_id": "run-a",
    "pipeline": "etl",
    "started_at": "2024-01-01T10:00:00",
    "finished_at": "2024-01-01T10:00:30",
    "status": "success",
}
_REC_B = {
    "run_id": "run-b",
    "pipeline": "etl",
    "started_at": "2024-01-02T10:00:00",
    "finished_at": "2024-01-02T10:01:00",
    "status": "success",
}
_REC_NO_FINISH = {
    "run_id": "run-c",
    "pipeline": "etl",
    "started_at": "2024-01-03T10:00:00",
    "finished_at": None,
    "status": "running",
}


def test_get_run_duration_returns_seconds():
    with patch("pipewatch.run_duration.load_run_record", return_value=_REC_A):
        dur = get_run_duration("run-a")
    assert dur == 30.0


def test_get_run_duration_missing_finished_returns_none():
    with patch("pipewatch.run_duration.load_run_record", return_value=_REC_NO_FINISH):
        dur = get_run_duration("run-c")
    assert dur is None


def test_get_durations_for_pipeline_filters_correctly():
    other = {**_REC_A, "run_id": "run-x", "pipeline": "other"}
    with patch(
        "pipewatch.run_duration.list_run_records", return_value=[_REC_A, _REC_B, other]
    ):
        results = get_durations_for_pipeline("etl")
    assert len(results) == 2
    run_ids = [r["run_id"] for r in results]
    assert "run-a" in run_ids
    assert "run-b" in run_ids
    assert "run-x" not in run_ids


def test_get_durations_for_pipeline_skips_incomplete():
    with patch(
        "pipewatch.run_duration.list_run_records",
        return_value=[_REC_A, _REC_NO_FINISH],
    ):
        results = get_durations_for_pipeline("etl")
    assert len(results) == 1
    assert results[0]["run_id"] == "run-a"


def test_get_durations_for_pipeline_respects_limit():
    many = [_REC_A, _REC_B]
    with patch("pipewatch.run_duration.list_run_records", return_value=many):
        results = get_durations_for_pipeline("etl", limit=1)
    assert len(results) == 1


def test_summarize_durations_empty():
    summary = summarize_durations([])
    assert summary["count"] == 0
    assert summary["min"] is None
    assert summary["avg"] is None


def test_summarize_durations_values():
    durations = [
        {"run_id": "a", "duration_seconds": 10.0},
        {"run_id": "b", "duration_seconds": 20.0},
        {"run_id": "c", "duration_seconds": 30.0},
    ]
    summary = summarize_durations(durations)
    assert summary["count"] == 3
    assert summary["min"] == 10.0
    assert summary["max"] == 30.0
    assert summary["avg"] == 20.0


def test_format_duration_report_no_runs():
    report = format_duration_report("etl", {"count": 0, "min": None, "max": None, "avg": None})
    assert "etl" in report
    assert "No completed runs" in report


def test_format_duration_report_with_data():
    summary = {"count": 2, "min": 10.0, "max": 30.0, "avg": 20.0}
    report = format_duration_report("etl", summary)
    assert "etl" in report
    assert "10.0" in report
    assert "30.0" in report
    assert "20.0" in report
