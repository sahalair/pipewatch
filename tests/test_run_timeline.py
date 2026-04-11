"""Tests for pipewatch.run_timeline."""

from __future__ import annotations

from unittest.mock import patch

import pytest

from pipewatch.run_timeline import (
    build_timeline_entries,
    format_timeline,
    get_pipeline_timeline,
)

_MODULE = "pipewatch.run_timeline"


def _make_run(run_id: str, pipeline: str, status: str = "success", started: str = "2024-01-01T10:00:00", finished: str = "2024-01-01T10:01:30") -> dict:
    return {
        "run_id": run_id,
        "pipeline": pipeline,
        "status": status,
        "started_at": started,
        "finished_at": finished,
    }


@patch(f"{_MODULE}.list_run_records")
def test_get_pipeline_timeline_filters_by_pipeline(mock_list):
    mock_list.return_value = [
        _make_run("r3", "etl"),
        _make_run("r2", "other"),
        _make_run("r1", "etl"),
    ]
    result = get_pipeline_timeline("etl")
    assert all(r["pipeline"] == "etl" for r in result)
    assert len(result) == 2


@patch(f"{_MODULE}.list_run_records")
def test_get_pipeline_timeline_oldest_first(mock_list):
    mock_list.return_value = [
        _make_run("r3", "etl"),
        _make_run("r2", "etl"),
        _make_run("r1", "etl"),
    ]
    result = get_pipeline_timeline("etl")
    assert result[0]["run_id"] == "r1"
    assert result[-1]["run_id"] == "r3"


@patch(f"{_MODULE}.list_run_records")
def test_get_pipeline_timeline_respects_limit(mock_list):
    mock_list.return_value = [_make_run(f"r{i}", "etl") for i in range(10, 0, -1)]
    result = get_pipeline_timeline("etl", limit=5)
    assert len(result) == 5


@patch(f"{_MODULE}.list_run_records")
def test_get_pipeline_timeline_empty(mock_list):
    mock_list.return_value = []
    result = get_pipeline_timeline("etl")
    assert result == []


def test_build_timeline_entries_index_starts_at_one():
    runs = [_make_run("r1", "etl"), _make_run("r2", "etl")]
    entries = build_timeline_entries(runs)
    assert entries[0]["index"] == 1
    assert entries[1]["index"] == 2


def test_build_timeline_entries_computes_duration():
    runs = [_make_run("r1", "etl", started="2024-01-01T10:00:00", finished="2024-01-01T10:01:30")]
    entries = build_timeline_entries(runs)
    assert entries[0]["duration_seconds"] == 90.0


def test_build_timeline_entries_duration_none_when_missing():
    run = _make_run("r1", "etl")
    run["finished_at"] = ""
    entries = build_timeline_entries([run])
    assert entries[0]["duration_seconds"] is None


def test_build_timeline_entries_required_keys():
    entries = build_timeline_entries([_make_run("r1", "etl")])
    for key in ("index", "run_id", "status", "started_at", "finished_at", "duration_seconds", "pipeline"):
        assert key in entries[0]


def test_format_timeline_empty():
    assert format_timeline([]) == "No runs found."


def test_format_timeline_contains_run_id():
    entries = build_timeline_entries([_make_run("abc-123", "etl")])
    output = format_timeline(entries)
    assert "abc-123" in output


def test_format_timeline_contains_status():
    entries = build_timeline_entries([_make_run("r1", "etl", status="failed")])
    output = format_timeline(entries)
    assert "failed" in output
