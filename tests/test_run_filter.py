"""Tests for pipewatch.run_filter."""

import pytest
from unittest.mock import patch
from pipewatch.run_filter import filter_runs, format_run_list


SAMPLE_RUNS = [
    {"run_id": "run-3", "started_at": "2024-03-01T10:00:00", "pipeline": "etl", "status": "ok", "exit_code": 0},
    {"run_id": "run-2", "started_at": "2024-02-01T09:00:00", "pipeline": "etl", "status": "failed", "exit_code": 1},
    {"run_id": "run-1", "started_at": "2024-01-01T08:00:00", "pipeline": "ingest", "status": "ok", "exit_code": 0},
]

SAMPLE_TAGS = {
    "run-3": ["prod", "nightly"],
    "run-2": ["prod"],
    "run-1": ["dev"],
}


def _patch(runs=None, tags=None):
    runs = runs if runs is not None else SAMPLE_RUNS
    tags = tags if tags is not None else SAMPLE_TAGS
    return (
        patch("pipewatch.run_filter.list_run_records", return_value=runs),
        patch("pipewatch.run_filter.load_tags", return_value=tags),
    )


def test_filter_runs_no_filters_returns_all():
    p1, p2 = _patch()
    with p1, p2:
        result = filter_runs()
    assert len(result) == 3


def test_filter_runs_by_status():
    p1, p2 = _patch()
    with p1, p2:
        result = filter_runs(status="ok")
    assert all(r["status"] == "ok" for r in result)
    assert len(result) == 2


def test_filter_runs_by_pipeline():
    p1, p2 = _patch()
    with p1, p2:
        result = filter_runs(pipeline="ingest")
    assert len(result) == 1
    assert result[0]["run_id"] == "run-1"


def test_filter_runs_by_single_tag():
    p1, p2 = _patch()
    with p1, p2:
        result = filter_runs(tags=["nightly"])
    assert len(result) == 1
    assert result[0]["run_id"] == "run-3"


def test_filter_runs_by_multiple_tags():
    p1, p2 = _patch()
    with p1, p2:
        result = filter_runs(tags=["prod", "nightly"])
    assert len(result) == 1
    assert result[0]["run_id"] == "run-3"


def test_filter_runs_tag_no_match():
    p1, p2 = _patch()
    with p1, p2:
        result = filter_runs(tags=["nonexistent"])
    assert result == []


def test_filter_runs_since():
    p1, p2 = _patch()
    with p1, p2:
        result = filter_runs(since="2024-02-01")
    assert all(r["started_at"] >= "2024-02-01" for r in result)
    assert len(result) == 2


def test_filter_runs_until():
    p1, p2 = _patch()
    with p1, p2:
        result = filter_runs(until="2024-01-31")
    assert len(result) == 1
    assert result[0]["run_id"] == "run-1"


def test_filter_runs_limit():
    p1, p2 = _patch()
    with p1, p2:
        result = filter_runs(limit=2)
    assert len(result) == 2


def test_format_run_list_empty():
    assert format_run_list([]) == "No runs found."


def test_format_run_list_shows_run_id():
    output = format_run_list(SAMPLE_RUNS)
    assert "run-3" in output
    assert "run-1" in output


def test_format_run_list_with_tags():
    output = format_run_list(SAMPLE_RUNS, all_tags=SAMPLE_TAGS)
    assert "nightly" in output
    assert "prod" in output
