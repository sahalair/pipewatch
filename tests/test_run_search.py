"""Tests for pipewatch.run_search."""

from __future__ import annotations

from unittest.mock import patch

import pytest

from pipewatch.run_search import search_runs, format_search_results


FAKE_RUNS = [
    {"run_id": "run-001", "pipeline": "etl", "status": "success", "started_at": "2024-01-01T10:00:00", "finished_at": "2024-01-01T10:05:00"},
    {"run_id": "run-002", "pipeline": "ml-train", "status": "failed", "started_at": "2024-01-02T09:00:00", "finished_at": "2024-01-02T09:30:00"},
    {"run_id": "run-003", "pipeline": "etl", "status": "failed", "started_at": "2024-01-03T08:00:00", "finished_at": None},
]


def _patch_all(monkeypatch, notes=None, annotations=None):
    monkeypatch.setattr("pipewatch.run_search.list_run_records", lambda data_dir=".pipewatch": FAKE_RUNS)
    monkeypatch.setattr("pipewatch.run_search.get_notes", lambda run_id, data_dir=".pipewatch": notes or [])
    monkeypatch.setattr("pipewatch.run_search.get_annotations", lambda run_id, data_dir=".pipewatch": annotations or {})


def test_search_runs_by_query_pipeline(monkeypatch):
    _patch_all(monkeypatch)
    results = search_runs(query="etl")
    ids = [r["run_id"] for r in results]
    assert "run-001" in ids
    assert "run-003" in ids
    assert "run-002" not in ids


def test_search_runs_by_query_status(monkeypatch):
    _patch_all(monkeypatch)
    results = search_runs(query="failed")
    ids = [r["run_id"] for r in results]
    assert "run-002" in ids
    assert "run-003" in ids
    assert "run-001" not in ids


def test_search_runs_by_run_id(monkeypatch):
    _patch_all(monkeypatch)
    results = search_runs(query="run-002")
    assert len(results) == 1
    assert results[0]["run_id"] == "run-002"


def test_search_runs_by_field(monkeypatch):
    _patch_all(monkeypatch)
    results = search_runs(field="pipeline", field_value="ml-train")
    assert len(results) == 1
    assert results[0]["run_id"] == "run-002"


def test_search_runs_no_match(monkeypatch):
    _patch_all(monkeypatch)
    results = search_runs(query="nonexistent-xyz")
    assert results == []


def test_search_runs_notes(monkeypatch):
    notes_by_run = {
        "run-001": [{"text": "all good", "author": None}],
        "run-002": [{"text": "training diverged unexpectedly", "author": "alice"}],
        "run-003": [],
    }
    monkeypatch.setattr("pipewatch.run_search.list_run_records", lambda data_dir=".pipewatch": FAKE_RUNS)
    monkeypatch.setattr("pipewatch.run_search.get_notes", lambda run_id, data_dir=".pipewatch": notes_by_run.get(run_id, []))
    monkeypatch.setattr("pipewatch.run_search.get_annotations", lambda run_id, data_dir=".pipewatch": {})

    results = search_runs(query="diverged", search_notes=True)
    assert len(results) == 1
    assert results[0]["run_id"] == "run-002"


def test_search_runs_annotations(monkeypatch):
    annotations_by_run = {
        "run-001": {"env": "production"},
        "run-002": {"env": "staging"},
        "run-003": {},
    }
    monkeypatch.setattr("pipewatch.run_search.list_run_records", lambda data_dir=".pipewatch": FAKE_RUNS)
    monkeypatch.setattr("pipewatch.run_search.get_notes", lambda run_id, data_dir=".pipewatch": [])
    monkeypatch.setattr("pipewatch.run_search.get_annotations", lambda run_id, data_dir=".pipewatch": annotations_by_run.get(run_id, {}))

    results = search_runs(query="staging", search_annotations=True)
    assert len(results) == 1
    assert results[0]["run_id"] == "run-002"


def test_format_search_results_empty():
    assert format_search_results([]) == "No matching runs found."


def test_format_search_results_shows_runs():
    output = format_search_results(FAKE_RUNS)
    assert "run-001" in output
    assert "etl" in output
    assert "success" in output
    assert "run-002" in output
    assert "failed" in output
