"""Tests for pipewatch.metric module."""

from __future__ import annotations

import pytest

from pipewatch.metric import (
    compare_metrics,
    load_metrics,
    record_metric,
    save_metrics,
)


def test_record_metric_required_fields():
    m = record_metric("run-1", "row_count", 42.0)
    assert m["run_id"] == "run-1"
    assert m["name"] == "row_count"
    assert m["value"] == 42.0
    assert "recorded_at" in m
    assert m["unit"] == ""


def test_record_metric_with_unit():
    m = record_metric("run-1", "duration", 3.14, unit="seconds")
    assert m["unit"] == "seconds"


def test_save_and_load_metrics(tmp_path, monkeypatch):
    monkeypatch.setattr("pipewatch.metric._METRICS_DIR", str(tmp_path / "metrics"))
    metrics = [record_metric("run-42", "rows", 100.0)]
    save_metrics("run-42", metrics)
    loaded = load_metrics("run-42")
    assert len(loaded) == 1
    assert loaded[0]["name"] == "rows"
    assert loaded[0]["value"] == 100.0


def test_load_metrics_missing_run(tmp_path, monkeypatch):
    monkeypatch.setattr("pipewatch.metric._METRICS_DIR", str(tmp_path / "metrics"))
    result = load_metrics("nonexistent-run")
    assert result == []


def test_compare_metrics_basic(tmp_path, monkeypatch):
    monkeypatch.setattr("pipewatch.metric._METRICS_DIR", str(tmp_path / "metrics"))
    save_metrics("run-a", [record_metric("run-a", "rows", 100.0)])
    save_metrics("run-b", [record_metric("run-b", "rows", 150.0)])
    result = compare_metrics("run-a", "run-b")
    assert len(result) == 1
    assert result[0]["name"] == "rows"
    assert result[0]["run_a"] == 100.0
    assert result[0]["run_b"] == 150.0
    assert result[0]["delta"] == pytest.approx(50.0)


def test_compare_metrics_missing_in_one_run(tmp_path, monkeypatch):
    monkeypatch.setattr("pipewatch.metric._METRICS_DIR", str(tmp_path / "metrics"))
    save_metrics("run-a", [record_metric("run-a", "rows", 100.0)])
    save_metrics("run-b", [
        record_metric("run-b", "rows", 120.0),
        record_metric("run-b", "errors", 5.0),
    ])
    result = compare_metrics("run-a", "run-b")
    names = [r["name"] for r in result]
    assert "errors" in names
    errors_entry = next(r for r in result if r["name"] == "errors")
    assert errors_entry["run_a"] is None
    assert errors_entry["delta"] is None


def test_compare_metrics_both_empty(tmp_path, monkeypatch):
    monkeypatch.setattr("pipewatch.metric._METRICS_DIR", str(tmp_path / "metrics"))
    result = compare_metrics("no-run-a", "no-run-b")
    assert result == []
