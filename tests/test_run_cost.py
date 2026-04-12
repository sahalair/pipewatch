"""Tests for pipewatch.run_cost."""

import json
import pytest
from pathlib import Path
from unittest.mock import patch

from pipewatch import run_cost


@pytest.fixture(autouse=True)
def _tmp_costs(tmp_path, monkeypatch):
    costs_file = tmp_path / ".pipewatch" / "run_costs.json"
    monkeypatch.setattr(run_cost, "_COSTS_FILE", str(costs_file))
    yield costs_file


def test_load_costs_missing_file_returns_empty():
    result = run_cost.load_costs()
    assert result == {}


def test_record_cost_creates_entry():
    entry = run_cost.record_cost("run-1", "etl", 0.042)
    assert entry["run_id"] == "run-1"
    assert entry["pipeline"] == "etl"
    assert entry["amount"] == 0.042
    assert entry["currency"] == "USD"


def test_record_cost_with_unit_and_notes():
    entry = run_cost.record_cost("run-2", "etl", 1.5, unit="cpu-hour", notes="spot instance")
    assert entry["unit"] == "cpu-hour"
    assert entry["notes"] == "spot instance"


def test_record_cost_negative_raises():
    with pytest.raises(ValueError, match="non-negative"):
        run_cost.record_cost("run-x", "etl", -1.0)


def test_record_cost_persists():
    run_cost.record_cost("run-3", "ingest", 0.1)
    loaded = run_cost.load_costs()
    assert "run-3" in loaded
    assert loaded["run-3"]["amount"] == 0.1


def test_get_cost_returns_entry():
    run_cost.record_cost("run-4", "transform", 2.0)
    entry = run_cost.get_cost("run-4")
    assert entry is not None
    assert entry["pipeline"] == "transform"


def test_get_cost_missing_returns_none():
    result = run_cost.get_cost("nonexistent")
    assert result is None


def test_list_costs_for_pipeline_filters():
    run_cost.record_cost("r1", "etl", 1.0)
    run_cost.record_cost("r2", "etl", 2.0)
    run_cost.record_cost("r3", "other", 3.0)
    results = run_cost.list_costs_for_pipeline("etl")
    assert len(results) == 2
    assert all(e["pipeline"] == "etl" for e in results)


def test_summarize_costs_empty_pipeline():
    summary = run_cost.summarize_costs("nopipe")
    assert summary["count"] == 0
    assert summary["total"] == 0.0


def test_summarize_costs_computes_correctly():
    run_cost.record_cost("s1", "mypl", 1.0)
    run_cost.record_cost("s2", "mypl", 3.0)
    summary = run_cost.summarize_costs("mypl")
    assert summary["count"] == 2
    assert summary["total"] == 4.0
    assert summary["average"] == 2.0
