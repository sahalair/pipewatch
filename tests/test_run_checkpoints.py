"""Tests for pipewatch.run_checkpoints."""

import pytest
from unittest.mock import patch

from pipewatch.run_checkpoints import (
    load_checkpoints,
    save_checkpoints,
    record_checkpoint,
    get_checkpoints,
    clear_checkpoints,
    checkpoint_summary,
)

RUN_ID = "run-abc-123"


@pytest.fixture(autouse=True)
def _tmp(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)


def test_load_checkpoints_missing_file_returns_empty():
    assert load_checkpoints(RUN_ID) == []


def test_save_and_load_checkpoints_roundtrip():
    checkpoints = [{"name": "start", "status": "ok", "timestamp": "t", "message": None, "data": {}}]
    save_checkpoints(RUN_ID, checkpoints)
    loaded = load_checkpoints(RUN_ID)
    assert loaded == checkpoints


def test_record_checkpoint_creates_entry():
    entry = record_checkpoint(RUN_ID, "ingest")
    assert entry["name"] == "ingest"
    assert entry["status"] == "ok"
    assert "timestamp" in entry


def test_record_checkpoint_with_status_and_message():
    entry = record_checkpoint(RUN_ID, "validate", status="warn", message="low rows")
    assert entry["status"] == "warn"
    assert entry["message"] == "low rows"


def test_record_checkpoint_with_data():
    entry = record_checkpoint(RUN_ID, "transform", data={"rows": 42})
    assert entry["data"] == {"rows": 42}


def test_record_checkpoint_invalid_status_raises():
    with pytest.raises(ValueError, match="Invalid checkpoint status"):
        record_checkpoint(RUN_ID, "step", status="bad")


def test_record_checkpoint_appends_multiple():
    record_checkpoint(RUN_ID, "a")
    record_checkpoint(RUN_ID, "b")
    checkpoints = load_checkpoints(RUN_ID)
    assert len(checkpoints) == 2
    assert checkpoints[0]["name"] == "a"
    assert checkpoints[1]["name"] == "b"


def test_get_checkpoints_returns_all():
    record_checkpoint(RUN_ID, "x")
    record_checkpoint(RUN_ID, "y")
    result = get_checkpoints(RUN_ID)
    assert len(result) == 2


def test_get_checkpoints_filters_by_name():
    record_checkpoint(RUN_ID, "load")
    record_checkpoint(RUN_ID, "validate")
    result = get_checkpoints(RUN_ID, name="load")
    assert len(result) == 1
    assert result[0]["name"] == "load"


def test_clear_checkpoints_removes_all():
    record_checkpoint(RUN_ID, "step1")
    record_checkpoint(RUN_ID, "step2")
    count = clear_checkpoints(RUN_ID)
    assert count == 2
    assert load_checkpoints(RUN_ID) == []


def test_clear_checkpoints_empty_run():
    count = clear_checkpoints(RUN_ID)
    assert count == 0


def test_checkpoint_summary_counts_statuses():
    record_checkpoint(RUN_ID, "a", status="ok")
    record_checkpoint(RUN_ID, "b", status="ok")
    record_checkpoint(RUN_ID, "c", status="warn")
    record_checkpoint(RUN_ID, "d", status="fail")
    summary = checkpoint_summary(RUN_ID)
    assert summary["total"] == 4
    assert summary["ok"] == 2
    assert summary["warn"] == 1
    assert summary["fail"] == 1


def test_checkpoint_summary_empty_run():
    summary = checkpoint_summary(RUN_ID)
    assert summary["total"] == 0
    assert summary["ok"] == 0
