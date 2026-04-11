"""Tests for pipewatch.run_events."""

from __future__ import annotations

import pytest

from pipewatch.run_events import (
    clear_events,
    format_events,
    get_events,
    load_events,
    record_event,
    save_events,
)


@pytest.fixture(autouse=True)
def _tmp_events(tmp_path, monkeypatch):
    import pipewatch.run_events as re_mod
    monkeypatch.setattr(re_mod, "_EVENTS_DIR", tmp_path / "events")


def test_load_events_missing_file():
    assert load_events("run-abc") == []


def test_record_event_creates_entry():
    ev = record_event("run-1", "stage_start", "Stage A started")
    assert ev["run_id"] == "run-1"
    assert ev["event_type"] == "stage_start"
    assert ev["message"] == "Stage A started"
    assert ev["level"] == "info"
    assert "timestamp" in ev
    assert ev["data"] == {}


def test_record_event_with_level_and_data():
    ev = record_event("run-2", "failure", "Oops", level="error", data={"code": 1})
    assert ev["level"] == "error"
    assert ev["data"] == {"code": 1}


def test_record_event_invalid_level_raises():
    with pytest.raises(ValueError, match="level must be one of"):
        record_event("run-3", "x", "msg", level="critical")


def test_record_event_appends_multiple():
    record_event("run-4", "a", "first")
    record_event("run-4", "b", "second")
    events = load_events("run-4")
    assert len(events) == 2
    assert events[0]["event_type"] == "a"
    assert events[1]["event_type"] == "b"


def test_get_events_no_filter():
    record_event("run-5", "t1", "m1", level="info")
    record_event("run-5", "t2", "m2", level="warning")
    assert len(get_events("run-5")) == 2


def test_get_events_filter_by_level():
    record_event("run-6", "t1", "m1", level="info")
    record_event("run-6", "t2", "m2", level="error")
    result = get_events("run-6", level="error")
    assert len(result) == 1
    assert result[0]["level"] == "error"


def test_get_events_filter_by_event_type():
    record_event("run-7", "stage_start", "A")
    record_event("run-7", "stage_end", "B")
    result = get_events("run-7", event_type="stage_end")
    assert len(result) == 1
    assert result[0]["event_type"] == "stage_end"


def test_clear_events_returns_count():
    record_event("run-8", "x", "y")
    record_event("run-8", "x", "z")
    count = clear_events("run-8")
    assert count == 2
    assert load_events("run-8") == []


def test_clear_events_missing_run():
    assert clear_events("nonexistent") == 0


def test_format_events_empty():
    assert format_events([]) == "(no events)"


def test_format_events_contains_info():
    record_event("run-9", "check", "All good")
    events = load_events("run-9")
    output = format_events(events)
    assert "check" in output
    assert "All good" in output
    assert "INFO" in output


def test_save_and_load_roundtrip():
    data = [{"run_id": "r", "event_type": "e", "level": "debug", "message": "m", "timestamp": "t", "data": {}}]
    save_events("run-10", data)
    assert load_events("run-10") == data
