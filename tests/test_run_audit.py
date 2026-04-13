"""Tests for pipewatch.run_audit."""
import pytest
from pipewatch.run_audit import (
    load_audit_log,
    save_audit_log,
    record_audit_event,
    get_audit_events,
    format_audit_log,
)


@pytest.fixture
def base(tmp_path):
    return str(tmp_path)


def test_load_audit_log_missing_file_returns_empty(base):
    assert load_audit_log(base) == []


def test_save_and_load_audit_log_roundtrip(base):
    entries = [{"run_id": "r1", "action": "created", "timestamp": "t", "actor": None, "details": {}}]
    save_audit_log(base, entries)
    assert load_audit_log(base) == entries


def test_record_audit_event_creates_entry(base):
    entry = record_audit_event(base, "run-123", "created")
    assert entry["run_id"] == "run-123"
    assert entry["action"] == "created"
    assert "timestamp" in entry
    assert entry["actor"] is None
    assert entry["details"] == {}


def test_record_audit_event_persists(base):
    record_audit_event(base, "run-1", "started")
    log = load_audit_log(base)
    assert len(log) == 1
    assert log[0]["action"] == "started"


def test_record_audit_event_with_actor_and_details(base):
    entry = record_audit_event(base, "run-2", "tagged", actor="alice", details={"tag": "prod"})
    assert entry["actor"] == "alice"
    assert entry["details"] == {"tag": "prod"}


def test_record_audit_event_invalid_action_raises(base):
    with pytest.raises(ValueError, match="Unknown audit action"):
        record_audit_event(base, "run-x", "launched")


def test_record_audit_event_appends_multiple(base):
    record_audit_event(base, "r1", "created")
    record_audit_event(base, "r1", "started")
    record_audit_event(base, "r1", "finished")
    log = load_audit_log(base)
    assert len(log) == 3
    assert [e["action"] for e in log] == ["created", "started", "finished"]


def test_get_audit_events_filter_by_run_id(base):
    record_audit_event(base, "r1", "created")
    record_audit_event(base, "r2", "created")
    events = get_audit_events(base, run_id="r1")
    assert all(e["run_id"] == "r1" for e in events)
    assert len(events) == 1


def test_get_audit_events_filter_by_action(base):
    record_audit_event(base, "r1", "created")
    record_audit_event(base, "r1", "finished")
    events = get_audit_events(base, action="finished")
    assert all(e["action"] == "finished" for e in events)


def test_get_audit_events_no_filters_returns_all(base):
    record_audit_event(base, "r1", "created")
    record_audit_event(base, "r2", "archived")
    assert len(get_audit_events(base)) == 2


def test_format_audit_log_empty():
    assert format_audit_log([]) == "No audit events found."


def test_format_audit_log_contains_run_id_and_action(base):
    record_audit_event(base, "run-abc", "noted", actor="bob")
    entries = load_audit_log(base)
    output = format_audit_log(entries)
    assert "run-abc" in output
    assert "noted" in output
    assert "bob" in output
