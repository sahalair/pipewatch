"""Tests for pipewatch.run_watchlist."""

import pytest

from pipewatch.run_watchlist import (
    add_to_watchlist,
    is_watched,
    list_watched_runs,
    load_watchlist,
    remove_from_watchlist,
)


def test_load_watchlist_missing_file(tmp_path):
    result = load_watchlist(str(tmp_path))
    assert result == {}


def test_add_to_watchlist_creates_entry(tmp_path):
    entry = add_to_watchlist(str(tmp_path), "run-001")
    assert entry["run_id"] == "run-001"
    assert entry["reason"] == ""


def test_add_to_watchlist_with_reason(tmp_path):
    entry = add_to_watchlist(str(tmp_path), "run-002", reason="flaky output")
    assert entry["reason"] == "flaky output"


def test_add_to_watchlist_persists(tmp_path):
    add_to_watchlist(str(tmp_path), "run-003", reason="check metrics")
    watchlist = load_watchlist(str(tmp_path))
    assert "run-003" in watchlist
    assert watchlist["run-003"]["reason"] == "check metrics"


def test_add_to_watchlist_overwrites_existing(tmp_path):
    add_to_watchlist(str(tmp_path), "run-004", reason="first")
    add_to_watchlist(str(tmp_path), "run-004", reason="updated")
    watchlist = load_watchlist(str(tmp_path))
    assert watchlist["run-004"]["reason"] == "updated"
    assert len(watchlist) == 1


def test_is_watched_true(tmp_path):
    add_to_watchlist(str(tmp_path), "run-005")
    assert is_watched(str(tmp_path), "run-005") is True


def test_is_watched_false(tmp_path):
    assert is_watched(str(tmp_path), "run-999") is False


def test_remove_from_watchlist_existing(tmp_path):
    add_to_watchlist(str(tmp_path), "run-006")
    removed = remove_from_watchlist(str(tmp_path), "run-006")
    assert removed is True
    assert is_watched(str(tmp_path), "run-006") is False


def test_remove_from_watchlist_nonexistent(tmp_path):
    removed = remove_from_watchlist(str(tmp_path), "run-missing")
    assert removed is False


def test_list_watched_runs_empty(tmp_path):
    assert list_watched_runs(str(tmp_path)) == []


def test_list_watched_runs_sorted(tmp_path):
    add_to_watchlist(str(tmp_path), "run-c")
    add_to_watchlist(str(tmp_path), "run-a")
    add_to_watchlist(str(tmp_path), "run-b")
    entries = list_watched_runs(str(tmp_path))
    ids = [e["run_id"] for e in entries]
    assert ids == ["run-a", "run-b", "run-c"]


def test_list_watched_runs_returns_all_fields(tmp_path):
    add_to_watchlist(str(tmp_path), "run-x", reason="important")
    entries = list_watched_runs(str(tmp_path))
    assert len(entries) == 1
    assert entries[0]["run_id"] == "run-x"
    assert entries[0]["reason"] == "important"
