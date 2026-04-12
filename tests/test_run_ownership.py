"""Tests for run_ownership module."""

import pytest
from pathlib import Path

from pipewatch.run_ownership import (
    get_owner,
    list_owned_runs,
    load_ownership,
    remove_owner,
    set_owner,
)


@pytest.fixture()
def base_dir(tmp_path):
    return tmp_path / "ownership"


def test_load_ownership_missing_file_returns_empty(base_dir):
    result = load_ownership(base_dir=base_dir)
    assert result == {}


def test_set_owner_creates_entry(base_dir):
    record = set_owner("run-1", "alice", base_dir=base_dir)
    assert record["owner"] == "alice"


def test_set_owner_with_team_and_contact(base_dir):
    record = set_owner(
        "run-1", "bob", team="data-eng", contact="bob@example.com", base_dir=base_dir
    )
    assert record["team"] == "data-eng"
    assert record["contact"] == "bob@example.com"


def test_set_owner_persists(base_dir):
    set_owner("run-2", "carol", base_dir=base_dir)
    data = load_ownership(base_dir=base_dir)
    assert "run-2" in data
    assert data["run-2"]["owner"] == "carol"


def test_set_owner_overwrites_existing(base_dir):
    set_owner("run-1", "alice", base_dir=base_dir)
    set_owner("run-1", "dave", base_dir=base_dir)
    record = get_owner("run-1", base_dir=base_dir)
    assert record["owner"] == "dave"


def test_get_owner_missing_returns_none(base_dir):
    result = get_owner("nonexistent", base_dir=base_dir)
    assert result is None


def test_get_owner_returns_record(base_dir):
    set_owner("run-3", "eve", team="ml", base_dir=base_dir)
    result = get_owner("run-3", base_dir=base_dir)
    assert result is not None
    assert result["owner"] == "eve"
    assert result["team"] == "ml"


def test_remove_owner_returns_true(base_dir):
    set_owner("run-4", "frank", base_dir=base_dir)
    removed = remove_owner("run-4", base_dir=base_dir)
    assert removed is True


def test_remove_owner_deletes_record(base_dir):
    set_owner("run-5", "grace", base_dir=base_dir)
    remove_owner("run-5", base_dir=base_dir)
    assert get_owner("run-5", base_dir=base_dir) is None


def test_remove_owner_missing_returns_false(base_dir):
    result = remove_owner("no-such-run", base_dir=base_dir)
    assert result is False


def test_list_owned_runs_all(base_dir):
    set_owner("run-a", "alice", team="alpha", base_dir=base_dir)
    set_owner("run-b", "bob", team="beta", base_dir=base_dir)
    results = list_owned_runs(base_dir=base_dir)
    run_ids = [r["run_id"] for r in results]
    assert "run-a" in run_ids
    assert "run-b" in run_ids


def test_list_owned_runs_filter_by_owner(base_dir):
    set_owner("run-x", "alice", base_dir=base_dir)
    set_owner("run-y", "bob", base_dir=base_dir)
    results = list_owned_runs(owner="alice", base_dir=base_dir)
    assert all(r["owner"] == "alice" for r in results)
    assert len(results) == 1


def test_list_owned_runs_filter_by_team(base_dir):
    set_owner("run-p", "alice", team="data-eng", base_dir=base_dir)
    set_owner("run-q", "bob", team="ml", base_dir=base_dir)
    results = list_owned_runs(team="ml", base_dir=base_dir)
    assert len(results) == 1
    assert results[0]["run_id"] == "run-q"


def test_list_owned_runs_empty(base_dir):
    results = list_owned_runs(base_dir=base_dir)
    assert results == []
