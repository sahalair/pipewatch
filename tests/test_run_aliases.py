"""Tests for pipewatch.run_aliases."""

import pytest

from pipewatch.run_aliases import (
    load_aliases,
    save_aliases,
    set_alias,
    remove_alias,
    resolve_alias,
    list_aliases,
    find_aliases_for_run,
)


def test_load_aliases_missing_file(tmp_path):
    result = load_aliases(str(tmp_path))
    assert result == {}


def test_save_and_load_aliases_roundtrip(tmp_path):
    data = {"prod": "run-abc", "latest": "run-xyz"}
    save_aliases(str(tmp_path), data)
    loaded = load_aliases(str(tmp_path))
    assert loaded == data


def test_set_alias_creates_entry(tmp_path):
    set_alias(str(tmp_path), "my-alias", "run-001")
    aliases = load_aliases(str(tmp_path))
    assert aliases["my-alias"] == "run-001"


def test_set_alias_overwrites_existing(tmp_path):
    set_alias(str(tmp_path), "stable", "run-001")
    set_alias(str(tmp_path), "stable", "run-002")
    assert load_aliases(str(tmp_path))["stable"] == "run-002"


def test_set_multiple_aliases(tmp_path):
    set_alias(str(tmp_path), "a", "run-1")
    set_alias(str(tmp_path), "b", "run-2")
    aliases = load_aliases(str(tmp_path))
    assert len(aliases) == 2
    assert aliases["a"] == "run-1"
    assert aliases["b"] == "run-2"


def test_remove_alias_existing(tmp_path):
    set_alias(str(tmp_path), "temp", "run-999")
    result = remove_alias(str(tmp_path), "temp")
    assert result is True
    assert "temp" not in load_aliases(str(tmp_path))


def test_remove_alias_nonexistent(tmp_path):
    result = remove_alias(str(tmp_path), "ghost")
    assert result is False


def test_resolve_alias_found(tmp_path):
    set_alias(str(tmp_path), "current", "run-42")
    assert resolve_alias(str(tmp_path), "current") == "run-42"


def test_resolve_alias_not_found(tmp_path):
    assert resolve_alias(str(tmp_path), "missing") is None


def test_list_aliases_empty(tmp_path):
    assert list_aliases(str(tmp_path)) == {}


def test_list_aliases_returns_all(tmp_path):
    set_alias(str(tmp_path), "x", "run-x")
    set_alias(str(tmp_path), "y", "run-y")
    result = list_aliases(str(tmp_path))
    assert set(result.keys()) == {"x", "y"}


def test_find_aliases_for_run(tmp_path):
    set_alias(str(tmp_path), "alpha", "run-77")
    set_alias(str(tmp_path), "beta", "run-77")
    set_alias(str(tmp_path), "gamma", "run-88")
    found = find_aliases_for_run(str(tmp_path), "run-77")
    assert sorted(found) == ["alpha", "beta"]


def test_find_aliases_for_run_none(tmp_path):
    set_alias(str(tmp_path), "only", "run-1")
    found = find_aliases_for_run(str(tmp_path), "run-999")
    assert found == []
