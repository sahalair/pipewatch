"""Tests for pipewatch.run_capacity."""

import pytest

from pipewatch.run_capacity import (
    check_capacity,
    get_capacity,
    list_capacity_rules,
    load_capacity,
    remove_capacity,
    set_capacity,
)


def test_load_capacity_missing_file_returns_empty(tmp_path):
    result = load_capacity(str(tmp_path))
    assert result == {}


def test_set_capacity_creates_entry(tmp_path):
    rule = set_capacity(str(tmp_path), "etl", 3)
    assert rule["pipeline"] == "etl"
    assert rule["max_concurrent"] == 3


def test_set_capacity_persists(tmp_path):
    set_capacity(str(tmp_path), "etl", 5)
    loaded = load_capacity(str(tmp_path))
    assert "etl" in loaded
    assert loaded["etl"]["max_concurrent"] == 5


def test_set_capacity_updates_existing(tmp_path):
    set_capacity(str(tmp_path), "etl", 2)
    set_capacity(str(tmp_path), "etl", 8)
    rule = get_capacity(str(tmp_path), "etl")
    assert rule["max_concurrent"] == 8


def test_set_capacity_invalid_raises(tmp_path):
    with pytest.raises(ValueError):
        set_capacity(str(tmp_path), "etl", 0)


def test_remove_capacity_existing(tmp_path):
    set_capacity(str(tmp_path), "etl", 2)
    result = remove_capacity(str(tmp_path), "etl")
    assert result is True
    assert get_capacity(str(tmp_path), "etl") is None


def test_remove_capacity_nonexistent_returns_false(tmp_path):
    result = remove_capacity(str(tmp_path), "missing")
    assert result is False


def test_get_capacity_missing_returns_none(tmp_path):
    assert get_capacity(str(tmp_path), "nope") is None


def test_check_capacity_no_rule(tmp_path):
    status = check_capacity(str(tmp_path), "pipe", 5)
    assert status["limited"] is False
    assert status["at_capacity"] is False
    assert status["max_concurrent"] is None


def test_check_capacity_under_limit(tmp_path):
    set_capacity(str(tmp_path), "pipe", 4)
    status = check_capacity(str(tmp_path), "pipe", 2)
    assert status["limited"] is True
    assert status["at_capacity"] is False


def test_check_capacity_at_limit(tmp_path):
    set_capacity(str(tmp_path), "pipe", 3)
    status = check_capacity(str(tmp_path), "pipe", 3)
    assert status["at_capacity"] is True


def test_check_capacity_over_limit(tmp_path):
    set_capacity(str(tmp_path), "pipe", 2)
    status = check_capacity(str(tmp_path), "pipe", 5)
    assert status["at_capacity"] is True


def test_list_capacity_rules_empty(tmp_path):
    assert list_capacity_rules(str(tmp_path)) == []


def test_list_capacity_rules_sorted(tmp_path):
    set_capacity(str(tmp_path), "zzz", 1)
    set_capacity(str(tmp_path), "aaa", 2)
    rules = list_capacity_rules(str(tmp_path))
    assert rules[0]["pipeline"] == "aaa"
    assert rules[1]["pipeline"] == "zzz"
