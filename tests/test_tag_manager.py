"""Tests for pipewatch.tag_manager."""

from __future__ import annotations

import pytest

from pipewatch.tag_manager import (
    add_tags,
    filter_runs_by_tag,
    get_tags,
    load_tags,
    remove_tags,
    save_tags,
)


def test_load_tags_missing_file(tmp_path):
    result = load_tags(str(tmp_path))
    assert result == {}


def test_save_and_load_tags_roundtrip(tmp_path):
    data = {"run-1": ["nightly", "prod"], "run-2": ["dev"]}
    save_tags(str(tmp_path), data)
    loaded = load_tags(str(tmp_path))
    assert loaded == data


def test_add_tags_new_run(tmp_path):
    result = add_tags(str(tmp_path), "run-1", ["nightly", "prod"])
    assert "nightly" in result
    assert "prod" in result


def test_add_tags_idempotent(tmp_path):
    add_tags(str(tmp_path), "run-1", ["nightly"])
    result = add_tags(str(tmp_path), "run-1", ["nightly"])
    assert result.count("nightly") == 1


def test_add_tags_merges_existing(tmp_path):
    add_tags(str(tmp_path), "run-1", ["nightly"])
    result = add_tags(str(tmp_path), "run-1", ["prod"])
    assert "nightly" in result
    assert "prod" in result


def test_remove_tags_removes_correctly(tmp_path):
    add_tags(str(tmp_path), "run-1", ["nightly", "prod"])
    result = remove_tags(str(tmp_path), "run-1", ["prod"])
    assert "prod" not in result
    assert "nightly" in result


def test_remove_tags_nonexistent_tag_is_noop(tmp_path):
    add_tags(str(tmp_path), "run-1", ["nightly"])
    result = remove_tags(str(tmp_path), "run-1", ["missing"])
    assert result == ["nightly"]


def test_get_tags_returns_empty_for_unknown_run(tmp_path):
    assert get_tags(str(tmp_path), "ghost-run") == []


def test_get_tags_returns_correct_list(tmp_path):
    add_tags(str(tmp_path), "run-1", ["a", "b"])
    assert get_tags(str(tmp_path), "run-1") == ["a", "b"]


def test_filter_runs_by_tag_empty(tmp_path):
    assert filter_runs_by_tag(str(tmp_path), "nightly") == []


def test_filter_runs_by_tag_returns_matching(tmp_path):
    add_tags(str(tmp_path), "run-1", ["nightly"])
    add_tags(str(tmp_path), "run-2", ["prod"])
    add_tags(str(tmp_path), "run-3", ["nightly", "prod"])
    result = filter_runs_by_tag(str(tmp_path), "nightly")
    assert "run-1" in result
    assert "run-3" in result
    assert "run-2" not in result


def test_filter_runs_by_tag_sorted(tmp_path):
    add_tags(str(tmp_path), "run-b", ["ci"])
    add_tags(str(tmp_path), "run-a", ["ci"])
    result = filter_runs_by_tag(str(tmp_path), "ci")
    assert result == sorted(result)
