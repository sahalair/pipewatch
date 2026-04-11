"""Tests for pipewatch.run_annotations."""

from __future__ import annotations

import pytest

from pipewatch.run_annotations import (
    delete_annotation,
    filter_runs_by_annotation,
    get_annotations,
    load_annotations,
    set_annotation,
)


def test_load_annotations_missing_file(tmp_path):
    result = load_annotations(str(tmp_path))
    assert result == {}


def test_set_and_get_annotation(tmp_path):
    set_annotation(str(tmp_path), "run-1", "env", "prod")
    annotations = get_annotations(str(tmp_path), "run-1")
    assert annotations["env"] == "prod"


def test_set_annotation_overwrites_existing(tmp_path):
    set_annotation(str(tmp_path), "run-1", "env", "staging")
    set_annotation(str(tmp_path), "run-1", "env", "prod")
    assert get_annotations(str(tmp_path), "run-1")["env"] == "prod"


def test_set_multiple_keys(tmp_path):
    set_annotation(str(tmp_path), "run-1", "env", "prod")
    set_annotation(str(tmp_path), "run-1", "owner", "alice")
    annotations = get_annotations(str(tmp_path), "run-1")
    assert annotations["env"] == "prod"
    assert annotations["owner"] == "alice"


def test_get_annotations_missing_run(tmp_path):
    assert get_annotations(str(tmp_path), "no-such-run") == {}


def test_delete_annotation_existing_key(tmp_path):
    set_annotation(str(tmp_path), "run-1", "env", "prod")
    removed = delete_annotation(str(tmp_path), "run-1", "env")
    assert removed is True
    assert get_annotations(str(tmp_path), "run-1") == {}


def test_delete_annotation_missing_key_returns_false(tmp_path):
    removed = delete_annotation(str(tmp_path), "run-1", "env")
    assert removed is False


def test_delete_last_key_removes_run_entry(tmp_path):
    set_annotation(str(tmp_path), "run-1", "env", "prod")
    delete_annotation(str(tmp_path), "run-1", "env")
    data = load_annotations(str(tmp_path))
    assert "run-1" not in data


def test_filter_runs_by_annotation_matches(tmp_path):
    set_annotation(str(tmp_path), "run-1", "env", "prod")
    set_annotation(str(tmp_path), "run-2", "env", "staging")
    set_annotation(str(tmp_path), "run-3", "env", "prod")
    result = filter_runs_by_annotation(str(tmp_path), "env", "prod")
    assert set(result) == {"run-1", "run-3"}


def test_filter_runs_by_annotation_no_match(tmp_path):
    set_annotation(str(tmp_path), "run-1", "env", "staging")
    result = filter_runs_by_annotation(str(tmp_path), "env", "prod")
    assert result == []


def test_annotations_persist_across_calls(tmp_path):
    set_annotation(str(tmp_path), "run-1", "version", "1.2.3")
    # Reload from disk
    all_data = load_annotations(str(tmp_path))
    assert all_data["run-1"]["version"] == "1.2.3"
