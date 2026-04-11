"""Tests for pipewatch.run_labels."""

from __future__ import annotations

import pytest

from pipewatch.run_labels import (
    filter_by_label,
    get_labels,
    load_labels,
    remove_label,
    set_label,
)


def test_load_labels_missing_file(tmp_path):
    result = load_labels(str(tmp_path))
    assert result == {}


def test_set_label_creates_entry(tmp_path):
    labels = set_label(str(tmp_path), "run-1", "env", "prod")
    assert labels == {"env": "prod"}


def test_set_label_persists(tmp_path):
    set_label(str(tmp_path), "run-1", "env", "prod")
    all_labels = load_labels(str(tmp_path))
    assert all_labels["run-1"]["env"] == "prod"


def test_set_label_overwrites_existing(tmp_path):
    set_label(str(tmp_path), "run-1", "env", "staging")
    labels = set_label(str(tmp_path), "run-1", "env", "prod")
    assert labels["env"] == "prod"


def test_set_multiple_labels(tmp_path):
    set_label(str(tmp_path), "run-1", "env", "prod")
    labels = set_label(str(tmp_path), "run-1", "team", "data")
    assert labels == {"env": "prod", "team": "data"}


def test_remove_label_existing_key(tmp_path):
    set_label(str(tmp_path), "run-1", "env", "prod")
    labels = remove_label(str(tmp_path), "run-1", "env")
    assert "env" not in labels


def test_remove_label_missing_key_is_noop(tmp_path):
    set_label(str(tmp_path), "run-1", "env", "prod")
    labels = remove_label(str(tmp_path), "run-1", "nonexistent")
    assert labels == {"env": "prod"}


def test_get_labels_returns_run_labels(tmp_path):
    set_label(str(tmp_path), "run-1", "env", "prod")
    set_label(str(tmp_path), "run-2", "env", "dev")
    assert get_labels(str(tmp_path), "run-1") == {"env": "prod"}
    assert get_labels(str(tmp_path), "run-2") == {"env": "dev"}


def test_get_labels_missing_run_returns_empty(tmp_path):
    assert get_labels(str(tmp_path), "no-such-run") == {}


def test_filter_by_label_key_only(tmp_path):
    set_label(str(tmp_path), "run-1", "env", "prod")
    set_label(str(tmp_path), "run-2", "env", "dev")
    set_label(str(tmp_path), "run-3", "team", "data")
    result = filter_by_label(str(tmp_path), "env")
    assert set(result) == {"run-1", "run-2"}


def test_filter_by_label_key_and_value(tmp_path):
    set_label(str(tmp_path), "run-1", "env", "prod")
    set_label(str(tmp_path), "run-2", "env", "dev")
    result = filter_by_label(str(tmp_path), "env", "prod")
    assert result == ["run-1"]


def test_filter_by_label_no_match(tmp_path):
    set_label(str(tmp_path), "run-1", "env", "prod")
    result = filter_by_label(str(tmp_path), "missing-key")
    assert result == []


def test_filter_returns_sorted(tmp_path):
    for rid in ["run-c", "run-a", "run-b"]:
        set_label(str(tmp_path), rid, "env", "prod")
    result = filter_by_label(str(tmp_path), "env")
    assert result == ["run-a", "run-b", "run-c"]
