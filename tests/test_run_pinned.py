"""Tests for pipewatch.run_pinned."""

import pytest
from pipewatch.run_pinned import (
    load_pinned,
    save_pinned,
    pin_run,
    unpin_run,
    resolve_pin,
    list_pins,
)


def test_load_pinned_missing_file(tmp_path):
    result = load_pinned(base_dir=str(tmp_path))
    assert result == {}


def test_save_and_load_pinned_roundtrip(tmp_path):
    data = {"baseline": "run-abc", "latest": "run-xyz"}
    save_pinned(data, base_dir=str(tmp_path))
    loaded = load_pinned(base_dir=str(tmp_path))
    assert loaded == data


def test_pin_run_creates_entry(tmp_path):
    result = pin_run("baseline", "run-001", base_dir=str(tmp_path))
    assert result["baseline"] == "run-001"
    assert load_pinned(base_dir=str(tmp_path))["baseline"] == "run-001"


def test_pin_run_overwrites_existing(tmp_path):
    pin_run("baseline", "run-001", base_dir=str(tmp_path))
    pin_run("baseline", "run-002", base_dir=str(tmp_path))
    assert load_pinned(base_dir=str(tmp_path))["baseline"] == "run-002"


def test_pin_run_multiple_labels(tmp_path):
    pin_run("v1", "run-aaa", base_dir=str(tmp_path))
    pin_run("v2", "run-bbb", base_dir=str(tmp_path))
    pinned = load_pinned(base_dir=str(tmp_path))
    assert len(pinned) == 2
    assert pinned["v1"] == "run-aaa"
    assert pinned["v2"] == "run-bbb"


def test_unpin_run_removes_label(tmp_path):
    pin_run("staging", "run-999", base_dir=str(tmp_path))
    removed = unpin_run("staging", base_dir=str(tmp_path))
    assert removed is True
    assert resolve_pin("staging", base_dir=str(tmp_path)) is None


def test_unpin_run_not_found_returns_false(tmp_path):
    result = unpin_run("nonexistent", base_dir=str(tmp_path))
    assert result is False


def test_unpin_run_does_not_affect_other_labels(tmp_path):
    """Removing one pin should leave other pins intact."""
    pin_run("keep", "run-keep", base_dir=str(tmp_path))
    pin_run("remove", "run-remove", base_dir=str(tmp_path))
    unpin_run("remove", base_dir=str(tmp_path))
    assert resolve_pin("keep", base_dir=str(tmp_path)) == "run-keep"
    assert resolve_pin("remove", base_dir=str(tmp_path)) is None


def test_resolve_pin_returns_run_id(tmp_path):
    pin_run("gold", "run-gold-123", base_dir=str(tmp_path))
    assert resolve_pin("gold", base_dir=str(tmp_path)) == "run-gold-123"


def test_resolve_pin_missing_returns_none(tmp_path):
    assert resolve_pin("missing", base_dir=str(tmp_path)) is None


def test_list_pins_empty(tmp_path):
    assert list_pins(base_dir=str(tmp_path)) == []


def test_list_pins_sorted_by_label(tmp_path):
    pin_run("zebra", "run-z", base_dir=str(tmp_path))
    pin_run("alpha", "run-a", base_dir=str(tmp_path))
    pin_run("middle", "run-m", base_dir=str(tmp_path))
    pins = list_pins(base_dir=str(tmp_path))
    labels = [p["label"] for p in pins]
    assert labels == sorted(labels)


def test_list_pins_has_required_keys(tmp_path):
    pin_run("ref", "run-ref", base_dir=str(tmp_path))
    pins = list_pins(base_dir=str(tmp_path))
    assert len(pins) == 1
    assert "label" in pins[0]
    assert "run_id" in pins[0]
