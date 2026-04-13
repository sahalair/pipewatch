"""Tests for pipewatch.run_attribution."""

import json
import pytest

from pipewatch.run_attribution import (
    load_attribution,
    save_attribution,
    set_attribution,
    get_attribution,
    filter_by_source,
    format_attribution,
)


@pytest.fixture()
def base(tmp_path):
    return str(tmp_path)


def test_load_attribution_missing_file_returns_empty(base):
    assert load_attribution(base) == {}


def test_save_and_load_attribution_roundtrip(base):
    data = {"run-1": {"source": "manual"}}
    save_attribution(base, data)
    loaded = load_attribution(base)
    assert loaded == data


def test_set_attribution_creates_entry(base):
    entry = set_attribution(base, "run-1", "scheduled")
    assert entry["source"] == "scheduled"
    assert get_attribution(base, "run-1") == entry


def test_set_attribution_with_triggered_by(base):
    entry = set_attribution(base, "run-2", "ci", triggered_by="github-actions")
    assert entry["triggered_by"] == "github-actions"


def test_set_attribution_with_note(base):
    entry = set_attribution(base, "run-3", "manual", note="hotfix deployment")
    assert entry["note"] == "hotfix deployment"


def test_set_attribution_persists(base):
    set_attribution(base, "run-4", "webhook")
    data = load_attribution(base)
    assert "run-4" in data
    assert data["run-4"]["source"] == "webhook"


def test_set_attribution_overwrites_existing(base):
    set_attribution(base, "run-5", "manual")
    set_attribution(base, "run-5", "retry")
    assert get_attribution(base, "run-5")["source"] == "retry"


def test_set_attribution_invalid_source_raises(base):
    with pytest.raises(ValueError, match="Invalid source"):
        set_attribution(base, "run-6", "magic")


def test_set_attribution_empty_run_id_raises(base):
    with pytest.raises(ValueError, match="run_id"):
        set_attribution(base, "", "manual")


def test_get_attribution_missing_run_returns_none(base):
    assert get_attribution(base, "nonexistent") is None


def test_filter_by_source_returns_matching_run_ids(base):
    set_attribution(base, "run-a", "manual")
    set_attribution(base, "run-b", "scheduled")
    set_attribution(base, "run-c", "manual")
    result = filter_by_source(base, "manual")
    assert set(result) == {"run-a", "run-c"}


def test_filter_by_source_empty_when_none_match(base):
    set_attribution(base, "run-x", "ci")
    assert filter_by_source(base, "webhook") == []


def test_format_attribution_basic():
    entry = {"source": "scheduled"}
    output = format_attribution("run-99", entry)
    assert "run-99" in output
    assert "scheduled" in output


def test_format_attribution_includes_triggered_by():
    entry = {"source": "ci", "triggered_by": "jenkins"}
    output = format_attribution("run-77", entry)
    assert "jenkins" in output


def test_format_attribution_includes_note():
    entry = {"source": "manual", "note": "emergency fix"}
    output = format_attribution("run-55", entry)
    assert "emergency fix" in output
