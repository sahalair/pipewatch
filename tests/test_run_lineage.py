"""Tests for pipewatch.run_lineage."""

import pytest
from pipewatch.run_lineage import (
    load_lineage,
    record_lineage,
    get_lineage,
    get_ancestors,
    get_descendants,
    format_lineage_report,
)


@pytest.fixture()
def base(tmp_path):
    return str(tmp_path)


def test_load_lineage_missing_file_returns_empty(base):
    result = load_lineage(base)
    assert result == {}


def test_record_lineage_creates_entry(base):
    entry = record_lineage("run-1", upstream=["run-0"], base_dir=base)
    assert "run-0" in entry["upstream"]
    assert entry["downstream"] == []


def test_record_lineage_persists(base):
    record_lineage("run-1", upstream=["run-0"], base_dir=base)
    loaded = load_lineage(base)
    assert "run-1" in loaded
    assert "run-0" in loaded["run-1"]["upstream"]


def test_record_lineage_idempotent(base):
    record_lineage("run-1", upstream=["run-0"], base_dir=base)
    record_lineage("run-1", upstream=["run-0"], base_dir=base)
    entry = get_lineage("run-1", base)
    assert entry["upstream"].count("run-0") == 1


def test_record_lineage_merges_multiple_calls(base):
    record_lineage("run-2", upstream=["run-0"], base_dir=base)
    record_lineage("run-2", upstream=["run-1"], downstream=["run-3"], base_dir=base)
    entry = get_lineage("run-2", base)
    assert "run-0" in entry["upstream"]
    assert "run-1" in entry["upstream"]
    assert "run-3" in entry["downstream"]


def test_get_lineage_missing_run_returns_empty(base):
    entry = get_lineage("nonexistent", base)
    assert entry == {"upstream": [], "downstream": []}


def test_get_ancestors_direct(base):
    record_lineage("run-1", upstream=["run-0"], base_dir=base)
    ancestors = get_ancestors("run-1", base)
    assert ancestors == ["run-0"]


def test_get_ancestors_transitive(base):
    record_lineage("run-1", upstream=["run-0"], base_dir=base)
    record_lineage("run-2", upstream=["run-1"], base_dir=base)
    ancestors = get_ancestors("run-2", base)
    assert "run-1" in ancestors
    assert "run-0" in ancestors


def test_get_ancestors_no_cycles(base):
    """Ensure BFS doesn't loop if lineage has no cycles."""
    record_lineage("run-1", upstream=["run-0"], base_dir=base)
    record_lineage("run-0", upstream=[], base_dir=base)
    ancestors = get_ancestors("run-1", base)
    assert ancestors == ["run-0"]


def test_get_descendants_direct(base):
    record_lineage("run-0", downstream=["run-1"], base_dir=base)
    descendants = get_descendants("run-0", base)
    assert descendants == ["run-1"]


def test_get_descendants_transitive(base):
    record_lineage("run-0", downstream=["run-1"], base_dir=base)
    record_lineage("run-1", downstream=["run-2"], base_dir=base)
    descendants = get_descendants("run-0", base)
    assert "run-1" in descendants
    assert "run-2" in descendants


def test_format_lineage_report_contains_run_id(base):
    record_lineage("run-1", upstream=["run-0"], downstream=["run-2"], base_dir=base)
    report = format_lineage_report("run-1", base)
    assert "run-1" in report
    assert "run-0" in report
    assert "run-2" in report


def test_format_lineage_report_no_relations(base):
    report = format_lineage_report("lonely-run", base)
    assert "none" in report
