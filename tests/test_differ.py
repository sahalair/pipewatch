"""Tests for pipewatch.differ and pipewatch.run_diff."""

from __future__ import annotations

import json
import tempfile
from pathlib import Path

import pytest

from pipewatch.differ import (
    diff_texts,
    diff_json_serializable,
    diff_summary,
    format_diff,
)
from pipewatch.run_diff import diff_runs
from pipewatch.run_logger import create_run_record, finish_run_record, save_run_record


# ---------------------------------------------------------------------------
# differ.py unit tests
# ---------------------------------------------------------------------------

def test_diff_texts_identical():
    lines = diff_texts("hello\n", "hello\n")
    assert lines == []


def test_diff_texts_detects_change():
    lines = diff_texts("foo\n", "bar\n")
    assert any(line.startswith("-foo") for line in lines)
    assert any(line.startswith("+bar") for line in lines)


def test_diff_texts_labels_appear_in_header():
    lines = diff_texts("a\n", "b\n", label_old="v1", label_new="v2")
    header = "".join(lines[:2])
    assert "v1" in header
    assert "v2" in header


def test_diff_json_serializable_identical():
    obj = {"key": [1, 2, 3]}
    assert diff_json_serializable(obj, obj) == []


def test_diff_json_serializable_detects_change():
    old = {"value": 1}
    new = {"value": 2}
    lines = diff_json_serializable(old, new)
    assert len(lines) > 0


def test_diff_summary_counts():
    diff_lines = ["+added line\n", "-removed line\n", " context\n"]
    summary = diff_summary(diff_lines)
    assert summary["added"] == 1
    assert summary["removed"] == 1
    assert summary["changed"] == 2


def test_diff_summary_empty():
    assert diff_summary([]) == {"added": 0, "removed": 0, "changed": 0}


def test_format_diff_no_color():
    lines = ["+foo\n", "-bar\n"]
    result = format_diff(lines, colorize=False)
    assert result == "+foo\n-bar\n"


def test_format_diff_with_color_contains_ansi():
    lines = ["+added\n", "-removed\n"]
    result = format_diff(lines, colorize=True)
    assert "\033[32m" in result  # green for added
    assert "\033[31m" in result  # red for removed


# ---------------------------------------------------------------------------
# run_diff.py integration tests
# ---------------------------------------------------------------------------

def _make_run(store_dir: Path, output_value):
    """Create, finish, and persist a run record; return its run_id."""
    rec = create_run_record(pipeline="test-pipe", metadata={"output": output_value})
    rec = finish_run_record(rec, status="success")
    save_run_record(rec, store_dir=store_dir)
    return rec["run_id"]


def test_diff_runs_identical():
    with tempfile.TemporaryDirectory() as tmp:
        store = Path(tmp)
        rid = _make_run(store, {"rows": 100})
        result = diff_runs(rid, rid, output_key="output", store_dir=store)
        assert result["identical"] is True
        assert result["summary"]["changed"] == 0


def test_diff_runs_detects_difference():
    with tempfile.TemporaryDirectory() as tmp:
        store = Path(tmp)
        rid_a = _make_run(store, {"rows": 100})
        rid_b = _make_run(store, {"rows": 200})
        result = diff_runs(rid_a, rid_b, output_key="output", store_dir=store)
        assert result["identical"] is False
        assert result["summary"]["changed"] > 0
