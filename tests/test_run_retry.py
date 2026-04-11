"""Tests for pipewatch.run_retry."""

from __future__ import annotations

import json
import pytest

from pipewatch.run_retry import (
    load_retry_map,
    save_retry_map,
    register_retry,
    get_retries,
    get_original_run,
    retry_chain,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _write_fake_record(base_dir, run_id: str, status: str = "ok") -> dict:
    """Write a minimal run record so load_run_record can find it."""
    from pathlib import Path
    record = {"run_id": run_id, "status": status, "pipeline": "test"}
    p = Path(base_dir) / f"{run_id}.json"
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(json.dumps(record))
    return record


# ---------------------------------------------------------------------------
# load / save
# ---------------------------------------------------------------------------

def test_load_retry_map_missing_file(tmp_path):
    assert load_retry_map(str(tmp_path)) == {}


def test_save_and_load_retry_map_roundtrip(tmp_path):
    data = {"run-1": ["run-2", "run-3"]}
    save_retry_map(str(tmp_path), data)
    assert load_retry_map(str(tmp_path)) == data


# ---------------------------------------------------------------------------
# register_retry
# ---------------------------------------------------------------------------

def test_register_retry_creates_entry(tmp_path):
    register_retry(str(tmp_path), "orig-1", "retry-1")
    assert load_retry_map(str(tmp_path)) == {"orig-1": ["retry-1"]}


def test_register_retry_appends_multiple(tmp_path):
    register_retry(str(tmp_path), "orig-1", "retry-1")
    register_retry(str(tmp_path), "orig-1", "retry-2")
    assert get_retries(str(tmp_path), "orig-1") == ["retry-1", "retry-2"]


def test_register_retry_independent_originals(tmp_path):
    register_retry(str(tmp_path), "orig-1", "retry-1")
    register_retry(str(tmp_path), "orig-2", "retry-2")
    m = load_retry_map(str(tmp_path))
    assert m["orig-1"] == ["retry-1"]
    assert m["orig-2"] == ["retry-2"]


# ---------------------------------------------------------------------------
# get_retries / get_original_run
# ---------------------------------------------------------------------------

def test_get_retries_empty(tmp_path):
    assert get_retries(str(tmp_path), "nonexistent") == []


def test_get_original_run_found(tmp_path):
    register_retry(str(tmp_path), "orig-1", "retry-1")
    assert get_original_run(str(tmp_path), "retry-1") == "orig-1"


def test_get_original_run_not_found(tmp_path):
    assert get_original_run(str(tmp_path), "ghost-run") is None


# ---------------------------------------------------------------------------
# retry_chain
# ---------------------------------------------------------------------------

def test_retry_chain_single_run(tmp_path):
    _write_fake_record(str(tmp_path), "orig-1")
    chain = retry_chain(str(tmp_path), "orig-1")
    assert len(chain) == 1
    assert chain[0]["run_id"] == "orig-1"


def test_retry_chain_with_retries(tmp_path):
    _write_fake_record(str(tmp_path), "orig-1")
    _write_fake_record(str(tmp_path), "retry-1")
    _write_fake_record(str(tmp_path), "retry-2")
    register_retry(str(tmp_path), "orig-1", "retry-1")
    register_retry(str(tmp_path), "orig-1", "retry-2")
    chain = retry_chain(str(tmp_path), "orig-1")
    assert [r["run_id"] for r in chain] == ["orig-1", "retry-1", "retry-2"]


def test_retry_chain_missing_record_raises(tmp_path):
    register_retry(str(tmp_path), "orig-1", "retry-missing")
    with pytest.raises(FileNotFoundError):
        retry_chain(str(tmp_path), "orig-1")
