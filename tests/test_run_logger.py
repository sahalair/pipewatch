"""Tests for pipewatch.run_logger and pipewatch.output_hasher."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from pipewatch.output_hasher import (
    hash_file,
    hash_json_serializable,
    hash_text,
    hashes_differ,
)
from pipewatch.run_logger import (
    create_run_record,
    finish_run_record,
    list_run_records,
    load_run_record,
    save_run_record,
)


# ---------------------------------------------------------------------------
# run_logger tests
# ---------------------------------------------------------------------------

def test_create_run_record_has_required_fields():
    rec = create_run_record("my_pipeline")
    assert rec["pipeline"] == "my_pipeline"
    assert rec["status"] == "running"
    assert rec["finished_at"] is None
    assert "run_id" in rec
    assert "started_at" in rec


def test_create_run_record_unique_ids():
    ids = {create_run_record("p")["run_id"] for _ in range(10)}
    assert len(ids) == 10


def test_finish_run_record():
    rec = create_run_record("p")
    finished = finish_run_record(rec, status="success", output_hash="abc123")
    assert finished["status"] == "success"
    assert finished["finished_at"] is not None
    assert finished["output_hash"] == "abc123"


def test_save_and_load_run_record(tmp_path: Path):
    rec = create_run_record("etl_job", metadata={"env": "test"})
    finish_run_record(rec)
    saved_path = save_run_record(rec, log_dir=tmp_path)
    assert saved_path.exists()
    loaded = load_run_record(saved_path)
    assert loaded["run_id"] == rec["run_id"]
    assert loaded["metadata"] == {"env": "test"}


def test_list_run_records_sorted(tmp_path: Path):
    for name in ["alpha", "beta", "gamma"]:
        rec = create_run_record("pipe", metadata={"name": name})
        finish_run_record(rec)
        save_run_record(rec, log_dir=tmp_path)

    records = list_run_records("pipe", log_dir=tmp_path)
    assert len(records) == 3
    starts = [r["started_at"] for r in records]
    assert starts == sorted(starts)


def test_list_run_records_empty(tmp_path: Path):
    assert list_run_records("nonexistent", log_dir=tmp_path) == []


# ---------------------------------------------------------------------------
# output_hasher tests
# ---------------------------------------------------------------------------

def test_hash_text_deterministic():
    assert hash_text("hello") == hash_text("hello")


def test_hash_text_differs_on_different_input():
    assert hash_text("hello") != hash_text("world")


def test_hash_file(tmp_path: Path):
    f = tmp_path / "data.txt"
    f.write_text("pipeline output")
    h1 = hash_file(f)
    h2 = hash_file(f)
    assert h1 == h2
    assert len(h1) == 64  # sha256 hex


def test_hash_json_serializable_stable():
    obj = {"b": 2, "a": 1}
    assert hash_json_serializable(obj) == hash_json_serializable({"a": 1, "b": 2})


def test_hashes_differ():
    assert hashes_differ("aaa", "bbb") is True
    assert hashes_differ("aaa", "aaa") is False
