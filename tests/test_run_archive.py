"""Tests for pipewatch.run_archive."""

import json
import os
import pytest

from pipewatch.run_logger import create_run_record, save_run_record
from pipewatch.run_archive import (
    archive_run,
    restore_run,
    list_archived_runs,
    purge_archive,
)


def _make_run(tmp_path, pipeline="test-pipeline"):
    record = create_run_record(pipeline)
    save_run_record(record, base_dir=str(tmp_path))
    return record


def test_archive_run_moves_record(tmp_path):
    record = _make_run(tmp_path)
    run_id = record["run_id"]
    archived = archive_run(run_id, base_dir=str(tmp_path))
    assert archived["run_id"] == run_id
    assert "archived_at" in archived
    active_path = tmp_path / "runs" / f"{run_id}.json"
    assert not active_path.exists()
    archive_path = tmp_path / "archive" / f"{run_id}.json"
    assert archive_path.exists()


def test_archive_run_not_found_raises(tmp_path):
    with pytest.raises(FileNotFoundError):
        archive_run("nonexistent-id", base_dir=str(tmp_path))


def test_restore_run_moves_back(tmp_path):
    record = _make_run(tmp_path)
    run_id = record["run_id"]
    archive_run(run_id, base_dir=str(tmp_path))
    restored = restore_run(run_id, base_dir=str(tmp_path))
    assert restored["run_id"] == run_id
    assert "archived_at" not in restored
    active_path = tmp_path / "runs" / f"{run_id}.json"
    assert active_path.exists()
    archive_path = tmp_path / "archive" / f"{run_id}.json"
    assert not archive_path.exists()


def test_restore_run_not_found_raises(tmp_path):
    with pytest.raises(FileNotFoundError):
        restore_run("nonexistent-id", base_dir=str(tmp_path))


def test_list_archived_runs_empty(tmp_path):
    result = list_archived_runs(base_dir=str(tmp_path))
    assert result == []


def test_list_archived_runs_returns_sorted(tmp_path):
    r1 = _make_run(tmp_path, pipeline="pipe-a")
    r2 = _make_run(tmp_path, pipeline="pipe-b")
    archive_run(r1["run_id"], base_dir=str(tmp_path))
    archive_run(r2["run_id"], base_dir=str(tmp_path))
    results = list_archived_runs(base_dir=str(tmp_path))
    assert len(results) == 2
    assert results[0]["archived_at"] >= results[1]["archived_at"]


def test_purge_archive_removes_all(tmp_path):
    r1 = _make_run(tmp_path)
    r2 = _make_run(tmp_path)
    archive_run(r1["run_id"], base_dir=str(tmp_path))
    archive_run(r2["run_id"], base_dir=str(tmp_path))
    count = purge_archive(base_dir=str(tmp_path))
    assert count == 2
    assert list_archived_runs(base_dir=str(tmp_path)) == []


def test_purge_archive_empty_dir(tmp_path):
    count = purge_archive(base_dir=str(tmp_path))
    assert count == 0
