"""Tests for pipewatch.snapshot module."""

import pytest

from pipewatch.snapshot import (
    delete_snapshot,
    list_snapshots,
    load_snapshot,
    save_snapshot,
    snapshot_exists,
)


def test_save_and_load_snapshot(tmp_path):
    data = {"rows": 42, "status": "ok"}
    path = save_snapshot("test_snap", data, snapshot_dir=str(tmp_path))
    assert path.exists()

    loaded = load_snapshot("test_snap", snapshot_dir=str(tmp_path))
    assert loaded["name"] == "test_snap"
    assert loaded["data"] == data


def test_save_snapshot_with_metadata(tmp_path):
    meta = {"run_id": "abc123", "pipeline": "etl"}
    save_snapshot("meta_snap", {"x": 1}, metadata=meta, snapshot_dir=str(tmp_path))
    loaded = load_snapshot("meta_snap", snapshot_dir=str(tmp_path))
    assert loaded["metadata"]["run_id"] == "abc123"


def test_load_snapshot_not_found(tmp_path):
    with pytest.raises(FileNotFoundError, match="missing_snap"):
        load_snapshot("missing_snap", snapshot_dir=str(tmp_path))


def test_list_snapshots_empty(tmp_path):
    assert list_snapshots(snapshot_dir=str(tmp_path)) == []


def test_list_snapshots_returns_sorted(tmp_path):
    for name in ["beta", "alpha", "gamma"]:
        save_snapshot(name, {}, snapshot_dir=str(tmp_path))
    names = list_snapshots(snapshot_dir=str(tmp_path))
    assert names == ["alpha", "beta", "gamma"]


def test_snapshot_exists_true(tmp_path):
    save_snapshot("exists_snap", {"a": 1}, snapshot_dir=str(tmp_path))
    assert snapshot_exists("exists_snap", snapshot_dir=str(tmp_path)) is True


def test_snapshot_exists_false(tmp_path):
    assert snapshot_exists("nope", snapshot_dir=str(tmp_path)) is False


def test_delete_snapshot(tmp_path):
    save_snapshot("to_delete", {"v": 99}, snapshot_dir=str(tmp_path))
    assert snapshot_exists("to_delete", snapshot_dir=str(tmp_path))
    result = delete_snapshot("to_delete", snapshot_dir=str(tmp_path))
    assert result is True
    assert not snapshot_exists("to_delete", snapshot_dir=str(tmp_path))


def test_delete_snapshot_not_found(tmp_path):
    result = delete_snapshot("ghost", snapshot_dir=str(tmp_path))
    assert result is False


def test_save_snapshot_creates_nested_dirs(tmp_path):
    nested = str(tmp_path / "deep" / "nested")
    save_snapshot("nested_snap", [1, 2, 3], snapshot_dir=nested)
    assert snapshot_exists("nested_snap", snapshot_dir=nested)
