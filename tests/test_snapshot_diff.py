"""Tests for pipewatch.snapshot_diff module."""

import pytest

from pipewatch.snapshot import save_snapshot
from pipewatch.snapshot_diff import diff_snapshots, print_snapshot_diff


def test_diff_snapshots_identical(tmp_path):
    data = {"count": 10, "status": "ok"}
    save_snapshot("snap_a", data, snapshot_dir=str(tmp_path))
    save_snapshot("snap_b", data, snapshot_dir=str(tmp_path))

    result = diff_snapshots("snap_a", "snap_b", snapshot_dir=str(tmp_path))
    assert result["changed"] is False
    assert result["hash_a"] == result["hash_b"]


def test_diff_snapshots_detects_change(tmp_path):
    save_snapshot("v1", {"count": 10}, snapshot_dir=str(tmp_path))
    save_snapshot("v2", {"count": 99}, snapshot_dir=str(tmp_path))

    result = diff_snapshots("v1", "v2", snapshot_dir=str(tmp_path))
    assert result["changed"] is True
    assert result["hash_a"] != result["hash_b"]


def test_diff_snapshots_result_has_required_keys(tmp_path):
    save_snapshot("s1", ["a", "b"], snapshot_dir=str(tmp_path))
    save_snapshot("s2", ["a", "c"], snapshot_dir=str(tmp_path))

    result = diff_snapshots("s1", "s2", snapshot_dir=str(tmp_path))
    for key in ("name_a", "name_b", "hash_a", "hash_b", "changed", "diff_lines", "summary"):
        assert key in result


def test_diff_snapshots_diff_lines_non_empty_on_change(tmp_path):
    save_snapshot("old", {"x": 1}, snapshot_dir=str(tmp_path))
    save_snapshot("new", {"x": 2}, snapshot_dir=str(tmp_path))

    result = diff_snapshots("old", "new", snapshot_dir=str(tmp_path))
    assert len(result["diff_lines"]) > 0


def test_diff_snapshots_missing_snapshot_raises(tmp_path):
    save_snapshot("only_one", {}, snapshot_dir=str(tmp_path))
    with pytest.raises(FileNotFoundError):
        diff_snapshots("only_one", "missing", snapshot_dir=str(tmp_path))


def test_print_snapshot_diff_identical(tmp_path, capsys):
    save_snapshot("pa", {"v": 1}, snapshot_dir=str(tmp_path))
    save_snapshot("pb", {"v": 1}, snapshot_dir=str(tmp_path))
    print_snapshot_diff("pa", "pb", snapshot_dir=str(tmp_path), color=False)
    captured = capsys.readouterr()
    assert "identical" in captured.out


def test_print_snapshot_diff_changed(tmp_path, capsys):
    save_snapshot("qa", {"v": 1}, snapshot_dir=str(tmp_path))
    save_snapshot("qb", {"v": 2}, snapshot_dir=str(tmp_path))
    print_snapshot_diff("qa", "qb", snapshot_dir=str(tmp_path), color=False)
    captured = capsys.readouterr()
    assert "qa" in captured.out
    assert "qb" in captured.out
