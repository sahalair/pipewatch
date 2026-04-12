"""Tests for pipewatch.run_cleanup."""

from __future__ import annotations

import json
import os
from datetime import datetime, timezone, timedelta

import pytest

from pipewatch.run_cleanup import (
    find_stale_runs,
    mark_runs_archived,
    purge_runs,
    format_cleanup_report,
)


def _write_run(base_dir, run_id, started_at, status="ok"):
    runs_dir = os.path.join(base_dir, "runs")
    os.makedirs(runs_dir, exist_ok=True)
    record = {"run_id": run_id, "started_at": started_at, "status": status}
    with open(os.path.join(runs_dir, f"{run_id}.json"), "w") as f:
        json.dump(record, f)
    return record


def _old_ts(days=60):
    return (datetime.now(tz=timezone.utc) - timedelta(days=days)).isoformat()


def _new_ts(days=1):
    return (datetime.now(tz=timezone.utc) - timedelta(days=days)).isoformat()


def test_find_stale_runs_returns_old(tmp_path):
    base = str(tmp_path)
    _write_run(base, "old-run", _old_ts(60))
    _write_run(base, "new-run", _new_ts(1))
    stale = find_stale_runs(base, older_than_days=30)
    ids = [r["run_id"] for r in stale]
    assert "old-run" in ids
    assert "new-run" not in ids


def test_find_stale_runs_empty_when_all_recent(tmp_path):
    base = str(tmp_path)
    _write_run(base, "r1", _new_ts(1))
    assert find_stale_runs(base, older_than_days=30) == []


def test_find_stale_runs_filters_by_status(tmp_path):
    base = str(tmp_path)
    _write_run(base, "failed-old", _old_ts(60), status="failed")
    _write_run(base, "ok-old", _old_ts(60), status="ok")
    stale = find_stale_runs(base, older_than_days=30, status="failed")
    ids = [r["run_id"] for r in stale]
    assert "failed-old" in ids
    assert "ok-old" not in ids


def test_mark_runs_archived_sets_flag(tmp_path):
    base = str(tmp_path)
    _write_run(base, "r1", _old_ts())
    processed = mark_runs_archived(base, ["r1"])
    assert "r1" in processed
    path = os.path.join(base, "runs", "r1.json")
    with open(path) as f:
        data = json.load(f)
    assert data["cleanup_status"] == "archived"


def test_mark_runs_archived_skips_missing(tmp_path):
    base = str(tmp_path)
    os.makedirs(os.path.join(base, "runs"))
    processed = mark_runs_archived(base, ["ghost-run"])
    assert processed == []


def test_purge_runs_removes_files(tmp_path):
    base = str(tmp_path)
    _write_run(base, "r1", _old_ts())
    _write_run(base, "r2", _old_ts())
    removed = purge_runs(base, ["r1", "r2"])
    assert set(removed) == {"r1", "r2"}
    assert not os.path.exists(os.path.join(base, "runs", "r1.json"))
    assert not os.path.exists(os.path.join(base, "runs", "r2.json"))


def test_purge_runs_ignores_nonexistent(tmp_path):
    base = str(tmp_path)
    os.makedirs(os.path.join(base, "runs"))
    removed = purge_runs(base, ["no-such-run"])
    assert removed == []


def test_format_cleanup_report_contains_counts(tmp_path):
    stale = [{"run_id": "r1", "started_at": _old_ts(), "status": "ok"}]
    purged = ["r1"]
    report = format_cleanup_report(stale, purged)
    assert "Stale runs found: 1" in report
    assert "Purged: 1" in report
    assert "r1" in report
