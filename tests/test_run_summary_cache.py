"""Tests for pipewatch.run_summary_cache."""

import time
import pytest

from pipewatch.run_summary_cache import (
    clear_cache,
    get_cached_summary,
    invalidate_cached_summary,
    load_cache,
    set_cached_summary,
)


@pytest.fixture()
def cache_dir(tmp_path):
    return str(tmp_path)


def test_load_cache_missing_file_returns_empty(cache_dir):
    cache = load_cache(cache_dir)
    assert cache["entries"] == {}
    assert cache["version"] == 1


def test_set_and_get_cached_summary(cache_dir):
    summary = {"status": "ok", "metrics": {"rows": 100}}
    set_cached_summary(cache_dir, "run-abc", summary)
    result = get_cached_summary(cache_dir, "run-abc")
    assert result == summary


def test_get_cached_summary_missing_run_returns_none(cache_dir):
    result = get_cached_summary(cache_dir, "nonexistent-run")
    assert result is None


def test_get_cached_summary_expired_returns_none(cache_dir):
    summary = {"status": "ok"}
    set_cached_summary(cache_dir, "run-xyz", summary)
    # TTL of 0 seconds means immediately expired
    result = get_cached_summary(cache_dir, "run-xyz", ttl_seconds=0.0)
    assert result is None


def test_get_cached_summary_within_ttl_returns_data(cache_dir):
    summary = {"status": "failed"}
    set_cached_summary(cache_dir, "run-ttl", summary)
    result = get_cached_summary(cache_dir, "run-ttl", ttl_seconds=60.0)
    assert result == summary


def test_set_cached_summary_overwrites_existing(cache_dir):
    set_cached_summary(cache_dir, "run-1", {"status": "ok"})
    set_cached_summary(cache_dir, "run-1", {"status": "failed"})
    result = get_cached_summary(cache_dir, "run-1")
    assert result == {"status": "failed"}


def test_invalidate_cached_summary_removes_entry(cache_dir):
    set_cached_summary(cache_dir, "run-del", {"status": "ok"})
    existed = invalidate_cached_summary(cache_dir, "run-del")
    assert existed is True
    assert get_cached_summary(cache_dir, "run-del") is None


def test_invalidate_cached_summary_nonexistent_returns_false(cache_dir):
    existed = invalidate_cached_summary(cache_dir, "ghost-run")
    assert existed is False


def test_clear_cache_removes_all_entries(cache_dir):
    set_cached_summary(cache_dir, "run-a", {"status": "ok"})
    set_cached_summary(cache_dir, "run-b", {"status": "ok"})
    count = clear_cache(cache_dir)
    assert count == 2
    assert get_cached_summary(cache_dir, "run-a") is None
    assert get_cached_summary(cache_dir, "run-b") is None


def test_clear_cache_empty_returns_zero(cache_dir):
    count = clear_cache(cache_dir)
    assert count == 0


def test_cache_persists_across_load(cache_dir):
    summary = {"pipeline": "etl", "exit_code": 0}
    set_cached_summary(cache_dir, "run-persist", summary)
    # Re-load from disk
    result = get_cached_summary(cache_dir, "run-persist", ttl_seconds=300.0)
    assert result == summary
