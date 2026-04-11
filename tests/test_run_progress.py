"""Tests for pipewatch.run_progress."""

import pytest

from pipewatch.run_progress import (
    finish_progress,
    format_progress,
    get_progress,
    load_progress,
    start_progress,
    update_progress,
)


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------


def _base(tmp_path):
    return str(tmp_path)


# ---------------------------------------------------------------------------
# load_progress
# ---------------------------------------------------------------------------


def test_load_progress_missing_file_returns_empty(tmp_path):
    result = load_progress(base_dir=_base(tmp_path))
    assert result == {}


# ---------------------------------------------------------------------------
# start_progress
# ---------------------------------------------------------------------------


def test_start_progress_creates_entry(tmp_path):
    entry = start_progress("run-1", "my_pipe", total_steps=5, base_dir=_base(tmp_path))
    assert entry["run_id"] == "run-1"
    assert entry["pipeline"] == "my_pipe"
    assert entry["total_steps"] == 5
    assert entry["current_step"] == 0
    assert entry["percent"] == 0.0
    assert "started_at" in entry


def test_start_progress_persists(tmp_path):
    start_progress("run-2", "pipe_a", base_dir=_base(tmp_path))
    data = load_progress(base_dir=_base(tmp_path))
    assert "run-2" in data


# ---------------------------------------------------------------------------
# update_progress
# ---------------------------------------------------------------------------


def test_update_progress_changes_step(tmp_path):
    start_progress("run-3", "pipe_b", total_steps=10, base_dir=_base(tmp_path))
    entry = update_progress("run-3", 4, step_name="transform", base_dir=_base(tmp_path))
    assert entry["current_step"] == 4
    assert entry["step_name"] == "transform"
    assert entry["percent"] == 40.0


def test_update_progress_zero_total_gives_zero_percent(tmp_path):
    start_progress("run-4", "pipe_c", total_steps=0, base_dir=_base(tmp_path))
    entry = update_progress("run-4", 3, base_dir=_base(tmp_path))
    assert entry["percent"] == 0.0


def test_update_progress_missing_run_raises(tmp_path):
    with pytest.raises(KeyError):
        update_progress("nonexistent", 1, base_dir=_base(tmp_path))


# ---------------------------------------------------------------------------
# get_progress
# ---------------------------------------------------------------------------


def test_get_progress_returns_entry(tmp_path):
    start_progress("run-5", "pipe_d", base_dir=_base(tmp_path))
    result = get_progress("run-5", base_dir=_base(tmp_path))
    assert result is not None
    assert result["run_id"] == "run-5"


def test_get_progress_missing_returns_none(tmp_path):
    assert get_progress("ghost", base_dir=_base(tmp_path)) is None


# ---------------------------------------------------------------------------
# finish_progress
# ---------------------------------------------------------------------------


def test_finish_progress_removes_entry(tmp_path):
    start_progress("run-6", "pipe_e", base_dir=_base(tmp_path))
    finish_progress("run-6", base_dir=_base(tmp_path))
    assert get_progress("run-6", base_dir=_base(tmp_path)) is None


def test_finish_progress_nonexistent_is_noop(tmp_path):
    # Should not raise
    finish_progress("ghost", base_dir=_base(tmp_path))


# ---------------------------------------------------------------------------
# format_progress
# ---------------------------------------------------------------------------


def test_format_progress_includes_pipeline_and_run_id(tmp_path):
    entry = start_progress("run-7", "my_pipeline", total_steps=4, base_dir=_base(tmp_path))
    entry = update_progress("run-7", 2, step_name="load", base_dir=_base(tmp_path))
    text = format_progress(entry)
    assert "my_pipeline" in text
    assert "run-7" in text
    assert "50.0%" in text
    assert "load" in text


def test_format_progress_no_step_name(tmp_path):
    entry = start_progress("run-8", "pipe_f", total_steps=2, base_dir=_base(tmp_path))
    text = format_progress(entry)
    assert "(" not in text or "step" in text
