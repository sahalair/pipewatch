import json
import pytest
from pathlib import Path
from pipewatch.run_blackout import (
    add_blackout_window,
    remove_blackout_window,
    is_in_blackout,
    list_blackout_windows,
    load_blackout_windows,
)


@pytest.fixture
def base(tmp_path):
    return str(tmp_path)


def test_load_blackout_windows_missing_file_returns_empty(base):
    result = load_blackout_windows(base)
    assert result == {}


def test_add_blackout_window_creates_entry(base):
    entry = add_blackout_window(
        "maintenance",
        "2030-01-01T00:00:00+00:00",
        "2030-01-01T06:00:00+00:00",
        base_dir=base,
    )
    assert entry["name"] == "maintenance"
    assert entry["start_time"] == "2030-01-01T00:00:00+00:00"
    assert entry["end_time"] == "2030-01-01T06:00:00+00:00"
    assert entry["pipelines"] == []
    assert entry["reason"] == ""


def test_add_blackout_window_with_pipelines_and_reason(base):
    entry = add_blackout_window(
        "deploy",
        "2030-06-01T12:00:00+00:00",
        "2030-06-01T14:00:00+00:00",
        pipelines=["etl", "reports"],
        reason="deployment freeze",
        base_dir=base,
    )
    assert entry["pipelines"] == ["etl", "reports"]
    assert entry["reason"] == "deployment freeze"


def test_add_blackout_window_empty_name_raises(base):
    with pytest.raises(ValueError):
        add_blackout_window("", "2030-01-01T00:00:00+00:00", "2030-01-01T06:00:00+00:00", base_dir=base)


def test_add_blackout_window_overwrites_existing(base):
    add_blackout_window("w1", "2030-01-01T00:00:00+00:00", "2030-01-01T01:00:00+00:00", base_dir=base)
    add_blackout_window("w1", "2030-02-01T00:00:00+00:00", "2030-02-01T01:00:00+00:00", base_dir=base)
    windows = load_blackout_windows(base)
    assert windows["w1"]["start_time"] == "2030-02-01T00:00:00+00:00"


def test_remove_blackout_window_existing(base):
    add_blackout_window("w1", "2030-01-01T00:00:00+00:00", "2030-01-01T01:00:00+00:00", base_dir=base)
    removed = remove_blackout_window("w1", base_dir=base)
    assert removed is True
    assert load_blackout_windows(base) == {}


def test_remove_blackout_window_nonexistent_returns_false(base):
    assert remove_blackout_window("ghost", base_dir=base) is False


def test_is_in_blackout_true_when_within_window(base):
    add_blackout_window(
        "active",
        "2025-01-01T00:00:00+00:00",
        "2099-12-31T23:59:59+00:00",
        base_dir=base,
    )
    assert is_in_blackout("any_pipeline", base_dir=base) is True


def test_is_in_blackout_false_when_outside_window(base):
    add_blackout_window(
        "past",
        "2000-01-01T00:00:00+00:00",
        "2000-01-02T00:00:00+00:00",
        base_dir=base,
    )
    assert is_in_blackout("any_pipeline", base_dir=base) is False


def test_is_in_blackout_respects_pipeline_filter(base):
    add_blackout_window(
        "targeted",
        "2025-01-01T00:00:00+00:00",
        "2099-12-31T23:59:59+00:00",
        pipelines=["etl"],
        base_dir=base,
    )
    assert is_in_blackout("etl", base_dir=base) is True
    assert is_in_blackout("reports", base_dir=base) is False


def test_is_in_blackout_with_explicit_time(base):
    add_blackout_window(
        "w",
        "2030-06-01T10:00:00+00:00",
        "2030-06-01T12:00:00+00:00",
        base_dir=base,
    )
    assert is_in_blackout("p", at="2030-06-01T11:00:00+00:00", base_dir=base) is True
    assert is_in_blackout("p", at="2030-06-01T13:00:00+00:00", base_dir=base) is False


def test_list_blackout_windows_sorted_by_start(base):
    add_blackout_window("b", "2030-06-01T00:00:00+00:00", "2030-06-01T01:00:00+00:00", base_dir=base)
    add_blackout_window("a", "2030-01-01T00:00:00+00:00", "2030-01-01T01:00:00+00:00", base_dir=base)
    result = list_blackout_windows(base)
    assert result[0]["name"] == "a"
    assert result[1]["name"] == "b"
