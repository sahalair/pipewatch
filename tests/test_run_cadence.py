"""Tests for pipewatch.run_cadence."""

from unittest.mock import patch
import pytest

from pipewatch.run_cadence import (
    compute_intervals,
    cadence_stats,
    classify_cadence,
    analyze_cadence,
    format_cadence_report,
    get_run_timestamps,
)


def _make_run(pipeline: str, started: str, status: str = "success") -> dict:
    return {"pipeline": pipeline, "started": started, "status": status}


_FAKE_RUNS = [
    _make_run("etl", "2024-01-01T00:00:00"),
    _make_run("etl", "2024-01-01T01:00:00"),
    _make_run("etl", "2024-01-01T02:00:00"),
    _make_run("other", "2024-01-01T00:30:00"),
]


def _patch(runs=None):
    return patch(
        "pipewatch.run_cadence.list_run_records",
        return_value=runs if runs is not None else _FAKE_RUNS,
    )


def test_compute_intervals_basic():
    ts = [0.0, 3600.0, 7200.0]
    assert compute_intervals(ts) == [3600.0, 3600.0]


def test_compute_intervals_single_element():
    assert compute_intervals([100.0]) == []


def test_compute_intervals_empty():
    assert compute_intervals([]) == []


def test_cadence_stats_basic():
    stats = cadence_stats([100.0, 200.0, 300.0])
    assert stats["count"] == 3
    assert stats["mean"] == 200.0
    assert stats["min"] == 100.0
    assert stats["max"] == 300.0
    assert isinstance(stats["stddev"], float)


def test_cadence_stats_empty():
    stats = cadence_stats([])
    assert stats["mean"] is None
    assert stats["count"] == 0


def test_classify_cadence_very_regular():
    stats = {"mean": 3600.0, "stddev": 10.0}
    assert classify_cadence(stats) == "very_regular"


def test_classify_cadence_regular():
    stats = {"mean": 3600.0, "stddev": 500.0}
    assert classify_cadence(stats) == "regular"


def test_classify_cadence_irregular():
    stats = {"mean": 3600.0, "stddev": 1500.0}
    assert classify_cadence(stats) == "irregular"


def test_classify_cadence_chaotic():
    stats = {"mean": 100.0, "stddev": 300.0}
    assert classify_cadence(stats) == "chaotic"


def test_classify_cadence_unknown_no_mean():
    stats = {"mean": None, "stddev": None}
    assert classify_cadence(stats) == "unknown"


def test_get_run_timestamps_filters_by_pipeline():
    with _patch():
        ts = get_run_timestamps("etl")
    assert len(ts) == 3


def test_get_run_timestamps_sorted_ascending():
    with _patch():
        ts = get_run_timestamps("etl")
    assert ts == sorted(ts)


def test_analyze_cadence_returns_required_keys():
    with _patch():
        result = analyze_cadence("etl")
    assert "pipeline" in result
    assert "run_count" in result
    assert "intervals" in result
    assert "cadence" in result


def test_analyze_cadence_regular_pipeline():
    with _patch():
        result = analyze_cadence("etl")
    assert result["cadence"] == "very_regular"
    assert result["run_count"] == 3


def test_analyze_cadence_unknown_for_single_run():
    single = [_make_run("solo", "2024-01-01T00:00:00")]
    with _patch(single):
        result = analyze_cadence("solo")
    assert result["cadence"] == "unknown"
    assert result["intervals"]["mean"] is None


def test_format_cadence_report_contains_pipeline():
    with _patch():
        result = analyze_cadence("etl")
    report = format_cadence_report(result)
    assert "etl" in report
    assert "VERY_REGULAR" in report


def test_format_cadence_report_no_data_message():
    result = {
        "pipeline": "empty",
        "run_count": 0,
        "intervals": {"mean": None, "stddev": None, "min": None, "max": None, "count": 0},
        "cadence": "unknown",
    }
    report = format_cadence_report(result)
    assert "Insufficient" in report
