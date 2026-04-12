"""Tests for pipewatch.run_throughput."""

from unittest.mock import patch

import pytest

from pipewatch.run_throughput import (
    _window_start,
    compute_throughput,
    format_throughput_report,
)
from datetime import datetime, timezone


# ---------------------------------------------------------------------------
# _window_start
# ---------------------------------------------------------------------------

def _dt(iso: str) -> datetime:
    return datetime.fromisoformat(iso).replace(tzinfo=timezone.utc)


def test_window_start_hour():
    dt = _dt("2024-03-15T14:37:22")
    assert _window_start(dt, "hour").isoformat() == "2024-03-15T14:00:00+00:00"


def test_window_start_day():
    dt = _dt("2024-03-15T14:37:22")
    assert _window_start(dt, "day").isoformat() == "2024-03-15T00:00:00+00:00"


def test_window_start_week():
    # 2024-03-15 is a Friday; week should start on Monday 2024-03-11
    dt = _dt("2024-03-15T14:37:22")
    result = _window_start(dt, "week")
    assert result.isoformat() == "2024-03-11T00:00:00+00:00"


def test_window_start_unknown_raises():
    with pytest.raises(ValueError, match="Unknown window"):
        _window_start(_dt("2024-03-15T00:00:00"), "month")


# ---------------------------------------------------------------------------
# compute_throughput
# ---------------------------------------------------------------------------

_FAKE_RECORDS = [
    {"pipeline": "etl", "started": "2024-03-15T08:00:00"},
    {"pipeline": "etl", "started": "2024-03-15T12:00:00"},
    {"pipeline": "etl", "started": "2024-03-16T09:00:00"},
    {"pipeline": "ingest", "started": "2024-03-15T10:00:00"},
    {"pipeline": "ingest", "started": "2024-03-17T11:00:00"},
]


def _patch(records=None):
    return patch(
        "pipewatch.run_throughput.list_run_records",
        return_value=records if records is not None else _FAKE_RECORDS,
    )


def test_compute_throughput_all_pipelines():
    with _patch():
        result = compute_throughput(window="day")
    # 2024-03-15: 3 runs (etl x2 + ingest), 2024-03-16: 1, 2024-03-17: 1
    assert result["2024-03-15T00:00:00+00:00"] == 3
    assert result["2024-03-16T00:00:00+00:00"] == 1
    assert result["2024-03-17T00:00:00+00:00"] == 1


def test_compute_throughput_filtered_pipeline():
    with _patch():
        result = compute_throughput(pipeline="etl", window="day")
    assert result["2024-03-15T00:00:00+00:00"] == 2
    assert result["2024-03-16T00:00:00+00:00"] == 1
    assert "2024-03-17T00:00:00+00:00" not in result


def test_compute_throughput_empty_returns_empty():
    with _patch(records=[]):
        result = compute_throughput(window="day")
    assert result == {}


def test_compute_throughput_skips_missing_started():
    records = [{"pipeline": "etl"}, {"pipeline": "etl", "started": "2024-03-15T08:00:00"}]
    with _patch(records=records):
        result = compute_throughput(window="day")
    assert sum(result.values()) == 1


def test_compute_throughput_result_is_sorted():
    with _patch():
        result = compute_throughput(window="day")
    keys = list(result.keys())
    assert keys == sorted(keys)


# ---------------------------------------------------------------------------
# format_throughput_report
# ---------------------------------------------------------------------------

def test_format_throughput_report_empty():
    assert format_throughput_report({}) == "No runs recorded."


def test_format_throughput_report_contains_bucket():
    data = {"2024-03-15T00:00:00+00:00": 5}
    report = format_throughput_report(data, window="day")
    assert "2024-03-15" in report
    assert "5" in report


def test_format_throughput_report_shows_total():
    data = {"2024-03-15T00:00:00+00:00": 3, "2024-03-16T00:00:00+00:00": 7}
    report = format_throughput_report(data)
    assert "Total runs: 10" in report
