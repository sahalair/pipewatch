"""Tests for pipewatch.run_trend."""

import pytest
from unittest.mock import patch
from pipewatch.run_trend import get_metric_trend, compute_trend_direction, format_trend_report


FAKE_RECORDS = [
    {"run_id": "aaa", "pipeline": "etl", "finished": "2024-01-01T10:00:00"},
    {"run_id": "bbb", "pipeline": "etl", "finished": "2024-01-02T10:00:00"},
    {"run_id": "ccc", "pipeline": "etl", "finished": "2024-01-03T10:00:00"},
    {"run_id": "ddd", "pipeline": "other", "finished": "2024-01-01T08:00:00"},
]

FAKE_METRICS = {
    "aaa": {"rows": {"value": 100, "unit": "rows"}},
    "bbb": {"rows": {"value": 150, "unit": "rows"}},
    "ccc": {"rows": {"value": 200, "unit": "rows"}},
    "ddd": {"rows": {"value": 50, "unit": "rows"}},
}


def _patch_all():
    return [
        patch("pipewatch.run_trend.list_run_records", return_value=FAKE_RECORDS),
        patch("pipewatch.run_trend.load_metrics", side_effect=lambda rid: FAKE_METRICS.get(rid, {})),
    ]


def test_get_metric_trend_filters_by_pipeline():
    with _patch_all()[0], _patch_all()[1]:
        result = get_metric_trend("etl", "rows")
    assert all(r["run_id"] in ("aaa", "bbb", "ccc") for r in result)
    assert len(result) == 3


def test_get_metric_trend_sorted_oldest_first():
    with _patch_all()[0], _patch_all()[1]:
        result = get_metric_trend("etl", "rows")
    assert result[0]["run_id"] == "aaa"
    assert result[-1]["run_id"] == "ccc"


def test_get_metric_trend_respects_limit():
    with _patch_all()[0], _patch_all()[1]:
        result = get_metric_trend("etl", "rows", limit=2)
    assert len(result) == 2
    assert result[-1]["run_id"] == "ccc"


def test_get_metric_trend_missing_metric_excluded():
    with _patch_all()[0], _patch_all()[1]:
        result = get_metric_trend("etl", "nonexistent")
    assert result == []


def test_compute_trend_direction_up():
    trend = [{"value": 100}, {"value": 200}]
    assert compute_trend_direction(trend) == "up"


def test_compute_trend_direction_down():
    trend = [{"value": 200}, {"value": 100}]
    assert compute_trend_direction(trend) == "down"


def test_compute_trend_direction_stable():
    trend = [{"value": 100}, {"value": 101}]
    assert compute_trend_direction(trend) == "stable"


def test_compute_trend_direction_insufficient_data():
    assert compute_trend_direction([]) == "insufficient_data"
    assert compute_trend_direction([{"value": 5}]) == "insufficient_data"


def test_format_trend_report_contains_pipeline_and_metric():
    with _patch_all()[0], _patch_all()[1]:
        trend = get_metric_trend("etl", "rows")
    report = format_trend_report("etl", "rows", trend)
    assert "etl" in report
    assert "rows" in report


def test_format_trend_report_empty_trend():
    report = format_trend_report("etl", "rows", [])
    assert "No data" in report


def test_format_trend_report_shows_direction():
    trend = [
        {"run_id": "x" * 36, "finished": "2024-01-01T00:00:00", "value": 10, "unit": ""},
        {"run_id": "y" * 36, "finished": "2024-01-02T00:00:00", "value": 50, "unit": ""},
    ]
    report = format_trend_report("etl", "rows", trend)
    assert "up" in report or "↑" in report
