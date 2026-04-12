"""Tests for pipewatch.run_heatmap."""

import pytest
from unittest.mock import patch

from pipewatch.run_heatmap import build_heatmap, format_heatmap, _day_bucket, _hour_bucket


FAKE_RUNS = [
    {"pipeline": "etl", "started_at": "2024-03-01T10:00:00", "status": "ok"},
    {"pipeline": "etl", "started_at": "2024-03-01T14:00:00", "status": "failed"},
    {"pipeline": "etl", "started_at": "2024-03-02T09:00:00", "status": "ok"},
    {"pipeline": "other", "started_at": "2024-03-01T11:00:00", "status": "ok"},
]


def _patch(runs=None):
    return patch(
        "pipewatch.run_heatmap.list_run_records",
        return_value=runs if runs is not None else FAKE_RUNS,
    )


def test_day_bucket_truncates_correctly():
    assert _day_bucket("2024-03-01T10:00:00") == "2024-03-01"


def test_hour_bucket_truncates_correctly():
    assert _hour_bucket("2024-03-01T10:00:00") == "2024-03-01T10"


def test_build_heatmap_day_all_pipelines():
    with _patch():
        hm = build_heatmap(bucket="day")
    assert "2024-03-01" in hm
    assert "2024-03-02" in hm
    assert hm["2024-03-01"]["total"] == 3
    assert hm["2024-03-01"]["failed"] == 1
    assert hm["2024-03-02"]["total"] == 1
    assert hm["2024-03-02"]["failed"] == 0


def test_build_heatmap_filters_by_pipeline():
    with _patch():
        hm = build_heatmap(pipeline="etl", bucket="day")
    assert "2024-03-01" in hm
    assert hm["2024-03-01"]["total"] == 2
    # "other" pipeline should not appear
    for vals in hm.values():
        assert vals["total"] <= 2


def test_build_heatmap_hour_bucket():
    with _patch():
        hm = build_heatmap(pipeline="etl", bucket="hour")
    assert "2024-03-01T10" in hm
    assert "2024-03-01T14" in hm
    assert hm["2024-03-01T14"]["failed"] == 1


def test_build_heatmap_invalid_bucket_raises():
    with _patch():
        with pytest.raises(ValueError, match="bucket must be"):
            build_heatmap(bucket="week")


def test_build_heatmap_empty_runs():
    with _patch(runs=[]):
        hm = build_heatmap()
    assert hm == {}


def test_build_heatmap_sorted_keys():
    with _patch():
        hm = build_heatmap(bucket="day")
    keys = list(hm.keys())
    assert keys == sorted(keys)


def test_format_heatmap_contains_bucket_keys():
    with _patch():
        hm = build_heatmap(bucket="day")
    output = format_heatmap(hm)
    assert "2024-03-01" in output
    assert "2024-03-02" in output


def test_format_heatmap_shows_totals():
    with _patch():
        hm = build_heatmap(pipeline="etl", bucket="day")
    output = format_heatmap(hm)
    assert "2" in output  # total for 2024-03-01
    assert "1" in output  # failed count or total for 2024-03-02


def test_format_heatmap_empty_returns_no_data():
    output = format_heatmap({})
    assert "No data" in output


def test_format_heatmap_bar_present():
    hm = {"2024-03-01": {"total": 5, "failed": 1}}
    output = format_heatmap(hm, bar_width=10)
    assert "[" in output and "]" in output
