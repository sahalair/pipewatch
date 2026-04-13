"""Tests for pipewatch.run_rollup."""

from unittest.mock import patch
from pipewatch.run_rollup import rollup_by_day, format_rollup_report


def _make_run(pipeline, started, finished=None, status="success"):
    return {
        "run_id": f"run-{started}",
        "pipeline": pipeline,
        "started": started,
        "finished": finished,
        "status": status,
    }


_RUNS = [
    _make_run("etl", "2024-06-01T10:00:00", "2024-06-01T10:05:00", "success"),
    _make_run("etl", "2024-06-01T12:00:00", "2024-06-01T12:02:00", "failed"),
    _make_run("etl", "2024-06-02T09:00:00", "2024-06-02T09:10:00", "success"),
    _make_run("other", "2024-06-01T08:00:00", "2024-06-01T08:01:00", "success"),
]


def _patch(runs=None):
    return patch("pipewatch.run_rollup.list_run_records", return_value=runs or _RUNS)


def test_rollup_by_day_returns_list():
    with _patch():
        result = rollup_by_day(pipeline="etl")
    assert isinstance(result, list)


def test_rollup_by_day_filters_by_pipeline():
    with _patch():
        result = rollup_by_day(pipeline="etl")
    days = [r["day"] for r in result]
    assert all(d in ("2024-06-01", "2024-06-02") for d in days)
    assert len(result) == 2


def test_rollup_by_day_all_pipelines():
    with _patch():
        result = rollup_by_day()
    totals = {r["day"]: r["total"] for r in result}
    assert totals["2024-06-01"] == 3  # etl x2 + other x1


def test_rollup_by_day_success_and_failed_counts():
    with _patch():
        result = rollup_by_day(pipeline="etl")
    by_day = {r["day"]: r for r in result}
    assert by_day["2024-06-01"]["success"] == 1
    assert by_day["2024-06-01"]["failed"] == 1


def test_rollup_by_day_success_rate():
    with _patch():
        result = rollup_by_day(pipeline="etl")
    by_day = {r["day"]: r for r in result}
    assert by_day["2024-06-01"]["success_rate"] == 0.5
    assert by_day["2024-06-02"]["success_rate"] == 1.0


def test_rollup_by_day_avg_duration():
    with _patch():
        result = rollup_by_day(pipeline="etl")
    by_day = {r["day"]: r for r in result}
    # 2024-06-01: 300s and 120s -> avg 210s
    assert by_day["2024-06-01"]["avg_duration_seconds"] == 210.0
    # 2024-06-02: 600s
    assert by_day["2024-06-02"]["avg_duration_seconds"] == 600.0


def test_rollup_by_day_no_finished_gives_none_avg():
    runs = [_make_run("etl", "2024-07-01T10:00:00", finished=None, status="success")]
    with _patch(runs):
        result = rollup_by_day(pipeline="etl")
    assert result[0]["avg_duration_seconds"] is None


def test_rollup_by_day_respects_limit():
    runs = [_make_run("etl", f"2024-01-{d:02d}T10:00:00", f"2024-01-{d:02d}T10:01:00") for d in range(1, 32)]
    with _patch(runs):
        result = rollup_by_day(pipeline="etl", limit=5)
    assert len(result) == 5


def test_rollup_by_day_empty_returns_empty():
    with _patch([]):
        result = rollup_by_day(pipeline="etl")
    assert result == []


def test_format_rollup_report_empty():
    out = format_rollup_report([])
    assert "No rollup" in out


def test_format_rollup_report_contains_day():
    with _patch():
        rollup = rollup_by_day(pipeline="etl")
    out = format_rollup_report(rollup)
    assert "2024-06-01" in out
    assert "2024-06-02" in out
