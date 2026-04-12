"""Tests for pipewatch.run_concurrency."""

from unittest.mock import patch
from datetime import datetime, timezone, timedelta

from pipewatch.run_concurrency import (
    get_active_runs_at,
    compute_peak_concurrency,
    concurrency_timeline,
    format_concurrency_report,
)

NOW = datetime(2024, 6, 1, 12, 0, 0, tzinfo=timezone.utc)


def _iso(dt: datetime) -> str:
    return dt.isoformat()


def _make_run(run_id, pipeline, start_offset_min, duration_min):
    start = NOW + timedelta(minutes=start_offset_min)
    end = start + timedelta(minutes=duration_min)
    return {
        "run_id": run_id,
        "pipeline": pipeline,
        "started": _iso(start),
        "finished": _iso(end),
    }


FAKE_RUNS = [
    _make_run("r1", "etl", 0, 10),
    _make_run("r2", "etl", 5, 10),
    _make_run("r3", "etl", 20, 5),
    _make_run("r4", "ml", 3, 4),
]


def test_get_active_runs_at_finds_overlapping():
    ts = (NOW + timedelta(minutes=6)).timestamp()
    active = get_active_runs_at(ts, runs=FAKE_RUNS)
    ids = {r["run_id"] for r in active}
    assert "r1" in ids
    assert "r2" in ids
    assert "r4" in ids


def test_get_active_runs_at_excludes_finished():
    ts = (NOW + timedelta(minutes=25)).timestamp()
    active = get_active_runs_at(ts, runs=FAKE_RUNS)
    ids = {r["run_id"] for r in active}
    assert "r1" not in ids
    assert "r2" not in ids
    assert "r3" in ids


def test_get_active_runs_at_empty_when_none_overlap():
    ts = (NOW + timedelta(minutes=50)).timestamp()
    active = get_active_runs_at(ts, runs=FAKE_RUNS)
    assert active == []


def test_compute_peak_concurrency_all_pipelines():
    peak = compute_peak_concurrency(runs=FAKE_RUNS)
    assert peak >= 2


def test_compute_peak_concurrency_single_pipeline():
    peak = compute_peak_concurrency(pipeline="etl", runs=FAKE_RUNS)
    assert peak == 2


def test_compute_peak_concurrency_empty_returns_zero():
    assert compute_peak_concurrency(runs=[]) == 0


def test_compute_peak_concurrency_no_overlap():
    runs = [
        _make_run("a", "p", 0, 5),
        _make_run("b", "p", 10, 5),
    ]
    assert compute_peak_concurrency(runs=runs) == 1


def test_concurrency_timeline_sorted():
    timeline = concurrency_timeline(runs=FAKE_RUNS)
    timestamps = [e["timestamp"] for e in timeline]
    assert timestamps == sorted(timestamps)


def test_concurrency_timeline_concurrency_never_negative():
    timeline = concurrency_timeline(runs=FAKE_RUNS)
    for entry in timeline:
        assert entry["concurrency"] >= 0


def test_concurrency_timeline_filters_by_pipeline():
    timeline = concurrency_timeline(pipeline="ml", runs=FAKE_RUNS)
    run_ids = {e["run_id"] for e in timeline}
    assert run_ids == {"r4"}


def test_format_concurrency_report_contains_peak():
    report = format_concurrency_report(pipeline="etl", runs=FAKE_RUNS)
    assert "Peak concurrent runs" in report
    assert "etl" in report


def test_format_concurrency_report_no_runs():
    report = format_concurrency_report(pipeline="nope", runs=[])
    assert "0" in report
