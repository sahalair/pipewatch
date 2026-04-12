"""Tests for pipewatch.run_coverage."""

from __future__ import annotations

import pytest

from pipewatch import run_coverage as rc


@pytest.fixture(autouse=True)
def _tmp_coverage(tmp_path, monkeypatch):
    monkeypatch.setattr(rc, "PIPEWATCH_DIR", tmp_path)


def test_load_coverage_missing_file_returns_empty():
    assert rc.load_coverage() == {}


def test_set_expected_pipeline_creates_entry():
    entry = rc.set_expected_pipeline("etl", min_runs=2)
    assert entry["pipeline"] == "etl"
    assert entry["min_runs"] == 2


def test_set_expected_pipeline_persists():
    rc.set_expected_pipeline("etl", min_runs=3)
    data = rc.load_coverage()
    assert "etl" in data
    assert data["etl"]["min_runs"] == 3


def test_set_expected_pipeline_overwrites_existing():
    rc.set_expected_pipeline("etl", min_runs=1)
    rc.set_expected_pipeline("etl", min_runs=5)
    data = rc.load_coverage()
    assert data["etl"]["min_runs"] == 5


def test_set_expected_pipeline_empty_name_raises():
    with pytest.raises(ValueError, match="empty"):
        rc.set_expected_pipeline("", min_runs=1)


def test_set_expected_pipeline_zero_min_runs_raises():
    with pytest.raises(ValueError, match="min_runs"):
        rc.set_expected_pipeline("etl", min_runs=0)


def test_remove_expected_pipeline_existing_returns_true():
    rc.set_expected_pipeline("etl")
    assert rc.remove_expected_pipeline("etl") is True
    assert "etl" not in rc.load_coverage()


def test_remove_expected_pipeline_nonexistent_returns_false():
    assert rc.remove_expected_pipeline("ghost") is False


def _runs(pipelines: list[str]) -> list[dict]:
    return [{"pipeline": p, "run_id": f"r-{i}"} for i, p in enumerate(pipelines)]


def test_compute_coverage_all_covered():
    rc.set_expected_pipeline("etl", min_runs=1)
    report = rc.compute_coverage(_runs(["etl", "etl"]))
    assert report["covered"] == 1
    assert report["missing"] == 0
    assert report["coverage_pct"] == 100.0


def test_compute_coverage_missing_pipeline():
    rc.set_expected_pipeline("etl", min_runs=1)
    rc.set_expected_pipeline("ingest", min_runs=1)
    report = rc.compute_coverage(_runs(["etl"]))
    assert report["covered"] == 1
    assert report["missing"] == 1
    assert report["coverage_pct"] == 50.0


def test_compute_coverage_min_runs_not_met():
    rc.set_expected_pipeline("etl", min_runs=3)
    report = rc.compute_coverage(_runs(["etl", "etl"]))
    detail = report["details"][0]
    assert detail["covered"] is False
    assert detail["actual_runs"] == 2


def test_compute_coverage_no_expected_returns_100():
    report = rc.compute_coverage(_runs(["etl"]))
    assert report["coverage_pct"] == 100.0
    assert report["total_expected"] == 0


def test_format_coverage_report_contains_pipeline_name():
    rc.set_expected_pipeline("etl", min_runs=1)
    report = rc.compute_coverage(_runs(["etl"]))
    text = rc.format_coverage_report(report)
    assert "etl" in text
    assert "OK" in text


def test_format_coverage_report_shows_miss():
    rc.set_expected_pipeline("ingest", min_runs=2)
    report = rc.compute_coverage(_runs(["ingest"]))
    text = rc.format_coverage_report(report)
    assert "MISS" in text
