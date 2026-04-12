"""Tests for pipewatch.run_flakiness."""

import pytest
from unittest.mock import patch
from pipewatch.run_flakiness import (
    count_flips,
    flakiness_score,
    classify_flakiness,
    analyze_pipeline_flakiness,
    format_flakiness_report,
    get_run_statuses_for_pipeline,
)


_FAKE_RECORDS = [
    {"run_id": "r1", "pipeline": "etl", "status": "ok",     "started": "2024-01-01T00:00:00"},
    {"run_id": "r2", "pipeline": "etl", "status": "failed", "started": "2024-01-02T00:00:00"},
    {"run_id": "r3", "pipeline": "etl", "status": "ok",     "started": "2024-01-03T00:00:00"},
    {"run_id": "r4", "pipeline": "etl", "status": "failed", "started": "2024-01-04T00:00:00"},
    {"run_id": "r5", "pipeline": "other", "status": "ok",   "started": "2024-01-05T00:00:00"},
]


def _patch(records=_FAKE_RECORDS):
    return patch("pipewatch.run_flakiness.list_run_records", return_value=records)


# --- count_flips ---

def test_count_flips_no_flips():
    assert count_flips(["ok", "ok", "ok"]) == 0


def test_count_flips_all_flips():
    assert count_flips(["ok", "failed", "ok", "failed"]) == 3


def test_count_flips_single_element():
    assert count_flips(["ok"]) == 0


def test_count_flips_empty():
    assert count_flips([]) == 0


# --- flakiness_score ---

def test_flakiness_score_stable():
    assert flakiness_score(["ok", "ok", "ok", "ok"]) == 0.0


def test_flakiness_score_max():
    assert flakiness_score(["ok", "failed", "ok", "failed"]) == 1.0


def test_flakiness_score_partial():
    score = flakiness_score(["ok", "ok", "failed", "ok"])
    assert 0.0 < score < 1.0


def test_flakiness_score_single():
    assert flakiness_score(["ok"]) == 0.0


# --- classify_flakiness ---

def test_classify_flakiness_stable():
    assert classify_flakiness(0.0) == "stable"
    assert classify_flakiness(0.19) == "stable"


def test_classify_flakiness_flaky():
    assert classify_flakiness(0.2) == "flaky"
    assert classify_flakiness(0.5) == "flaky"


def test_classify_flakiness_highly_flaky():
    assert classify_flakiness(0.6) == "highly_flaky"
    assert classify_flakiness(1.0) == "highly_flaky"


# --- get_run_statuses_for_pipeline ---

def test_get_run_statuses_filters_by_pipeline():
    with _patch():
        result = get_run_statuses_for_pipeline("etl")
    pipelines = [p for _, p in result]
    assert all(s in ("ok", "failed") for _, s in result)
    assert len(result) == 4


def test_get_run_statuses_excludes_other_pipeline():
    with _patch():
        result = get_run_statuses_for_pipeline("other")
    assert len(result) == 1


# --- analyze_pipeline_flakiness ---

def test_analyze_pipeline_flakiness_returns_required_keys():
    with _patch():
        result = analyze_pipeline_flakiness("etl")
    for key in ("pipeline", "run_count", "flip_count", "flakiness_score", "classification", "recent_statuses"):
        assert key in result


def test_analyze_pipeline_flakiness_correct_values():
    with _patch():
        result = analyze_pipeline_flakiness("etl")
    assert result["run_count"] == 4
    assert result["flip_count"] == 3
    assert result["classification"] == "highly_flaky"


# --- format_flakiness_report ---

def test_format_flakiness_report_contains_pipeline():
    with _patch():
        analysis = analyze_pipeline_flakiness("etl")
    report = format_flakiness_report(analysis)
    assert "etl" in report
    assert "HIGHLY_FLAKY" in report
    assert "->" in report
