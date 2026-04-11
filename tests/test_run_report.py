"""Tests for pipewatch.run_report and pipewatch.cli_report."""

from __future__ import annotations

import pytest
from unittest.mock import patch, MagicMock

from pipewatch.run_report import build_run_report, format_run_report


RECORD = {
    "run_id": "run-abc",
    "pipeline": "etl",
    "started_at": "2024-01-01T00:00:00",
    "finished_at": "2024-01-01T00:01:00",
    "exit_code": 0,
}

STATUS = {
    "run_id": "run-abc",
    "ok": True,
    "exit_code": 0,
    "alerts_triggered": [],
    "metric_alerts": [],
}

METRICS = [
    {"name": "rows", "value": 100, "unit": "rows"},
]


def _patch_all(record=RECORD, status=STATUS, metrics=METRICS, runs=None):
    if runs is None:
        runs = [RECORD]
    return [
        patch("pipewatch.run_report.load_run_record", return_value=record),
        patch("pipewatch.run_report.get_pipeline_status", return_value=status),
        patch("pipewatch.run_report.load_metrics", return_value=metrics),
        patch("pipewatch.run_report.list_run_records", return_value=runs),
    ]


def test_build_run_report_returns_required_keys():
    with _patch_all()[0], _patch_all()[1], _patch_all()[2], _patch_all()[3]:
        patches = _patch_all()
        with patches[0], patches[1], patches[2], patches[3]:
            report = build_run_report("run-abc")
    assert "run_id" in report
    assert "record" in report
    assert "status" in report
    assert "metrics" in report


def test_build_run_report_raises_when_not_found():
    with patch("pipewatch.run_report.load_run_record", return_value=None):
        with pytest.raises(FileNotFoundError):
            build_run_report("missing-run")


def test_build_run_report_no_metric_diff_for_first_run():
    patches = _patch_all(runs=[RECORD])
    with patches[0], patches[1], patches[2], patches[3]:
        report = build_run_report("run-abc")
    assert report["metric_diff"] is None


def test_build_run_report_metric_diff_for_second_run():
    prev_record = {**RECORD, "run_id": "run-prev"}
    runs = [prev_record, RECORD]
    prev_metrics = [{"name": "rows", "value": 80, "unit": "rows"}]
    diff = {"rows": {"previous": 80, "current": 100, "delta": 20}}

    with patch("pipewatch.run_report.load_run_record", return_value=RECORD), \
         patch("pipewatch.run_report.get_pipeline_status", return_value=STATUS), \
         patch("pipewatch.run_report.load_metrics", side_effect=[METRICS, prev_metrics]), \
         patch("pipewatch.run_report.list_run_records", return_value=runs), \
         patch("pipewatch.run_report.compare_metrics", return_value=diff):
        report = build_run_report("run-abc")

    assert report["metric_diff"] == diff


def test_format_run_report_contains_run_id():
    report = {
        "run_id": "run-abc",
        "record": RECORD,
        "status": STATUS,
        "metrics": METRICS,
        "metric_diff": None,
    }
    with patch("pipewatch.run_report.format_status_report", return_value="Status OK"):
        output = format_run_report(report)
    assert "run-abc" in output


def test_format_run_report_shows_metrics():
    report = {
        "run_id": "run-abc",
        "record": RECORD,
        "status": STATUS,
        "metrics": METRICS,
        "metric_diff": None,
    }
    with patch("pipewatch.run_report.format_status_report", return_value=""):
        output = format_run_report(report)
    assert "rows" in output
    assert "100" in output


def test_format_run_report_no_metrics_message():
    report = {
        "run_id": "run-abc",
        "record": RECORD,
        "status": STATUS,
        "metrics": [],
        "metric_diff": None,
    }
    with patch("pipewatch.run_report.format_status_report", return_value=""):
        output = format_run_report(report)
    assert "none recorded" in output
