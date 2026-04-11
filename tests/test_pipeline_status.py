"""Tests for pipewatch.pipeline_status."""

import pytest
from unittest.mock import patch, MagicMock
from pipewatch.pipeline_status import get_pipeline_status, summarize_recent_runs, format_status_report


FAKE_RUN = {
    "run_id": "run-abc",
    "pipeline": "etl",
    "started_at": "2024-01-01T00:00:00Z",
    "finished_at": "2024-01-01T00:01:00Z",
    "exit_code": 0,
}


def _patch_all(run=FAKE_RUN, metrics=None, rules=None, alert_result=None):
    metrics = metrics or []
    rules = rules or []
    alert_result = alert_result or {"triggered": False}
    return (
        patch("pipewatch.pipeline_status.load_run_record", return_value=run),
        patch("pipewatch.pipeline_status.load_metrics", return_value=metrics),
        patch("pipewatch.pipeline_status.load_alert_rules", return_value=rules),
        patch("pipewatch.pipeline_status.evaluate_alert", return_value=alert_result),
    )


def test_get_pipeline_status_ok():
    with patch("pipewatch.pipeline_status.load_run_record", return_value=FAKE_RUN), \
         patch("pipewatch.pipeline_status.load_metrics", return_value=[]), \
         patch("pipewatch.pipeline_status.load_alert_rules", return_value=[]):
        result = get_pipeline_status("run-abc")
    assert result["status"] == "ok"
    assert result["run_id"] == "run-abc"


def test_get_pipeline_status_failed_exit_code():
    failed_run = {**FAKE_RUN, "exit_code": 1}
    with patch("pipewatch.pipeline_status.load_run_record", return_value=failed_run), \
         patch("pipewatch.pipeline_status.load_metrics", return_value=[]), \
         patch("pipewatch.pipeline_status.load_alert_rules", return_value=[]):
        result = get_pipeline_status("run-abc")
    assert result["status"] == "failed"


def test_get_pipeline_status_alert_triggered():
    rule = {"name": "hash_change", "pipeline": "etl"}
    alert_result = {"triggered": True, "rule_name": "hash_change", "message": "Output changed"}
    with patch("pipewatch.pipeline_status.load_run_record", return_value=FAKE_RUN), \
         patch("pipewatch.pipeline_status.load_metrics", return_value=[]), \
         patch("pipewatch.pipeline_status.load_alert_rules", return_value=[rule]), \
         patch("pipewatch.pipeline_status.evaluate_alert", return_value=alert_result):
        result = get_pipeline_status("run-abc")
    assert result["status"] == "alert"
    assert result["alert_count"] == 1


def test_get_pipeline_status_not_found():
    with patch("pipewatch.pipeline_status.load_run_record", return_value=None):
        with pytest.raises(KeyError, match="run-missing"):
            get_pipeline_status("run-missing")


def test_get_pipeline_status_metric_count():
    metrics = [{"name": "rows"}, {"name": "duration"}]
    with patch("pipewatch.pipeline_status.load_run_record", return_value=FAKE_RUN), \
         patch("pipewatch.pipeline_status.load_metrics", return_value=metrics), \
         patch("pipewatch.pipeline_status.load_alert_rules", return_value=[]):
        result = get_pipeline_status("run-abc")
    assert result["metric_count"] == 2


def test_summarize_recent_runs():
    with patch("pipewatch.pipeline_status.list_run_records", return_value=["run-1", "run-2"]), \
         patch("pipewatch.pipeline_status.load_run_record", return_value=FAKE_RUN), \
         patch("pipewatch.pipeline_status.load_metrics", return_value=[]), \
         patch("pipewatch.pipeline_status.load_alert_rules", return_value=[]):
        summaries = summarize_recent_runs(n=2)
    assert len(summaries) == 2
    assert all(s["status"] == "ok" for s in summaries)


def test_summarize_recent_runs_handles_error():
    with patch("pipewatch.pipeline_status.list_run_records", return_value=["run-bad"]), \
         patch("pipewatch.pipeline_status.load_run_record", side_effect=Exception("disk error")):
        summaries = summarize_recent_runs(n=1)
    assert summaries[0]["status"] == "error"


def test_format_status_report_contains_run_id():
    status = {
        "run_id": "run-abc",
        "status": "ok",
        "metric_count": 3,
        "alert_count": 0,
        "run": FAKE_RUN,
        "triggered_alerts": [],
    }
    report = format_status_report(status)
    assert "run-abc" in report
    assert "OK" in report
    assert "3" in report


def test_format_status_report_shows_alerts():
    status = {
        "run_id": "run-abc",
        "status": "alert",
        "metric_count": 0,
        "alert_count": 1,
        "run": FAKE_RUN,
        "triggered_alerts": [{"rule_name": "hash_change", "message": "Output changed"}],
    }
    report = format_status_report(status)
    assert "[ALERT]" in report
    assert "hash_change" in report
