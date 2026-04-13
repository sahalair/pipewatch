"""Tests for pipewatch.run_forecast."""

import pytest
from unittest.mock import patch
from pipewatch import run_forecast


DEFAULT_THROUGHPUT = {"pipeline": "etl", "window": "day", "rate": 3.0}
DEFAULT_DURATIONS = [60.0, 90.0, 75.0, 80.0, 70.0]
DEFAULT_SUMMARY = {"mean": 75.0, "min": 60.0, "max": 90.0, "count": 5}


def _patch(throughput=None, durations=None, summary=None):
    tp = throughput or DEFAULT_THROUGHPUT
    dur = durations if durations is not None else DEFAULT_DURATIONS
    summ = summary or DEFAULT_SUMMARY
    p1 = patch("pipewatch.run_forecast.compute_throughput", return_value=tp)
    p2 = patch("pipewatch.run_forecast.get_durations_for_pipeline", return_value=dur)
    p3 = patch("pipewatch.run_forecast.summarize_durations", return_value=summ)
    return p1, p2, p3


def test_forecast_run_count_returns_required_keys():
    p1, p2, p3 = _patch()
    with p1, p2, p3:
        result = run_forecast.forecast_run_count("etl")
    assert "pipeline" in result
    assert "window" in result
    assert "lookahead" in result
    assert "rate_per_window" in result
    assert "forecast_total" in result


def test_forecast_run_count_calculates_total():
    p1, p2, p3 = _patch(throughput={"rate": 4.0})
    with p1, p2, p3:
        result = run_forecast.forecast_run_count("etl", lookahead=5)
    assert result["forecast_total"] == 20.0
    assert result["lookahead"] == 5


def test_forecast_run_count_zero_rate():
    p1, p2, p3 = _patch(throughput={"rate": 0.0})
    with p1, p2, p3:
        result = run_forecast.forecast_run_count("etl", lookahead=10)
    assert result["forecast_total"] == 0.0


def test_forecast_duration_returns_required_keys():
    p1, p2, p3 = _patch()
    with p1, p2, p3:
        result = run_forecast.forecast_duration("etl")
    assert "pipeline" in result
    assert "lookahead" in result
    assert "avg_duration_seconds" in result
    assert "forecast_total_seconds" in result


def test_forecast_duration_calculates_total():
    p1, p2, p3 = _patch(summary={"mean": 100.0})
    with p1, p2, p3:
        result = run_forecast.forecast_duration("etl", lookahead=3)
    assert result["avg_duration_seconds"] == 100.0
    assert result["forecast_total_seconds"] == 300.0


def test_forecast_duration_no_data_returns_message():
    p1, p2, p3 = _patch(durations=[], summary={"mean": None})
    with p1, p2, p3:
        result = run_forecast.forecast_duration("etl")
    assert result["avg_duration_seconds"] is None
    assert result["forecast_total_seconds"] is None
    assert "message" in result


def test_format_forecast_report_contains_pipeline():
    count_fc = {"pipeline": "etl", "window": "day", "lookahead": 7, "rate_per_window": 3.0, "forecast_total": 21.0}
    dur_fc = {"pipeline": "etl", "lookahead": 7, "avg_duration_seconds": 75.0, "forecast_total_seconds": 525.0}
    report = run_forecast.format_forecast_report(count_fc, dur_fc)
    assert "etl" in report
    assert "21.0" in report
    assert "75.0" in report


def test_format_forecast_report_shows_note_when_no_data():
    count_fc = {"pipeline": "etl", "window": "day", "lookahead": 3, "rate_per_window": 0.0, "forecast_total": 0.0}
    dur_fc = {"pipeline": "etl", "lookahead": 3, "avg_duration_seconds": None, "forecast_total_seconds": None, "message": "Insufficient data for forecast."}
    report = run_forecast.format_forecast_report(count_fc, dur_fc)
    assert "Insufficient" in report
