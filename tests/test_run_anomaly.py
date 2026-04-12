"""Tests for pipewatch.run_anomaly."""

import pytest
from unittest.mock import patch
from pipewatch.run_anomaly import (
    _mean,
    _stddev,
    z_score,
    detect_anomaly,
    analyze_run_anomalies,
    format_anomaly_report,
    get_metric_history,
)


def test_mean_basic():
    assert _mean([1.0, 2.0, 3.0]) == pytest.approx(2.0)


def test_stddev_basic():
    values = [2.0, 4.0, 4.0, 4.0, 5.0, 5.0, 7.0, 9.0]
    mu = _mean(values)
    assert _stddev(values, mu) == pytest.approx(2.0)


def test_z_score_too_short_returns_none():
    assert z_score(10.0, [1.0, 2.0]) is None


def test_z_score_constant_series_returns_none():
    assert z_score(5.0, [5.0, 5.0, 5.0]) is None


def test_z_score_normal_value():
    history = [10.0, 10.0, 10.0, 10.0, 10.2, 9.8, 10.1]
    score = z_score(10.0, history)
    assert score is not None
    assert abs(score) < 1.0


def test_z_score_outlier():
    history = [1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0]
    score = z_score(100.0, history)
    assert score is not None
    assert score > 2.5


def test_detect_anomaly_insufficient_history():
    result = detect_anomaly(5.0, [1.0, 2.0])
    assert result["anomaly"] is False
    assert result["z_score"] is None
    assert result["reason"] == "insufficient_history"


def test_detect_anomaly_within_bounds():
    history = [10.0] * 10
    result = detect_anomaly(10.0, history)
    assert result["anomaly"] is False
    assert result["reason"] == "within_bounds" or result["reason"] == "insufficient_history"


def test_detect_anomaly_flagged():
    history = [1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0]
    result = detect_anomaly(100.0, history, threshold=2.5)
    assert result["anomaly"] is True
    assert result["reason"] == "z_score_exceeded"
    assert result["z_score"] is not None


def test_detect_anomaly_custom_threshold():
    history = [10.0, 10.0, 10.0, 10.0, 10.0, 10.0, 10.0, 10.0, 10.0, 12.0]
    result = detect_anomaly(10.0, history, threshold=0.0)
    assert result["anomaly"] is True


@patch("pipewatch.run_anomaly.list_runs", return_value=["r1", "r2"])
@patch("pipewatch.run_anomaly.load_metrics")
def test_get_metric_history(mock_load, mock_list):
    mock_load.side_effect = [
        [{"pipeline": "etl", "name": "rows", "value": 100.0}],
        [{"pipeline": "etl", "name": "rows", "value": 200.0}],
    ]
    history = get_metric_history("etl", "rows")
    assert history == [100.0, 200.0]


@patch("pipewatch.run_anomaly.list_runs", return_value=["r1"])
@patch("pipewatch.run_anomaly.load_metrics")
def test_analyze_run_anomalies_returns_list(mock_load, mock_list):
    mock_load.return_value = [
        {"pipeline": "etl", "name": "rows", "value": 50.0}
    ]
    with patch("pipewatch.run_anomaly.get_metric_history", return_value=[50.0] * 5):
        results = analyze_run_anomalies("r1", "etl")
    assert isinstance(results, list)
    assert results[0]["metric"] == "rows"
    assert "anomaly" in results[0]


def test_format_anomaly_report_empty():
    out = format_anomaly_report([])
    assert "No metrics" in out


def test_format_anomaly_report_with_results():
    results = [
        {"metric": "rows", "value": 100.0, "anomaly": False, "z_score": 0.1, "reason": "within_bounds"},
        {"metric": "errors", "value": 999.0, "anomaly": True, "z_score": 3.5, "reason": "z_score_exceeded"},
    ]
    out = format_anomaly_report(results)
    assert "ANOMALY" in out
    assert "errors" in out
    assert "rows" in out
    assert "1/2" in out
