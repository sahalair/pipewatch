"""Tests for pipewatch.run_correlation."""

import pytest
from unittest.mock import patch

from pipewatch.run_correlation import (
    pearson_correlation,
    correlate_metrics,
    _classify_correlation,
    format_correlation_report,
    get_metric_series,
)

_RUNS = ["r1", "r2", "r3", "r4"]

_METRICS = {
    "r1": [{"pipeline": "etl", "name": "rows", "value": 10}, {"pipeline": "etl", "name": "errors", "value": 1}],
    "r2": [{"pipeline": "etl", "name": "rows", "value": 20}, {"pipeline": "etl", "name": "errors", "value": 2}],
    "r3": [{"pipeline": "etl", "name": "rows", "value": 30}, {"pipeline": "etl", "name": "errors", "value": 3}],
    "r4": [{"pipeline": "etl", "name": "rows", "value": 40}, {"pipeline": "etl", "name": "errors", "value": 4}],
}


def _patch_all():
    return [
        patch("pipewatch.run_correlation.list_runs", return_value=_RUNS),
        patch("pipewatch.run_correlation.load_metrics", side_effect=lambda run_id, base_dir=".": _METRICS.get(run_id, [])),
    ]


def test_pearson_correlation_perfect_positive():
    r = pearson_correlation([1, 2, 3, 4], [2, 4, 6, 8])
    assert r is not None
    assert abs(r - 1.0) < 1e-9


def test_pearson_correlation_perfect_negative():
    r = pearson_correlation([1, 2, 3, 4], [4, 3, 2, 1])
    assert r is not None
    assert abs(r + 1.0) < 1e-9


def test_pearson_correlation_too_short_returns_none():
    assert pearson_correlation([1], [1]) is None
    assert pearson_correlation([], []) is None


def test_pearson_correlation_constant_series_returns_none():
    assert pearson_correlation([5, 5, 5], [1, 2, 3]) is None


def test_classify_correlation_strong():
    assert _classify_correlation(0.9) == "strong"
    assert _classify_correlation(-0.85) == "strong"


def test_classify_correlation_moderate():
    assert _classify_correlation(0.6) == "moderate"


def test_classify_correlation_weak():
    assert _classify_correlation(0.3) == "weak"


def test_classify_correlation_negligible():
    assert _classify_correlation(0.05) == "negligible"


def test_classify_correlation_none_returns_undefined():
    assert _classify_correlation(None) == "undefined"


def test_get_metric_series_returns_values():
    patches = _patch_all()
    with patches[0], patches[1]:
        series = get_metric_series("etl", "rows")
    values = [e["value"] for e in series]
    assert sorted(values) == [10, 20, 30, 40]


def test_correlate_metrics_strong_positive():
    patches = _patch_all()
    with patches[0], patches[1]:
        result = correlate_metrics("etl", "rows", "errors")
    assert result["n"] == 4
    assert result["correlation"] == 1.0
    assert result["strength"] == "strong"


def test_correlate_metrics_no_common_runs():
    with patch("pipewatch.run_correlation.list_runs", return_value=["r1"]), \
         patch("pipewatch.run_correlation.load_metrics", return_value=[]):
        result = correlate_metrics("etl", "rows", "errors")
    assert result["n"] == 0
    assert result["correlation"] is None
    assert "insufficient" in result["strength"]


def test_format_correlation_report_contains_fields():
    result = {"pipeline": "etl", "metric_a": "rows", "metric_b": "errors",
              "n": 4, "correlation": 1.0, "strength": "strong"}
    report = format_correlation_report(result)
    assert "etl" in report
    assert "rows" in report
    assert "errors" in report
    assert "strong" in report
    assert "1.0" in report
