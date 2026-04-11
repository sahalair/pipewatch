"""Tests for pipewatch.run_compare."""

from __future__ import annotations

import pytest
from unittest.mock import patch, MagicMock

from pipewatch.run_compare import compare_runs, format_run_comparison


RUN_A = "run-aaa"
RUN_B = "run-bbb"

_REC_A = {"run_id": RUN_A, "pipeline": "etl", "status": "success"}
_REC_B = {"run_id": RUN_B, "pipeline": "etl", "status": "failed"}

_METRICS_A = {"rows": {"value": 100, "unit": "count"}, "duration": {"value": 5.0}}
_METRICS_B = {"rows": {"value": 120, "unit": "count"}, "duration": {"value": 5.0}}

_TAGS = {RUN_A: ["prod", "v1"], RUN_B: ["prod", "v2"]}
_ANNOTATIONS = {RUN_A: {"env": "prod"}, RUN_B: {"env": "staging"}}


def _patch_all(monkeypatch):
    monkeypatch.setattr("pipewatch.run_compare.load_run_record",
                        lambda rid, data_dir=".": _REC_A if rid == RUN_A else _REC_B)
    monkeypatch.setattr("pipewatch.run_compare.load_metrics",
                        lambda rid, data_dir=".": _METRICS_A if rid == RUN_A else _METRICS_B)
    monkeypatch.setattr("pipewatch.run_compare.load_tags",
                        lambda data_dir=".": _TAGS)
    monkeypatch.setattr("pipewatch.run_compare.load_annotations",
                        lambda data_dir=".": _ANNOTATIONS)


def test_compare_runs_returns_required_keys(monkeypatch):
    _patch_all(monkeypatch)
    result = compare_runs(RUN_A, RUN_B)
    for key in ("run_a", "run_b", "status", "pipeline", "metrics", "tags", "annotations"):
        assert key in result


def test_compare_runs_status_changed(monkeypatch):
    _patch_all(monkeypatch)
    result = compare_runs(RUN_A, RUN_B)
    assert result["status"]["changed"] is True
    assert result["status"]["run_a"] == "success"
    assert result["status"]["run_b"] == "failed"


def test_compare_runs_metric_changed(monkeypatch):
    _patch_all(monkeypatch)
    result = compare_runs(RUN_A, RUN_B)
    assert result["metrics"]["rows"]["changed"] is True
    assert result["metrics"]["rows"]["run_a"] == 100
    assert result["metrics"]["rows"]["run_b"] == 120


def test_compare_runs_metric_unchanged(monkeypatch):
    _patch_all(monkeypatch)
    result = compare_runs(RUN_A, RUN_B)
    assert result["metrics"]["duration"]["changed"] is False


def test_compare_runs_tags(monkeypatch):
    _patch_all(monkeypatch)
    result = compare_runs(RUN_A, RUN_B)
    assert result["tags"]["only_in_a"] == ["v1"]
    assert result["tags"]["only_in_b"] == ["v2"]
    assert result["tags"]["common"] == ["prod"]


def test_compare_runs_raises_when_not_found(monkeypatch):
    monkeypatch.setattr(
        "pipewatch.run_compare.load_run_record",
        lambda rid, data_dir=".": (_ for _ in ()).throw(FileNotFoundError("not found")),
    )
    monkeypatch.setattr("pipewatch.run_compare.load_metrics", lambda *a, **kw: {})
    monkeypatch.setattr("pipewatch.run_compare.load_tags", lambda **kw: {})
    monkeypatch.setattr("pipewatch.run_compare.load_annotations", lambda **kw: {})
    with pytest.raises(FileNotFoundError):
        compare_runs("bad-id", RUN_B)


def test_format_run_comparison_contains_run_ids(monkeypatch):
    _patch_all(monkeypatch)
    result = compare_runs(RUN_A, RUN_B)
    text = format_run_comparison(result)
    assert RUN_A in text
    assert RUN_B in text


def test_format_run_comparison_marks_changed_status(monkeypatch):
    _patch_all(monkeypatch)
    result = compare_runs(RUN_A, RUN_B)
    text = format_run_comparison(result)
    assert "* status" in text


def test_format_run_comparison_marks_changed_metric(monkeypatch):
    _patch_all(monkeypatch)
    result = compare_runs(RUN_A, RUN_B)
    text = format_run_comparison(result)
    assert "* rows" in text
