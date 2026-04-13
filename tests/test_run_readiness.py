"""Tests for pipewatch.run_readiness."""

from __future__ import annotations

from unittest.mock import patch

import pytest

from pipewatch.run_readiness import evaluate_readiness, format_readiness_report

_NOW = "2024-06-01T12:00:00+00:00"


def _patch_all(health=None, quotas=None, blackouts=None):
    health = health or {"status": "ok", "reason": ""}
    quotas = quotas or {}
    blackouts = blackouts or []
    return [
        patch("pipewatch.run_readiness.evaluate_health", return_value=health),
        patch("pipewatch.run_readiness.load_quotas", return_value=quotas),
        patch("pipewatch.run_readiness.load_blackout_windows", return_value=blackouts),
    ]


def test_evaluate_readiness_returns_required_keys():
    patches = _patch_all()
    with patches[0], patches[1], patches[2]:
        result = evaluate_readiness("my_pipeline", now_iso=_NOW)
    assert "pipeline" in result
    assert "ready" in result
    assert "issues" in result
    assert "checked_at" in result


def test_evaluate_readiness_all_clear_is_ready():
    patches = _patch_all()
    with patches[0], patches[1], patches[2]:
        result = evaluate_readiness("my_pipeline", now_iso=_NOW)
    assert result["ready"] is True
    assert result["issues"] == []


def test_evaluate_readiness_unhealthy_pipeline_not_ready():
    patches = _patch_all(health={"status": "fail", "reason": "critical severity"})
    with patches[0], patches[1], patches[2]:
        result = evaluate_readiness("my_pipeline", now_iso=_NOW)
    assert result["ready"] is False
    assert any("health" in issue for issue in result["issues"])


def test_evaluate_readiness_quota_exceeded_not_ready():
    quotas = {"my_pipeline": {"used": 10, "limit": 10}}
    patches = _patch_all(quotas=quotas)
    with patches[0], patches[1], patches[2]:
        result = evaluate_readiness("my_pipeline", now_iso=_NOW)
    assert result["ready"] is False
    assert any("quota" in issue for issue in result["issues"])


def test_evaluate_readiness_quota_not_exceeded_is_ready():
    quotas = {"my_pipeline": {"used": 5, "limit": 10}}
    patches = _patch_all(quotas=quotas)
    with patches[0], patches[1], patches[2]:
        result = evaluate_readiness("my_pipeline", now_iso=_NOW)
    assert result["ready"] is True


def test_evaluate_readiness_in_blackout_not_ready():
    windows = [
        {
            "name": "maintenance",
            "start": "2024-06-01T11:00:00+00:00",
            "end": "2024-06-01T13:00:00+00:00",
            "pipelines": [],
            "reason": "scheduled maintenance",
        }
    ]
    patches = _patch_all(blackouts=windows)
    with patches[0], patches[1], patches[2]:
        result = evaluate_readiness("my_pipeline", now_iso=_NOW)
    assert result["ready"] is False
    assert any("blackout" in issue for issue in result["issues"])


def test_evaluate_readiness_blackout_for_other_pipeline_is_ready():
    windows = [
        {
            "name": "maintenance",
            "start": "2024-06-01T11:00:00+00:00",
            "end": "2024-06-01T13:00:00+00:00",
            "pipelines": ["other_pipeline"],
            "reason": "scheduled maintenance",
        }
    ]
    patches = _patch_all(blackouts=windows)
    with patches[0], patches[1], patches[2]:
        result = evaluate_readiness("my_pipeline", now_iso=_NOW)
    assert result["ready"] is True


def test_evaluate_readiness_multiple_issues():
    quotas = {"my_pipeline": {"used": 10, "limit": 10}}
    windows = [
        {
            "name": "maint",
            "start": "2024-06-01T11:00:00+00:00",
            "end": "2024-06-01T13:00:00+00:00",
            "pipelines": [],
            "reason": "maint",
        }
    ]
    patches = _patch_all(
        health={"status": "fail", "reason": "sla breach"},
        quotas=quotas,
        blackouts=windows,
    )
    with patches[0], patches[1], patches[2]:
        result = evaluate_readiness("my_pipeline", now_iso=_NOW)
    assert result["ready"] is False
    assert len(result["issues"]) == 3


def test_format_readiness_report_ready():
    result = {
        "pipeline": "etl",
        "ready": True,
        "issues": [],
        "checked_at": _NOW,
    }
    report = format_readiness_report(result)
    assert "READY" in report
    assert "etl" in report


def test_format_readiness_report_not_ready_shows_issues():
    result = {
        "pipeline": "etl",
        "ready": False,
        "issues": ["quota: used 10/10", "blackout: maintenance"],
        "checked_at": _NOW,
    }
    report = format_readiness_report(result)
    assert "NOT READY" in report
    assert "quota" in report
    assert "blackout" in report
