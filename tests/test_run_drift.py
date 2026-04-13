"""Tests for run_drift.py."""

from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import patch

import pytest

from pipewatch.run_drift import detect_drift, format_drift_report


_ENV_A = {
    "python_version": "3.11.0",
    "platform": "linux",
    "cwd": "/home/user/project",
    "hostname": "host-a",
}

_ENV_B = {
    "python_version": "3.11.0",
    "platform": "linux",
    "cwd": "/home/user/project",
    "hostname": "host-b",  # different
}


def _patch(env_a=_ENV_A, env_b=_ENV_B):
    def _load(run_id, base_dir=".pipewatch"):
        return env_a if run_id == "run-a" else env_b

    return patch("pipewatch.run_drift.load_environment", side_effect=_load)


def test_detect_drift_returns_required_keys():
    with _patch():
        report = detect_drift("run-a", "run-b")
    assert "run_a" in report
    assert "run_b" in report
    assert "drifted_keys" in report
    assert "drift_detected" in report
    assert "checked_keys" in report


def test_detect_drift_detects_hostname_change():
    with _patch():
        report = detect_drift("run-a", "run-b")
    assert report["drift_detected"] is True
    keys = [d["key"] for d in report["drifted_keys"]]
    assert "hostname" in keys


def test_detect_drift_no_drift_when_identical():
    with _patch(env_a=_ENV_A, env_b=_ENV_A):
        report = detect_drift("run-a", "run-b")
    assert report["drift_detected"] is False
    assert report["drifted_keys"] == []


def test_detect_drift_respects_custom_keys():
    with _patch():
        report = detect_drift("run-a", "run-b", keys=["python_version"])
    assert report["checked_keys"] == ["python_version"]
    assert report["drift_detected"] is False


def test_detect_drift_records_old_and_new_values():
    with _patch():
        report = detect_drift("run-a", "run-b")
    entry = next(d for d in report["drifted_keys"] if d["key"] == "hostname")
    assert entry["run_a"] == "host-a"
    assert entry["run_b"] == "host-b"


def test_format_drift_report_contains_run_ids():
    with _patch():
        report = detect_drift("run-a", "run-b")
    text = format_drift_report(report)
    assert "run-a" in text
    assert "run-b" in text


def test_format_drift_report_shows_changed_field():
    with _patch():
        report = detect_drift("run-a", "run-b")
    text = format_drift_report(report)
    assert "hostname" in text
    assert "host-a" in text
    assert "host-b" in text


def test_format_drift_report_no_drift_message():
    with _patch(env_a=_ENV_A, env_b=_ENV_A):
        report = detect_drift("run-a", "run-b")
    text = format_drift_report(report)
    assert "no" in text.lower()
