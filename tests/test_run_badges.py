"""Tests for pipewatch.run_badges."""

from __future__ import annotations

from unittest.mock import patch

import pytest

from pipewatch.run_badges import (
    build_score_badge,
    build_status_badge,
    build_flakiness_badge,
    format_badge_text,
)

_FAKE_RUN = {
    "run_id": "run-001",
    "pipeline": "etl",
    "status": "ok",
    "started_at": "2024-01-01T10:00:00",
}


def _patch_all(runs=None, score=None, flakiness=None):
    if runs is None:
        runs = [_FAKE_RUN]
    if score is None:
        score = {"score": 88.0, "grade": "B"}
    if flakiness is None:
        flakiness = {"flakiness_score": 0.05, "classification": "stable"}
    return [
        patch("pipewatch.run_badges.list_run_records", return_value=runs),
        patch("pipewatch.run_badges.compute_score", return_value=score),
        patch("pipewatch.run_badges.analyze_pipeline_flakiness", return_value=flakiness),
    ]


def test_build_score_badge_returns_required_keys():
    with _patch_all()[0], _patch_all()[1], _patch_all()[2]:
        patches = _patch_all()
        with patches[0], patches[1], patches[2]:
            badge = build_score_badge("etl")
    assert "label" in badge
    assert "message" in badge
    assert "color" in badge


def test_build_score_badge_no_runs_returns_no_runs():
    patches = _patch_all(runs=[])
    with patches[0], patches[1], patches[2]:
        badge = build_score_badge("etl")
    assert badge["message"] == "no runs"
    assert badge["color"] == "lightgrey"


def test_build_score_badge_grade_a_is_brightgreen():
    patches = _patch_all(score={"score": 95.0, "grade": "A"})
    with patches[0], patches[1], patches[2]:
        badge = build_score_badge("etl")
    assert badge["color"] == "brightgreen"
    assert "A" in badge["message"]


def test_build_status_badge_ok_is_brightgreen():
    patches = _patch_all()
    with patches[0], patches[1], patches[2]:
        badge = build_status_badge("etl")
    assert badge["message"] == "ok"
    assert badge["color"] == "brightgreen"


def test_build_status_badge_failed_is_red():
    run = {**_FAKE_RUN, "status": "failed"}
    patches = _patch_all(runs=[run])
    with patches[0], patches[1], patches[2]:
        badge = build_status_badge("etl")
    assert badge["color"] == "red"


def test_build_status_badge_no_runs():
    patches = _patch_all(runs=[])
    with patches[0], patches[1], patches[2]:
        badge = build_status_badge("etl")
    assert badge["message"] == "no runs"


def test_build_flakiness_badge_stable_is_brightgreen():
    patches = _patch_all(flakiness={"flakiness_score": 0.02, "classification": "stable"})
    with patches[0], patches[1], patches[2]:
        badge = build_flakiness_badge("etl")
    assert badge["color"] == "brightgreen"
    assert "stable" in badge["message"]


def test_build_flakiness_badge_high_score_is_red():
    patches = _patch_all(flakiness={"flakiness_score": 0.75, "classification": "flaky"})
    with patches[0], patches[1], patches[2]:
        badge = build_flakiness_badge("etl")
    assert badge["color"] == "red"


def test_format_badge_text_contains_label_and_message():
    badge = {"label": "status", "message": "ok", "color": "brightgreen"}
    text = format_badge_text(badge)
    assert "status" in text
    assert "ok" in text
