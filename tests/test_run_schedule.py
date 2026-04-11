"""Tests for pipewatch.run_schedule."""

from __future__ import annotations

import json
from datetime import datetime, timezone, timedelta
from pathlib import Path

import pytest

from pipewatch.run_schedule import (
    load_schedules,
    save_schedules,
    set_schedule,
    remove_schedule,
    check_overdue,
)


@pytest.fixture()
def sched_path(tmp_path: Path) -> Path:
    return tmp_path / "schedules.json"


def test_load_schedules_missing_file(sched_path):
    result = load_schedules(sched_path)
    assert result == {}


def test_save_and_load_schedules_roundtrip(sched_path):
    data = {"my_pipeline": {"pipeline": "my_pipeline", "interval_minutes": 60}}
    save_schedules(data, sched_path)
    loaded = load_schedules(sched_path)
    assert loaded == data


def test_set_schedule_creates_entry(sched_path):
    rule = set_schedule("etl", 30, sched_path)
    assert rule["pipeline"] == "etl"
    assert rule["interval_minutes"] == 30
    schedules = load_schedules(sched_path)
    assert "etl" in schedules


def test_set_schedule_updates_existing(sched_path):
    set_schedule("etl", 30, sched_path)
    set_schedule("etl", 90, sched_path)
    schedules = load_schedules(sched_path)
    assert schedules["etl"]["interval_minutes"] == 90


def test_set_schedule_invalid_interval(sched_path):
    with pytest.raises(ValueError):
        set_schedule("etl", 0, sched_path)
    with pytest.raises(ValueError):
        set_schedule("etl", -5, sched_path)


def test_remove_schedule_returns_true_when_exists(sched_path):
    set_schedule("etl", 30, sched_path)
    result = remove_schedule("etl", sched_path)
    assert result is True
    assert "etl" not in load_schedules(sched_path)


def test_remove_schedule_returns_false_when_missing(sched_path):
    result = remove_schedule("nonexistent", sched_path)
    assert result is False


def test_check_overdue_unscheduled_pipeline(sched_path):
    result = check_overdue("ghost", None, sched_path)
    assert result["scheduled"] is False
    assert result["overdue"] is False


def test_check_overdue_no_last_run(sched_path):
    set_schedule("etl", 60, sched_path)
    result = check_overdue("etl", None, sched_path)
    assert result["scheduled"] is True
    assert result["overdue"] is True
    assert result["minutes_overdue"] == 60


def test_check_overdue_recent_run_not_overdue(sched_path):
    set_schedule("etl", 60, sched_path)
    recent = datetime.now(timezone.utc) - timedelta(minutes=10)
    result = check_overdue("etl", recent.isoformat(), sched_path)
    assert result["overdue"] is False
    assert result["minutes_overdue"] == 0


def test_check_overdue_old_run_is_overdue(sched_path):
    set_schedule("etl", 30, sched_path)
    old = datetime.now(timezone.utc) - timedelta(minutes=90)
    result = check_overdue("etl", old.isoformat(), sched_path)
    assert result["overdue"] is True
    assert result["minutes_overdue"] >= 59
