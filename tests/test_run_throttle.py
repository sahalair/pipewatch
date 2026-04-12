"""Tests for pipewatch.run_throttle."""

import json
import os
import pytest
from unittest.mock import patch
from datetime import datetime, timezone, timedelta

from pipewatch.run_throttle import (
    load_throttle_rules,
    save_throttle_rules,
    set_throttle,
    remove_throttle,
    record_trigger,
    is_throttled,
)


@pytest.fixture
def base(tmp_path):
    return str(tmp_path)


def test_load_throttle_rules_missing_file_returns_empty(base):
    result = load_throttle_rules(base)
    assert result == {}


def test_save_and_load_throttle_rules_roundtrip(base):
    rules = {"my_pipeline": {"pipeline": "my_pipeline", "min_interval_seconds": 60, "last_triggered": None}}
    save_throttle_rules(rules, base)
    loaded = load_throttle_rules(base)
    assert loaded == rules


def test_set_throttle_creates_entry(base):
    rule = set_throttle("etl", 120, base)
    assert rule["pipeline"] == "etl"
    assert rule["min_interval_seconds"] == 120
    assert rule["last_triggered"] is None


def test_set_throttle_persists(base):
    set_throttle("etl", 300, base)
    rules = load_throttle_rules(base)
    assert "etl" in rules
    assert rules["etl"]["min_interval_seconds"] == 300


def test_set_throttle_updates_existing(base):
    set_throttle("etl", 60, base)
    set_throttle("etl", 180, base)
    rules = load_throttle_rules(base)
    assert rules["etl"]["min_interval_seconds"] == 180


def test_set_throttle_negative_raises(base):
    with pytest.raises(ValueError):
        set_throttle("etl", -1, base)


def test_remove_throttle_existing(base):
    set_throttle("etl", 60, base)
    removed = remove_throttle("etl", base)
    assert removed is True
    rules = load_throttle_rules(base)
    assert "etl" not in rules


def test_remove_throttle_nonexistent_returns_false(base):
    result = remove_throttle("ghost", base)
    assert result is False


def test_record_trigger_updates_last_triggered(base):
    set_throttle("etl", 60, base)
    ts = record_trigger("etl", base)
    assert ts is not None
    rules = load_throttle_rules(base)
    assert rules["etl"]["last_triggered"] == ts


def test_record_trigger_no_rule_returns_none(base):
    result = record_trigger("unknown", base)
    assert result is None


def test_is_throttled_no_rule(base):
    result = is_throttled("etl", base)
    assert result["throttled"] is False
    assert result["reason"] == "no rule"


def test_is_throttled_never_triggered(base):
    set_throttle("etl", 60, base)
    result = is_throttled("etl", base)
    assert result["throttled"] is False
    assert result["reason"] == "never triggered"


def test_is_throttled_within_interval(base):
    set_throttle("etl", 3600, base)
    recent = datetime.now(timezone.utc).isoformat()
    rules = load_throttle_rules(base)
    rules["etl"]["last_triggered"] = recent
    save_throttle_rules(rules, base)
    result = is_throttled("etl", base)
    assert result["throttled"] is True
    assert result["seconds_remaining"] > 0


def test_is_throttled_after_interval_elapsed(base):
    set_throttle("etl", 1, base)
    old = (datetime.now(timezone.utc) - timedelta(seconds=10)).isoformat()
    rules = load_throttle_rules(base)
    rules["etl"]["last_triggered"] = old
    save_throttle_rules(rules, base)
    result = is_throttled("etl", base)
    assert result["throttled"] is False
    assert result["seconds_remaining"] == 0.0
