"""Tests for pipewatch.alert module."""

from __future__ import annotations

import json
import pytest

from pipewatch.alert import (
    create_alert_rule,
    evaluate_alert,
    format_alert_message,
    load_alert_rules,
    save_alert_rules,
)


def test_create_alert_rule_required_fields():
    rule = create_alert_rule(name="my-rule", snapshot_key="output.txt")
    assert rule["name"] == "my-rule"
    assert rule["snapshot_key"] == "output.txt"
    assert "threshold" in rule
    assert "notify" in rule
    assert rule["enabled"] is True


def test_create_alert_rule_custom_threshold():
    rule = create_alert_rule(name="r", snapshot_key="k", threshold=0.5)
    assert rule["threshold"] == 0.5


def test_save_and_load_alert_rules(tmp_path):
    alerts_file = str(tmp_path / "alerts.json")
    rules = [
        create_alert_rule("rule1", "snap1"),
        create_alert_rule("rule2", "snap2", threshold=0.2),
    ]
    save_alert_rules(rules, alerts_file)
    loaded = load_alert_rules(alerts_file)
    assert len(loaded) == 2
    assert loaded[0]["name"] == "rule1"
    assert loaded[1]["threshold"] == 0.2


def test_load_alert_rules_missing_file(tmp_path):
    result = load_alert_rules(str(tmp_path / "nonexistent.json"))
    assert result == []


def test_evaluate_alert_no_change():
    rule = create_alert_rule("r", "k", threshold=0.0)
    diff_result = {"changed": False, "diff_lines": [], "summary": "identical"}
    assert evaluate_alert(rule, diff_result) is False


def test_evaluate_alert_fires_on_change():
    rule = create_alert_rule("r", "k", threshold=0.0)
    diff_result = {
        "changed": True,
        "diff_lines": ["-old line", "+new line"],
        "summary": "2 lines changed",
    }
    assert evaluate_alert(rule, diff_result) is True


def test_evaluate_alert_threshold_not_exceeded():
    rule = create_alert_rule("r", "k", threshold=0.9)
    diff_result = {
        "changed": True,
        "diff_lines": ["-old", "+new", " same", " same", " same", " same", " same", " same", " same", " same"],
        "summary": "small change",
    }
    # 2 changed out of 10 = 0.2, below threshold 0.9
    assert evaluate_alert(rule, diff_result) is False


def test_evaluate_alert_disabled_rule():
    rule = create_alert_rule("r", "k", threshold=0.0)
    rule["enabled"] = False
    diff_result = {"changed": True, "diff_lines": ["-a", "+b"], "summary": "changed"}
    assert evaluate_alert(rule, diff_result) is False


def test_format_alert_message_contains_name_and_key():
    rule = create_alert_rule("my-rule", "output.csv")
    diff_result = {"changed": True, "diff_lines": [], "summary": "3 lines differ"}
    msg = format_alert_message(rule, diff_result)
    assert "my-rule" in msg
    assert "output.csv" in msg
    assert "ALERT" in msg
    assert "3 lines differ" in msg
