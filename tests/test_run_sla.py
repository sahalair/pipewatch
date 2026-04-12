"""Tests for pipewatch/run_sla.py"""

import pytest
from pathlib import Path
from pipewatch.run_sla import (
    set_sla, remove_sla, get_sla, load_sla_rules,
    evaluate_sla, format_sla_report,
)


@pytest.fixture
def sla_dir(tmp_path):
    return tmp_path / "sla"


def test_load_sla_rules_missing_file_returns_empty(sla_dir):
    rules = load_sla_rules(base_dir=sla_dir)
    assert rules == {}


def test_set_sla_creates_entry(sla_dir):
    rule = set_sla("etl", 120.0, base_dir=sla_dir)
    assert rule["pipeline"] == "etl"
    assert rule["max_duration_seconds"] == 120.0


def test_set_sla_with_description(sla_dir):
    rule = set_sla("ingest", 60.0, description="Must finish in 1 min", base_dir=sla_dir)
    assert rule["description"] == "Must finish in 1 min"


def test_set_sla_persists(sla_dir):
    set_sla("etl", 90.0, base_dir=sla_dir)
    rules = load_sla_rules(base_dir=sla_dir)
    assert "etl" in rules
    assert rules["etl"]["max_duration_seconds"] == 90.0


def test_set_sla_overwrites_existing(sla_dir):
    set_sla("etl", 60.0, base_dir=sla_dir)
    set_sla("etl", 120.0, base_dir=sla_dir)
    rules = load_sla_rules(base_dir=sla_dir)
    assert rules["etl"]["max_duration_seconds"] == 120.0


def test_get_sla_returns_rule(sla_dir):
    set_sla("etl", 100.0, base_dir=sla_dir)
    rule = get_sla("etl", base_dir=sla_dir)
    assert rule is not None
    assert rule["max_duration_seconds"] == 100.0


def test_get_sla_missing_returns_none(sla_dir):
    assert get_sla("nonexistent", base_dir=sla_dir) is None


def test_remove_sla_existing(sla_dir):
    set_sla("etl", 60.0, base_dir=sla_dir)
    removed = remove_sla("etl", base_dir=sla_dir)
    assert removed is True
    assert get_sla("etl", base_dir=sla_dir) is None


def test_remove_sla_nonexistent_returns_false(sla_dir):
    assert remove_sla("ghost", base_dir=sla_dir) is False


def test_evaluate_sla_no_rule(sla_dir):
    result = evaluate_sla("unknown", 50.0, base_dir=sla_dir)
    assert result["sla_defined"] is False
    assert result["violated"] is False


def test_evaluate_sla_not_violated(sla_dir):
    set_sla("etl", 120.0, base_dir=sla_dir)
    result = evaluate_sla("etl", 90.0, base_dir=sla_dir)
    assert result["sla_defined"] is True
    assert result["violated"] is False


def test_evaluate_sla_violated(sla_dir):
    set_sla("etl", 60.0, base_dir=sla_dir)
    result = evaluate_sla("etl", 90.0, base_dir=sla_dir)
    assert result["violated"] is True


def test_evaluate_sla_exact_boundary(sla_dir):
    set_sla("etl", 60.0, base_dir=sla_dir)
    result = evaluate_sla("etl", 60.0, base_dir=sla_dir)
    assert result["violated"] is False


def test_format_sla_report_no_sla(sla_dir):
    result = evaluate_sla("unknown", 50.0, base_dir=sla_dir)
    report = format_sla_report(result)
    assert "No SLA defined" in report


def test_format_sla_report_ok(sla_dir):
    set_sla("etl", 120.0, base_dir=sla_dir)
    result = evaluate_sla("etl", 90.0, base_dir=sla_dir)
    report = format_sla_report(result)
    assert "OK" in report
    assert "etl" in report


def test_format_sla_report_violated(sla_dir):
    set_sla("etl", 60.0, base_dir=sla_dir)
    result = evaluate_sla("etl", 90.0, base_dir=sla_dir)
    report = format_sla_report(result)
    assert "VIOLATED" in report
