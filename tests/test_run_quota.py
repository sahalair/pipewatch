"""Tests for pipewatch.run_quota."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from pipewatch.run_quota import (
    load_quotas,
    save_quotas,
    set_quota,
    remove_quota,
    get_quota,
    check_quota,
    DEFAULT_QUOTA,
)


@pytest.fixture()
def quota_file(tmp_path: Path) -> Path:
    return tmp_path / "quotas.json"


def test_load_quotas_missing_file_returns_empty(quota_file: Path) -> None:
    result = load_quotas(quota_file)
    assert result == {}


def test_save_and_load_quotas_roundtrip(quota_file: Path) -> None:
    data = {"etl": 50, "reports": 200}
    save_quotas(data, quota_file)
    loaded = load_quotas(quota_file)
    assert loaded == data


def test_set_quota_creates_entry(quota_file: Path) -> None:
    quotas = set_quota("etl", 30, quota_file)
    assert quotas["etl"] == 30
    assert load_quotas(quota_file)["etl"] == 30


def test_set_quota_updates_existing(quota_file: Path) -> None:
    set_quota("etl", 30, quota_file)
    set_quota("etl", 75, quota_file)
    assert load_quotas(quota_file)["etl"] == 75


def test_set_quota_invalid_limit_raises(quota_file: Path) -> None:
    with pytest.raises(ValueError):
        set_quota("etl", 0, quota_file)


def test_remove_quota_existing(quota_file: Path) -> None:
    set_quota("etl", 10, quota_file)
    removed = remove_quota("etl", quota_file)
    assert removed is True
    assert "etl" not in load_quotas(quota_file)


def test_remove_quota_nonexistent_returns_false(quota_file: Path) -> None:
    removed = remove_quota("nonexistent", quota_file)
    assert removed is False


def test_get_quota_returns_set_value(quota_file: Path) -> None:
    set_quota("reports", 42, quota_file)
    assert get_quota("reports", quota_file) == 42


def test_get_quota_returns_default_when_missing(quota_file: Path) -> None:
    assert get_quota("unknown_pipeline", quota_file) == DEFAULT_QUOTA


def test_check_quota_not_exceeded(quota_file: Path) -> None:
    set_quota("etl", 50, quota_file)
    result = check_quota("etl", 10, quota_file)
    assert result["pipeline"] == "etl"
    assert result["limit"] == 50
    assert result["current"] == 10
    assert result["exceeded"] is False


def test_check_quota_exceeded(quota_file: Path) -> None:
    set_quota("etl", 5, quota_file)
    result = check_quota("etl", 5, quota_file)
    assert result["exceeded"] is True


def test_check_quota_uses_default_when_no_rule(quota_file: Path) -> None:
    result = check_quota("no_rule_pipeline", 1, quota_file)
    assert result["limit"] == DEFAULT_QUOTA
    assert result["exceeded"] is False
