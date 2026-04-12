"""Tests for pipewatch.run_baseline and pipewatch.cli_baseline."""

from __future__ import annotations

import argparse
import pytest

from pipewatch.run_baseline import (
    load_baselines,
    set_baseline,
    get_baseline,
    remove_baseline,
    list_baselines,
)
from pipewatch.cli_baseline import cmd_set, cmd_get, cmd_remove, cmd_list


# ---------------------------------------------------------------------------
# run_baseline unit tests
# ---------------------------------------------------------------------------

def test_load_baselines_missing_file_returns_empty(tmp_path):
    result = load_baselines(base_dir=str(tmp_path))
    assert result == {}


def test_set_and_get_baseline(tmp_path):
    set_baseline("etl", "run-001", base_dir=str(tmp_path))
    assert get_baseline("etl", base_dir=str(tmp_path)) == "run-001"


def test_set_baseline_overwrites_existing(tmp_path):
    set_baseline("etl", "run-001", base_dir=str(tmp_path))
    set_baseline("etl", "run-002", base_dir=str(tmp_path))
    assert get_baseline("etl", base_dir=str(tmp_path)) == "run-002"


def test_get_baseline_missing_returns_none(tmp_path):
    assert get_baseline("nonexistent", base_dir=str(tmp_path)) is None


def test_set_baseline_empty_pipeline_raises(tmp_path):
    with pytest.raises(ValueError, match="pipeline"):
        set_baseline("", "run-001", base_dir=str(tmp_path))


def test_set_baseline_empty_run_id_raises(tmp_path):
    with pytest.raises(ValueError, match="run_id"):
        set_baseline("etl", "", base_dir=str(tmp_path))


def test_remove_baseline_returns_true_when_exists(tmp_path):
    set_baseline("etl", "run-001", base_dir=str(tmp_path))
    assert remove_baseline("etl", base_dir=str(tmp_path)) is True
    assert get_baseline("etl", base_dir=str(tmp_path)) is None


def test_remove_baseline_returns_false_when_missing(tmp_path):
    assert remove_baseline("ghost", base_dir=str(tmp_path)) is False


def test_list_baselines_sorted(tmp_path):
    set_baseline("zzz", "run-z", base_dir=str(tmp_path))
    set_baseline("aaa", "run-a", base_dir=str(tmp_path))
    entries = list_baselines(base_dir=str(tmp_path))
    assert [e["pipeline"] for e in entries] == ["aaa", "zzz"]


def test_list_baselines_empty(tmp_path):
    assert list_baselines(base_dir=str(tmp_path)) == []


# ---------------------------------------------------------------------------
# cli_baseline command tests
# ---------------------------------------------------------------------------

def make_args(**kwargs):
    defaults = {"pipeline": "etl", "run_id": "run-001"}
    defaults.update(kwargs)
    return argparse.Namespace(**defaults)


def test_cmd_set_prints_confirmation(tmp_path, capsys, monkeypatch):
    monkeypatch.chdir(tmp_path)
    cmd_set(make_args(pipeline="etl", run_id="run-42"))
    out = capsys.readouterr().out
    assert "etl" in out
    assert "run-42" in out


def test_cmd_get_shows_run_id(tmp_path, capsys, monkeypatch):
    monkeypatch.chdir(tmp_path)
    set_baseline("etl", "run-99")
    cmd_get(make_args(pipeline="etl"))
    out = capsys.readouterr().out
    assert "run-99" in out


def test_cmd_get_missing_prints_message(tmp_path, capsys, monkeypatch):
    monkeypatch.chdir(tmp_path)
    cmd_get(make_args(pipeline="nope"))
    out = capsys.readouterr().out
    assert "No baseline" in out


def test_cmd_remove_existing(tmp_path, capsys, monkeypatch):
    monkeypatch.chdir(tmp_path)
    set_baseline("etl", "run-1")
    cmd_remove(make_args(pipeline="etl"))
    out = capsys.readouterr().out
    assert "removed" in out


def test_cmd_remove_missing_exits(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    with pytest.raises(SystemExit):
        cmd_remove(make_args(pipeline="ghost"))


def test_cmd_list_empty(tmp_path, capsys, monkeypatch):
    monkeypatch.chdir(tmp_path)
    cmd_list(make_args())
    out = capsys.readouterr().out
    assert "No baselines" in out


def test_cmd_list_shows_entries(tmp_path, capsys, monkeypatch):
    monkeypatch.chdir(tmp_path)
    set_baseline("etl", "run-1")
    set_baseline("ingest", "run-2")
    cmd_list(make_args())
    out = capsys.readouterr().out
    assert "etl" in out
    assert "ingest" in out
