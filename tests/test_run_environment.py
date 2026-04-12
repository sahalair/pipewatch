"""Tests for pipewatch.run_environment."""

import json
from pathlib import Path
from unittest.mock import patch

import pytest

from pipewatch.run_environment import (
    capture_environment,
    compare_environments,
    load_environment,
    record_environment,
    save_environment,
)


@pytest.fixture(autouse=True)
def _tmp_env_dir(tmp_path, monkeypatch):
    import pipewatch.run_environment as m
    env_dir = tmp_path / "environments"
    monkeypatch.setattr(m, "_ENV_DIR", env_dir)


def test_load_environment_missing_file_returns_empty():
    assert load_environment("nonexistent-run") == {}


def test_save_and_load_environment_roundtrip():
    env = {"python_version": "3.11.0", "cwd": "/tmp"}
    save_environment("run-abc", env)
    loaded = load_environment("run-abc")
    assert loaded == env


def test_capture_environment_has_required_keys():
    env = capture_environment()
    assert "python_version" in env
    assert "cwd" in env
    assert "env_vars" in env


def test_capture_environment_python_version_is_string():
    env = capture_environment()
    assert isinstance(env["python_version"], str)
    assert len(env["python_version"]) > 0


def test_capture_environment_with_extras():
    env = capture_environment(extras={"my_key": "my_value"})
    assert env["extras"] == {"my_key": "my_value"}


def test_capture_environment_without_extras_no_extras_key():
    env = capture_environment()
    assert "extras" not in env


def test_record_environment_persists_and_returns(tmp_path):
    env = record_environment("run-xyz")
    assert "python_version" in env
    loaded = load_environment("run-xyz")
    assert loaded["python_version"] == env["python_version"]


def test_record_environment_with_extras():
    env = record_environment("run-extras", extras={"team": "data"})
    assert env["extras"]["team"] == "data"
    loaded = load_environment("run-extras")
    assert loaded["extras"]["team"] == "data"


def test_compare_environments_identical():
    env = {"python_version": "3.11.0", "cwd": "/tmp"}
    save_environment("run-a", env)
    save_environment("run-b", env)
    result = compare_environments("run-a", "run-b")
    assert result["identical"] is True
    assert result["changed"] == {}


def test_compare_environments_detects_change():
    save_environment("run-c", {"python_version": "3.10.0", "cwd": "/home"})
    save_environment("run-d", {"python_version": "3.11.0", "cwd": "/home"})
    result = compare_environments("run-c", "run-d")
    assert result["identical"] is False
    assert "python_version" in result["changed"]
    assert result["changed"]["python_version"]["a"] == "3.10.0"
    assert result["changed"]["python_version"]["b"] == "3.11.0"


def test_compare_environments_missing_key_in_one():
    save_environment("run-e", {"python_version": "3.11.0"})
    save_environment("run-f", {"python_version": "3.11.0", "cwd": "/tmp"})
    result = compare_environments("run-e", "run-f")
    assert result["identical"] is False
    assert "cwd" in result["changed"]
    assert result["changed"]["cwd"]["a"] is None


def test_compare_environments_result_has_required_keys():
    save_environment("run-g", {})
    save_environment("run-h", {})
    result = compare_environments("run-g", "run-h")
    assert "run_id_a" in result
    assert "run_id_b" in result
    assert "changed" in result
    assert "identical" in result
