"""Tests for pipewatch.run_dependencies."""

from __future__ import annotations

import pytest

from pipewatch.run_dependencies import (
    add_dependency,
    get_dependencies,
    get_dependents,
    load_dependencies,
    remove_dependency,
    save_dependencies,
)


def test_load_dependencies_missing_file(tmp_path):
    deps = load_dependencies(base_dir=str(tmp_path))
    assert deps == {}


def test_save_and_load_roundtrip(tmp_path):
    data = {"run-2": ["run-1"], "run-3": ["run-1", "run-2"]}
    save_dependencies(data, base_dir=str(tmp_path))
    loaded = load_dependencies(base_dir=str(tmp_path))
    assert loaded == data


def test_add_dependency_creates_entry(tmp_path):
    add_dependency("run-b", "run-a", base_dir=str(tmp_path))
    assert "run-a" in get_dependencies("run-b", base_dir=str(tmp_path))


def test_add_dependency_idempotent(tmp_path):
    add_dependency("run-b", "run-a", base_dir=str(tmp_path))
    add_dependency("run-b", "run-a", base_dir=str(tmp_path))
    deps = get_dependencies("run-b", base_dir=str(tmp_path))
    assert deps.count("run-a") == 1


def test_add_multiple_upstreams(tmp_path):
    add_dependency("run-c", "run-a", base_dir=str(tmp_path))
    add_dependency("run-c", "run-b", base_dir=str(tmp_path))
    deps = get_dependencies("run-c", base_dir=str(tmp_path))
    assert set(deps) == {"run-a", "run-b"}


def test_get_dependencies_unknown_run(tmp_path):
    assert get_dependencies("no-such-run", base_dir=str(tmp_path)) == []


def test_remove_dependency_existing(tmp_path):
    add_dependency("run-b", "run-a", base_dir=str(tmp_path))
    result = remove_dependency("run-b", "run-a", base_dir=str(tmp_path))
    assert result is True
    assert get_dependencies("run-b", base_dir=str(tmp_path)) == []


def test_remove_dependency_cleans_empty_key(tmp_path):
    add_dependency("run-b", "run-a", base_dir=str(tmp_path))
    remove_dependency("run-b", "run-a", base_dir=str(tmp_path))
    deps = load_dependencies(base_dir=str(tmp_path))
    assert "run-b" not in deps


def test_remove_dependency_nonexistent_returns_false(tmp_path):
    result = remove_dependency("run-b", "run-a", base_dir=str(tmp_path))
    assert result is False


def test_get_dependents(tmp_path):
    add_dependency("run-b", "run-a", base_dir=str(tmp_path))
    add_dependency("run-c", "run-a", base_dir=str(tmp_path))
    dependents = get_dependents("run-a", base_dir=str(tmp_path))
    assert set(dependents) == {"run-b", "run-c"}


def test_get_dependents_none(tmp_path):
    add_dependency("run-b", "run-a", base_dir=str(tmp_path))
    assert get_dependents("run-b", base_dir=str(tmp_path)) == []
