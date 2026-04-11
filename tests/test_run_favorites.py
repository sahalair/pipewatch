"""Tests for run_favorites and cli_favorites."""

import pytest
from unittest.mock import patch
from pipewatch.run_favorites import (
    add_favorite,
    remove_favorite,
    is_favorite,
    list_favorites,
    find_favorites_by_label,
    load_favorites,
)
from pipewatch.cli_favorites import cmd_add, cmd_remove, cmd_list, cmd_check


def make_args(**kwargs):
    class Args:
        pass
    a = Args()
    for k, v in kwargs.items():
        setattr(a, k, v)
    return a


def test_load_favorites_missing_file(tmp_path):
    result = load_favorites(base_dir=str(tmp_path))
    assert result == {}


def test_add_favorite_creates_entry(tmp_path):
    entry = add_favorite("run-001", label="gold", note="best run", base_dir=str(tmp_path))
    assert entry["run_id"] == "run-001"
    assert entry["label"] == "gold"
    assert entry["note"] == "best run"


def test_add_favorite_persists(tmp_path):
    add_favorite("run-002", base_dir=str(tmp_path))
    favorites = load_favorites(base_dir=str(tmp_path))
    assert "run-002" in favorites


def test_add_favorite_overwrites_existing(tmp_path):
    add_favorite("run-003", label="old", base_dir=str(tmp_path))
    add_favorite("run-003", label="new", base_dir=str(tmp_path))
    favorites = load_favorites(base_dir=str(tmp_path))
    assert favorites["run-003"]["label"] == "new"


def test_remove_favorite_returns_true(tmp_path):
    add_favorite("run-004", base_dir=str(tmp_path))
    result = remove_favorite("run-004", base_dir=str(tmp_path))
    assert result is True
    assert not is_favorite("run-004", base_dir=str(tmp_path))


def test_remove_favorite_returns_false_if_missing(tmp_path):
    result = remove_favorite("nonexistent", base_dir=str(tmp_path))
    assert result is False


def test_is_favorite_true_and_false(tmp_path):
    add_favorite("run-005", base_dir=str(tmp_path))
    assert is_favorite("run-005", base_dir=str(tmp_path)) is True
    assert is_favorite("run-999", base_dir=str(tmp_path)) is False


def test_list_favorites_sorted(tmp_path):
    add_favorite("run-c", base_dir=str(tmp_path))
    add_favorite("run-a", base_dir=str(tmp_path))
    add_favorite("run-b", base_dir=str(tmp_path))
    ids = [e["run_id"] for e in list_favorites(base_dir=str(tmp_path))]
    assert ids == sorted(ids)


def test_find_favorites_by_label(tmp_path):
    add_favorite("run-x", label="important", base_dir=str(tmp_path))
    add_favorite("run-y", label="other", base_dir=str(tmp_path))
    results = find_favorites_by_label("important", base_dir=str(tmp_path))
    assert len(results) == 1
    assert results[0]["run_id"] == "run-x"


def test_cmd_add_prints_confirmation(tmp_path, capsys):
    args = make_args(run_id="run-z", label="top", note="")
    with patch("pipewatch.cli_favorites.add_favorite", return_value={"run_id": "run-z", "label": "top", "note": ""}) as mock_add:
        cmd_add(args)
    captured = capsys.readouterr()
    assert "run-z" in captured.out


def test_cmd_remove_prints_unstarred(capsys):
    args = make_args(run_id="run-del")
    with patch("pipewatch.cli_favorites.remove_favorite", return_value=True):
        cmd_remove(args)
    assert "Unstarred" in capsys.readouterr().out


def test_cmd_remove_not_found_message(capsys):
    args = make_args(run_id="run-missing")
    with patch("pipewatch.cli_favorites.remove_favorite", return_value=False):
        cmd_remove(args)
    assert "not in favorites" in capsys.readouterr().out


def test_cmd_list_empty(capsys):
    args = make_args(label=None)
    with patch("pipewatch.cli_favorites.list_favorites", return_value=[]):
        cmd_list(args)
    assert "No favorites" in capsys.readouterr().out


def test_cmd_check_starred(capsys):
    args = make_args(run_id="run-star")
    with patch("pipewatch.cli_favorites.is_favorite", return_value=True):
        cmd_check(args)
    assert "starred" in capsys.readouterr().out
