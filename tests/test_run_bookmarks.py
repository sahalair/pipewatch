"""Tests for run_bookmarks and cli_bookmarks."""

import argparse
import sys
from pathlib import Path

import pytest

from pipewatch.run_bookmarks import (
    add_bookmark,
    list_bookmarks,
    load_bookmarks,
    remove_bookmark,
    resolve_bookmark,
    save_bookmarks,
)
from pipewatch.cli_bookmarks import cmd_add, cmd_list, cmd_remove, cmd_resolve


def make_args(**kwargs) -> argparse.Namespace:
    return argparse.Namespace(**kwargs)


def test_load_bookmarks_missing_file(tmp_path):
    result = load_bookmarks(base_dir=tmp_path)
    assert result == {}


def test_save_and_load_roundtrip(tmp_path):
    bookmarks = {"baseline": "run-001", "latest": "run-099"}
    save_bookmarks(bookmarks, base_dir=tmp_path)
    loaded = load_bookmarks(base_dir=tmp_path)
    assert loaded == bookmarks


def test_add_bookmark_creates_entry(tmp_path):
    result = add_bookmark("prod", "run-42", base_dir=tmp_path)
    assert result["prod"] == "run-42"


def test_add_bookmark_overwrites_existing(tmp_path):
    add_bookmark("prod", "run-1", base_dir=tmp_path)
    add_bookmark("prod", "run-2", base_dir=tmp_path)
    assert load_bookmarks(base_dir=tmp_path)["prod"] == "run-2"


def test_remove_bookmark_returns_true(tmp_path):
    add_bookmark("old", "run-5", base_dir=tmp_path)
    removed = remove_bookmark("old", base_dir=tmp_path)
    assert removed is True
    assert "old" not in load_bookmarks(base_dir=tmp_path)


def test_remove_bookmark_missing_returns_false(tmp_path):
    removed = remove_bookmark("ghost", base_dir=tmp_path)
    assert removed is False


def test_resolve_bookmark_found(tmp_path):
    add_bookmark("ref", "run-77", base_dir=tmp_path)
    assert resolve_bookmark("ref", base_dir=tmp_path) == "run-77"


def test_resolve_bookmark_not_found(tmp_path):
    assert resolve_bookmark("missing", base_dir=tmp_path) is None


def test_list_bookmarks_sorted(tmp_path):
    add_bookmark("zebra", "run-z", base_dir=tmp_path)
    add_bookmark("alpha", "run-a", base_dir=tmp_path)
    entries = list_bookmarks(base_dir=tmp_path)
    aliases = [e["alias"] for e in entries]
    assert aliases == sorted(aliases)


def test_cmd_add_prints_confirmation(tmp_path, capsys, monkeypatch):
    monkeypatch.chdir(tmp_path)
    args = make_args(alias="snap", run_id="run-10")
    cmd_add(args)
    out = capsys.readouterr().out
    assert "snap" in out
    assert "run-10" in out


def test_cmd_list_empty(tmp_path, capsys, monkeypatch):
    monkeypatch.chdir(tmp_path)
    args = make_args()
    cmd_list(args)
    out = capsys.readouterr().out
    assert "No bookmarks" in out


def test_cmd_remove_existing(tmp_path, capsys, monkeypatch):
    monkeypatch.chdir(tmp_path)
    add_bookmark("del", "run-del")
    args = make_args(alias="del")
    cmd_remove(args)
    out = capsys.readouterr().out
    assert "Removed" in out


def test_cmd_resolve_not_found_exits(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    args = make_args(alias="nope")
    with pytest.raises(SystemExit):
        cmd_resolve(args)
