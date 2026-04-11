"""Tests for pipewatch.run_notes and pipewatch.cli_notes."""

from __future__ import annotations

import json
from types import SimpleNamespace

import pytest

from pipewatch.run_notes import (
    add_note,
    delete_notes,
    format_notes,
    get_notes,
    load_notes,
    save_notes,
)
from pipewatch.cli_notes import cmd_add, cmd_delete, cmd_list


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _args(base_dir, **kwargs):
    return SimpleNamespace(base_dir=str(base_dir), **kwargs)


# ---------------------------------------------------------------------------
# Unit tests – run_notes module
# ---------------------------------------------------------------------------

def test_load_notes_missing_file(tmp_path):
    assert load_notes(str(tmp_path)) == {}


def test_save_and_load_notes_roundtrip(tmp_path):
    data = {"run-1": [{"timestamp": "t", "text": "hello", "author": None}]}
    save_notes(str(tmp_path), data)
    assert load_notes(str(tmp_path)) == data


def test_add_note_creates_entry(tmp_path):
    note = add_note(str(tmp_path), "run-1", "first note")
    assert note["text"] == "first note"
    assert "timestamp" in note
    assert note["author"] is None


def test_add_note_with_author(tmp_path):
    note = add_note(str(tmp_path), "run-1", "msg", author="alice")
    assert note["author"] == "alice"


def test_add_note_accumulates(tmp_path):
    add_note(str(tmp_path), "run-1", "a")
    add_note(str(tmp_path), "run-1", "b")
    notes = get_notes(str(tmp_path), "run-1")
    assert len(notes) == 2
    assert notes[0]["text"] == "a"
    assert notes[1]["text"] == "b"


def test_get_notes_unknown_run_returns_empty(tmp_path):
    assert get_notes(str(tmp_path), "no-such-run") == []


def test_delete_notes_removes_all(tmp_path):
    add_note(str(tmp_path), "run-1", "x")
    add_note(str(tmp_path), "run-1", "y")
    removed = delete_notes(str(tmp_path), "run-1")
    assert removed == 2
    assert get_notes(str(tmp_path), "run-1") == []


def test_delete_notes_missing_run_returns_zero(tmp_path):
    assert delete_notes(str(tmp_path), "ghost") == 0


def test_format_notes_empty():
    assert format_notes([]) == "(no notes)"


def test_format_notes_contains_text():
    notes = [{"timestamp": "2024-01-01T00:00:00", "text": "check this", "author": "bob"}]
    out = format_notes(notes)
    assert "check this" in out
    assert "bob" in out


# ---------------------------------------------------------------------------
# CLI tests
# ---------------------------------------------------------------------------

def test_cmd_add_prints_confirmation(tmp_path, capsys):
    cmd_add(_args(tmp_path, run_id="run-42", text="looks good", author=None))
    out = capsys.readouterr().out
    assert "Note added" in out


def test_cmd_list_shows_notes(tmp_path, capsys):
    add_note(str(tmp_path), "run-7", "important", author="carol")
    cmd_list(_args(tmp_path, run_id="run-7"))
    out = capsys.readouterr().out
    assert "important" in out
    assert "carol" in out


def test_cmd_list_empty_run(tmp_path, capsys):
    cmd_list(_args(tmp_path, run_id="run-empty"))
    out = capsys.readouterr().out
    assert "no notes" in out


def test_cmd_delete_reports_count(tmp_path, capsys):
    add_note(str(tmp_path), "run-9", "del me")
    cmd_delete(_args(tmp_path, run_id="run-9"))
    out = capsys.readouterr().out
    assert "1" in out
