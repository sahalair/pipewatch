"""Attach and retrieve free-text notes on pipeline runs."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Dict, List, Optional

from pipewatch.run_logger import _now_iso

_NOTES_FILENAME = "run_notes.json"


def _notes_path(base_dir: str) -> Path:
    return Path(base_dir) / _NOTES_FILENAME


def load_notes(base_dir: str) -> Dict[str, List[dict]]:
    """Return all notes keyed by run_id."""
    path = _notes_path(base_dir)
    if not path.exists():
        return {}
    with path.open() as fh:
        return json.load(fh)


def save_notes(base_dir: str, notes: Dict[str, List[dict]]) -> None:
    path = _notes_path(base_dir)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w") as fh:
        json.dump(notes, fh, indent=2)


def add_note(base_dir: str, run_id: str, text: str, author: Optional[str] = None) -> dict:
    """Append a note to *run_id* and persist.  Returns the new note record."""
    note = {"timestamp": _now_iso(), "text": text, "author": author}
    notes = load_notes(base_dir)
    notes.setdefault(run_id, []).append(note)
    save_notes(base_dir, notes)
    return note


def get_notes(base_dir: str, run_id: str) -> List[dict]:
    """Return all notes for *run_id* (empty list if none)."""
    return load_notes(base_dir).get(run_id, [])


def delete_notes(base_dir: str, run_id: str) -> int:
    """Remove all notes for *run_id*.  Returns the number of notes removed."""
    notes = load_notes(base_dir)
    removed = notes.pop(run_id, [])
    save_notes(base_dir, notes)
    return len(removed)


def format_notes(notes: List[dict]) -> str:
    """Render a list of note records as a human-readable string."""
    if not notes:
        return "(no notes)"
    lines = []
    for n in notes:
        author = n.get("author") or "anonymous"
        lines.append(f"[{n['timestamp']}] {author}: {n['text']}")
    return "\n".join(lines)
