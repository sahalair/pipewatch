"""Structured key-value annotations attached to pipeline runs."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

_ANNOTATIONS_FILE = "annotations.json"


def _annotations_path(base_dir: str) -> Path:
    return Path(base_dir) / _ANNOTATIONS_FILE


def load_annotations(base_dir: str) -> dict[str, dict[str, Any]]:
    """Return mapping of run_id -> {key: value} annotations."""
    path = _annotations_path(base_dir)
    if not path.exists():
        return {}
    with path.open() as fh:
        return json.load(fh)


def save_annotations(base_dir: str, data: dict[str, dict[str, Any]]) -> None:
    path = _annotations_path(base_dir)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w") as fh:
        json.dump(data, fh, indent=2)


def set_annotation(base_dir: str, run_id: str, key: str, value: Any) -> None:
    """Set a single annotation key on a run, overwriting if it already exists."""
    data = load_annotations(base_dir)
    data.setdefault(run_id, {})[key] = value
    save_annotations(base_dir, data)


def get_annotations(base_dir: str, run_id: str) -> dict[str, Any]:
    """Return all annotations for *run_id* (empty dict if none)."""
    return load_annotations(base_dir).get(run_id, {})


def delete_annotation(base_dir: str, run_id: str, key: str) -> bool:
    """Remove *key* from *run_id* annotations. Returns True if key existed."""
    data = load_annotations(base_dir)
    run_data = data.get(run_id, {})
    if key not in run_data:
        return False
    del run_data[key]
    if not run_data:
        data.pop(run_id, None)
    else:
        data[run_id] = run_data
    save_annotations(base_dir, data)
    return True


def filter_runs_by_annotation(
    base_dir: str, key: str, value: Any
) -> list[str]:
    """Return run IDs where annotation *key* equals *value*."""
    data = load_annotations(base_dir)
    return [
        run_id
        for run_id, annotations in data.items()
        if annotations.get(key) == value
    ]
