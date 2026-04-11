"""Manage user-defined labels (key:value pairs) for pipeline runs."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Dict, List, Optional

_LABELS_FILE = "labels.json"


def _labels_path(base_dir: str) -> Path:
    return Path(base_dir) / _LABELS_FILE


def load_labels(base_dir: str) -> Dict[str, Dict[str, str]]:
    """Load all labels from disk. Returns {run_id: {key: value}}."""
    path = _labels_path(base_dir)
    if not path.exists():
        return {}
    with path.open() as fh:
        return json.load(fh)


def save_labels(base_dir: str, labels: Dict[str, Dict[str, str]]) -> None:
    """Persist the full labels mapping to disk."""
    path = _labels_path(base_dir)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w") as fh:
        json.dump(labels, fh, indent=2)


def set_label(base_dir: str, run_id: str, key: str, value: str) -> Dict[str, str]:
    """Set a single label key=value for a run. Returns updated labels for that run."""
    all_labels = load_labels(base_dir)
    run_labels = all_labels.get(run_id, {})
    run_labels[key] = value
    all_labels[run_id] = run_labels
    save_labels(base_dir, all_labels)
    return run_labels


def remove_label(base_dir: str, run_id: str, key: str) -> Dict[str, str]:
    """Remove a label key from a run. Returns updated labels for that run."""
    all_labels = load_labels(base_dir)
    run_labels = all_labels.get(run_id, {})
    run_labels.pop(key, None)
    all_labels[run_id] = run_labels
    save_labels(base_dir, all_labels)
    return run_labels


def get_labels(base_dir: str, run_id: str) -> Dict[str, str]:
    """Return all labels for a specific run."""
    return load_labels(base_dir).get(run_id, {})


def filter_by_label(
    base_dir: str, key: str, value: Optional[str] = None
) -> List[str]:
    """Return run IDs that have the given label key (and optionally value)."""
    all_labels = load_labels(base_dir)
    results = []
    for run_id, labels in all_labels.items():
        if key in labels:
            if value is None or labels[key] == value:
                results.append(run_id)
    return sorted(results)
