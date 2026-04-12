"""Baseline management for pipeline runs.

Allows pinning a run as the 'baseline' for a pipeline so that future
runs can be compared against it automatically.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Optional

_BASELINES_FILE = ".pipewatch/baselines.json"


def _baselines_path(base_dir: str = ".") -> Path:
    return Path(base_dir) / _BASELINES_FILE


def load_baselines(base_dir: str = ".") -> dict:
    """Load all baseline mappings {pipeline: run_id}."""
    path = _baselines_path(base_dir)
    if not path.exists():
        return {}
    with path.open() as f:
        return json.load(f)


def save_baselines(baselines: dict, base_dir: str = ".") -> None:
    """Persist baseline mappings to disk."""
    path = _baselines_path(base_dir)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w") as f:
        json.dump(baselines, f, indent=2)


def set_baseline(pipeline: str, run_id: str, base_dir: str = ".") -> None:
    """Set *run_id* as the baseline for *pipeline*."""
    if not pipeline:
        raise ValueError("pipeline must not be empty")
    if not run_id:
        raise ValueError("run_id must not be empty")
    baselines = load_baselines(base_dir)
    baselines[pipeline] = run_id
    save_baselines(baselines, base_dir)


def get_baseline(pipeline: str, base_dir: str = ".") -> Optional[str]:
    """Return the baseline run_id for *pipeline*, or None if not set."""
    return load_baselines(base_dir).get(pipeline)


def remove_baseline(pipeline: str, base_dir: str = ".") -> bool:
    """Remove the baseline for *pipeline*. Returns True if it existed."""
    baselines = load_baselines(base_dir)
    if pipeline not in baselines:
        return False
    del baselines[pipeline]
    save_baselines(baselines, base_dir)
    return True


def list_baselines(base_dir: str = ".") -> list[dict]:
    """Return a sorted list of {pipeline, run_id} dicts."""
    baselines = load_baselines(base_dir)
    return [
        {"pipeline": p, "run_id": r}
        for p, r in sorted(baselines.items())
    ]
