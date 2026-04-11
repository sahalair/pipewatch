"""Track and report progress of in-flight pipeline runs."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

_PROGRESS_FILE = "progress.json"


def _progress_path(base_dir: str = ".pipewatch") -> Path:
    return Path(base_dir) / _PROGRESS_FILE


def load_progress(base_dir: str = ".pipewatch") -> dict[str, Any]:
    """Load all in-progress run records."""
    path = _progress_path(base_dir)
    if not path.exists():
        return {}
    with path.open() as fh:
        return json.load(fh)


def save_progress(data: dict[str, Any], base_dir: str = ".pipewatch") -> None:
    """Persist the progress map to disk."""
    path = _progress_path(base_dir)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w") as fh:
        json.dump(data, fh, indent=2)


def start_progress(
    run_id: str,
    pipeline: str,
    total_steps: int = 0,
    base_dir: str = ".pipewatch",
) -> dict[str, Any]:
    """Register a run as in-progress."""
    data = load_progress(base_dir)
    entry: dict[str, Any] = {
        "run_id": run_id,
        "pipeline": pipeline,
        "started_at": datetime.now(timezone.utc).isoformat(),
        "total_steps": total_steps,
        "current_step": 0,
        "step_name": None,
        "percent": 0.0,
    }
    data[run_id] = entry
    save_progress(data, base_dir)
    return entry


def update_progress(
    run_id: str,
    current_step: int,
    step_name: str | None = None,
    base_dir: str = ".pipewatch",
) -> dict[str, Any]:
    """Update the step counter for an in-progress run."""
    data = load_progress(base_dir)
    if run_id not in data:
        raise KeyError(f"No in-progress record for run_id={run_id!r}")
    entry = data[run_id]
    entry["current_step"] = current_step
    entry["step_name"] = step_name
    total = entry.get("total_steps") or 0
    entry["percent"] = round(current_step / total * 100, 1) if total > 0 else 0.0
    save_progress(data, base_dir)
    return entry


def finish_progress(run_id: str, base_dir: str = ".pipewatch") -> None:
    """Remove a run from the in-progress tracker."""
    data = load_progress(base_dir)
    data.pop(run_id, None)
    save_progress(data, base_dir)


def get_progress(run_id: str, base_dir: str = ".pipewatch") -> dict[str, Any] | None:
    """Return the progress entry for *run_id*, or None if not found."""
    return load_progress(base_dir).get(run_id)


def format_progress(entry: dict[str, Any]) -> str:
    """Return a human-readable one-liner for a progress entry."""
    step_info = (
        f"step {entry['current_step']}/{entry['total_steps']}"
        if entry.get("total_steps")
        else f"step {entry['current_step']}"
    )
    name_part = f" ({entry['step_name']})" if entry.get("step_name") else ""
    pct = entry.get("percent", 0.0)
    return (
        f"[{entry['pipeline']}] {entry['run_id']} — "
        f"{step_info}{name_part} — {pct:.1f}%"
    )
