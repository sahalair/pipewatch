"""Track named checkpoints within a pipeline run."""

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional

_BASE = Path(".pipewatch")


def _checkpoints_path(run_id: str) -> Path:
    return _BASE / "checkpoints" / f"{run_id}.json"


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def load_checkpoints(run_id: str) -> List[Dict]:
    """Load all checkpoints for a run. Returns empty list if none exist."""
    path = _checkpoints_path(run_id)
    if not path.exists():
        return []
    with path.open() as f:
        return json.load(f)


def save_checkpoints(run_id: str, checkpoints: List[Dict]) -> None:
    """Persist checkpoints for a run."""
    path = _checkpoints_path(run_id)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w") as f:
        json.dump(checkpoints, f, indent=2)


def record_checkpoint(
    run_id: str,
    name: str,
    status: str = "ok",
    message: Optional[str] = None,
    data: Optional[Dict] = None,
) -> Dict:
    """Record a named checkpoint for a run."""
    if status not in ("ok", "warn", "fail"):
        raise ValueError(f"Invalid checkpoint status: {status!r}")
    entry = {
        "name": name,
        "status": status,
        "timestamp": _now_iso(),
        "message": message,
        "data": data or {},
    }
    checkpoints = load_checkpoints(run_id)
    checkpoints.append(entry)
    save_checkpoints(run_id, checkpoints)
    return entry


def get_checkpoints(run_id: str, name: Optional[str] = None) -> List[Dict]:
    """Return checkpoints for a run, optionally filtered by name."""
    checkpoints = load_checkpoints(run_id)
    if name is not None:
        checkpoints = [c for c in checkpoints if c["name"] == name]
    return checkpoints


def clear_checkpoints(run_id: str) -> int:
    """Delete all checkpoints for a run. Returns number removed."""
    checkpoints = load_checkpoints(run_id)
    count = len(checkpoints)
    save_checkpoints(run_id, [])
    return count


def checkpoint_summary(run_id: str) -> Dict:
    """Return a summary dict of checkpoint counts by status."""
    checkpoints = load_checkpoints(run_id)
    summary: Dict[str, int] = {"ok": 0, "warn": 0, "fail": 0, "total": len(checkpoints)}
    for c in checkpoints:
        summary[c["status"]] = summary.get(c["status"], 0) + 1
    return summary
