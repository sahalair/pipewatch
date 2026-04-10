"""Structured run logger for recording pipeline execution metadata."""

from __future__ import annotations

import json
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

DEFAULT_LOG_DIR = Path(".pipewatch")


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def create_run_record(
    pipeline_name: str,
    metadata: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Build a new run record dict with a unique run_id and timestamp."""
    return {
        "run_id": str(uuid.uuid4()),
        "pipeline": pipeline_name,
        "started_at": _now_iso(),
        "finished_at": None,
        "status": "running",
        "metadata": metadata or {},
    }


def finish_run_record(
    record: dict[str, Any],
    status: str = "success",
    output_hash: str | None = None,
) -> dict[str, Any]:
    """Mark a run record as finished."""
    record["finished_at"] = _now_iso()
    record["status"] = status
    if output_hash is not None:
        record["output_hash"] = output_hash
    return record


def save_run_record(record: dict[str, Any], log_dir: Path = DEFAULT_LOG_DIR) -> Path:
    """Persist a run record as a JSON file under log_dir/<pipeline>/."""
    pipeline_dir = log_dir / record["pipeline"]
    pipeline_dir.mkdir(parents=True, exist_ok=True)
    file_path = pipeline_dir / f"{record['run_id']}.json"
    file_path.write_text(json.dumps(record, indent=2))
    return file_path


def load_run_record(file_path: Path) -> dict[str, Any]:
    """Load a run record from a JSON file."""
    return json.loads(file_path.read_text())


def list_run_records(
    pipeline_name: str, log_dir: Path = DEFAULT_LOG_DIR
) -> list[dict[str, Any]]:
    """Return all run records for a pipeline, sorted by started_at ascending."""
    pipeline_dir = log_dir / pipeline_name
    if not pipeline_dir.exists():
        return []
    records = [
        load_run_record(p)
        for p in sorted(pipeline_dir.glob("*.json"))
    ]
    return sorted(records, key=lambda r: r["started_at"])
