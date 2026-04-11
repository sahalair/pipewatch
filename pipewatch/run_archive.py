"""Archive and restore pipeline run records."""

import json
import os
import shutil
from datetime import datetime
from typing import List, Optional

from pipewatch.run_logger import load_run_record, list_run_records


def _archive_dir(base_dir: str) -> str:
    return os.path.join(base_dir, "archive")


def _archive_path(base_dir: str, run_id: str) -> str:
    return os.path.join(_archive_dir(base_dir), f"{run_id}.json")


def archive_run(run_id: str, base_dir: str = ".") -> dict:
    """Move a run record into the archive directory."""
    record = load_run_record(run_id, base_dir=base_dir)
    os.makedirs(_archive_dir(base_dir), exist_ok=True)
    dest = _archive_path(base_dir, run_id)
    record["archived_at"] = datetime.utcnow().isoformat() + "Z"
    with open(dest, "w") as fh:
        json.dump(record, fh, indent=2)
    src = os.path.join(base_dir, "runs", f"{run_id}.json")
    if os.path.exists(src):
        os.remove(src)
    return record


def restore_run(run_id: str, base_dir: str = ".") -> dict:
    """Restore an archived run record back to the active runs directory."""
    src = _archive_path(base_dir, run_id)
    if not os.path.exists(src):
        raise FileNotFoundError(f"Archived run not found: {run_id}")
    with open(src) as fh:
        record = json.load(fh)
    record.pop("archived_at", None)
    runs_dir = os.path.join(base_dir, "runs")
    os.makedirs(runs_dir, exist_ok=True)
    dest = os.path.join(runs_dir, f"{run_id}.json")
    with open(dest, "w") as fh:
        json.dump(record, fh, indent=2)
    os.remove(src)
    return record


def list_archived_runs(base_dir: str = ".") -> List[dict]:
    """Return all archived run records sorted by archived_at descending."""
    archive_dir = _archive_dir(base_dir)
    if not os.path.isdir(archive_dir):
        return []
    records = []
    for fname in os.listdir(archive_dir):
        if fname.endswith(".json"):
            with open(os.path.join(archive_dir, fname)) as fh:
                records.append(json.load(fh))
    records.sort(key=lambda r: r.get("archived_at", ""), reverse=True)
    return records


def purge_archive(base_dir: str = ".") -> int:
    """Delete all archived run records. Returns count of purged records."""
    archive_dir = _archive_dir(base_dir)
    if not os.path.isdir(archive_dir):
        return 0
    count = 0
    for fname in os.listdir(archive_dir):
        if fname.endswith(".json"):
            os.remove(os.path.join(archive_dir, fname))
            count += 1
    return count
