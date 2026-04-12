"""Utilities for cleaning up old or stale run records."""

from __future__ import annotations

import os
from datetime import datetime, timezone, timedelta
from typing import List, Optional

from pipewatch.run_logger import list_run_records, load_run_record, save_run_record


def _parse_iso(ts: str) -> datetime:
    return datetime.fromisoformat(ts.replace("Z", "+00:00"))


def find_stale_runs(
    base_dir: str,
    older_than_days: int = 30,
    status: Optional[str] = None,
) -> List[dict]:
    """Return runs older than *older_than_days* days, optionally filtered by status."""
    cutoff = datetime.now(tz=timezone.utc) - timedelta(days=older_than_days)
    stale = []
    for record in list_run_records(base_dir):
        started = record.get("started_at")
        if not started:
            continue
        try:
            ts = _parse_iso(started)
        except ValueError:
            continue
        if ts < cutoff:
            if status is None or record.get("status") == status:
                stale.append(record)
    return stale


def mark_runs_archived(
    base_dir: str,
    run_ids: List[str],
) -> List[str]:
    """Tag each run in *run_ids* with cleanup_status='archived'. Returns processed IDs."""
    processed = []
    for run_id in run_ids:
        try:
            record = load_run_record(base_dir, run_id)
        except FileNotFoundError:
            continue
        record["cleanup_status"] = "archived"
        save_run_record(base_dir, record)
        processed.append(run_id)
    return processed


def purge_runs(
    base_dir: str,
    run_ids: List[str],
) -> List[str]:
    """Delete run record files for the given IDs. Returns IDs that were removed."""
    removed = []
    for run_id in run_ids:
        path = os.path.join(base_dir, "runs", f"{run_id}.json")
        if os.path.exists(path):
            os.remove(path)
            removed.append(run_id)
    return removed


def format_cleanup_report(stale: List[dict], purged: List[str]) -> str:
    lines = [f"Stale runs found: {len(stale)}"]
    for r in stale:
        lines.append(f"  - {r['run_id']}  started={r.get('started_at', 'N/A')}  status={r.get('status', 'N/A')}")
    lines.append(f"Purged: {len(purged)}")
    for rid in purged:
        lines.append(f"  - {rid}")
    return "\n".join(lines)
