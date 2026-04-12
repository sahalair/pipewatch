"""Utilities for computing and summarizing run durations."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Dict, List, Optional

from pipewatch.run_logger import load_run_record, list_run_records


def _parse_iso(ts: str) -> datetime:
    """Parse an ISO 8601 timestamp into a timezone-aware datetime."""
    return datetime.fromisoformat(ts).replace(tzinfo=timezone.utc)


def get_run_duration(run_id: str, base_dir: str = ".") -> Optional[float]:
    """Return the duration of a run in seconds, or None if unavailable."""
    record = load_run_record(run_id, base_dir=base_dir)
    started = record.get("started_at")
    finished = record.get("finished_at")
    if not started or not finished:
        return None
    return (_parse_iso(finished) - _parse_iso(started)).total_seconds()


def get_durations_for_pipeline(
    pipeline: str, base_dir: str = ".", limit: int = 50
) -> List[Dict]:
    """Return a list of {run_id, duration_seconds} dicts for a pipeline."""
    records = list_run_records(base_dir=base_dir)
    results = []
    for rec in records:
        if rec.get("pipeline") != pipeline:
            continue
        started = rec.get("started_at")
        finished = rec.get("finished_at")
        if not started or not finished:
            continue
        duration = (_parse_iso(finished) - _parse_iso(started)).total_seconds()
        results.append({"run_id": rec["run_id"], "duration_seconds": duration})
        if len(results) >= limit:
            break
    return results


def summarize_durations(durations: List[Dict]) -> Dict:
    """Compute min, max, and average duration from a list of duration dicts."""
    if not durations:
        return {"count": 0, "min": None, "max": None, "avg": None}
    values = [d["duration_seconds"] for d in durations]
    return {
        "count": len(values),
        "min": round(min(values), 3),
        "max": round(max(values), 3),
        "avg": round(sum(values) / len(values), 3),
    }


def format_duration_report(pipeline: str, summary: Dict) -> str:
    """Format a human-readable duration summary report."""
    lines = [f"Duration report for pipeline: {pipeline}"]
    lines.append("-" * 40)
    if summary["count"] == 0:
        lines.append("No completed runs found.")
    else:
        lines.append(f"Runs analysed : {summary['count']}")
        lines.append(f"Min duration  : {summary['min']}s")
        lines.append(f"Max duration  : {summary['max']}s")
        lines.append(f"Avg duration  : {summary['avg']}s")
    return "\n".join(lines)
