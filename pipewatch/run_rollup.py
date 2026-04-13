"""Aggregate pipeline run statistics into rollup summaries by time window."""

from __future__ import annotations

from typing import Dict, List, Optional
from pipewatch.run_logger import list_run_records


def _parse_iso(ts: str):
    from datetime import datetime, timezone
    return datetime.fromisoformat(ts.replace("Z", "+00:00")).replace(tzinfo=timezone.utc)


def rollup_by_day(pipeline: Optional[str] = None, limit: int = 30) -> List[Dict]:
    """Return per-day rollup dicts for the given pipeline (or all pipelines)."""
    from collections import defaultdict

    runs = list_run_records()
    buckets: Dict[str, Dict] = defaultdict(lambda: {"total": 0, "success": 0, "failed": 0, "durations": []})

    for run in runs:
        if pipeline and run.get("pipeline") != pipeline:
            continue
        started = run.get("started")
        if not started:
            continue
        day = started[:10]  # YYYY-MM-DD
        b = buckets[day]
        b["total"] += 1
        status = run.get("status", "unknown")
        if status == "success":
            b["success"] += 1
        elif status == "failed":
            b["failed"] += 1
        finished = run.get("finished")
        if finished:
            try:
                dur = (_parse_iso(finished) - _parse_iso(started)).total_seconds()
                b["durations"].append(dur)
            except Exception:
                pass

    results = []
    for day in sorted(buckets.keys(), reverse=True)[:limit]:
        b = buckets[day]
        durations = b["durations"]
        avg_duration = round(sum(durations) / len(durations), 2) if durations else None
        results.append({
            "day": day,
            "total": b["total"],
            "success": b["success"],
            "failed": b["failed"],
            "success_rate": round(b["success"] / b["total"], 4) if b["total"] else 0.0,
            "avg_duration_seconds": avg_duration,
        })
    return results


def format_rollup_report(rollup: List[Dict]) -> str:
    """Format a rollup list into a human-readable table string."""
    if not rollup:
        return "No rollup data available."
    lines = [f"{'Day':<12} {'Total':>6} {'OK':>6} {'Fail':>6} {'Rate':>7} {'Avg Dur':>10}"]
    lines.append("-" * 52)
    for row in rollup:
        avg = f"{row['avg_duration_seconds']:.1f}s" if row["avg_duration_seconds"] is not None else "  n/a"
        lines.append(
            f"{row['day']:<12} {row['total']:>6} {row['success']:>6} {row['failed']:>6} "
            f"{row['success_rate']:>6.1%} {avg:>10}"
        )
    return "\n".join(lines)
