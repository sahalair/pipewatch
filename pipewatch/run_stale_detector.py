"""Detect runs that have been stuck in a non-terminal state for too long."""

from datetime import datetime, timezone, timedelta
from typing import List, Dict, Optional
from pipewatch.run_logger import list_run_records

_DEFAULT_TIMEOUT_MINUTES = 60


def _parse_iso(ts: str) -> datetime:
    return datetime.fromisoformat(ts.replace("Z", "+00:00"))


def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


def detect_stuck_runs(
    pipeline: Optional[str] = None,
    timeout_minutes: int = _DEFAULT_TIMEOUT_MINUTES,
    base_dir: str = ".pipewatch",
) -> List[Dict]:
    """Return runs that started but never finished within the timeout window."""
    cutoff = _now_utc() - timedelta(minutes=timeout_minutes)
    all_runs = list_run_records(base_dir=base_dir)
    stuck = []
    for run in all_runs:
        if pipeline and run.get("pipeline") != pipeline:
            continue
        if run.get("status") not in (None, "running"):
            continue
        started_raw = run.get("started_at")
        if not started_raw:
            continue
        try:
            started = _parse_iso(started_raw)
        except ValueError:
            continue
        if started < cutoff:
            run["stuck_for_minutes"] = int(
                (_now_utc() - started).total_seconds() / 60
            )
            stuck.append(run)
    return stuck


def format_stale_report(stuck_runs: List[Dict]) -> str:
    """Format a human-readable report of stuck runs."""
    if not stuck_runs:
        return "No stuck runs detected."
    lines = [f"Stuck runs detected: {len(stuck_runs)}", ""]
    for run in stuck_runs:
        run_id = run.get("run_id", "unknown")
        pipeline = run.get("pipeline", "unknown")
        started = run.get("started_at", "unknown")
        minutes = run.get("stuck_for_minutes", "?")
        lines.append(f"  [{pipeline}] {run_id}")
        lines.append(f"    Started : {started}")
        lines.append(f"    Stuck   : {minutes} min")
    return "\n".join(lines)
