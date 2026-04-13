"""run_freshness.py — Evaluate how fresh (recently active) a pipeline is."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Optional

from pipewatch.run_logger import list_run_records


def _parse_iso(ts: str) -> datetime:
    return datetime.fromisoformat(ts.replace("Z", "+00:00"))


def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


def compute_freshness(pipeline: str, max_age_hours: float = 24.0) -> dict:
    """Return freshness info for *pipeline*.

    Fields returned:
      - pipeline: str
      - last_run_at: ISO string or None
      - age_hours: float or None
      - is_fresh: bool
      - max_age_hours: float
      - grade: str  ("fresh" | "stale" | "unknown")
    """
    all_runs = list_run_records()
    pipeline_runs = [
        r for r in all_runs if r.get("pipeline") == pipeline
    ]

    if not pipeline_runs:
        return {
            "pipeline": pipeline,
            "last_run_at": None,
            "age_hours": None,
            "is_fresh": False,
            "max_age_hours": max_age_hours,
            "grade": "unknown",
        }

    # Pick the most recent finished_at (or started_at as fallback)
    def _best_ts(r: dict) -> Optional[str]:
        return r.get("finished_at") or r.get("started_at")

    timestamped = [(r, _best_ts(r)) for r in pipeline_runs if _best_ts(r)]
    if not timestamped:
        return {
            "pipeline": pipeline,
            "last_run_at": None,
            "age_hours": None,
            "is_fresh": False,
            "max_age_hours": max_age_hours,
            "grade": "unknown",
        }

    latest_run, latest_ts = max(timestamped, key=lambda x: x[1])
    age_hours = (_now_utc() - _parse_iso(latest_ts)).total_seconds() / 3600.0
    is_fresh = age_hours <= max_age_hours

    return {
        "pipeline": pipeline,
        "last_run_at": latest_ts,
        "age_hours": round(age_hours, 3),
        "is_fresh": is_fresh,
        "max_age_hours": max_age_hours,
        "grade": "fresh" if is_fresh else "stale",
    }


def format_freshness_report(result: dict) -> str:
    lines = [
        f"Pipeline : {result['pipeline']}",
        f"Grade    : {result['grade'].upper()}",
        f"Last run : {result['last_run_at'] or 'never'}",
    ]
    if result["age_hours"] is not None:
        lines.append(f"Age      : {result['age_hours']:.1f} h  (max {result['max_age_hours']:.1f} h)")
    return "\n".join(lines)
