"""Compute pipeline run velocity: how many runs complete per time window."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional

from pipewatch.run_logger import list_run_records

_WINDOWS = {
    "hour": timedelta(hours=1),
    "day": timedelta(days=1),
    "week": timedelta(weeks=1),
}


def _parse_iso(ts: str) -> datetime:
    return datetime.fromisoformat(ts).replace(tzinfo=timezone.utc)


def _now_utc() -> datetime:
    return datetime.now(tz=timezone.utc)


def compute_velocity(
    pipeline: str,
    window: str = "day",
    base_dir: str = ".pipewatch",
) -> Dict:
    """Return completed-run count and rate for *pipeline* within *window*.

    Returns a dict with keys: pipeline, window, count, rate_per_hour.
    """
    if window not in _WINDOWS:
        raise ValueError(f"Unknown window '{window}'. Choose from: {list(_WINDOWS)}")

    delta = _WINDOWS[window]
    cutoff = _now_utc() - delta

    runs = list_run_records(base_dir=base_dir)
    completed = [
        r for r in runs
        if r.get("pipeline") == pipeline
        and r.get("finished")
        and _parse_iso(r["finished"]) >= cutoff
    ]

    count = len(completed)
    hours = delta.total_seconds() / 3600
    rate = round(count / hours, 4) if hours > 0 else 0.0

    return {
        "pipeline": pipeline,
        "window": window,
        "count": count,
        "rate_per_hour": rate,
    }


def velocity_for_all_pipelines(
    window: str = "day",
    base_dir: str = ".pipewatch",
) -> List[Dict]:
    """Return velocity stats for every distinct pipeline."""
    runs = list_run_records(base_dir=base_dir)
    pipelines = sorted({r["pipeline"] for r in runs if r.get("pipeline")})
    return [compute_velocity(p, window=window, base_dir=base_dir) for p in pipelines]


def format_velocity_report(stats: List[Dict]) -> str:
    """Format a list of velocity dicts into a human-readable table."""
    if not stats:
        return "No velocity data available."
    lines = [f"{'Pipeline':<30} {'Window':<8} {'Count':>6} {'Rate/hr':>10}"]
    lines.append("-" * 60)
    for s in stats:
        lines.append(
            f"{s['pipeline']:<30} {s['window']:<8} {s['count']:>6} {s['rate_per_hour']:>10.4f}"
        )
    return "\n".join(lines)
