"""run_decay.py — Tracks how 'fresh' a pipeline is based on recency of successful runs."""

import math
from datetime import datetime, timezone
from typing import Optional

from pipewatch.run_logger import list_run_records


def _parse_iso(ts: str) -> datetime:
    return datetime.fromisoformat(ts.replace("Z", "+00:00"))


def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


def compute_decay_score(pipeline: str, half_life_hours: float = 24.0) -> dict:
    """Compute a freshness/decay score in [0.0, 1.0] for a pipeline.

    Score decays exponentially based on time since last successful run.
    1.0 = just ran successfully, 0.0 = very stale.
    """
    if half_life_hours <= 0:
        raise ValueError("half_life_hours must be positive")

    runs = [
        r for r in list_run_records()
        if r.get("pipeline") == pipeline and r.get("status") == "ok"
    ]

    if not runs:
        return {
            "pipeline": pipeline,
            "score": 0.0,
            "grade": "stale",
            "last_success_iso": None,
            "hours_since_success": None,
        }

    runs_sorted = sorted(runs, key=lambda r: r.get("finished") or r.get("started", ""), reverse=True)
    last_run = runs_sorted[0]
    ts_str = last_run.get("finished") or last_run.get("started")
    last_dt = _parse_iso(ts_str)
    now = _now_utc()
    hours_elapsed = (now - last_dt).total_seconds() / 3600.0
    score = math.exp(-math.log(2) * hours_elapsed / half_life_hours)
    score = max(0.0, min(1.0, score))
    grade = _grade_decay(score)

    return {
        "pipeline": pipeline,
        "score": round(score, 4),
        "grade": grade,
        "last_success_iso": ts_str,
        "hours_since_success": round(hours_elapsed, 2),
    }


def _grade_decay(score: float) -> str:
    if score >= 0.75:
        return "fresh"
    if score >= 0.40:
        return "aging"
    if score >= 0.10:
        return "stale"
    return "expired"


def format_decay_report(result: dict) -> str:
    lines = [
        f"Pipeline : {result['pipeline']}",
        f"Freshness: {result['grade'].upper()} (score={result['score']})",
    ]
    if result["hours_since_success"] is not None:
        lines.append(f"Last OK  : {result['last_success_iso']} ({result['hours_since_success']}h ago)")
    else:
        lines.append("Last OK  : never")
    return "\n".join(lines)
