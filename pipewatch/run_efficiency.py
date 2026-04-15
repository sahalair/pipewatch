"""Compute efficiency scores for pipeline runs based on duration and resource usage."""

from __future__ import annotations

from typing import Optional

from pipewatch.run_logger import list_run_records
from pipewatch.run_duration import get_run_duration, get_durations_for_pipeline, summarize_durations
from pipewatch.run_cost import load_costs


def _grade_efficiency(score: float) -> str:
    if score >= 90:
        return "A"
    if score >= 75:
        return "B"
    if score >= 60:
        return "C"
    if score >= 40:
        return "D"
    return "F"


def _duration_component(pipeline: str, run_id: str, base_dir: str = ".") -> float:
    """Return a 0-100 score based on how the run's duration compares to the pipeline median."""
    durations = get_durations_for_pipeline(pipeline, base_dir=base_dir, limit=30)
    if not durations:
        return 50.0
    stats = summarize_durations(durations)
    median = stats.get("median")
    if median is None or median <= 0:
        return 50.0
    run_duration = get_run_duration(run_id, base_dir=base_dir)
    if run_duration is None:
        return 50.0
    ratio = run_duration / median
    if ratio <= 0.8:
        return 100.0
    if ratio <= 1.0:
        return 85.0
    if ratio <= 1.5:
        return 65.0
    if ratio <= 2.0:
        return 40.0
    return 10.0


def _cost_component(run_id: str, base_dir: str = ".") -> float:
    """Return a 0-100 score; penalise runs with recorded cost entries."""
    costs = load_costs(run_id, base_dir=base_dir)
    if not costs:
        return 80.0
    total = sum(c.get("amount", 0.0) for c in costs)
    if total <= 0:
        return 80.0
    if total < 1:
        return 70.0
    if total < 10:
        return 55.0
    if total < 100:
        return 35.0
    return 10.0


def compute_efficiency(pipeline: str, run_id: str, base_dir: str = ".") -> dict:
    """Compute a composite efficiency score for a single run."""
    dur = _duration_component(pipeline, run_id, base_dir=base_dir)
    cost = _cost_component(run_id, base_dir=base_dir)
    score = round(dur * 0.6 + cost * 0.4, 2)
    return {
        "pipeline": pipeline,
        "run_id": run_id,
        "score": score,
        "grade": _grade_efficiency(score),
        "duration_component": round(dur, 2),
        "cost_component": round(cost, 2),
    }


def format_efficiency_report(result: dict) -> str:
    lines = [
        f"Efficiency Report — {result['pipeline']} / {result['run_id']}",
        f"  Score          : {result['score']} ({result['grade']})",
        f"  Duration score : {result['duration_component']}",
        f"  Cost score     : {result['cost_component']}",
    ]
    return "\n".join(lines)
