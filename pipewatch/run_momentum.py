"""run_momentum.py — compute momentum score for a pipeline based on recent run trends."""

from __future__ import annotations

from typing import Optional

from pipewatch.run_logger import list_run_records
from pipewatch.run_flakiness import analyze_pipeline_flakiness
from pipewatch.run_velocity import compute_velocity


def _success_rate(runs: list) -> float:
    """Return fraction of runs with status 'ok'."""
    if not runs:
        return 0.0
    ok = sum(1 for r in runs if r.get("status") == "ok")
    return ok / len(runs)


def compute_momentum(
    pipeline: str,
    *,
    base_dir: str = ".pipewatch",
    window: str = "day",
    limit: int = 20,
) -> dict:
    """Compute a momentum score (0-100) for *pipeline*.

    Momentum combines:
    - recent success rate (50 pts)
    - velocity trend (25 pts)
    - flakiness penalty (25 pts)
    """
    all_runs = list_run_records(base_dir=base_dir)
    pipeline_runs = [
        r for r in all_runs if r.get("pipeline") == pipeline
    ][-limit:]

    if not pipeline_runs:
        return {
            "pipeline": pipeline,
            "momentum": 0.0,
            "grade": "F",
            "success_rate": 0.0,
            "velocity": 0,
            "flakiness": "unknown",
            "components": {"success": 0.0, "velocity": 0.0, "flakiness": 0.0},
        }

    success_rate = _success_rate(pipeline_runs)
    success_component = success_rate * 50.0

    vel = compute_velocity(pipeline, window=window, base_dir=base_dir)
    velocity_count: int = vel.get("count", 0)
    velocity_component = min(velocity_count / max(1, limit) * 25.0, 25.0)

    flak = analyze_pipeline_flakiness(pipeline, base_dir=base_dir)
    flakiness_label: str = flak.get("classification", "stable")
    flakiness_penalty = {
        "stable": 0.0,
        "low": 5.0,
        "medium": 12.5,
        "high": 20.0,
        "very_high": 25.0,
    }.get(flakiness_label, 0.0)
    flakiness_component = 25.0 - flakiness_penalty

    score = round(success_component + velocity_component + flakiness_component, 2)

    grade: str
    if score >= 90:
        grade = "A"
    elif score >= 75:
        grade = "B"
    elif score >= 60:
        grade = "C"
    elif score >= 40:
        grade = "D"
    else:
        grade = "F"

    return {
        "pipeline": pipeline,
        "momentum": score,
        "grade": grade,
        "success_rate": round(success_rate, 4),
        "velocity": velocity_count,
        "flakiness": flakiness_label,
        "components": {
            "success": round(success_component, 2),
            "velocity": round(velocity_component, 2),
            "flakiness": round(flakiness_component, 2),
        },
    }


def format_momentum_report(result: dict) -> str:
    """Return a human-readable momentum report."""
    lines = [
        f"Pipeline : {result['pipeline']}",
        f"Momentum : {result['momentum']:.1f} / 100  [{result['grade']}]",
        f"Success  : {result['success_rate'] * 100:.1f}%",
        f"Velocity : {result['velocity']} runs in window",
        f"Flakiness: {result['flakiness']}",
        "Components:",
        f"  success   = {result['components']['success']:.2f} / 50.00",
        f"  velocity  = {result['components']['velocity']:.2f} / 25.00",
        f"  flakiness = {result['components']['flakiness']:.2f} / 25.00",
    ]
    return "\n".join(lines)
