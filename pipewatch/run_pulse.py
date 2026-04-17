"""run_pulse.py — Compute a 'pulse' score reflecting recent pipeline activity and health."""
from __future__ import annotations

from typing import Optional

from pipewatch.run_logger import list_run_records
from pipewatch.run_uptime import compute_uptime
from pipewatch.run_velocity import compute_velocity
from pipewatch.run_flakiness import analyze_pipeline_flakiness


def _grade_pulse(score: float) -> str:
    if score >= 0.85:
        return "A"
    if score >= 0.70:
        return "B"
    if score >= 0.55:
        return "C"
    if score >= 0.40:
        return "D"
    return "F"


def compute_pulse(pipeline: str, base_dir: str = ".") -> dict:
    """Compute a pulse score [0.0, 1.0] for a pipeline."""
    uptime = compute_uptime(pipeline, base_dir=base_dir)
    uptime_ratio = uptime.get("ratio") or 0.0

    velocity = compute_velocity(pipeline, window="day", base_dir=base_dir)
    run_count = velocity.get("run_count", 0)
    velocity_component = min(run_count / 10.0, 1.0)

    flakiness = analyze_pipeline_flakiness(pipeline, base_dir=base_dir)
    flakiness_score = flakiness.get("score", 0.0)
    stability_component = max(0.0, 1.0 - flakiness_score)

    score = round(
        0.45 * uptime_ratio + 0.30 * stability_component + 0.25 * velocity_component,
        4,
    )
    return {
        "pipeline": pipeline,
        "score": score,
        "grade": _grade_pulse(score),
        "uptime_ratio": round(uptime_ratio, 4),
        "velocity_component": round(velocity_component, 4),
        "stability_component": round(stability_component, 4),
    }


def format_pulse_report(result: dict) -> str:
    lines = [
        f"Pulse Report — {result['pipeline']}",
        f"  Score     : {result['score']:.4f}  ({result['grade']})",
        f"  Uptime    : {result['uptime_ratio']:.2%}",
        f"  Velocity  : {result['velocity_component']:.2%}",
        f"  Stability : {result['stability_component']:.2%}",
    ]
    return "\n".join(lines)
