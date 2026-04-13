"""Run scoring: assign a composite quality score to a pipeline run."""

from __future__ import annotations

from typing import Any

from pipewatch.run_severity import severity_rank, classify_severity
from pipewatch.run_flakiness import analyze_pipeline_flakiness
from pipewatch.run_duration import get_durations_for_pipeline, summarize_durations
from pipewatch.run_sla import check_sla


_SEVERITY_WEIGHT = 0.40
_FLAKINESS_WEIGHT = 0.30
_DURATION_WEIGHT = 0.20
_SLA_WEIGHT = 0.10


def _severity_component(run: dict[str, Any]) -> float:
    """Return 0-100 based on run severity (higher is better)."""
    severity = classify_severity(run)
    rank = severity_rank(severity)  # 0=ok, 1=low, 2=medium, 3=high, 4=critical
    return max(0.0, 100.0 - rank * 25.0)


def _flakiness_component(pipeline: str, base_dir: str = ".") -> float:
    """Return 0-100 based on pipeline flakiness (higher is more stable)."""
    result = analyze_pipeline_flakiness(pipeline, base_dir=base_dir)
    score = result.get("flakiness_score", 0.0)
    return max(0.0, 100.0 - score * 100.0)


def _duration_component(pipeline: str, base_dir: str = ".") -> float:
    """Return 0-100: penalise if latest run is much slower than the median."""
    durations = get_durations_for_pipeline(pipeline, base_dir=base_dir, limit=20)
    if not durations:
        return 100.0
    stats = summarize_durations(durations)
    median = stats.get("median", 0.0)
    latest = durations[-1]
    if median == 0:
        return 100.0
    ratio = latest / median
    if ratio <= 1.5:
        return 100.0
    if ratio >= 3.0:
        return 0.0
    return max(0.0, 100.0 - (ratio - 1.5) / 1.5 * 100.0)


def _sla_component(run: dict[str, Any], base_dir: str = ".") -> float:
    """Return 100 if SLA is met, 0 otherwise."""
    try:
        result = check_sla(run["run_id"], base_dir=base_dir)
        return 100.0 if result.get("met", True) else 0.0
    except Exception:
        return 100.0


def compute_score(
    run: dict[str, Any],
    base_dir: str = ".",
) -> dict[str, Any]:
    """Compute a composite quality score (0-100) for a run."""
    pipeline = run.get("pipeline", "")
    sev = _severity_component(run)
    flak = _flakiness_component(pipeline, base_dir=base_dir)
    dur = _duration_component(pipeline, base_dir=base_dir)
    sla = _sla_component(run, base_dir=base_dir)

    total = (
        sev * _SEVERITY_WEIGHT
        + flak * _FLAKINESS_WEIGHT
        + dur * _DURATION_WEIGHT
        + sla * _SLA_WEIGHT
    )
    grade = _grade(total)
    return {
        "run_id": run.get("run_id", ""),
        "pipeline": pipeline,
        "score": round(total, 2),
        "grade": grade,
        "components": {
            "severity": round(sev, 2),
            "flakiness": round(flak, 2),
            "duration": round(dur, 2),
            "sla": round(sla, 2),
        },
    }


def _grade(score: float) -> str:
    if score >= 90:
        return "A"
    if score >= 75:
        return "B"
    if score >= 60:
        return "C"
    if score >= 40:
        return "D"
    return "F"


def format_score_report(result: dict[str, Any]) -> str:
    lines = [
        f"Run Score Report",
        f"  Run ID   : {result['run_id']}",
        f"  Pipeline : {result['pipeline']}",
        f"  Score    : {result['score']:.1f} / 100  [{result['grade']}]",
        "  Components:",
    ]
    for key, val in result["components"].items():
        lines.append(f"    {key:<12}: {val:.1f}")
    return "\n".join(lines)
