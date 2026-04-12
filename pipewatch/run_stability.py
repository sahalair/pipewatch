"""Run stability scoring: aggregates flakiness, anomaly, and SLA data into a stability score."""

from typing import Optional
from pipewatch.run_flakiness import analyze_pipeline_flakiness
from pipewatch.run_anomaly import detect_anomaly
from pipewatch.run_sla import load_sla_rules, check_sla

_FLAKINESS_WEIGHT = 0.4
_ANOMALY_WEIGHT = 0.3
_SLA_WEIGHT = 0.3

_FLAKINESS_RANK = {"stable": 1.0, "slightly_flaky": 0.7, "flaky": 0.4, "very_flaky": 0.1}


def _flakiness_component(pipeline: str, base_dir: str) -> float:
    """Return a 0-1 score where 1 is perfectly stable."""
    result = analyze_pipeline_flakiness(pipeline, base_dir=base_dir)
    label = result.get("classification", "stable")
    return _FLAKINESS_RANK.get(label, 0.5)


def _anomaly_component(pipeline: str, metric: str, base_dir: str) -> float:
    """Return 1.0 if no anomaly detected, 0.0 if anomaly detected, 0.5 if unknown."""
    result = detect_anomaly(pipeline, metric, base_dir=base_dir)
    if result is None:
        return 0.5
    return 0.0 if result.get("is_anomaly") else 1.0


def _sla_component(pipeline: str, base_dir: str) -> float:
    """Return fraction of SLA checks that pass (1.0 = all pass, 0.0 = all fail)."""
    rules = load_sla_rules(base_dir=base_dir)
    pipeline_rules = [r for r in rules if r.get("pipeline") == pipeline]
    if not pipeline_rules:
        return 1.0
    passed = sum(1 for r in pipeline_rules if check_sla(r, base_dir=base_dir).get("passed", False))
    return passed / len(pipeline_rules)


def compute_stability_score(
    pipeline: str,
    metric: Optional[str] = None,
    base_dir: str = ".pipewatch",
) -> dict:
    """Compute an overall stability score (0-100) for a pipeline."""
    flakiness = _flakiness_component(pipeline, base_dir)
    anomaly = _anomaly_component(pipeline, metric or "duration", base_dir)
    sla = _sla_component(pipeline, base_dir)

    score = (
        flakiness * _FLAKINESS_WEIGHT
        + anomaly * _ANOMALY_WEIGHT
        + sla * _SLA_WEIGHT
    ) * 100

    return {
        "pipeline": pipeline,
        "score": round(score, 2),
        "components": {
            "flakiness": round(flakiness * 100, 2),
            "anomaly": round(anomaly * 100, 2),
            "sla": round(sla * 100, 2),
        },
        "grade": _grade(score),
    }


def _grade(score: float) -> str:
    if score >= 90:
        return "A"
    if score >= 75:
        return "B"
    if score >= 55:
        return "C"
    if score >= 35:
        return "D"
    return "F"


def format_stability_report(result: dict) -> str:
    lines = [
        f"Stability Report — {result['pipeline']}",
        f"  Score : {result['score']:.1f} / 100  (Grade: {result['grade']})",
        "  Components:",
        f"    Flakiness : {result['components']['flakiness']:.1f}",
        f"    Anomaly   : {result['components']['anomaly']:.1f}",
        f"    SLA       : {result['components']['sla']:.1f}",
    ]
    return "\n".join(lines)
