"""Compute a reliability score for a pipeline based on uptime, flakiness, and SLA compliance."""

from pipewatch.run_uptime import compute_uptime
from pipewatch.run_flakiness import analyze_pipeline_flakiness
from pipewatch.run_sla import load_sla_rules, check_sla


def _grade_reliability(score: float) -> str:
    if score >= 0.90:
        return "A"
    if score >= 0.75:
        return "B"
    if score >= 0.60:
        return "C"
    if score >= 0.40:
        return "D"
    return "F"


def _uptime_component(pipeline: str, base_dir: str, limit: int) -> float:
    result = compute_uptime(pipeline, base_dir=base_dir, limit=limit)
    ratio = result.get("ratio")
    return ratio if ratio is not None else 0.0


def _flakiness_component(pipeline: str, base_dir: str, limit: int) -> float:
    result = analyze_pipeline_flakiness(pipeline, base_dir=base_dir, limit=limit)
    score = result.get("flakiness_score", 0.0)
    # invert: low flakiness => high component
    return max(0.0, 1.0 - score)


def _sla_component(pipeline: str, base_dir: str) -> float:
    rules = load_sla_rules(base_dir=base_dir)
    rule = rules.get(pipeline)
    if rule is None:
        return 1.0  # no SLA defined => not penalised
    result = check_sla(pipeline, rule, base_dir=base_dir)
    return 1.0 if result.get("ok") else 0.0


def compute_reliability(
    pipeline: str,
    base_dir: str = ".pipewatch",
    limit: int = 50,
) -> dict:
    """Return a reliability report dict for *pipeline*."""
    uptime = _uptime_component(pipeline, base_dir, limit)
    flakiness = _flakiness_component(pipeline, base_dir, limit)
    sla = _sla_component(pipeline, base_dir)

    score = round(uptime * 0.50 + flakiness * 0.30 + sla * 0.20, 4)
    return {
        "pipeline": pipeline,
        "score": score,
        "grade": _grade_reliability(score),
        "uptime_component": round(uptime, 4),
        "flakiness_component": round(flakiness, 4),
        "sla_component": round(sla, 4),
    }


def format_reliability_report(report: dict) -> str:
    lines = [
        f"Reliability Report — {report['pipeline']}",
        f"  Score   : {report['score']:.2%}  [{report['grade']}]",
        f"  Uptime  : {report['uptime_component']:.2%}  (weight 50%)",
        f"  Stability: {report['flakiness_component']:.2%}  (weight 30%)",
        f"  SLA     : {report['sla_component']:.2%}  (weight 20%)",
    ]
    return "\n".join(lines)
