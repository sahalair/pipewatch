"""Run volatility: measures how erratically a pipeline's duration changes over time."""

from typing import Optional
from pipewatch.run_duration import get_durations_for_pipeline
from pipewatch.run_logger import list_run_records


def _mean(values: list) -> float:
    return sum(values) / len(values) if values else 0.0


def _stddev(values: list) -> float:
    if len(values) < 2:
        return 0.0
    m = _mean(values)
    variance = sum((v - m) ** 2 for v in values) / (len(values) - 1)
    return variance ** 0.5


def _coefficient_of_variation(values: list) -> Optional[float]:
    """Return CV = stddev / mean, or None if mean is zero."""
    if not values:
        return None
    m = _mean(values)
    if m == 0.0:
        return None
    return _stddev(values) / m


def _grade_volatility(cv: Optional[float]) -> str:
    if cv is None:
        return "unknown"
    if cv < 0.1:
        return "stable"
    if cv < 0.25:
        return "low"
    if cv < 0.5:
        return "moderate"
    if cv < 1.0:
        return "high"
    return "extreme"


def compute_volatility(pipeline: str, base_dir: str = ".", limit: int = 20) -> dict:
    """Compute duration volatility for a pipeline."""
    durations = get_durations_for_pipeline(pipeline, base_dir=base_dir, limit=limit)
    cv = _coefficient_of_variation(durations)
    grade = _grade_volatility(cv)
    return {
        "pipeline": pipeline,
        "sample_count": len(durations),
        "mean_duration": round(_mean(durations), 3) if durations else None,
        "stddev_duration": round(_stddev(durations), 3) if durations else None,
        "coefficient_of_variation": round(cv, 4) if cv is not None else None,
        "grade": grade,
    }


def format_volatility_report(result: dict) -> str:
    lines = [
        f"Pipeline : {result['pipeline']}",
        f"Samples  : {result['sample_count']}",
        f"Mean dur : {result['mean_duration']}s",
        f"Std dev  : {result['stddev_duration']}s",
        f"CV       : {result['coefficient_of_variation']}",
        f"Grade    : {result['grade'].upper()}",
    ]
    return "\n".join(lines)


def volatility_for_all_pipelines(base_dir: str = ".", limit: int = 20) -> list:
    records = list_run_records(base_dir=base_dir)
    pipelines = sorted({r["pipeline"] for r in records if "pipeline" in r})
    return [compute_volatility(p, base_dir=base_dir, limit=limit) for p in pipelines]
