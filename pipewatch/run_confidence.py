"""Compute a confidence score for a pipeline based on historical run data."""

from typing import Optional
from pipewatch.run_flakiness import analyze_pipeline_flakiness
from pipewatch.run_uptime import compute_uptime
from pipewatch.run_anomaly import get_metric_history, detect_anomaly
from pipewatch.run_logger import list_run_records


def _uptime_component(pipeline: str, base_dir: str) -> float:
    """Return 0.0-1.0 based on recent uptime ratio."""
    result = compute_uptime(pipeline, base_dir=base_dir, limit=20)
    ratio = result.get("ratio")
    if ratio is None:
        return 0.5
    return float(ratio)


def _flakiness_component(pipeline: str, base_dir: str) -> float:
    """Return 0.0-1.0 penalty-free score (1.0 = not flaky)."""
    result = analyze_pipeline_flakiness(pipeline, base_dir=base_dir)
    score = result.get("flakiness_score", 0.0)
    return max(0.0, 1.0 - float(score))


def _volume_component(pipeline: str, base_dir: str, min_runs: int = 10) -> float:
    """Return 0.0-1.0 based on how many runs exist (confidence grows with data)."""
    runs = [
        r for r in list_run_records(base_dir=base_dir)
        if r.get("pipeline") == pipeline
    ]
    count = len(runs)
    if count == 0:
        return 0.0
    return min(1.0, count / min_runs)


def compute_confidence(
    pipeline: str,
    base_dir: str = ".pipewatch",
    weights: Optional[dict] = None,
) -> dict:
    """Compute a confidence score (0-100) for a pipeline.

    Returns a dict with score, grade, and component breakdown.
    """
    if weights is None:
        weights = {"uptime": 0.4, "flakiness": 0.4, "volume": 0.2}

    uptime = _uptime_component(pipeline, base_dir)
    flakiness = _flakiness_component(pipeline, base_dir)
    volume = _volume_component(pipeline, base_dir)

    raw = (
        weights["uptime"] * uptime
        + weights["flakiness"] * flakiness
        + weights["volume"] * volume
    )
    score = round(raw * 100, 2)

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
        "score": score,
        "grade": grade,
        "components": {
            "uptime": round(uptime * 100, 2),
            "flakiness": round(flakiness * 100, 2),
            "volume": round(volume * 100, 2),
        },
    }


def format_confidence_report(result: dict) -> str:
    """Return a human-readable confidence report."""
    lines = [
        f"Pipeline : {result['pipeline']}",
        f"Confidence: {result['score']:.1f} / 100  [{result['grade']}]",
        "Components:",
        f"  Uptime    : {result['components']['uptime']:.1f}",
        f"  Flakiness : {result['components']['flakiness']:.1f}",
        f"  Volume    : {result['components']['volume']:.1f}",
    ]
    return "\n".join(lines)
