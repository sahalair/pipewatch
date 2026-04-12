"""Track and analyze flaky pipeline runs (alternating pass/fail patterns)."""

from typing import Optional
from pipewatch.run_logger import list_run_records


def get_run_statuses_for_pipeline(pipeline: str, base_dir: str = ".", limit: int = 20) -> list:
    """Return a list of (run_id, status) tuples for a pipeline, oldest first."""
    records = list_run_records(base_dir=base_dir)
    filtered = [
        r for r in records
        if r.get("pipeline") == pipeline and r.get("status") in ("ok", "failed")
    ]
    filtered.sort(key=lambda r: r.get("started", ""))
    return [(r["run_id"], r["status"]) for r in filtered[-limit:]]


def count_flips(statuses: list) -> int:
    """Count the number of status transitions (flips) in a list of statuses."""
    if len(statuses) < 2:
        return 0
    return sum(
        1 for i in range(1, len(statuses))
        if statuses[i] != statuses[i - 1]
    )


def flakiness_score(statuses: list) -> float:
    """Return a flakiness score between 0.0 (stable) and 1.0 (maximally flaky)."""
    n = len(statuses)
    if n < 2:
        return 0.0
    return round(count_flips(statuses) / (n - 1), 4)


def classify_flakiness(score: float) -> str:
    """Classify a flakiness score as 'stable', 'flaky', or 'highly_flaky'."""
    if score < 0.2:
        return "stable"
    if score < 0.6:
        return "flaky"
    return "highly_flaky"


def analyze_pipeline_flakiness(
    pipeline: str,
    base_dir: str = ".",
    limit: int = 20,
) -> dict:
    """Return a flakiness analysis dict for a pipeline."""
    runs = get_run_statuses_for_pipeline(pipeline, base_dir=base_dir, limit=limit)
    statuses = [s for _, s in runs]
    score = flakiness_score(statuses)
    return {
        "pipeline": pipeline,
        "run_count": len(runs),
        "flip_count": count_flips(statuses),
        "flakiness_score": score,
        "classification": classify_flakiness(score),
        "recent_statuses": statuses,
    }


def format_flakiness_report(analysis: dict) -> str:
    """Format a flakiness analysis dict as a human-readable string."""
    lines = [
        f"Pipeline : {analysis['pipeline']}",
        f"Runs     : {analysis['run_count']}",
        f"Flips    : {analysis['flip_count']}",
        f"Score    : {analysis['flakiness_score']:.4f}",
        f"Class    : {analysis['classification'].upper()}",
        f"Statuses : {' -> '.join(analysis['recent_statuses']) or '(none)'}",
    ]
    return "\n".join(lines)
