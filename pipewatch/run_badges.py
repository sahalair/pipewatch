"""Run badge generation — produce status/score badge metadata for pipelines."""

from __future__ import annotations

from typing import Any

from pipewatch.run_scoring import compute_score
from pipewatch.run_flakiness import analyze_pipeline_flakiness
from pipewatch.run_logger import list_run_records

_GRADE_COLORS = {
    "A": "brightgreen",
    "B": "green",
    "C": "yellow",
    "D": "orange",
    "F": "red",
}

_STATUS_COLORS = {
    "ok": "brightgreen",
    "failed": "red",
    "unknown": "lightgrey",
}


def _last_run_for_pipeline(pipeline: str, base_dir: str = ".") -> dict[str, Any] | None:
    """Return the most recent run record for *pipeline*, or None."""
    records = [
        r for r in list_run_records(base_dir=base_dir)
        if r.get("pipeline") == pipeline
    ]
    if not records:
        return None
    return sorted(records, key=lambda r: r.get("started_at", ""))[-1]


def build_score_badge(pipeline: str, base_dir: str = ".") -> dict[str, str]:
    """Return badge metadata dict for the pipeline quality score."""
    run = _last_run_for_pipeline(pipeline, base_dir=base_dir)
    if run is None:
        return {"label": "score", "message": "no runs", "color": "lightgrey"}
    result = compute_score(run["run_id"], base_dir=base_dir)
    grade = result.get("grade", "F")
    score = result.get("score", 0)
    color = _GRADE_COLORS.get(grade, "lightgrey")
    return {"label": "score", "message": f"{score:.0f} ({grade})", "color": color}


def build_status_badge(pipeline: str, base_dir: str = ".") -> dict[str, str]:
    """Return badge metadata dict for the latest run status."""
    run = _last_run_for_pipeline(pipeline, base_dir=base_dir)
    if run is None:
        return {"label": "status", "message": "no runs", "color": "lightgrey"}
    status = run.get("status", "unknown")
    color = _STATUS_COLORS.get(status, "lightgrey")
    return {"label": "status", "message": status, "color": color}


def build_flakiness_badge(pipeline: str, base_dir: str = ".") -> dict[str, str]:
    """Return badge metadata dict for pipeline flakiness classification."""
    result = analyze_pipeline_flakiness(pipeline, base_dir=base_dir)
    classification = result.get("classification", "unknown")
    score = result.get("flakiness_score", 0.0)
    color = "brightgreen" if score < 0.1 else ("yellow" if score < 0.4 else "red")
    return {"label": "flakiness", "message": f"{classification} ({score:.2f})", "color": color}


def format_badge_text(badge: dict[str, str]) -> str:
    """Render a badge as a simple text representation."""
    return f"[{badge['label']}: {badge['message']}]"
