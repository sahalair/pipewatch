"""run_uptime.py — Compute uptime ratio and availability stats for pipelines."""

from __future__ import annotations

from typing import Any

from pipewatch.run_logger import list_run_records


def _filter_runs(runs: list[dict], pipeline: str | None) -> list[dict]:
    if pipeline is None:
        return runs
    return [r for r in runs if r.get("pipeline") == pipeline]


def compute_uptime(
    pipeline: str | None = None,
    limit: int = 100,
    base_dir: str = ".pipewatch",
) -> dict[str, Any]:
    """Return uptime ratio and counts for *pipeline* (or all pipelines).

    A run is considered *successful* when its ``exit_code`` is ``0`` and
    ``status`` is ``"ok"``.
    """
    all_runs = list_run_records(base_dir=base_dir)
    runs = _filter_runs(all_runs, pipeline)
    runs = runs[-limit:] if len(runs) > limit else runs

    total = len(runs)
    if total == 0:
        return {
            "pipeline": pipeline,
            "total": 0,
            "successful": 0,
            "failed": 0,
            "uptime_ratio": None,
            "grade": "N/A",
        }

    successful = sum(
        1
        for r in runs
        if r.get("status") == "ok" and r.get("exit_code", 1) == 0
    )
    failed = total - successful
    ratio = successful / total

    if ratio >= 0.99:
        grade = "A"
    elif ratio >= 0.95:
        grade = "B"
    elif ratio >= 0.90:
        grade = "C"
    elif ratio >= 0.75:
        grade = "D"
    else:
        grade = "F"

    return {
        "pipeline": pipeline,
        "total": total,
        "successful": successful,
        "failed": failed,
        "uptime_ratio": round(ratio, 4),
        "grade": grade,
    }


def format_uptime_report(result: dict[str, Any]) -> str:
    """Return a human-readable uptime report string."""
    label = result["pipeline"] or "(all pipelines)"
    if result["total"] == 0:
        return f"Uptime [{label}]: no runs recorded."
    pct = round(result["uptime_ratio"] * 100, 2)
    return (
        f"Uptime [{label}]: {pct}% "
        f"({result['successful']}/{result['total']} successful) "
        f"— Grade: {result['grade']}"
    )
