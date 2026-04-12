"""Track and report pipeline run coverage — how many expected pipelines ran in a window."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

PIPEWATCH_DIR = Path(".pipewatch")


def _coverage_path() -> Path:
    path = PIPEWATCH_DIR / "coverage.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    return path


def load_coverage() -> dict[str, Any]:
    """Load the coverage config (expected pipelines)."""
    path = _coverage_path()
    if not path.exists():
        return {}
    with path.open() as f:
        return json.load(f)


def save_coverage(data: dict[str, Any]) -> None:
    with _coverage_path().open("w") as f:
        json.dump(data, f, indent=2)


def set_expected_pipeline(pipeline: str, min_runs: int = 1) -> dict[str, Any]:
    """Register a pipeline as expected to run at least min_runs times."""
    if not pipeline:
        raise ValueError("pipeline name must not be empty")
    if min_runs < 1:
        raise ValueError("min_runs must be >= 1")
    data = load_coverage()
    data[pipeline] = {"pipeline": pipeline, "min_runs": min_runs}
    save_coverage(data)
    return data[pipeline]


def remove_expected_pipeline(pipeline: str) -> bool:
    """Remove a pipeline from expected coverage. Returns True if it existed."""
    data = load_coverage()
    if pipeline in data:
        del data[pipeline]
        save_coverage(data)
        return True
    return False


def compute_coverage(run_records: list[dict[str, Any]]) -> dict[str, Any]:
    """Given all run records, compute coverage against expected pipelines."""
    expected = load_coverage()
    counts: dict[str, int] = {}
    for run in run_records:
        pipe = run.get("pipeline", "")
        if pipe:
            counts[pipe] = counts.get(pipe, 0) + 1

    results: list[dict[str, Any]] = []
    for pipeline, cfg in expected.items():
        actual = counts.get(pipeline, 0)
        met = actual >= cfg["min_runs"]
        results.append({
            "pipeline": pipeline,
            "min_runs": cfg["min_runs"],
            "actual_runs": actual,
            "covered": met,
        })

    total = len(results)
    covered = sum(1 for r in results if r["covered"])
    return {
        "total_expected": total,
        "covered": covered,
        "missing": total - covered,
        "coverage_pct": round(100.0 * covered / total, 1) if total else 100.0,
        "details": results,
    }


def format_coverage_report(report: dict[str, Any]) -> str:
    lines = [
        f"Pipeline Coverage: {report['covered']}/{report['total_expected']} "
        f"({report['coverage_pct']}%)",
        "-" * 44,
    ]
    for d in report["details"]:
        status = "OK " if d["covered"] else "MISS"
        lines.append(
            f"  [{status}] {d['pipeline']:30s}  "
            f"runs={d['actual_runs']}/{d['min_runs']}"
        )
    if not report["details"]:
        lines.append("  (no expected pipelines configured)")
    return "\n".join(lines)
