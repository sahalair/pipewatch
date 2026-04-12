"""Run maturity scoring: how 'mature' (stable, well-observed) a pipeline is."""

from typing import Optional
from pipewatch.run_logger import list_run_records
from pipewatch.run_stability import compute_stability_score
from pipewatch.run_baseline import get_baseline
from pipewatch.run_annotations import get_annotations
from pipewatch.run_ownership import get_owner


MATURITY_LEVELS = [
    (90, "platinum"),
    (75, "gold"),
    (55, "silver"),
    (35, "bronze"),
    (0,  "emerging"),
]


def _grade(score: float) -> str:
    for threshold, label in MATURITY_LEVELS:
        if score >= threshold:
            return label
    return "emerging"


def compute_maturity(pipeline: str, base_dir: str = ".pipewatch") -> dict:
    """Compute a maturity score for a pipeline (0-100)."""
    records = [
        r for r in list_run_records(base_dir=base_dir)
        if r.get("pipeline") == pipeline
    ]
    run_count = len(records)

    # Component 1: volume of runs (max 25 pts, saturates at 50 runs)
    volume_score = min(run_count / 50.0, 1.0) * 25.0

    # Component 2: stability (max 50 pts)
    stability_score = 0.0
    if run_count >= 2:
        try:
            stab = compute_stability_score(pipeline, base_dir=base_dir)
            stability_score = stab.get("score", 0.0) * 0.50
        except Exception:
            pass

    # Component 3: has baseline defined (10 pts)
    baseline = get_baseline(pipeline, base_dir=base_dir)
    baseline_score = 10.0 if baseline else 0.0

    # Component 4: has owner defined (10 pts)
    owner = get_owner(pipeline, base_dir=base_dir)
    owner_score = 10.0 if owner else 0.0

    # Component 5: has annotations (5 pts)
    annotations = get_annotations(pipeline, base_dir=base_dir)
    annotation_score = 5.0 if annotations else 0.0

    total = volume_score + stability_score + baseline_score + owner_score + annotation_score
    total = round(min(total, 100.0), 2)

    return {
        "pipeline": pipeline,
        "score": total,
        "grade": _grade(total),
        "run_count": run_count,
        "components": {
            "volume": round(volume_score, 2),
            "stability": round(stability_score, 2),
            "baseline": baseline_score,
            "ownership": owner_score,
            "annotations": annotation_score,
        },
    }


def format_maturity_report(report: dict) -> str:
    lines = [
        f"Pipeline : {report['pipeline']}",
        f"Score    : {report['score']:.1f} / 100  [{report['grade'].upper()}]",
        f"Runs     : {report['run_count']}",
        "Components:",
    ]
    for key, val in report["components"].items():
        lines.append(f"  {key:<12}: {val:.1f}")
    return "\n".join(lines)
