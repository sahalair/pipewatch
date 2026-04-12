"""Run reputation scoring based on historical reliability."""

from typing import Optional
from pipewatch.run_flakiness import analyze_pipeline_flakiness
from pipewatch.run_stability import compute_stability_score
from pipewatch.run_sla import load_sla_rules


REPUTATION_GRADES = [
    (90, "excellent"),
    (75, "good"),
    (55, "fair"),
    (35, "poor"),
    (0,  "critical"),
]


def compute_reputation(
    pipeline: str,
    base_dir: str = ".",
    limit: int = 20,
) -> dict:
    """Compute a reputation record for a pipeline."""
    flakiness = analyze_pipeline_flakiness(pipeline, base_dir=base_dir, limit=limit)
    stability = compute_stability_score(pipeline, base_dir=base_dir, limit=limit)

    flakiness_penalty = min(flakiness.get("score", 0.0) * 40, 40)
    stability_score = stability.get("score", 100.0)
    raw_score = max(0.0, stability_score - flakiness_penalty)
    score = round(raw_score, 2)

    grade = "critical"
    for threshold, label in REPUTATION_GRADES:
        if score >= threshold:
            grade = label
            break

    return {
        "pipeline": pipeline,
        "score": score,
        "grade": grade,
        "flakiness_score": flakiness.get("score", 0.0),
        "flakiness_label": flakiness.get("label", "unknown"),
        "stability_score": stability_score,
        "stability_grade": stability.get("grade", "?"),
        "run_count": flakiness.get("run_count", 0),
    }


def format_reputation_report(rep: dict) -> str:
    """Format a reputation record as a human-readable string."""
    lines = [
        f"Pipeline Reputation: {rep['pipeline']}",
        f"  Reputation Score : {rep['score']:.1f} / 100  [{rep['grade'].upper()}]",
        f"  Stability Score  : {rep['stability_score']:.1f}  (grade: {rep['stability_grade']})",
        f"  Flakiness Score  : {rep['flakiness_score']:.2f}  ({rep['flakiness_label']})",
        f"  Runs Analysed    : {rep['run_count']}",
    ]
    return "\n".join(lines)
