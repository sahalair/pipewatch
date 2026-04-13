"""Summarise feedback across runs for a pipeline."""

from typing import Optional
from pipewatch.run_feedback import load_feedback, average_rating


def _star_bar(rating: float, width: int = 5) -> str:
    filled = round(rating)
    return "★" * filled + "☆" * (width - filled)


def build_feedback_summary(base_dir: str, runs: list) -> dict:
    """Return aggregated feedback stats for a list of run_ids."""
    data = load_feedback(base_dir)
    rated = [r for r in runs if r in data]
    unrated = [r for r in runs if r not in data]
    ratings = [data[r]["rating"] for r in rated]

    distribution = {i: 0 for i in range(1, 6)}
    for r in ratings:
        distribution[r] += 1

    avg = round(sum(ratings) / len(ratings), 2) if ratings else None

    return {
        "total_runs": len(runs),
        "rated_runs": len(rated),
        "unrated_runs": len(unrated),
        "average_rating": avg,
        "distribution": distribution,
        "entries": {r: data[r] for r in rated},
    }


def format_feedback_summary(summary: dict, pipeline: Optional[str] = None) -> str:
    lines = []
    header = f"Feedback Summary"
    if pipeline:
        header += f" — {pipeline}"
    lines.append(header)
    lines.append("-" * len(header))
    lines.append(f"Total runs : {summary['total_runs']}")
    lines.append(f"Rated      : {summary['rated_runs']}")
    lines.append(f"Unrated    : {summary['unrated_runs']}")

    avg = summary["average_rating"]
    if avg is not None:
        lines.append(f"Avg rating : {avg}/5  {_star_bar(avg)}")
    else:
        lines.append("Avg rating : N/A")

    lines.append("Distribution:")
    for star in range(5, 0, -1):
        count = summary["distribution"].get(star, 0)
        bar = "#" * count
        lines.append(f"  {star}★  {bar} ({count})")

    return "\n".join(lines)
