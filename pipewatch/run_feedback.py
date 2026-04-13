"""User feedback and ratings for pipeline runs."""

import json
import os
from typing import Optional

VALID_RATINGS = {1, 2, 3, 4, 5}
_FEEDBACK_FILE = "feedback.json"


def _feedback_path(base_dir: str) -> str:
    return os.path.join(base_dir, _FEEDBACK_FILE)


def load_feedback(base_dir: str) -> dict:
    path = _feedback_path(base_dir)
    if not os.path.exists(path):
        return {}
    with open(path, "r") as f:
        return json.load(f)


def save_feedback(base_dir: str, data: dict) -> None:
    os.makedirs(base_dir, exist_ok=True)
    with open(_feedback_path(base_dir), "w") as f:
        json.dump(data, f, indent=2)


def add_feedback(
    base_dir: str,
    run_id: str,
    rating: int,
    comment: Optional[str] = None,
    author: Optional[str] = None,
) -> dict:
    if rating not in VALID_RATINGS:
        raise ValueError(f"Rating must be one of {sorted(VALID_RATINGS)}, got {rating}")
    if not run_id:
        raise ValueError("run_id must not be empty")
    data = load_feedback(base_dir)
    entry = {"rating": rating, "comment": comment, "author": author}
    data[run_id] = entry
    save_feedback(base_dir, data)
    return entry


def get_feedback(base_dir: str, run_id: str) -> Optional[dict]:
    return load_feedback(base_dir).get(run_id)


def remove_feedback(base_dir: str, run_id: str) -> bool:
    data = load_feedback(base_dir)
    if run_id not in data:
        return False
    del data[run_id]
    save_feedback(base_dir, data)
    return True


def average_rating(base_dir: str, pipeline: Optional[str] = None, runs: Optional[list] = None) -> Optional[float]:
    """Compute average rating, optionally filtered to a list of run_ids."""
    data = load_feedback(base_dir)
    if runs is not None:
        ratings = [data[r]["rating"] for r in runs if r in data]
    else:
        ratings = [v["rating"] for v in data.values()]
    if not ratings:
        return None
    return round(sum(ratings) / len(ratings), 2)
