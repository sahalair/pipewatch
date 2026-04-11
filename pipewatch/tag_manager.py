"""Tag management for pipeline runs — attach, remove, and filter runs by tags."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Dict, List, Optional

TAGS_FILENAME = "tags.json"


def _tags_path(run_dir: str) -> Path:
    return Path(run_dir) / TAGS_FILENAME


def load_tags(run_dir: str) -> Dict[str, List[str]]:
    """Load the tags index from *run_dir*.

    Returns a dict mapping run_id -> list[str].
    """
    path = _tags_path(run_dir)
    if not path.exists():
        return {}
    with path.open("r", encoding="utf-8") as fh:
        return json.load(fh)


def save_tags(run_dir: str, tags: Dict[str, List[str]]) -> None:
    """Persist *tags* index to *run_dir*."""
    path = _tags_path(run_dir)
    Path(run_dir).mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as fh:
        json.dump(tags, fh, indent=2)


def add_tags(run_dir: str, run_id: str, new_tags: List[str]) -> List[str]:
    """Add *new_tags* to *run_id* and return the updated tag list."""
    tags = load_tags(run_dir)
    current = set(tags.get(run_id, []))
    current.update(new_tags)
    tags[run_id] = sorted(current)
    save_tags(run_dir, tags)
    return tags[run_id]


def remove_tags(run_dir: str, run_id: str, remove: List[str]) -> List[str]:
    """Remove *remove* tags from *run_id* and return the updated tag list."""
    tags = load_tags(run_dir)
    current = set(tags.get(run_id, []))
    current -= set(remove)
    tags[run_id] = sorted(current)
    save_tags(run_dir, tags)
    return tags[run_id]


def get_tags(run_dir: str, run_id: str) -> List[str]:
    """Return tags for *run_id* (empty list if none)."""
    return load_tags(run_dir).get(run_id, [])


def filter_runs_by_tag(run_dir: str, tag: str) -> List[str]:
    """Return sorted list of run_ids that carry *tag*."""
    tags = load_tags(run_dir)
    return sorted(rid for rid, t_list in tags.items() if tag in t_list)
