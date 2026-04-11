"""Manage favorite (starred) runs for quick access."""

import json
from pathlib import Path
from typing import Dict, List, Optional

_FAVORITES_FILE = ".pipewatch/favorites.json"


def _favorites_path(base_dir: str = ".") -> Path:
    return Path(base_dir) / _FAVORITES_FILE


def load_favorites(base_dir: str = ".") -> Dict[str, dict]:
    """Load all favorited runs. Returns {run_id: {label, note}}."""
    path = _favorites_path(base_dir)
    if not path.exists():
        return {}
    with open(path, "r") as f:
        return json.load(f)


def save_favorites(favorites: Dict[str, dict], base_dir: str = ".") -> None:
    """Persist favorites to disk."""
    path = _favorites_path(base_dir)
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w") as f:
        json.dump(favorites, f, indent=2)


def add_favorite(run_id: str, label: str = "", note: str = "", base_dir: str = ".") -> dict:
    """Star a run, optionally with a label and note."""
    favorites = load_favorites(base_dir)
    entry = {"run_id": run_id, "label": label, "note": note}
    favorites[run_id] = entry
    save_favorites(favorites, base_dir)
    return entry


def remove_favorite(run_id: str, base_dir: str = ".") -> bool:
    """Unstar a run. Returns True if it existed, False otherwise."""
    favorites = load_favorites(base_dir)
    if run_id not in favorites:
        return False
    del favorites[run_id]
    save_favorites(favorites, base_dir)
    return True


def is_favorite(run_id: str, base_dir: str = ".") -> bool:
    """Check if a run is currently starred."""
    return run_id in load_favorites(base_dir)


def list_favorites(base_dir: str = ".") -> List[dict]:
    """Return all favorites as a sorted list of entries."""
    favorites = load_favorites(base_dir)
    return sorted(favorites.values(), key=lambda e: e["run_id"])


def find_favorites_by_label(label: str, base_dir: str = ".") -> List[dict]:
    """Return favorites matching a given label."""
    return [e for e in list_favorites(base_dir) if e.get("label") == label]
