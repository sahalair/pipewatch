"""Bookmark management for pipeline runs."""

import json
from pathlib import Path
from typing import Dict, List, Optional

_BOOKMARKS_FILE = Path(".pipewatch") / "bookmarks.json"


def _bookmarks_path(base_dir: Optional[Path] = None) -> Path:
    root = base_dir or Path(".")
    return root / ".pipewatch" / "bookmarks.json"


def load_bookmarks(base_dir: Optional[Path] = None) -> Dict[str, str]:
    """Load bookmarks mapping alias -> run_id."""
    path = _bookmarks_path(base_dir)
    if not path.exists():
        return {}
    with path.open("r") as f:
        return json.load(f)


def save_bookmarks(bookmarks: Dict[str, str], base_dir: Optional[Path] = None) -> None:
    """Persist bookmarks to disk."""
    path = _bookmarks_path(base_dir)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w") as f:
        json.dump(bookmarks, f, indent=2)


def add_bookmark(alias: str, run_id: str, base_dir: Optional[Path] = None) -> Dict[str, str]:
    """Add or overwrite a bookmark alias pointing to run_id."""
    bookmarks = load_bookmarks(base_dir)
    bookmarks[alias] = run_id
    save_bookmarks(bookmarks, base_dir)
    return bookmarks


def remove_bookmark(alias: str, base_dir: Optional[Path] = None) -> bool:
    """Remove a bookmark by alias. Returns True if removed, False if not found."""
    bookmarks = load_bookmarks(base_dir)
    if alias not in bookmarks:
        return False
    del bookmarks[alias]
    save_bookmarks(bookmarks, base_dir)
    return True


def resolve_bookmark(alias: str, base_dir: Optional[Path] = None) -> Optional[str]:
    """Return the run_id for a given alias, or None if not found."""
    bookmarks = load_bookmarks(base_dir)
    return bookmarks.get(alias)


def list_bookmarks(base_dir: Optional[Path] = None) -> List[Dict[str, str]]:
    """Return a sorted list of {alias, run_id} dicts."""
    bookmarks = load_bookmarks(base_dir)
    return [
        {"alias": alias, "run_id": run_id}
        for alias, run_id in sorted(bookmarks.items())
    ]
