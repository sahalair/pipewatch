"""Watchlist: mark pipeline runs for close monitoring."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Dict, List, Optional

_WATCHLIST_FILE = "watchlist.json"


def _watchlist_path(base_dir: str) -> Path:
    return Path(base_dir) / _WATCHLIST_FILE


def load_watchlist(base_dir: str) -> Dict[str, dict]:
    """Load the watchlist from disk. Returns empty dict if missing."""
    path = _watchlist_path(base_dir)
    if not path.exists():
        return {}
    with path.open() as fh:
        return json.load(fh)


def save_watchlist(base_dir: str, watchlist: Dict[str, dict]) -> None:
    """Persist the watchlist to disk."""
    path = _watchlist_path(base_dir)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w") as fh:
        json.dump(watchlist, fh, indent=2)


def add_to_watchlist(base_dir: str, run_id: str, reason: Optional[str] = None) -> dict:
    """Add a run to the watchlist. Returns the watchlist entry."""
    watchlist = load_watchlist(base_dir)
    entry = {"run_id": run_id, "reason": reason or ""}
    watchlist[run_id] = entry
    save_watchlist(base_dir, watchlist)
    return entry


def remove_from_watchlist(base_dir: str, run_id: str) -> bool:
    """Remove a run from the watchlist. Returns True if it existed."""
    watchlist = load_watchlist(base_dir)
    if run_id not in watchlist:
        return False
    del watchlist[run_id]
    save_watchlist(base_dir, watchlist)
    return True


def is_watched(base_dir: str, run_id: str) -> bool:
    """Return True if the run is on the watchlist."""
    return run_id in load_watchlist(base_dir)


def list_watched_runs(base_dir: str) -> List[dict]:
    """Return all watchlist entries sorted by run_id."""
    watchlist = load_watchlist(base_dir)
    return sorted(watchlist.values(), key=lambda e: e["run_id"])
