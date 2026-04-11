"""Cache layer for run summary data to avoid repeated computation."""

import json
import os
import time
from typing import Any, Dict, Optional

_CACHE_VERSION = 1


def _cache_path(base_dir: str) -> str:
    return os.path.join(base_dir, ".pipewatch_summary_cache.json")


def load_cache(base_dir: str) -> Dict[str, Any]:
    """Load the summary cache from disk. Returns empty cache on missing file."""
    path = _cache_path(base_dir)
    if not os.path.exists(path):
        return {"version": _CACHE_VERSION, "entries": {}}
    with open(path, "r", encoding="utf-8") as fh:
        data = json.load(fh)
    if data.get("version") != _CACHE_VERSION:
        return {"version": _CACHE_VERSION, "entries": {}}
    return data


def save_cache(base_dir: str, cache: Dict[str, Any]) -> None:
    """Persist the summary cache to disk."""
    os.makedirs(base_dir, exist_ok=True)
    path = _cache_path(base_dir)
    with open(path, "w", encoding="utf-8") as fh:
        json.dump(cache, fh, indent=2)


def get_cached_summary(base_dir: str, run_id: str, ttl_seconds: float = 300.0) -> Optional[Dict[str, Any]]:
    """Return cached summary for run_id if present and not expired, else None."""
    cache = load_cache(base_dir)
    entry = cache["entries"].get(run_id)
    if entry is None:
        return None
    age = time.time() - entry.get("cached_at", 0)
    if age > ttl_seconds:
        return None
    return entry.get("summary")


def set_cached_summary(base_dir: str, run_id: str, summary: Dict[str, Any]) -> None:
    """Store a summary in the cache for the given run_id."""
    cache = load_cache(base_dir)
    cache["entries"][run_id] = {
        "summary": summary,
        "cached_at": time.time(),
    }
    save_cache(base_dir, cache)


def invalidate_cached_summary(base_dir: str, run_id: str) -> bool:
    """Remove a single run's summary from the cache. Returns True if it existed."""
    cache = load_cache(base_dir)
    existed = run_id in cache["entries"]
    cache["entries"].pop(run_id, None)
    save_cache(base_dir, cache)
    return existed


def clear_cache(base_dir: str) -> int:
    """Clear all cached summaries. Returns the number of entries removed."""
    cache = load_cache(base_dir)
    count = len(cache["entries"])
    cache["entries"] = {}
    save_cache(base_dir, cache)
    return count
