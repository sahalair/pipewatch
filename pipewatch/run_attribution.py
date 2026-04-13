"""Track and query the attribution (trigger source) for pipeline runs."""

import json
from pathlib import Path
from typing import Dict, List, Optional

_VALID_SOURCES = {"manual", "scheduled", "webhook", "ci", "retry", "unknown"}


def _attribution_path(base_dir: str) -> Path:
    return Path(base_dir) / "attribution.json"


def load_attribution(base_dir: str) -> Dict[str, dict]:
    """Load attribution map from disk. Returns empty dict if missing."""
    path = _attribution_path(base_dir)
    if not path.exists():
        return {}
    with path.open() as fh:
        return json.load(fh)


def save_attribution(base_dir: str, data: Dict[str, dict]) -> None:
    """Persist attribution map to disk."""
    path = _attribution_path(base_dir)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w") as fh:
        json.dump(data, fh, indent=2)


def set_attribution(
    base_dir: str,
    run_id: str,
    source: str,
    triggered_by: Optional[str] = None,
    note: Optional[str] = None,
) -> dict:
    """Record attribution for a run. Source must be one of the valid values."""
    if source not in _VALID_SOURCES:
        raise ValueError(
            f"Invalid source '{source}'. Must be one of: {sorted(_VALID_SOURCES)}"
        )
    if not run_id or not run_id.strip():
        raise ValueError("run_id must not be empty")

    data = load_attribution(base_dir)
    entry: dict = {"source": source}
    if triggered_by:
        entry["triggered_by"] = triggered_by
    if note:
        entry["note"] = note
    data[run_id] = entry
    save_attribution(base_dir, data)
    return entry


def get_attribution(base_dir: str, run_id: str) -> Optional[dict]:
    """Return attribution entry for a run, or None if not recorded."""
    return load_attribution(base_dir).get(run_id)


def filter_by_source(base_dir: str, source: str) -> List[str]:
    """Return list of run_ids whose attribution source matches."""
    data = load_attribution(base_dir)
    return [rid for rid, entry in data.items() if entry.get("source") == source]


def format_attribution(run_id: str, entry: dict) -> str:
    """Format a single attribution entry for display."""
    parts = [f"run: {run_id}", f"source: {entry.get('source', 'unknown')}"]
    if "triggered_by" in entry:
        parts.append(f"triggered_by: {entry['triggered_by']}")
    if "note" in entry:
        parts.append(f"note: {entry['note']}")
    return "  ".join(parts)
