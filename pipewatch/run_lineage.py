"""Track upstream/downstream lineage relationships between pipeline runs."""

import json
from pathlib import Path
from typing import Dict, List, Optional

_LINEAGE_FILE = ".pipewatch/lineage.json"


def _lineage_path(base_dir: str = ".") -> Path:
    return Path(base_dir) / _LINEAGE_FILE


def load_lineage(base_dir: str = ".") -> Dict[str, Dict]:
    """Load the full lineage map from disk."""
    path = _lineage_path(base_dir)
    if not path.exists():
        return {}
    with path.open() as f:
        return json.load(f)


def save_lineage(lineage: Dict[str, Dict], base_dir: str = ".") -> None:
    """Persist the lineage map to disk."""
    path = _lineage_path(base_dir)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w") as f:
        json.dump(lineage, f, indent=2)


def record_lineage(
    run_id: str,
    upstream: Optional[List[str]] = None,
    downstream: Optional[List[str]] = None,
    base_dir: str = ".",
) -> Dict:
    """Record lineage for a run, merging with any existing entry."""
    lineage = load_lineage(base_dir)
    entry = lineage.get(run_id, {"upstream": [], "downstream": []})
    for uid in upstream or []:
        if uid not in entry["upstream"]:
            entry["upstream"].append(uid)
    for did in downstream or []:
        if did not in entry["downstream"]:
            entry["downstream"].append(did)
    lineage[run_id] = entry
    save_lineage(lineage, base_dir)
    return entry


def get_lineage(run_id: str, base_dir: str = ".") -> Dict:
    """Return the lineage entry for a run, or empty lists if not found."""
    lineage = load_lineage(base_dir)
    return lineage.get(run_id, {"upstream": [], "downstream": []})


def get_ancestors(run_id: str, base_dir: str = ".") -> List[str]:
    """Return all transitive upstream ancestors of a run (BFS)."""
    lineage = load_lineage(base_dir)
    visited: List[str] = []
    queue = list(lineage.get(run_id, {}).get("upstream", []))
    while queue:
        current = queue.pop(0)
        if current not in visited:
            visited.append(current)
            queue.extend(lineage.get(current, {}).get("upstream", []))
    return visited


def get_descendants(run_id: str, base_dir: str = ".") -> List[str]:
    """Return all transitive downstream descendants of a run (BFS)."""
    lineage = load_lineage(base_dir)
    visited: List[str] = []
    queue = list(lineage.get(run_id, {}).get("downstream", []))
    while queue:
        current = queue.pop(0)
        if current not in visited:
            visited.append(current)
            queue.extend(lineage.get(current, {}).get("downstream", []))
    return visited


def format_lineage_report(run_id: str, base_dir: str = ".") -> str:
    """Return a human-readable lineage report for a run."""
    entry = get_lineage(run_id, base_dir)
    ancestors = get_ancestors(run_id, base_dir)
    descendants = get_descendants(run_id, base_dir)
    lines = [
        f"Lineage report for run: {run_id}",
        f"  Direct upstream  : {', '.join(entry['upstream']) or 'none'}",
        f"  Direct downstream: {', '.join(entry['downstream']) or 'none'}",
        f"  All ancestors    : {', '.join(ancestors) or 'none'}",
        f"  All descendants  : {', '.join(descendants) or 'none'}",
    ]
    return "\n".join(lines)
