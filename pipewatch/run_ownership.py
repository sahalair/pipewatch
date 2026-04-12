"""Ownership management for pipeline runs."""

import json
from pathlib import Path
from typing import Dict, List, Optional

_DEFAULT_DIR = Path(".pipewatch/ownership")


def _ownership_path(base_dir: Path = _DEFAULT_DIR) -> Path:
    base_dir.mkdir(parents=True, exist_ok=True)
    return base_dir / "owners.json"


def load_ownership(base_dir: Path = _DEFAULT_DIR) -> Dict[str, Dict]:
    """Load ownership records from disk."""
    path = _ownership_path(base_dir)
    if not path.exists():
        return {}
    with open(path) as f:
        return json.load(f)


def save_ownership(data: Dict[str, Dict], base_dir: Path = _DEFAULT_DIR) -> None:
    """Persist ownership records to disk."""
    path = _ownership_path(base_dir)
    with open(path, "w") as f:
        json.dump(data, f, indent=2)


def set_owner(
    run_id: str,
    owner: str,
    team: Optional[str] = None,
    contact: Optional[str] = None,
    base_dir: Path = _DEFAULT_DIR,
) -> Dict:
    """Set or update the owner for a run."""
    data = load_ownership(base_dir)
    record = {"owner": owner, "team": team, "contact": contact}
    data[run_id] = {k: v for k, v in record.items() if v is not None}
    save_ownership(data, base_dir)
    return data[run_id]


def get_owner(run_id: str, base_dir: Path = _DEFAULT_DIR) -> Optional[Dict]:
    """Return ownership info for a run, or None if not set."""
    data = load_ownership(base_dir)
    return data.get(run_id)


def remove_owner(run_id: str, base_dir: Path = _DEFAULT_DIR) -> bool:
    """Remove ownership record for a run. Returns True if removed."""
    data = load_ownership(base_dir)
    if run_id not in data:
        return False
    del data[run_id]
    save_ownership(data, base_dir)
    return True


def list_owned_runs(
    owner: Optional[str] = None,
    team: Optional[str] = None,
    base_dir: Path = _DEFAULT_DIR,
) -> List[Dict]:
    """List runs optionally filtered by owner or team."""
    data = load_ownership(base_dir)
    results = []
    for run_id, info in data.items():
        if owner and info.get("owner") != owner:
            continue
        if team and info.get("team") != team:
            continue
        results.append({"run_id": run_id, **info})
    return results
