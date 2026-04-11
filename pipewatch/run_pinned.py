"""Manage pinned runs — runs marked as important reference points."""

import json
from pathlib import Path
from typing import Dict, List, Optional

_PINNED_FILE = ".pipewatch/pinned.json"


def _pinned_path(base_dir: str = ".") -> Path:
    return Path(base_dir) / _PINNED_FILE


def load_pinned(base_dir: str = ".") -> Dict[str, str]:
    """Load pinned runs. Returns a dict mapping label -> run_id."""
    path = _pinned_path(base_dir)
    if not path.exists():
        return {}
    with open(path, "r") as f:
        return json.load(f)


def save_pinned(pinned: Dict[str, str], base_dir: str = ".") -> None:
    """Persist the pinned runs map to disk."""
    path = _pinned_path(base_dir)
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w") as f:
        json.dump(pinned, f, indent=2)


def pin_run(label: str, run_id: str, base_dir: str = ".") -> Dict[str, str]:
    """Pin a run under the given label. Overwrites any existing pin with that label."""
    pinned = load_pinned(base_dir)
    pinned[label] = run_id
    save_pinned(pinned, base_dir)
    return pinned


def unpin_run(label: str, base_dir: str = ".") -> bool:
    """Remove a pin by label. Returns True if removed, False if label not found."""
    pinned = load_pinned(base_dir)
    if label not in pinned:
        return False
    del pinned[label]
    save_pinned(pinned, base_dir)
    return True


def resolve_pin(label: str, base_dir: str = ".") -> Optional[str]:
    """Return the run_id for a given pin label, or None if not found."""
    return load_pinned(base_dir).get(label)


def list_pins(base_dir: str = ".") -> List[Dict[str, str]]:
    """Return a sorted list of {label, run_id} dicts."""
    pinned = load_pinned(base_dir)
    return [{"label": k, "run_id": v} for k, v in sorted(pinned.items())]
