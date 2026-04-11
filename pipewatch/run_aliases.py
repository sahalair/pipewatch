"""Manage human-readable aliases for run IDs."""

import json
from pathlib import Path
from typing import Dict, Optional

_ALIASES_FILE = "aliases.json"


def _aliases_path(base_dir: str) -> Path:
    return Path(base_dir) / _ALIASES_FILE


def load_aliases(base_dir: str) -> Dict[str, str]:
    """Load alias->run_id mapping from disk."""
    path = _aliases_path(base_dir)
    if not path.exists():
        return {}
    with open(path, "r") as f:
        return json.load(f)


def save_aliases(base_dir: str, aliases: Dict[str, str]) -> None:
    """Persist alias->run_id mapping to disk."""
    path = _aliases_path(base_dir)
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w") as f:
        json.dump(aliases, f, indent=2)


def set_alias(base_dir: str, alias: str, run_id: str) -> None:
    """Create or overwrite an alias pointing to run_id."""
    aliases = load_aliases(base_dir)
    aliases[alias] = run_id
    save_aliases(base_dir, aliases)


def remove_alias(base_dir: str, alias: str) -> bool:
    """Remove an alias. Returns True if it existed, False otherwise."""
    aliases = load_aliases(base_dir)
    if alias not in aliases:
        return False
    del aliases[alias]
    save_aliases(base_dir, aliases)
    return True


def resolve_alias(base_dir: str, alias: str) -> Optional[str]:
    """Return the run_id for the given alias, or None if not found."""
    return load_aliases(base_dir).get(alias)


def list_aliases(base_dir: str) -> Dict[str, str]:
    """Return all alias->run_id mappings."""
    return load_aliases(base_dir)


def find_aliases_for_run(base_dir: str, run_id: str) -> list:
    """Return all aliases that point to the given run_id."""
    aliases = load_aliases(base_dir)
    return [alias for alias, rid in aliases.items() if rid == run_id]
