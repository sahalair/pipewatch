"""Utility to resolve a run identifier that may be an alias or a direct run ID."""

from typing import Optional

from pipewatch.run_aliases import resolve_alias
from pipewatch.run_logger import load_run_record


def resolve_run_id(base_dir: str, identifier: str) -> Optional[str]:
    """Return a verified run_id from either a direct ID or an alias.

    Resolution order:
      1. Try to load the identifier as a direct run_id.
      2. Try to resolve it as an alias and load the resulting run_id.

    Returns the run_id string if found, or None.
    """
    # Direct run ID check
    try:
        record = load_run_record(base_dir, identifier)
        if record:
            return identifier
    except (FileNotFoundError, KeyError, ValueError):
        pass

    # Alias resolution
    resolved = resolve_alias(base_dir, identifier)
    if resolved is None:
        return None

    try:
        record = load_run_record(base_dir, resolved)
        if record:
            return resolved
    except (FileNotFoundError, KeyError, ValueError):
        pass

    return None


def require_run_id(base_dir: str, identifier: str) -> str:
    """Like resolve_run_id but raises ValueError if not found."""
    run_id = resolve_run_id(base_dir, identifier)
    if run_id is None:
        raise ValueError(
            f"Could not resolve '{identifier}' to a known run ID or alias."
        )
    return run_id
