"""Track and query dependencies between pipeline runs."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Dict, List, Optional

_DEPS_FILENAME = "run_dependencies.json"


def _deps_path(base_dir: str = ".pipewatch") -> Path:
    return Path(base_dir) / _DEPS_FILENAME


def load_dependencies(base_dir: str = ".pipewatch") -> Dict[str, List[str]]:
    """Load the dependency map {run_id: [upstream_run_id, ...]}."""
    path = _deps_path(base_dir)
    if not path.exists():
        return {}
    with path.open() as fh:
        return json.load(fh)


def save_dependencies(deps: Dict[str, List[str]], base_dir: str = ".pipewatch") -> None:
    """Persist the dependency map to disk."""
    path = _deps_path(base_dir)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w") as fh:
        json.dump(deps, fh, indent=2)


def add_dependency(run_id: str, upstream_id: str, base_dir: str = ".pipewatch") -> None:
    """Record that *run_id* depends on *upstream_id*."""
    deps = load_dependencies(base_dir)
    deps.setdefault(run_id, [])
    if upstream_id not in deps[run_id]:
        deps[run_id].append(upstream_id)
    save_dependencies(deps, base_dir)


def remove_dependency(run_id: str, upstream_id: str, base_dir: str = ".pipewatch") -> bool:
    """Remove a single upstream dependency.  Returns True if it existed."""
    deps = load_dependencies(base_dir)
    if run_id not in deps or upstream_id not in deps[run_id]:
        return False
    deps[run_id].remove(upstream_id)
    if not deps[run_id]:
        del deps[run_id]
    save_dependencies(deps, base_dir)
    return True


def get_dependencies(run_id: str, base_dir: str = ".pipewatch") -> List[str]:
    """Return the list of upstream run IDs for *run_id*."""
    return load_dependencies(base_dir).get(run_id, [])


def get_dependents(run_id: str, base_dir: str = ".pipewatch") -> List[str]:
    """Return all run IDs that list *run_id* as an upstream dependency."""
    deps = load_dependencies(base_dir)
    return [rid for rid, upstreams in deps.items() if run_id in upstreams]
