"""Retry tracking for pipeline runs."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Dict, List, Optional

from pipewatch.run_logger import load_run_record

_RETRY_FILE = "retries.json"


def _retries_path(base_dir: str) -> Path:
    return Path(base_dir) / _RETRY_FILE


def load_retry_map(base_dir: str) -> Dict[str, List[str]]:
    """Load the retry map {original_run_id: [retry_run_id, ...]}."""
    path = _retries_path(base_dir)
    if not path.exists():
        return {}
    with path.open() as fh:
        return json.load(fh)


def save_retry_map(base_dir: str, retry_map: Dict[str, List[str]]) -> None:
    """Persist the retry map to disk."""
    path = _retries_path(base_dir)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w") as fh:
        json.dump(retry_map, fh, indent=2)


def register_retry(base_dir: str, original_run_id: str, retry_run_id: str) -> None:
    """Record that *retry_run_id* is a retry attempt for *original_run_id*."""
    retry_map = load_retry_map(base_dir)
    retry_map.setdefault(original_run_id, []).append(retry_run_id)
    save_retry_map(base_dir, retry_map)


def get_retries(base_dir: str, run_id: str) -> List[str]:
    """Return all retry run IDs for *run_id*, or an empty list."""
    return load_retry_map(base_dir).get(run_id, [])


def get_original_run(base_dir: str, retry_run_id: str) -> Optional[str]:
    """Return the original run ID for a given retry run, or None."""
    for original, retries in load_retry_map(base_dir).items():
        if retry_run_id in retries:
            return original
    return None


def retry_chain(base_dir: str, run_id: str) -> List[dict]:
    """Return ordered list of run records: [original, retry1, retry2, ...].

    Raises FileNotFoundError if any record in the chain is missing.
    """
    retries = get_retries(base_dir, run_id)
    ids = [run_id] + retries
    return [load_run_record(base_dir, rid) for rid in ids]
