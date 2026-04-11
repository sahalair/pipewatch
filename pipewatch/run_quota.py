"""Run quota management: enforce limits on stored runs per pipeline."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Dict, Optional

DEFAULT_QUOTA_FILE = Path(".pipewatch") / "quotas.json"
DEFAULT_QUOTA = 100


def _quota_path(base_dir: Path = DEFAULT_QUOTA_FILE) -> Path:
    return base_dir


def load_quotas(quota_file: Path = DEFAULT_QUOTA_FILE) -> Dict[str, int]:
    """Load quota rules from disk. Returns empty dict if file missing."""
    if not quota_file.exists():
        return {}
    with quota_file.open() as fh:
        return json.load(fh)


def save_quotas(quotas: Dict[str, int], quota_file: Path = DEFAULT_QUOTA_FILE) -> None:
    """Persist quota rules to disk."""
    quota_file.parent.mkdir(parents=True, exist_ok=True)
    with quota_file.open("w") as fh:
        json.dump(quotas, fh, indent=2)


def set_quota(pipeline: str, limit: int, quota_file: Path = DEFAULT_QUOTA_FILE) -> Dict[str, int]:
    """Set or update the run quota for a pipeline."""
    if limit < 1:
        raise ValueError(f"Quota limit must be >= 1, got {limit}")
    quotas = load_quotas(quota_file)
    quotas[pipeline] = limit
    save_quotas(quotas, quota_file)
    return quotas


def remove_quota(pipeline: str, quota_file: Path = DEFAULT_QUOTA_FILE) -> bool:
    """Remove the quota rule for a pipeline. Returns True if removed."""
    quotas = load_quotas(quota_file)
    if pipeline not in quotas:
        return False
    del quotas[pipeline]
    save_quotas(quotas, quota_file)
    return True


def get_quota(pipeline: str, quota_file: Path = DEFAULT_QUOTA_FILE) -> int:
    """Return the quota limit for a pipeline, or the default."""
    quotas = load_quotas(quota_file)
    return quotas.get(pipeline, DEFAULT_QUOTA)


def check_quota(
    pipeline: str,
    current_count: int,
    quota_file: Path = DEFAULT_QUOTA_FILE,
) -> Dict[str, object]:
    """Check whether a pipeline has exceeded its quota.

    Returns a dict with keys: pipeline, limit, current, exceeded.
    """
    limit = get_quota(pipeline, quota_file)
    exceeded = current_count >= limit
    return {
        "pipeline": pipeline,
        "limit": limit,
        "current": current_count,
        "exceeded": exceeded,
    }
