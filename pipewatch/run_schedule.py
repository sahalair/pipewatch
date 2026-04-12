"""Run schedule tracking: record expected intervals and detect overdue pipelines."""

from __future__ import annotations

import json
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Any

_SCHEDULES_FILE = Path(".pipewatch") / "schedules.json"


def _schedules_path() -> Path:
    return _SCHEDULES_FILE


def load_schedules(path: Path | None = None) -> dict[str, Any]:
    """Load all schedule rules from disk. Returns {} if missing."""
    p = path or _schedules_path()
    if not p.exists():
        return {}
    with p.open() as f:
        return json.load(f)


def save_schedules(schedules: dict[str, Any], path: Path | None = None) -> None:
    """Persist schedule rules to disk."""
    p = path or _schedules_path()
    p.parent.mkdir(parents=True, exist_ok=True)
    with p.open("w") as f:
        json.dump(schedules, f, indent=2)


def set_schedule(pipeline: str, interval_minutes: int, path: Path | None = None) -> dict[str, Any]:
    """Set or update the expected run interval for a pipeline."""
    if interval_minutes <= 0:
        raise ValueError("interval_minutes must be a positive integer")
    schedules = load_schedules(path)
    schedules[pipeline] = {
        "pipeline": pipeline,
        "interval_minutes": interval_minutes,
    }
    save_schedules(schedules, path)
    return schedules[pipeline]


def remove_schedule(pipeline: str, path: Path | None = None) -> bool:
    """Remove the schedule for a pipeline. Returns True if it existed."""
    schedules = load_schedules(path)
    if pipeline not in schedules:
        return False
    del schedules[pipeline]
    save_schedules(schedules, path)
    return True


def check_overdue(
    pipeline: str,
    last_run_iso: str | None,
    path: Path | None = None,
) -> dict[str, Any]:
    """Check whether a pipeline is overdue based on its schedule.

    Returns a dict with keys: pipeline, scheduled, overdue, minutes_overdue.
    """
    schedules = load_schedules(path)
    if pipeline not in schedules:
        return {"pipeline": pipeline, "scheduled": False, "overdue": False, "minutes_overdue": 0}

    interval = schedules[pipeline]["interval_minutes"]
    now = datetime.now(timezone.utc)

    if last_run_iso is None:
        return {"pipeline": pipeline, "scheduled": True, "overdue": True, "minutes_overdue": interval}

    last_run = datetime.fromisoformat(last_run_iso)
    if last_run.tzinfo is None:
        last_run = last_run.replace(tzinfo=timezone.utc)

    deadline = last_run + timedelta(minutes=interval)
    overdue = now > deadline
    minutes_overdue = max(0, int((now - deadline).total_seconds() / 60)) if overdue else 0

    return {
        "pipeline": pipeline,
        "scheduled": True,
        "overdue": overdue,
        "minutes_overdue": minutes_overdue,
    }


def check_all_overdue(
    last_runs: dict[str, str | None],
    path: Path | None = None,
) -> list[dict[str, Any]]:
    """Check overdue status for all scheduled pipelines.

    Args:
        last_runs: Mapping of pipeline name to its last run ISO timestamp (or None).
        path: Optional path to the schedules file.

    Returns:
        A list of overdue-check result dicts for every scheduled pipeline.
        Pipelines present in schedules but absent from ``last_runs`` are treated
        as never having run (i.e. last_run_iso=None).
    """
    schedules = load_schedules(path)
    return [
        check_overdue(pipeline, last_runs.get(pipeline), path)
        for pipeline in schedules
    ]
