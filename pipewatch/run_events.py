"""Run event log: record and retrieve structured events for a run."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from pipewatch.run_logger import _now_iso

_EVENTS_DIR = Path(".pipewatch") / "events"


def _events_path(run_id: str) -> Path:
    return _EVENTS_DIR / f"{run_id}.json"


def load_events(run_id: str) -> list[dict[str, Any]]:
    """Return all events recorded for *run_id*, oldest first."""
    path = _events_path(run_id)
    if not path.exists():
        return []
    return json.loads(path.read_text())


def save_events(run_id: str, events: list[dict[str, Any]]) -> None:
    """Persist *events* for *run_id*."""
    _EVENTS_DIR.mkdir(parents=True, exist_ok=True)
    _events_path(run_id).write_text(json.dumps(events, indent=2))


def record_event(
    run_id: str,
    event_type: str,
    message: str,
    *,
    level: str = "info",
    data: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Append a new event to the log for *run_id* and return it."""
    valid_levels = {"info", "warning", "error", "debug"}
    if level not in valid_levels:
        raise ValueError(f"level must be one of {valid_levels}, got {level!r}")

    event: dict[str, Any] = {
        "run_id": run_id,
        "timestamp": _now_iso(),
        "event_type": event_type,
        "level": level,
        "message": message,
        "data": data or {},
    }
    events = load_events(run_id)
    events.append(event)
    save_events(run_id, events)
    return event


def get_events(
    run_id: str,
    *,
    level: str | None = None,
    event_type: str | None = None,
) -> list[dict[str, Any]]:
    """Return events for *run_id*, optionally filtered by level/event_type."""
    events = load_events(run_id)
    if level:
        events = [e for e in events if e["level"] == level]
    if event_type:
        events = [e for e in events if e["event_type"] == event_type]
    return events


def clear_events(run_id: str) -> int:
    """Delete all events for *run_id*. Returns the number of events removed."""
    path = _events_path(run_id)
    if not path.exists():
        return 0
    count = len(load_events(run_id))
    path.unlink()
    return count


def format_events(events: list[dict[str, Any]]) -> str:
    """Return a human-readable string for *events*."""
    if not events:
        return "(no events)"
    lines = []
    for e in events:
        tag = f"[{e['level'].upper()}]"
        lines.append(f"{e['timestamp']} {tag} {e['event_type']}: {e['message']}")
    return "\n".join(lines)
