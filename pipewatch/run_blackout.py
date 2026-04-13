"""Blackout window management — define time windows during which pipeline runs should be suppressed."""

import json
from pathlib import Path
from typing import Optional
from datetime import datetime, timezone

_BLACKOUT_FILE = ".pipewatch/blackout_windows.json"


def _blackout_path(base_dir: str = ".") -> Path:
    return Path(base_dir) / ".pipewatch" / "blackout_windows.json"


def load_blackout_windows(base_dir: str = ".") -> dict:
    path = _blackout_path(base_dir)
    if not path.exists():
        return {}
    with open(path) as f:
        return json.load(f)


def save_blackout_windows(windows: dict, base_dir: str = ".") -> None:
    path = _blackout_path(base_dir)
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w") as f:
        json.dump(windows, f, indent=2)


def add_blackout_window(
    name: str,
    start_time: str,
    end_time: str,
    pipelines: Optional[list] = None,
    reason: str = "",
    base_dir: str = ".",
) -> dict:
    """Add or overwrite a named blackout window."""
    if not name:
        raise ValueError("Blackout window name must not be empty.")
    windows = load_blackout_windows(base_dir)
    entry = {
        "name": name,
        "start_time": start_time,
        "end_time": end_time,
        "pipelines": pipelines or [],
        "reason": reason,
    }
    windows[name] = entry
    save_blackout_windows(windows, base_dir)
    return entry


def remove_blackout_window(name: str, base_dir: str = ".") -> bool:
    windows = load_blackout_windows(base_dir)
    if name not in windows:
        return False
    del windows[name]
    save_blackout_windows(windows, base_dir)
    return True


def is_in_blackout(
    pipeline: str,
    at: Optional[str] = None,
    base_dir: str = ".",
) -> bool:
    """Return True if the given pipeline is currently in a blackout window."""
    windows = load_blackout_windows(base_dir)
    if not windows:
        return False
    now_str = at or datetime.now(timezone.utc).isoformat()
    now = datetime.fromisoformat(now_str.replace("Z", "+00:00"))
    for window in windows.values():
        pipelines = window.get("pipelines", [])
        if pipelines and pipeline not in pipelines:
            continue
        try:
            start = datetime.fromisoformat(window["start_time"].replace("Z", "+00:00"))
            end = datetime.fromisoformat(window["end_time"].replace("Z", "+00:00"))
        except (KeyError, ValueError):
            continue
        if start <= now <= end:
            return True
    return False


def list_blackout_windows(base_dir: str = ".") -> list:
    windows = load_blackout_windows(base_dir)
    return sorted(windows.values(), key=lambda w: w.get("start_time", ""))
