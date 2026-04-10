"""Snapshot management for pipewatch pipeline outputs."""

import json
import os
from pathlib import Path
from typing import Any, Optional

DEFAULT_SNAPSHOT_DIR = ".pipewatch/snapshots"


def _snapshot_path(name: str, snapshot_dir: str = DEFAULT_SNAPSHOT_DIR) -> Path:
    """Return the full path for a named snapshot file."""
    return Path(snapshot_dir) / f"{name}.json"


def save_snapshot(
    name: str,
    data: Any,
    metadata: Optional[dict] = None,
    snapshot_dir: str = DEFAULT_SNAPSHOT_DIR,
) -> Path:
    """Persist a snapshot of pipeline output data to disk.

    Args:
        name: Unique identifier for this snapshot.
        data: JSON-serializable pipeline output to snapshot.
        metadata: Optional dict of extra info (e.g. run_id, timestamp).
        snapshot_dir: Directory to store snapshots in.

    Returns:
        Path to the saved snapshot file.
    """
    path = _snapshot_path(name, snapshot_dir)
    path.parent.mkdir(parents=True, exist_ok=True)

    record = {"name": name, "data": data, "metadata": metadata or {}}
    with open(path, "w", encoding="utf-8") as f:
        json.dump(record, f, indent=2, default=str)

    return path


def load_snapshot(name: str, snapshot_dir: str = DEFAULT_SNAPSHOT_DIR) -> dict:
    """Load a previously saved snapshot by name.

    Raises:
        FileNotFoundError: If no snapshot with that name exists.
    """
    path = _snapshot_path(name, snapshot_dir)
    if not path.exists():
        raise FileNotFoundError(f"No snapshot found for '{name}' at {path}")

    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def list_snapshots(snapshot_dir: str = DEFAULT_SNAPSHOT_DIR) -> list[str]:
    """Return a sorted list of all snapshot names in the snapshot directory."""
    directory = Path(snapshot_dir)
    if not directory.exists():
        return []
    return sorted(p.stem for p in directory.glob("*.json"))


def delete_snapshot(name: str, snapshot_dir: str = DEFAULT_SNAPSHOT_DIR) -> bool:
    """Delete a snapshot by name. Returns True if deleted, False if not found."""
    path = _snapshot_path(name, snapshot_dir)
    if path.exists():
        os.remove(path)
        return True
    return False


def snapshot_exists(name: str, snapshot_dir: str = DEFAULT_SNAPSHOT_DIR) -> bool:
    """Check whether a snapshot with the given name exists."""
    return _snapshot_path(name, snapshot_dir).exists()
