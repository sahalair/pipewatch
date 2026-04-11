"""Metric tracking for pipeline runs — record numeric values and compare across runs."""

from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from typing import Any

_METRICS_DIR = ".pipewatch/metrics"


def _metrics_path(run_id: str) -> str:
    return os.path.join(_METRICS_DIR, f"{run_id}.json")


def record_metric(run_id: str, name: str, value: float, unit: str = "") -> dict:
    """Create a single metric entry."""
    return {
        "run_id": run_id,
        "name": name,
        "value": value,
        "unit": unit,
        "recorded_at": datetime.now(timezone.utc).isoformat(),
    }


def save_metrics(run_id: str, metrics: list[dict]) -> None:
    """Persist a list of metric entries for a given run."""
    os.makedirs(_METRICS_DIR, exist_ok=True)
    path = _metrics_path(run_id)
    with open(path, "w", encoding="utf-8") as fh:
        json.dump(metrics, fh, indent=2)


def load_metrics(run_id: str) -> list[dict]:
    """Load metrics for a given run. Returns empty list if not found."""
    path = _metrics_path(run_id)
    if not os.path.exists(path):
        return []
    with open(path, "r", encoding="utf-8") as fh:
        return json.load(fh)


def list_runs() -> list[str]:
    """Return all run IDs that have saved metrics, sorted alphabetically."""
    if not os.path.isdir(_METRICS_DIR):
        return []
    return sorted(
        fname[:-5]
        for fname in os.listdir(_METRICS_DIR)
        if fname.endswith(".json")
    )


def compare_metrics(run_id_a: str, run_id_b: str) -> list[dict]:
    """Compare metrics between two runs. Returns a list of comparison dicts."""
    metrics_a = {m["name"]: m for m in load_metrics(run_id_a)}
    metrics_b = {m["name"]: m for m in load_metrics(run_id_b)}
    all_names = sorted(set(metrics_a) | set(metrics_b))

    results = []
    for name in all_names:
        a = metrics_a.get(name)
        b = metrics_b.get(name)
        val_a = a["value"] if a else None
        val_b = b["value"] if b else None
        delta = (val_b - val_a) if (val_a is not None and val_b is not None) else None
        results.append({
            "name": name,
            "run_a": val_a,
            "run_b": val_b,
            "delta": delta,
            "unit": (b or a or {}).get("unit", ""),
        })
    return results
