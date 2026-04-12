"""Track and summarize estimated compute cost per pipeline run."""

import json
from pathlib import Path
from typing import Dict, List, Optional

_COSTS_FILE = ".pipewatch/run_costs.json"


def _costs_path() -> Path:
    return Path(_COSTS_FILE)


def load_costs() -> Dict[str, dict]:
    """Load all cost records; returns empty dict if file missing."""
    p = _costs_path()
    if not p.exists():
        return {}
    with p.open() as f:
        return json.load(f)


def save_costs(costs: Dict[str, dict]) -> None:
    """Persist cost records to disk."""
    p = _costs_path()
    p.parent.mkdir(parents=True, exist_ok=True)
    with p.open("w") as f:
        json.dump(costs, f, indent=2)


def record_cost(
    run_id: str,
    pipeline: str,
    amount: float,
    currency: str = "USD",
    unit: Optional[str] = None,
    notes: Optional[str] = None,
) -> dict:
    """Record an estimated cost entry for a run."""
    if amount < 0:
        raise ValueError("Cost amount must be non-negative.")
    entry = {
        "run_id": run_id,
        "pipeline": pipeline,
        "amount": amount,
        "currency": currency,
        "unit": unit,
        "notes": notes,
    }
    costs = load_costs()
    costs[run_id] = entry
    save_costs(costs)
    return entry


def get_cost(run_id: str) -> Optional[dict]:
    """Retrieve cost record for a specific run."""
    return load_costs().get(run_id)


def list_costs_for_pipeline(pipeline: str) -> List[dict]:
    """Return all cost entries for the given pipeline."""
    return [
        v for v in load_costs().values()
        if v.get("pipeline") == pipeline
    ]


def summarize_costs(pipeline: str) -> dict:
    """Compute total and average cost across runs for a pipeline."""
    entries = list_costs_for_pipeline(pipeline)
    if not entries:
        return {"pipeline": pipeline, "count": 0, "total": 0.0, "average": 0.0, "currency": "USD"}
    total = sum(e["amount"] for e in entries)
    currency = entries[0]["currency"]
    return {
        "pipeline": pipeline,
        "count": len(entries),
        "total": round(total, 6),
        "average": round(total / len(entries), 6),
        "currency": currency,
    }
