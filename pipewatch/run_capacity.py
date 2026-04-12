"""Track and evaluate pipeline concurrency capacity limits."""

import json
import os
from typing import Dict, List, Optional

CAPACITY_FILE = ".pipewatch/capacity.json"


def _capacity_path(base_dir: str) -> str:
    return os.path.join(base_dir, "capacity.json")


def load_capacity(base_dir: str) -> Dict:
    path = _capacity_path(base_dir)
    if not os.path.exists(path):
        return {}
    with open(path) as f:
        return json.load(f)


def save_capacity(base_dir: str, data: Dict) -> None:
    os.makedirs(base_dir, exist_ok=True)
    with open(_capacity_path(base_dir), "w") as f:
        json.dump(data, f, indent=2)


def set_capacity(base_dir: str, pipeline: str, max_concurrent: int) -> Dict:
    if max_concurrent < 1:
        raise ValueError("max_concurrent must be at least 1")
    data = load_capacity(base_dir)
    data[pipeline] = {"pipeline": pipeline, "max_concurrent": max_concurrent}
    save_capacity(base_dir, data)
    return data[pipeline]


def remove_capacity(base_dir: str, pipeline: str) -> bool:
    data = load_capacity(base_dir)
    if pipeline not in data:
        return False
    del data[pipeline]
    save_capacity(base_dir, data)
    return True


def get_capacity(base_dir: str, pipeline: str) -> Optional[Dict]:
    data = load_capacity(base_dir)
    return data.get(pipeline)


def check_capacity(base_dir: str, pipeline: str, active_runs: int) -> Dict:
    """Return a capacity status dict for a pipeline given current active runs."""
    rule = get_capacity(base_dir, pipeline)
    if rule is None:
        return {"pipeline": pipeline, "limited": False, "active": active_runs,
                "max_concurrent": None, "at_capacity": False}
    at_capacity = active_runs >= rule["max_concurrent"]
    return {
        "pipeline": pipeline,
        "limited": True,
        "active": active_runs,
        "max_concurrent": rule["max_concurrent"],
        "at_capacity": at_capacity,
    }


def list_capacity_rules(base_dir: str) -> List[Dict]:
    data = load_capacity(base_dir)
    return sorted(data.values(), key=lambda r: r["pipeline"])
