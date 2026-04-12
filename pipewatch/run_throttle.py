"""Run throttle: limit how frequently a pipeline can be triggered."""

import json
import os
from datetime import datetime, timezone, timedelta
from typing import Optional

_THROTTLE_FILE = ".pipewatch/throttle.json"


def _throttle_path(base_dir: str = ".") -> str:
    return os.path.join(base_dir, ".pipewatch", "throttle.json")


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def load_throttle_rules(base_dir: str = ".") -> dict:
    path = _throttle_path(base_dir)
    if not os.path.exists(path):
        return {}
    with open(path, "r") as f:
        return json.load(f)


def save_throttle_rules(rules: dict, base_dir: str = ".") -> None:
    path = _throttle_path(base_dir)
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w") as f:
        json.dump(rules, f, indent=2)


def set_throttle(pipeline: str, min_interval_seconds: int, base_dir: str = ".") -> dict:
    """Set a minimum interval (in seconds) between runs for a pipeline."""
    if min_interval_seconds < 0:
        raise ValueError("min_interval_seconds must be non-negative")
    rules = load_throttle_rules(base_dir)
    rules[pipeline] = {
        "pipeline": pipeline,
        "min_interval_seconds": min_interval_seconds,
        "last_triggered": rules.get(pipeline, {}).get("last_triggered"),
    }
    save_throttle_rules(rules, base_dir)
    return rules[pipeline]


def remove_throttle(pipeline: str, base_dir: str = ".") -> bool:
    rules = load_throttle_rules(base_dir)
    if pipeline not in rules:
        return False
    del rules[pipeline]
    save_throttle_rules(rules, base_dir)
    return True


def record_trigger(pipeline: str, base_dir: str = ".") -> Optional[str]:
    """Record that a pipeline was triggered now. Returns the ISO timestamp."""
    rules = load_throttle_rules(base_dir)
    if pipeline not in rules:
        return None
    ts = _now_iso()
    rules[pipeline]["last_triggered"] = ts
    save_throttle_rules(rules, base_dir)
    return ts


def is_throttled(pipeline: str, base_dir: str = ".") -> dict:
    """Check if a pipeline is currently throttled.

    Returns a dict with keys: throttled (bool), reason (str), seconds_remaining (float|None).
    """
    rules = load_throttle_rules(base_dir)
    if pipeline not in rules:
        return {"throttled": False, "reason": "no rule", "seconds_remaining": None}
    rule = rules[pipeline]
    last = rule.get("last_triggered")
    if not last:
        return {"throttled": False, "reason": "never triggered", "seconds_remaining": None}
    interval = rule["min_interval_seconds"]
    last_dt = datetime.fromisoformat(last)
    now = datetime.now(timezone.utc)
    elapsed = (now - last_dt).total_seconds()
    if elapsed < interval:
        remaining = interval - elapsed
        return {
            "throttled": True,
            "reason": f"must wait {remaining:.1f}s before next run",
            "seconds_remaining": remaining,
        }
    return {"throttled": False, "reason": "interval elapsed", "seconds_remaining": 0.0}
