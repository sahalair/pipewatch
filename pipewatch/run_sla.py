"""SLA (Service Level Agreement) tracking for pipeline runs."""

import json
from pathlib import Path
from typing import Optional

DEFAULT_SLA_DIR = Path(".pipewatch/sla")


def _sla_path(base_dir: Path = DEFAULT_SLA_DIR) -> Path:
    base_dir.mkdir(parents=True, exist_ok=True)
    return base_dir / "sla_rules.json"


def load_sla_rules(base_dir: Path = DEFAULT_SLA_DIR) -> dict:
    path = _sla_path(base_dir)
    if not path.exists():
        return {}
    with open(path) as f:
        return json.load(f)


def save_sla_rules(rules: dict, base_dir: Path = DEFAULT_SLA_DIR) -> None:
    path = _sla_path(base_dir)
    with open(path, "w") as f:
        json.dump(rules, f, indent=2)


def set_sla(pipeline: str, max_duration_seconds: float,
            description: str = "", base_dir: Path = DEFAULT_SLA_DIR) -> dict:
    """Define or update an SLA rule for a pipeline."""
    rules = load_sla_rules(base_dir)
    rules[pipeline] = {
        "pipeline": pipeline,
        "max_duration_seconds": max_duration_seconds,
        "description": description,
    }
    save_sla_rules(rules, base_dir)
    return rules[pipeline]


def remove_sla(pipeline: str, base_dir: Path = DEFAULT_SLA_DIR) -> bool:
    rules = load_sla_rules(base_dir)
    if pipeline not in rules:
        return False
    del rules[pipeline]
    save_sla_rules(rules, base_dir)
    return True


def get_sla(pipeline: str, base_dir: Path = DEFAULT_SLA_DIR) -> Optional[dict]:
    return load_sla_rules(base_dir).get(pipeline)


def evaluate_sla(pipeline: str, duration_seconds: float,
                 base_dir: Path = DEFAULT_SLA_DIR) -> dict:
    """Check whether a run duration violates the SLA for a pipeline."""
    rule = get_sla(pipeline, base_dir)
    if rule is None:
        return {"pipeline": pipeline, "sla_defined": False, "violated": False,
                "duration_seconds": duration_seconds, "max_duration_seconds": None}
    violated = duration_seconds > rule["max_duration_seconds"]
    return {
        "pipeline": pipeline,
        "sla_defined": True,
        "violated": violated,
        "duration_seconds": duration_seconds,
        "max_duration_seconds": rule["max_duration_seconds"],
        "description": rule.get("description", ""),
    }


def format_sla_report(result: dict) -> str:
    if not result["sla_defined"]:
        return f"[SLA] No SLA defined for pipeline '{result['pipeline']}'"
    status = "VIOLATED" if result["violated"] else "OK"
    return (
        f"[SLA] Pipeline: {result['pipeline']} | "
        f"Status: {status} | "
        f"Duration: {result['duration_seconds']:.1f}s / "
        f"Max: {result['max_duration_seconds']:.1f}s"
    )
