"""Audit log for pipeline run lifecycle events."""
import json
import os
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

_VALID_ACTIONS = {"created", "started", "finished", "archived", "restored", "deleted", "tagged", "annotated", "noted"}


def _audit_path(base_dir: str) -> str:
    return os.path.join(base_dir, "audit_log.json")


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def load_audit_log(base_dir: str) -> List[Dict[str, Any]]:
    path = _audit_path(base_dir)
    if not os.path.exists(path):
        return []
    with open(path, "r") as f:
        return json.load(f)


def save_audit_log(base_dir: str, entries: List[Dict[str, Any]]) -> None:
    os.makedirs(base_dir, exist_ok=True)
    with open(_audit_path(base_dir), "w") as f:
        json.dump(entries, f, indent=2)


def record_audit_event(
    base_dir: str,
    run_id: str,
    action: str,
    actor: Optional[str] = None,
    details: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    if action not in _VALID_ACTIONS:
        raise ValueError(f"Unknown audit action: {action!r}. Must be one of {sorted(_VALID_ACTIONS)}")
    entry = {
        "run_id": run_id,
        "action": action,
        "timestamp": _now_iso(),
        "actor": actor,
        "details": details or {},
    }
    log = load_audit_log(base_dir)
    log.append(entry)
    save_audit_log(base_dir, log)
    return entry


def get_audit_events(
    base_dir: str,
    run_id: Optional[str] = None,
    action: Optional[str] = None,
) -> List[Dict[str, Any]]:
    log = load_audit_log(base_dir)
    if run_id:
        log = [e for e in log if e["run_id"] == run_id]
    if action:
        log = [e for e in log if e["action"] == action]
    return log


def format_audit_log(entries: List[Dict[str, Any]]) -> str:
    if not entries:
        return "No audit events found."
    lines = []
    for e in entries:
        actor_str = f" by {e['actor']}" if e.get("actor") else ""
        detail_str = f" | {e['details']}" if e.get("details") else ""
        lines.append(f"[{e['timestamp']}] {e['run_id']} — {e['action']}{actor_str}{detail_str}")
    return "\n".join(lines)
