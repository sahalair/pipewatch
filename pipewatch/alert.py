"""Alert rules for monitoring pipeline output changes."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

DEFAULT_ALERTS_FILE = ".pipewatch_alerts.json"


def _alerts_path(alerts_file: str = DEFAULT_ALERTS_FILE) -> Path:
    return Path(alerts_file)


def create_alert_rule(
    name: str,
    snapshot_key: str,
    threshold: float = 0.0,
    notify: str = "stdout",
) -> dict[str, Any]:
    """Create an alert rule record.

    Args:
        name: Human-readable rule name.
        snapshot_key: The snapshot key to monitor.
        threshold: Minimum change ratio (0.0–1.0) to trigger alert.
        notify: Notification method ('stdout' only for now).

    Returns:
        A dict representing the alert rule.
    """
    return {
        "name": name,
        "snapshot_key": snapshot_key,
        "threshold": threshold,
        "notify": notify,
        "enabled": True,
    }


def save_alert_rules(
    rules: list[dict[str, Any]],
    alerts_file: str = DEFAULT_ALERTS_FILE,
) -> None:
    """Persist alert rules to a JSON file."""
    path = _alerts_path(alerts_file)
    path.write_text(json.dumps(rules, indent=2))


def load_alert_rules(
    alerts_file: str = DEFAULT_ALERTS_FILE,
) -> list[dict[str, Any]]:
    """Load alert rules from a JSON file. Returns empty list if not found."""
    path = _alerts_path(alerts_file)
    if not path.exists():
        return []
    return json.loads(path.read_text())


def evaluate_alert(
    rule: dict[str, Any],
    diff_result: dict[str, Any],
) -> bool:
    """Return True if the diff result triggers the alert rule.

    Args:
        rule: An alert rule dict (from create_alert_rule).
        diff_result: A diff result dict with 'changed' and 'diff_lines' keys.

    Returns:
        True if the alert should fire.
    """
    if not rule.get("enabled", True):
        return False
    if not diff_result.get("changed", False):
        return False
    diff_lines = diff_result.get("diff_lines", [])
    changed_lines = sum(
        1 for line in diff_lines if line.startswith("+") or line.startswith("-")
    )
    total_lines = max(len(diff_lines), 1)
    ratio = changed_lines / total_lines
    return ratio > rule.get("threshold", 0.0)


def format_alert_message(rule: dict[str, Any], diff_result: dict[str, Any]) -> str:
    """Format a human-readable alert message."""
    key = rule.get("snapshot_key", "unknown")
    name = rule.get("name", "unnamed")
    summary = diff_result.get("summary", "changes detected")
    return f"[ALERT] Rule '{name}' triggered for snapshot '{key}': {summary}"
