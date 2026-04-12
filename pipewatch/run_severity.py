"""Run severity classification based on status, alerts, and events."""

from typing import Dict, Any, List

SEVERITY_LEVELS = ["ok", "low", "medium", "high", "critical"]

_SEVERITY_RANK = {level: i for i, level in enumerate(SEVERITY_LEVELS)}


def classify_severity(run: Dict[str, Any]) -> str:
    """Return a severity level string for a given run record dict."""
    status = run.get("status", "unknown")
    if status == "failed":
        exit_code = run.get("exit_code", 1)
        if exit_code is not None and exit_code >= 2:
            return "critical"
        return "high"
    if status == "unknown":
        return "medium"
    alerts = run.get("alerts", [])
    if alerts:
        return "medium"
    return "ok"


def severity_rank(level: str) -> int:
    """Return integer rank for a severity level (higher = more severe)."""
    return _SEVERITY_RANK.get(level, 0)


def highest_severity(levels: List[str]) -> str:
    """Return the most severe level from a list of severity strings."""
    if not levels:
        return "ok"
    return max(levels, key=severity_rank)


def format_severity_badge(level: str) -> str:
    """Return a short text badge for display."""
    badges = {
        "ok": "[OK]",
        "low": "[LOW]",
        "medium": "[MED]",
        "high": "[HIGH]",
        "critical": "[CRIT]",
    }
    return badges.get(level, "[?]")


def summarize_severities(runs: List[Dict[str, Any]]) -> Dict[str, int]:
    """Return a count of each severity level across a list of runs."""
    counts: Dict[str, int] = {level: 0 for level in SEVERITY_LEVELS}
    for run in runs:
        level = classify_severity(run)
        counts[level] = counts.get(level, 0) + 1
    return counts


def format_severity_summary(counts: Dict[str, int]) -> str:
    """Format a severity summary dict into a human-readable string."""
    lines = ["Severity Summary:", "-" * 24]
    for level in SEVERITY_LEVELS:
        badge = format_severity_badge(level)
        lines.append(f"  {badge:<8} {counts.get(level, 0):>4}")
    return "\n".join(lines)
