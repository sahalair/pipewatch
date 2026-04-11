"""Summarise event logs across runs for reporting and alerting."""

from __future__ import annotations

from collections import Counter
from typing import Any

from pipewatch.run_events import get_events


def summarize_events(run_id: str) -> dict[str, Any]:
    """Return a summary dict of event counts by level and type for *run_id*."""
    all_events = get_events(run_id)
    level_counts: Counter[str] = Counter(e["level"] for e in all_events)
    type_counts: Counter[str] = Counter(e["event_type"] for e in all_events)
    return {
        "run_id": run_id,
        "total": len(all_events),
        "by_level": dict(level_counts),
        "by_type": dict(type_counts),
        "has_errors": level_counts.get("error", 0) > 0,
        "has_warnings": level_counts.get("warning", 0) > 0,
    }


def events_have_errors(run_id: str) -> bool:
    """Return True if any error-level events exist for *run_id*."""
    return bool(get_events(run_id, level="error"))


def format_event_summary(summary: dict[str, Any]) -> str:
    """Return a human-readable string for an event *summary*."""
    lines = [
        f"Run: {summary['run_id']}",
        f"Total events: {summary['total']}",
    ]
    if summary["by_level"]:
        lines.append("By level:")
        for lvl, cnt in sorted(summary["by_level"].items()):
            lines.append(f"  {lvl}: {cnt}")
    if summary["by_type"]:
        lines.append("By type:")
        for typ, cnt in sorted(summary["by_type"].items()):
            lines.append(f"  {typ}: {cnt}")
    if summary["has_errors"]:
        lines.append("⚠ Errors detected.")
    return "\n".join(lines)


def compare_event_summaries(
    summary_a: dict[str, Any],
    summary_b: dict[str, Any],
) -> dict[str, Any]:
    """Return a diff of two event summaries (level counts only)."""
    levels = set(summary_a["by_level"]) | set(summary_b["by_level"])
    changes: dict[str, dict[str, int]] = {}
    for lvl in sorted(levels):
        a_cnt = summary_a["by_level"].get(lvl, 0)
        b_cnt = summary_b["by_level"].get(lvl, 0)
        if a_cnt != b_cnt:
            changes[lvl] = {"before": a_cnt, "after": b_cnt, "delta": b_cnt - a_cnt}
    return {
        "run_a": summary_a["run_id"],
        "run_b": summary_b["run_id"],
        "total_before": summary_a["total"],
        "total_after": summary_b["total"],
        "level_changes": changes,
    }
