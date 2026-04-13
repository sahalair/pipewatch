"""Summary and reporting utilities for the run audit log."""
from collections import Counter
from typing import Any, Dict, List, Optional

from pipewatch.run_audit import get_audit_events


def summarize_audit_log(
    base_dir: str,
    run_id: Optional[str] = None,
) -> Dict[str, Any]:
    """Return a summary dict of audit events for a run or all runs."""
    events = get_audit_events(base_dir, run_id=run_id)
    action_counts = Counter(e["action"] for e in events)
    actors = list({e["actor"] for e in events if e.get("actor")})
    return {
        "total_events": len(events),
        "action_counts": dict(action_counts),
        "unique_actors": sorted(actors),
        "run_id": run_id,
    }


def most_active_runs(
    base_dir: str,
    limit: int = 5,
) -> List[Dict[str, Any]]:
    """Return the top N run IDs by audit event count."""
    events = get_audit_events(base_dir)
    counts: Counter = Counter(e["run_id"] for e in events)
    return [
        {"run_id": run_id, "event_count": count}
        for run_id, count in counts.most_common(limit)
    ]


def format_audit_summary(summary: Dict[str, Any]) -> str:
    lines = []
    label = f"Run {summary['run_id']}" if summary.get("run_id") else "All runs"
    lines.append(f"Audit Summary — {label}")
    lines.append(f"  Total events : {summary['total_events']}")
    if summary["action_counts"]:
        lines.append("  Actions      :")
        for action, count in sorted(summary["action_counts"].items()):
            lines.append(f"    {action:<14} {count}")
    actors = summary.get("unique_actors", [])
    if actors:
        lines.append(f"  Actors       : {', '.join(actors)}")
    else:
        lines.append("  Actors       : (none)")
    return "\n".join(lines)
