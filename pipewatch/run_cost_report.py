"""Build and format cost reports across pipeline runs."""

from typing import List, Optional
from pipewatch.run_cost import list_costs_for_pipeline, summarize_costs


def build_cost_report(pipeline: str, limit: Optional[int] = None) -> dict:
    """Build a structured cost report for a pipeline."""
    entries = list_costs_for_pipeline(pipeline)
    entries_sorted = sorted(entries, key=lambda e: e["run_id"])
    if limit is not None:
        entries_sorted = entries_sorted[-limit:]
    summary = summarize_costs(pipeline)
    return {
        "pipeline": pipeline,
        "entries": entries_sorted,
        "summary": summary,
    }


def format_cost_report(report: dict) -> str:
    """Format a cost report as a human-readable string."""
    lines = []
    pipeline = report["pipeline"]
    summary = report["summary"]
    entries = report["entries"]

    lines.append(f"=== Cost Report: {pipeline} ===")
    if not entries:
        lines.append("  No cost records found.")
        return "\n".join(lines)

    lines.append(f"  Runs recorded : {summary['count']}")
    lines.append(f"  Total cost    : {summary['total']} {summary['currency']}")
    lines.append(f"  Average cost  : {summary['average']} {summary['currency']}")
    lines.append("")
    lines.append("  Per-run breakdown:")
    for e in entries:
        unit_str = f" [{e['unit']}]" if e.get("unit") else ""
        notes_str = f" — {e['notes']}" if e.get("notes") else ""
        lines.append(f"    {e['run_id']}: {e['amount']} {e['currency']}{unit_str}{notes_str}")

    return "\n".join(lines)


def top_cost_runs(pipeline: str, n: int = 3) -> List[dict]:
    """Return the n most expensive runs for a pipeline."""
    entries = list_costs_for_pipeline(pipeline)
    return sorted(entries, key=lambda e: e["amount"], reverse=True)[:n]
