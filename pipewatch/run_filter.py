"""Filter and query run records by tags, status, date range, and metrics."""

from typing import List, Optional, Dict, Any
from pipewatch.run_logger import list_run_records
from pipewatch.tag_manager import load_tags


def filter_runs(
    base_dir: str = ".",
    tags: Optional[List[str]] = None,
    status: Optional[str] = None,
    pipeline: Optional[str] = None,
    since: Optional[str] = None,
    until: Optional[str] = None,
    limit: Optional[int] = None,
) -> List[Dict[str, Any]]:
    """Return run records matching the given filters.

    Args:
        base_dir: Root directory for pipewatch data.
        tags: If provided, only runs that have ALL of these tags are returned.
        status: If provided, only runs with this exit-status string are returned.
        pipeline: If provided, only runs whose pipeline name matches.
        since: ISO-8601 date string; only runs started on or after this date.
        until: ISO-8601 date string; only runs started on or before this date.
        limit: If provided, return at most this many runs (most-recent first).

    Returns:
        List of matching run record dicts, sorted newest-first.
    """
    runs = list_run_records(base_dir=base_dir)  # already sorted newest-first

    if tags:
        all_tags = load_tags(base_dir=base_dir)
        tag_set = set(tags)
        runs = [
            r for r in runs
            if tag_set.issubset(set(all_tags.get(r["run_id"], [])))
        ]

    if status is not None:
        runs = [r for r in runs if str(r.get("exit_code", "")) == str(status)
                or r.get("status") == status]

    if pipeline is not None:
        runs = [r for r in runs if r.get("pipeline") == pipeline]

    if since is not None:
        runs = [r for r in runs if r.get("started_at", "") >= since]

    if until is not None:
        runs = [r for r in runs if r.get("started_at", "") <= until]

    if limit is not None:
        runs = runs[:limit]

    return runs


def format_run_list(runs: List[Dict[str, Any]], all_tags: Optional[Dict[str, List[str]]] = None) -> str:
    """Format a list of run records as a human-readable string."""
    if not runs:
        return "No runs found."

    lines = []
    for r in runs:
        run_id = r.get("run_id", "?")
        started = r.get("started_at", "unknown")[:19]
        pipeline = r.get("pipeline", "")
        status = r.get("status", r.get("exit_code", "?"))
        tag_str = ""
        if all_tags and run_id in all_tags:
            tag_str = "  [" + ", ".join(sorted(all_tags[run_id])) + "]"
        lines.append(f"{run_id}  {started}  pipeline={pipeline}  status={status}{tag_str}")

    return "\n".join(lines)
