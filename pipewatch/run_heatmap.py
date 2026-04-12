"""Run heatmap: aggregate run counts and failure rates by time bucket."""

from __future__ import annotations

from collections import defaultdict
from typing import Dict, List, Optional

from pipewatch.run_logger import list_run_records


def _hour_bucket(iso: str) -> str:
    """Truncate an ISO timestamp to YYYY-MM-DDTHH."""
    return iso[:13] if iso else "unknown"


def _day_bucket(iso: str) -> str:
    """Truncate an ISO timestamp to YYYY-MM-DD."""
    return iso[:10] if iso else "unknown"


def build_heatmap(
    pipeline: Optional[str] = None,
    bucket: str = "day",
    base_dir: str = ".pipewatch",
) -> Dict[str, Dict[str, int]]:
    """Return a heatmap dict: {bucket_key: {"total": n, "failed": n}}.

    Args:
        pipeline: If given, filter to this pipeline name.
        bucket: Granularity — "day" or "hour".
        base_dir: Root directory for run records.
    """
    if bucket not in ("day", "hour"):
        raise ValueError(f"bucket must be 'day' or 'hour', got {bucket!r}")

    bucket_fn = _day_bucket if bucket == "day" else _hour_bucket
    runs = list_run_records(base_dir=base_dir)

    counts: Dict[str, Dict[str, int]] = defaultdict(lambda: {"total": 0, "failed": 0})

    for run in runs:
        if pipeline and run.get("pipeline") != pipeline:
            continue
        key = bucket_fn(run.get("started_at", ""))
        counts[key]["total"] += 1
        if run.get("status") == "failed":
            counts[key]["failed"] += 1

    return dict(sorted(counts.items()))


def format_heatmap(heatmap: Dict[str, Dict[str, int]], bar_width: int = 20) -> str:
    """Render the heatmap as a simple ASCII bar chart."""
    if not heatmap:
        return "No data available."

    max_total = max(v["total"] for v in heatmap.values()) or 1
    lines: List[str] = []
    header = f"{'Bucket':<20}  {'Total':>6}  {'Failed':>6}  Chart"
    lines.append(header)
    lines.append("-" * (len(header) + bar_width))

    for key, vals in heatmap.items():
        total = vals["total"]
        failed = vals["failed"]
        filled = round(bar_width * total / max_total)
        bar = "#" * filled + "." * (bar_width - filled)
        lines.append(f"{key:<20}  {total:>6}  {failed:>6}  [{bar}]")

    return "\n".join(lines)
