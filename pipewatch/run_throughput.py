"""Track and analyze pipeline run throughput (runs per time window)."""

from __future__ import annotations

from collections import defaultdict
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Optional

from pipewatch.run_logger import list_run_records


def _parse_iso(ts: str) -> datetime:
    """Parse an ISO 8601 timestamp into a timezone-aware datetime."""
    dt = datetime.fromisoformat(ts)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt


def _window_start(dt: datetime, window: str) -> datetime:
    """Return the start of the time window containing *dt*.

    Supported windows: 'hour', 'day', 'week'.
    """
    if window == "hour":
        return dt.replace(minute=0, second=0, microsecond=0)
    if window == "day":
        return dt.replace(hour=0, minute=0, second=0, microsecond=0)
    if window == "week":
        start = dt - timedelta(days=dt.weekday())
        return start.replace(hour=0, minute=0, second=0, microsecond=0)
    raise ValueError(f"Unknown window: {window!r}. Use 'hour', 'day', or 'week'.")


def compute_throughput(
    pipeline: Optional[str] = None,
    window: str = "day",
    base_dir: str = ".pipewatch",
) -> Dict[str, int]:
    """Return a mapping of window-start ISO strings to run counts.

    Args:
        pipeline: If given, only count runs for this pipeline.
        window: Granularity — 'hour', 'day', or 'week'.
        base_dir: Storage root passed to :func:`list_run_records`.

    Returns:
        Ordered dict of ``{window_start_iso: count}``.
    """
    records = list_run_records(base_dir=base_dir)
    counts: Dict[str, int] = defaultdict(int)

    for record in records:
        if pipeline and record.get("pipeline") != pipeline:
            continue
        started = record.get("started")
        if not started:
            continue
        try:
            dt = _parse_iso(started)
        except (ValueError, TypeError):
            continue
        bucket = _window_start(dt, window).isoformat()
        counts[bucket] += 1

    return dict(sorted(counts.items()))


def format_throughput_report(
    throughput: Dict[str, int],
    window: str = "day",
) -> str:
    """Return a human-readable throughput report."""
    if not throughput:
        return "No runs recorded."

    lines: List[str] = [f"Throughput by {window}:", ""]
    max_count = max(throughput.values()) or 1
    bar_width = 30

    for bucket, count in throughput.items():
        bar = "#" * int(count / max_count * bar_width)
        lines.append(f"  {bucket}  {bar:<{bar_width}}  {count}")

    total = sum(throughput.values())
    lines += ["", f"Total runs: {total}"]
    return "\n".join(lines)
