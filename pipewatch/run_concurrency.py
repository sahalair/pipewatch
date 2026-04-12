"""Track and analyze concurrent pipeline run overlap."""

from __future__ import annotations

from typing import List, Dict, Optional

from pipewatch.run_logger import list_run_records


def _parse_iso(ts: str) -> float:
    from datetime import datetime, timezone
    dt = datetime.fromisoformat(ts)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.timestamp()


def get_active_runs_at(timestamp: float, runs: Optional[List[dict]] = None) -> List[dict]:
    """Return all runs that were active at the given POSIX timestamp."""
    if runs is None:
        runs = list_run_records()
    active = []
    for run in runs:
        started = run.get("started")
        finished = run.get("finished")
        if not started:
            continue
        start_ts = _parse_iso(started)
        if finished:
            end_ts = _parse_iso(finished)
        else:
            import time
            end_ts = time.time()
        if start_ts <= timestamp <= end_ts:
            active.append(run)
    return active


def compute_peak_concurrency(pipeline: Optional[str] = None, runs: Optional[List[dict]] = None) -> int:
    """Return the maximum number of runs that were active simultaneously."""
    if runs is None:
        runs = list_run_records()
    if pipeline:
        runs = [r for r in runs if r.get("pipeline") == pipeline]
    if not runs:
        return 0
    events: List[tuple] = []
    for run in runs:
        started = run.get("started")
        finished = run.get("finished")
        if not started:
            continue
        events.append((_parse_iso(started), 1))
        if finished:
            events.append((_parse_iso(finished), -1))
    events.sort(key=lambda x: (x[0], x[1]))
    peak = current = 0
    for _, delta in events:
        current += delta
        if current > peak:
            peak = current
    return peak


def concurrency_timeline(pipeline: Optional[str] = None, runs: Optional[List[dict]] = None) -> List[Dict]:
    """Return a sorted list of concurrency change events."""
    if runs is None:
        runs = list_run_records()
    if pipeline:
        runs = [r for r in runs if r.get("pipeline") == pipeline]
    events: List[tuple] = []
    for run in runs:
        started = run.get("started")
        finished = run.get("finished")
        if not started:
            continue
        events.append((_parse_iso(started), 1, started, run.get("run_id", "")))
        if finished:
            events.append((_parse_iso(finished), -1, finished, run.get("run_id", "")))
    events.sort(key=lambda x: (x[0], x[1]))
    timeline = []
    current = 0
    for ts, delta, iso, run_id in events:
        current += delta
        timeline.append({"timestamp": iso, "delta": delta, "concurrency": current, "run_id": run_id})
    return timeline


def format_concurrency_report(pipeline: Optional[str] = None, runs: Optional[List[dict]] = None) -> str:
    peak = compute_peak_concurrency(pipeline=pipeline, runs=runs)
    timeline = concurrency_timeline(pipeline=pipeline, runs=runs)
    label = pipeline or "(all pipelines)"
    lines = [f"Concurrency Report — {label}", f"  Peak concurrent runs: {peak}"]
    if timeline:
        lines.append("  Timeline:")
        for entry in timeline[-10:]:
            sign = "+" if entry["delta"] > 0 else "-"
            lines.append(f"    {entry['timestamp']}  {sign}1  => {entry['concurrency']} active  ({entry['run_id']})")
    return "\n".join(lines)
