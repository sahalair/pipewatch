"""Analyze the cadence (regularity) of pipeline runs."""

from __future__ import annotations

from typing import Optional

from pipewatch.run_logger import list_run_records


def _parse_iso(ts: str) -> float:
    from datetime import datetime, timezone
    dt = datetime.fromisoformat(ts)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.timestamp()


def get_run_timestamps(pipeline: str, limit: int = 50) -> list[float]:
    """Return sorted (ascending) epoch timestamps for a pipeline's runs."""
    records = list_run_records()
    runs = [
        r for r in records
        if r.get("pipeline") == pipeline and r.get("started")
    ]
    runs.sort(key=lambda r: r["started"])
    runs = runs[-limit:]
    return [_parse_iso(r["started"]) for r in runs]


def compute_intervals(timestamps: list[float]) -> list[float]:
    """Return list of gaps (seconds) between consecutive timestamps."""
    if len(timestamps) < 2:
        return []
    return [timestamps[i + 1] - timestamps[i] for i in range(len(timestamps) - 1)]


def cadence_stats(intervals: list[float]) -> dict:
    """Return mean, stddev, min, max of intervals in seconds."""
    if not intervals:
        return {"mean": None "min": None, "max": None, "count": 0}
    n = len(intervals)
    mean = sum(intervals) / n
    variance = sum((x - mean) ** 2 for x in intervals) / n
    stddev = variance ** 0.5
    return {
        "mean": round(mean, 2),
        "stddev": round(stddev, 2),
        "min": round(min(intervals), 2),
        "max": round(max(intervals), 2),
        "count": n,
    }


def classify_cadence(stats: dict) -> str:
    """Classify cadence regularity based on coefficient of variation."""
    if stats["mean"] is None or stats["mean"] == 0:
        return "unknown"
    cv = stats["stddev"] / stats["mean"]
    if cv < 0.1:
        return "very_regular"
    if cv < 0.3:
        return "regular"
    if cv < 0.6:
        return "irregular"
    return "chaotic"


def analyze_cadence(pipeline: str, limit: int = 50) -> dict:
    """Full cadence analysis for a pipeline."""
    timestamps = get_run_timestamps(pipeline, limit=limit)
    intervals = compute_intervals(timestamps)
    stats = cadence_stats(intervals)
    label = classify_cadence(stats)
    return {
        "pipeline": pipeline,
        "run_count": len(timestamps),
        "intervals": stats,
        "cadence": label,
    }


def format_cadence_report(result: dict) -> str:
    """Format a cadence analysis result as a human-readable string."""
    lines = [
        f"Pipeline : {result['pipeline']}",
        f"Runs     : {result['run_count']}",
        f"Cadence  : {result['cadence'].upper()}",
    ]
    iv = result["intervals"]
    if iv["mean"] is not None:
        lines += [
            f"Mean gap : {iv['mean']}s",
            f"Std dev  : {iv['stddev']}s",
            f"Min gap  : {iv['min']}s",
            f"Max gap  : {iv['max']}s",
        ]
    else:
        lines.append("Insufficient data for interval analysis.")
    return "\n".join(lines)
