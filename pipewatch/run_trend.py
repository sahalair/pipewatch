"""Trend analysis for pipeline run metrics over time."""

from typing import Optional
from pipewatch.run_logger import list_run_records
from pipewatch.metric import load_metrics


def _parse_iso(ts: str) -> float:
    from datetime import datetime, timezone
    return datetime.fromisoformat(ts).replace(tzinfo=timezone.utc).timestamp()


def get_metric_trend(pipeline: str, metric_name: str, limit: int = 10) -> list[dict]:
    """Return recent metric values for a pipeline, oldest first."""
    records = [
        r for r in list_run_records()
        if r.get("pipeline") == pipeline and r.get("finished")
    ]
    records.sort(key=lambda r: _parse_iso(r["finished"]))
    records = records[-limit:]

    trend = []
    for r in records:
        run_id = r["run_id"]
        metrics = load_metrics(run_id)
        entry = metrics.get(metric_name)
        if entry is not None:
            trend.append({
                "run_id": run_id,
                "finished": r["finished"],
                "value": entry["value"],
                "unit": entry.get("unit", ""),
            })
    return trend


def compute_trend_direction(trend: list[dict]) -> str:
    """Return 'up', 'down', 'stable', or 'insufficient_data'."""
    values = [e["value"] for e in trend]
    if len(values) < 2:
        return "insufficient_data"
    delta = values[-1] - values[0]
    pct = abs(delta) / (abs(values[0]) + 1e-9)
    if pct < 0.02:
        return "stable"
    return "up" if delta > 0 else "down"


def format_trend_report(pipeline: str, metric_name: str, trend: list[dict]) -> str:
    """Format a human-readable trend report."""
    lines = [f"Trend: {pipeline} / {metric_name} ({len(trend)} runs)"]
    if not trend:
        lines.append("  No data found.")
        return "\n".join(lines)

    direction = compute_trend_direction(trend)
    arrow = {"up": "↑", "down": "↓", "stable": "→", "insufficient_data": "?"}[direction]
    lines.append(f"  Direction: {arrow} {direction}")
    lines.append("  Run                                   Finished             Value")
    for e in trend:
        unit = f" {e['unit']}" if e["unit"] else ""
        lines.append(f"  {e['run_id'][:36]:<36}  {e['finished'][:19]}  {e['value']}{unit}")
    return "\n".join(lines)
