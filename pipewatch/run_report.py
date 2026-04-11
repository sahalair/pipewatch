"""Generate summary reports for pipeline runs, combining status, metrics, and alerts."""

from __future__ import annotations

from typing import Any

from pipewatch.run_logger import load_run_record, list_run_records
from pipewatch.metric import load_metrics, compare_metrics
from pipewatch.pipeline_status import get_pipeline_status, format_status_report


def build_run_report(run_id: str, storage_dir: str = ".pipewatch") -> dict[str, Any]:
    """Build a comprehensive report for a single run."""
    record = load_run_record(run_id, storage_dir=storage_dir)
    if record is None:
        raise FileNotFoundError(f"Run record not found: {run_id}")

    status = get_pipeline_status(run_id, storage_dir=storage_dir)
    metrics = load_metrics(run_id, storage_dir=storage_dir)

    # Find previous run for metric comparison
    all_runs = list_run_records(storage_dir=storage_dir)
    run_ids = [r["run_id"] for r in all_runs]
    current_index = run_ids.index(run_id) if run_id in run_ids else -1
    metric_diff = None
    if current_index > 0:
        prev_run_id = run_ids[current_index - 1]
        prev_metrics = load_metrics(prev_run_id, storage_dir=storage_dir)
        if prev_metrics and metrics:
            metric_diff = compare_metrics(prev_metrics, metrics)

    return {
        "run_id": run_id,
        "record": record,
        "status": status,
        "metrics": metrics,
        "metric_diff": metric_diff,
    }


def format_run_report(report: dict[str, Any]) -> str:
    """Format a run report as a human-readable string."""
    lines: list[str] = []
    run_id = report["run_id"]
    lines.append(f"{'=' * 60}")
    lines.append(f"Run Report: {run_id}")
    lines.append(f"{'=' * 60}")

    # Status section
    lines.append(format_status_report(report["status"]))

    # Metrics section
    metrics = report.get("metrics") or []
    if metrics:
        lines.append("\nMetrics:")
        for m in metrics:
            unit = f" {m['unit']}" if m.get("unit") else ""
            lines.append(f"  {m['name']}: {m['value']}{unit}")
    else:
        lines.append("\nMetrics: (none recorded)")

    # Metric diff section
    diff = report.get("metric_diff")
    if diff:
        lines.append("\nMetric Changes vs Previous Run:")
        for name, change in diff.items():
            lines.append(f"  {name}: {change['previous']} -> {change['current']} ({change['delta']:+g})")

    lines.append("")
    return "\n".join(lines)
