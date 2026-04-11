"""Pipeline status aggregation: combines run records, metrics, and alerts into a status summary."""

from typing import Any
from pipewatch.run_logger import load_run_record, list_run_records
from pipewatch.metric import load_metrics
from pipewatch.alert import load_alert_rules, evaluate_alert


def get_pipeline_status(run_id: str, storage_dir: str = ".") -> dict[str, Any]:
    """Return a status summary dict for a given run_id."""
    run = load_run_record(run_id, storage_dir=storage_dir)
    if run is None:
        raise KeyError(f"Run not found: {run_id}")

    metrics = load_metrics(run_id, storage_dir=storage_dir)
    alert_rules = load_alert_rules(storage_dir=storage_dir)

    triggered_alerts = []
    for rule in alert_rules:
        result = evaluate_alert(rule, run_id, storage_dir=storage_dir)
        if result.get("triggered"):
            triggered_alerts.append(result)

    status = "ok"
    if run.get("exit_code") not in (0, None):
        status = "failed"
    elif triggered_alerts:
        status = "alert"

    return {
        "run_id": run_id,
        "status": status,
        "run": run,
        "metrics": metrics,
        "triggered_alerts": triggered_alerts,
        "alert_count": len(triggered_alerts),
        "metric_count": len(metrics),
    }


def summarize_recent_runs(n: int = 5, storage_dir: str = ".") -> list[dict[str, Any]]:
    """Return status summaries for the N most recent runs."""
    run_ids = list_run_records(storage_dir=storage_dir)
    recent = run_ids[:n]
    summaries = []
    for run_id in recent:
        try:
            summary = get_pipeline_status(run_id, storage_dir=storage_dir)
            summaries.append(summary)
        except Exception:
            summaries.append({"run_id": run_id, "status": "error", "error": "failed to load"})
    return summaries


def format_status_report(status: dict[str, Any]) -> str:
    """Format a status dict as a human-readable string."""
    lines = [
        f"Run ID : {status['run_id']}",
        f"Status : {status['status'].upper()}",
        f"Metrics: {status.get('metric_count', 0)}",
        f"Alerts : {status.get('alert_count', 0)}",
    ]
    run = status.get("run", {})
    if run.get("finished_at"):
        lines.append(f"Finished: {run['finished_at']}")
    for alert in status.get("triggered_alerts", []):
        lines.append(f"  [ALERT] {alert.get('rule_name', '?')}: {alert.get('message', '')}")
    return "\n".join(lines)
