"""Detect metric regressions between pipeline runs."""

from typing import Optional
from pipewatch.metric import load_metrics, list_runs


def get_metric_values(pipeline: str, metric_name: str, limit: int = 20) -> list:
    """Return list of (run_id, value) tuples for a metric, oldest first."""
    runs = list_runs()
    results = []
    for run_id in runs:
        metrics = load_metrics(run_id)
        for m in metrics:
            if m.get("pipeline") == pipeline and m.get("name") == metric_name:
                results.append((run_id, m["value"]))
    return results[-limit:]


def detect_regression(
    values: list,
    threshold_pct: float = 10.0,
    window: int = 3,
) -> Optional[dict]:
    """Detect if the latest value regresses vs the rolling mean of previous window.

    Returns a dict with regression info or None if no regression detected.
    """
    if len(values) < window + 1:
        return None

    recent = [v for _, v in values[-(window + 1):-1]]
    latest_run, latest_val = values[-1]

    if not recent:
        return None

    baseline = sum(recent) / len(recent)
    if baseline == 0:
        return None

    change_pct = ((latest_val - baseline) / abs(baseline)) * 100.0

    if abs(change_pct) >= threshold_pct:
        direction = "increase" if change_pct > 0 else "decrease"
        return {
            "run_id": latest_run,
            "latest_value": latest_val,
            "baseline": round(baseline, 4),
            "change_pct": round(change_pct, 2),
            "direction": direction,
            "regressed": True,
        }
    return None


def analyze_regression(
    pipeline: str,
    metric_name: str,
    threshold_pct: float = 10.0,
    window: int = 3,
    limit: int = 20,
) -> dict:
    """Full regression analysis for a pipeline metric."""
    values = get_metric_values(pipeline, metric_name, limit=limit)
    regression = detect_regression(values, threshold_pct=threshold_pct, window=window)
    return {
        "pipeline": pipeline,
        "metric": metric_name,
        "data_points": len(values),
        "regression": regression,
    }


def format_regression_report(result: dict) -> str:
    """Format a regression analysis result as a human-readable string."""
    lines = [
        f"Pipeline : {result['pipeline']}",
        f"Metric   : {result['metric']}",
        f"Data pts : {result['data_points']}",
    ]
    reg = result.get("regression")
    if reg:
        lines.append(f"Status   : REGRESSION DETECTED")
        lines.append(f"Run      : {reg['run_id']}")
        lines.append(f"Latest   : {reg['latest_value']}")
        lines.append(f"Baseline : {reg['baseline']}")
        lines.append(f"Change   : {reg['change_pct']:+.2f}% ({reg['direction']})")
    else:
        lines.append("Status   : OK (no regression)")
    return "\n".join(lines)
