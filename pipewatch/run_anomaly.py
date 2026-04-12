"""Detect anomalies in pipeline run metrics using simple z-score analysis."""

import math
from typing import Optional
from pipewatch.metric import load_metrics, list_runs


def _mean(values: list[float]) -> float:
    return sum(values) / len(values)


def _stddev(values: list[float], mean: float) -> float:
    variance = sum((v - mean) ** 2 for v in values) / len(values)
    return math.sqrt(variance)


def get_metric_history(
    pipeline: str, metric_name: str, limit: int = 20
) -> list[float]:
    """Return recent metric values for a pipeline, oldest first."""
    runs = list_runs()
    values = []
    for run_id in runs:
        metrics = load_metrics(run_id)
        for m in metrics:
            if m.get("pipeline") == pipeline and m.get("name") == metric_name:
                values.append(m["value"])
    return values[-limit:]


def z_score(value: float, history: list[float]) -> Optional[float]:
    """Compute z-score of value relative to history. Returns None if insufficient data."""
    if len(history) < 3:
        return None
    mu = _mean(history)
    sigma = _stddev(history, mu)
    if sigma == 0.0:
        return None
    return (value - mu) / sigma


def detect_anomaly(
    value: float, history: list[float], threshold: float = 2.5
) -> dict:
    """Return anomaly detection result for a single metric value."""
    score = z_score(value, history)
    if score is None:
        return {"anomaly": False, "z_score": None, "reason": "insufficient_history"}
    is_anomaly = abs(score) >= threshold
    return {
        "anomaly": is_anomaly,
        "z_score": round(score, 4),
        "threshold": threshold,
        "reason": "z_score_exceeded" if is_anomaly else "within_bounds",
    }


def analyze_run_anomalies(
    run_id: str, pipeline: str, threshold: float = 2.5
) -> list[dict]:
    """Analyze all metrics for a run and return anomaly results."""
    metrics = load_metrics(run_id)
    results = []
    for m in metrics:
        if m.get("pipeline") != pipeline:
            continue
        name = m["name"]
        value = m["value"]
        history = get_metric_history(pipeline, name)
        # Exclude current value from history for fair comparison
        history_without_current = [v for v in history if v != value] or history
        result = detect_anomaly(value, history_without_current, threshold)
        results.append({
            "metric": name,
            "value": value,
            **result,
        })
    return results


def format_anomaly_report(results: list[dict]) -> str:
    """Format anomaly analysis results as a human-readable string."""
    if not results:
        return "No metrics found for anomaly analysis."
    lines = ["Anomaly Report", "=" * 40]
    for r in results:
        flag = "[ANOMALY]" if r["anomaly"] else "[OK]    "
        z = f"z={r['z_score']}" if r["z_score"] is not None else "z=n/a"
        lines.append(f"  {flag} {r['metric']}: {r['value']}  ({z})  reason={r['reason']}")
    anomaly_count = sum(1 for r in results if r["anomaly"])
    lines.append(f"\n{anomaly_count}/{len(results)} metrics flagged as anomalous.")
    return "\n".join(lines)
