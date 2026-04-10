"""Metric-based alert evaluation — check if metric deltas exceed thresholds."""

from __future__ import annotations

from pipewatch.metric import compare_metrics


def evaluate_metric_alerts(
    run_id_a: str,
    run_id_b: str,
    rules: list[dict],
) -> list[dict]:
    """Evaluate metric alert rules against a pair of runs.

    Each rule must have:
      - name: metric name to watch
      - threshold: maximum allowed absolute delta (float)
      - direction: 'any' | 'increase' | 'decrease'

    Returns a list of triggered alert dicts.
    """
    comparisons = {c["name"]: c for c in compare_metrics(run_id_a, run_id_b)}
    triggered = []

    for rule in rules:
        name = rule.get("name")
        threshold = float(rule.get("threshold", 0.0))
        direction = rule.get("direction", "any")

        if name not in comparisons:
            continue

        entry = comparisons[name]
        delta = entry["delta"]

        if delta is None:
            continue

        fired = False
        if direction == "any" and abs(delta) > threshold:
            fired = True
        elif direction == "increase" and delta > threshold:
            fired = True
        elif direction == "decrease" and delta < -threshold:
            fired = True

        if fired:
            triggered.append({
                "metric": name,
                "run_a": entry["run_a"],
                "run_b": entry["run_b"],
                "delta": delta,
                "threshold": threshold,
                "direction": direction,
                "unit": entry["unit"],
            })

    return triggered


def format_metric_alerts(triggered: list[dict]) -> str:
    """Return a human-readable summary of triggered metric alerts."""
    if not triggered:
        return "No metric alerts triggered."
    lines = ["Metric alerts triggered:"]
    for t in triggered:
        unit = f" {t['unit']}" if t["unit"] else ""
        lines.append(
            f"  [{t['metric']}] delta={t['delta']:+.4g}{unit} "
            f"(threshold={t['threshold']}{unit}, direction={t['direction']})"
        )
    return "\n".join(lines)
