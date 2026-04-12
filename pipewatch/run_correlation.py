"""Correlate metrics across pipeline runs to find relationships."""

from typing import Optional
from pipewatch.metric import load_metrics, list_runs


def get_metric_series(pipeline: str, metric_name: str, base_dir: str = ".") -> list[dict]:
    """Return a list of {run_id, value} dicts for a metric across runs."""
    series = []
    for run_id in list_runs(base_dir=base_dir):
        metrics = load_metrics(run_id, base_dir=base_dir)
        for m in metrics:
            if m.get("pipeline") == pipeline and m.get("name") == metric_name:
                series.append({"run_id": run_id, "value": m["value"]})
    return series


def pearson_correlation(xs: list[float], ys: list[float]) -> Optional[float]:
    """Compute Pearson correlation coefficient between two equal-length lists."""
    n = len(xs)
    if n < 2 or len(ys) != n:
        return None
    mean_x = sum(xs) / n
    mean_y = sum(ys) / n
    num = sum((x - mean_x) * (y - mean_y) for x, y in zip(xs, ys))
    den_x = sum((x - mean_x) ** 2 for x in xs) ** 0.5
    den_y = sum((y - mean_y) ** 2 for y in ys) ** 0.5
    if den_x == 0 or den_y == 0:
        return None
    return num / (den_x * den_y)


def correlate_metrics(
    pipeline: str,
    metric_a: str,
    metric_b: str,
    base_dir: str = ".",
) -> dict:
    """Compute correlation between two metrics for a pipeline."""
    series_a = {e["run_id"]: e["value"] for e in get_metric_series(pipeline, metric_a, base_dir)}
    series_b = {e["run_id"]: e["value"] for e in get_metric_series(pipeline, metric_b, base_dir)}
    common = sorted(set(series_a) & set(series_b))
    if not common:
        return {"pipeline": pipeline, "metric_a": metric_a, "metric_b": metric_b,
                "n": 0, "correlation": None, "strength": "insufficient data"}
    xs = [series_a[r] for r in common]
    ys = [series_b[r] for r in common]
    r = pearson_correlation(xs, ys)
    return {
        "pipeline": pipeline,
        "metric_a": metric_a,
        "metric_b": metric_b,
        "n": len(common),
        "correlation": round(r, 4) if r is not None else None,
        "strength": _classify_correlation(r),
    }


def _classify_correlation(r: Optional[float]) -> str:
    if r is None:
        return "undefined"
    abs_r = abs(r)
    if abs_r >= 0.8:
        return "strong"
    if abs_r >= 0.5:
        return "moderate"
    if abs_r >= 0.2:
        return "weak"
    return "negligible"


def format_correlation_report(result: dict) -> str:
    lines = [
        f"Pipeline : {result['pipeline']}",
        f"Metric A : {result['metric_a']}",
        f"Metric B : {result['metric_b']}",
        f"Samples  : {result['n']}",
        f"r        : {result['correlation']}",
        f"Strength : {result['strength']}",
    ]
    return "\n".join(lines)
