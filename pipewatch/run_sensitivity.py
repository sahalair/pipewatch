"""Run sensitivity analysis: detect which metrics most influence pipeline outcomes."""

from typing import Optional
from pipewatch.run_correlation import get_metric_series, pearson_correlation
from pipewatch.run_logger import list_run_records


def get_outcome_series(pipeline: str, base_dir: str = ".pipewatch") -> list[float]:
    """Return 1.0 for success, 0.0 for failure for each run of the pipeline."""
    runs = [
        r for r in list_run_records(base_dir=base_dir)
        if r.get("pipeline") == pipeline
    ]
    runs.sort(key=lambda r: r.get("started", ""))
    return [1.0 if r.get("status") == "ok" else 0.0 for r in runs]


def analyze_sensitivity(
    pipeline: str,
    metrics: list[str],
    base_dir: str = ".pipewatch",
    min_samples: int = 5,
) -> list[dict]:
    """Compute correlation of each metric against pipeline success/failure."""
    outcome = get_outcome_series(pipeline, base_dir=base_dir)
    if len(outcome) < min_samples:
        return []

    results = []
    for metric in metrics:
        series = get_metric_series(pipeline, metric, base_dir=base_dir)
        if len(series) < min_samples:
            continue
        # Align lengths by taking the last N common points
        n = min(len(outcome), len(series))
        corr = pearson_correlation(outcome[-n:], series[-n:])
        if corr is None:
            continue
        results.append({
            "metric": metric,
            "correlation": round(corr, 4),
            "influence": _classify_influence(corr),
        })

    results.sort(key=lambda x: abs(x["correlation"]), reverse=True)
    return results


def _classify_influence(corr: float) -> str:
    abs_c = abs(corr)
    if abs_c >= 0.7:
        return "strong"
    if abs_c >= 0.4:
        return "moderate"
    if abs_c >= 0.2:
        return "weak"
    return "negligible"


def format_sensitivity_report(pipeline: str, results: list[dict]) -> str:
    if not results:
        return f"[sensitivity] {pipeline}: insufficient data or no metrics found."
    lines = [f"Sensitivity report for pipeline: {pipeline}", "-" * 44]
    for r in results:
        sign = "+" if r["correlation"] >= 0 else "-"
        lines.append(
            f"  {r['metric']:<24} corr={sign}{abs(r['correlation']):.4f}  ({r['influence']})"
        )
    return "\n".join(lines)
