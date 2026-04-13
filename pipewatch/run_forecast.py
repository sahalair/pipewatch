"""Forecast future run counts and durations based on historical throughput and duration data."""

from typing import Optional
from pipewatch.run_throughput import compute_throughput
from pipewatch.run_duration import get_durations_for_pipeline, summarize_durations


def forecast_run_count(
    pipeline: str,
    window: str = "day",
    lookahead: int = 7,
    base_dir: str = ".pipewatch",
) -> dict:
    """Forecast expected run count over the next `lookahead` windows."""
    throughput = compute_throughput(pipeline=pipeline, window=window, base_dir=base_dir)
    rate = throughput.get("rate", 0.0)
    return {
        "pipeline": pipeline,
        "window": window,
        "lookahead": lookahead,
        "rate_per_window": rate,
        "forecast_total": round(rate * lookahead, 2),
    }


def forecast_duration(
    pipeline: str,
    lookahead: int = 5,
    limit: int = 20,
    base_dir: str = ".pipewatch",
) -> dict:
    """Forecast expected duration for upcoming runs using recent average."""
    durations = get_durations_for_pipeline(pipeline, limit=limit, base_dir=base_dir)
    summary = summarize_durations(durations)
    avg = summary.get("mean")
    if avg is None:
        return {
            "pipeline": pipeline,
            "lookahead": lookahead,
            "avg_duration_seconds": None,
            "forecast_total_seconds": None,
            "message": "Insufficient data for forecast.",
        }
    return {
        "pipeline": pipeline,
        "lookahead": lookahead,
        "avg_duration_seconds": round(avg, 2),
        "forecast_total_seconds": round(avg * lookahead, 2),
    }


def format_forecast_report(count_forecast: dict, duration_forecast: dict) -> str:
    """Format a human-readable forecast report."""
    lines = [
        f"Forecast Report — Pipeline: {count_forecast['pipeline']}",
        "-" * 50,
        f"Window       : {count_forecast['window']}",
        f"Lookahead    : {count_forecast['lookahead']} windows",
        f"Rate/window  : {count_forecast['rate_per_window']}",
        f"Forecast runs: {count_forecast['forecast_total']}",
        "",
        f"Avg duration : {duration_forecast.get('avg_duration_seconds', 'N/A')} s",
        f"Forecast time: {duration_forecast.get('forecast_total_seconds', 'N/A')} s",
    ]
    msg = duration_forecast.get("message")
    if msg:
        lines.append(f"Note         : {msg}")
    return "\n".join(lines)
