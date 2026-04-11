"""Timeline view: ordered sequence of run events for a pipeline."""

from __future__ import annotations

from typing import Any

from pipewatch.run_logger import list_run_records


def get_pipeline_timeline(
    pipeline: str,
    limit: int = 20,
    base_dir: str = ".pipewatch",
) -> list[dict[str, Any]]:
    """Return the most recent *limit* runs for *pipeline*, oldest-first."""
    all_runs: list[dict[str, Any]] = list_run_records(base_dir=base_dir)
    pipeline_runs = [
        r for r in all_runs if r.get("pipeline") == pipeline
    ]
    # list_run_records returns newest-first; reverse for timeline order
    pipeline_runs = list(reversed(pipeline_runs))
    return pipeline_runs[-limit:]


def build_timeline_entries(
    runs: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    """Annotate each run with duration_seconds and a relative index."""
    entries: list[dict[str, Any]] = []
    for idx, run in enumerate(runs):
        started = run.get("started_at", "")
        finished = run.get("finished_at", "")
        duration: float | None = None
        if started and finished:
            from datetime import datetime

            fmt = "%Y-%m-%dT%H:%M:%S"
            try:
                s = datetime.fromisoformat(started)
                f = datetime.fromisoformat(finished)
                duration = round((f - s).total_seconds(), 3)
            except ValueError:
                pass
        entries.append(
            {
                "index": idx + 1,
                "run_id": run.get("run_id", ""),
                "status": run.get("status", "unknown"),
                "started_at": started,
                "finished_at": finished,
                "duration_seconds": duration,
                "pipeline": run.get("pipeline", ""),
            }
        )
    return entries


def format_timeline(entries: list[dict[str, Any]]) -> str:
    """Return a human-readable timeline string."""
    if not entries:
        return "No runs found."
    lines: list[str] = []
    header = f"{'#':<4} {'Run ID':<36} {'Status':<10} {'Started':<20} {'Duration':>12}"
    lines.append(header)
    lines.append("-" * len(header))
    for e in entries:
        dur = f"{e['duration_seconds']:.1f}s" if e["duration_seconds"] is not None else "n/a"
        started = e["started_at"][:19] if e["started_at"] else "n/a"
        lines.append(
            f"{e['index']:<4} {e['run_id']:<36} {e['status']:<10} {started:<20} {dur:>12}"
        )
    return "\n".join(lines)
