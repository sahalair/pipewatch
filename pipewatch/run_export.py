"""Export run records and associated data to portable formats (JSON, CSV)."""

import csv
import io
import json
from typing import Any

from pipewatch.run_logger import load_run_record, list_run_records
from pipewatch.tag_manager import load_tags
from pipewatch.metric import load_metrics


def export_run_to_dict(run_id: str, base_dir: str = ".") -> dict[str, Any]:
    """Collect all data for a single run into a flat dictionary."""
    record = load_run_record(run_id, base_dir=base_dir)

    tags_store = load_tags(base_dir=base_dir)
    tags = tags_store.get(run_id, [])

    try:
        metrics = load_metrics(run_id, base_dir=base_dir)
    except FileNotFoundError:
        metrics = []

    return {
        "run_id": record["run_id"],
        "pipeline": record.get("pipeline", ""),
        "status": record.get("status", ""),
        "exit_code": record.get("exit_code"),
        "started_at": record.get("started_at", ""),
        "finished_at": record.get("finished_at", ""),
        "tags": tags,
        "metrics": metrics,
    }


def export_runs_to_json(run_ids: list[str], base_dir: str = ".") -> str:
    """Serialize multiple runs to a JSON string."""
    records = [export_run_to_dict(rid, base_dir=base_dir) for rid in run_ids]
    return json.dumps(records, indent=2)


def export_runs_to_csv(run_ids: list[str], base_dir: str = ".") -> str:
    """Serialize multiple runs to a CSV string (flat, no metrics/tags expansion)."""
    fieldnames = [
        "run_id",
        "pipeline",
        "status",
        "exit_code",
        "started_at",
        "finished_at",
        "tags",
        "metric_count",
    ]
    output = io.StringIO()
    writer = csv.DictWriter(output, fieldnames=fieldnames)
    writer.writeheader()

    for rid in run_ids:
        data = export_run_to_dict(rid, base_dir=base_dir)
        writer.writerow({
            "run_id": data["run_id"],
            "pipeline": data["pipeline"],
            "status": data["status"],
            "exit_code": data["exit_code"],
            "started_at": data["started_at"],
            "finished_at": data["finished_at"],
            "tags": ";".join(data["tags"]),
            "metric_count": len(data["metrics"]),
        })

    return output.getvalue()


def export_all_runs(fmt: str = "json", base_dir: str = ".") -> str:
    """Export all known runs in the requested format ('json' or 'csv')."""
    run_ids = [r["run_id"] for r in list_run_records(base_dir=base_dir)]
    if fmt == "csv":
        return export_runs_to_csv(run_ids, base_dir=base_dir)
    return export_runs_to_json(run_ids, base_dir=base_dir)
