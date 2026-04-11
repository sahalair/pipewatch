"""Compare two runs side-by-side: metrics, status, tags, and annotations."""

from __future__ import annotations

from typing import Any

from pipewatch.run_logger import load_run_record
from pipewatch.metric import load_metrics
from pipewatch.tag_manager import load_tags
from pipewatch.run_annotations import load_annotations


def compare_runs(run_id_a: str, run_id_b: str, data_dir: str = ".") -> dict[str, Any]:
    """Return a structured comparison between two runs."""
    rec_a = load_run_record(run_id_a, data_dir=data_dir)
    rec_b = load_run_record(run_id_b, data_dir=data_dir)

    metrics_a = load_metrics(run_id_a, data_dir=data_dir)
    metrics_b = load_metrics(run_id_b, data_dir=data_dir)

    all_tags = load_tags(data_dir=data_dir)
    tags_a = set(all_tags.get(run_id_a, []))
    tags_b = set(all_tags.get(run_id_b, []))

    all_annotations = load_annotations(data_dir=data_dir)
    ann_a = all_annotations.get(run_id_a, {})
    ann_b = all_annotations.get(run_id_b, {})

    metric_names = sorted(set(metrics_a) | set(metrics_b))
    metric_diff: dict[str, dict[str, Any]] = {}
    for name in metric_names:
        val_a = metrics_a.get(name, {}).get("value")
        val_b = metrics_b.get(name, {}).get("value")
        changed = val_a != val_b
        metric_diff[name] = {"run_a": val_a, "run_b": val_b, "changed": changed}

    return {
        "run_a": run_id_a,
        "run_b": run_id_b,
        "status": {
            "run_a": rec_a.get("status"),
            "run_b": rec_b.get("status"),
            "changed": rec_a.get("status") != rec_b.get("status"),
        },
        "pipeline": {
            "run_a": rec_a.get("pipeline"),
            "run_b": rec_b.get("pipeline"),
        },
        "metrics": metric_diff,
        "tags": {
            "only_in_a": sorted(tags_a - tags_b),
            "only_in_b": sorted(tags_b - tags_a),
            "common": sorted(tags_a & tags_b),
        },
        "annotations": {
            "run_a": ann_a,
            "run_b": ann_b,
        },
    }


def format_run_comparison(comparison: dict[str, Any]) -> str:
    """Return a human-readable string for a run comparison."""
    lines: list[str] = []
    a, b = comparison["run_a"], comparison["run_b"]
    lines.append(f"Compare  {a}  vs  {b}")
    lines.append("-" * 60)

    st = comparison["status"]
    marker = "*" if st["changed"] else " "
    lines.append(f"  {marker} status   : {st['run_a']}  ->  {st['run_b']}")

    lines.append("  Metrics:")
    for name, info in comparison["metrics"].items():
        m = "*" if info["changed"] else " "
        lines.append(f"    {m} {name}: {info['run_a']}  ->  {info['run_b']}")

    tags = comparison["tags"]
    if tags["only_in_a"]:
        lines.append(f"  Tags only in A : {', '.join(tags['only_in_a'])}")
    if tags["only_in_b"]:
        lines.append(f"  Tags only in B : {', '.join(tags['only_in_b'])}")
    if tags["common"]:
        lines.append(f"  Common tags    : {', '.join(tags['common'])}")

    lines.append("")
    return "\n".join(lines)
