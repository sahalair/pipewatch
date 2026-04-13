"""Run saturation: measures how close a pipeline is to its capacity limits."""

from typing import Optional
from pipewatch.run_capacity import load_capacity
from pipewatch.run_throughput import compute_throughput


def compute_saturation(pipeline: str, base_dir: str = ".", window: str = "day") -> dict:
    """Compute saturation ratio for a pipeline vs its declared capacity."""
    capacity_data = load_capacity(base_dir=base_dir)
    rule = capacity_data.get(pipeline)

    throughput = compute_throughput(base_dir=base_dir, window=window)
    pipeline_tp = next(
        (r for r in throughput if r.get("pipeline") == pipeline), None
    )
    actual = pipeline_tp["count"] if pipeline_tp else 0

    if rule is None:
        return {
            "pipeline": pipeline,
            "actual": actual,
            "capacity": None,
            "saturation": None,
            "grade": "N/A",
            "warning": "No capacity rule defined.",
        }

    max_runs = rule.get("max_runs", 0)
    saturation = round(actual / max_runs, 4) if max_runs > 0 else None
    grade = _grade_saturation(saturation)

    return {
        "pipeline": pipeline,
        "actual": actual,
        "capacity": max_runs,
        "saturation": saturation,
        "grade": grade,
        "warning": "Over capacity!" if saturation is not None and saturation > 1.0 else None,
    }


def _grade_saturation(saturation: Optional[float]) -> str:
    if saturation is None:
        return "N/A"
    if saturation <= 0.5:
        return "LOW"
    if saturation <= 0.75:
        return "MODERATE"
    if saturation <= 0.9:
        return "HIGH"
    if saturation <= 1.0:
        return "NEAR_LIMIT"
    return "OVER_CAPACITY"


def format_saturation_report(result: dict) -> str:
    lines = [
        f"Pipeline : {result['pipeline']}",
        f"Actual   : {result['actual']}",
        f"Capacity : {result['capacity'] if result['capacity'] is not None else 'N/A'}",
        f"Saturation: {result['saturation'] if result['saturation'] is not None else 'N/A'}",
        f"Grade    : {result['grade']}",
    ]
    if result.get("warning"):
        lines.append(f"WARNING  : {result['warning']}")
    return "\n".join(lines)


def saturation_for_all_pipelines(base_dir: str = ".", window: str = "day") -> list:
    """Return saturation results for all pipelines with a capacity rule."""
    capacity_data = load_capacity(base_dir=base_dir)
    return [
        compute_saturation(pipeline, base_dir=base_dir, window=window)
        for pipeline in capacity_data
    ]
