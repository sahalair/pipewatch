"""Run longevity: measures how long a pipeline has been consistently active."""
from datetime import datetime, timezone
from typing import Optional
from pipewatch.run_logger import list_run_records


def _parse_iso(ts: str) -> datetime:
    return datetime.fromisoformat(ts).replace(tzinfo=timezone.utc)


def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


def compute_longevity(pipeline: str, base_dir: str = ".") -> dict:
    """Compute longevity metrics for a pipeline."""
    records = [
        r for r in list_run_records(base_dir=base_dir)
        if r.get("pipeline") == pipeline and r.get("started")
    ]

    if not records:
        return {
            "pipeline": pipeline,
            "run_count": 0,
            "first_run": None,
            "last_run": None,
            "age_days": None,
            "active_days": None,
            "activity_ratio": None,
            "grade": "N/A",
        }

    starts = sorted([_parse_iso(r["started"]) for r in records])
    first = starts[0]
    last = starts[-1]
    now = _now_utc()
    age_days = max((now - first).days, 1)

    unique_days = len({s.date() for s in starts})
    activity_ratio = round(unique_days / age_days, 4)

    return {
        "pipeline": pipeline,
        "run_count": len(records),
        "first_run": first.isoformat(),
        "last_run": last.isoformat(),
        "age_days": age_days,
        "active_days": unique_days,
        "activity_ratio": activity_ratio,
        "grade": _grade_longevity(activity_ratio, age_days),
    }


def _grade_longevity(ratio: float, age_days: int) -> str:
    if age_days < 7:
        return "New"
    if ratio >= 0.8:
        return "A"
    if ratio >= 0.6:
        return "B"
    if ratio >= 0.4:
        return "C"
    if ratio >= 0.2:
        return "D"
    return "F"


def format_longevity_report(result: dict) -> str:
    if result["run_count"] == 0:
        return f"[longevity] {result['pipeline']}: no runs recorded."
    lines = [
        f"[longevity] {result['pipeline']}",
        f"  Grade          : {result['grade']}",
        f"  Run count      : {result['run_count']}",
        f"  First run      : {result['first_run']}",
        f"  Last run       : {result['last_run']}",
        f"  Age (days)     : {result['age_days']}",
        f"  Active days    : {result['active_days']}",
        f"  Activity ratio : {result['activity_ratio']}",
    ]
    return "\n".join(lines)
