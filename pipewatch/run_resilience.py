"""Compute a resilience score for a pipeline based on recovery speed after failures."""

from typing import Optional
from pipewatch.run_logger import list_run_records
from pipewatch.run_flakiness import analyze_pipeline_flakiness
from pipewatch.run_uptime import compute_uptime


def _parse_iso(ts: str) -> float:
    from datetime import datetime, timezone
    return datetime.fromisoformat(ts).replace(tzinfo=timezone.utc).timestamp()


def _mean_recovery_time(pipeline: str, base_dir: str = ".") -> Optional[float]:
    """Return mean seconds to recover (failure -> next success) or None."""
    runs = [
        r for r in list_run_records(base_dir=base_dir)
        if r.get("pipeline") == pipeline
    ]
    runs.sort(key=lambda r: r.get("started", ""))

    recovery_times = []
    i = 0
    while i < len(runs):
        if runs[i].get("exit_code", 0) != 0:
            fail_ts_str = runs[i].get("finished") or runs[i].get("started")
            if not fail_ts_str:
                i += 1
                continue
            fail_ts = _parse_iso(fail_ts_str)
            for j in range(i + 1, len(runs)):
                if runs[j].get("exit_code", 0) == 0:
                    rec_ts_str = runs[j].get("started")
                    if rec_ts_str:
                        recovery_times.append(_parse_iso(rec_ts_str) - fail_ts)
                    i = j
                    break
            else:
                break
        i += 1

    if not recovery_times:
        return None
    return sum(recovery_times) / len(recovery_times)


def compute_resilience(pipeline: str, base_dir: str = ".") -> dict:
    """Return a resilience report dict for the given pipeline."""
    uptime = compute_uptime(pipeline, base_dir=base_dir)
    uptime_ratio = uptime.get("ratio") or 0.0

    flakiness = analyze_pipeline_flakiness(pipeline, base_dir=base_dir)
    flakiness_score = flakiness.get("score", 0.0)

    mrt = _mean_recovery_time(pipeline, base_dir=base_dir)

    # Recovery component: fast recovery = high score (cap at 3600s = 1 hour)
    if mrt is None:
        recovery_component = 1.0 if uptime_ratio >= 1.0 else 0.5
    else:
        recovery_component = max(0.0, 1.0 - mrt / 3600.0)

    stability_component = max(0.0, 1.0 - flakiness_score)
    score = round((uptime_ratio * 0.4 + stability_component * 0.3 + recovery_component * 0.3) * 100, 1)

    grade = "A" if score >= 90 else "B" if score >= 75 else "C" if score >= 60 else "D" if score >= 40 else "F"

    return {
        "pipeline": pipeline,
        "score": score,
        "grade": grade,
        "uptime_ratio": round(uptime_ratio, 4),
        "flakiness_score": round(flakiness_score, 4),
        "mean_recovery_seconds": round(mrt, 1) if mrt is not None else None,
        "recovery_component": round(recovery_component, 4),
    }


def format_resilience_report(report: dict) -> str:
    lines = [
        f"Resilience Report — {report['pipeline']}",
        f"  Score          : {report['score']} / 100  [{report['grade']}]",
        f"  Uptime Ratio   : {report['uptime_ratio']}",
        f"  Flakiness      : {report['flakiness_score']}",
        f"  Mean Recovery  : {report['mean_recovery_seconds']}s",
    ]
    return "\n".join(lines)
