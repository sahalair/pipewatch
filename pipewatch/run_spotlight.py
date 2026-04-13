"""run_spotlight.py — Highlight notable runs based on score, severity, and anomaly flags."""

from typing import Optional
from pipewatch.run_logger import list_run_records
from pipewatch.run_scoring import compute_score
from pipewatch.run_severity import classify_severity
from pipewatch.run_anomaly import detect_anomaly


def _safe_score(run: dict) -> float:
    try:
        return compute_score(run["run_id"])["score"]
    except Exception:
        return 0.0


def spotlight_runs(
    pipeline: Optional[str] = None,
    limit: int = 5,
    base_dir: str = ".pipewatch",
) -> list:
    """Return top notable runs sorted by score descending."""
    runs = list_run_records(base_dir=base_dir)
    if pipeline:
        runs = [r for r in runs if r.get("pipeline") == pipeline]

    results = []
    for run in runs:
        run_id = run.get("run_id", "")
        severity = classify_severity(run)
        score_data = {}
        try:
            score_data = compute_score(run_id, base_dir=base_dir)
        except Exception:
            pass
        score = score_data.get("score", 0.0)
        grade = score_data.get("grade", "?")
        anomaly = detect_anomaly(run_id, "duration", base_dir=base_dir)
        results.append({
            "run_id": run_id,
            "pipeline": run.get("pipeline", ""),
            "status": run.get("status", "unknown"),
            "severity": severity,
            "score": score,
            "grade": grade,
            "anomaly_flag": anomaly is not None and anomaly.get("is_anomaly", False),
        })

    results.sort(key=lambda r: r["score"], reverse=True)
    return results[:limit]


def format_spotlight_report(entries: list) -> str:
    """Format spotlight entries into a human-readable string."""
    if not entries:
        return "No spotlight runs found."
    lines = ["=== Run Spotlight ==="]
    for e in entries:
        flag = " [ANOMALY]" if e["anomaly_flag"] else ""
        lines.append(
            f"  {e['run_id'][:12]}  pipeline={e['pipeline']}  "
            f"status={e['status']}  severity={e['severity']}  "
            f"score={e['score']:.1f} ({e['grade']}){flag}"
        )
    return "\n".join(lines)
