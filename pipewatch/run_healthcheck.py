"""Health check evaluation for pipeline runs."""

from __future__ import annotations

from typing import Any

from pipewatch.run_logger import load_run_record, list_run_records
from pipewatch.run_severity import classify_severity
from pipewatch.run_flakiness import analyze_pipeline_flakiness
from pipewatch.run_sla import load_sla_rules, check_sla


HEALTH_OK = "healthy"
HEALTH_WARN = "warning"
HEALTH_FAIL = "failing"


def _sla_status(pipeline: str, base_dir: str = ".") -> str | None:
    """Return 'violated' if any SLA rule is breached, else None."""
    rules = load_sla_rules(base_dir=base_dir)
    rule = rules.get(pipeline)
    if not rule:
        return None
    runs = list_run_records(base_dir=base_dir)
    pipeline_runs = [r for r in runs if r.get("pipeline") == pipeline]
    if not pipeline_runs:
        return None
    latest = sorted(pipeline_runs, key=lambda r: r.get("started", ""))[-1]
    result = check_sla(latest, rule)
    return "violated" if result.get("violated") else None


def evaluate_health(pipeline: str, base_dir: str = ".") -> dict[str, Any]:
    """Evaluate overall health of a pipeline based on recent runs."""
    runs = list_run_records(base_dir=base_dir)
    pipeline_runs = [
        r for r in runs if r.get("pipeline") == pipeline
    ]

    if not pipeline_runs:
        return {
            "pipeline": pipeline,
            "status": HEALTH_WARN,
            "reason": "no runs found",
            "details": {},
        }

    latest = sorted(pipeline_runs, key=lambda r: r.get("started", ""))[-1]
    severity = classify_severity(latest)
    flakiness = analyze_pipeline_flakiness(pipeline, base_dir=base_dir)
    sla = _sla_status(pipeline, base_dir=base_dir)

    issues = []
    if severity in ("high", "critical"):
        issues.append(f"latest run severity: {severity}")
    if flakiness.get("classification") in ("flaky", "very_flaky"):
        issues.append(f"pipeline is {flakiness['classification']}")
    if sla == "violated":
        issues.append("SLA violated")

    if any("critical" in i or "SLA" in i for i in issues):
        status = HEALTH_FAIL
    elif issues:
        status = HEALTH_WARN
    else:
        status = HEALTH_OK

    return {
        "pipeline": pipeline,
        "status": status,
        "reason": "; ".join(issues) if issues else "all checks passed",
        "details": {
            "latest_run_id": latest.get("run_id"),
            "severity": severity,
            "flakiness": flakiness.get("classification"),
            "sla": sla or "ok",
        },
    }


def format_health_report(result: dict[str, Any]) -> str:
    """Format a health check result as a human-readable string."""
    lines = [
        f"Pipeline : {result['pipeline']}",
        f"Health   : {result['status'].upper()}",
        f"Reason   : {result['reason']}",
        "Details  :",
    ]
    for key, val in result.get("details", {}).items():
        lines.append(f"  {key}: {val}")
    return "\n".join(lines)
