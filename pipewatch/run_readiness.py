"""Evaluate whether a pipeline is ready to run based on health, quota, and blackout windows."""

from __future__ import annotations

from typing import Any

from pipewatch.run_healthcheck import evaluate_health
from pipewatch.run_quota import load_quotas
from pipewatch.run_blackout import load_blackout_windows


def _is_in_blackout(pipeline: str, now_iso: str, windows: list[dict]) -> tuple[bool, str]:
    """Return (True, reason) if pipeline is currently in a blackout window."""
    from datetime import datetime, timezone

    try:
        now = datetime.fromisoformat(now_iso.replace("Z", "+00:00"))
    except ValueError:
        return False, ""

    for w in windows:
        pipelines = w.get("pipelines") or []
        if pipelines and pipeline not in pipelines:
            continue
        try:
            start = datetime.fromisoformat(w["start"].replace("Z", "+00:00"))
            end = datetime.fromisoformat(w["end"].replace("Z", "+00:00"))
        except (KeyError, ValueError):
            continue
        if start <= now <= end:
            reason = w.get("reason") or w.get("name", "blackout window active")
            return True, reason
    return False, ""


def evaluate_readiness(
    pipeline: str,
    base_dir: str = ".",
    now_iso: str | None = None,
) -> dict[str, Any]:
    """Evaluate pipeline readiness and return a structured result."""
    from datetime import datetime, timezone

    if now_iso is None:
        now_iso = datetime.now(timezone.utc).isoformat()

    issues: list[str] = []
    ready = True

    # Health check
    health = evaluate_health(pipeline, base_dir=base_dir)
    if health.get("status") != "ok":
        issues.append(f"health: {health.get('reason', 'unhealthy')}")
        ready = False

    # Quota check
    quotas = load_quotas(base_dir=base_dir)
    quota = quotas.get(pipeline)
    if quota is not None:
        used = quota.get("used", 0)
        limit = quota.get("limit", 0)
        if limit > 0 and used >= limit:
            issues.append(f"quota: used {used}/{limit}")
            ready = False

    # Blackout window check
    windows = load_blackout_windows(base_dir=base_dir)
    in_blackout, blackout_reason = _is_in_blackout(pipeline, now_iso, windows)
    if in_blackout:
        issues.append(f"blackout: {blackout_reason}")
        ready = False

    return {
        "pipeline": pipeline,
        "ready": ready,
        "issues": issues,
        "checked_at": now_iso,
    }


def format_readiness_report(result: dict[str, Any]) -> str:
    """Format a readiness result as a human-readable string."""
    status = "READY" if result["ready"] else "NOT READY"
    lines = [f"Pipeline : {result['pipeline']}", f"Status   : {status}"]
    if result["issues"]:
        lines.append("Issues   :")
        for issue in result["issues"]:
            lines.append(f"  - {issue}")
    lines.append(f"Checked  : {result['checked_at']}")
    return "\n".join(lines)
