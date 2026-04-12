"""Policy rules for escalating or suppressing severity levels."""

from typing import Dict, Any, List, Optional

_DEFAULT_POLICIES: List[Dict[str, Any]] = []


def create_policy(
    name: str,
    pipeline: Optional[str] = None,
    min_severity: str = "ok",
    action: str = "alert",
    notes: str = "",
) -> Dict[str, Any]:
    """Create a severity policy rule dict."""
    valid_actions = {"alert", "suppress", "escalate"}
    if action not in valid_actions:
        raise ValueError(f"action must be one of {valid_actions}, got {action!r}")
    from pipewatch.run_severity import SEVERITY_LEVELS
    if min_severity not in SEVERITY_LEVELS:
        raise ValueError(f"min_severity must be one of {SEVERITY_LEVELS}")
    return {
        "name": name,
        "pipeline": pipeline,
        "min_severity": min_severity,
        "action": action,
        "notes": notes,
    }


def apply_policies(
    run: Dict[str, Any],
    severity: str,
    policies: List[Dict[str, Any]],
) -> str:
    """Apply matching policies to a severity level and return the effective level."""
    from pipewatch.run_severity import severity_rank, SEVERITY_LEVELS
    effective = severity
    pipeline = run.get("pipeline")
    for policy in policies:
        if policy.get("pipeline") and policy["pipeline"] != pipeline:
            continue
        if severity_rank(severity) < severity_rank(policy["min_severity"]):
            continue
        action = policy.get("action", "alert")
        if action == "suppress":
            effective = "ok"
        elif action == "escalate":
            idx = SEVERITY_LEVELS.index(effective)
            if idx < len(SEVERITY_LEVELS) - 1:
                effective = SEVERITY_LEVELS[idx + 1]
    return effective


def filter_runs_by_effective_severity(
    runs: List[Dict[str, Any]],
    min_level: str,
    policies: Optional[List[Dict[str, Any]]] = None,
) -> List[Dict[str, Any]]:
    """Return runs whose effective severity meets or exceeds min_level."""
    from pipewatch.run_severity import classify_severity, severity_rank
    active_policies = policies or []
    result = []
    for run in runs:
        base = classify_severity(run)
        effective = apply_policies(run, base, active_policies)
        if severity_rank(effective) >= severity_rank(min_level):
            result.append(run)
    return result
