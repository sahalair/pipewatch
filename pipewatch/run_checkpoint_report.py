"""Generate a human-readable checkpoint report for a run."""

from typing import Dict, List

from pipewatch.run_checkpoints import load_checkpoints, checkpoint_summary

_STATUS_ICON = {"ok": "✓", "warn": "!", "fail": "✗"}


def build_checkpoint_report(run_id: str) -> Dict:
    """Build a structured checkpoint report for a run."""
    checkpoints = load_checkpoints(run_id)
    summary = checkpoint_summary(run_id)
    has_failures = summary["fail"] > 0
    has_warnings = summary["warn"] > 0
    overall = "fail" if has_failures else ("warn" if has_warnings else "ok")
    return {
        "run_id": run_id,
        "overall": overall,
        "summary": summary,
        "checkpoints": checkpoints,
    }


def format_checkpoint_report(report: Dict) -> str:
    """Format a checkpoint report as a printable string."""
    lines: List[str] = []
    run_id = report["run_id"]
    overall = report["overall"]
    icon = _STATUS_ICON.get(overall, "?")
    lines.append(f"Checkpoint Report — Run: {run_id}")
    lines.append(f"Overall status: {icon} {overall.upper()}")
    s = report["summary"]
    lines.append(f"  Total: {s['total']}  OK: {s['ok']}  Warn: {s['warn']}  Fail: {s['fail']}")
    lines.append("")
    if not report["checkpoints"]:
        lines.append("  (no checkpoints recorded)")
        return "\n".join(lines)
    lines.append(f"  {'Status':<6}  {'Name':<30}  {'Timestamp':<28}  Message")
    lines.append("  " + "-" * 80)
    for c in report["checkpoints"]:
        status_icon = _STATUS_ICON.get(c["status"], "?")
        msg = c.get("message") or ""
        lines.append(f"  {status_icon} {c['status']:<4}  {c['name']:<30}  {c['timestamp']:<28}  {msg}")
    return "\n".join(lines)


def checkpoints_passed(run_id: str) -> bool:
    """Return True if all checkpoints for a run have status 'ok'."""
    report = build_checkpoint_report(run_id)
    return report["overall"] == "ok"
