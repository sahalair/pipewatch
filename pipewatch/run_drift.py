"""run_drift.py — detect configuration/environment drift between pipeline runs."""

from __future__ import annotations

from typing import Any

from pipewatch.run_environment import load_environment


DRIFT_KEYS = ["python_version", "platform", "cwd", "hostname"]


def detect_drift(
    run_id_a: str,
    run_id_b: str,
    base_dir: str = ".pipewatch",
    keys: list[str] | None = None,
) -> dict[str, Any]:
    """Compare environment snapshots for two runs and return a drift report."""
    env_a = load_environment(run_id_a, base_dir=base_dir)
    env_b = load_environment(run_id_b, base_dir=base_dir)

    watched = keys if keys is not None else DRIFT_KEYS

    drifted: list[dict[str, Any]] = []
    for key in watched:
        val_a = env_a.get(key)
        val_b = env_b.get(key)
        if val_a != val_b:
            drifted.append({"key": key, "run_a": val_a, "run_b": val_b})

    return {
        "run_a": run_id_a,
        "run_b": run_id_b,
        "drifted_keys": drifted,
        "drift_detected": len(drifted) > 0,
        "checked_keys": watched,
    }


def format_drift_report(report: dict[str, Any]) -> str:
    """Return a human-readable drift report."""
    lines: list[str] = [
        f"Drift Report: {report['run_a']} vs {report['run_b']}",
        f"Checked keys : {', '.join(report['checked_keys'])}",
        f"Drift detected: {'yes' if report['drift_detected'] else 'no'}",
    ]
    if report["drift_detected"]:
        lines.append("")
        lines.append("Changed fields:")
        for entry in report["drifted_keys"]:
            lines.append(
                f"  {entry['key']}: {entry['run_a']!r} -> {entry['run_b']!r}"
            )
    return "\n".join(lines)
