"""Track and retrieve environment metadata for pipeline runs."""

import json
import os
from pathlib import Path
from typing import Any

_ENV_DIR = Path(".pipewatch") / "environments"


def _env_path(run_id: str) -> Path:
    return _ENV_DIR / f"{run_id}.json"


def load_environment(run_id: str) -> dict[str, Any]:
    """Load environment metadata for a run. Returns empty dict if not found."""
    path = _env_path(run_id)
    if not path.exists():
        return {}
    with path.open() as f:
        return json.load(f)


def save_environment(run_id: str, env: dict[str, Any]) -> None:
    """Persist environment metadata for a run."""
    _ENV_DIR.mkdir(parents=True, exist_ok=True)
    with _env_path(run_id).open("w") as f:
        json.dump(env, f, indent=2)


def capture_environment(extras: dict[str, Any] | None = None) -> dict[str, Any]:
    """Capture current process environment metadata.

    Collects a safe subset of env vars plus any caller-supplied extras.
    """
    safe_keys = [
        "PATH", "VIRTUAL_ENV", "CONDA_DEFAULT_ENV",
        "USER", "HOME", "SHELL", "LANG", "TZ",
        "CI", "GITHUB_ACTIONS", "GITHUB_WORKFLOW", "GITHUB_RUN_ID",
    ]
    env: dict[str, Any] = {
        "python_version": _python_version(),
        "cwd": os.getcwd(),
        "env_vars": {k: os.environ[k] for k in safe_keys if k in os.environ},
    }
    if extras:
        env["extras"] = extras
    return env


def record_environment(
    run_id: str,
    extras: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Capture and save environment metadata for a run. Returns the captured dict."""
    env = capture_environment(extras=extras)
    save_environment(run_id, env)
    return env


def compare_environments(
    run_id_a: str,
    run_id_b: str,
) -> dict[str, Any]:
    """Return a diff summary between two run environments."""
    env_a = load_environment(run_id_a)
    env_b = load_environment(run_id_b)

    changed: dict[str, dict[str, Any]] = {}
    all_keys = set(env_a) | set(env_b)
    for key in sorted(all_keys):
        va, vb = env_a.get(key), env_b.get(key)
        if va != vb:
            changed[key] = {"a": va, "b": vb}

    return {
        "run_id_a": run_id_a,
        "run_id_b": run_id_b,
        "changed": changed,
        "identical": len(changed) == 0,
    }


def _python_version() -> str:
    import sys
    return sys.version.split()[0]
