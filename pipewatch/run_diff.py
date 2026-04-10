"""High-level helpers to diff two pipeline run records by their stored output."""

from __future__ import annotations

from pathlib import Path
from typing import Any

from .differ import diff_json_serializable, diff_summary, format_diff
from .run_logger import load_run_record


DEFAULT_STORE_DIR = Path(".pipewatch")


def diff_runs(
    run_id_old: str,
    run_id_new: str,
    output_key: str = "output",
    store_dir: Path = DEFAULT_STORE_DIR,
    colorize: bool = False,
) -> dict[str, Any]:
    """Load two run records and diff the value stored under *output_key*.

    Returns a dict with keys:
        - ``diff_lines``: raw list of unified-diff lines
        - ``formatted``: diff as a single formatted string
        - ``summary``: dict with ``added``, ``removed``, ``changed`` counts
        - ``identical``: bool, True when there are no differences
    """
    old_record = load_run_record(run_id_old, store_dir=store_dir)
    new_record = load_run_record(run_id_new, store_dir=store_dir)

    old_value = old_record.get(output_key)
    new_value = new_record.get(output_key)

    diff_lines = diff_json_serializable(
        old_value,
        new_value,
        label_old=f"{run_id_old}/{output_key}",
        label_new=f"{run_id_new}/{output_key}",
    )

    summary = diff_summary(diff_lines)
    return {
        "diff_lines": diff_lines,
        "formatted": format_diff(diff_lines, colorize=colorize),
        "summary": summary,
        "identical": summary["changed"] == 0,
    }


def print_run_diff(
    run_id_old: str,
    run_id_new: str,
    output_key: str = "output",
    store_dir: Path = DEFAULT_STORE_DIR,
    colorize: bool = True,
) -> None:
    """Convenience wrapper that prints the diff to stdout."""
    result = diff_runs(
        run_id_old,
        run_id_new,
        output_key=output_key,
        store_dir=store_dir,
        colorize=colorize,
    )
    if result["identical"]:
        print("No differences found.")
    else:
        summary = result["summary"]
        print(f"+{summary['added']} lines  -{summary['removed']} lines")
        print(result["formatted"], end="")
