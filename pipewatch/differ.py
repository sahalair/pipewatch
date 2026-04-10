"""Diff utilities for comparing pipeline run outputs."""

from __future__ import annotations

import difflib
import json
from typing import Any


def diff_texts(old: str, new: str, label_old: str = "old", label_new: str = "new") -> list[str]:
    """Return a unified diff between two text strings as a list of lines."""
    old_lines = old.splitlines(keepends=True)
    new_lines = new.splitlines(keepends=True)
    return list(
        difflib.unified_diff(
            old_lines,
            new_lines,
            fromfile=label_old,
            tofile=label_new,
        )
    )


def diff_json_serializable(old: Any, new: Any, label_old: str = "old", label_new: str = "new") -> list[str]:
    """Return a unified diff between two JSON-serializable objects."""
    old_text = json.dumps(old, indent=2, sort_keys=True)
    new_text = json.dumps(new, indent=2, sort_keys=True)
    return diff_texts(old_text, new_text, label_old=label_old, label_new=label_new)


def diff_summary(diff_lines: list[str]) -> dict[str, int]:
    """Return a summary dict with counts of added, removed, and unchanged lines."""
    added = sum(1 for line in diff_lines if line.startswith("+") and not line.startswith("+++"))
    removed = sum(1 for line in diff_lines if line.startswith("-") and not line.startswith("---"))
    return {"added": added, "removed": removed, "changed": added + removed}


def format_diff(diff_lines: list[str], colorize: bool = False) -> str:
    """Format diff lines into a single string, optionally with ANSI color codes."""
    if not colorize:
        return "".join(diff_lines)

    result: list[str] = []
    for line in diff_lines:
        if line.startswith("+") and not line.startswith("+++"):
            result.append(f"\033[32m{line}\033[0m")
        elif line.startswith("-") and not line.startswith("---"):
            result.append(f"\033[31m{line}\033[0m")
        elif line.startswith("@@"):
            result.append(f"\033[36m{line}\033[0m")
        else:
            result.append(line)
    return "".join(result)
