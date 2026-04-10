"""Compare two snapshots and report differences."""

from typing import Optional

from pipewatch.differ import diff_json_serializable, diff_summary, format_diff
from pipewatch.output_hasher import hash_json_serializable, hashes_differ
from pipewatch.snapshot import load_snapshot


def diff_snapshots(
    name_a: str,
    name_b: str,
    snapshot_dir: str = ".pipewatch/snapshots",
) -> dict:
    """Load two snapshots by name and compute their diff.

    Returns a result dict with keys:
        - name_a, name_b: snapshot names
        - hash_a, hash_b: content hashes
        - changed: bool indicating whether data differs
        - diff_lines: list of unified diff lines
        - summary: human-readable summary string
    """
    snap_a = load_snapshot(name_a, snapshot_dir)
    snap_b = load_snapshot(name_b, snapshot_dir)

    data_a = snap_a["data"]
    data_b = snap_b["data"]

    hash_a = hash_json_serializable(data_a)
    hash_b = hash_json_serializable(data_b)
    changed = hashes_differ(hash_a, hash_b)

    diff_lines = diff_json_serializable(data_a, data_b, label_a=name_a, label_b=name_b)
    summary = diff_summary(diff_lines)

    return {
        "name_a": name_a,
        "name_b": name_b,
        "hash_a": hash_a,
        "hash_b": hash_b,
        "changed": changed,
        "diff_lines": diff_lines,
        "summary": summary,
    }


def print_snapshot_diff(
    name_a: str,
    name_b: str,
    snapshot_dir: str = ".pipewatch/snapshots",
    color: bool = True,
) -> None:
    """Print a colored diff between two named snapshots to stdout."""
    result = diff_snapshots(name_a, name_b, snapshot_dir)

    if not result["changed"]:
        print(f"Snapshots '{name_a}' and '{name_b}' are identical.")
        return

    print(f"Diff: '{name_a}' → '{name_b}'")
    print(f"  hash_a: {result['hash_a']}")
    print(f"  hash_b: {result['hash_b']}")
    print(f"  {result['summary']}")
    print()
    print(format_diff(result["diff_lines"], color=color))
