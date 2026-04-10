"""CLI commands for snapshot management in pipewatch."""

import argparse
import sys

from pipewatch.snapshot import (
    delete_snapshot,
    list_snapshots,
    load_snapshot,
    save_snapshot,
    snapshot_exists,
)
from pipewatch.snapshot_diff import print_snapshot_diff

DEFAULT_DIR = ".pipewatch/snapshots"


def cmd_save(args: argparse.Namespace) -> int:
    """Save stdin JSON or a literal value as a named snapshot."""
    import json

    raw = sys.stdin.read().strip()
    try:
        data = json.loads(raw)
    except json.JSONDecodeError as exc:
        print(f"Error: invalid JSON input — {exc}", file=sys.stderr)
        return 1

    path = save_snapshot(args.name, data, snapshot_dir=args.dir)
    print(f"Snapshot '{args.name}' saved to {path}")
    return 0


def cmd_list(args: argparse.Namespace) -> int:
    """List all available snapshots."""
    names = list_snapshots(snapshot_dir=args.dir)
    if not names:
        print("No snapshots found.")
    else:
        for name in names:
            print(name)
    return 0


def cmd_diff(args: argparse.Namespace) -> int:
    """Diff two snapshots by name."""
    for name in (args.snapshot_a, args.snapshot_b):
        if not snapshot_exists(name, snapshot_dir=args.dir):
            print(f"Error: snapshot '{name}' not found.", file=sys.stderr)
            return 1
    print_snapshot_diff(
        args.snapshot_a, args.snapshot_b, snapshot_dir=args.dir, color=not args.no_color
    )
    return 0


def cmd_delete(args: argparse.Namespace) -> int:
    """Delete a snapshot by name."""
    removed = delete_snapshot(args.name, snapshot_dir=args.dir)
    if removed:
        print(f"Snapshot '{args.name}' deleted.")
    else:
        print(f"Snapshot '{args.name}' not found.", file=sys.stderr)
        return 1
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="pipewatch snapshot", description="Manage pipeline output snapshots."
    )
    parser.add_argument("--dir", default=DEFAULT_DIR, help="Snapshot storage directory.")
    sub = parser.add_subparsers(dest="command", required=True)

    p_save = sub.add_parser("save", help="Save a snapshot from stdin JSON.")
    p_save.add_argument("name", help="Snapshot name.")
    p_save.set_defaults(func=cmd_save)

    p_list = sub.add_parser("list", help="List all snapshots.")
    p_list.set_defaults(func=cmd_list)

    p_diff = sub.add_parser("diff", help="Diff two snapshots.")
    p_diff.add_argument("snapshot_a")
    p_diff.add_argument("snapshot_b")
    p_diff.add_argument("--no-color", action="store_true")
    p_diff.set_defaults(func=cmd_diff)

    p_del = sub.add_parser("delete", help="Delete a snapshot.")
    p_del.add_argument("name")
    p_del.set_defaults(func=cmd_delete)

    return parser


def main(argv=None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    return args.func(args)
