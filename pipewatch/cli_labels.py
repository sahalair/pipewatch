"""CLI commands for managing run labels."""

from __future__ import annotations

import argparse
import sys

from pipewatch.run_labels import (
    filter_by_label,
    get_labels,
    remove_label,
    set_label,
)

DEFAULT_DIR = ".pipewatch"


def cmd_set(args: argparse.Namespace) -> None:
    labels = set_label(args.base_dir, args.run_id, args.key, args.value)
    print(f"Label set for run '{args.run_id}': {args.key}={args.value}")
    print("Current labels:", labels)


def cmd_remove(args: argparse.Namespace) -> None:
    labels = remove_label(args.base_dir, args.run_id, args.key)
    print(f"Label '{args.key}' removed from run '{args.run_id}'.")
    print("Remaining labels:", labels)


def cmd_list(args: argparse.Namespace) -> None:
    labels = get_labels(args.base_dir, args.run_id)
    if not labels:
        print(f"No labels for run '{args.run_id}'.")
        return
    for key, value in sorted(labels.items()):
        print(f"  {key}: {value}")


def cmd_filter(args: argparse.Namespace) -> None:
    value = getattr(args, "value", None)
    run_ids = filter_by_label(args.base_dir, args.key, value)
    if not run_ids:
        print("No runs match the given label criteria.")
        return
    for run_id in run_ids:
        print(run_id)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="pipewatch-labels",
        description="Manage labels for pipeline runs.",
    )
    parser.add_argument("--base-dir", default=DEFAULT_DIR, help="Storage directory.")
    sub = parser.add_subparsers(dest="command", required=True)

    p_set = sub.add_parser("set", help="Set a label on a run.")
    p_set.add_argument("run_id")
    p_set.add_argument("key")
    p_set.add_argument("value")
    p_set.set_defaults(func=cmd_set)

    p_rm = sub.add_parser("remove", help="Remove a label from a run.")
    p_rm.add_argument("run_id")
    p_rm.add_argument("key")
    p_rm.set_defaults(func=cmd_remove)

    p_ls = sub.add_parser("list", help="List labels for a run.")
    p_ls.add_argument("run_id")
    p_ls.set_defaults(func=cmd_list)

    p_flt = sub.add_parser("filter", help="Find runs matching a label.")
    p_flt.add_argument("key")
    p_flt.add_argument("value", nargs="?", default=None)
    p_flt.set_defaults(func=cmd_filter)

    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
