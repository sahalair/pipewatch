"""CLI entry-point for tag management commands."""

from __future__ import annotations

import argparse
import sys

from pipewatch.tag_manager import (
    add_tags,
    filter_runs_by_tag,
    get_tags,
    remove_tags,
)

DEFAULT_RUN_DIR = ".pipewatch/runs"


def cmd_add(args: argparse.Namespace) -> None:
    updated = add_tags(args.run_dir, args.run_id, args.tags)
    print(f"Tags for {args.run_id}: {', '.join(updated) if updated else '(none)'}")


def cmd_remove(args: argparse.Namespace) -> None:
    updated = remove_tags(args.run_dir, args.run_id, args.tags)
    print(f"Tags for {args.run_id}: {', '.join(updated) if updated else '(none)'}")


def cmd_list(args: argparse.Namespace) -> None:
    tags = get_tags(args.run_dir, args.run_id)
    if tags:
        for t in tags:
            print(t)
    else:
        print(f"No tags for run {args.run_id}")


def cmd_filter(args: argparse.Namespace) -> None:
    run_ids = filter_runs_by_tag(args.run_dir, args.tag)
    if run_ids:
        for rid in run_ids:
            print(rid)
    else:
        print(f"No runs tagged '{args.tag}'")


def build_parser(run_dir: str = DEFAULT_RUN_DIR) -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="pipewatch-tag", description="Manage run tags")
    parser.set_defaults(run_dir=run_dir)
    sub = parser.add_subparsers(dest="command", required=True)

    p_add = sub.add_parser("add", help="Add tags to a run")
    p_add.add_argument("run_id")
    p_add.add_argument("tags", nargs="+")
    p_add.set_defaults(func=cmd_add)

    p_rm = sub.add_parser("remove", help="Remove tags from a run")
    p_rm.add_argument("run_id")
    p_rm.add_argument("tags", nargs="+")
    p_rm.set_defaults(func=cmd_remove)

    p_ls = sub.add_parser("list", help="List tags for a run")
    p_ls.add_argument("run_id")
    p_ls.set_defaults(func=cmd_list)

    p_flt = sub.add_parser("filter", help="List runs with a given tag")
    p_flt.add_argument("tag")
    p_flt.set_defaults(func=cmd_filter)

    return parser


def main(argv: list[str] | None = None) -> None:
    parser = build_parser()
    args = parser.parse_args(argv)
    args.func(args)


if __name__ == "__main__":  # pragma: no cover
    main()
