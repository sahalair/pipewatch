"""CLI commands for managing run bookmarks."""

import argparse
import sys
from pathlib import Path

from pipewatch.run_bookmarks import (
    add_bookmark,
    remove_bookmark,
    resolve_bookmark,
    list_bookmarks,
)


def cmd_add(args: argparse.Namespace) -> None:
    add_bookmark(args.alias, args.run_id)
    print(f"Bookmarked '{args.alias}' -> {args.run_id}")


def cmd_remove(args: argparse.Namespace) -> None:
    removed = remove_bookmark(args.alias)
    if removed:
        print(f"Removed bookmark '{args.alias}'")
    else:
        print(f"No bookmark named '{args.alias}'")


def cmd_resolve(args: argparse.Namespace) -> None:
    run_id = resolve_bookmark(args.alias)
    if run_id:
        print(run_id)
    else:
        print(f"No bookmark named '{args.alias}'", file=sys.stderr)
        sys.exit(1)


def cmd_list(args: argparse.Namespace) -> None:
    entries = list_bookmarks()
    if not entries:
        print("No bookmarks saved.")
        return
    width = max(len(e["alias"]) for e in entries)
    for entry in entries:
        print(f"{entry['alias']:<{width}}  {entry['run_id']}")


def build_parser(subparsers=None) -> argparse.ArgumentParser:
    if subparsers is None:
        parser = argparse.ArgumentParser(description="Manage run bookmarks")
        sub = parser.add_subparsers(dest="command")
    else:
        parser = subparsers.add_parser("bookmark", help="Manage run bookmarks")
        sub = parser.add_subparsers(dest="command")

    p_add = sub.add_parser("add", help="Bookmark a run with an alias")
    p_add.add_argument("alias", help="Short alias for the bookmark")
    p_add.add_argument("run_id", help="Run ID to bookmark")
    p_add.set_defaults(func=cmd_add)

    p_rm = sub.add_parser("remove", help="Remove a bookmark")
    p_rm.add_argument("alias", help="Alias to remove")
    p_rm.set_defaults(func=cmd_remove)

    p_res = sub.add_parser("resolve", help="Resolve alias to run ID")
    p_res.add_argument("alias", help="Alias to resolve")
    p_res.set_defaults(func=cmd_resolve)

    p_ls = sub.add_parser("list", help="List all bookmarks")
    p_ls.set_defaults(func=cmd_list)

    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()
    if not hasattr(args, "func"):
        parser.print_help()
        sys.exit(1)
    args.func(args)


if __name__ == "__main__":
    main()
