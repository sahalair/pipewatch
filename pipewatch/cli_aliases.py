"""CLI commands for managing run aliases."""

import argparse
import sys

from pipewatch.run_aliases import (
    set_alias,
    remove_alias,
    resolve_alias,
    list_aliases,
    find_aliases_for_run,
)

DEFAULT_DIR = ".pipewatch"


def cmd_set(args) -> None:
    set_alias(args.base_dir, args.alias, args.run_id)
    print(f"Alias '{args.alias}' -> '{args.run_id}' saved.")


def cmd_remove(args) -> None:
    removed = remove_alias(args.base_dir, args.alias)
    if removed:
        print(f"Alias '{args.alias}' removed.")
    else:
        print(f"Alias '{args.alias}' not found.", file=sys.stderr)
        sys.exit(1)


def cmd_resolve(args) -> None:
    run_id = resolve_alias(args.base_dir, args.alias)
    if run_id is None:
        print(f"Alias '{args.alias}' not found.", file=sys.stderr)
        sys.exit(1)
    print(run_id)


def cmd_list(args) -> None:
    aliases = list_aliases(args.base_dir)
    if not aliases:
        print("No aliases defined.")
        return
    for alias, run_id in sorted(aliases.items()):
        print(f"{alias:30s} -> {run_id}")


def cmd_find(args) -> None:
    found = find_aliases_for_run(args.base_dir, args.run_id)
    if not found:
        print(f"No aliases found for run '{args.run_id}'.")
    else:
        for alias in found:
            print(alias)


def build_parser(parent=None) -> argparse.ArgumentParser:
    parser = (
        parent.add_parser("alias", help="Manage run aliases")
        if parent
        else argparse.ArgumentParser(prog="pipewatch-alias")
    )
    parser.add_argument("--base-dir", default=DEFAULT_DIR)
    sub = parser.add_subparsers(dest="subcmd", required=True)

    p_set = sub.add_parser("set", help="Set an alias for a run")
    p_set.add_argument("alias")
    p_set.add_argument("run_id")
    p_set.set_defaults(func=cmd_set)

    p_rm = sub.add_parser("remove", help="Remove an alias")
    p_rm.add_argument("alias")
    p_rm.set_defaults(func=cmd_remove)

    p_res = sub.add_parser("resolve", help="Resolve alias to run ID")
    p_res.add_argument("alias")
    p_res.set_defaults(func=cmd_resolve)

    p_list = sub.add_parser("list", help="List all aliases")
    p_list.set_defaults(func=cmd_list)

    p_find = sub.add_parser("find", help="Find aliases for a run ID")
    p_find.add_argument("run_id")
    p_find.set_defaults(func=cmd_find)

    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
