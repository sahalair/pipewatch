"""CLI commands for managing run dependencies."""

from __future__ import annotations

import argparse
import sys

from pipewatch.run_dependencies import (
    add_dependency,
    get_dependencies,
    get_dependents,
    load_dependencies,
    remove_dependency,
)


def cmd_add(args: argparse.Namespace) -> None:
    add_dependency(args.run_id, args.upstream_id, base_dir=args.base_dir)
    print(f"Dependency added: {args.run_id} depends on {args.upstream_id}")


def cmd_remove(args: argparse.Namespace) -> None:
    removed = remove_dependency(args.run_id, args.upstream_id, base_dir=args.base_dir)
    if removed:
        print(f"Dependency removed: {args.run_id} no longer depends on {args.upstream_id}")
    else:
        print(f"No such dependency: {args.run_id} -> {args.upstream_id}", file=sys.stderr)


def cmd_list(args: argparse.Namespace) -> None:
    upstreams = get_dependencies(args.run_id, base_dir=args.base_dir)
    if not upstreams:
        print(f"No dependencies recorded for run '{args.run_id}'.")
    else:
        print(f"Dependencies of '{args.run_id}':")
        for uid in upstreams:
            print(f"  {uid}")


def cmd_dependents(args: argparse.Namespace) -> None:
    dependents = get_dependents(args.run_id, base_dir=args.base_dir)
    if not dependents:
        print(f"No runs depend on '{args.run_id}'.")
    else:
        print(f"Runs that depend on '{args.run_id}':")
        for rid in dependents:
            print(f"  {rid}")


def build_parser(parent: argparse.ArgumentParser | None = None) -> argparse.ArgumentParser:
    parser = parent or argparse.ArgumentParser(prog="pipewatch deps", description="Manage run dependencies")
    parser.add_argument("--base-dir", default=".pipewatch", help="Storage directory")
    sub = parser.add_subparsers(dest="dep_cmd", required=True)

    p_add = sub.add_parser("add", help="Add a dependency")
    p_add.add_argument("run_id")
    p_add.add_argument("upstream_id")
    p_add.set_defaults(func=cmd_add)

    p_rm = sub.add_parser("remove", help="Remove a dependency")
    p_rm.add_argument("run_id")
    p_rm.add_argument("upstream_id")
    p_rm.set_defaults(func=cmd_remove)

    p_ls = sub.add_parser("list", help="List upstream dependencies of a run")
    p_ls.add_argument("run_id")
    p_ls.set_defaults(func=cmd_list)

    p_dep = sub.add_parser("dependents", help="List runs that depend on a run")
    p_dep.add_argument("run_id")
    p_dep.set_defaults(func=cmd_dependents)

    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
