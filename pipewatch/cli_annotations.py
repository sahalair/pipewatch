"""CLI commands for managing run annotations."""

from __future__ import annotations

import argparse
import json
import sys

from pipewatch.run_annotations import (
    delete_annotation,
    filter_runs_by_annotation,
    get_annotations,
    set_annotation,
)

_DEFAULT_DIR = ".pipewatch"


def cmd_set(args: argparse.Namespace) -> None:
    set_annotation(args.base_dir, args.run_id, args.key, args.value)
    print(f"Annotation '{args.key}' set on run {args.run_id}.")


def cmd_get(args: argparse.Namespace) -> None:
    annotations = get_annotations(args.base_dir, args.run_id)
    if not annotations:
        print(f"No annotations for run {args.run_id}.")
        return
    for k, v in annotations.items():
        print(f"  {k}: {v}")


def cmd_delete(args: argparse.Namespace) -> None:
    removed = delete_annotation(args.base_dir, args.run_id, args.key)
    if removed:
        print(f"Annotation '{args.key}' removed from run {args.run_id}.")
    else:
        print(f"Annotation '{args.key}' not found on run {args.run_id}.")
        sys.exit(1)


def cmd_filter(args: argparse.Namespace) -> None:
    run_ids = filter_runs_by_annotation(args.base_dir, args.key, args.value)
    if not run_ids:
        print("No runs matched.")
        return
    for run_id in run_ids:
        print(run_id)


def build_parser(parent: argparse.ArgumentParser | None = None) -> argparse.ArgumentParser:
    parser = parent or argparse.ArgumentParser(prog="pipewatch annotations")
    parser.add_argument("--base-dir", default=_DEFAULT_DIR)
    sub = parser.add_subparsers(dest="subcmd", required=True)

    p_set = sub.add_parser("set", help="Set an annotation on a run")
    p_set.add_argument("run_id")
    p_set.add_argument("key")
    p_set.add_argument("value")
    p_set.set_defaults(func=cmd_set)

    p_get = sub.add_parser("get", help="Get all annotations for a run")
    p_get.add_argument("run_id")
    p_get.set_defaults(func=cmd_get)

    p_del = sub.add_parser("delete", help="Delete an annotation from a run")
    p_del.add_argument("run_id")
    p_del.add_argument("key")
    p_del.set_defaults(func=cmd_delete)

    p_filter = sub.add_parser("filter", help="Find runs by annotation key=value")
    p_filter.add_argument("key")
    p_filter.add_argument("value")
    p_filter.set_defaults(func=cmd_filter)

    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
