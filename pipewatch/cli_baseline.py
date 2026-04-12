"""CLI commands for managing run baselines."""

from __future__ import annotations

import argparse
import sys

from pipewatch.run_baseline import (
    set_baseline,
    get_baseline,
    remove_baseline,
    list_baselines,
)


def cmd_set(args: argparse.Namespace) -> None:
    set_baseline(args.pipeline, args.run_id)
    print(f"Baseline for '{args.pipeline}' set to run '{args.run_id}'.")


def cmd_get(args: argparse.Namespace) -> None:
    run_id = get_baseline(args.pipeline)
    if run_id is None:
        print(f"No baseline set for pipeline '{args.pipeline}'.")
    else:
        print(f"{args.pipeline}: {run_id}")


def cmd_remove(args: argparse.Namespace) -> None:
    removed = remove_baseline(args.pipeline)
    if removed:
        print(f"Baseline for '{args.pipeline}' removed.")
    else:
        print(f"No baseline found for pipeline '{args.pipeline}'.")
        sys.exit(1)


def cmd_list(args: argparse.Namespace) -> None:  # noqa: ARG001
    entries = list_baselines()
    if not entries:
        print("No baselines configured.")
        return
    for entry in entries:
        print(f"{entry['pipeline']}: {entry['run_id']}")


def build_parser(parent: argparse.ArgumentParser | None = None) -> argparse.ArgumentParser:
    parser = parent or argparse.ArgumentParser(
        prog="pipewatch baseline",
        description="Manage run baselines per pipeline.",
    )
    sub = parser.add_subparsers(dest="baseline_cmd")

    p_set = sub.add_parser("set", help="Set a baseline run for a pipeline.")
    p_set.add_argument("pipeline", help="Pipeline name.")
    p_set.add_argument("run_id", help="Run ID to use as baseline.")
    p_set.set_defaults(func=cmd_set)

    p_get = sub.add_parser("get", help="Get the baseline run for a pipeline.")
    p_get.add_argument("pipeline", help="Pipeline name.")
    p_get.set_defaults(func=cmd_get)

    p_rm = sub.add_parser("remove", help="Remove a baseline for a pipeline.")
    p_rm.add_argument("pipeline", help="Pipeline name.")
    p_rm.set_defaults(func=cmd_remove)

    p_ls = sub.add_parser("list", help="List all baselines.")
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
