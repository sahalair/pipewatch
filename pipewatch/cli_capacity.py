"""CLI commands for managing pipeline capacity limits."""

import argparse
import sys

from pipewatch.run_capacity import (
    check_capacity,
    list_capacity_rules,
    remove_capacity,
    set_capacity,
)

DEFAULT_DIR = ".pipewatch/capacity"


def cmd_set(args) -> None:
    rule = set_capacity(args.base_dir, args.pipeline, args.max_concurrent)
    print(f"Capacity set: {rule['pipeline']} → max {rule['max_concurrent']} concurrent run(s)")


def cmd_remove(args) -> None:
    removed = remove_capacity(args.base_dir, args.pipeline)
    if removed:
        print(f"Capacity rule removed for: {args.pipeline}")
    else:
        print(f"No capacity rule found for: {args.pipeline}", file=sys.stderr)


def cmd_list(args) -> None:
    rules = list_capacity_rules(args.base_dir)
    if not rules:
        print("No capacity rules defined.")
        return
    for rule in rules:
        print(f"{rule['pipeline']}: max {rule['max_concurrent']} concurrent")


def cmd_check(args) -> None:
    status = check_capacity(args.base_dir, args.pipeline, args.active)
    if not status["limited"]:
        print(f"{args.pipeline}: no capacity limit configured")
        return
    label = "AT CAPACITY" if status["at_capacity"] else "OK"
    print(
        f"{args.pipeline}: {status['active']}/{status['max_concurrent']} active — {label}"
    )
    if status["at_capacity"]:
        sys.exit(1)


def build_parser(parent=None) -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="pipewatch capacity",
        description="Manage pipeline concurrency capacity limits.",
    ) if parent is None else parent
    parser.add_argument("--base-dir", default=DEFAULT_DIR)
    sub = parser.add_subparsers(dest="cmd")

    p_set = sub.add_parser("set", help="Set a capacity limit for a pipeline")
    p_set.add_argument("pipeline")
    p_set.add_argument("max_concurrent", type=int)
    p_set.set_defaults(func=cmd_set)

    p_rm = sub.add_parser("remove", help="Remove a capacity limit")
    p_rm.add_argument("pipeline")
    p_rm.set_defaults(func=cmd_remove)

    p_ls = sub.add_parser("list", help="List all capacity rules")
    p_ls.set_defaults(func=cmd_list)

    p_chk = sub.add_parser("check", help="Check if a pipeline is at capacity")
    p_chk.add_argument("pipeline")
    p_chk.add_argument("active", type=int, help="Number of currently active runs")
    p_chk.set_defaults(func=cmd_check)

    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()
    if not hasattr(args, "func"):
        parser.print_help()
        sys.exit(0)
    args.func(args)


if __name__ == "__main__":
    main()
