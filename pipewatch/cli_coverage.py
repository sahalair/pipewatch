"""CLI commands for pipeline run coverage tracking."""

from __future__ import annotations

import argparse
import json
import sys

from pipewatch.run_coverage import (
    compute_coverage,
    format_coverage_report,
    load_coverage,
    remove_expected_pipeline,
    set_expected_pipeline,
)
from pipewatch.run_logger import list_run_records


def cmd_set(args: argparse.Namespace) -> None:
    rule = set_expected_pipeline(args.pipeline, args.min_runs)
    print(f"Coverage rule set: {rule['pipeline']} (min_runs={rule['min_runs']})")


def cmd_remove(args: argparse.Namespace) -> None:
    removed = remove_expected_pipeline(args.pipeline)
    if removed:
        print(f"Removed coverage rule for '{args.pipeline}'.")
    else:
        print(f"No coverage rule found for '{args.pipeline}'.")


def cmd_list(args: argparse.Namespace) -> None:
    data = load_coverage()
    if not data:
        print("No expected pipelines configured.")
        return
    for entry in data.values():
        print(f"  {entry['pipeline']:30s}  min_runs={entry['min_runs']}")


def cmd_report(args: argparse.Namespace) -> None:
    runs = list_run_records()
    report = compute_coverage(runs)
    if getattr(args, "json", False):
        print(json.dumps(report, indent=2))
    else:
        print(format_coverage_report(report))
    if report["missing"] > 0:
        sys.exit(1)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="pipewatch-coverage",
        description="Track pipeline run coverage.",
    )
    sub = parser.add_subparsers(dest="command")

    p_set = sub.add_parser("set", help="Set expected pipeline coverage rule")
    p_set.add_argument("pipeline", help="Pipeline name")
    p_set.add_argument("--min-runs", type=int, default=1, dest="min_runs",
                       help="Minimum runs expected (default: 1)")
    p_set.set_defaults(func=cmd_set)

    p_rm = sub.add_parser("remove", help="Remove a coverage rule")
    p_rm.add_argument("pipeline", help="Pipeline name")
    p_rm.set_defaults(func=cmd_remove)

    p_ls = sub.add_parser("list", help="List expected pipelines")
    p_ls.set_defaults(func=cmd_list)

    p_rep = sub.add_parser("report", help="Show coverage report")
    p_rep.add_argument("--json", action="store_true", help="Output as JSON")
    p_rep.set_defaults(func=cmd_report)

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
