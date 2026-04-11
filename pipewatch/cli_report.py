"""CLI commands for generating and displaying pipeline run reports."""

from __future__ import annotations

import argparse
import sys

from pipewatch.run_logger import list_run_records
from pipewatch.run_report import build_run_report, format_run_report


def cmd_show(args: argparse.Namespace) -> None:
    """Show report for a specific run or the latest run."""
    storage_dir = getattr(args, "storage_dir", ".pipewatch")
    run_id = getattr(args, "run_id", None)

    if not run_id:
        runs = list_run_records(storage_dir=storage_dir)
        if not runs:
            print("No runs found.", file=sys.stderr)
            sys.exit(1)
        run_id = runs[-1]["run_id"]
        print(f"(Showing latest run: {run_id})")

    try:
        report = build_run_report(run_id, storage_dir=storage_dir)
    except FileNotFoundError as exc:
        print(str(exc), file=sys.stderr)
        sys.exit(1)

    print(format_run_report(report))


def cmd_list(args: argparse.Namespace) -> None:
    """List recent runs with brief status."""
    storage_dir = getattr(args, "storage_dir", ".pipewatch")
    limit = getattr(args, "limit", 10)
    runs = list_run_records(storage_dir=storage_dir)

    if not runs:
        print("No runs recorded.")
        return

    recent = runs[-limit:]
    print(f"{'Run ID':<38} {'Pipeline':<20} {'Exit Code':<10} {'Started':<25}")
    print("-" * 95)
    for r in reversed(recent):
        pipeline = r.get("pipeline", "unknown")
        exit_code = r.get("exit_code", "?")
        started = r.get("started_at", "")[:19]
        print(f"{r['run_id']:<38} {pipeline:<20} {str(exit_code):<10} {started:<25}")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="pipewatch-report",
        description="Generate pipeline run reports",
    )
    parser.add_argument("--storage-dir", default=".pipewatch", dest="storage_dir")
    sub = parser.add_subparsers(dest="command")

    show_p = sub.add_parser("show", help="Show report for a run")
    show_p.add_argument("run_id", nargs="?", default=None, help="Run ID (default: latest)")
    show_p.set_defaults(func=cmd_show)

    list_p = sub.add_parser("list", help="List recent runs")
    list_p.add_argument("--limit", type=int, default=10, help="Number of runs to show")
    list_p.set_defaults(func=cmd_list)

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
