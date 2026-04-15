"""CLI commands for the run efficiency feature."""

from __future__ import annotations

import argparse
import json
import sys

from pipewatch.run_logger import load_run_record
from pipewatch.run_efficiency import compute_efficiency, format_efficiency_report


def cmd_show(args: argparse.Namespace) -> None:
    try:
        record = load_run_record(args.run_id, base_dir=args.base_dir)
    except FileNotFoundError:
        print(f"Run not found: {args.run_id}", file=sys.stderr)
        sys.exit(1)

    pipeline = record.get("pipeline", "")
    result = compute_efficiency(pipeline, args.run_id, base_dir=args.base_dir)

    if args.json:
        print(json.dumps(result, indent=2))
    else:
        print(format_efficiency_report(result))


def cmd_list(args: argparse.Namespace) -> None:
    from pipewatch.run_logger import list_run_records

    records = list_run_records(base_dir=args.base_dir)
    if args.pipeline:
        records = [r for r in records if r.get("pipeline") == args.pipeline]

    if not records:
        print("No runs found.")
        return

    results = [
        compute_efficiency(r.get("pipeline", ""), r["run_id"], base_dir=args.base_dir)
        for r in records
    ]
    results.sort(key=lambda x: x["score"], reverse=True)

    if args.json:
        print(json.dumps(results, indent=2))
    else:
        header = f"{'Run ID':<36}  {'Pipeline':<20}  {'Score':>6}  Grade"
        print(header)
        print("-" * len(header))
        for r in results:
            print(f"{r['run_id']:<36}  {r['pipeline']:<20}  {r['score']:>6}  {r['grade']}")


def build_parser(parent: argparse.ArgumentParser | None = None) -> argparse.ArgumentParser:
    parser = parent or argparse.ArgumentParser(
        prog="pipewatch efficiency",
        description="Compute and display run efficiency scores.",
    )
    parser.add_argument("--base-dir", default=".", help="Base directory for data files.")
    sub = parser.add_subparsers(dest="subcommand")

    p_show = sub.add_parser("show", help="Show efficiency for a single run.")
    p_show.add_argument("run_id", help="Run ID to evaluate.")
    p_show.add_argument("--json", action="store_true", help="Output as JSON.")
    p_show.set_defaults(func=cmd_show)

    p_list = sub.add_parser("list", help="List efficiency scores for all runs.")
    p_list.add_argument("--pipeline", default=None, help="Filter by pipeline name.")
    p_list.add_argument("--json", action="store_true", help="Output as JSON.")
    p_list.set_defaults(func=cmd_list)

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
