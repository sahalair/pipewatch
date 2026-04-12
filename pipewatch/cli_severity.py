"""CLI commands for run severity classification."""

import argparse
import json
import sys
from pipewatch.run_logger import list_run_records
from pipewatch.run_severity import (
    classify_severity,
    format_severity_badge,
    summarize_severities,
    format_severity_summary,
)


def cmd_show(args: argparse.Namespace) -> None:
    """Show the severity level for a specific run."""
    records = list_run_records()
    match = next((r for r in records if r["run_id"] == args.run_id), None)
    if match is None:
        print(f"Run not found: {args.run_id}", file=sys.stderr)
        sys.exit(1)
    level = classify_severity(match)
    badge = format_severity_badge(level)
    if args.json:
        print(json.dumps({"run_id": args.run_id, "severity": level}))
    else:
        print(f"{badge} {args.run_id}  severity={level}")


def cmd_list(args: argparse.Namespace) -> None:
    """List severity levels for all (or filtered) runs."""
    records = list_run_records()
    if args.pipeline:
        records = [r for r in records if r.get("pipeline") == args.pipeline]
    if args.level:
        records = [r for r in records if classify_severity(r) == args.level]
    if args.json:
        output = [{"run_id": r["run_id"], "severity": classify_severity(r)} for r in records]
        print(json.dumps(output, indent=2))
        return
    if not records:
        print("No runs found.")
        return
    for r in records:
        level = classify_severity(r)
        badge = format_severity_badge(level)
        print(f"{badge} {r['run_id']}  pipeline={r.get('pipeline', '-')}  status={r.get('status', '-')}")


def cmd_summary(args: argparse.Namespace) -> None:
    """Print a severity count summary across all runs."""
    records = list_run_records()
    if args.pipeline:
        records = [r for r in records if r.get("pipeline") == args.pipeline]
    counts = summarize_severities(records)
    if args.json:
        print(json.dumps(counts))
    else:
        print(format_severity_summary(counts))


def build_parser(parent: argparse.ArgumentParser = None) -> argparse.ArgumentParser:
    parser = parent or argparse.ArgumentParser(prog="pipewatch severity")
    sub = parser.add_subparsers(dest="severity_cmd")

    p_show = sub.add_parser("show", help="Show severity for a run")
    p_show.add_argument("run_id")
    p_show.add_argument("--json", action="store_true")
    p_show.set_defaults(func=cmd_show)

    p_list = sub.add_parser("list", help="List severity for all runs")
    p_list.add_argument("--pipeline")
    p_list.add_argument("--level", choices=["ok", "low", "medium", "high", "critical"])
    p_list.add_argument("--json", action="store_true")
    p_list.set_defaults(func=cmd_list)

    p_sum = sub.add_parser("summary", help="Severity count summary")
    p_sum.add_argument("--pipeline")
    p_sum.add_argument("--json", action="store_true")
    p_sum.set_defaults(func=cmd_summary)

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
