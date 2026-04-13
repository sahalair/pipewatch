"""CLI commands for managing run cost records."""

import argparse
import json
import sys

from pipewatch.run_cost import record_cost, get_cost, list_costs_for_pipeline, summarize_costs


def cmd_record(args) -> None:
    entry = record_cost(
        run_id=args.run_id,
        pipeline=args.pipeline,
        amount=args.amount,
        currency=args.currency,
        unit=getattr(args, "unit", None),
        notes=getattr(args, "notes", None),
    )
    print(f"Recorded cost {entry['amount']} {entry['currency']} for run {args.run_id}.")


def cmd_get(args) -> None:
    entry = get_cost(args.run_id)
    if entry is None:
        print(f"No cost record found for run {args.run_id}.", file=sys.stderr)
        sys.exit(1)
    if getattr(args, "json", False):
        print(json.dumps(entry, indent=2))
    else:
        print(f"Run:      {entry['run_id']}")
        print(f"Pipeline: {entry['pipeline']}")
        print(f"Amount:   {entry['amount']} {entry['currency']}")
        if entry.get("unit"):
            print(f"Unit:     {entry['unit']}")
        if entry.get("notes"):
            print(f"Notes:    {entry['notes']}")


def cmd_list(args) -> None:
    entries = list_costs_for_pipeline(args.pipeline)
    if not entries:
        print(f"No cost records for pipeline '{args.pipeline}'.")
        return
    for e in entries:
        unit_str = f" [{e['unit']}]" if e.get("unit") else ""
        print(f"{e['run_id']}: {e['amount']} {e['currency']}{unit_str}")


def cmd_summary(args) -> None:
    summary = summarize_costs(args.pipeline)
    if summary["count"] == 0:
        print(f"No cost records for pipeline '{args.pipeline}'.")
        return
    print(f"Pipeline : {summary['pipeline']}")
    print(f"Runs     : {summary['count']}")
    print(f"Total    : {summary['total']} {summary['currency']}")
    print(f"Average  : {summary['average']} {summary['currency']}")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="pipewatch-cost", description="Manage run cost records.")
    sub = parser.add_subparsers(dest="command")

    p_rec = sub.add_parser("record", help="Record a cost for a run.")
    p_rec.add_argument("run_id")
    p_rec.add_argument("pipeline")
    p_rec.add_argument("amount", type=float)
    p_rec.add_argument("--currency", default="USD")
    p_rec.add_argument("--unit")
    p_rec.add_argument("--notes")
    p_rec.set_defaults(func=cmd_record)

    p_get = sub.add_parser("get", help="Get cost for a specific run.")
    p_get.add_argument("run_id")
    p_get.add_argument("--json", action="store_true")
    p_get.set_defaults(func=cmd_get)

    p_list = sub.add_parser("list", help="List costs for a pipeline.")
    p_list.add_argument("pipeline")
    p_list.set_defaults(func=cmd_list)

    p_sum = sub.add_parser("summary", help="Summarize costs for a pipeline.")
    p_sum.add_argument("pipeline")
    p_sum.set_defaults(func=cmd_summary)

    return parser


def main():
    parser = build_parser()
    args = parser.parse_args()
    if not hasattr(args, "func"):
        parser.print_help()
        sys.exit(1)
    args.func(args)
