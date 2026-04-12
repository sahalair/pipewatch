"""CLI commands for run concurrency analysis."""

from __future__ import annotations

import argparse
import json
import sys

from pipewatch.run_concurrency import (
    compute_peak_concurrency,
    concurrency_timeline,
    format_concurrency_report,
)
from pipewatch.run_logger import list_run_records


def cmd_peak(args: argparse.Namespace) -> None:
    runs = list_run_records()
    peak = compute_peak_concurrency(pipeline=args.pipeline or None, runs=runs)
    if args.json:
        print(json.dumps({"pipeline": args.pipeline, "peak_concurrency": peak}))
    else:
        label = args.pipeline or "(all pipelines)"
        print(f"Peak concurrency for {label}: {peak}")


def cmd_timeline(args: argparse.Namespace) -> None:
    runs = list_run_records()
    timeline = concurrency_timeline(pipeline=args.pipeline or None, runs=runs)
    if args.json:
        print(json.dumps(timeline, indent=2))
        return
    if not timeline:
        print("No concurrency events found.")
        return
    limit = getattr(args, "limit", 20)
    for entry in timeline[-limit:]:
        sign = "+" if entry["delta"] > 0 else "-"
        print(f"{entry['timestamp']}  {sign}1  => {entry['concurrency']} active  ({entry['run_id']})")


def cmd_report(args: argparse.Namespace) -> None:
    runs = list_run_records()
    report = format_concurrency_report(pipeline=args.pipeline or None, runs=runs)
    print(report)


def build_parser(parent: argparse.ArgumentParser | None = None) -> argparse.ArgumentParser:
    parser = parent or argparse.ArgumentParser(
        prog="pipewatch concurrency",
        description="Analyze concurrent pipeline run overlap.",
    )
    sub = parser.add_subparsers(dest="concurrency_cmd")

    p_peak = sub.add_parser("peak", help="Show peak concurrent runs")
    p_peak.add_argument("--pipeline", default="", help="Filter by pipeline name")
    p_peak.add_argument("--json", action="store_true", help="Output as JSON")
    p_peak.set_defaults(func=cmd_peak)

    p_tl = sub.add_parser("timeline", help="Show concurrency change timeline")
    p_tl.add_argument("--pipeline", default="", help="Filter by pipeline name")
    p_tl.add_argument("--limit", type=int, default=20, help="Max events to show")
    p_tl.add_argument("--json", action="store_true", help="Output as JSON")
    p_tl.set_defaults(func=cmd_timeline)

    p_rep = sub.add_parser("report", help="Print full concurrency report")
    p_rep.add_argument("--pipeline", default="", help="Filter by pipeline name")
    p_rep.set_defaults(func=cmd_report)

    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()
    if not getattr(args, "func", None):
        parser.print_help()
        sys.exit(0)
    args.func(args)


if __name__ == "__main__":
    main()
