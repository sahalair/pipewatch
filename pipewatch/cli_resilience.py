"""CLI for run resilience reports."""

import argparse
import json
import sys

from pipewatch.run_resilience import compute_resilience, format_resilience_report
from pipewatch.run_logger import list_run_records


def cmd_show(args) -> None:
    try:
        report = compute_resilience(args.pipeline, base_dir=args.base_dir)
    except Exception as exc:
        print(f"Error: {exc}", file=sys.stderr)
        sys.exit(1)

    if args.json:
        print(json.dumps(report, indent=2))
    else:
        print(format_resilience_report(report))


def cmd_list(args) -> None:
    records = list_run_records(base_dir=args.base_dir)
    pipelines = sorted({r["pipeline"] for r in records if "pipeline" in r})

    if not pipelines:
        print("No pipelines found.")
        return

    reports = [compute_resilience(p, base_dir=args.base_dir) for p in pipelines]
    reports.sort(key=lambda r: r["score"], reverse=True)

    if args.json:
        print(json.dumps(reports, indent=2))
        return

    header = f"{'Pipeline':<30} {'Score':>6}  {'Grade':>5}  {'Uptime':>8}  {'Recovery(s)':>12}"
    print(header)
    print("-" * len(header))
    for r in reports:
        rec = r["mean_recovery_seconds"]
        rec_str = str(rec) if rec is not None else "N/A"
        print(f"{r['pipeline']:<30} {r['score']:>6}  {r['grade']:>5}  {r['uptime_ratio']:>8.4f}  {rec_str:>12}")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="pipewatch-resilience", description="Pipeline resilience reports")
    parser.add_argument("--base-dir", default=".", help="Base directory for data files")
    sub = parser.add_subparsers(dest="command")

    p_show = sub.add_parser("show", help="Show resilience for a pipeline")
    p_show.add_argument("pipeline", help="Pipeline name")
    p_show.add_argument("--json", action="store_true", help="Output as JSON")
    p_show.set_defaults(func=cmd_show)

    p_list = sub.add_parser("list", help="List resilience for all pipelines")
    p_list.add_argument("--json", action="store_true", help="Output as JSON")
    p_list.set_defaults(func=cmd_list)

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
