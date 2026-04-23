"""CLI entry-point for the run-reliability feature."""

import argparse
import json
import sys

from pipewatch.run_logger import list_run_records
from pipewatch.run_reliability import compute_reliability, format_reliability_report


def cmd_show(args) -> None:
    try:
        report = compute_reliability(
            args.pipeline,
            base_dir=args.base_dir,
            limit=args.limit,
        )
    except Exception as exc:  # noqa: BLE001
        print(f"Error: {exc}", file=sys.stderr)
        sys.exit(1)

    if args.json:
        print(json.dumps(report, indent=2))
    else:
        print(format_reliability_report(report))


def cmd_list(args) -> None:
    records = list_run_records(base_dir=args.base_dir)
    pipelines = sorted({r["pipeline"] for r in records if "pipeline" in r})
    if not pipelines:
        print("No pipelines found.")
        return

    reports = [
        compute_reliability(p, base_dir=args.base_dir, limit=args.limit)
        for p in pipelines
    ]
    reports.sort(key=lambda r: r["score"], reverse=True)

    if args.json:
        print(json.dumps(reports, indent=2))
    else:
        header = f"{'Pipeline':<30} {'Score':>7}  Grade"
        print(header)
        print("-" * len(header))
        for r in reports:
            print(f"{r['pipeline']:<30} {r['score']:>7.2%}  {r['grade']}")


def build_parser(parent=None):
    if parent is None:
        parser = argparse.ArgumentParser(prog="pipewatch reliability")
    else:
        parser = parent

    parser.add_argument("--base-dir", default=".pipewatch")
    parser.add_argument("--limit", type=int, default=50)
    parser.add_argument("--json", action="store_true")

    sub = parser.add_subparsers(dest="subcmd")

    p_show = sub.add_parser("show", help="Show reliability for a pipeline")
    p_show.add_argument("pipeline")
    p_show.set_defaults(func=cmd_show)

    p_list = sub.add_parser("list", help="List reliability for all pipelines")
    p_list.set_defaults(func=cmd_list)

    return parser


def main():
    parser = build_parser()
    args = parser.parse_args()
    if not hasattr(args, "func"):
        parser.print_help()
        sys.exit(1)
    args.func(args)


if __name__ == "__main__":
    main()
