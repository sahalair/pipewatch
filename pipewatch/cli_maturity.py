"""CLI commands for pipeline maturity scoring."""

import argparse
import json
import sys

from pipewatch.run_maturity import compute_maturity, format_maturity_report
from pipewatch.run_logger import list_run_records


def cmd_show(args) -> None:
    try:
        report = compute_maturity(args.pipeline, base_dir=args.base_dir)
    except Exception as exc:
        print(f"Error: {exc}", file=sys.stderr)
        sys.exit(1)

    if args.json:
        print(json.dumps(report, indent=2))
    else:
        print(format_maturity_report(report))


def cmd_list(args) -> None:
    records = list_run_records(base_dir=args.base_dir)
    pipelines = sorted({r.get("pipeline", "") for r in records if r.get("pipeline")})

    if not pipelines:
        print("No pipelines found.")
        return

    reports = []
    for p in pipelines:
        try:
            reports.append(compute_maturity(p, base_dir=args.base_dir))
        except Exception:
            pass

    reports.sort(key=lambda r: r["score"], reverse=True)

    if args.json:
        print(json.dumps(reports, indent=2))
        return

    header = f"{'Pipeline':<30} {'Score':>6}  {'Grade':<10}  {'Runs':>5}"
    print(header)
    print("-" * len(header))
    for r in reports:
        print(f"{r['pipeline']:<30} {r['score']:>6.1f}  {r['grade']:<10}  {r['run_count']:>5}")


def build_parser(parent: argparse.ArgumentParser = None) -> argparse.ArgumentParser:
    parser = parent or argparse.ArgumentParser(
        prog="pipewatch maturity",
        description="Pipeline maturity scoring.",
    )
    parser.add_argument("--base-dir", default=".pipewatch", help="Data directory.")
    sub = parser.add_subparsers(dest="subcmd")

    p_show = sub.add_parser("show", help="Show maturity for a pipeline.")
    p_show.add_argument("pipeline", help="Pipeline name.")
    p_show.add_argument("--json", action="store_true", help="Output JSON.")
    p_show.set_defaults(func=cmd_show)

    p_list = sub.add_parser("list", help="List maturity for all pipelines.")
    p_list.add_argument("--json", action="store_true", help="Output JSON.")
    p_list.set_defaults(func=cmd_list)

    return parser


def main(argv=None) -> None:
    parser = build_parser()
    args = parser.parse_args(argv)
    if not hasattr(args, "func"):
        parser.print_help()
        sys.exit(0)
    args.func(args)


if __name__ == "__main__":
    main()
