"""cli_pulse.py — CLI commands for pipeline pulse reporting."""
from __future__ import annotations

import argparse
import json
import sys

from pipewatch.run_logger import list_run_records
from pipewatch.run_pulse import compute_pulse, format_pulse_report


def cmd_show(args) -> None:
    try:
        result = compute_pulse(args.pipeline, base_dir=args.base_dir)
    except Exception as exc:
        print(f"Error: {exc}", file=sys.stderr)
        sys.exit(1)
    if args.json:
        print(json.dumps(result, indent=2))
    else:
        print(format_pulse_report(result))


def cmd_list(args) -> None:
    records = list_run_records(base_dir=args.base_dir)
    pipelines = sorted({r["pipeline"] for r in records if r.get("pipeline")})
    if not pipelines:
        print("No pipelines found.")
        return
    results = [compute_pulse(p, base_dir=args.base_dir) for p in pipelines]
    results.sort(key=lambda r: r["score"], reverse=True)
    if args.json:
        print(json.dumps(results, indent=2))
    else:
        header = f"{'Pipeline':<30} {'Score':>7} {'Grade':>5}"
        print(header)
        print("-" * len(header))
        for r in results:
            print(f"{r['pipeline']:<30} {r['score']:>7.4f} {r['grade']:>5}")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Pipeline pulse reporting")
    parser.add_argument("--base-dir", default=".", help="Base data directory")
    sub = parser.add_subparsers(dest="command")

    p_show = sub.add_parser("show", help="Show pulse for a pipeline")
    p_show.add_argument("pipeline")
    p_show.add_argument("--json", action="store_true")
    p_show.set_defaults(func=cmd_show)

    p_list = sub.add_parser("list", help="List pulse for all pipelines")
    p_list.add_argument("--json", action="store_true")
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
