"""CLI interface for pipeline health checks."""

from __future__ import annotations

import argparse
import json
import sys

from pipewatch.run_healthcheck import evaluate_health, format_health_report


def cmd_check(args: argparse.Namespace) -> None:
    result = evaluate_health(args.pipeline, base_dir=args.base_dir)
    if args.json:
        print(json.dumps(result, indent=2))
    else:
        print(format_health_report(result))
    if result["status"] == "failing" and not args.no_exit:
        sys.exit(1)


def cmd_list(args: argparse.Namespace) -> None:
    from pipewatch.run_logger import list_run_records

    runs = list_run_records(base_dir=args.base_dir)
    pipelines = sorted({r["pipeline"] for r in runs if "pipeline" in r})
    if not pipelines:
        print("No pipelines found.")
        return
    for pipeline in pipelines:
        result = evaluate_health(pipeline, base_dir=args.base_dir)
        status = result["status"].upper()
        reason = result["reason"]
        print(f"{pipeline:<30} {status:<10} {reason}")


def build_parser(parent: argparse._SubParsersAction | None = None) -> argparse.ArgumentParser:  # type: ignore[name-defined]
    if parent is not None:
        parser = parent.add_parser("healthcheck", help="Pipeline health checks")
    else:
        parser = argparse.ArgumentParser(prog="pipewatch-healthcheck")

    parser.add_argument("--base-dir", default=".", help="Base data directory")
    sub = parser.add_subparsers(dest="subcommand")

    p_check = sub.add_parser("check", help="Check health of a pipeline")
    p_check.add_argument("pipeline", help="Pipeline name")
    p_check.add_argument("--json", action="store_true", help="Output as JSON")
    p_check.add_argument(
        "--no-exit", action="store_true", help="Do not exit with code 1 on failure"
    )
    p_check.set_defaults(func=cmd_check)

    p_list = sub.add_parser("list", help="List health status for all pipelines")
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
