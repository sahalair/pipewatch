"""cli_spotlight.py — CLI entry point for the run spotlight feature."""

import argparse
import json
import sys

from pipewatch.run_spotlight import spotlight_runs, format_spotlight_report


def cmd_show(args) -> None:
    entries = spotlight_runs(
        pipeline=args.pipeline,
        limit=args.limit,
        base_dir=args.base_dir,
    )
    if args.json:
        print(json.dumps(entries, indent=2))
    else:
        print(format_spotlight_report(entries))


def build_parser(parent=None) -> argparse.ArgumentParser:
    if parent is None:
        parser = argparse.ArgumentParser(
            prog="pipewatch-spotlight",
            description="Highlight notable pipeline runs.",
        )
    else:
        parser = parent

    parser.add_argument("--pipeline", default=None, help="Filter by pipeline name")
    parser.add_argument(
        "--limit", type=int, default=5, help="Number of runs to show (default: 5)"
    )
    parser.add_argument(
        "--json", action="store_true", help="Output as JSON"
    )
    parser.add_argument(
        "--base-dir", default=".pipewatch", dest="base_dir", help="Data directory"
    )
    parser.set_defaults(func=cmd_show)
    return parser


def main(argv=None) -> None:
    parser = build_parser()
    args = parser.parse_args(argv)
    if not hasattr(args, "func"):
        parser.print_help()
        sys.exit(1)
    args.func(args)


if __name__ == "__main__":
    main()
