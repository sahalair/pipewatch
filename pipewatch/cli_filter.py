"""CLI sub-command for filtering and listing pipeline runs."""

import argparse
import sys
from pipewatch.run_filter import filter_runs, format_run_list
from pipewatch.tag_manager import load_tags


def cmd_filter(args: argparse.Namespace) -> None:
    """Handle the 'filter' sub-command."""
    tags = args.tags.split(",") if args.tags else None
    runs = filter_runs(
        base_dir=args.base_dir,
        tags=tags,
        status=args.status,
        pipeline=args.pipeline,
        since=args.since,
        until=args.until,
        limit=args.limit,
    )

    if not runs:
        print("No runs matched the given filters.", file=sys.stderr)
        return

    all_tags = load_tags(base_dir=args.base_dir) if args.show_tags else None
    print(format_run_list(runs, all_tags=all_tags))


def build_parser(parent: argparse.ArgumentParser = None) -> argparse.ArgumentParser:
    """Build (or attach to a parent) the argument parser for run filtering."""
    if parent is None:
        parser = argparse.ArgumentParser(
            prog="pipewatch-filter",
            description="Filter and list pipeline runs.",
        )
    else:
        parser = parent

    parser.add_argument(
        "--base-dir", default=".", help="Root directory for pipewatch data."
    )
    parser.add_argument(
        "--tags", default=None,
        help="Comma-separated list of tags; only runs with ALL tags are shown.",
    )
    parser.add_argument(
        "--status", default=None,
        help="Filter by run status or exit code (e.g. 'ok', '0', 'failed').",
    )
    parser.add_argument(
        "--pipeline", default=None, help="Filter by pipeline name."
    )
    parser.add_argument(
        "--since", default=None,
        help="Show runs started on or after this ISO-8601 date (e.g. 2024-01-01).",
    )
    parser.add_argument(
        "--until", default=None,
        help="Show runs started on or before this ISO-8601 date.",
    )
    parser.add_argument(
        "--limit", type=int, default=None, help="Maximum number of runs to show."
    )
    parser.add_argument(
        "--show-tags", action="store_true", help="Display tags alongside each run."
    )
    parser.set_defaults(func=cmd_filter)
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
