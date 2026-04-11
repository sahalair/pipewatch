"""CLI entry-point for the run-compare feature."""

from __future__ import annotations

import argparse
import json
import sys

from pipewatch.run_compare import compare_runs, format_run_comparison


def cmd_compare(args: argparse.Namespace) -> None:
    """Compare two runs and print a report."""
    try:
        result = compare_runs(
            args.run_a,
            args.run_b,
            data_dir=getattr(args, "data_dir", "."),
        )
    except FileNotFoundError as exc:
        print(f"Error: {exc}", file=sys.stderr)
        sys.exit(1)

    if getattr(args, "json", False):
        print(json.dumps(result, indent=2))
    else:
        print(format_run_comparison(result))


def build_parser(parent: argparse._SubParsersAction | None = None) -> argparse.ArgumentParser:  # type: ignore[type-arg]
    description = "Compare two pipeline runs side-by-side."
    if parent is not None:
        parser = parent.add_parser("compare", help=description)
    else:
        parser = argparse.ArgumentParser(
            prog="pipewatch-compare",
            description=description,
        )

    parser.add_argument("run_a", help="First run ID")
    parser.add_argument("run_b", help="Second run ID")
    parser.add_argument(
        "--data-dir",
        dest="data_dir",
        default=".",
        help="Root directory for pipewatch data (default: current directory)",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        default=False,
        help="Output comparison as JSON",
    )
    parser.set_defaults(func=cmd_compare)
    return parser


def main() -> None:  # pragma: no cover
    parser = build_parser()
    args = parser.parse_args()
    if hasattr(args, "func"):
        args.func(args)
    else:
        parser.print_help()


if __name__ == "__main__":  # pragma: no cover
    main()
