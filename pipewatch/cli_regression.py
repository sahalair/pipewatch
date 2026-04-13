"""CLI commands for metric regression detection."""

import argparse
import json
import sys

from pipewatch.run_regression import analyze_regression, format_regression_report


def cmd_check(args) -> None:
    """Check for regression in a pipeline metric."""
    result = analyze_regression(
        pipeline=args.pipeline,
        metric_name=args.metric,
        threshold_pct=args.threshold,
        window=args.window,
        limit=args.limit,
    )
    if getattr(args, "json", False):
        print(json.dumps(result, indent=2))
    else:
        print(format_regression_report(result))

    if result.get("regression"):
        sys.exit(2)


def build_parser(parser: argparse.ArgumentParser = None) -> argparse.ArgumentParser:
    if parser is None:
        parser = argparse.ArgumentParser(
            prog="pipewatch-regression",
            description="Detect metric regressions in pipeline runs.",
        )
    sub = parser.add_subparsers(dest="command")

    p_check = sub.add_parser("check", help="Check for regression in a metric")
    p_check.add_argument("pipeline", help="Pipeline name")
    p_check.add_argument("metric", help="Metric name")
    p_check.add_argument(
        "--threshold",
        type=float,
        default=10.0,
        help="Percent change threshold (default: 10.0)",
    )
    p_check.add_argument(
        "--window",
        type=int,
        default=3,
        help="Rolling window size for baseline (default: 3)",
    )
    p_check.add_argument(
        "--limit",
        type=int,
        default=20,
        help="Max historical data points to consider (default: 20)",
    )
    p_check.add_argument(
        "--json",
        action="store_true",
        default=False,
        help="Output as JSON",
    )
    p_check.set_defaults(func=cmd_check)

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
