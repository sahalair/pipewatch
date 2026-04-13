"""CLI for run sensitivity analysis."""

import argparse
import json
import sys

from pipewatch.run_sensitivity import analyze_sensitivity, format_sensitivity_report


def cmd_show(args) -> None:
    metrics = [m.strip() for m in args.metrics.split(",") if m.strip()]
    if not metrics:
        print("Error: --metrics must be a non-empty comma-separated list.", file=sys.stderr)
        sys.exit(1)

    results = analyze_sensitivity(
        pipeline=args.pipeline,
        metrics=metrics,
        base_dir=args.base_dir,
        min_samples=args.min_samples,
    )

    if args.json:
        print(json.dumps({"pipeline": args.pipeline, "sensitivity": results}, indent=2))
        return

    print(format_sensitivity_report(args.pipeline, results))


def build_parser(subparsers=None):
    desc = "Analyze metric sensitivity for a pipeline."
    if subparsers is not None:
        p = subparsers.add_parser("sensitivity", help=desc)
    else:
        p = argparse.ArgumentParser(prog="pipewatch-sensitivity", description=desc)

    p.add_argument("pipeline", help="Pipeline name to analyze.")
    p.add_argument(
        "--metrics",
        required=True,
        help="Comma-separated list of metric names to evaluate.",
    )
    p.add_argument(
        "--min-samples",
        type=int,
        default=5,
        dest="min_samples",
        help="Minimum number of data points required (default: 5).",
    )
    p.add_argument(
        "--base-dir",
        default=".pipewatch",
        dest="base_dir",
        help="Base directory for pipewatch data.",
    )
    p.add_argument("--json", action="store_true", help="Output as JSON.")
    p.set_defaults(func=cmd_show)
    return p


def main():
    parser = build_parser()
    args = parser.parse_args()
    if hasattr(args, "func"):
        args.func(args)
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
