"""CLI commands for metric correlation analysis."""

import argparse
import json
import sys

from pipewatch.run_correlation import correlate_metrics, format_correlation_report


def cmd_correlate(args) -> None:
    result = correlate_metrics(
        pipeline=args.pipeline,
        metric_a=args.metric_a,
        metric_b=args.metric_b,
        base_dir=getattr(args, "base_dir", "."),
    )
    if args.json:
        print(json.dumps(result, indent=2))
    else:
        print(format_correlation_report(result))


def build_parser(parser: argparse.ArgumentParser = None) -> argparse.ArgumentParser:
    if parser is None:
        parser = argparse.ArgumentParser(
            prog="pipewatch correlation",
            description="Correlate two metrics across pipeline runs.",
        )
    sub = parser.add_subparsers(dest="subcmd")

    p_corr = sub.add_parser("run", help="Compute Pearson correlation between two metrics")
    p_corr.add_argument("pipeline", help="Pipeline name")
    p_corr.add_argument("metric_a", help="First metric name")
    p_corr.add_argument("metric_b", help="Second metric name")
    p_corr.add_argument("--json", action="store_true", help="Output as JSON")
    p_corr.add_argument("--base-dir", dest="base_dir", default=".", help="Data directory")
    p_corr.set_defaults(func=cmd_correlate)

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
