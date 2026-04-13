"""CLI commands for pipeline confidence scoring."""

import argparse
import json
import sys

from pipewatch.run_confidence import compute_confidence, format_confidence_report
from pipewatch.run_logger import list_run_records


def cmd_show(args) -> None:
    """Show confidence score for a single pipeline."""
    result = compute_confidence(args.pipeline, base_dir=args.base_dir)
    if args.json:
        print(json.dumps(result, indent=2))
    else:
        print(format_confidence_report(result))


def cmd_list(args) -> None:
    """Show confidence scores for all known pipelines."""
    records = list_run_records(base_dir=args.base_dir)
    pipelines = sorted({r["pipeline"] for r in records if r.get("pipeline")})

    if not pipelines:
        print("No pipelines found.")
        return

    results = [
        compute_confidence(p, base_dir=args.base_dir) for p in pipelines
    ]
    results.sort(key=lambda r: r["score"], reverse=True)

    if args.json:
        print(json.dumps(results, indent=2))
    else:
        header = f"{'Pipeline':<30} {'Score':>7}  Grade"
        print(header)
        print("-" * len(header))
        for r in results:
            print(f"{r['pipeline']:<30} {r['score']:>7.1f}  {r['grade']}")


def build_parser(subparsers=None):
    if subparsers is None:
        parser = argparse.ArgumentParser(
            description="Pipeline confidence scoring"
        )
        sub = parser.add_subparsers(dest="command")
    else:
        parser = subparsers.add_parser("confidence", help="Pipeline confidence")
        sub = parser.add_subparsers(dest="confidence_command")

    parser.add_argument(
        "--base-dir", default=".pipewatch", help="Storage base directory"
    )

    p_show = sub.add_parser("show", help="Show confidence for a pipeline")
    p_show.add_argument("pipeline", help="Pipeline name")
    p_show.add_argument("--json", action="store_true", help="JSON output")
    p_show.set_defaults(func=cmd_show)

    p_list = sub.add_parser("list", help="List confidence for all pipelines")
    p_list.add_argument("--json", action="store_true", help="JSON output")
    p_list.set_defaults(func=cmd_list)

    return parser


def main(argv=None):
    parser = build_parser()
    args = parser.parse_args(argv)
    if not hasattr(args, "func"):
        parser.print_help()
        sys.exit(1)
    args.func(args)


if __name__ == "__main__":
    main()
