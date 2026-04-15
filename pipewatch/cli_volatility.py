"""CLI for run volatility reports."""

import argparse
import json
import sys

from pipewatch.run_volatility import (
    compute_volatility,
    format_volatility_report,
    volatility_for_all_pipelines,
)


def cmd_show(args) -> None:
    result = compute_volatility(args.pipeline, base_dir=args.base_dir, limit=args.limit)
    if args.json:
        print(json.dumps(result, indent=2))
    else:
        print(format_volatility_report(result))


def cmd_list(args) -> None:
    results = volatility_for_all_pipelines(base_dir=args.base_dir, limit=args.limit)
    if not results:
        print("No pipelines found.")
        return
    if args.json:
        print(json.dumps(results, indent=2))
    else:
        header = f"{'Pipeline':<30} {'Samples':>7} {'CV':>8} {'Grade':<10}"
        print(header)
        print("-" * len(header))
        for r in results:
            cv = r["coefficient_of_variation"]
            cv_str = f"{cv:.4f}" if cv is not None else "  N/A"
            print(f"{r['pipeline']:<30} {r['sample_count']:>7} {cv_str:>8} {r['grade']:<10}")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Pipeline duration volatility")
    parser.add_argument("--base-dir", default=".", help="Base directory for data")
    parser.add_argument("--limit", type=int, default=20, help="Max runs to consider")
    parser.add_argument("--json", action="store_true", help="Output as JSON")
    sub = parser.add_subparsers(dest="command")

    show_p = sub.add_parser("show", help="Show volatility for a pipeline")
    show_p.add_argument("pipeline", help="Pipeline name")
    show_p.set_defaults(func=cmd_show)

    list_p = sub.add_parser("list", help="List volatility for all pipelines")
    list_p.set_defaults(func=cmd_list)

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
