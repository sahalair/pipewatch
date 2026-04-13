"""cli_decay.py — CLI interface for pipeline freshness/decay scoring."""

import argparse
import json
import sys

from pipewatch.run_decay import compute_decay_score, format_decay_report
from pipewatch.run_logger import list_run_records


def cmd_show(args) -> None:
    try:
        result = compute_decay_score(args.pipeline, half_life_hours=args.half_life)
    except ValueError as exc:
        print(f"Error: {exc}", file=sys.stderr)
        sys.exit(1)

    if args.json:
        print(json.dumps(result, indent=2))
    else:
        print(format_decay_report(result))


def cmd_list(args) -> None:
    pipelines = sorted({r.get("pipeline") for r in list_run_records() if r.get("pipeline")})
    if not pipelines:
        print("No pipelines found.")
        return

    results = [compute_decay_score(p, half_life_hours=args.half_life) for p in pipelines]
    results.sort(key=lambda r: r["score"])

    if args.json:
        print(json.dumps(results, indent=2))
        return

    print(f"{'Pipeline':<30} {'Score':>6}  {'Grade':<8}  Hours Since OK")
    print("-" * 62)
    for r in results:
        hours = f"{r['hours_since_success']}h" if r["hours_since_success"] is not None else "never"
        print(f"{r['pipeline']:<30} {r['score']:>6.3f}  {r['grade']:<8}  {hours}")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="pipewatch-decay", description="Pipeline freshness decay scoring")
    parser.add_argument("--half-life", type=float, default=24.0, metavar="HOURS",
                        help="Half-life in hours for decay calculation (default: 24)")
    parser.add_argument("--json", action="store_true", help="Output as JSON")
    sub = parser.add_subparsers(dest="command", required=True)

    p_show = sub.add_parser("show", help="Show decay score for a pipeline")
    p_show.add_argument("pipeline", help="Pipeline name")
    p_show.set_defaults(func=cmd_show)

    p_list = sub.add_parser("list", help="List decay scores for all pipelines")
    p_list.set_defaults(func=cmd_list)

    return parser


def main():
    parser = build_parser()
    args = parser.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
