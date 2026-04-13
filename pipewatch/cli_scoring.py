"""CLI for run scoring."""

from __future__ import annotations

import argparse
import json
import sys

from pipewatch.run_logger import load_run_record, list_run_records
from pipewatch.run_scoring import compute_score, format_score_report


def cmd_show(args: argparse.Namespace) -> None:
    run = load_run_record(args.run_id, base_dir=args.base_dir)
    if run is None:
        print(f"Run not found: {args.run_id}", file=sys.stderr)
        sys.exit(1)
    result = compute_score(run, base_dir=args.base_dir)
    if args.json:
        print(json.dumps(result, indent=2))
    else:
        print(format_score_report(result))


def cmd_list(args: argparse.Namespace) -> None:
    records = list_run_records(base_dir=args.base_dir)
    if args.pipeline:
        records = [r for r in records if r.get("pipeline") == args.pipeline]
    if not records:
        print("No runs found.")
        return
    results = [compute_score(r, base_dir=args.base_dir) for r in records]
    results.sort(key=lambda x: x["score"], reverse=True)
    if args.json:
        print(json.dumps(results, indent=2))
    else:
        header = f"{'Run ID':<36}  {'Pipeline':<20}  {'Score':>6}  Grade"
        print(header)
        print("-" * len(header))
        for r in results:
            print(
                f"{r['run_id']:<36}  {r['pipeline']:<20}  {r['score']:>6.1f}  {r['grade']}"
            )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="pipewatch-scoring",
        description="Score pipeline runs by composite quality metric.",
    )
    parser.add_argument("--base-dir", default=".", help="Data directory.")
    sub = parser.add_subparsers(dest="command")

    p_show = sub.add_parser("show", help="Show score for a single run.")
    p_show.add_argument("run_id")
    p_show.add_argument("--json", action="store_true")
    p_show.set_defaults(func=cmd_show)

    p_list = sub.add_parser("list", help="List scores for all runs.")
    p_list.add_argument("--pipeline", default=None)
    p_list.add_argument("--json", action="store_true")
    p_list.set_defaults(func=cmd_list)

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
