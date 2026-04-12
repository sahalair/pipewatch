"""CLI for pipeline reputation scoring."""

import argparse
import json
import sys

from pipewatch.run_reputation import compute_reputation, format_reputation_report


def cmd_show(args) -> None:
    rep = compute_reputation(
        pipeline=args.pipeline,
        base_dir=getattr(args, "base_dir", "."),
        limit=getattr(args, "limit", 20),
    )
    if getattr(args, "json", False):
        print(json.dumps(rep, indent=2))
    else:
        print(format_reputation_report(rep))


def cmd_list(args) -> None:
    """Show reputation for a list of pipelines."""
    pipelines = args.pipelines
    if not pipelines:
        print("No pipelines specified.")
        return
    base_dir = getattr(args, "base_dir", ".")
    limit = getattr(args, "limit", 20)
    records = [compute_reputation(p, base_dir=base_dir, limit=limit) for p in pipelines]
    if getattr(args, "json", False):
        print(json.dumps(records, indent=2))
    else:
        for rep in sorted(records, key=lambda r: r["score"], reverse=True):
            print(
                f"{rep['pipeline']:<30}  score={rep['score']:5.1f}  "
                f"grade={rep['grade']:<10}  runs={rep['run_count']}"
            )


def build_parser(parent=None) -> argparse.ArgumentParser:
    if parent is None:
        parser = argparse.ArgumentParser(
            prog="pipewatch reputation",
            description="Pipeline reputation scoring.",
        )
    else:
        parser = parent

    sub = parser.add_subparsers(dest="reputation_cmd")

    p_show = sub.add_parser("show", help="Show reputation for one pipeline.")
    p_show.add_argument("pipeline", help="Pipeline name.")
    p_show.add_argument("--limit", type=int, default=20)
    p_show.add_argument("--json", action="store_true")
    p_show.add_argument("--base-dir", dest="base_dir", default=".")
    p_show.set_defaults(func=cmd_show)

    p_list = sub.add_parser("list", help="Show reputation for multiple pipelines.")
    p_list.add_argument("pipelines", nargs="+", help="Pipeline names.")
    p_list.add_argument("--limit", type=int, default=20)
    p_list.add_argument("--json", action="store_true")
    p_list.add_argument("--base-dir", dest="base_dir", default=".")
    p_list.set_defaults(func=cmd_list)

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
