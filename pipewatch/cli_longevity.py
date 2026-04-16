"""CLI for run longevity reporting."""
import argparse
import json
from pipewatch.run_logger import list_run_records
from pipewatch.run_longevity import compute_longevity, format_longevity_report


def cmd_show(args) -> None:
    result = compute_longevity(args.pipeline, base_dir=args.base_dir)
    if args.json:
        print(json.dumps(result, indent=2))
    else:
        print(format_longevity_report(result))


def cmd_list(args) -> None:
    records = list_run_records(base_dir=args.base_dir)
    pipelines = sorted({r["pipeline"] for r in records if r.get("pipeline")})
    if not pipelines:
        print("No pipelines found.")
        return
    results = [compute_longevity(p, base_dir=args.base_dir) for p in pipelines]
    if args.json:
        print(json.dumps(results, indent=2))
    else:
        for r in results:
            print(format_longevity_report(r))
            print()


def build_parser(parent=None) -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="pipewatch longevity",
        description="Pipeline longevity reports.",
    ) if parent is None else parent
    parser.add_argument("--base-dir", default=".", help="Base directory for data.")
    parser.add_argument("--json", action="store_true", help="Output as JSON.")
    sub = parser.add_subparsers(dest="subcmd")

    p_show = sub.add_parser("show", help="Show longevity for a pipeline.")
    p_show.add_argument("pipeline", help="Pipeline name.")
    p_show.set_defaults(func=cmd_show)

    p_list = sub.add_parser("list", help="List longevity for all pipelines.")
    p_list.set_defaults(func=cmd_list)

    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()
    if not hasattr(args, "func"):
        parser.print_help()
        return
    args.func(args)


if __name__ == "__main__":
    main()
