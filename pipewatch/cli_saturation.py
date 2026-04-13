"""CLI commands for run saturation reporting."""

import argparse
import json
import sys

from pipewatch.run_saturation import (
    compute_saturation,
    format_saturation_report,
    saturation_for_all_pipelines,
)


def cmd_show(args):
    result = compute_saturation(
        pipeline=args.pipeline,
        base_dir=getattr(args, "base_dir", "."),
        window=getattr(args, "window", "day"),
    )
    if getattr(args, "json", False):
        print(json.dumps(result, indent=2))
    else:
        print(format_saturation_report(result))


def cmd_list(args):
    results = saturation_for_all_pipelines(
        base_dir=getattr(args, "base_dir", "."),
        window=getattr(args, "window", "day"),
    )
    if not results:
        print("No capacity rules defined.")
        return
    if getattr(args, "json", False):
        print(json.dumps(results, indent=2))
    else:
        for r in results:
            warning = f"  [{r['warning']}]" if r.get("warning") else ""
            sat = r["saturation"]
            sat_str = f"{sat:.2%}" if sat is not None else "N/A"
            print(f"{r['pipeline']}: {sat_str} ({r['grade']}){warning}")


def build_parser(subparsers=None):
    if subparsers is None:
        parser = argparse.ArgumentParser(prog="pipewatch-saturation")
        sub = parser.add_subparsers(dest="command")
    else:
        parser = subparsers.add_parser("saturation", help="Pipeline saturation")
        sub = parser.add_subparsers(dest="saturation_command")

    p_show = sub.add_parser("show", help="Show saturation for a pipeline")
    p_show.add_argument("pipeline")
    p_show.add_argument("--window", default="day", choices=["hour", "day", "week"])
    p_show.add_argument("--json", action="store_true")
    p_show.set_defaults(func=cmd_show)

    p_list = sub.add_parser("list", help="List saturation for all pipelines")
    p_list.add_argument("--window", default="day", choices=["hour", "day", "week"])
    p_list.add_argument("--json", action="store_true")
    p_list.set_defaults(func=cmd_list)

    return parser


def main():
    parser = build_parser()
    args = parser.parse_args()
    if not hasattr(args, "func"):
        parser.print_help()
        sys.exit(1)
    args.func(args)


if __name__ == "__main__":
    main()
