"""CLI entry-point for run badge commands."""

from __future__ import annotations

import argparse
import json
import sys

from pipewatch.run_badges import (
    build_score_badge,
    build_status_badge,
    build_flakiness_badge,
    format_badge_text,
)


def cmd_score(args: argparse.Namespace) -> None:
    badge = build_score_badge(args.pipeline, base_dir=args.base_dir)
    if args.json:
        print(json.dumps(badge, indent=2))
    else:
        print(format_badge_text(badge))


def cmd_status(args: argparse.Namespace) -> None:
    badge = build_status_badge(args.pipeline, base_dir=args.base_dir)
    if args.json:
        print(json.dumps(badge, indent=2))
    else:
        print(format_badge_text(badge))


def cmd_flakiness(args: argparse.Namespace) -> None:
    badge = build_flakiness_badge(args.pipeline, base_dir=args.base_dir)
    if args.json:
        print(json.dumps(badge, indent=2))
    else:
        print(format_badge_text(badge))


def cmd_all(args: argparse.Namespace) -> None:
    badges = {
        "score": build_score_badge(args.pipeline, base_dir=args.base_dir),
        "status": build_status_badge(args.pipeline, base_dir=args.base_dir),
        "flakiness": build_flakiness_badge(args.pipeline, base_dir=args.base_dir),
    }
    if args.json:
        print(json.dumps(badges, indent=2))
    else:
        for badge in badges.values():
            print(format_badge_text(badge))


def build_parser(parent: argparse.ArgumentParser | None = None) -> argparse.ArgumentParser:
    p = parent or argparse.ArgumentParser(prog="pipewatch badges")
    p.add_argument("--base-dir", default=".", help="pipewatch data directory")
    p.add_argument("--json", action="store_true", help="output as JSON")
    sub = p.add_subparsers(dest="badge_cmd")

    for name, fn in [("score", cmd_score), ("status", cmd_status),
                     ("flakiness", cmd_flakiness), ("all", cmd_all)]:
        sp = sub.add_parser(name)
        sp.add_argument("pipeline")
        sp.set_defaults(func=fn)

    return p


def main(argv: list[str] | None = None) -> None:
    parser = build_parser()
    args = parser.parse_args(argv)
    if not hasattr(args, "func"):
        parser.print_help()
        sys.exit(1)
    args.func(args)


if __name__ == "__main__":
    main()
