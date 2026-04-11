"""CLI commands for managing run checkpoints."""

import argparse
import json
import sys

from pipewatch.run_checkpoints import (
    record_checkpoint,
    get_checkpoints,
    clear_checkpoints,
    checkpoint_summary,
)


def cmd_add(args) -> None:
    data = None
    if args.data:
        try:
            data = json.loads(args.data)
        except json.JSONDecodeError as exc:
            print(f"Error: invalid JSON for --data: {exc}", file=sys.stderr)
            sys.exit(1)
    entry = record_checkpoint(
        run_id=args.run_id,
        name=args.name,
        status=args.status,
        message=args.message,
        data=data,
    )
    print(f"Checkpoint '{entry['name']}' recorded [{entry['status']}] at {entry['timestamp']}")


def cmd_list(args) -> None:
    checkpoints = get_checkpoints(args.run_id, name=args.name)
    if not checkpoints:
        print("No checkpoints found.")
        return
    for c in checkpoints:
        msg = f"  {c['message']}" if c.get("message") else ""
        print(f"[{c['status'].upper():4s}] {c['name']:30s} {c['timestamp']}{msg}")


def cmd_summary(args) -> None:
    summary = checkpoint_summary(args.run_id)
    print(f"Run: {args.run_id}")
    print(f"  Total : {summary['total']}")
    print(f"  OK    : {summary['ok']}")
    print(f"  Warn  : {summary['warn']}")
    print(f"  Fail  : {summary['fail']}")


def cmd_clear(args) -> None:
    count = clear_checkpoints(args.run_id)
    print(f"Cleared {count} checkpoint(s) for run {args.run_id}.")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="pipewatch checkpoints", description="Manage run checkpoints.")
    sub = parser.add_subparsers(dest="command")

    p_add = sub.add_parser("add", help="Record a checkpoint")
    p_add.add_argument("run_id")
    p_add.add_argument("name")
    p_add.add_argument("--status", default="ok", choices=["ok", "warn", "fail"])
    p_add.add_argument("--message", default=None)
    p_add.add_argument("--data", default=None, help="JSON string of extra data")
    p_add.set_defaults(func=cmd_add)

    p_list = sub.add_parser("list", help="List checkpoints for a run")
    p_list.add_argument("run_id")
    p_list.add_argument("--name", default=None, help="Filter by checkpoint name")
    p_list.set_defaults(func=cmd_list)

    p_sum = sub.add_parser("summary", help="Summarise checkpoint statuses")
    p_sum.add_argument("run_id")
    p_sum.set_defaults(func=cmd_summary)

    p_clr = sub.add_parser("clear", help="Clear all checkpoints for a run")
    p_clr.add_argument("run_id")
    p_clr.set_defaults(func=cmd_clear)

    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()
    if not args.command:
        parser.print_help()
        sys.exit(0)
    args.func(args)


if __name__ == "__main__":
    main()
