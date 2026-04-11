"""CLI commands for run event logs."""

from __future__ import annotations

import argparse
import sys

from pipewatch.run_events import (
    clear_events,
    format_events,
    get_events,
    record_event,
)


def cmd_add(args: argparse.Namespace) -> None:
    event = record_event(
        args.run_id,
        args.event_type,
        args.message,
        level=args.level,
    )
    print(f"Event recorded at {event['timestamp']} [{event['level'].upper()}]")


def cmd_list(args: argparse.Namespace) -> None:
    events = get_events(
        args.run_id,
        level=getattr(args, "level", None),
        event_type=getattr(args, "event_type", None),
    )
    print(format_events(events))


def cmd_clear(args: argparse.Namespace) -> None:
    removed = clear_events(args.run_id)
    print(f"Cleared {removed} event(s) for run '{args.run_id}'.")


def build_parser(parent: argparse._SubParsersAction | None = None) -> argparse.ArgumentParser:  # type: ignore[type-arg]
    if parent is not None:
        p = parent.add_parser("events", help="Manage run event logs")
    else:
        p = argparse.ArgumentParser(prog="pipewatch events", description="Manage run event logs")

    sub = p.add_subparsers(dest="events_cmd", required=True)

    add_p = sub.add_parser("add", help="Record a new event")
    add_p.add_argument("run_id")
    add_p.add_argument("event_type")
    add_p.add_argument("message")
    add_p.add_argument("--level", default="info", choices=["info", "warning", "error", "debug"])
    add_p.set_defaults(func=cmd_add)

    list_p = sub.add_parser("list", help="List events for a run")
    list_p.add_argument("run_id")
    list_p.add_argument("--level", default=None)
    list_p.add_argument("--event-type", dest="event_type", default=None)
    list_p.set_defaults(func=cmd_list)

    clear_p = sub.add_parser("clear", help="Clear all events for a run")
    clear_p.add_argument("run_id")
    clear_p.set_defaults(func=cmd_clear)

    return p


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()
    if not hasattr(args, "func"):
        parser.print_help()
        sys.exit(1)
    args.func(args)


if __name__ == "__main__":
    main()
