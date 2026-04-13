"""CLI commands for the run audit log."""
import argparse
import json
import sys
from pipewatch.run_audit import record_audit_event, get_audit_events, format_audit_log

DEFAULT_DIR = ".pipewatch"


def cmd_add(args) -> None:
    details = {}
    if args.details:
        try:
            details = json.loads(args.details)
        except json.JSONDecodeError:
            print(f"Error: --details must be valid JSON.", file=sys.stderr)
            sys.exit(1)
    entry = record_audit_event(
        base_dir=args.base_dir,
        run_id=args.run_id,
        action=args.action,
        actor=args.actor,
        details=details,
    )
    print(f"Audit event recorded: {entry['action']} for run {entry['run_id']} at {entry['timestamp']}")


def cmd_list(args) -> None:
    entries = get_audit_events(
        base_dir=args.base_dir,
        run_id=getattr(args, "run_id", None),
        action=getattr(args, "action", None),
    )
    if args.json:
        print(json.dumps(entries, indent=2))
    else:
        print(format_audit_log(entries))


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="pipewatch audit", description="Manage run audit log")
    parser.add_argument("--base-dir", default=DEFAULT_DIR)
    sub = parser.add_subparsers(dest="command")

    p_add = sub.add_parser("add", help="Record an audit event")
    p_add.add_argument("run_id")
    p_add.add_argument("action")
    p_add.add_argument("--actor", default=None)
    p_add.add_argument("--details", default=None, help="JSON string of extra details")
    p_add.set_defaults(func=cmd_add)

    p_list = sub.add_parser("list", help="List audit events")
    p_list.add_argument("--run-id", dest="run_id", default=None)
    p_list.add_argument("--action", default=None)
    p_list.add_argument("--json", action="store_true")
    p_list.set_defaults(func=cmd_list)

    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()
    if not hasattr(args, "func"):
        parser.print_help()
        sys.exit(0)
    args.func(args)


if __name__ == "__main__":
    main()
