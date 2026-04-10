"""CLI commands for managing and evaluating pipewatch alert rules."""

from __future__ import annotations

import argparse
import sys

from pipewatch.alert import (
    create_alert_rule,
    evaluate_alert,
    format_alert_message,
    load_alert_rules,
    save_alert_rules,
)
from pipewatch.snapshot_diff import diff_snapshots


def cmd_add(args: argparse.Namespace) -> None:
    """Add a new alert rule."""
    rules = load_alert_rules(args.alerts_file)
    rule = create_alert_rule(
        name=args.name,
        snapshot_key=args.snapshot_key,
        threshold=args.threshold,
        notify=args.notify,
    )
    rules.append(rule)
    save_alert_rules(rules, args.alerts_file)
    print(f"Alert rule '{args.name}' added for snapshot '{args.snapshot_key}'.")


def cmd_list(args: argparse.Namespace) -> None:
    """List all alert rules."""
    rules = load_alert_rules(args.alerts_file)
    if not rules:
        print("No alert rules defined.")
        return
    for i, rule in enumerate(rules, 1):
        status = "enabled" if rule.get("enabled", True) else "disabled"
        print(
            f"{i}. [{status}] {rule['name']} "
            f"— snapshot: {rule['snapshot_key']} "
            f"| threshold: {rule['threshold']}"
        )


def cmd_check(args: argparse.Namespace) -> None:
    """Evaluate all alert rules against recent snapshot diffs."""
    rules = load_alert_rules(args.alerts_file)
    if not rules:
        print("No alert rules to evaluate.")
        return
    fired = False
    for rule in rules:
        key = rule["snapshot_key"]
        try:
            result = diff_snapshots(key, args.snapshots_dir)
        except (FileNotFoundError, ValueError) as exc:
            print(f"[SKIP] Rule '{rule['name']}': {exc}")
            continue
        if evaluate_alert(rule, result):
            fired = True
            print(format_alert_message(rule, result))
    if not fired:
        print("No alerts triggered.")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="pipewatch-alert",
        description="Manage and evaluate pipewatch alert rules.",
    )
    parser.add_argument(
        "--alerts-file",
        default=".pipewatch_alerts.json",
        help="Path to alerts JSON file.",
    )
    parser.add_argument(
        "--snapshots-dir",
        default=".pipewatch_snapshots",
        help="Directory containing snapshots.",
    )
    sub = parser.add_subparsers(dest="command")

    add_p = sub.add_parser("add", help="Add an alert rule.")
    add_p.add_argument("name", help="Rule name.")
    add_p.add_argument("snapshot_key", help="Snapshot key to monitor.")
    add_p.add_argument("--threshold", type=float, default=0.0)
    add_p.add_argument("--notify", default="stdout")
    add_p.set_defaults(func=cmd_add)

    list_p = sub.add_parser("list", help="List alert rules.")
    list_p.set_defaults(func=cmd_list)

    check_p = sub.add_parser("check", help="Evaluate alert rules.")
    check_p.set_defaults(func=cmd_check)

    return parser


def main(argv: list[str] | None = None) -> None:
    parser = build_parser()
    args = parser.parse_args(argv)
    if not hasattr(args, "func"):
        parser.print_help()
        sys.exit(1)
    args.func(args)


if __name__ == "__main__":
    main()
