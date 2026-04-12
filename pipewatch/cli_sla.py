"""CLI commands for managing pipeline SLA rules."""

import argparse
import json
import sys
from pathlib import Path

from pipewatch.run_sla import (
    set_sla, remove_sla, get_sla, load_sla_rules,
    evaluate_sla, format_sla_report, DEFAULT_SLA_DIR,
)
from pipewatch.run_duration import get_durations_for_pipeline


def cmd_set(args, base_dir: Path = DEFAULT_SLA_DIR) -> None:
    rule = set_sla(args.pipeline, args.max_seconds,
                   description=args.description or "", base_dir=base_dir)
    print(f"SLA set for '{rule['pipeline']}': max {rule['max_duration_seconds']}s")


def cmd_remove(args, base_dir: Path = DEFAULT_SLA_DIR) -> None:
    removed = remove_sla(args.pipeline, base_dir=base_dir)
    if removed:
        print(f"SLA rule removed for '{args.pipeline}'.")
    else:
        print(f"No SLA rule found for '{args.pipeline}'.")


def cmd_list(args, base_dir: Path = DEFAULT_SLA_DIR) -> None:
    rules = load_sla_rules(base_dir)
    if not rules:
        print("No SLA rules defined.")
        return
    for pipeline, rule in sorted(rules.items()):
        desc = f" — {rule['description']}" if rule.get("description") else ""
        print(f"  {pipeline}: max {rule['max_duration_seconds']}s{desc}")


def cmd_check(args, base_dir: Path = DEFAULT_SLA_DIR) -> None:
    durations = get_durations_for_pipeline(args.pipeline, limit=args.last or 10)
    if not durations:
        print(f"No completed runs found for pipeline '{args.pipeline}'.")
        return
    violations = 0
    for entry in durations:
        result = evaluate_sla(args.pipeline, entry["duration_seconds"], base_dir=base_dir)
        report = format_sla_report(result)
        if args.json:
            print(json.dumps(result))
        else:
            print(f"  [{entry['run_id'][:8]}] {report}")
        if result.get("violated"):
            violations += 1
    if not args.json:
        print(f"\n{violations}/{len(durations)} runs violated the SLA.")
    if violations and not args.json:
        sys.exit(1)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="pipewatch sla",
                                     description="Manage pipeline SLA rules.")
    sub = parser.add_subparsers(dest="command")

    p_set = sub.add_parser("set", help="Define an SLA for a pipeline")
    p_set.add_argument("pipeline")
    p_set.add_argument("max_seconds", type=float)
    p_set.add_argument("--description", default="")
    p_set.set_defaults(func=cmd_set)

    p_rm = sub.add_parser("remove", help="Remove an SLA rule")
    p_rm.add_argument("pipeline")
    p_rm.set_defaults(func=cmd_remove)

    p_ls = sub.add_parser("list", help="List all SLA rules")
    p_ls.set_defaults(func=cmd_list)

    p_chk = sub.add_parser("check", help="Check recent runs against SLA")
    p_chk.add_argument("pipeline")
    p_chk.add_argument("--last", type=int, default=10)
    p_chk.add_argument("--json", action="store_true")
    p_chk.set_defaults(func=cmd_check)

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
