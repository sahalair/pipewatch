"""CLI commands for managing pipeline run schedules."""

from __future__ import annotations

import argparse
import sys
from typing import Any

from pipewatch.run_schedule import set_schedule, remove_schedule, load_schedules, check_overdue
from pipewatch.run_logger import load_run_record, list_run_records


def cmd_set(args: Any) -> None:
    rule = set_schedule(args.pipeline, args.interval)
    print(f"Schedule set: {rule['pipeline']} every {rule['interval_minutes']} minute(s).")


def cmd_remove(args: Any) -> None:
    removed = remove_schedule(args.pipeline)
    if removed:
        print(f"Schedule removed for pipeline '{args.pipeline}'.")
    else:
        print(f"No schedule found for pipeline '{args.pipeline}'.")


def cmd_list(args: Any) -> None:
    schedules = load_schedules()
    if not schedules:
        print("No schedules configured.")
        return
    print(f"{'Pipeline':<30} {'Interval (min)':>15}")
    print("-" * 47)
    for name, rule in sorted(schedules.items()):
        print(f"{name:<30} {rule['interval_minutes']:>15}")


def cmd_check(args: Any) -> None:
    schedules = load_schedules()
    if not schedules:
        print("No schedules configured.")
        return

    pipelines = [args.pipeline] if args.pipeline else list(schedules.keys())
    any_overdue = False

    for pipeline in sorted(pipelines):
        last_run_iso = _get_last_run_iso(pipeline)
        result = check_overdue(pipeline, last_run_iso)
        if not result["scheduled"]:
            print(f"[UNSCHEDULED] {pipeline}")
            continue
        if result["overdue"]:
            any_overdue = True
            print(f"[OVERDUE]     {pipeline}  (+{result['minutes_overdue']} min overdue)")
        else:
            print(f"[OK]          {pipeline}")

    if any_overdue:
        sys.exit(1)


def _get_last_run_iso(pipeline: str) -> str | None:
    """Return the ISO timestamp of the most recent completed run for a pipeline."""
    try:
        records = list_run_records()
    except Exception:
        return None
    matching = [
        r for r in records
        if r.get("pipeline") == pipeline and r.get("finished_at")
    ]
    if not matching:
        return None
    return max(matching, key=lambda r: r["finished_at"])["finished_at"]


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="pipewatch-schedule", description="Manage pipeline schedules.")
    sub = parser.add_subparsers(dest="command", required=True)

    p_set = sub.add_parser("set", help="Set schedule for a pipeline")
    p_set.add_argument("pipeline")
    p_set.add_argument("interval", type=int, help="Expected interval in minutes")
    p_set.set_defaults(func=cmd_set)

    p_rm = sub.add_parser("remove", help="Remove schedule for a pipeline")
    p_rm.add_argument("pipeline")
    p_rm.set_defaults(func=cmd_remove)

    p_ls = sub.add_parser("list", help="List all schedules")
    p_ls.set_defaults(func=cmd_list)

    p_chk = sub.add_parser("check", help="Check for overdue pipelines")
    p_chk.add_argument("pipeline", nargs="?", default=None, help="Specific pipeline (default: all)")
    p_chk.set_defaults(func=cmd_check)

    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
