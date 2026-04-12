"""CLI commands for run ownership management."""

import argparse
import json
import sys
from pathlib import Path

from pipewatch.run_ownership import (
    get_owner,
    list_owned_runs,
    remove_owner,
    set_owner,
)

_DEFAULT_DIR = Path(".pipewatch/ownership")


def cmd_set(args: argparse.Namespace) -> None:
    base_dir = Path(getattr(args, "base_dir", _DEFAULT_DIR))
    record = set_owner(
        run_id=args.run_id,
        owner=args.owner,
        team=getattr(args, "team", None),
        contact=getattr(args, "contact", None),
        base_dir=base_dir,
    )
    print(f"Owner set for run '{args.run_id}': {record}")


def cmd_get(args: argparse.Namespace) -> None:
    base_dir = Path(getattr(args, "base_dir", _DEFAULT_DIR))
    record = get_owner(args.run_id, base_dir=base_dir)
    if record is None:
        print(f"No owner found for run '{args.run_id}'.")
        sys.exit(1)
    if getattr(args, "json", False):
        print(json.dumps(record, indent=2))
    else:
        for k, v in record.items():
            print(f"  {k}: {v}")


def cmd_remove(args: argparse.Namespace) -> None:
    base_dir = Path(getattr(args, "base_dir", _DEFAULT_DIR))
    removed = remove_owner(args.run_id, base_dir=base_dir)
    if removed:
        print(f"Ownership removed for run '{args.run_id}'.")
    else:
        print(f"No ownership record found for run '{args.run_id}'.")


def cmd_list(args: argparse.Namespace) -> None:
    base_dir = Path(getattr(args, "base_dir", _DEFAULT_DIR))
    runs = list_owned_runs(
        owner=getattr(args, "owner", None),
        team=getattr(args, "team", None),
        base_dir=base_dir,
    )
    if not runs:
        print("No ownership records found.")
        return
    if getattr(args, "json", False):
        print(json.dumps(runs, indent=2))
    else:
        for r in runs:
            parts = [r["run_id"], r.get("owner", "")]
            if r.get("team"):
                parts.append(f"team={r['team']}")
            if r.get("contact"):
                parts.append(f"contact={r['contact']}")
            print("  " + "  ".join(parts))


def build_parser(parent: argparse.ArgumentParser = None) -> argparse.ArgumentParser:
    p = parent or argparse.ArgumentParser(description="Manage run ownership")
    sub = p.add_subparsers(dest="ownership_cmd")

    ps = sub.add_parser("set", help="Set owner for a run")
    ps.add_argument("run_id")
    ps.add_argument("owner")
    ps.add_argument("--team")
    ps.add_argument("--contact")
    ps.set_defaults(func=cmd_set)

    pg = sub.add_parser("get", help="Get owner for a run")
    pg.add_argument("run_id")
    pg.add_argument("--json", action="store_true")
    pg.set_defaults(func=cmd_get)

    pr = sub.add_parser("remove", help="Remove owner for a run")
    pr.add_argument("run_id")
    pr.set_defaults(func=cmd_remove)

    pl = sub.add_parser("list", help="List owned runs")
    pl.add_argument("--owner")
    pl.add_argument("--team")
    pl.add_argument("--json", action="store_true")
    pl.set_defaults(func=cmd_list)

    return p


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()
    if hasattr(args, "func"):
        args.func(args)
    else:
        parser.print_help()
