"""CLI commands for managing run feedback."""

import argparse
import json
import sys

from pipewatch.run_feedback import add_feedback, get_feedback, remove_feedback, load_feedback, average_rating

_DEFAULT_DIR = ".pipewatch"


def cmd_add(args) -> None:
    base_dir = getattr(args, "base_dir", _DEFAULT_DIR)
    try:
        entry = add_feedback(base_dir, args.run_id, int(args.rating), getattr(args, "comment", None), getattr(args, "author", None))
        print(f"Feedback recorded for run '{args.run_id}': rating={entry['rating']}")
    except ValueError as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)


def cmd_get(args) -> None:
    base_dir = getattr(args, "base_dir", _DEFAULT_DIR)
    entry = get_feedback(base_dir, args.run_id)
    if entry is None:
        print(f"No feedback found for run '{args.run_id}'.")
        return
    if getattr(args, "json", False):
        print(json.dumps(entry, indent=2))
    else:
        print(f"Run: {args.run_id}")
        print(f"  Rating : {entry['rating']}/5")
        print(f"  Comment: {entry['comment'] or '—'}")
        print(f"  Author : {entry['author'] or '—'}")


def cmd_remove(args) -> None:
    base_dir = getattr(args, "base_dir", _DEFAULT_DIR)
    removed = remove_feedback(base_dir, args.run_id)
    if removed:
        print(f"Feedback removed for run '{args.run_id}'.")
    else:
        print(f"No feedback found for run '{args.run_id}'.")


def cmd_list(args) -> None:
    base_dir = getattr(args, "base_dir", _DEFAULT_DIR)
    data = load_feedback(base_dir)
    if not data:
        print("No feedback entries found.")
        return
    for run_id, entry in sorted(data.items()):
        stars = "*" * entry["rating"]
        comment = entry["comment"] or ""
        print(f"{run_id:36s}  {stars:<5}  {comment}")


def cmd_average(args) -> None:
    base_dir = getattr(args, "base_dir", _DEFAULT_DIR)
    avg = average_rating(base_dir)
    if avg is None:
        print("No feedback available to compute average.")
    else:
        print(f"Average rating: {avg}/5")


def build_parser(parent=None) -> argparse.ArgumentParser:
    p = parent or argparse.ArgumentParser(prog="pipewatch feedback", description="Manage run feedback")
    sub = p.add_subparsers(dest="feedback_cmd")

    a = sub.add_parser("add", help="Add or update feedback for a run")
    a.add_argument("run_id")
    a.add_argument("rating", type=int, choices=[1, 2, 3, 4, 5])
    a.add_argument("--comment", default=None)
    a.add_argument("--author", default=None)
    a.set_defaults(func=cmd_add)

    g = sub.add_parser("get", help="Show feedback for a run")
    g.add_argument("run_id")
    g.add_argument("--json", action="store_true")
    g.set_defaults(func=cmd_get)

    r = sub.add_parser("remove", help="Remove feedback for a run")
    r.add_argument("run_id")
    r.set_defaults(func=cmd_remove)

    sub.add_parser("list", help="List all feedback").set_defaults(func=cmd_list)
    sub.add_parser("average", help="Show average rating").set_defaults(func=cmd_average)
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
