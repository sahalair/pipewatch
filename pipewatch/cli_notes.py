"""CLI sub-commands for run notes."""

from __future__ import annotations

import argparse
import sys

from pipewatch.run_notes import add_note, delete_notes, format_notes, get_notes

_DEFAULT_DIR = ".pipewatch"


def cmd_add(args: argparse.Namespace) -> None:
    note = add_note(
        base_dir=args.base_dir,
        run_id=args.run_id,
        text=args.text,
        author=getattr(args, "author", None),
    )
    print(f"Note added at {note['timestamp']}.")


def cmd_list(args: argparse.Namespace) -> None:
    notes = get_notes(base_dir=args.base_dir, run_id=args.run_id)
    print(format_notes(notes))


def cmd_delete(args: argparse.Namespace) -> None:
    count = delete_notes(base_dir=args.base_dir, run_id=args.run_id)
    print(f"Deleted {count} note(s) for run '{args.run_id}'.")


def build_parser(parent: argparse.ArgumentParser | None = None) -> argparse.ArgumentParser:
    parser = parent or argparse.ArgumentParser(prog="pipewatch notes", description="Manage run notes")
    parser.add_argument("--base-dir", default=_DEFAULT_DIR, help="Storage directory")
    sub = parser.add_subparsers(dest="notes_cmd", required=True)

    p_add = sub.add_parser("add", help="Add a note to a run")
    p_add.add_argument("run_id", help="Target run ID")
    p_add.add_argument("text", help="Note text")
    p_add.add_argument("--author", default=None, help="Author name")
    p_add.set_defaults(func=cmd_add)

    p_list = sub.add_parser("list", help="List notes for a run")
    p_list.add_argument("run_id", help="Target run ID")
    p_list.set_defaults(func=cmd_list)

    p_del = sub.add_parser("delete", help="Delete all notes for a run")
    p_del.add_argument("run_id", help="Target run ID")
    p_del.set_defaults(func=cmd_delete)

    return parser


def main(argv: list[str] | None = None) -> None:
    parser = build_parser()
    args = parser.parse_args(argv)
    if hasattr(args, "func"):
        args.func(args)
    else:
        parser.print_help()
        sys.exit(1)


if __name__ == "__main__":
    main()
