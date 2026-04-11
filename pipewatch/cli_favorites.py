"""CLI commands for managing favorite (starred) runs."""

import argparse
from pipewatch.run_favorites import (
    add_favorite,
    remove_favorite,
    is_favorite,
    list_favorites,
    find_favorites_by_label,
)


def cmd_add(args) -> None:
    entry = add_favorite(
        run_id=args.run_id,
        label=getattr(args, "label", "") or "",
        note=getattr(args, "note", "") or "",
    )
    print(f"Starred run '{entry['run_id']}' (label={entry['label']!r})")


def cmd_remove(args) -> None:
    removed = remove_favorite(args.run_id)
    if removed:
        print(f"Unstarred run '{args.run_id}'")
    else:
        print(f"Run '{args.run_id}' was not in favorites")


def cmd_list(args) -> None:
    label_filter = getattr(args, "label", None)
    if label_filter:
        entries = find_favorites_by_label(label_filter)
    else:
        entries = list_favorites()

    if not entries:
        print("No favorites found.")
        return

    for entry in entries:
        parts = [entry["run_id"]]
        if entry.get("label"):
            parts.append(f"[{entry['label']}]")
        if entry.get("note"):
            parts.append(f"- {entry['note']}")
        print(" ".join(parts))


def cmd_check(args) -> None:
    starred = is_favorite(args.run_id)
    status = "starred" if starred else "not starred"
    print(f"Run '{args.run_id}' is {status}")


def build_parser(parser: argparse.ArgumentParser = None) -> argparse.ArgumentParser:
    if parser is None:
        parser = argparse.ArgumentParser(prog="pipewatch favorites")
    sub = parser.add_subparsers(dest="favorites_cmd")

    p_add = sub.add_parser("add", help="Star a run")
    p_add.add_argument("run_id")
    p_add.add_argument("--label", default="")
    p_add.add_argument("--note", default="")
    p_add.set_defaults(func=cmd_add)

    p_rm = sub.add_parser("remove", help="Unstar a run")
    p_rm.add_argument("run_id")
    p_rm.set_defaults(func=cmd_remove)

    p_ls = sub.add_parser("list", help="List starred runs")
    p_ls.add_argument("--label", default=None)
    p_ls.set_defaults(func=cmd_list)

    p_chk = sub.add_parser("check", help="Check if a run is starred")
    p_chk.add_argument("run_id")
    p_chk.set_defaults(func=cmd_check)

    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()
    if hasattr(args, "func"):
        args.func(args)
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
