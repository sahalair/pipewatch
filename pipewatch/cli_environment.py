"""CLI commands for run environment metadata."""

import argparse
import json
import sys

from pipewatch.run_environment import (
    compare_environments,
    load_environment,
    record_environment,
)


def cmd_capture(args: argparse.Namespace) -> None:
    """Capture and save environment metadata for a run."""
    extras: dict | None = None
    if args.extra:
        extras = {}
        for item in args.extra:
            if "=" not in item:
                print(f"[error] extra must be key=value, got: {item!r}", file=sys.stderr)
                sys.exit(1)
            k, v = item.split("=", 1)
            extras[k.strip()] = v.strip()

    env = record_environment(args.run_id, extras=extras)
    print(f"[ok] environment captured for run '{args.run_id}'")
    if args.verbose:
        print(json.dumps(env, indent=2))


def cmd_show(args: argparse.Namespace) -> None:
    """Print environment metadata for a run."""
    env = load_environment(args.run_id)
    if not env:
        print(f"[warn] no environment found for run '{args.run_id}'")
        return
    print(json.dumps(env, indent=2))


def cmd_compare(args: argparse.Namespace) -> None:
    """Compare environments of two runs."""
    result = compare_environments(args.run_id_a, args.run_id_b)
    if result["identical"]:
        print("Environments are identical.")
        return

    print(f"Environment diff: {args.run_id_a} vs {args.run_id_b}")
    print("-" * 50)
    for key, diff in result["changed"].items():
        print(f"  {key}:")
        print(f"    a: {diff['a']}")
        print(f"    b: {diff['b']}")


def build_parser(parent: argparse._SubParsersAction | None = None) -> argparse.ArgumentParser:
    desc = "Manage run environment metadata."
    if parent is not None:
        parser = parent.add_parser("environment", help=desc)
    else:
        parser = argparse.ArgumentParser(prog="pipewatch environment", description=desc)

    sub = parser.add_subparsers(dest="env_cmd", required=True)

    p_capture = sub.add_parser("capture", help="Capture environment for a run")
    p_capture.add_argument("run_id")
    p_capture.add_argument("--extra", nargs="*", metavar="KEY=VALUE",
                           help="Additional key=value pairs to store")
    p_capture.add_argument("--verbose", action="store_true")
    p_capture.set_defaults(func=cmd_capture)

    p_show = sub.add_parser("show", help="Show environment for a run")
    p_show.add_argument("run_id")
    p_show.set_defaults(func=cmd_show)

    p_cmp = sub.add_parser("compare", help="Compare environments of two runs")
    p_cmp.add_argument("run_id_a")
    p_cmp.add_argument("run_id_b")
    p_cmp.set_defaults(func=cmd_compare)

    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
