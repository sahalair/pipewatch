"""cli_drift.py — CLI commands for environment drift detection."""

from __future__ import annotations

import argparse
import json
import sys

from pipewatch.run_drift import detect_drift, format_drift_report


def cmd_check(args: argparse.Namespace) -> None:
    keys = args.keys.split(",") if args.keys else None
    try:
        report = detect_drift(
            args.run_a,
            args.run_b,
            base_dir=args.base_dir,
            keys=keys,
        )
    except FileNotFoundError as exc:
        print(f"Error: {exc}", file=sys.stderr)
        sys.exit(1)

    if args.json:
        print(json.dumps(report, indent=2))
    else:
        print(format_drift_report(report))

    if report["drift_detected"] and args.fail_on_drift:
        sys.exit(2)


def build_parser(parent: argparse._SubParsersAction | None = None) -> argparse.ArgumentParser:  # type: ignore[type-arg]
    if parent is not None:
        parser = parent.add_parser("drift", help="Detect environment drift between runs")
    else:
        parser = argparse.ArgumentParser(
            prog="pipewatch-drift",
            description="Detect environment drift between pipeline runs",
        )

    sub = parser.add_subparsers(dest="drift_cmd")

    chk = sub.add_parser("check", help="Compare environments of two runs")
    chk.add_argument("run_a", help="First run ID")
    chk.add_argument("run_b", help="Second run ID")
    chk.add_argument("--keys", default=None, help="Comma-separated keys to check")
    chk.add_argument("--base-dir", default=".pipewatch")
    chk.add_argument("--json", action="store_true", help="Output as JSON")
    chk.add_argument(
        "--fail-on-drift",
        action="store_true",
        help="Exit with code 2 when drift is detected",
    )
    chk.set_defaults(func=cmd_check)

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
