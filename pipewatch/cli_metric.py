"""CLI commands for metric tracking in pipewatch."""

from __future__ import annotations

import argparse
import sys

from pipewatch.metric import compare_metrics, load_metrics, record_metric, save_metrics


def cmd_record(args: argparse.Namespace) -> None:
    metrics = load_metrics(args.run_id)
    entry = record_metric(args.run_id, args.name, args.value, args.unit or "")
    metrics.append(entry)
    save_metrics(args.run_id, metrics)
    unit_str = f" {args.unit}" if args.unit else ""
    print(f"Recorded metric '{args.name}' = {args.value}{unit_str} for run {args.run_id}")


def cmd_list(args: argparse.Namespace) -> None:
    metrics = load_metrics(args.run_id)
    if not metrics:
        print(f"No metrics found for run {args.run_id}")
        return
    print(f"Metrics for run {args.run_id}:")
    for m in metrics:
        unit_str = f" {m['unit']}" if m.get("unit") else ""
        print(f"  {m['name']}: {m['value']}{unit_str}")


def cmd_compare(args: argparse.Namespace) -> None:
    comparisons = compare_metrics(args.run_a, args.run_b)
    if not comparisons:
        print("No metrics found for either run.")
        return
    print(f"Metric comparison: {args.run_a} -> {args.run_b}")
    print(f"  {'Name':<25} {'Run A':>12} {'Run B':>12} {'Delta':>12}  Unit")
    print("  " + "-" * 65)
    for c in comparisons:
        run_a = f"{c['run_a']}" if c["run_a"] is not None else "N/A"
        run_b = f"{c['run_b']}" if c["run_b"] is not None else "N/A"
        delta = f"{c['delta']:+.4g}" if c["delta"] is not None else "N/A"
        print(f"  {c['name']:<25} {run_a:>12} {run_b:>12} {delta:>12}  {c['unit']}")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="pipewatch metric", description="Metric tracking commands")
    sub = parser.add_subparsers(dest="command", required=True)

    p_record = sub.add_parser("record", help="Record a metric for a run")
    p_record.add_argument("run_id", help="Run ID")
    p_record.add_argument("name", help="Metric name")
    p_record.add_argument("value", type=float, help="Metric value")
    p_record.add_argument("--unit", default="", help="Optional unit label")
    p_record.set_defaults(func=cmd_record)

    p_list = sub.add_parser("list", help="List metrics for a run")
    p_list.add_argument("run_id", help="Run ID")
    p_list.set_defaults(func=cmd_list)

    p_cmp = sub.add_parser("compare", help="Compare metrics between two runs")
    p_cmp.add_argument("run_a", help="First run ID")
    p_cmp.add_argument("run_b", help="Second run ID")
    p_cmp.set_defaults(func=cmd_compare)

    return parser


def main(argv: list[str] | None = None) -> None:
    parser = build_parser()
    args = parser.parse_args(argv)
    args.func(args)


if __name__ == "__main__":
    main()
