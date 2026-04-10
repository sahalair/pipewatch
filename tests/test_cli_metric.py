"""Tests for pipewatch.cli_metric commands."""

from __future__ import annotations

import pytest

import pipewatch.metric as metric_mod
from pipewatch.cli_metric import build_parser, cmd_compare, cmd_list, cmd_record


def make_args(**kwargs):
    class Args:
        pass
    a = Args()
    for k, v in kwargs.items():
        setattr(a, k, v)
    return a


def test_cmd_record_creates_metric(tmp_path, monkeypatch, capsys):
    monkeypatch.setattr(metric_mod, "_METRICS_DIR", str(tmp_path / "metrics"))
    args = make_args(run_id="run-1", name="rows", value=99.0, unit="")
    cmd_record(args)
    out = capsys.readouterr().out
    assert "rows" in out
    assert "99.0" in out
    loaded = metric_mod.load_metrics("run-1")
    assert len(loaded) == 1
    assert loaded[0]["value"] == 99.0


def test_cmd_record_with_unit(tmp_path, monkeypatch, capsys):
    monkeypatch.setattr(metric_mod, "_METRICS_DIR", str(tmp_path / "metrics"))
    args = make_args(run_id="run-2", name="duration", value=1.5, unit="seconds")
    cmd_record(args)
    out = capsys.readouterr().out
    assert "seconds" in out


def test_cmd_list_empty(tmp_path, monkeypatch, capsys):
    monkeypatch.setattr(metric_mod, "_METRICS_DIR", str(tmp_path / "metrics"))
    args = make_args(run_id="ghost-run")
    cmd_list(args)
    out = capsys.readouterr().out
    assert "No metrics" in out


def test_cmd_list_shows_metrics(tmp_path, monkeypatch, capsys):
    monkeypatch.setattr(metric_mod, "_METRICS_DIR", str(tmp_path / "metrics"))
    metric_mod.save_metrics("run-x", [metric_mod.record_metric("run-x", "count", 7.0, "items")])
    args = make_args(run_id="run-x")
    cmd_list(args)
    out = capsys.readouterr().out
    assert "count" in out
    assert "7.0" in out


def test_cmd_compare_output(tmp_path, monkeypatch, capsys):
    monkeypatch.setattr(metric_mod, "_METRICS_DIR", str(tmp_path / "metrics"))
    metric_mod.save_metrics("r1", [metric_mod.record_metric("r1", "rows", 100.0)])
    metric_mod.save_metrics("r2", [metric_mod.record_metric("r2", "rows", 200.0)])
    args = make_args(run_a="r1", run_b="r2")
    cmd_compare(args)
    out = capsys.readouterr().out
    assert "rows" in out
    assert "+100" in out or "+1e+02" in out or "100" in out


def test_cmd_compare_no_metrics(tmp_path, monkeypatch, capsys):
    monkeypatch.setattr(metric_mod, "_METRICS_DIR", str(tmp_path / "metrics"))
    args = make_args(run_a="empty-a", run_b="empty-b")
    cmd_compare(args)
    out = capsys.readouterr().out
    assert "No metrics" in out


def test_build_parser_record():
    parser = build_parser()
    args = parser.parse_args(["record", "run-99", "latency", "0.5", "--unit", "ms"])
    assert args.run_id == "run-99"
    assert args.name == "latency"
    assert args.value == 0.5
    assert args.unit == "ms"
