"""Tests for pipewatch.cli_checkpoints."""

import pytest
from unittest.mock import patch, MagicMock

from pipewatch.cli_checkpoints import cmd_add, cmd_list, cmd_summary, cmd_clear, build_parser

RUN_ID = "run-xyz-999"


def make_args(**kwargs):
    class Args:
        pass
    a = Args()
    defaults = {
        "run_id": RUN_ID,
        "name": "step1",
        "status": "ok",
        "message": None,
        "data": None,
    }
    defaults.update(kwargs)
    for k, v in defaults.items():
        setattr(a, k, v)
    return a


@pytest.fixture(autouse=True)
def _tmp(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)


def test_cmd_add_records_checkpoint(capsys):
    cmd_add(make_args())
    out = capsys.readouterr().out
    assert "step1" in out
    assert "ok" in out.lower()


def test_cmd_add_with_status_warn(capsys):
    cmd_add(make_args(status="warn", message="slow"))
    out = capsys.readouterr().out
    assert "warn" in out.lower()


def test_cmd_add_with_valid_json_data(capsys):
    cmd_add(make_args(data='{"rows": 10}'))
    out = capsys.readouterr().out
    assert "step1" in out


def test_cmd_add_with_invalid_json_exits(capsys):
    import sys
    with pytest.raises(SystemExit):
        cmd_add(make_args(data="not-json"))


def test_cmd_list_empty(capsys):
    args = make_args()
    args.name = None
    cmd_list(args)
    out = capsys.readouterr().out
    assert "No checkpoints" in out


def test_cmd_list_shows_entries(capsys):
    from pipewatch.run_checkpoints import record_checkpoint
    record_checkpoint(RUN_ID, "ingest", status="ok")
    args = make_args()
    args.name = None
    cmd_list(args)
    out = capsys.readouterr().out
    assert "ingest" in out


def test_cmd_summary_prints_counts(capsys):
    from pipewatch.run_checkpoints import record_checkpoint
    record_checkpoint(RUN_ID, "a", status="ok")
    record_checkpoint(RUN_ID, "b", status="fail")
    cmd_summary(make_args())
    out = capsys.readouterr().out
    assert "Total" in out
    assert "2" in out


def test_cmd_clear_removes_checkpoints(capsys):
    from pipewatch.run_checkpoints import record_checkpoint, load_checkpoints
    record_checkpoint(RUN_ID, "step")
    cmd_clear(make_args())
    assert load_checkpoints(RUN_ID) == []
    out = capsys.readouterr().out
    assert "Cleared" in out


def test_build_parser_returns_parser():
    parser = build_parser()
    assert parser is not None
