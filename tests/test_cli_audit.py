"""Tests for pipewatch.cli_audit."""
import json
import pytest
from pipewatch.cli_audit import cmd_add, cmd_list, build_parser
from pipewatch.run_audit import load_audit_log


class Args:
    def __init__(self, **kwargs):
        for k, v in kwargs.items():
            setattr(self, k, v)


def make_args(**kwargs):
    defaults = {
        "base_dir": None,
        "run_id": "run-1",
        "action": "created",
        "actor": None,
        "details": None,
        "json": False,
    }
    defaults.update(kwargs)
    return Args(**defaults)


@pytest.fixture
def base(tmp_path):
    return str(tmp_path)


def test_cmd_add_records_event(base, capsys):
    args = make_args(base_dir=base, run_id="r1", action="created")
    cmd_add(args)
    log = load_audit_log(base)
    assert len(log) == 1
    assert log[0]["run_id"] == "r1"


def test_cmd_add_prints_confirmation(base, capsys):
    args = make_args(base_dir=base, run_id="r1", action="started")
    cmd_add(args)
    out = capsys.readouterr().out
    assert "started" in out
    assert "r1" in out


def test_cmd_add_with_actor_and_details(base, capsys):
    args = make_args(base_dir=base, run_id="r2", action="tagged", actor="alice", details='{"tag": "v1"}')
    cmd_add(args)
    log = load_audit_log(base)
    assert log[0]["actor"] == "alice"
    assert log[0]["details"] == {"tag": "v1"}


def test_cmd_add_invalid_details_json_exits(base, capsys):
    args = make_args(base_dir=base, run_id="r3", action="noted", details="not-json")
    with pytest.raises(SystemExit):
        cmd_add(args)


def test_cmd_list_empty(base, capsys):
    args = make_args(base_dir=base, run_id=None, action=None)
    cmd_list(args)
    out = capsys.readouterr().out
    assert "No audit events" in out


def test_cmd_list_json_output(base, capsys):
    from pipewatch.run_audit import record_audit_event
    record_audit_event(base, "r1", "created")
    args = make_args(base_dir=base, run_id=None, action=None, json=True)
    cmd_list(args)
    out = capsys.readouterr().out
    data = json.loads(out)
    assert isinstance(data, list)
    assert data[0]["run_id"] == "r1"


def test_build_parser_returns_parser():
    parser = build_parser()
    assert parser is not None


def test_build_parser_add_subcommand(base):
    parser = build_parser()
    args = parser.parse_args(["--base-dir", base, "add", "r1", "created"])
    assert args.run_id == "r1"
    assert args.action == "created"
