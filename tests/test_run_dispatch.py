"""Tests for pipewatch/run_dispatch.py"""

import pytest
from pipewatch.run_dispatch import (
    clear_handlers,
    dispatch,
    list_handlers,
    register_handler,
    unregister_handler,
)


@pytest.fixture(autouse=True)
def _reset():
    clear_handlers()
    yield
    clear_handlers()


def _make_run(pipeline="etl", status="success"):
    return {"run_id": "r1", "pipeline": pipeline, "status": status}


def test_register_and_list_handlers():
    def my_handler(run): pass
    register_handler(my_handler, pipeline="etl", status="success")
    handlers = list_handlers()
    assert any(h["handler"] == "my_handler" for h in handlers)


def test_register_same_handler_twice_is_idempotent():
    def my_handler(run): pass
    register_handler(my_handler, pipeline="etl", status="success")
    register_handler(my_handler, pipeline="etl", status="success")
    handlers = list_handlers(pipeline="etl", status="success")
    assert len([h for h in handlers if h["handler"] == "my_handler"]) == 1


def test_dispatch_calls_exact_match():
    called = []
    def my_handler(run): called.append(run["run_id"])
    register_handler(my_handler, pipeline="etl", status="success")
    dispatch(_make_run(pipeline="etl", status="success"))
    assert called == ["r1"]


def test_dispatch_does_not_call_wrong_status():
    called = []
    def my_handler(run): called.append(run["run_id"])
    register_handler(my_handler, pipeline="etl", status="failed")
    dispatch(_make_run(pipeline="etl", status="success"))
    assert called == []


def test_dispatch_wildcard_pipeline():
    called = []
    def wildcard_handler(run): called.append(run["pipeline"])
    register_handler(wildcard_handler, pipeline="*", status="failed")
    dispatch(_make_run(pipeline="ingest", status="failed"))
    dispatch(_make_run(pipeline="transform", status="failed"))
    assert called == ["ingest", "transform"]


def test_dispatch_wildcard_status():
    called = []
    def all_status_handler(run): called.append(run["status"])
    register_handler(all_status_handler, pipeline="etl", status="*")
    dispatch(_make_run(pipeline="etl", status="success"))
    dispatch(_make_run(pipeline="etl", status="failed"))
    assert "success" in called and "failed" in called


def test_dispatch_handler_called_only_once_even_if_multi_match():
    called = []
    def my_handler(run): called.append(1)
    register_handler(my_handler, pipeline="etl", status="success")
    register_handler(my_handler, pipeline="*", status="*")
    dispatch(_make_run(pipeline="etl", status="success"))
    assert len(called) == 1


def test_dispatch_returns_handler_names():
    def alpha(run): pass
    def beta(run): pass
    register_handler(alpha, pipeline="etl", status="success")
    register_handler(beta, pipeline="*", status="*")
    names = dispatch(_make_run(pipeline="etl", status="success"))
    assert "alpha" in names
    assert "beta" in names


def test_unregister_handler_removes_it():
    def my_handler(run): pass
    register_handler(my_handler, pipeline="etl", status="success")
    result = unregister_handler(my_handler, pipeline="etl", status="success")
    assert result is True
    called = []
    my_handler2 = lambda run: called.append(1)
    dispatch(_make_run())
    assert called == []


def test_unregister_nonexistent_returns_false():
    def ghost(run): pass
    result = unregister_handler(ghost, pipeline="etl", status="success")
    assert result is False


def test_list_handlers_filtered_by_pipeline():
    def h1(run): pass
    def h2(run): pass
    register_handler(h1, pipeline="etl", status="success")
    register_handler(h2, pipeline="other", status="success")
    handlers = list_handlers(pipeline="etl")
    names = [h["handler"] for h in handlers]
    assert "h1" in names
    assert "h2" not in names
