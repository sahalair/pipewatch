"""Tests for pipewatch.run_hooks."""

import pytest

from pipewatch.run_hooks import (
    fire_hook,
    list_hooks,
    register_hook,
    unregister_hooks,
)


@pytest.fixture(autouse=True)
def _clear_hooks():
    """Ensure hooks are clean before and after every test."""
    unregister_hooks()
    yield
    unregister_hooks()


_SAMPLE_RUN = {"run_id": "abc123", "pipeline": "etl", "status": "ok"}


def test_register_and_list_hooks():
    called = []

    def my_hook(run):
        called.append(run["run_id"])

    register_hook("on_run_start", my_hook)
    hooks = list_hooks()
    assert "my_hook" in hooks["on_run_start"]


def test_register_unknown_event_raises():
    with pytest.raises(ValueError, match="Unknown event"):
        register_hook("on_unknown", lambda r: None)


def test_fire_hook_calls_callback():
    results = []

    def capture(run):
        results.append(run["run_id"])

    register_hook("on_run_finish", capture)
    errors = fire_hook("on_run_finish", _SAMPLE_RUN)
    assert errors == []
    assert results == ["abc123"]


def test_fire_hook_multiple_callbacks():
    log = []
    register_hook("on_run_finish", lambda r: log.append("a"))
    register_hook("on_run_finish", lambda r: log.append("b"))
    fire_hook("on_run_finish", _SAMPLE_RUN)
    assert log == ["a", "b"]


def test_fire_hook_captures_errors():
    def bad_hook(run):
        raise RuntimeError("boom")

    register_hook("on_run_fail", bad_hook)
    errors = fire_hook("on_run_fail", _SAMPLE_RUN)
    assert len(errors) == 1
    assert "bad_hook" in errors[0]
    assert "boom" in errors[0]


def test_fire_hook_continues_after_error():
    log = []

    def bad(run):
        raise ValueError("oops")

    def good(run):
        log.append("good")

    register_hook("on_run_fail", bad)
    register_hook("on_run_fail", good)
    errors = fire_hook("on_run_fail", _SAMPLE_RUN)
    assert log == ["good"]
    assert len(errors) == 1


def test_fire_unknown_event_raises():
    with pytest.raises(ValueError, match="Unknown event"):
        fire_hook("on_mystery", _SAMPLE_RUN)


def test_unregister_specific_event():
    register_hook("on_run_start", lambda r: None)
    register_hook("on_run_finish", lambda r: None)
    unregister_hooks("on_run_start")
    hooks = list_hooks()
    assert hooks["on_run_start"] == []
    assert len(hooks["on_run_finish"]) == 1


def test_unregister_all_events():
    register_hook("on_run_start", lambda r: None)
    register_hook("on_run_finish", lambda r: None)
    unregister_hooks()
    hooks = list_hooks()
    assert all(v == [] for v in hooks.values())


def test_unregister_unknown_event_raises():
    with pytest.raises(ValueError, match="Unknown event"):
        unregister_hooks("on_ghost")
