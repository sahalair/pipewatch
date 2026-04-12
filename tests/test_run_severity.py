"""Tests for pipewatch.run_severity."""

import pytest
from pipewatch.run_severity import (
    classify_severity,
    severity_rank,
    highest_severity,
    format_severity_badge,
    summarize_severities,
    format_severity_summary,
    SEVERITY_LEVELS,
)


def _run(status="ok", exit_code=0, alerts=None):
    r = {"run_id": "r1", "status": status, "exit_code": exit_code}
    if alerts is not None:
        r["alerts"] = alerts
    return r


def test_classify_severity_ok():
    assert classify_severity(_run("ok")) == "ok"


def test_classify_severity_failed_exit_1():
    assert classify_severity(_run("failed", exit_code=1)) == "high"


def test_classify_severity_failed_exit_2_is_critical():
    assert classify_severity(_run("failed", exit_code=2)) == "critical"


def test_classify_severity_unknown_is_medium():
    assert classify_severity(_run("unknown")) == "medium"


def test_classify_severity_with_alerts_is_medium():
    assert classify_severity(_run("ok", alerts=["some_alert"])) == "medium"


def test_severity_rank_order():
    assert severity_rank("ok") < severity_rank("low")
    assert severity_rank("low") < severity_rank("medium")
    assert severity_rank("medium") < severity_rank("high")
    assert severity_rank("high") < severity_rank("critical")


def test_severity_rank_unknown_returns_zero():
    assert severity_rank("unknown_level") == 0


def test_highest_severity_picks_worst():
    assert highest_severity(["ok", "critical", "medium"]) == "critical"


def test_highest_severity_empty_returns_ok():
    assert highest_severity([]) == "ok"


def test_format_severity_badge_all_levels():
    for level in SEVERITY_LEVELS:
        badge = format_severity_badge(level)
        assert isinstance(badge, str)
        assert len(badge) > 0


def test_format_severity_badge_unknown():
    assert format_severity_badge("nope") == "[?]"


def test_summarize_severities_counts():
    runs = [
        _run("ok"),
        _run("ok"),
        _run("failed", exit_code=1),
        _run("failed", exit_code=3),
    ]
    counts = summarize_severities(runs)
    assert counts["ok"] == 2
    assert counts["high"] == 1
    assert counts["critical"] == 1


def test_summarize_severities_empty():
    counts = summarize_severities([])
    assert all(v == 0 for v in counts.values())


def test_format_severity_summary_contains_levels():
    counts = {level: 0 for level in SEVERITY_LEVELS}
    counts["critical"] = 3
    output = format_severity_summary(counts)
    assert "critical" in output.lower() or "CRIT" in output
    assert "3" in output
