"""Tests for pipewatch.run_feedback."""

import pytest
from pipewatch.run_feedback import (
    add_feedback,
    get_feedback,
    remove_feedback,
    load_feedback,
    average_rating,
    VALID_RATINGS,
)


def test_add_feedback_creates_entry(tmp_path):
    entry = add_feedback(str(tmp_path), "run-1", 4)
    assert entry["rating"] == 4
    assert entry["comment"] is None
    assert entry["author"] is None


def test_add_feedback_with_comment_and_author(tmp_path):
    entry = add_feedback(str(tmp_path), "run-1", 5, comment="Great run!", author="alice")
    assert entry["comment"] == "Great run!"
    assert entry["author"] == "alice"


def test_add_feedback_persists(tmp_path):
    add_feedback(str(tmp_path), "run-1", 3)
    data = load_feedback(str(tmp_path))
    assert "run-1" in data
    assert data["run-1"]["rating"] == 3


def test_add_feedback_overwrites_existing(tmp_path):
    add_feedback(str(tmp_path), "run-1", 2)
    add_feedback(str(tmp_path), "run-1", 5, comment="Updated")
    entry = get_feedback(str(tmp_path), "run-1")
    assert entry["rating"] == 5
    assert entry["comment"] == "Updated"


def test_add_feedback_invalid_rating_raises(tmp_path):
    with pytest.raises(ValueError, match="Rating must be"):
        add_feedback(str(tmp_path), "run-1", 0)


def test_add_feedback_invalid_rating_6_raises(tmp_path):
    with pytest.raises(ValueError):
        add_feedback(str(tmp_path), "run-1", 6)


def test_add_feedback_empty_run_id_raises(tmp_path):
    with pytest.raises(ValueError, match="run_id"):
        add_feedback(str(tmp_path), "", 3)


def test_get_feedback_returns_none_when_missing(tmp_path):
    result = get_feedback(str(tmp_path), "nonexistent")
    assert result is None


def test_remove_feedback_returns_true(tmp_path):
    add_feedback(str(tmp_path), "run-1", 4)
    assert remove_feedback(str(tmp_path), "run-1") is True
    assert get_feedback(str(tmp_path), "run-1") is None


def test_remove_feedback_missing_returns_false(tmp_path):
    assert remove_feedback(str(tmp_path), "ghost") is False


def test_load_feedback_missing_file_returns_empty(tmp_path):
    result = load_feedback(str(tmp_path))
    assert result == {}


def test_average_rating_single(tmp_path):
    add_feedback(str(tmp_path), "run-1", 4)
    assert average_rating(str(tmp_path)) == 4.0


def test_average_rating_multiple(tmp_path):
    add_feedback(str(tmp_path), "run-1", 2)
    add_feedback(str(tmp_path), "run-2", 4)
    assert average_rating(str(tmp_path)) == 3.0


def test_average_rating_empty_returns_none(tmp_path):
    assert average_rating(str(tmp_path)) is None


def test_average_rating_filtered_by_runs(tmp_path):
    add_feedback(str(tmp_path), "run-1", 5)
    add_feedback(str(tmp_path), "run-2", 1)
    avg = average_rating(str(tmp_path), runs=["run-1"])
    assert avg == 5.0


def test_valid_ratings_set():
    assert VALID_RATINGS == {1, 2, 3, 4, 5}
