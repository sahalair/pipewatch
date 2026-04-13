"""Boundary tests for _grade_saturation in pipewatch.run_saturation."""

import pytest
from pipewatch.run_saturation import _grade_saturation


@pytest.mark.parametrize("value,expected", [
    (0.0, "LOW"),
    (0.5, "LOW"),
    (0.51, "MODERATE"),
    (0.75, "MODERATE"),
    (0.76, "HIGH"),
    (0.9, "HIGH"),
    (0.91, "NEAR_LIMIT"),
    (1.0, "NEAR_LIMIT"),
    (1.001, "OVER_CAPACITY"),
    (2.0, "OVER_CAPACITY"),
    (None, "N/A"),
])
def test_grade_saturation_parametrized(value, expected):
    assert _grade_saturation(value) == expected


def test_grade_saturation_exact_50_percent_is_low():
    assert _grade_saturation(0.50) == "LOW"


def test_grade_saturation_exact_75_percent_is_moderate():
    assert _grade_saturation(0.75) == "MODERATE"


def test_grade_saturation_exact_90_percent_is_high():
    assert _grade_saturation(0.90) == "HIGH"


def test_grade_saturation_exact_100_percent_is_near_limit():
    assert _grade_saturation(1.0) == "NEAR_LIMIT"


def test_grade_saturation_zero_is_low():
    assert _grade_saturation(0.0) == "LOW"
