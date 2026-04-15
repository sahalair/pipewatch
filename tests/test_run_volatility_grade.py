"""Boundary tests for _grade_volatility in pipewatch.run_volatility."""

import pytest
from pipewatch.run_volatility import _grade_volatility


@pytest.mark.parametrize("cv,expected", [
    (0.0,   "stable"),
    (0.099, "stable"),
    (0.1,   "low"),
    (0.249, "low"),
    (0.25,  "moderate"),
    (0.499, "moderate"),
    (0.5,   "high"),
    (0.999, "high"),
    (1.0,   "extreme"),
    (2.5,   "extreme"),
    (None,  "unknown"),
])
def test_grade_volatility_parametrized(cv, expected):
    assert _grade_volatility(cv) == expected


def test_grade_volatility_exact_boundary_0_1_is_low():
    assert _grade_volatility(0.1) == "low"


def test_grade_volatility_exact_boundary_0_25_is_moderate():
    assert _grade_volatility(0.25) == "moderate"


def test_grade_volatility_exact_boundary_0_5_is_high():
    assert _grade_volatility(0.5) == "high"


def test_grade_volatility_exact_boundary_1_0_is_extreme():
    assert _grade_volatility(1.0) == "extreme"
