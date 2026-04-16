"""Parametrized boundary tests for _grade_longevity."""
import pytest
from pipewatch.run_longevity import _grade_longevity


@pytest.mark.parametrize("ratio,age,expected", [
    (1.0, 3, "New"),
    (0.0, 6, "New"),
    (0.8, 7, "A"),
    (0.9, 30, "A"),
    (0.6, 10, "B"),
    (0.79, 10, "B"),
    (0.4, 10, "C"),
    (0.59, 10, "C"),
    (0.2, 10, "D"),
    (0.39, 10, "D"),
    (0.0, 10, "F"),
    (0.19, 10, "F"),
])
def test_grade_longevity_parametrized(ratio, age, expected):
    assert _grade_longevity(ratio, age) == expected


def test_grade_longevity_exact_boundary_08_is_a():
    assert _grade_longevity(0.8, 100) == "A"


def test_grade_longevity_exact_boundary_06_is_b():
    assert _grade_longevity(0.6, 100) == "B"


def test_grade_longevity_exact_boundary_04_is_c():
    assert _grade_longevity(0.4, 100) == "C"


def test_grade_longevity_exact_boundary_02_is_d():
    assert _grade_longevity(0.2, 100) == "D"
