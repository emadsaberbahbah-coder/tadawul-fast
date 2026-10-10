"""Calibration leaves retain the tracker's public imports and mathematics."""
from __future__ import annotations

import ast
from pathlib import Path

import pytest

from scripts import performance_math
from scripts import track_performance as tracker


def test_tracker_reexports_the_extracted_leaves_and_quantile():
    assert tracker.wilson_interval is performance_math.wilson_interval
    assert tracker.shrink_factor is performance_math.shrink_factor
    assert tracker._Z95 == performance_math._Z95


@pytest.mark.parametrize("wins,decided,expected", (
    (0, 0, (0.0, 0.0)), (-9, 10, (0.0, 27.8)),
    (10, 10, (72.2, 100.0)), (5, 10, (23.7, 76.3)),
    (99, 10, (72.2, 100.0)), (0, -1, (0.0, 0.0)),
))
def test_wilson_known_boundary_and_small_sample_intervals(wins, decided, expected):
    assert performance_math.wilson_interval(wins, decided) == expected


@pytest.mark.parametrize("raw,decided,k,expected", (
    (4, 0, 30, 1), (0.5, -10, 30, 1), (2, 30, 30, 1.5),
    (0.5, 30, 30, 0.75), (2, 1, 0, 1.5),
))
def test_shrinkage_respects_no_data_and_prior_weight(raw, decided, k, expected):
    assert performance_math.shrink_factor(raw, decided, k) == expected


def test_extracted_module_has_no_network_environment_or_mutation_imports():
    tree = ast.parse(Path(performance_math.__file__).read_text(encoding="utf-8"))
    imported = {node.module for node in ast.walk(tree) if isinstance(node, ast.ImportFrom)}
    imported |= {alias.name for node in ast.walk(tree) if isinstance(node, ast.Import) for alias in node.names}
    assert imported <= {"__future__", "math", "typing"}
