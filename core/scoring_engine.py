#!/usr/bin/env python3
# core/scoring_engine.py
"""
================================================================================
Scoring Engine Compatibility Bridge — v2.2.0
(COMPATIBILITY / CONTRACT-HARDENED / STARTUP-SAFE)
================================================================================

Purpose
- Preserve the legacy `core.scoring_engine` import surface expected by tests,
  scripts, and older callers.
- Delegate actual scoring logic to `core.scoring`.
- Keep startup behavior safe: no network I/O, no heavy imports, no side effects.

This module intentionally exposes:
- compute_scores(...)
- enrich_with_scores(...)
- score_row(...)
- score_quote(...)
- rank_rows_by_overall(...)
- assign_rank_overall(...)
- score_and_rank_rows(...)
- AssetScores
- ScoringEngine
- ScoringWeights
- ForecastParameters
- SCORING_ENGINE_VERSION
================================================================================
"""

from __future__ import annotations

from dataclasses import asdict, is_dataclass
from typing import Any, Dict, Mapping, Optional, Sequence

from core.scoring import (
    __version__ as _SCORING_VERSION,
    AssetScores,
    ForecastParameters,
    ScoringWeights,
    ScoreWeights,
    assign_rank_overall,
    compute_scores as _compute_scores,
    enrich_with_scores as _enrich_with_scores,
    rank_rows_by_overall,
    score_and_rank_rows,
    score_quote,
    score_row,
)

SCORING_ENGINE_VERSION = _SCORING_VERSION
VERSION = SCORING_ENGINE_VERSION


# =============================================================================
# Helpers
# =============================================================================
def _as_dict(obj: Any) -> Dict[str, Any]:
    if obj is None:
        return {}
    if isinstance(obj, dict):
        return dict(obj)
    if isinstance(obj, Mapping):
        return dict(obj)
    if is_dataclass(obj):
        try:
            return asdict(obj)
        except Exception:
            return {}
    try:
        model_dump = getattr(obj, "model_dump", None)
        if callable(model_dump):
            dumped = model_dump(mode="python")
            if isinstance(dumped, Mapping):
                return dict(dumped)
    except Exception:
        pass
    try:
        dict_fn = getattr(obj, "dict", None)
        if callable(dict_fn):
            dumped = dict_fn()
            if isinstance(dumped, Mapping):
                return dict(dumped)
    except Exception:
        pass
    try:
        raw = getattr(obj, "__dict__", None)
        if isinstance(raw, Mapping):
            return dict(raw)
    except Exception:
        pass
    return {}


# =============================================================================
# Public functions
# =============================================================================
def compute_scores(row: Dict[str, Any], *, settings: Any = None) -> Dict[str, Any]:
    """
    Legacy scoring_engine entrypoint.
    Delegates to core.scoring.compute_scores().
    """
    return _as_dict(_compute_scores(row or {}, settings=settings))


def enrich_with_scores(
    row: Dict[str, Any],
    *,
    settings: Any = None,
    in_place: bool = False,
) -> Dict[str, Any]:
    """
    Legacy compatibility helper expected by contract tests.
    Returns a dict and optionally mutates the input row.
    """
    result = _enrich_with_scores(row or {}, settings=settings, in_place=in_place)
    return _as_dict(result)


# =============================================================================
# Engine-style wrapper
# =============================================================================
class ScoringEngine:
    """
    Small compatibility wrapper for older class-based callers.
    """

    def __init__(
        self,
        *,
        settings: Any = None,
        weights: Optional[ScoreWeights] = None,
        forecast_parameters: Optional[ForecastParameters] = None,
    ) -> None:
        self.settings = settings
        self.weights = weights
        self.forecast_parameters = forecast_parameters

    def compute_scores(self, row: Dict[str, Any]) -> Dict[str, Any]:
        return compute_scores(row, settings=self.settings)

    def enrich_with_scores(self, row: Dict[str, Any], *, in_place: bool = False) -> Dict[str, Any]:
        return enrich_with_scores(row, settings=self.settings, in_place=in_place)

    def score_row(self, row: Dict[str, Any]) -> Dict[str, Any]:
        return _as_dict(score_row(row or {}, settings=self.settings))

    def score_quote(self, row: Dict[str, Any]) -> Dict[str, Any]:
        return _as_dict(score_quote(row or {}, settings=self.settings))

    def rank_rows_by_overall(self, rows: Sequence[Dict[str, Any]]) -> list[Dict[str, Any]]:
        return rank_rows_by_overall(list(rows or []))

    def assign_rank_overall(
        self,
        rows: Sequence[Dict[str, Any]],
        *,
        key_overall: str = "overall_score",
        inplace: bool = True,
        rank_key: str = "rank_overall",
    ) -> list[Dict[str, Any]]:
        return assign_rank_overall(
            list(rows or []),
            key_overall=key_overall,
            inplace=inplace,
            rank_key=rank_key,
        )

    def score_and_rank_rows(
        self,
        rows: Sequence[Dict[str, Any]],
        *,
        key_overall: str = "overall_score",
        inplace: bool = False,
    ) -> list[Dict[str, Any]]:
        return score_and_rank_rows(
            list(rows or []),
            settings=self.settings,
            key_overall=key_overall,
            inplace=inplace,
        )


__all__ = [
    "compute_scores",
    "enrich_with_scores",
    "score_row",
    "score_quote",
    "rank_rows_by_overall",
    "assign_rank_overall",
    "score_and_rank_rows",
    "AssetScores",
    "ScoringEngine",
    "ScoringWeights",
    "ForecastParameters",
    "ScoreWeights",
    "SCORING_ENGINE_VERSION",
    "VERSION",
]
