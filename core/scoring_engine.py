#!/usr/bin/env python3
# core/scoring_engine.py
"""
================================================================================
Scoring Engine Compatibility Bridge — v2.3.2
(COMPATIBILITY / CONTRACT-HARDENED / STARTUP-SAFE / LABEL-ALIGNED)
================================================================================

Purpose
- Preserve the legacy `core.scoring_engine` import surface expected by tests,
  scripts, and older callers.
- Delegate actual scoring logic to `core.scoring`.
- Re-export recommendation normalization helpers introduced in newer
  `core.scoring` revisions so older callers stay aligned.
- Keep startup behavior safe: no network I/O, no heavy side effects.

v2.3.2 vs v2.3.1
- FIX: _NORMALIZE_TO_CANONICAL fallback: "AVOID" -> "SELL" (was "REDUCE").
  scoring.py v2.4.0 maps AVOID -> SELL in its _RECOMMENDATION_CODE_ALIASES.
  The bridge fallback (used when core.scoring < v2.3.0) was inconsistent.
  Both tables now agree: AVOID = exit / unfavorable = SELL.

v2.3.1 vs v2.3.0
- FIX: Hard imports of RECOMMENDATION_LABEL_MAP and normalize_recommendation_label
  wrapped in try/except with local fallback definitions.
  v2.3.0 imported them directly — if core.scoring is still pre-v2.3.0, the
  entire module would fail to import with ImportError.
- FIX: Fallback RECOMMENDATION_LABEL_MAP aligned with canonical 5-value vocab.
- FIX: normalize_recommendation_label fallback normalizes STRONG_BUY variants
  and maps ACCUMULATE -> BUY consistently.

v2.3.0 vs v2.2.0 (project file baseline)
- Added RECOMMENDATION_LABEL_MAP re-export.
- Added normalize_recommendation_label re-export.
- Added normalize_recommendation_code with canonical 5-value enum fallback.
- Added CANONICAL_RECOMMENDATION_CODES tuple.
- ScoringEngine exposes normalize_recommendation_label and
  normalize_recommendation_code as instance methods.

Exports:
- compute_scores, enrich_with_scores, score_row, score_quote
- rank_rows_by_overall, assign_rank_overall, score_and_rank_rows
- normalize_recommendation_label, normalize_recommendation_code
- RECOMMENDATION_LABEL_MAP, CANONICAL_RECOMMENDATION_CODES
- AssetScores, ScoringEngine, ScoringWeights, ForecastParameters, ScoreWeights
- SCORING_ENGINE_VERSION
================================================================================
"""

from __future__ import annotations

from dataclasses import asdict, is_dataclass
from typing import Any, Dict, Mapping, Optional, Sequence

# ---------------------------------------------------------------------------
# Stable imports — always present in core.scoring since v2.0+
# ---------------------------------------------------------------------------
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

# ---------------------------------------------------------------------------
# New symbols added in core.scoring v2.3.0.
# Wrapped in try/except: this module imports safely even when the production
# core.scoring is still pre-v2.3.0.
# ---------------------------------------------------------------------------

# Canonical 5-value recommendation vocabulary
CANONICAL_RECOMMENDATION_CODES = (
    "STRONG_BUY",
    "BUY",
    "HOLD",
    "REDUCE",
    "SELL",
)

_FALLBACK_RECOMMENDATION_LABEL_MAP: Dict[str, str] = {
    "STRONG_BUY": "Strong Buy",
    "BUY":        "Buy",
    "HOLD":       "Hold",
    "REDUCE":     "Reduce",
    "SELL":       "Sell",
}

# FIX v2.3.2: AVOID -> "SELL" (was "REDUCE").
# Aligned with scoring.py v2.4.0 _RECOMMENDATION_CODE_ALIASES where AVOID -> "SELL".
# "AVOID" means "do not hold / exit" — that maps to an exit recommendation.
_NORMALIZE_TO_CANONICAL: Dict[str, str] = {
    "ACCUMULATE":     "BUY",
    "ADD":            "BUY",
    "OUTPERFORM":     "BUY",
    "OVERWEIGHT":     "BUY",
    "STRONG BUY":     "STRONG_BUY",
    "STRONGBUY":      "STRONG_BUY",
    "STRONG_BUY":     "STRONG_BUY",
    "BUY":            "BUY",
    "HOLD":           "HOLD",
    "NEUTRAL":        "HOLD",
    "MARKET PERFORM": "HOLD",
    "REDUCE":         "REDUCE",
    "UNDERPERFORM":   "REDUCE",
    "UNDERWEIGHT":    "REDUCE",
    "TRIM":           "REDUCE",
    "AVOID":          "SELL",   # FIX v2.3.2: was "REDUCE"
    "SELL":           "SELL",
    "STRONG SELL":    "SELL",
    "STRONG_SELL":    "SELL",
    "EXIT":           "SELL",
}


def _fallback_normalize_recommendation_label(label: Any) -> str:
    """
    Fallback implementation of normalize_recommendation_label.
    Used when core.scoring < v2.3.0.
    Returns canonical code (STRONG_BUY / BUY / HOLD / REDUCE / SELL).
    NOTE: Both normalize_recommendation_label and normalize_recommendation_code
    return the canonical CODE string; the function name is kept for compatibility.
    """
    raw = str(label or "").strip().upper()
    raw = raw.replace("-", " ").replace("_", " ")
    while "  " in raw:
        raw = raw.replace("  ", " ")
    raw = raw.strip()
    if raw in _NORMALIZE_TO_CANONICAL:
        return _NORMALIZE_TO_CANONICAL[raw]
    raw_underscore = raw.replace(" ", "_")
    if raw_underscore in _NORMALIZE_TO_CANONICAL:
        return _NORMALIZE_TO_CANONICAL[raw_underscore]
    return "HOLD"


def _fallback_normalize_recommendation_code(label: Any) -> str:
    """
    Fallback implementation of normalize_recommendation_code.
    Returns canonical code — identical to _fallback_normalize_recommendation_label.
    """
    return _fallback_normalize_recommendation_label(label)


try:
    from core.scoring import RECOMMENDATION_LABEL_MAP
except ImportError:
    RECOMMENDATION_LABEL_MAP = _FALLBACK_RECOMMENDATION_LABEL_MAP  # type: ignore[assignment]

try:
    from core.scoring import normalize_recommendation_label
except ImportError:
    normalize_recommendation_label = _fallback_normalize_recommendation_label  # type: ignore[assignment]

try:
    from core.scoring import normalize_recommendation_code
except ImportError:
    normalize_recommendation_code = _fallback_normalize_recommendation_code  # type: ignore[assignment]

try:
    from core.scoring import CANONICAL_RECOMMENDATION_CODES as _CORE_CODES
    CANONICAL_RECOMMENDATION_CODES = _CORE_CODES
except ImportError:
    pass  # keep the local tuple defined above

# ---------------------------------------------------------------------------
# Version
# ---------------------------------------------------------------------------
SCORING_ENGINE_VERSION = "2.3.2"
VERSION = SCORING_ENGINE_VERSION
__version__ = SCORING_ENGINE_VERSION

_SCORING_MODULE_VERSION = _SCORING_VERSION


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
    """Legacy scoring_engine entrypoint. Delegates to core.scoring.compute_scores()."""
    return _as_dict(_compute_scores(row or {}, settings=settings))


def enrich_with_scores(
    row: Dict[str, Any],
    *,
    settings: Any = None,
    in_place: bool = False,
) -> Dict[str, Any]:
    """Legacy compatibility helper. Returns a dict and optionally mutates the input row."""
    result = _enrich_with_scores(row or {}, settings=settings, in_place=in_place)
    return _as_dict(result)


# =============================================================================
# Engine-style wrapper
# =============================================================================
class ScoringEngine:
    """
    Compatibility wrapper for class-based callers.
    All methods delegate to core.scoring functions.
    """

    version = SCORING_ENGINE_VERSION

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

    def rank_rows_by_overall(self, rows: Sequence[Dict[str, Any]]) -> list:
        return rank_rows_by_overall(list(rows or []))

    def assign_rank_overall(
        self,
        rows: Sequence[Dict[str, Any]],
        *,
        key_overall: str = "overall_score",
        inplace: bool = True,
        rank_key: str = "rank_overall",
    ) -> list:
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
    ) -> list:
        return score_and_rank_rows(
            list(rows or []),
            settings=self.settings,
            key_overall=key_overall,
            inplace=inplace,
        )

    def normalize_recommendation_label(self, label: Any) -> str:
        """Normalize any recommendation label to the canonical 5-value vocabulary."""
        return normalize_recommendation_label(label)

    def normalize_recommendation_code(self, label: Any) -> str:
        """Alias for normalize_recommendation_label — returns canonical code."""
        return normalize_recommendation_code(label)


__all__ = [
    "compute_scores",
    "enrich_with_scores",
    "score_row",
    "score_quote",
    "rank_rows_by_overall",
    "assign_rank_overall",
    "score_and_rank_rows",
    "normalize_recommendation_label",
    "normalize_recommendation_code",
    "RECOMMENDATION_LABEL_MAP",
    "CANONICAL_RECOMMENDATION_CODES",
    "AssetScores",
    "ScoringEngine",
    "ScoringWeights",
    "ForecastParameters",
    "ScoreWeights",
    "SCORING_ENGINE_VERSION",
    "VERSION",
    "__version__",
]
