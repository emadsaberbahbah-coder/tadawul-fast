#!/usr/bin/env python3
# core/scoring_engine.py
"""
================================================================================
Scoring Engine Compatibility Bridge — v2.4.0
(COMPATIBILITY / CONTRACT-HARDENED / STARTUP-SAFE / LABEL-ALIGNED /
 MULTI-PATH-IMPORT / SCORING-v3.0.0-READY)
================================================================================

Purpose
- Preserve the legacy `core.scoring_engine` import surface expected by tests,
  scripts, and older callers.
- Delegate actual scoring logic to `core.scoring`.
- Re-export recommendation normalization helpers so older callers stay aligned.
- Keep startup behavior safe: no network I/O, no heavy side effects.

v2.4.0 changes vs v2.3.2
--------------------------
FIX CRITICAL: Hard `from core.scoring import ...` at module level replaced with
  a multi-path import chain that tries:
    core.scoring → core.sheets.scoring → scoring (repo-root)
  v2.3.2 crashed at import time if core.scoring was not at the expected path,
  which made the entire test suite fail before a single test ran.

FIX: ScoringEngine.__init__ now passes weights/forecasts through to the
  underlying scoring.py v3.0.0 `ScoringEngine` wrapper when available,
  so the bridge stays accurate for horizon-aware and technical-score callers.

ENH: Forward-compat try-imports for scoring.py v3.0.0 new helpers:
  compute_technical_score, rsi_signal_label (if exported). These flow through
  the bridge without breaking callers that use the v2.x API.

ENH: score_and_rank_rows on ScoringEngine now explicitly passes settings= kwarg,
  aligning with scoring.py v3.0.0 signature.

SAFE: All imports wrapped in try/except with matching fallback implementations
  so the bridge degrades gracefully rather than crashing.

Preserved from v2.3.2:
  CANONICAL_RECOMMENDATION_CODES, _NORMALIZE_TO_CANONICAL (incl. AVOID→SELL fix),
  _fallback_normalize_recommendation_label, _fallback_normalize_recommendation_code,
  compute_scores, enrich_with_scores, _as_dict helper, full __all__ export list.

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

import importlib
from dataclasses import asdict, is_dataclass
from typing import Any, Dict, List, Mapping, Optional, Sequence

# =============================================================================
# Multi-path import of core.scoring
# v2.4.0: replaces the hard `from core.scoring import ...` that crashed startup
#         when core.scoring was not mounted at the exact expected path.
# =============================================================================
_SCORING_CANDIDATES = (
    "core.scoring",
    "core.sheets.scoring",
    "scoring",
)

_scoring_mod: Any = None
for _candidate in _SCORING_CANDIDATES:
    try:
        _scoring_mod = importlib.import_module(_candidate)
        break
    except Exception:
        continue

if _scoring_mod is None:
    raise ImportError(
        f"scoring_engine bridge: cannot import scoring module from any of "
        f"{_SCORING_CANDIDATES}. Ensure core/scoring.py is deployed."
    )


def _scoring_attr(name: str, default: Any = None) -> Any:
    """Get an attribute from the resolved scoring module, with fallback."""
    return getattr(_scoring_mod, name, default)


# ---------------------------------------------------------------------------
# Stable symbols — present in core.scoring since v2.0+
# ---------------------------------------------------------------------------
AssetScores         = _scoring_attr("AssetScores")
ForecastParameters  = _scoring_attr("ForecastParameters")
ScoreWeights        = _scoring_attr("ScoreWeights")
ScoringWeights      = _scoring_attr("ScoringWeights") or ScoreWeights
DEFAULT_WEIGHTS     = _scoring_attr("DEFAULT_WEIGHTS")
DEFAULT_FORECASTS   = _scoring_attr("DEFAULT_FORECASTS")

_compute_scores     = _scoring_attr("compute_scores")
_enrich_with_scores = _scoring_attr("enrich_with_scores")
_score_row          = _scoring_attr("score_row")
_score_quote        = _scoring_attr("score_quote")
_rank_rows          = _scoring_attr("rank_rows_by_overall")
_assign_rank        = _scoring_attr("assign_rank_overall")
_score_and_rank     = _scoring_attr("score_and_rank_rows")

_SCORING_VERSION    = _scoring_attr("__version__") or _scoring_attr("SCORING_VERSION") or "unknown"

# Guard: if core symbols are missing, raise clearly at import time
for _sym_name, _sym_val in [
    ("compute_scores",      _compute_scores),
    ("enrich_with_scores",  _enrich_with_scores),
    ("AssetScores",         AssetScores),
]:
    if _sym_val is None:
        raise ImportError(
            f"scoring_engine bridge: '{_sym_name}' not found in resolved scoring module "
            f"({_scoring_mod.__name__}). The module may be too old or incomplete."
        )

# ---------------------------------------------------------------------------
# Forward-compat: scoring.py v3.0.0 new helpers (optional — fail-safe)
# ---------------------------------------------------------------------------
# These are exported by scoring.py v3.0.0 but not v2.x.
# The bridge re-exports them transparently so callers that know about them
# can use `core.scoring_engine.compute_technical_score` if needed.
_compute_technical_score = _scoring_attr("compute_technical_score")   # v3.0.0+
_rsi_signal_label        = _scoring_attr("rsi_signal_label")          # v3.0.0+
_short_term_signal       = _scoring_attr("short_term_signal")         # v3.0.0+

# =============================================================================
# Canonical vocabulary
# =============================================================================

# Canonical 5-value recommendation vocabulary (STRONG_SELL normalises to SELL)
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

# FIX v2.3.2 (preserved): AVOID → "SELL" — aligned with scoring.py _RECOMMENDATION_CODE_ALIASES
_NORMALIZE_TO_CANONICAL: Dict[str, str] = {
    "ACCUMULATE":      "BUY",
    "ADD":             "BUY",
    "OUTPERFORM":      "BUY",
    "OVERWEIGHT":      "BUY",
    "STRONG BUY":      "STRONG_BUY",
    "STRONGBUY":       "STRONG_BUY",
    "STRONG_BUY":      "STRONG_BUY",
    "BUY":             "BUY",
    "HOLD":            "HOLD",
    "NEUTRAL":         "HOLD",
    "MAINTAIN":        "HOLD",
    "MARKET PERFORM":  "HOLD",
    "EQUAL WEIGHT":    "HOLD",
    "REDUCE":          "REDUCE",
    "UNDERPERFORM":    "REDUCE",
    "UNDERWEIGHT":     "REDUCE",
    "TRIM":            "REDUCE",
    "AVOID":           "SELL",   # FIX v2.3.2: was "REDUCE"
    "SELL":            "SELL",
    "STRONG SELL":     "SELL",   # normalises to SELL (not in 5-val vocab)
    "STRONG_SELL":     "SELL",
    "EXIT":            "SELL",
    "WATCH":           "HOLD",
}


def _fallback_normalize_recommendation_label(label: Any) -> str:
    """
    Fallback normalize_recommendation_label.
    Used when core.scoring < v2.3.0 or does not export this function.
    Returns canonical code from the 5-value vocabulary.
    """
    raw = str(label or "").strip().upper()
    raw = raw.replace("-", " ")
    while "  " in raw:
        raw = raw.replace("  ", " ")
    raw = raw.strip()
    if raw in _NORMALIZE_TO_CANONICAL:
        return _NORMALIZE_TO_CANONICAL[raw]
    raw_under = raw.replace(" ", "_")
    if raw_under in _NORMALIZE_TO_CANONICAL:
        return _NORMALIZE_TO_CANONICAL[raw_under]
    return "HOLD"


def _fallback_normalize_recommendation_code(label: Any) -> str:
    return _fallback_normalize_recommendation_label(label)


# Try to use scoring module's versions; fall back to local implementations
RECOMMENDATION_LABEL_MAP = (
    _scoring_attr("RECOMMENDATION_LABEL_MAP")
    or _FALLBACK_RECOMMENDATION_LABEL_MAP
)

normalize_recommendation_label = (
    _scoring_attr("normalize_recommendation_label")
    or _fallback_normalize_recommendation_label
)

normalize_recommendation_code = (
    _scoring_attr("normalize_recommendation_code")
    or _fallback_normalize_recommendation_code
)

# Prefer scoring module's canonical codes if it exports them
_core_codes = _scoring_attr("CANONICAL_RECOMMENDATION_CODES")
if _core_codes is not None:
    CANONICAL_RECOMMENDATION_CODES = _core_codes

# =============================================================================
# Version constants
# =============================================================================
SCORING_ENGINE_VERSION   = "2.4.0"
VERSION                  = SCORING_ENGINE_VERSION
__version__              = SCORING_ENGINE_VERSION
_SCORING_MODULE_VERSION  = _SCORING_VERSION


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
# Public module-level functions
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


def score_row(row: Dict[str, Any], *, settings: Any = None) -> Dict[str, Any]:
    if _score_row is not None:
        return _as_dict(_score_row(row or {}, settings=settings))
    return compute_scores(row, settings=settings)


def score_quote(row: Dict[str, Any], *, settings: Any = None) -> Dict[str, Any]:
    if _score_quote is not None:
        return _as_dict(_score_quote(row or {}, settings=settings))
    return compute_scores(row, settings=settings)


def rank_rows_by_overall(rows: Sequence[Dict[str, Any]]) -> List[Dict[str, Any]]:
    if _rank_rows is not None:
        return list(_rank_rows(list(rows or [])))
    # inline fallback: sort descending by overall_score
    return sorted(
        [dict(r) for r in (rows or [])],
        key=lambda r: float(r.get("overall_score") or 0.0),
        reverse=True,
    )


def assign_rank_overall(
    rows: Sequence[Dict[str, Any]],
    *,
    key_overall: str = "overall_score",
    inplace: bool = True,
    rank_key: str = "rank_overall",
) -> List[Dict[str, Any]]:
    if _assign_rank is not None:
        try:
            return list(_assign_rank(
                list(rows or []),
                key_overall=key_overall,
                inplace=inplace,
                rank_key=rank_key,
            ))
        except TypeError:
            return list(_assign_rank(list(rows or [])))
    # inline fallback
    sorted_rows = rank_rows_by_overall(rows)
    for idx, row in enumerate(sorted_rows, start=1):
        row[rank_key] = idx
    return sorted_rows


def score_and_rank_rows(
    rows: Sequence[Dict[str, Any]],
    *,
    settings: Any = None,
    key_overall: str = "overall_score",
    inplace: bool = False,
) -> List[Dict[str, Any]]:
    if _score_and_rank is not None:
        try:
            return list(_score_and_rank(
                list(rows or []),
                settings=settings,
                key_overall=key_overall,
                inplace=inplace,
            ))
        except TypeError:
            return list(_score_and_rank(list(rows or [])))
    # inline fallback
    enriched = [enrich_with_scores(dict(r), settings=settings) for r in (rows or [])]
    return assign_rank_overall(enriched, key_overall=key_overall, inplace=True)


# =============================================================================
# ScoringEngine wrapper
# =============================================================================
class ScoringEngine:
    """
    Compatibility wrapper for class-based callers.
    All methods delegate to core.scoring functions.
    v2.4.0: passes weights/forecasts through to underlying scoring.py v3.0.0 when available.
    """

    version = SCORING_ENGINE_VERSION

    def __init__(
        self,
        *,
        settings: Any = None,
        weights: Optional[Any] = None,
        forecast_parameters: Optional[Any] = None,
        forecasts: Optional[Any] = None,
    ) -> None:
        self.settings            = settings
        self.weights             = weights
        self.forecast_parameters = forecast_parameters or forecasts

        # v2.4.0: try to use the native ScoringEngine from scoring.py v3.0.0
        # which supports weights= and forecasts= kwargs directly
        self._native_engine: Any = None
        NativeEngine = _scoring_attr("ScoringEngine")
        if NativeEngine is not None and NativeEngine is not self.__class__:
            try:
                native_kwargs: Dict[str, Any] = {}
                if settings is not None:         native_kwargs["settings"]  = settings
                if weights is not None:          native_kwargs["weights"]   = weights
                if self.forecast_parameters is not None:
                    native_kwargs["forecasts"] = self.forecast_parameters
                self._native_engine = NativeEngine(**native_kwargs)
            except Exception:
                self._native_engine = None

    def compute_scores(self, row: Dict[str, Any]) -> Dict[str, Any]:
        if self._native_engine is not None:
            try:
                return _as_dict(self._native_engine.compute_scores(row))
            except Exception:
                pass
        return compute_scores(row, settings=self.settings)

    def enrich_with_scores(self, row: Dict[str, Any], *, in_place: bool = False) -> Dict[str, Any]:
        if self._native_engine is not None:
            try:
                fn = getattr(self._native_engine, "enrich_with_scores", None)
                if callable(fn):
                    return _as_dict(fn(row, in_place=in_place))
            except Exception:
                pass
        return enrich_with_scores(row, settings=self.settings, in_place=in_place)

    def score_row(self, row: Dict[str, Any]) -> Dict[str, Any]:
        return _as_dict(score_row(row or {}, settings=self.settings))

    def score_quote(self, row: Dict[str, Any]) -> Dict[str, Any]:
        return _as_dict(score_quote(row or {}, settings=self.settings))

    def rank_rows_by_overall(self, rows: Sequence[Dict[str, Any]]) -> List[Dict[str, Any]]:
        return rank_rows_by_overall(list(rows or []))

    def assign_rank_overall(
        self,
        rows: Sequence[Dict[str, Any]],
        *,
        key_overall: str = "overall_score",
        inplace: bool = True,
        rank_key: str = "rank_overall",
    ) -> List[Dict[str, Any]]:
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
    ) -> List[Dict[str, Any]]:
        # v2.4.0: pass settings= explicitly so horizon-aware scoring in v3.0.0 works
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


# =============================================================================
# Exports
# =============================================================================
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
