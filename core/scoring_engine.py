#!/usr/bin/env python3
# core/scoring_engine.py
"""
================================================================================
Scoring Engine Compatibility Bridge -- v3.1.0
(COMPATIBILITY / CONTRACT-HARDENED / STARTUP-SAFE / LABEL-ALIGNED /
 MULTI-PATH-IMPORT / SCORING-v4.1.0-READY)
================================================================================

Purpose
-------
- Preserve the legacy ``core.scoring_engine`` import surface expected by
  tests, scripts, and older callers.
- Delegate actual scoring logic to ``core.scoring`` (v4.1.0).
- Re-export recommendation normalization helpers so older callers stay
  aligned with the canonical reco vocabulary enforced by
  ``core.reco_normalize``.
- Keep startup behavior safe: no network I/O, no heavy side effects.

v3.1.0 changes (what moved from v3.0.0)
---------------------------------------
- FIX: ``score_and_rank_rows`` and ``assign_rank_overall`` TypeError
  fallback paths previously called the native helper with only the
  positional ``rows`` argument, silently dropping ``settings``,
  ``key_overall``, ``inplace``, and ``rank_key``. Rows would then be
  scored/ranked with defaults that did not match the caller's intent.
  v3.1.0 retries the call with best-effort positional arguments so no
  configuration is lost.

- FIX: ``_fallback_normalize_recommendation_label`` now attempts to
  delegate to ``core.reco_normalize.normalize_recommendation`` before
  falling back to the local 24-entry alias map. ``scoring.py`` v4.1.0
  exports ``normalize_recommendation_code`` but not
  ``normalize_recommendation_label``, which means the bridge's own
  fallback is the primary path for the ``_label`` name. Delegating to
  ``reco_normalize`` gives full coverage of English/Arabic/French/
  Spanish/German/Portuguese alias terms instead of silently mapping
  unknown inputs to HOLD.

- ENHANCE: re-export ``Horizon``, ``Signal``, and ``RSISignal`` enums
  so callers can write ``from core.scoring_engine import Horizon``
  without reaching into ``core.scoring`` directly.

- ENHANCE: re-export ``compute_recommendation`` (the horizon-aware
  recommender function). It is public in scoring.py v4.1.0 __all__ and
  is used by advisors that import from this bridge.

- FIX: ``rank_rows_by_overall`` fallback path now matches the native
  contract -- rows are returned in *input order* with a ``rank_overall``
  field annotated on each. v3.0.0's fallback returned rows sorted
  descending by ``overall_score``, which diverged from the native
  ``core.scoring.rank_rows_by_overall`` behavior (annotate-only). The
  divergence was latent because the native path is always taken in a
  healthy deployment, but any code path exercising the fallback would
  have returned a different row order.

- FIX: ``detect_horizon`` return-type annotation relaxed from
  ``Tuple[str, Optional[int]]`` to ``Tuple[Any, Optional[int]]``. The
  native scoring module returns ``Tuple[Horizon, Optional[int]]``
  where ``Horizon`` is a ``(str, Enum)`` subclass -- string equality
  still works, but annotating as plain ``str`` was misleading to
  type-checkers.

- Bump version: ``SCORING_ENGINE_VERSION = "3.1.0"``. Mirrors the
  alignment bump that pushed scoring.py to v4.1.0.

Preserved
---------
- All public symbols in v3.0.0's __all__.
- Multi-path import order: ``core.scoring`` -> ``core.sheets.scoring``
  -> ``scoring``.
- ``_as_dict`` polymorphic coercion (dataclass / Mapping / pydantic v1
  / pydantic v2 / plain __dict__).
- Native ``ScoringEngine`` pass-through with graceful degradation when
  native instantiation fails.
- Fallback implementations for every delegated function so the bridge
  degrades gracefully if scoring.py is missing v4.1.0 symbols.
================================================================================
"""

from __future__ import annotations

import importlib
from dataclasses import asdict, is_dataclass
from typing import (
    Any,
    Callable,
    Dict,
    List,
    Mapping,
    Optional,
    Sequence,
    Tuple,
)

# =============================================================================
# Multi-Path Import of core.scoring
# =============================================================================

_SCORING_CANDIDATES: Tuple[str, ...] = (
    "core.scoring",
    "core.sheets.scoring",
    "scoring",
)

_scoring_mod: Any = None
_scoring_import_error: Optional[str] = None

for _candidate in _SCORING_CANDIDATES:
    try:
        _scoring_mod = importlib.import_module(_candidate)
        break
    except ImportError as e:
        _scoring_import_error = str(e)
        continue

if _scoring_mod is None:
    raise ImportError(
        f"scoring_engine bridge: cannot import scoring module from any of "
        f"{_SCORING_CANDIDATES}. Ensure core/scoring.py is deployed. "
        f"Last error: {_scoring_import_error}"
    )


def _scoring_attr(name: str, default: Any = None) -> Any:
    """Get attribute from scoring module with fallback."""
    return getattr(_scoring_mod, name, default)


# =============================================================================
# Best-Effort Import of reco_normalize (for richer fallback coverage)
# =============================================================================

_RECO_CANDIDATES: Tuple[str, ...] = (
    "core.reco_normalize",
    "reco_normalize",
)

_reco_mod: Any = None
for _rc in _RECO_CANDIDATES:
    try:
        _reco_mod = importlib.import_module(_rc)
        break
    except ImportError:
        continue

_reco_normalize_fn: Optional[Callable[[Any], Any]] = (
    getattr(_reco_mod, "normalize_recommendation", None) if _reco_mod is not None else None
)


def _reco_delegate(label: Any) -> Optional[str]:
    """
    Attempt to normalize ``label`` using ``reco_normalize.normalize_recommendation``.

    Returns the canonical uppercase code (e.g. "STRONG_BUY") on success,
    or ``None`` if reco_normalize is unavailable or returned an unknown
    shape.
    """
    if _reco_normalize_fn is None:
        return None
    try:
        result = _reco_normalize_fn(label)
    except Exception:
        return None

    # reco_normalize returns a Recommendation enum whose .value / str()
    # is the canonical code. Enum has str(Recommendation.BUY) == "Recommendation.BUY"
    # in some Python versions, so prefer .value / .name.
    try:
        val = getattr(result, "value", None)
        if isinstance(val, str) and val:
            return val.upper()
    except Exception:
        pass
    try:
        name = getattr(result, "name", None)
        if isinstance(name, str) and name:
            return name.upper()
    except Exception:
        pass
    if isinstance(result, str) and result:
        return result.upper()
    return None


# =============================================================================
# Version Constants
# =============================================================================

SCORING_ENGINE_VERSION = "3.1.0"
VERSION = SCORING_ENGINE_VERSION
__version__ = SCORING_ENGINE_VERSION

_SCORING_VERSION = (
    _scoring_attr("__version__") or _scoring_attr("SCORING_VERSION") or "unknown"
)

# =============================================================================
# Stable Symbols from scoring.py
# =============================================================================

# Data classes / enums
AssetScores = _scoring_attr("AssetScores")
ForecastParameters = _scoring_attr("ForecastParameters")
ScoreWeights = _scoring_attr("ScoreWeights")
ScoringWeights = _scoring_attr("ScoringWeights") or ScoreWeights

# Horizon / Signal / RSISignal enums (v3.1.0 re-export)
Horizon = _scoring_attr("Horizon")
Signal = _scoring_attr("Signal")
RSISignal = _scoring_attr("RSISignal")

# Defaults
DEFAULT_WEIGHTS = _scoring_attr("DEFAULT_WEIGHTS")
DEFAULT_FORECASTS = _scoring_attr("DEFAULT_FORECASTS")

# Core functions
_compute_scores = _scoring_attr("compute_scores")
_enrich_with_scores = _scoring_attr("enrich_with_scores")
_score_row = _scoring_attr("score_row")
_score_quote = _scoring_attr("score_quote")
_rank_rows = _scoring_attr("rank_rows_by_overall")
_assign_rank = _scoring_attr("assign_rank_overall")
_score_and_rank = _scoring_attr("score_and_rank_rows")

# Horizon-aware helpers
_compute_technical_score = _scoring_attr("compute_technical_score")
_rsi_signal = _scoring_attr("rsi_signal")
_short_term_signal = _scoring_attr("short_term_signal")
_detect_horizon = _scoring_attr("detect_horizon")
_get_weights_for_horizon = _scoring_attr("get_weights_for_horizon")
_derive_upside_pct = _scoring_attr("derive_upside_pct")
_derive_volume_ratio = _scoring_attr("derive_volume_ratio")
_derive_day_range_position = _scoring_attr("derive_day_range_position")
_invest_period_label = _scoring_attr("invest_period_label")

# Horizon-aware recommender (v3.1.0 re-export)
_compute_recommendation = _scoring_attr("compute_recommendation")

# Validation -- required symbols only
for _sym_name, _sym_val in (
    ("compute_scores", _compute_scores),
    ("enrich_with_scores", _enrich_with_scores),
    ("AssetScores", AssetScores),
):
    if _sym_val is None:
        raise ImportError(
            f"scoring_engine bridge: '{_sym_name}' not found in resolved "
            f"scoring module ({_scoring_mod.__name__}). The module may be "
            f"too old or incomplete."
        )

# =============================================================================
# Recommendation Normalization
# =============================================================================

# Canonical 5-value recommendation vocabulary (matches reco_normalize).
CANONICAL_RECOMMENDATION_CODES: Tuple[str, ...] = (
    "STRONG_BUY",
    "BUY",
    "HOLD",
    "REDUCE",
    "SELL",
)

_FALLBACK_RECOMMENDATION_LABEL_MAP: Dict[str, str] = {
    "STRONG_BUY": "Strong Buy",
    "BUY": "Buy",
    "HOLD": "Hold",
    "REDUCE": "Reduce",
    "SELL": "Sell",
}

# Last-resort alias table used only if both scoring.py and reco_normalize
# are unavailable (should never happen in a healthy deployment).
_NORMALIZE_TO_CANONICAL: Dict[str, str] = {
    "ACCUMULATE": "BUY",
    "ADD": "BUY",
    "OUTPERFORM": "BUY",
    "OVERWEIGHT": "BUY",
    "STRONG BUY": "STRONG_BUY",
    "STRONGBUY": "STRONG_BUY",
    "STRONG_BUY": "STRONG_BUY",
    "BUY": "BUY",
    "HOLD": "HOLD",
    "NEUTRAL": "HOLD",
    "MAINTAIN": "HOLD",
    "MARKET PERFORM": "HOLD",
    "EQUAL WEIGHT": "HOLD",
    "REDUCE": "REDUCE",
    "UNDERPERFORM": "REDUCE",
    "UNDERWEIGHT": "REDUCE",
    "TRIM": "REDUCE",
    "AVOID": "SELL",
    "SELL": "SELL",
    "STRONG SELL": "SELL",
    "STRONG_SELL": "SELL",
    "EXIT": "SELL",
    "WATCH": "HOLD",
}


def _fallback_normalize_recommendation_label(label: Any) -> str:
    """
    Fallback normalize_recommendation_label.

    Used when ``core.scoring`` does not export ``normalize_recommendation_label``
    directly (which is the common case with scoring.py v4.1.0).

    Resolution order:
      1. Delegate to ``core.reco_normalize.normalize_recommendation`` if
         available (full multi-language coverage).
      2. Local 24-entry alias map.
      3. Default to "HOLD".
    """
    # Step 1: try reco_normalize delegation
    delegated = _reco_delegate(label)
    if delegated is not None:
        return delegated

    # Step 2: local alias table
    raw = str(label or "").strip().upper()
    raw = raw.replace("-", " ")
    while "  " in raw:
        raw = raw.replace("  ", " ")
    raw = raw.strip()

    if not raw:
        return "HOLD"

    if raw in _NORMALIZE_TO_CANONICAL:
        return _NORMALIZE_TO_CANONICAL[raw]

    raw_under = raw.replace(" ", "_")
    if raw_under in _NORMALIZE_TO_CANONICAL:
        return _NORMALIZE_TO_CANONICAL[raw_under]

    return "HOLD"


def _fallback_normalize_recommendation_code(label: Any) -> str:
    """Fallback normalize_recommendation_code (alias for the label variant)."""
    return _fallback_normalize_recommendation_label(label)


# Prefer scoring module's versions; fall back to local implementations.
RECOMMENDATION_LABEL_MAP: Dict[str, str] = (
    _scoring_attr("RECOMMENDATION_LABEL_MAP") or _FALLBACK_RECOMMENDATION_LABEL_MAP
)

normalize_recommendation_label: Callable[[Any], str] = (
    _scoring_attr("normalize_recommendation_label")
    or _fallback_normalize_recommendation_label
)

normalize_recommendation_code: Callable[[Any], str] = (
    _scoring_attr("normalize_recommendation_code")
    or _fallback_normalize_recommendation_code
)

# Prefer scoring module's canonical codes if exported.
_core_codes = _scoring_attr("CANONICAL_RECOMMENDATION_CODES")
if _core_codes is not None:
    try:
        CANONICAL_RECOMMENDATION_CODES = tuple(_core_codes)
    except TypeError:
        pass  # keep local default


# =============================================================================
# Helper Functions
# =============================================================================

def _as_dict(obj: Any) -> Dict[str, Any]:
    """
    Convert a variety of input shapes to a plain ``dict``.

    Handles: None, dict, Mapping, dataclass, pydantic v2 (model_dump),
    pydantic v1 (.dict()), and plain objects with __dict__.
    """
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

    # Pydantic v2
    try:
        model_dump = getattr(obj, "model_dump", None)
        if callable(model_dump):
            dumped = model_dump(mode="python")
            if isinstance(dumped, Mapping):
                return dict(dumped)
    except Exception:
        pass

    # Pydantic v1
    try:
        dict_fn = getattr(obj, "dict", None)
        if callable(dict_fn):
            dumped = dict_fn()
            if isinstance(dumped, Mapping):
                return dict(dumped)
    except Exception:
        pass

    # Plain object
    try:
        raw = getattr(obj, "__dict__", None)
        if isinstance(raw, Mapping):
            return dict(raw)
    except Exception:
        pass

    return {}


# =============================================================================
# Public Module-Level Functions
# =============================================================================

def compute_scores(row: Dict[str, Any], settings: Any = None) -> Dict[str, Any]:
    """
    Compute scores for a row.

    Args:
        row: Input data row.
        settings: Optional settings object (env-loaded ScoringConfig,
            pydantic Settings model, or plain Mapping).

    Returns:
        Dictionary with computed scores.
    """
    return _as_dict(_compute_scores(row or {}, settings=settings))


def enrich_with_scores(
    row: Dict[str, Any],
    settings: Any = None,
    in_place: bool = False,
) -> Dict[str, Any]:
    """
    Enrich a row with scores.

    Args:
        row: Input data row.
        settings: Optional settings object.
        in_place: Whether to modify the input row in place.

    Returns:
        Enriched row dictionary.
    """
    result = _enrich_with_scores(row or {}, settings=settings, in_place=in_place)
    return _as_dict(result)


def score_row(row: Dict[str, Any], settings: Any = None) -> Dict[str, Any]:
    """Score a single row."""
    if _score_row is not None:
        return _as_dict(_score_row(row or {}, settings=settings))
    return compute_scores(row, settings=settings)


def score_quote(row: Dict[str, Any], settings: Any = None) -> Dict[str, Any]:
    """Score a quote row."""
    if _score_quote is not None:
        return _as_dict(_score_quote(row or {}, settings=settings))
    return compute_scores(row, settings=settings)


def rank_rows_by_overall(rows: Sequence[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Assign overall-score rank to rows.

    NOTE: This matches the contract of ``core.scoring.rank_rows_by_overall``
    (v4.1.0) -- rows are returned in *input order* with a ``rank_overall``
    field annotated on each. The rank reflects descending sort order by
    overall_score, but the returned list is NOT sorted. Call sites that
    want sorted output should do ``sorted(rows, key=lambda r: r["rank_overall"])``.

    Args:
        rows: List of rows to rank.

    Returns:
        Rows in input order, each annotated with ``rank_overall``.
    """
    if _rank_rows is not None:
        return list(_rank_rows(list(rows or [])))

    # Fallback: annotate rank_overall on each row in descending order of
    # overall_score while preserving input order (matches native contract).
    rows_list = [dict(r) for r in (rows or [])]
    indexed = list(enumerate(rows_list))
    indexed.sort(
        key=lambda item: float(item[1].get("overall_score") or 0.0),
        reverse=True,
    )
    for rank, (_, row) in enumerate(indexed, start=1):
        row["rank_overall"] = rank
    return rows_list


def assign_rank_overall(
    rows: Sequence[Dict[str, Any]],
    key_overall: str = "overall_score",
    inplace: bool = True,
    rank_key: str = "rank_overall",
) -> List[Dict[str, Any]]:
    """
    Assign rank to rows based on overall score.

    Args:
        rows: List of rows to rank.
        key_overall: Key for overall score.
        inplace: Whether to modify rows in place.
        rank_key: Key for rank field.

    Returns:
        Rows with rank assigned.
    """
    rows_list = list(rows or [])

    if _assign_rank is not None:
        # Preferred path: keyword call
        try:
            return list(_assign_rank(
                rows_list,
                key_overall=key_overall,
                inplace=inplace,
                rank_key=rank_key,
            ))
        except TypeError:
            pass
        # Graceful degradation: best-effort positional call that still
        # preserves all four arguments. v3.0.0 dropped them here.
        try:
            return list(_assign_rank(rows_list, key_overall, inplace, rank_key))
        except TypeError:
            pass
        # Last resort: native signature takes rows only.
        try:
            return list(_assign_rank(rows_list))
        except Exception:
            pass

    # Pure Python fallback
    sorted_rows = rank_rows_by_overall(rows_list)
    for idx, row in enumerate(sorted_rows, start=1):
        row[rank_key] = idx
    return sorted_rows


def score_and_rank_rows(
    rows: Sequence[Dict[str, Any]],
    settings: Any = None,
    key_overall: str = "overall_score",
    inplace: bool = False,
) -> List[Dict[str, Any]]:
    """
    Score and rank rows.

    Args:
        rows: List of rows to score and rank.
        settings: Optional settings object.
        key_overall: Key for overall score.
        inplace: Whether to modify rows in place.

    Returns:
        Scored and ranked rows.
    """
    rows_list = list(rows or [])

    if _score_and_rank is not None:
        # Preferred path: keyword call
        try:
            return list(_score_and_rank(
                rows_list,
                settings=settings,
                key_overall=key_overall,
                inplace=inplace,
            ))
        except TypeError:
            pass
        # Graceful degradation preserving all arguments positionally.
        # v3.0.0 fallback silently dropped settings/key_overall/inplace here.
        try:
            return list(_score_and_rank(rows_list, settings, key_overall, inplace))
        except TypeError:
            pass
        # Two-arg shape
        try:
            return list(_score_and_rank(rows_list, settings))
        except TypeError:
            pass
        # Last resort: positional rows only
        try:
            return list(_score_and_rank(rows_list))
        except Exception:
            pass

    # Pure Python fallback
    enriched = [enrich_with_scores(dict(r), settings=settings) for r in rows_list]
    return assign_rank_overall(
        enriched,
        key_overall=key_overall,
        inplace=True,
        rank_key="rank_overall",
    )


# =============================================================================
# Horizon-Aware Helpers
# =============================================================================

def compute_technical_score(row: Dict[str, Any]) -> Optional[float]:
    """Compute technical score (0-100)."""
    if _compute_technical_score is not None:
        return _compute_technical_score(row)
    return None


def rsi_signal(row: Dict[str, Any]) -> str:
    """Get RSI signal (Oversold / Neutral / Overbought / N/A) from row."""
    if _rsi_signal is not None:
        return _rsi_signal(row.get("rsi_14"))
    return "N/A"


def short_term_signal(row: Dict[str, Any], horizon: Any = "month") -> str:
    """
    Get short-term trading signal.

    Args:
        row: Input data row (expects ``technical_score``, ``momentum_score``,
            ``risk_score``).
        horizon: Investment horizon label or ``Horizon`` enum. Because
            ``Horizon`` is ``(str, Enum)``, passing either the enum or the
            string value works for dict/equality lookups in the native
            helper.

    Returns:
        Trading signal (STRONG_BUY / BUY / HOLD / SELL).
    """
    if _short_term_signal is not None:
        technical = row.get("technical_score")
        momentum = row.get("momentum_score")
        risk = row.get("risk_score")
        return _short_term_signal(technical, momentum, risk, horizon)
    return "HOLD"


def detect_horizon(
    settings: Any = None,
    row: Optional[Dict[str, Any]] = None,
) -> Tuple[Any, Optional[int]]:
    """
    Detect investment horizon.

    Args:
        settings: Optional settings object.
        row: Optional row data.

    Returns:
        Tuple of ``(horizon_label, horizon_days)``. ``horizon_label`` is
        a ``Horizon`` enum from the native path (subclass of ``str``), or
        the literal string ``"month"`` when the native helper is missing.
    """
    if _detect_horizon is not None:
        return _detect_horizon(settings, row)
    return "month", None


def get_weights_for_horizon(horizon: Any, settings: Any = None) -> Any:
    """
    Get weights for a specific horizon.

    Args:
        horizon: Horizon label (str or ``Horizon`` enum).
        settings: Optional settings object.

    Returns:
        ``ScoreWeights`` instance.
    """
    if _get_weights_for_horizon is not None:
        return _get_weights_for_horizon(horizon, settings)
    return DEFAULT_WEIGHTS


def derive_upside_pct(row: Dict[str, Any]) -> Optional[float]:
    """Derive upside percentage from row."""
    if _derive_upside_pct is not None:
        return _derive_upside_pct(row)
    return None


def derive_volume_ratio(row: Dict[str, Any]) -> Optional[float]:
    """Derive volume ratio from row."""
    if _derive_volume_ratio is not None:
        return _derive_volume_ratio(row)
    return None


def derive_day_range_position(row: Dict[str, Any]) -> Optional[float]:
    """Derive day range position from row."""
    if _derive_day_range_position is not None:
        return _derive_day_range_position(row)
    return None


def invest_period_label(horizon: Any, horizon_days: Optional[int] = None) -> str:
    """Get investment period label (e.g. ``1D``, ``1W``, ``1M``, ``3M``, ``12M``)."""
    if _invest_period_label is not None:
        return _invest_period_label(horizon, horizon_days)
    return "1M"


def compute_recommendation(
    overall: Optional[float],
    risk: Optional[float],
    confidence100: Optional[float],
    roi3: Optional[float],
    horizon: Any = None,
    technical: Optional[float] = None,
    momentum: Optional[float] = None,
    roi1: Optional[float] = None,
    roi12: Optional[float] = None,
) -> Tuple[str, str]:
    """
    Compute horizon-aware recommendation + reason.

    Delegates to ``core.scoring.compute_recommendation``. If ``horizon``
    is None, defaults to the native helper's ``Horizon.MONTH``.

    Returns:
        ``(recommendation_code, reason_text)`` tuple where
        ``recommendation_code`` is one of the canonical codes.
    """
    if _compute_recommendation is None:
        return "HOLD", "scoring.compute_recommendation unavailable."

    if horizon is None and Horizon is not None:
        horizon = getattr(Horizon, "MONTH", "month")
    elif horizon is None:
        horizon = "month"

    try:
        return _compute_recommendation(
            overall, risk, confidence100, roi3,
            horizon=horizon,
            technical=technical,
            momentum=momentum,
            roi1=roi1,
            roi12=roi12,
        )
    except TypeError:
        # Minimal-signature fallback
        try:
            return _compute_recommendation(overall, risk, confidence100, roi3)
        except Exception as e:
            return "HOLD", f"compute_recommendation fallback failed: {e}"
    except Exception as e:
        return "HOLD", f"compute_recommendation raised: {e}"


# =============================================================================
# ScoringEngine Wrapper
# =============================================================================

class ScoringEngine:
    """
    Compatibility wrapper for class-based callers.

    All methods delegate to ``core.scoring`` functions. When a native
    ``ScoringEngine`` is available in the resolved scoring module, it is
    preferred over the free-function fallback path.
    """

    version = SCORING_ENGINE_VERSION

    def __init__(
        self,
        settings: Any = None,
        weights: Optional[Any] = None,
        forecast_parameters: Optional[Any] = None,
        forecasts: Optional[Any] = None,
    ) -> None:
        """
        Initialize scoring engine.

        Args:
            settings: Optional settings object.
            weights: Optional weights for scoring.
            forecast_parameters: Optional forecast parameters.
            forecasts: Alias for ``forecast_parameters``.
        """
        self.settings = settings
        self.weights = weights
        self.forecast_parameters = forecast_parameters or forecasts

        # Try to use native ScoringEngine from scoring.py
        self._native_engine: Any = None
        NativeEngine = _scoring_attr("ScoringEngine")

        if NativeEngine is not None and NativeEngine is not self.__class__:
            try:
                native_kwargs: Dict[str, Any] = {}
                if settings is not None:
                    native_kwargs["settings"] = settings
                if weights is not None:
                    native_kwargs["weights"] = weights
                if self.forecast_parameters is not None:
                    native_kwargs["forecasts"] = self.forecast_parameters
                self._native_engine = NativeEngine(**native_kwargs)
            except Exception:
                self._native_engine = None

    # -- scoring ---------------------------------------------------------

    def compute_scores(self, row: Dict[str, Any]) -> Dict[str, Any]:
        """Compute scores for a row."""
        if self._native_engine is not None:
            try:
                return _as_dict(self._native_engine.compute_scores(row))
            except Exception:
                pass
        return compute_scores(row, settings=self.settings)

    def enrich_with_scores(
        self,
        row: Dict[str, Any],
        in_place: bool = False,
    ) -> Dict[str, Any]:
        """Enrich row with scores."""
        if self._native_engine is not None:
            try:
                fn = getattr(self._native_engine, "enrich_with_scores", None)
                if callable(fn):
                    return _as_dict(fn(row, in_place=in_place))
            except Exception:
                pass
        return enrich_with_scores(row, settings=self.settings, in_place=in_place)

    def score_row(self, row: Dict[str, Any]) -> Dict[str, Any]:
        """Score a single row."""
        return _as_dict(score_row(row or {}, settings=self.settings))

    def score_quote(self, row: Dict[str, Any]) -> Dict[str, Any]:
        """Score a quote row."""
        return _as_dict(score_quote(row or {}, settings=self.settings))

    # -- ranking ---------------------------------------------------------

    def rank_rows_by_overall(self, rows: Sequence[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Rank rows by overall score."""
        return rank_rows_by_overall(list(rows or []))

    def assign_rank_overall(
        self,
        rows: Sequence[Dict[str, Any]],
        key_overall: str = "overall_score",
        inplace: bool = True,
        rank_key: str = "rank_overall",
    ) -> List[Dict[str, Any]]:
        """Assign rank to rows."""
        return assign_rank_overall(
            list(rows or []),
            key_overall=key_overall,
            inplace=inplace,
            rank_key=rank_key,
        )

    def score_and_rank_rows(
        self,
        rows: Sequence[Dict[str, Any]],
        key_overall: str = "overall_score",
        inplace: bool = False,
    ) -> List[Dict[str, Any]]:
        """Score and rank rows."""
        return score_and_rank_rows(
            list(rows or []),
            settings=self.settings,
            key_overall=key_overall,
            inplace=inplace,
        )

    # -- recommendation normalization -----------------------------------

    def normalize_recommendation_label(self, label: Any) -> str:
        """Normalize recommendation label to canonical code."""
        return normalize_recommendation_label(label)

    def normalize_recommendation_code(self, label: Any) -> str:
        """Normalize recommendation label to canonical code."""
        return normalize_recommendation_code(label)

    # -- horizon-aware helpers ------------------------------------------

    def compute_technical_score(self, row: Dict[str, Any]) -> Optional[float]:
        """Compute technical score."""
        return compute_technical_score(row)

    def rsi_signal(self, row: Dict[str, Any]) -> str:
        """Get RSI signal."""
        return rsi_signal(row)

    def short_term_signal(self, row: Dict[str, Any], horizon: Any = "month") -> str:
        """Get short-term trading signal."""
        return short_term_signal(row, horizon)

    def detect_horizon(
        self,
        row: Optional[Dict[str, Any]] = None,
    ) -> Tuple[Any, Optional[int]]:
        """Detect investment horizon."""
        return detect_horizon(self.settings, row)

    def get_weights_for_horizon(self, horizon: Any) -> Any:
        """Get weights for horizon."""
        return get_weights_for_horizon(horizon, self.settings)

    def derive_upside_pct(self, row: Dict[str, Any]) -> Optional[float]:
        """Derive upside percentage."""
        return derive_upside_pct(row)

    def derive_volume_ratio(self, row: Dict[str, Any]) -> Optional[float]:
        """Derive volume ratio."""
        return derive_volume_ratio(row)

    def derive_day_range_position(self, row: Dict[str, Any]) -> Optional[float]:
        """Derive day range position."""
        return derive_day_range_position(row)

    def invest_period_label(
        self,
        horizon: Any,
        horizon_days: Optional[int] = None,
    ) -> str:
        """Get invest period label."""
        return invest_period_label(horizon, horizon_days)

    def compute_recommendation(
        self,
        overall: Optional[float],
        risk: Optional[float],
        confidence100: Optional[float],
        roi3: Optional[float],
        horizon: Any = None,
        technical: Optional[float] = None,
        momentum: Optional[float] = None,
        roi1: Optional[float] = None,
        roi12: Optional[float] = None,
    ) -> Tuple[str, str]:
        """Compute horizon-aware recommendation."""
        return compute_recommendation(
            overall, risk, confidence100, roi3,
            horizon=horizon,
            technical=technical,
            momentum=momentum,
            roi1=roi1,
            roi12=roi12,
        )


# =============================================================================
# Module Exports
# =============================================================================

__all__ = [
    # Version
    "SCORING_ENGINE_VERSION",
    "VERSION",
    "__version__",
    # Core functions
    "compute_scores",
    "enrich_with_scores",
    "score_row",
    "score_quote",
    "rank_rows_by_overall",
    "assign_rank_overall",
    "score_and_rank_rows",
    # Recommendation helpers
    "normalize_recommendation_label",
    "normalize_recommendation_code",
    "RECOMMENDATION_LABEL_MAP",
    "CANONICAL_RECOMMENDATION_CODES",
    # Data classes
    "AssetScores",
    "ScoringWeights",
    "ForecastParameters",
    "ScoreWeights",
    # Enums (v3.1.0 re-export)
    "Horizon",
    "Signal",
    "RSISignal",
    # Defaults
    "DEFAULT_WEIGHTS",
    "DEFAULT_FORECASTS",
    # Wrapper
    "ScoringEngine",
    # Horizon-aware helpers
    "compute_technical_score",
    "rsi_signal",
    "short_term_signal",
    "detect_horizon",
    "get_weights_for_horizon",
    "derive_upside_pct",
    "derive_volume_ratio",
    "derive_day_range_position",
    "invest_period_label",
    # v3.1.0 re-export
    "compute_recommendation",
]
