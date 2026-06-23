# -*- coding: utf-8 -*-
"""
core.quality_gates — TFB data-trust & plausibility decision layer
=================================================================
Version: 1.0.0

WHY THIS MODULE EXISTS
----------------------
The engineering audit found the precise cause of the BBD.US and 5023.SR
failures: the system DETECTS bad/sparse data but never ESCALATES detection into
a block or exclusion. 5023.SR (no name, momentum-only fallback) was warned, then
scored, ranked #10, and held. BBD.US (cost basis 250 on a ~$3.50 stock) passed a
presence-only gate and produced a confident ADD on an absurd value. The detection
exists; the CONSEQUENCE does not.

This module is that missing consequence — the single source of truth for two
decisions:
  1. cost_basis_plausibility(): is a holding's cost basis believable enough to
     produce a BUY/ADD/TRIM/SELL? (kills the BBD confident-ADD)
  2. trust_level(): is a row's data trustworthy enough to rank and recommend?
     (excludes the 5023 ghost from selection)

It is deliberately the DECISION layer, not the detection layer. It consumes
already-computed signals (a data-quality score, a name, a sparse/fallback flag,
a quote age, prices) and returns gate verdicts. The data-quality SCORE keeps
being computed where it already is (audit_data_quality.py); this module decides
what to DO about it, so the engine, portfolio actions, and the audit all share
one escalation policy instead of three.

DESIGN PRINCIPLES (same as core.stats / the Calibrator)
-------------------------------------------------------
* Pure functions, no logging, no I/O, no Sheets access -> fully unit-tested.
* numpy-free except via core.stats (used only for the MAD outlier check).
* JSON-serializable dict outputs (verdicts get logged / surfaced on the row).
* ONE config surface (DEFAULT_FIELD_MAP / DEFAULT_THRESHOLDS) for the live row
  schema and the heuristic cut-points; field names are alias-resolved
  case-insensitively, so it works across schema drift. Confirm/adjust to live.
* Thresholds are documented heuristics, NOT magic numbers; the principled
  cost-basis check (MAD vs a price history) is preferred when history is given,
  with the ratio check as the assumption-light fallback.

CHANGELOG
---------
v1.0.1
  - Field-map aligned to the live row schema (Phase 1 of adoption). This module
    is NOT yet on the live decision path, so this is config-only and changes no
    running behavior. Specifically:
      * dq_score now resolves the primary live field "data_quality_score"
        (previously only "data_quality"/"quality_score" matched, so the actual
        column would have been missed on a live row).
      * sparse_flag now also recognizes the engine's banded "low_data_trust"
        tag (data_engine_v2 v5.88.0), so a row the engine already marks
        low-trust is excluded here too.
    NOTE: the live schema carries no quote-age-in-hours field, so the (soft)
    freshness check is currently a no-op by design -- left in place for
    forward-compatibility, not a defect.
v1.0.0
  - Initial data-trust & plausibility decision layer (cost_basis_plausibility,
    trust_level, evaluate_holding).
"""

from __future__ import annotations

from typing import Any, Dict, List, Mapping, Optional, Sequence

try:
    from core import stats as st  # production location
except Exception:  # pragma: no cover - standalone/test fallback
    import stats as st  # type: ignore

__version__ = "1.0.1"

# =============================================================================
# CONFIG  ===  CONFIRM AGAINST THE LIVE ROW SCHEMA
# =============================================================================
# Logical field -> acceptable key aliases (case-insensitive, first match wins).

DEFAULT_FIELD_MAP: Dict[str, List[str]] = {
    "symbol": ["symbol", "ticker"],
    "name": ["name", "company_name", "company", "security_name", "longname",
             "short_name"],
    "current_price": ["current_price", "price", "last", "last_price", "close"],
    "avg_cost": ["avg_cost", "cost_basis", "average_cost", "buy_price",
                 "entry_price", "cost"],
    "quantity": ["quantity", "qty", "shares", "position_qty", "units", "position"],
    "dq_score": ["data_quality_score", "dq", "data_quality", "dq_score",
                 "quality_score", "dqscore", "data_quality_band"],
    "age_hours": ["age_hours", "quote_age_hours", "staleness_hours"],
    "sparse_flag": ["sparse", "momentum_only", "is_sparse", "data_sparse",
                    "fallback", "momentum_only_fallback", "low_data_trust"],
    "fundamentals_present": ["fundamentals_present", "has_fundamentals"],
}

DEFAULT_THRESHOLDS: Dict[str, float] = {
    "hi_ratio": 5.0,        # cost basis > 5x current price -> implausible
    "lo_ratio": 0.2,        # cost basis < 0.2x current price -> implausible
    "mad_k": 3.5,           # modified-z cutoff vs a price history (Iglewicz-Hoaglin)
    "max_age_hours": 48.0,  # quote older than this -> stale (soft)
    "min_dq": 50.0,         # DQ below this -> low trust (hard contributor)
}

TRUST_HIGH = "HIGH"
TRUST_MEDIUM = "MEDIUM"
TRUST_LOW = "LOW"


# =============================================================================
# Internal helpers
# =============================================================================

def _norm(s: Any) -> str:
    return str(s).strip().lower() if s is not None else ""


def _resolve(row: Mapping[str, Any], aliases: Sequence[str]) -> Any:
    """Return the first present, alias-matched value from a row (case-insensitive)."""
    norm_row = {_norm(k): v for k, v in row.items()}
    for a in aliases:
        if _norm(a) in norm_row:
            return norm_row[_norm(a)]
    return None


def _field(row: Mapping[str, Any], logical: str,
           field_map: Mapping[str, List[str]]) -> Any:
    return _resolve(row, field_map.get(logical, [logical]))


def _to_float(v: Any) -> Optional[float]:
    if v is None or v == "":
        return None
    s = str(v).strip().rstrip("%").replace(",", "")
    try:
        return float(s)
    except Exception:
        return None


def _is_blank(v: Any) -> bool:
    return v is None or str(v).strip() == ""


def _truthy(v: Any) -> bool:
    if isinstance(v, bool):
        return v
    return _norm(v) in ("true", "1", "yes", "y", "sparse", "momentum_only",
                        "fallback")


# =============================================================================
# 1) COST-BASIS PLAUSIBILITY  (the BBD gate)
# =============================================================================

def cost_basis_plausibility(
    avg_cost: Any,
    current_price: Any,
    *,
    quantity: Any = None,
    history: Optional[Sequence[float]] = None,
    hi_ratio: float = DEFAULT_THRESHOLDS["hi_ratio"],
    lo_ratio: float = DEFAULT_THRESHOLDS["lo_ratio"],
    mad_k: float = DEFAULT_THRESHOLDS["mad_k"],
) -> Dict[str, Any]:
    """
    Decide whether a holding's cost basis is believable enough to act on.

    Order of checks:
      (0) presence  - if a position exists but cost basis is blank / non-numeric
                      / <= 0, BLOCK.
      (1) MAD       - if a recent price `history` is given, flag when the cost
                      basis is a >mad_k modified-z outlier vs that history
                      (principled). BBD: $250 vs a ~$3.50 history is a >40-MAD
                      outlier.
      (2) ratio     - otherwise compare to `current_price`: BLOCK if cost basis
                      is > hi_ratio x or < lo_ratio x the current price.

    Returns dict: verdict ("OK" | "BLOCKED"), plausible (bool), reason (str),
    method ("presence" | "mad" | "ratio" | "unknown").
    """
    qty = _to_float(quantity)
    has_position = qty is None or qty > 0  # if qty unknown, assume a position exists
    cost = _to_float(avg_cost)

    # (0) presence / validity
    if has_position and (cost is None or cost <= 0):
        detail = "blank/non-numeric" if cost is None else f"{cost:g}"
        return {"verdict": "BLOCKED", "plausible": False, "method": "presence",
                "reason": f"Cost basis {detail} is missing or non-positive while a "
                          f"holding exists — review cost basis."}
    if cost is None:
        return {"verdict": "OK", "plausible": True, "method": "unknown",
                "reason": "No position / no cost basis to validate."}

    # (1) MAD vs price history (preferred when available)
    hist = [h for h in (history or []) if _to_float(h) is not None]
    if len(hist) >= 3:
        if st.is_mad_outlier([float(h) for h in hist], float(cost), k=mad_k):
            med = sorted(float(h) for h in hist)[len(hist) // 2]
            return {"verdict": "BLOCKED", "plausible": False, "method": "mad",
                    "reason": f"Cost basis {cost:g} is a >{mad_k} MAD outlier vs "
                              f"recent prices (median ~{med:g}) — review cost basis."}
        return {"verdict": "OK", "plausible": True, "method": "mad",
                "reason": f"Cost basis {cost:g} is consistent with recent prices."}

    # (2) ratio vs current price (assumption-light fallback)
    px = _to_float(current_price)
    if px is not None and px > 0:
        ratio = cost / px
        if ratio > hi_ratio:
            return {"verdict": "BLOCKED", "plausible": False, "method": "ratio",
                    "reason": f"Cost basis {cost:g} is {ratio:.1f}x current price "
                              f"{px:g} — review cost basis."}
        if ratio < lo_ratio:
            return {"verdict": "BLOCKED", "plausible": False, "method": "ratio",
                    "reason": f"Cost basis {cost:g} is {ratio:.2f}x current price "
                              f"{px:g} — review cost basis."}
        return {"verdict": "OK", "plausible": True, "method": "ratio",
                "reason": f"Cost basis {cost:g} is {ratio:.2f}x current price {px:g}."}

    # nothing to compare against
    return {"verdict": "OK", "plausible": True, "method": "unknown",
            "reason": "Insufficient reference (no history or current price) to "
                      "validate cost basis."}


# =============================================================================
# 2) TRUST LEVEL  (the 5023 ghost gate)
# =============================================================================

def trust_level(row: Mapping[str, Any], *,
                field_map: Mapping[str, List[str]] = DEFAULT_FIELD_MAP,
                thresholds: Mapping[str, float] = DEFAULT_THRESHOLDS) -> Dict[str, Any]:
    """
    Classify a row's data trustworthiness and decide whether it may be ranked.

    Hard fails -> LOW -> excluded from ranking:
      * identity: name is blank or equals the symbol (5023.SR: no real name)
      * density:  a sparse / momentum-only-fallback flag is set
      * quality:  a present DQ score below `min_dq`
    Soft issues -> MEDIUM (kept, but flagged):
      * stale quote (age > max_age_hours), missing DQ, missing fundamentals
    Otherwise -> HIGH.

    Returns dict: level, exclude_from_ranking (bool), reasons (list[str]),
    signals (dict of the individual checks).
    """
    sym = _field(row, "symbol", field_map)
    name = _field(row, "name", field_map)
    dq = _to_float(_field(row, "dq_score", field_map))
    age = _to_float(_field(row, "age_hours", field_map))
    sparse = _field(row, "sparse_flag", field_map)
    funds = _field(row, "fundamentals_present", field_map)

    min_dq = float(thresholds.get("min_dq", DEFAULT_THRESHOLDS["min_dq"]))
    max_age = float(thresholds.get("max_age_hours", DEFAULT_THRESHOLDS["max_age_hours"]))

    reasons: List[str] = []
    hard_fail = False
    soft_issue = False

    # identity: only a fail if a name field resolved but is blank/== symbol
    identity_ok = True
    if name is not None:  # name column present
        if _is_blank(name) or _norm(name) == _norm(sym):
            identity_ok = False
            hard_fail = True
            reasons.append("No real security name (name blank or equals symbol).")

    # density / sparseness
    dense_ok = True
    if sparse is not None and _truthy(sparse):
        dense_ok = False
        hard_fail = True
        reasons.append("Sparse data / momentum-only fallback in use.")

    # data-quality floor
    dq_ok = True
    if dq is not None and dq < min_dq:
        dq_ok = False
        hard_fail = True
        reasons.append(f"Data-quality score {dq:g} below floor {min_dq:g}.")
    elif dq is None:
        soft_issue = True
        reasons.append("Data-quality score unavailable.")

    # freshness (soft)
    fresh_ok = True
    if age is not None and age > max_age:
        fresh_ok = False
        soft_issue = True
        reasons.append(f"Stale quote ({age:g}h > {max_age:g}h).")

    # fundamentals presence (soft)
    if funds is not None and not _truthy(funds):
        soft_issue = True
        reasons.append("Core fundamentals missing.")

    if hard_fail:
        level = TRUST_LOW
    elif soft_issue:
        level = TRUST_MEDIUM
    else:
        level = TRUST_HIGH
        reasons.append("Identity, density, freshness and data quality all pass.")

    return {
        "level": level,
        "exclude_from_ranking": level == TRUST_LOW,
        "reasons": reasons,
        "signals": {
            "identity_ok": identity_ok,
            "dense_ok": dense_ok,
            "dq_ok": dq_ok,
            "fresh_ok": fresh_ok,
        },
    }


def should_exclude_from_ranking(row_or_trust: Any, **kwargs: Any) -> bool:
    """Convenience: True if the row (or a precomputed trust dict) is LOW trust."""
    if isinstance(row_or_trust, dict) and "level" in row_or_trust \
            and "exclude_from_ranking" in row_or_trust:
        return bool(row_or_trust["exclude_from_ranking"])
    return bool(trust_level(row_or_trust, **kwargs)["exclude_from_ranking"])


# =============================================================================
# 3) COMBINED HOLDING GATE  (what portfolio_actions / the engine call)
# =============================================================================

def evaluate_holding(row: Mapping[str, Any], *,
                     field_map: Mapping[str, List[str]] = DEFAULT_FIELD_MAP,
                     thresholds: Mapping[str, float] = DEFAULT_THRESHOLDS,
                     history: Optional[Sequence[float]] = None) -> Dict[str, Any]:
    """
    One call for a portfolio holding: combine trust and cost-basis plausibility
    into an action gate the recommendation layer can apply directly.

    Returns dict:
        trust                 - the trust_level() dict
        cost_basis            - the cost_basis_plausibility() dict
        action_block          - True if any BUY/ADD/TRIM/SELL should be blocked
        recommendation_override - None | "REVIEW" (low trust) | "BLOCKED"
                                  (implausible cost basis)
        block_reason          - plain-language reason to surface on the row
    """
    trust = trust_level(row, field_map=field_map, thresholds=thresholds)
    cb = cost_basis_plausibility(
        _field(row, "avg_cost", field_map),
        _field(row, "current_price", field_map),
        quantity=_field(row, "quantity", field_map),
        history=history,
        hi_ratio=float(thresholds.get("hi_ratio", DEFAULT_THRESHOLDS["hi_ratio"])),
        lo_ratio=float(thresholds.get("lo_ratio", DEFAULT_THRESHOLDS["lo_ratio"])),
        mad_k=float(thresholds.get("mad_k", DEFAULT_THRESHOLDS["mad_k"])),
    )

    override: Optional[str] = None
    reason = ""
    if cb["verdict"] == "BLOCKED":
        override = "BLOCKED"
        reason = cb["reason"]
    elif trust["level"] == TRUST_LOW:
        override = "REVIEW"
        reason = "; ".join(trust["reasons"])

    return {
        "trust": trust,
        "cost_basis": cb,
        "action_block": override is not None,
        "recommendation_override": override,
        "block_reason": reason,
    }
