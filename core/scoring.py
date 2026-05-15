#!/usr/bin/env python3
# core/scoring.py
"""
================================================================================
Scoring Module — v5.3.0
(CANONICAL RECOMMENDATION SOURCE OF TRUTH — PRIORITY-EMITTING — PROVENANCE-TAGGED /
 [v5.2.9] CANONICAL-BUCKETS-ALIGNED (routes risk/confidence buckets through
 core.buckets v1.0.0 — last module in the stack to do so) /
 [PRESERVED v5.2.6] DATA_ENGINE_V2 v5.67.0 FRACTION-CONTRACT ALIGNMENT /
 [PRESERVED v5.2.5] CROSS-PROVIDER CONTRACT ALIGNMENT WITH v4.7.3 / v6.1.0 /
 v8.2.0 / v4.3.0 PROVIDERS + DATA_ENGINE_V2 v5.60.0 /
 [PRESERVED v5.2.4] POST-DEPLOY SYNTHESIZER-OVER-AGGRESSION GUARD /
 [PRESERVED v5.2.3] AUDIT-DRIVEN HARDENING / NO-FABRICATED-CONFIDENCE /
 RECOMMENDATION-COHERENCE-GUARDED / RISK-PENALTY-REBALANCED /
 REVENUE-COLLAPSE-AWARE QUALITY / UNIT-MISMATCH-SAFE FORECASTS /
 ILLIQUID-AWARE FORECAST SKIP /
 [PRESERVED] VIEW-COMPLETENESS HARDENED / VIEW-TOKENS-IN-EVERY-REASON /
 ACCUMULATE-ALIGNED / 5-TIER + CONVICTION FLOORS /
 INSIGHTS-INTEGRATED / PHANTOM-ROW-SAFE / SCHEMA-ALIGNED /
 v5.53.0 / v5.60.0 / v5.67.0 ENGINE-UNIT COMPATIBLE)
================================================================================

v5.2.9 changes (vs v5.2.8)  —  CANONICAL-BUCKETS ALIGNMENT
-----------------------------------------------------------
Routes scoring.py's `risk_bucket` and `confidence_bucket` helpers through
the canonical `core.buckets` v1.0.0 module, eliminating the last pocket of
divergent bucket math in the TFB Python stack.

THE DIVERGENCE
--------------
core.buckets v1.0.0 was established as the SINGLE authoritative bucket
helper, and over a sequence of releases the rest of the stack was routed
through it:
  - data_engine_v2          v5.70.0  (all 4 bucket assignment sites)
  - core/analysis/top10_selector  v4.13.0
  - core/investment_advisor (orchestrator)  v5.3.2
  - core/investment_advisor_engine          v4.4.2

scoring.py was the one module still carrying its OWN bucket math:
  - `risk_bucket`:        Title-case "Low" / "Moderate" / "High",
                          thresholds  <=35  /  <=65
  - `confidence_bucket`:  Title-case "High" / "Medium" / "Low",
                          thresholds  >=0.75  /  >=0.50  (0-1 fraction)

core.buckets is canonical UPPERCASE — "LOW" / "MODERATE" / "HIGH" for
risk (thresholds <35 / 35-70 / >=70) and "LOW" / "MODERATE" / "HIGH"
for confidence (note: middle tier "MODERATE", not "Medium"; 0-100-scale-
aware with auto-detection of 0-1 fraction input). So scoring.py's
`AssetScores.risk_bucket` / `AssetScores.confidence_bucket` row fields
disagreed with every other module's bucket fields — both in CASING and,
at the threshold boundaries, in TIER.

THE FIX (this file)
-------------------
PHASE 1 — except-guarded `core.buckets` import.
  New module-level guarded import block (right after
  RECOMMENDATION_SOURCE_TAG) binds `_bk_risk_bucket_from_score`,
  `_bk_confidence_bucket_from_score`, `_bk_normalize_risk_bucket`,
  `_bk_normalize_confidence_bucket`, and the `_BUCKETS_AVAILABLE` flag.
  This is the same except-guarded cross-module-dependency pattern used
  by data_engine_v2, top10_selector, and the investment_advisor pair.

PHASE 2 — `risk_bucket` / `confidence_bucket` rewritten.
  When `_BUCKETS_AVAILABLE`, both helpers route through core.buckets and
  emit the CANONICAL UPPERCASE bucket. When core.buckets is NOT
  importable, the LEGACY v5.2.8 inline thresholds run unchanged
  (PRESERVED VERBATIM) — a deployment shipping scoring.py without
  core/buckets.py sees ZERO behaviour change. The `None` -> `None`
  contract is preserved on BOTH paths (a None input short-circuits
  before either branch), and any unexpected core.buckets failure falls
  through to the legacy path rather than raising.

PHASE 3 — version bump + capability surface.
  Version 5.2.8 -> 5.2.9; this changelog block; new module-level
  `BUCKETS_CANONICAL` constant (True when the canonical path is live)
  exported in `__all__` so audit / diagnostic tooling can confirm the
  canonical bucket path is active.

[NOT TOUCHED — deliberately]
  - `derive_risk_view` is SEPARATE from `risk_bucket`: it builds the
    UPPERCASE view-prefix token ("LOW"/"MODERATE"/"HIGH") and is left
    byte-identical — it is not a bucket helper and routing it through
    core.buckets would be a double-transform.
  - All recommendation logic (`compute_recommendation`,
    `_compute_priority`, `normalize_recommendation_code`,
    `derive_canonical_recommendation`, `apply_canonical_recommendation`).
    scoring.py's 5-tier canonical recommendation set remains the
    established contract; the 5-tier-vs-8-tier reco_normalize question
    is out of scope for a bucket-alignment release.
  - `ScoringConfig.risk_low_threshold` / `risk_moderate_threshold` /
    `confidence_high` / `confidence_medium` are KEPT — they still drive
    the legacy fallback path and the env-tuning surface.

[PRESERVED — strictly] All v5.2.8 / v5.2.7 / v5.2.6 / v5.2.5 / v5.2.4 /
v5.2.3 / v5.2.2 / v5.2.1 / v5.2.0 helpers, signatures, dataclass fields,
behaviors, constants, and the entire prior narrative. `risk_bucket` and
`confidence_bucket` keep their exact signatures and were already in
`__all__`; the only `__all__` change is the additive `BUCKETS_CANONICAL`
export. Public API surface is strictly extended; no removals.

================================================================================

v5.2.8 changes (vs v5.2.7)  —  FIELD-NAME COLLISION FIX
--------------------------------------------------------
Renames the v5.2.7 canonical-priority row field to avoid a name collision
with the schema v2.7.0 / data_engine_v2 v5.60.0 Decision Matrix surface.

THE COLLISION
-------------
v5.2.7 emitted `row["recommendation_priority"]` as a string in P1..P5.
Schema v2.7.0 / data_engine_v2 v5.60.0 also emit a row field named
`recommendation_priority` — but as an int in 1..8, identifying which of
the 8-tier classifier's rules fired (`_classify_8tier`).

Both names landed in different waves; neither shipped to production
before this fix. Resolution: rename the v5.2.7 emission. The schema
v2.7.0 owner of the name was declared earlier and has a much larger
blast radius (schema_registry, data_engine_v2, Apps Script
04_Format.gs, audit_data_quality), so the v5.2.7 emission moves.

THE RENAME
----------
Field name: `recommendation_priority`  →  `recommendation_priority_band`
Semantic:   P1..P5 priority band (string)  — unchanged
Constant:   CANONICAL_PRIORITIES = ("P1", "P2", "P3", "P4", "P5") — unchanged
Source tag: "scoring.py v5.2.7"  →  "scoring.py v5.2.8" (auto-tracked
            via f-string against __version__).

Sites updated (11 occurrences):
  - AssetScores dataclass field declaration
  - derive_canonical_recommendation read site
  - apply_canonical_recommendation patch dict key
  - compute_scores AssetScores construction kwarg
  - score_and_rank_rows post-insights re-sync write
  - All references in module docstrings and inline comments

No semantic or threshold changes. No other behaviour changes from
v5.2.7. Idempotency check still uses RECOMMENDATION_SOURCE_TAG —
which auto-bumps to "scoring.py v5.2.8" — so freshly-scored rows
match the new constant. Any caller still reading `recommendation`
(the 5-tier canonical code) is unaffected; `recommendation_reason`
unchanged.

DOWNSTREAM IMPACT
-----------------
- core.scoring_engine bridge v3.4.3 → v3.4.4 (re-exports the new
  field name; updates `_V527_CASCADE_BRIDGE_FIELDS` →
  `_V528_CASCADE_BRIDGE_FIELDS`).
- Apps Script 04_Format.gs: reads `recommendation_priority_band`
  from row dict to build the `P# [VERDICT]:` Detail string.
- audit_data_quality / Diagnostic_Report: any grouping by priority
  reads `recommendation_priority_band` for the P1..P5 view; the
  separate `recommendation_priority` int (1..8) from schema v2.7.0
  remains available for rule-which-fired audits.
- investment_advisor_engine (v3.5.0 → v3.6.0, pending): consumes
  `apply_canonical_recommendation` which now writes the new key.
- data_engine_v2._compute_recommendation (pending v5.61.0 update):
  same.

[PRESERVED VERBATIM] All other v5.2.7 surface: derive_canonical_
recommendation, apply_canonical_recommendation, CANONICAL_PRIORITIES,
PRIO_P1..PRIO_P5, RECOMMENDATION_SOURCE_TAG, _compute_priority. All
v5.2.6 and earlier features. The `_V527_CASCADE_BRIDGE_FIELDS`
constant in the scoring_engine bridge becomes `_V528_*` to track the
v5.2.8 row-field contract.

================================================================================

v5.2.7 changes (vs v5.2.6)  —  CANONICAL RECOMMENDATION SOURCE OF TRUTH
-----------------------------------------------------------------------
Closes the structural cascade-discrepancy surfaced by the May 13 2026
audit of the deployed Top_10_Investments / Market_Leaders snapshot.

THE BUG
-------
Two recommendation engines were running concurrently in production with
DIFFERENT thresholds, writing DIFFERENT fields on the same row:

  - scoring.py.compute_recommendation()             writes top-line
                                                    `Recommendation`
  - investment_advisor_engine._recommendation_from_scores() writes the
                                                    `[VERDICT]` token
                                                    inside `Recommendation
                                                    Detail`

The audit on a 140-row snapshot found 24 rows (≈17%) where the two
engines disagreed:
  - EXE.US: Recommendation=REDUCE, Detail="P2 [BUY]: Fund STRONG | ..."
  - TREE.US: Recommendation=REDUCE, Detail="P2 [STRONG_BUY]: ..."
  - WPM.TO / WPM.US / AEM.US / KGC.US / NLY.US / RDN.US / RRC.US / BRO.US /
    TSM.US / NVO.US / PAGS.US / STNE.US / FIS.US / GPOR.US / CNX.US / SD.US /
    DELTA.BK / NVO.US — same pattern.

End-user-facing credibility killer — same row shows two contradictory
verdicts.

THE FIX (this file)
-------------------
Make scoring.py the SINGLE SOURCE OF TRUTH by emitting two new fields
that downstream consumers can read from instead of recomputing:

  - recommendation_priority_band  (P1..P5, computed centrally — see
                              `_compute_priority`)
  - recommendation_source    (provenance tag — "scoring.py v5.2.7")

Plus two public helpers for downstream engines to consume:

  - derive_canonical_recommendation(row, *, settings=None)
        -> (recommendation, reason, priority)
    Reads scores already on the row and derives the canonical triple.
    Lightweight — does NOT re-run the full compute_scores pipeline
    (no insights_builder, no sector batch pass).

  - apply_canonical_recommendation(row, *, settings=None, overwrite=False)
        -> patch dict
    Drop-in replacement for legacy `_compute_recommendation` calls
    in data_engine_v2 and `_recommendation_from_scores` calls in
    investment_advisor_engine. The `overwrite=False` mode is
    idempotent — returns {} if the row already carries the v5.2.7
    provenance tag.

THE PRIORITY MODEL
------------------
Aligned with the priority bucketing already in the production Detail
strings (P2..P4 visible in the snapshot) and with the audit module's
`AlertPriority` enum:

  P1 = Critical action: STRONG_SELL, or REDUCE/SELL on broken/illiquid row
       (risk≥90 + conf<45, or risk≥85 + overall<30)
  P2 = STRONG_BUY, OR high-conviction BUY (overall≥80 + conf≥70)
  P3 = BUY (normal conviction)
  P4 = HOLD (default)
  P5 = SELL or low-priority REDUCE

DOWNSTREAM SEQUENCING (delivered in follow-ups)
-----------------------------------------------
This file alone won't fix the 24 inconsistent rows yet — the
investment_advisor_engine still recomputes its own [VERDICT] string.
Two follow-up files complete the fix:

  1. investment_advisor_engine.py — replace `_recommendation_from_scores`
     with `scoring.apply_canonical_recommendation(row, overwrite=False)`.
     Kills the parallel "Strong Buy / Buy / Hold / Avoid" ladder with
     its different thresholds; the Detail's [VERDICT] becomes identical
     to the top-line.
  2. data_engine_v2.py — replace `_compute_recommendation` with the
     same call. Removes the ACCUMULATE 4-bucket logic and the
     conflicting threshold math.

[PRESERVED — strictly] All v5.2.6 / v5.2.5 / v5.2.4 / v5.2.3 / v5.2.2 /
v5.2.1 / v5.2.0 helpers, signatures, dataclass fields, behaviors,
constants, and the entire prior narrative. The new fields are appended
at the end of AssetScores (additive only). The new helpers are
additive. The existing `compute_recommendation` logic — including the
view-prefix builder, the coherence guard, the reco_normalize
delegation, and the alignment pass — is byte-identical. Public API
surface is strictly extended; no removals from __all__.

================================================================================

v5.2.6 changes (vs v5.2.5)  —  DATA_ENGINE_V2 v5.67.0 FRACTION-CONTRACT ALIGNMENT
---------------------------------------------------------------------------------
[PRESERVED — see prior file header for full narrative]
Closes one defensive-shape-detection gap surfaced by the May 13 2026
data_engine_v2 v5.67.0 unit-contract alignment release. New helper
`_as_upside_fraction` with shape detection by magnitude. Applied at
two call sites in `derive_upside_pct` and `compute_scores` upside-
suppression tracking. Idempotent across both shapes.

================================================================================

v5.2.5 changes (vs v5.2.4)  —  CROSS-PROVIDER CONTRACT ALIGNMENT
-----------------------------------------------------------------
[PRESERVED — see prior file header for full narrative]
Engine-applied valuation clear surfacing, last_error_class surfacing,
defensive _as_pct_position_fraction shape detection, warnings-string-
aware unforecastable detection, _warning_tags_from_row helper.

================================================================================

v5.2.4 changes (vs v5.2.3)  —  POST-DEPLOY SYNTHESIZER-OVER-AGGRESSION GUARD
----------------------------------------------------------------------------
[PRESERVED — see prior file header for full narrative]
SYNTHESIS CEILING guard, TWO-SIDED SUSPECT BOUNDS on upside_pct,
guarded upside computation in compute_valuation_score, error tracking
for suspect upsides.

================================================================================

v5.2.3 changes (vs v5.2.2)  —  AUDIT-DRIVEN HARDENING
-----------------------------------------------------
[PRESERVED — see prior file header for full narrative]
NO-FABRICATED-CONFIDENCE, RECOMMENDATION-COHERENCE-GUARD, RISK-PENALTY
rebalance (0.55 -> 0.40), REVENUE-COLLAPSE-AWARE quality haircut,
UNIT-MISMATCH SANITY guard, ILLIQUID-AWARE EARLY EXIT.

================================================================================

v5.2.2 / v5.2.1 / v5.2.0 / v5.1.0 changes
------------------------------------------
[PRESERVED — see prior file headers for full narratives]
Defensive shape helpers, view-prefix builder, view-completeness
hardening, canonical recommendation alignment, insights_builder
integration.

WHY v5.3.0 — v5.71.0 COMPATIBILITY HOTFIX + STRICT SCORING CONTRACT
--------------------------------------------------------------------------------
This release is both a compatibility hotfix and a scoring-contract hardening
pass. It is required because data_engine_v2 v5.71.0 imports private helper
names (`_recommendation`, `_risk_bucket`, `_confidence_bucket`) while
scoring.py v5.2.9 exposed only the public names (`compute_recommendation`,
`risk_bucket`, `confidence_bucket`). v5.3.0 exports BOTH naming conventions
and points them to the same internal logic so the v5.71.0 engine no longer
falls back to conservative HOLD recommendations.

Bug/contract fixes covered in v5.3.0:
  A. private/public helper compatibility for data_engine_v2 v5.71.0.
  B. structured recommendation_reason format with env-controlled legacy mode.
  C. final recommendation enum closed to STRONG_BUY/BUY/HOLD/REDUCE/SELL/
     STRONG_SELL; ACCUMULATE remains an input alias only and is never output.
  D. high-risk rows (e.g. risk≈77 and overall<78) cannot escape into HOLD.
  E. bucket thresholds are module-level constants and bucket helpers are scale
     safe.
  F. insufficient scoring inputs return nullable overall_score by default, with
     TFB_SCORING_NULLABLE_OVERALL=false emergency rollback to legacy 50.0.
  G. opportunity_source exposes whether opportunity_score is ROI-based, a
     valuation/momentum fallback, or insufficient.
  H. scoring_schema_version is emitted for cache invalidation.
  I. scoring_errors remains attached to every output row and includes forecast
     errors.
  J. diagnostic logs [v5.3.0 SCORE], [v5.3.0 INSUFFICIENT], and
     [v5.3.0 BUCKET_MISMATCH] make silent fallbacks visible.

[PRESERVED] The existing v5.2.9/v5.2.8/v5.2.7 history and all public entry
point signatures remain intact. New fields are additive and have safe defaults.

================================================================================

Public API (extended in v5.3.0):
  All v5.2.8 exports preserved. New in v5.2.9: BUCKETS_CANONICAL.
  (v5.2.7 added: derive_canonical_recommendation,
  apply_canonical_recommendation, RECOMMENDATION_SOURCE_TAG,
  CANONICAL_PRIORITIES, PRIO_P1..PRIO_P5.)
================================================================================
"""

from __future__ import annotations

import logging
import math
import os
import re
from dataclasses import asdict, dataclass, field, replace
from datetime import datetime, timedelta, timezone
from enum import Enum
from typing import (
    Any,
    Dict,
    List,
    Mapping,
    Optional,
    Sequence,
    Set,
    Tuple,
)

logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())

# =============================================================================
# Version
# =============================================================================

__version__ = "5.3.0"
SCORING_VERSION = __version__

# v5.2.7: provenance tag emitted by compute_scores + apply_canonical_recommendation.
# Downstream engines (investment_advisor_engine, data_engine_v2) read this to
# decide whether a row has already been canonically scored (idempotency guard).
RECOMMENDATION_SOURCE_TAG = f"scoring.py v{__version__}"

# =============================================================================
# v5.3.0 — Canonical scoring contract constants / env controls
# =============================================================================

# v5.3.0 FIX: Close the final recommendation enum. ACCUMULATE is accepted only
# as a legacy input alias and is never emitted as final output.
RECOMMENDATION_ENUM: Tuple[str, ...] = (
    "STRONG_BUY", "BUY", "HOLD", "REDUCE", "SELL", "STRONG_SELL",
)

# v5.3.0 FIX: Bucket thresholds are declarative constants so downstream code
# can test/inspect them instead of duplicating magic numbers.
RISK_BUCKET_THRESHOLDS: Tuple[float, float] = (30.0, 60.0)
CONFIDENCE_BUCKET_THRESHOLDS: Tuple[float, float] = (0.45, 0.75)

# v5.3.0 FIX: Explicit output precision contract.
SCORE_PRECISION = 2
FRACTION_PRECISION = 4
ROI_PRECISION = 6
PENALTY_PRECISION = 4


def _env_bool_v530(name: str, default: bool = False) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    s = str(raw).strip().lower()
    if s in {"1", "true", "yes", "y", "on", "t"}:
        return True
    if s in {"0", "false", "no", "n", "off", "f"}:
        return False
    return default


def _env_text_v530(name: str, default: str = "") -> str:
    raw = os.getenv(name)
    if raw is None:
        return default
    text = str(raw).strip()
    return text if text else default


def _nullable_overall_enabled() -> bool:
    # v5.3.0 FIX: New safer default; false is emergency rollback to v5.2.x
    # fabricated 50.0 overall score for insufficient inputs.
    return _env_bool_v530("TFB_SCORING_NULLABLE_OVERALL", True)


def _strict_buckets_enabled() -> bool:
    return _env_bool_v530("TFB_SCORING_STRICT_BUCKETS", True)


def _structured_reason_enabled() -> bool:
    return _env_text_v530("TFB_SCORING_REASON_FORMAT", "structured").lower() != "legacy"


def _fmt_score_component(value: Optional[float]) -> str:
    if value is None:
        return "NA"
    try:
        return f"{float(value):.1f}"
    except Exception:
        return "NA"


def _fmt_roi3_component(value: Optional[float]) -> str:
    if value is None:
        return "NA"
    try:
        v = float(value)
        if abs(v) > 1.5:
            return f"{v:.1f}"
        return f"{v * 100.0:.1f}"
    except Exception:
        return "NA"


def _format_recommendation_reason(
    rec: str,
    prose: str,
    overall: Optional[float],
    risk: Optional[float],
    confidence100: Optional[float],
    roi3: Optional[float],
) -> str:
    # v5.3.0 FIX: Single structured, parseable reason format used by the
    # authoritative scoring layer. Legacy prose-only mode is retained solely
    # for emergency rollback.
    canonical = str(rec or "HOLD").upper().replace(" ", "_")
    text = str(prose or "No explanation available.").strip()
    if not _structured_reason_enabled():
        return text
    return (
        f"{canonical}: {text} | "
        f"overall={_fmt_score_component(overall)} "
        f"risk={_fmt_score_component(risk)} "
        f"conf={_fmt_score_component(confidence100)} "
        f"roi3m={_fmt_roi3_component(roi3)}%"
    )


def _expected_risk_bucket(score: Optional[float]) -> Optional[str]:
    if score is None:
        return None
    try:
        s = float(score)
        if 0.0 <= abs(s) <= 1.5:
            s *= 100.0
    except Exception:
        return None
    low, moderate = RISK_BUCKET_THRESHOLDS
    if s <= low:
        return "LOW"
    if s <= moderate:
        return "MODERATE"
    return "HIGH"


def _expected_confidence_bucket(value: Optional[float]) -> Optional[str]:
    if value is None:
        return None
    try:
        v = float(value)
        if abs(v) > 1.5:
            v /= 100.0
        v = max(0.0, min(1.0, v))
    except Exception:
        return None
    low_to_moderate, high = CONFIDENCE_BUCKET_THRESHOLDS
    if v >= high:
        return "HIGH"
    if v >= low_to_moderate:
        return "MODERATE"
    return "LOW"


def _log_bucket_mismatch(field: str, score: Optional[float], bucket: Optional[str], expected: Optional[str]) -> None:
    # v5.3.0 FIX: Makes threshold drift visible without raising in production.
    if not _strict_buckets_enabled():
        return
    if expected and bucket and str(bucket).upper() != str(expected).upper():
        logger.warning(
            "[v5.3.0 BUCKET_MISMATCH] field=%s score=%s bucket=%s expected=%s",
            field, score, bucket, expected,
        )

# =============================================================================
# v5.2.9 — core.buckets integration  (NEW)
# =============================================================================
#
# v5.2.9 routes scoring.py's `risk_bucket` and `confidence_bucket` helpers
# through the canonical core.buckets v1.0.0 module — the single authoritative
# bucket helper that data_engine_v2 v5.70.0, top10_selector v4.13.0, the
# investment_advisor orchestrator v5.3.2, and the investment_advisor engine
# v4.4.2 already route through. Before v5.2.9, scoring.py carried its own
# divergent bucket math (Title-case "Low"/"Moderate"/"High", confidence
# "High"/"Medium"/"Low", thresholds <=35 / <=65) — the last module in the
# stack with non-canonical bucket logic.
#
# The import is except-guarded exactly like every other cross-module
# dependency in this codebase: when core.buckets is importable, scoring.py
# emits the CANONICAL UPPERCASE buckets; when it is NOT importable (a
# deployment that ships scoring.py without core/buckets.py), the LEGACY
# v5.2.8 inline thresholds are preserved verbatim as the fallback, so such
# a deployment sees ZERO behaviour change. `_BUCKETS_AVAILABLE` records
# which path is live and is surfaced via the module-level
# `BUCKETS_CANONICAL` capability constant.
try:
    from core.buckets import (  # noqa: WPS433
        risk_bucket_from_score as _bk_risk_bucket_from_score,
        confidence_bucket_from_score as _bk_confidence_bucket_from_score,
        normalize_risk_bucket as _bk_normalize_risk_bucket,
        normalize_confidence_bucket as _bk_normalize_confidence_bucket,
    )
    _BUCKETS_AVAILABLE = True
except Exception:  # ImportError or any partial-install failure
    try:
        from buckets import (  # noqa: WPS433
            risk_bucket_from_score as _bk_risk_bucket_from_score,
            confidence_bucket_from_score as _bk_confidence_bucket_from_score,
            normalize_risk_bucket as _bk_normalize_risk_bucket,
            normalize_confidence_bucket as _bk_normalize_confidence_bucket,
        )
        _BUCKETS_AVAILABLE = True
    except Exception:
        _bk_risk_bucket_from_score = None  # type: ignore
        _bk_confidence_bucket_from_score = None  # type: ignore
        _bk_normalize_risk_bucket = None  # type: ignore
        _bk_normalize_confidence_bucket = None  # type: ignore
        _BUCKETS_AVAILABLE = False

# v5.2.9: capability constant — True when risk_bucket/confidence_bucket are
# routing through canonical core.buckets, False when the legacy v5.2.8
# inline-threshold fallback is live. Exported in __all__ so downstream
# audit/diagnostic tooling can confirm the canonical bucket path is active.
BUCKETS_CANONICAL: bool = _BUCKETS_AVAILABLE

# =============================================================================
# Time Helpers
# =============================================================================

_UTC = timezone.utc
_RIYADH_TZ = timezone(timedelta(hours=3))


def _utc_iso(dt: Optional[datetime] = None) -> str:
    d = dt or datetime.now(_UTC)
    if d.tzinfo is None:
        d = d.replace(tzinfo=_UTC)
    return d.astimezone(_UTC).isoformat()


def _riyadh_iso(dt: Optional[datetime] = None) -> str:
    if dt is None:
        return datetime.now(_RIYADH_TZ).isoformat()
    d = dt
    if d.tzinfo is None:
        d = d.replace(tzinfo=_UTC)
    return d.astimezone(_RIYADH_TZ).isoformat()


# =============================================================================
# Enums (preserved)
# =============================================================================

class Horizon(str, Enum):
    DAY = "day"
    WEEK = "week"
    MONTH = "month"
    LONG = "long"


class Signal(str, Enum):
    STRONG_BUY = "STRONG_BUY"
    BUY = "BUY"
    HOLD = "HOLD"
    REDUCE = "REDUCE"
    SELL = "SELL"


class RSISignal(str, Enum):
    OVERSOLD = "Oversold"
    NEUTRAL = "Neutral"
    OVERBOUGHT = "Overbought"
    N_A = "N/A"


# =============================================================================
# v5.2.7 — Priority enum (NEW)
# =============================================================================
#
# Aligned with audit_data_quality.AlertPriority (P1..P5) so downstream
# audit / diagnostic correlation can compare against the same labels.
# Also matches the priority bucketing already visible in the production
# `Recommendation Detail` strings (P2..P4 seen in the May 13 snapshot).

PRIO_P1 = "P1"   # Critical action: STRONG_SELL / broken-row REDUCE
PRIO_P2 = "P2"   # STRONG_BUY or high-conviction BUY
PRIO_P3 = "P3"   # BUY (normal conviction)
PRIO_P4 = "P4"   # HOLD (default)
PRIO_P5 = "P5"   # SELL or low-priority REDUCE

CANONICAL_PRIORITIES: Tuple[str, ...] = (
    PRIO_P1, PRIO_P2, PRIO_P3, PRIO_P4, PRIO_P5,
)
_CANONICAL_PRIORITIES_SET: Set[str] = set(CANONICAL_PRIORITIES)


# =============================================================================
# Custom Exceptions (preserved)
# =============================================================================

class ScoringError(Exception):
    pass


class InvalidHorizonError(ScoringError):
    pass


class MissingDataError(ScoringError):
    pass


# =============================================================================
# v5.2.3 — Audit-driven thresholds  (PRESERVED)
# =============================================================================
#
# Centralising these here so a future ops-side tuning round can move
# them via env vars without touching the function bodies.

# Recommendation-coherence guard. Rows where reco_normalize said
# BUY/STRONG_BUY but the row's own roi3 is below this threshold AND
# confidence is below the confidence threshold get downgraded to HOLD.
_COHERENCE_ROI3_FLOOR_FRACTION = -0.02   # -2% over 3 months
_COHERENCE_CONFIDENCE_FLOOR = 65.0       # 65% AI confidence

# Forecast unit-mismatch guard. roi12 values below this get suppressed
# as suspected unit-conversion bugs. -50% is deliberately conservative.
_FORECAST_ROI12_UNIT_MISMATCH_FLOOR = -0.50

# Quality revenue-collapse haircut. Linear ramp from "no penalty" at
# the start threshold to "max penalty" at the floor threshold. The
# haircut multiplies the final 0-1 combined quality score.
_QUALITY_REVENUE_COLLAPSE_START = -0.30   # Start applying haircut at -30% YoY
_QUALITY_REVENUE_COLLAPSE_FLOOR = -0.75   # Floor (max haircut) at -75% YoY
_QUALITY_REVENUE_COLLAPSE_MAX_HAIRCUT = 0.55  # Min multiplier (1.0 = no penalty)

# Confidence non-fabrication thresholds. Below all three, return None
# instead of synthesizing.
_CONFIDENCE_FALLBACK_MIN_COMPLETENESS = 0.40
_CONFIDENCE_FALLBACK_MIN_PROVIDERS = 1


# =============================================================================
# v5.2.7 — Priority assignment thresholds  (NEW)
# =============================================================================
#
# Centralised so the priority model can be tuned without touching
# `_compute_priority` body.

# P2 vs P3 split for BUY recommendations: high-conviction BUY (overall ≥ 80
# AND confidence ≥ 70) gets P2, normal BUY gets P3.
_PRIORITY_BUY_HIGH_CONVICTION_OVERALL_FLOOR = 80.0
_PRIORITY_BUY_HIGH_CONVICTION_CONFIDENCE_FLOOR = 70.0

# P1 vs P5 split for REDUCE: REDUCE on a broken row (risk ≥ 90 AND
# confidence < 45) escalates to P1 (urgent action). Otherwise P5.
_PRIORITY_REDUCE_CRITICAL_RISK_FLOOR = 90.0
_PRIORITY_REDUCE_CRITICAL_CONFIDENCE_CEILING = 45.0

# P1 vs P5 split for SELL: SELL on a deeply-broken row (risk ≥ 85 AND
# overall < 30) escalates to P1. Otherwise P5.
_PRIORITY_SELL_CRITICAL_RISK_FLOOR = 85.0
_PRIORITY_SELL_CRITICAL_OVERALL_CEILING = 30.0


# =============================================================================
# v5.2.4 — Synthesizer-over-aggression guard  (PRESERVED)
# =============================================================================

# Forecast-synthesis CEILING guard (POSITIVE side, v5.2.4).
_FORECAST_ROI12_SYNTHESIS_CEILING = 2.00   # +200% over 12 months

# Two-sided suspect bounds for the user-visible upside_pct field.
# v5.2.6 NOTE: These bounds are FRACTION-FORM, aligned with the
# data_engine_v2 v5.67.0 emit contract.
_UPSIDE_PCT_SUSPECT_FLOOR = -0.90      # -90% (fraction form)
_UPSIDE_PCT_SUSPECT_CEILING = 2.00     # +200% (fraction form)


def _is_upside_suspect(upside: Optional[float]) -> bool:
    """
    v5.2.4: Returns True when an upside_pct value falls outside the
    data-quality sanity bounds.

    v5.2.6 NOTE: The bounds are FRACTION-FORM, aligned with the
    data_engine_v2 v5.67.0 emit contract (upside_pct stored as
    fraction). When the caller may have POINTS-form input (pre-v5.67.0
    engine row, legacy snapshot, test fixture, third-party importer),
    use `_as_upside_fraction()` to shape-normalize BEFORE calling
    this function. The two existing call sites in scoring.py
    (`derive_upside_pct` and `compute_valuation_score`) have been
    updated to do this in v5.2.6.
    """
    if upside is None:
        return False
    return (
        upside < _UPSIDE_PCT_SUSPECT_FLOOR
        or upside > _UPSIDE_PCT_SUSPECT_CEILING
    )


# =============================================================================
# v5.2.5 — Cross-provider contract alignment  (PRESERVED)
# =============================================================================

_PROVIDER_ERROR_CLASSES_KNOWN: Set[str] = {
    "AuthError",
    "IpBlocked",
    "RateLimited",
    "NotFound",
    "NetworkError",
    "InvalidPayload",
    "FetchError",
    "InvalidSymbol",
    "MissingApiKey",
    "KsaBlocked",
}

_ENGINE_DROPPED_VALUATION_TAGS: Set[str] = {
    "intrinsic_unit_mismatch_suspected",
    "upside_synthesis_suspect",
    "engine_52w_high_unit_mismatch_dropped",
    "engine_52w_low_unit_mismatch_dropped",
    "engine_52w_high_low_inverted",
}

_ENGINE_UNFORECASTABLE_TAGS: Set[str] = {
    "forecast_unavailable",
    "forecast_unavailable_no_source",
    "forecast_cleared_consistency_sweep",
    "forecast_skipped_unavailable",
}


def _warning_tags_from_row(row: Mapping[str, Any]) -> Set[str]:
    """
    v5.2.5: Parse the row's `warnings` field into a Set[str] of
    normalized tags. Handles None / empty / "; "-joined string /
    list-or-tuple input. Each tag is stripped of whitespace. Empty
    parts after split are dropped. Used by
    _row_engine_dropped_valuation() and the warnings-aware branch of
    _is_row_unforecastable().
    """
    if row is None:
        return set()
    raw = row.get("warnings") if isinstance(row, Mapping) else None
    if raw is None:
        return set()

    parts: List[str] = []
    if isinstance(raw, str):
        for piece in raw.split(";"):
            s = piece.strip()
            if s:
                parts.append(s)
    elif isinstance(raw, (list, tuple, set)):
        for item in raw:
            if item is None:
                continue
            try:
                s = str(item).strip()
            except Exception:
                continue
            if s:
                parts.append(s)
    else:
        try:
            s = str(raw).strip()
        except Exception:
            s = ""
        if s:
            parts.append(s)

    out: Set[str] = set()
    for p in parts:
        out.add(p)
        if ":" in p:
            bare = p.split(":", 1)[0].strip()
            if bare:
                out.add(bare)
    return out


def _row_engine_dropped_valuation(row: Mapping[str, Any]) -> bool:
    """
    v5.2.5: Returns True when the row's warnings string indicates
    the engine (data_engine_v2 v5.60.0 Phase H / I / P) dropped
    intrinsic_value or upside_pct due to unit-mismatch or
    synthesizer-overshoot detection upstream.
    """
    if row is None:
        return False
    tags = _warning_tags_from_row(row)
    if not tags:
        return False
    return bool(tags & _ENGINE_DROPPED_VALUATION_TAGS)


# =============================================================================
# Configuration
# =============================================================================

@dataclass(frozen=True)
class ScoringConfig:
    day_threshold: int = 5
    week_threshold: int = 14
    month_threshold: int = 90

    default_valuation: float = 0.30
    default_momentum: float = 0.25
    default_quality: float = 0.20
    default_growth: float = 0.15
    default_opportunity: float = 0.10
    default_technical: float = 0.00

    # v5.2.3: lowered from 0.55 to 0.40 to address audit finding #3
    # (REDUCE-heavy distribution).
    risk_penalty_strength: float = 0.40

    confidence_penalty_strength: float = 0.45

    confidence_high: float = 0.75
    confidence_medium: float = 0.50

    risk_low_threshold: float = 35.0
    risk_moderate_threshold: float = 65.0

    @classmethod
    def from_env(cls) -> "ScoringConfig":
        def _env_float(name: str, default: float) -> float:
            try:
                return float(os.getenv(name, str(default)))
            except Exception:
                return default

        def _env_int(name: str, default: int) -> int:
            try:
                return int(os.getenv(name, str(default)))
            except Exception:
                return default

        return cls(
            day_threshold=_env_int("SCORING_DAY_THRESHOLD", 5),
            week_threshold=_env_int("SCORING_WEEK_THRESHOLD", 14),
            month_threshold=_env_int("SCORING_MONTH_THRESHOLD", 90),
            default_valuation=_env_float("SCORING_W_VALUATION", 0.30),
            default_momentum=_env_float("SCORING_W_MOMENTUM", 0.25),
            default_quality=_env_float("SCORING_W_QUALITY", 0.20),
            default_growth=_env_float("SCORING_W_GROWTH", 0.15),
            default_opportunity=_env_float("SCORING_W_OPPORTUNITY", 0.10),
            default_technical=_env_float("SCORING_W_TECHNICAL", 0.00),
            risk_penalty_strength=_env_float("SCORING_RISK_PENALTY", 0.40),
            confidence_penalty_strength=_env_float("SCORING_CONFIDENCE_PENALTY", 0.45),
        )


_CONFIG = ScoringConfig.from_env()

# =============================================================================
# Horizon Thresholds (preserved)
# =============================================================================

_HORIZON_DAYS_CUTOFFS: Tuple[Tuple[int, Horizon], ...] = (
    (_CONFIG.day_threshold, Horizon.DAY),
    (_CONFIG.week_threshold, Horizon.WEEK),
    (_CONFIG.month_threshold, Horizon.MONTH),
)


# =============================================================================
# v5.2.0 — View / recommendation hardening constants (PRESERVED)
# =============================================================================

_CANONICAL_REC_LABELS_SET: Set[str] = set(RECOMMENDATION_ENUM)

_CANONICAL_REC_ALIASES: Dict[str, str] = {
    "ACCUMULATE": "BUY",
    "ADD": "BUY",
    "OUTPERFORM": "BUY",
    "OVERWEIGHT": "BUY",
    "STRONGBUY": "STRONG_BUY",
    "MARKET_PERFORM": "HOLD",
    "MARKET PERFORM": "HOLD",
    "NEUTRAL": "HOLD",
    "WATCH": "HOLD",
    "MAINTAIN": "HOLD",
    "TRIM": "REDUCE",
    "UNDERWEIGHT": "REDUCE",
    "AVOID": "SELL",
    "EXIT": "SELL",
    "UNDERPERFORM": "SELL",
    "STRONG_SELL": "STRONG_SELL",
    "STRONGSELL": "STRONG_SELL",
    "STRONG SELL": "STRONG_SELL",
}

_TRAILING_ARROW_RE = re.compile(
    r'(\s*(?:\u2192|->|=>|\bTHEN\b)\s*)'
    r'('
    r'STRONG[\s_]?BUY|BUY|ACCUMULATE|ADD|OUTPERFORM|OVERWEIGHT'
    r'|HOLD|NEUTRAL|MAINTAIN|WATCH|MARKET[\s_]?PERFORM'
    r'|REDUCE|TRIM|UNDERWEIGHT'
    r'|SELL|AVOID|EXIT|UNDERPERFORM|STRONG[\s_]?SELL'
    r')'
    r'\s*\.?\s*$',
    re.IGNORECASE,
)

_LEGACY_LABEL_PATTERNS: List[Tuple[re.Pattern, str]] = [
    (
        re.compile(
            r'(?<![A-Za-z0-9_])' + re.escape(legacy) + r'(?![A-Za-z0-9_])',
            re.IGNORECASE,
        ),
        canonical,
    )
    for legacy, canonical in _CANONICAL_REC_ALIASES.items()
]


def _view_or_na(view: Optional[str]) -> str:
    if view is None:
        return "N/A"
    s = str(view).strip()
    if not s:
        return "N/A"
    return s


def _build_view_prefix(
    fundamental: Optional[str],
    technical: Optional[str],
    risk: Optional[str],
    value: Optional[str],
) -> str:
    return (
        "Fund: " + _view_or_na(fundamental)
        + " | Tech: " + _view_or_na(technical)
        + " | Risk: " + _view_or_na(risk)
        + " | Val: " + _view_or_na(value)
    )


def _align_reason_to_canonical_recommendation(
    reason: Optional[str],
    canonical_rec: Optional[str],
) -> str:
    if not reason:
        return reason or ""

    text = str(reason)

    if not canonical_rec:
        canonical_rec = "HOLD"

    for pattern, canonical_label in _LEGACY_LABEL_PATTERNS:
        if canonical_label == "HOLD" and pattern.pattern.upper().find('NEUTRAL') > -1:
            continue
        text = pattern.sub(canonical_label, text)

    match = _TRAILING_ARROW_RE.search(text)
    if match:
        arrow_part = match.group(1)
        existing_label_raw = match.group(2).strip().upper().replace(' ', '_')

        if existing_label_raw in _CANONICAL_REC_LABELS_SET:
            existing_canonical = existing_label_raw
        else:
            existing_canonical = _CANONICAL_REC_ALIASES.get(
                existing_label_raw,
                _CANONICAL_REC_ALIASES.get(
                    existing_label_raw.replace('_', ' '),
                    None,
                ),
            )

        if (
            existing_canonical is not None
            and existing_canonical in _CANONICAL_REC_LABELS_SET
            and existing_label_raw != canonical_rec
        ):
            text = text[: match.start()] + arrow_part + canonical_rec

    return text


def score_views_completeness(
    row_or_scores: Mapping[str, Any],
) -> Dict[str, Any]:
    """
    v5.2.0: Per-row completeness summary for the four view fields.
    Treats None / "" / "N/A" as MISSING.
    """
    fields = (
        "fundamental_view",
        "technical_view",
        "risk_view",
        "value_view",
    )
    present = 0
    missing: List[str] = []
    for f in fields:
        v = row_or_scores.get(f) if isinstance(row_or_scores, Mapping) else None
        if v is None:
            missing.append(f)
            continue
        s = str(v).strip().upper()
        if not s or s == "N/A":
            missing.append(f)
            continue
        present += 1
    total = len(fields)
    ratio = present / total if total else 0.0
    return {
        "present": present,
        "total": total,
        "ratio": round(ratio, 4),
        "missing": missing,
    }


# =============================================================================
# Data Classes
# =============================================================================

@dataclass(slots=True)
class ScoreWeights:
    w_valuation: float = _CONFIG.default_valuation
    w_momentum: float = _CONFIG.default_momentum
    w_quality: float = _CONFIG.default_quality
    w_growth: float = _CONFIG.default_growth
    w_opportunity: float = _CONFIG.default_opportunity
    w_technical: float = _CONFIG.default_technical
    risk_penalty_strength: float = _CONFIG.risk_penalty_strength
    confidence_penalty_strength: float = _CONFIG.confidence_penalty_strength

    def normalize(self) -> "ScoreWeights":
        total = (self.w_valuation + self.w_momentum + self.w_quality +
                 self.w_growth + self.w_opportunity + self.w_technical)
        if total > 0:
            return ScoreWeights(
                w_valuation=self.w_valuation / total,
                w_momentum=self.w_momentum / total,
                w_quality=self.w_quality / total,
                w_growth=self.w_growth / total,
                w_opportunity=self.w_opportunity / total,
                w_technical=self.w_technical / total,
                risk_penalty_strength=self.risk_penalty_strength,
                confidence_penalty_strength=self.confidence_penalty_strength,
            )
        return self

    def as_factor_weights_map(self) -> Dict[str, float]:
        return {
            "valuation_score": self.w_valuation,
            "momentum_score": self.w_momentum,
            "quality_score": self.w_quality,
            "growth_score": self.w_growth,
            "opportunity_score": self.w_opportunity,
            "technical_score": self.w_technical,
        }


@dataclass(slots=True)
class ForecastParameters:
    min_roi_1m: float = -0.25
    max_roi_1m: float = 0.25
    min_roi_3m: float = -0.35
    max_roi_3m: float = 0.35
    min_roi_12m: float = -0.65
    max_roi_12m: float = 0.65
    ratio_1m_of_12m: float = 0.18
    ratio_3m_of_12m: float = 0.42


@dataclass(slots=True)
class AssetScores:
    valuation_score: Optional[float] = None
    momentum_score: Optional[float] = None
    quality_score: Optional[float] = None
    growth_score: Optional[float] = None
    value_score: Optional[float] = None
    opportunity_score: Optional[float] = None
    # v5.3.0 FIX: Makes opportunity_score provenance explicit.
    opportunity_source: str = ""
    confidence_score: Optional[float] = None
    forecast_confidence: Optional[float] = None
    confidence_bucket: Optional[str] = None
    risk_score: Optional[float] = None
    risk_bucket: Optional[str] = None
    overall_score: Optional[float] = None
    overall_score_raw: Optional[float] = None
    overall_penalty_factor: Optional[float] = None

    technical_score: Optional[float] = None
    rsi_signal: Optional[str] = None
    short_term_signal: Optional[str] = None
    day_range_position: Optional[float] = None
    volume_ratio: Optional[float] = None
    upside_pct: Optional[float] = None
    invest_period_label: Optional[str] = None
    horizon_label: Optional[str] = None
    horizon_days_effective: Optional[int] = None

    fundamental_view: Optional[str] = None
    technical_view: Optional[str] = None
    risk_view: Optional[str] = None
    value_view: Optional[str] = None

    recommendation: str = "HOLD"
    recommendation_reason: str = "Insufficient data."

    forecast_price_1m: Optional[float] = None
    forecast_price_3m: Optional[float] = None
    forecast_price_12m: Optional[float] = None
    expected_roi_1m: Optional[float] = None
    expected_roi_3m: Optional[float] = None
    expected_roi_12m: Optional[float] = None
    expected_return_1m: Optional[float] = None
    expected_return_3m: Optional[float] = None
    expected_return_12m: Optional[float] = None
    expected_price_1m: Optional[float] = None
    expected_price_3m: Optional[float] = None
    expected_price_12m: Optional[float] = None

    sector_relative_score: Optional[float] = None
    conviction_score: Optional[float] = None
    top_factors: str = ""
    top_risks: str = ""
    position_size_hint: str = ""

    scoring_updated_utc: str = field(default_factory=_utc_iso)
    scoring_updated_riyadh: str = field(default_factory=_riyadh_iso)
    scoring_errors: List[str] = field(default_factory=list)

    # v5.2.7 additions — appended at end for additive-only schema compatibility.
    # Downstream dict consumers that iterate fields in declaration order are
    # unaffected; new fields are simply tacked on after scoring_errors.
    recommendation_priority_band: Optional[str] = None
    recommendation_source: str = ""
    # v5.3.0 FIX: Additive schema/version marker for cache invalidation.
    scoring_schema_version: str = ""

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


ScoringWeights = ScoreWeights

# v5.3.0 FIX: Explicit defaults exposed under descriptive names; legacy aliases preserved.
DEFAULT_SCORING_WEIGHTS = ScoreWeights()
DEFAULT_FORECAST_PARAMETERS = ForecastParameters()
DEFAULT_WEIGHTS = DEFAULT_SCORING_WEIGHTS
DEFAULT_FORECASTS = DEFAULT_FORECAST_PARAMETERS


# =============================================================================
# Pure Utility Functions (preserved + v5.2.5 Phase M / v5.2.6 Phase Q updates)
# =============================================================================

def _clamp(value: float, min_val: float, max_val: float) -> float:
    return max(min_val, min(value, max_val))


def _round(value: Optional[float], ndigits: int = 2) -> Optional[float]:
    if value is None:
        return None
    try:
        return round(value, ndigits)
    except Exception:
        return None


def _safe_float(value: Any) -> Optional[float]:
    if value is None:
        return None
    try:
        if isinstance(value, (int, float)) and not isinstance(value, bool):
            return float(value)
        s = str(value).strip().replace(",", "")
        if not s or s.lower() in {"na", "n/a", "none", "null", ""}:
            return None
        if s.endswith("%"):
            f = float(s[:-1].strip()) / 100.0
        else:
            f = float(s)
        if math.isnan(f) or math.isinf(f):
            return None
        return f
    except (ValueError, TypeError):
        return None


def _safe_str(value: Any, default: str = "") -> str:
    try:
        s = str(value).strip()
        return s if s else default
    except Exception:
        return default


def _safe_bool(value: Any) -> bool:
    """v5.2.3: forgiving bool coercion. Recognises common truthy strings."""
    if value is None:
        return False
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    s = str(value).strip().lower()
    if not s:
        return False
    if s in {"true", "t", "yes", "y", "1", "on"}:
        return True
    if s in {"false", "f", "no", "n", "0", "off", "none", "null", "na", "n/a"}:
        return False
    return False


def _get(row: Mapping[str, Any], *keys: str) -> Any:
    for key in keys:
        if key in row and row[key] is not None:
            return row[key]
    return None


def _get_float(row: Mapping[str, Any], *keys: str) -> Optional[float]:
    return _safe_float(_get(row, *keys))


def _as_fraction(value: Any) -> Optional[float]:
    f = _safe_float(value)
    if f is None:
        return None
    if abs(f) >= 1.5:
        return f / 100.0
    return f


def _as_pct_position_fraction(value: Any) -> Optional[float]:
    """
    v5.2.5: 52W-position-style fields with defensive shape detection.

    Shape detection (Phase M):
      |value| <= 1.5  ->  FRACTION; pass through with clamp to [0,1]
      |value|  > 1.5  ->  PERCENT POINTS; divide by 100 and clamp

    Idempotent across both shapes.
    """
    f = _safe_float(value)
    if f is None:
        return None
    if abs(f) <= 1.5:
        return _clamp(f, 0.0, 1.0)
    return _clamp(f / 100.0, 0.0, 1.0)


def _as_upside_fraction(value: Any) -> Optional[float]:
    """
    v5.2.6 — Phase Q: upside_pct shape detection.

    Shape detection:
      |value| <= 2.5  ->  FRACTION (engine v5.67.0 canonical form;
                                    covers the entire legitimate range
                                    [-0.90, +2.00] with comfortable
                                    margin for analyst-supplied
                                    high-conviction upsides up to +250%)
      |value|  > 2.5  ->  PERCENT POINTS (pre-v5.67.0 engine, legacy
                                          snapshots, non-engine paths;
                                          divide by 100)

    Idempotent across both shapes for any value within the engine's
    cap range. None inputs return None.
    """
    f = _safe_float(value)
    if f is None:
        return None
    if abs(f) <= 2.5:
        return f
    return f / 100.0


def _as_roi_fraction(value: Any) -> Optional[float]:
    f = _safe_float(value)
    if f is None:
        return None
    if abs(f) > 1.0:
        return f / 100.0
    return f


def _norm_score_0_100(value: Any) -> Optional[float]:
    f = _safe_float(value)
    if f is None:
        return None
    if 0.0 <= f <= 1.5:
        f *= 100.0
    return _clamp(f, 0.0, 100.0)


def _norm_confidence_0_1(value: Any) -> Optional[float]:
    f = _safe_float(value)
    if f is None:
        return None
    if f > 1.5:
        f /= 100.0
    return _clamp(f, 0.0, 1.0)


# =============================================================================
# Horizon Detection (preserved)
# =============================================================================

def detect_horizon(settings: Any = None, row: Optional[Mapping[str, Any]] = None) -> Tuple[Horizon, Optional[int]]:
    horizon_days: Optional[float] = None

    if settings is not None:
        if isinstance(settings, Mapping):
            horizon_days = _safe_float(settings.get("horizon_days") or settings.get("invest_period_days"))
        else:
            horizon_days = _safe_float(
                getattr(settings, "horizon_days", None) or
                getattr(settings, "invest_period_days", None)
            )

    if horizon_days is None and row is not None:
        horizon_days = _get_float(row, "horizon_days", "invest_period_days")

    if horizon_days is None:
        return Horizon.MONTH, None

    hd = int(abs(horizon_days))
    for cutoff, label in _HORIZON_DAYS_CUTOFFS:
        if hd <= cutoff:
            return label, hd
    return Horizon.LONG, hd


def get_weights_for_horizon(horizon: Horizon, settings: Any = None) -> ScoreWeights:
    presets = {
        Horizon.DAY: ScoreWeights(
            w_technical=0.50, w_momentum=0.30, w_quality=0.10,
            w_valuation=0.00, w_growth=0.00, w_opportunity=0.10,
        ),
        Horizon.WEEK: ScoreWeights(
            w_technical=0.25, w_momentum=0.25, w_valuation=0.20,
            w_quality=0.20, w_growth=0.00, w_opportunity=0.10,
        ),
        Horizon.MONTH: ScoreWeights(
            w_technical=0.00, w_valuation=0.30, w_momentum=0.25,
            w_quality=0.20, w_growth=0.15, w_opportunity=0.10,
        ),
        Horizon.LONG: ScoreWeights(
            w_technical=0.00, w_valuation=0.35, w_quality=0.25,
            w_growth=0.20, w_momentum=0.15, w_opportunity=0.05,
        ),
    }

    base = presets.get(horizon, presets[Horizon.MONTH])

    if settings is None:
        return base.normalize()

    def _try(name: str, current: float) -> float:
        try:
            v = settings.get(name) if isinstance(settings, Mapping) else getattr(settings, name, None)
            if v is None:
                return current
            f = float(v)
            return f if not math.isnan(f) and not math.isinf(f) else current
        except Exception:
            return current

    result = replace(base)
    result.risk_penalty_strength = _clamp(_try("risk_penalty_strength", result.risk_penalty_strength), 0.0, 1.0)
    result.confidence_penalty_strength = _clamp(_try("confidence_penalty_strength", result.confidence_penalty_strength), 0.0, 1.0)

    return result.normalize()


# =============================================================================
# Derived Field Helpers — v5.2.6 Phase Q applied to derive_upside_pct
# =============================================================================

def derive_volume_ratio(row: Mapping[str, Any]) -> Optional[float]:
    vr = _get_float(row, "volume_ratio")
    if vr is not None and vr > 0:
        return _round(vr, 4)

    vol = _get_float(row, "volume")
    avg = _get_float(row, "avg_volume_10d")
    if avg is None:
        avg = _get_float(row, "avg_volume_30d")

    if vol is None or avg is None or avg <= 0:
        return None

    return _round(vol / avg, 4)


def derive_day_range_position(row: Mapping[str, Any]) -> Optional[float]:
    drp = _get_float(row, "day_range_position")
    if drp is not None:
        return _round(_clamp(drp, 0.0, 1.0), 4)

    price = _get_float(row, "current_price", "price", "last_price")
    low = _get_float(row, "day_low")
    high = _get_float(row, "day_high")

    if price is None or low is None or high is None:
        return None

    range_span = high - low
    if range_span <= 0:
        return _round(0.5, 4)

    return _round(_clamp((price - low) / range_span, 0.0, 1.0), 4)


def derive_upside_pct(row: Mapping[str, Any]) -> Optional[float]:
    """
    Compute the displayed upside_pct field.

    v5.2.6 — Phase Q. When the row carries a supplied `upside_pct`,
    normalize its shape via `_as_upside_fraction` before evaluating
    the suspect bounds.

    v5.2.4 — TWO-SIDED SUSPECT BOUNDS (preserved). Suppresses values
    outside [_UPSIDE_PCT_SUSPECT_FLOOR, _UPSIDE_PCT_SUSPECT_CEILING]
    (default -90% to +200% in fraction form) by returning None.
    """
    usp = _as_upside_fraction(_get(row, "upside_pct"))
    if usp is not None:
        if _is_upside_suspect(usp):
            return None
        return _round(usp, 4)

    price = _get_float(row, "current_price", "price", "last_price")
    intrinsic = _get_float(row, "intrinsic_value", "fair_value")

    if price is None or price <= 0 or intrinsic is None or intrinsic <= 0:
        return None

    raw = (intrinsic - price) / price

    if _is_upside_suspect(raw):
        return None

    return _round(raw, 4)


def invest_period_label(horizon: Horizon, horizon_days: Optional[int] = None) -> str:
    if horizon_days is not None:
        if horizon_days <= 1:
            return "1D"
        if horizon_days <= 6:
            return "1W"
        if horizon_days <= 30:
            return "1M"
        if horizon_days <= 90:
            return "3M"
        return "12M"

    return {
        Horizon.DAY: "1D",
        Horizon.WEEK: "1W",
        Horizon.MONTH: "1M",
        Horizon.LONG: "12M",
    }.get(horizon, "1M")


# =============================================================================
# Technical Score (preserved)
# =============================================================================

def _rsi_to_zone_score(rsi: Optional[float]) -> Optional[float]:
    if rsi is None:
        return None
    if rsi <= 25:
        return 0.95
    if rsi <= 35:
        return 0.88
    if rsi <= 45:
        return 0.78
    if rsi <= 55:
        return 0.68
    if rsi <= 60:
        return 0.58
    if rsi <= 65:
        return 0.45
    if rsi <= 70:
        return 0.30
    if rsi <= 75:
        return 0.18
    return 0.08


def _volume_ratio_to_score(ratio: Optional[float]) -> Optional[float]:
    if ratio is None or ratio < 0:
        return None
    if ratio >= 3.0:
        return 1.00
    if ratio >= 2.0:
        return 0.90
    if ratio >= 1.5:
        return 0.75
    if ratio >= 1.0:
        return 0.55
    if ratio >= 0.7:
        return 0.40
    return 0.20


def _day_range_to_score(drp: Optional[float]) -> Optional[float]:
    if drp is None:
        return None
    return _clamp(1.0 - (drp ** 0.7), 0.0, 1.0)


def compute_technical_score(row: Mapping[str, Any]) -> Optional[float]:
    rsi = _get_float(row, "rsi_14", "rsi", "rsi14")
    vol_ratio = derive_volume_ratio(row)
    drp = derive_day_range_position(row)

    parts: List[Tuple[float, float]] = []

    rsi_score = _rsi_to_zone_score(rsi)
    if rsi_score is not None:
        parts.append((0.40, rsi_score))

    vol_score = _volume_ratio_to_score(vol_ratio)
    if vol_score is not None:
        parts.append((0.30, vol_score))

    drp_score = _day_range_to_score(drp)
    if drp_score is not None:
        parts.append((0.30, drp_score))

    if not parts:
        return None

    wsum = sum(w for w, _ in parts)
    score_01 = sum(w * v for w, v in parts) / max(1e-9, wsum)
    return _round(100.0 * _clamp(score_01, 0.0, 1.0), 2)


def rsi_signal(rsi: Optional[float]) -> str:
    if rsi is None:
        return RSISignal.N_A.value
    if rsi < 30:
        return RSISignal.OVERSOLD.value
    if rsi > 70:
        return RSISignal.OVERBOUGHT.value
    return RSISignal.NEUTRAL.value


def short_term_signal(
    technical: Optional[float],
    momentum: Optional[float],
    risk: Optional[float],
    horizon: Horizon,
) -> str:
    t = technical if technical is not None else 50.0
    m = momentum if momentum is not None else 50.0
    r = risk if risk is not None else 50.0

    if horizon == Horizon.DAY:
        if t >= 75 and m >= 70 and r <= 50:
            return Signal.STRONG_BUY.value
        if t >= 60 and m >= 55 and r <= 65:
            return Signal.BUY.value
        if t < 38 or m < 30:
            return Signal.SELL.value
        return Signal.HOLD.value

    if horizon == Horizon.WEEK:
        if t >= 65 and m >= 60 and r <= 60:
            return Signal.BUY.value
        if t < 35 or m < 30:
            return Signal.SELL.value
        return Signal.HOLD.value

    if m >= 70 and t >= 55:
        return Signal.BUY.value
    if m <= 30 or t <= 35:
        return Signal.SELL.value
    return Signal.HOLD.value


# =============================================================================
# Forecast Helpers — v5.2.5 Phase N preserved
# =============================================================================

def _forecast_params_from_settings(settings: Any) -> ForecastParameters:
    if settings is None:
        return DEFAULT_FORECAST_PARAMETERS

    def _try_fraction(name: str, current: float) -> float:
        try:
            v = settings.get(name) if isinstance(settings, Mapping) else getattr(settings, name, None)
            if v is None:
                return current
            f = float(v)
            if math.isnan(f) or math.isinf(f):
                return current
            if abs(f) > 1.5:
                f /= 100.0
            return f
        except Exception:
            return current

    return ForecastParameters(
        min_roi_1m=_try_fraction("min_roi_1m", DEFAULT_FORECAST_PARAMETERS.min_roi_1m),
        max_roi_1m=_try_fraction("max_roi_1m", DEFAULT_FORECAST_PARAMETERS.max_roi_1m),
        min_roi_3m=_try_fraction("min_roi_3m", DEFAULT_FORECAST_PARAMETERS.min_roi_3m),
        max_roi_3m=_try_fraction("max_roi_3m", DEFAULT_FORECAST_PARAMETERS.max_roi_3m),
        min_roi_12m=_try_fraction("min_roi_12m", DEFAULT_FORECAST_PARAMETERS.min_roi_12m),
        max_roi_12m=_try_fraction("max_roi_12m", DEFAULT_FORECAST_PARAMETERS.max_roi_12m),
    )


def _empty_forecast_patch() -> Dict[str, Any]:
    """v5.2.3: helper for forecast-skip paths."""
    return {
        "forecast_price_1m": None,
        "forecast_price_3m": None,
        "forecast_price_12m": None,
        "expected_roi_1m": None,
        "expected_roi_3m": None,
        "expected_roi_12m": None,
        "expected_return_1m": None,
        "expected_return_3m": None,
        "expected_return_12m": None,
        "expected_price_1m": None,
        "expected_price_3m": None,
        "expected_price_12m": None,
    }


def _is_row_unforecastable(row: Mapping[str, Any]) -> bool:
    """
    v5.2.5 — WARNINGS-STRING-AWARE detection (Phase N).
    Returns True when forecast synthesis should be skipped entirely.
    """
    if _safe_bool(_get(row, "forecast_unavailable", "is_forecast_unavailable")):
        return True

    tags = _warning_tags_from_row(row)
    if tags and (tags & _ENGINE_UNFORECASTABLE_TAGS):
        return True

    dq_label = str(_get(row, "data_quality") or "").strip().upper()
    dq_is_unrecoverable = dq_label in {"STALE", "MISSING", "ERROR"}

    if not dq_is_unrecoverable:
        return False

    fair = _get_float(
        row,
        "intrinsic_value", "fair_value", "target_price",
        "target_mean_price",
    )
    api_roi3 = _as_roi_fraction(_get(row, "expected_roi_3m", "expected_return_3m"))
    api_roi12 = _as_roi_fraction(_get(row, "expected_roi_12m", "expected_return_12m"))
    api_fp12 = _get_float(row, "forecast_price_12m", "expected_price_12m")

    return fair is None and api_roi3 is None and api_roi12 is None and api_fp12 is None


def derive_forecast_patch(
    row: Mapping[str, Any],
    forecasts: ForecastParameters,
) -> Tuple[Dict[str, Any], List[str]]:
    """
    v5.2.4: hardened forecast derivation (preserved).
    """
    errors: List[str] = []

    if _is_row_unforecastable(row):
        errors.append("forecast_skipped_unavailable")
        return _empty_forecast_patch(), errors

    patch: Dict[str, Any] = {}

    price = _get_float(row, "current_price", "price", "last_price", "last")
    fair = _get_float(
        row,
        "intrinsic_value", "fair_value", "target_price",
        "target_mean_price", "forecast_price_12m",
        "forecast_price_3m", "forecast_price_1m"
    )

    roi1 = _as_roi_fraction(_get(row, "expected_roi_1m", "expected_return_1m"))
    roi3 = _as_roi_fraction(_get(row, "expected_roi_3m", "expected_return_3m"))
    roi12 = _as_roi_fraction(_get(row, "expected_roi_12m", "expected_return_12m"))

    fp1 = _get_float(row, "forecast_price_1m", "expected_price_1m")
    fp3 = _get_float(row, "forecast_price_3m", "expected_price_3m")
    fp12 = _get_float(row, "forecast_price_12m", "expected_price_12m")

    if price is not None and price > 0:
        if roi12 is None and fp12 is not None and fp12 > 0:
            roi12 = (fp12 / price) - 1.0
        if roi3 is None and fp3 is not None and fp3 > 0:
            roi3 = (fp3 / price) - 1.0
        if roi1 is None and fp1 is not None and fp1 > 0:
            roi1 = (fp1 / price) - 1.0

    roi12_synthesized_from_fair = False
    if price is not None and price > 0 and roi12 is None and fair is not None and fair > 0:
        roi12 = (fair / price) - 1.0
        roi12_synthesized_from_fair = True

    if roi12 is not None and roi12 < _FORECAST_ROI12_UNIT_MISMATCH_FLOOR:
        errors.append("forecast_suspect_unit_mismatch")
        roi12 = None
        if roi12_synthesized_from_fair:
            roi1 = None
            roi3 = None
            fp1 = None
            fp3 = None
            fp12 = None
        else:
            fp12 = None

    if (
        roi12 is not None
        and roi12_synthesized_from_fair
        and roi12 > _FORECAST_ROI12_SYNTHESIS_CEILING
    ):
        errors.append("forecast_suspect_synthesis_overshoot")
        roi12 = None
        roi1 = None
        roi3 = None
        fp1 = None
        fp3 = None
        fp12 = None

    if roi12 is not None:
        roi12 = _clamp(roi12, forecasts.min_roi_12m, forecasts.max_roi_12m)
    if roi3 is None and roi12 is not None:
        roi3 = _clamp(roi12 * forecasts.ratio_3m_of_12m, forecasts.min_roi_3m, forecasts.max_roi_3m)
    if roi1 is None and roi12 is not None:
        roi1 = _clamp(roi12 * forecasts.ratio_1m_of_12m, forecasts.min_roi_1m, forecasts.max_roi_1m)

    if roi3 is not None:
        roi3 = _clamp(roi3, forecasts.min_roi_3m, forecasts.max_roi_3m)
    if roi1 is not None:
        roi1 = _clamp(roi1, forecasts.min_roi_1m, forecasts.max_roi_1m)

    if price is not None and price > 0:
        if fp12 is None and roi12 is not None:
            fp12 = price * (1.0 + roi12)
        if fp3 is None and roi3 is not None:
            fp3 = price * (1.0 + roi3)
        if fp1 is None and roi1 is not None:
            fp1 = price * (1.0 + roi1)
    elif fair is None:
        errors.append("price_unavailable_for_forecast")

    patch["forecast_price_1m"] = _round(fp1, 4)
    patch["forecast_price_3m"] = _round(fp3, 4)
    patch["forecast_price_12m"] = _round(fp12, 4)
    patch["expected_roi_1m"] = _round(roi1, 6)
    patch["expected_roi_3m"] = _round(roi3, 6)
    patch["expected_roi_12m"] = _round(roi12, 6)

    patch["expected_return_1m"] = patch["expected_roi_1m"]
    patch["expected_return_3m"] = patch["expected_roi_3m"]
    patch["expected_return_12m"] = patch["expected_roi_12m"]
    patch["expected_price_1m"] = patch["forecast_price_1m"]
    patch["expected_price_3m"] = patch["forecast_price_3m"]
    patch["expected_price_12m"] = patch["forecast_price_12m"]

    return patch, errors


# =============================================================================
# Component Scoring — preserved from v5.2.5
# =============================================================================

def _data_quality_factor(row: Mapping[str, Any]) -> float:
    dq = str(_get(row, "data_quality") or "").strip().upper()
    quality_map = {
        "EXCELLENT": 0.95,
        "HIGH": 0.85,
        "GOOD": 0.80,
        "MEDIUM": 0.68,
        "FAIR": 0.60,
        "POOR": 0.40,
        "STALE": 0.45,
        "MISSING": 0.20,
        "ERROR": 0.15,
    }
    return quality_map.get(dq, 0.60)


def _completeness_factor(row: Mapping[str, Any]) -> float:
    # v5.3.0 NOTE: This local field list must be reviewed whenever the
    # instrument canonical schema changes; it intentionally avoids importing
    # data_engine_v2 to prevent a circular import.
    core_fields = [
        "symbol", "name", "currency", "exchange", "current_price", "previous_close",
        "day_high", "day_low", "week_52_high", "week_52_low", "volume", "market_cap",
        "pe_ttm", "pb_ratio", "ps_ratio", "dividend_yield", "rsi_14",
        "volatility_30d", "volatility_90d", "expected_roi_3m", "forecast_price_3m",
        "forecast_confidence",
    ]
    present = sum(1 for k in core_fields if row.get(k) not in (None, "", [], {}))
    return present / max(1, len(core_fields))


def _revenue_collapse_haircut(revenue_growth: Optional[float]) -> float:
    """
    v5.2.3: Multiplicative haircut for quality scores when revenue
    has collapsed YoY.
    """
    if revenue_growth is None:
        return 1.0
    if revenue_growth >= _QUALITY_REVENUE_COLLAPSE_START:
        return 1.0

    span = _QUALITY_REVENUE_COLLAPSE_FLOOR - _QUALITY_REVENUE_COLLAPSE_START
    if span >= 0:
        return 1.0
    progress = (revenue_growth - _QUALITY_REVENUE_COLLAPSE_START) / span
    progress = _clamp(progress, 0.0, 1.0)
    haircut = 1.0 - progress * (1.0 - _QUALITY_REVENUE_COLLAPSE_MAX_HAIRCUT)
    return _clamp(haircut, _QUALITY_REVENUE_COLLAPSE_MAX_HAIRCUT, 1.0)


def compute_quality_score(row: Mapping[str, Any]) -> Optional[float]:
    """
    Compute quality score (0-100). v5.2.3: REVENUE-COLLAPSE-AWARE.
    [PRESERVED] Phantom-row gate.
    """
    roe = _as_fraction(_get(row, "roe", "return_on_equity", "returnOnEquity"))
    roa = _as_fraction(_get(row, "roa", "return_on_assets", "returnOnAssets"))
    op_margin = _as_fraction(_get(row, "operating_margin", "operatingMarginTTM"))
    net_margin = _as_fraction(_get(row, "profit_margin", "net_margin", "profitMargins"))
    de = _get_float(row, "debt_to_equity", "debtToEquity")

    has_any_financial = any(x is not None for x in (roe, roa, op_margin, net_margin, de))

    dq_label = str(_get(row, "data_quality") or "").strip().upper()
    dq_is_weak = dq_label in {"", "POOR", "STALE", "MISSING", "ERROR", "UNKNOWN"}

    completeness = _completeness_factor(row)

    if not has_any_financial and dq_is_weak and completeness < 0.30:
        return None

    dq = _data_quality_factor(row)
    data_quality_proxy = _clamp(0.55 * dq + 0.45 * completeness, 0.0, 1.0)

    fin_parts: List[Tuple[float, float]] = []

    if roe is not None:
        fin_parts.append((0.30, _clamp((roe - 0.05) / 0.30, 0.0, 1.0)))
    if roa is not None:
        fin_parts.append((0.25, _clamp((roa - 0.02) / 0.16, 0.0, 1.0)))
    if op_margin is not None:
        fin_parts.append((0.25, _clamp((op_margin - 0.05) / 0.35, 0.0, 1.0)))
    if net_margin is not None:
        fin_parts.append((0.15, _clamp((net_margin - 0.02) / 0.28, 0.0, 1.0)))
    if de is not None and de >= 0:
        fin_parts.append((0.05, _clamp(1.0 - (de / 2.5), 0.0, 1.0)))

    if fin_parts:
        wsum = sum(w for w, _ in fin_parts)
        financial_quality = sum(w * v for w, v in fin_parts) / max(1e-9, wsum)
        combined = 0.40 * financial_quality + 0.60 * data_quality_proxy
    else:
        combined = data_quality_proxy

    revenue_growth = _as_fraction(
        _get(row, "revenue_growth_yoy", "revenue_growth", "growth_yoy")
    )
    haircut = _revenue_collapse_haircut(revenue_growth)
    combined *= haircut

    return _round(100.0 * _clamp(combined, 0.0, 1.0), 2)


def compute_confidence_score(row: Mapping[str, Any]) -> Tuple[Optional[float], Optional[float]]:
    """
    v5.2.3 NO-FABRICATED-CONFIDENCE (preserved).
    Returns (confidence_score_100, forecast_confidence_01).
    """
    fc = _safe_float(
        _get(row, "forecast_confidence", "ai_confidence", "confidence_score", "confidence")
    )
    if fc is not None:
        fc01 = (fc / 100.0) if fc > 1.5 else fc
        fc01 = _clamp(fc01, 0.0, 1.0)
        return _round(fc01 * 100.0, 2), _round(fc01, 4)

    has_dq_signal = bool(str(_get(row, "data_quality") or "").strip())
    completeness = _completeness_factor(row)
    provs = row.get("data_sources") or row.get("providers") or []
    try:
        pcount = len(provs) if isinstance(provs, list) else 0
    except Exception:
        pcount = 0

    has_any_signal = (
        has_dq_signal
        or completeness >= _CONFIDENCE_FALLBACK_MIN_COMPLETENESS
        or pcount >= _CONFIDENCE_FALLBACK_MIN_PROVIDERS
    )

    if not has_any_signal:
        return None, None

    dq = _data_quality_factor(row)
    prov_factor = _clamp(pcount / 3.0, 0.0, 1.0)
    conf01 = _clamp(0.55 * dq + 0.35 * completeness + 0.10 * prov_factor, 0.0, 1.0)
    return _round(conf01 * 100.0, 2), _round(conf01, 4)


def compute_valuation_score(row: Mapping[str, Any]) -> Optional[float]:
    """
    v5.2.4 — GUARDED UPSIDE COMPUTATION (preserved verbatim).
    """
    price = _get_float(row, "current_price", "price", "last_price", "last")
    if price is None or price <= 0:
        return None

    fair = _get_float(
        row,
        "intrinsic_value", "fair_value", "target_price",
        "forecast_price_3m", "forecast_price_12m", "forecast_price_1m"
    )

    upside: Optional[float] = None
    if fair is not None and fair > 0:
        raw_upside = (fair / price) - 1.0
        if not _is_upside_suspect(raw_upside):
            upside = raw_upside

    roi3 = _as_roi_fraction(_get(row, "expected_roi_3m", "expected_return_3m"))
    roi12 = _as_roi_fraction(_get(row, "expected_roi_12m", "expected_return_12m"))

    def _roi_norm(frac: Optional[float], cap: float) -> Optional[float]:
        if frac is None:
            return None
        return _clamp((frac + cap) / (2 * cap), 0.0, 1.0)

    pe = _get_float(row, "pe_ttm", "pe_ratio")
    pb = _get_float(row, "pb_ratio", "pb", "price_to_book")
    ps = _get_float(row, "ps_ratio", "ps", "price_to_sales")
    peg = _get_float(row, "peg_ratio", "peg")
    ev = _get_float(row, "ev_ebitda", "ev_to_ebitda")

    def _low_is_good(x: Optional[float], lo: float, hi: float) -> Optional[float]:
        if x is None or x <= 0:
            return None
        if x <= lo:
            return 1.0
        if x >= hi:
            return 0.0
        return 1.0 - ((x - lo) / (hi - lo))

    anchors = [
        a for a in (
            _low_is_good(pe, 8.0, 35.0),
            _low_is_good(pb, 0.8, 6.0),
            _low_is_good(ps, 1.0, 10.0),
            _low_is_good(peg, 0.8, 4.0),
            _low_is_good(ev, 6.0, 25.0),
        ) if a is not None
    ]
    anchor_avg = (sum(anchors) / len(anchors)) if anchors else None

    upside_n = _roi_norm(upside, 0.50)
    roi3_n = _roi_norm(roi3, 0.35)
    roi12_n = _roi_norm(roi12, 0.80)

    if upside_n is None and roi3_n is None and roi12_n is None:
        return None

    FULL_WEIGHT = 1.00
    components: List[Tuple[float, Optional[float]]] = [
        (0.40, upside_n),
        (0.30, roi3_n),
        (0.20, roi12_n),
        (0.10, anchor_avg),
    ]

    total = 0.0
    for weight, value in components:
        if value is not None:
            total += weight * value
        else:
            total += weight * 0.5

    score_01 = total / FULL_WEIGHT
    return _round(100.0 * _clamp(score_01, 0.0, 1.0), 2)


def compute_growth_score(row: Mapping[str, Any]) -> Optional[float]:
    g = _as_fraction(_get(row, "revenue_growth_yoy", "revenue_growth", "growth_yoy"))
    if g is None:
        return None
    return _round(_clamp(((g + 0.30) / 0.60) * 100.0, 0.0, 100.0), 2)


def compute_momentum_score(row: Mapping[str, Any]) -> Optional[float]:
    """
    Compute momentum score (0-100). v5.2.5: _as_pct_position_fraction
    detects shape (fraction vs percent points) defensively.
    """
    pct = _as_roi_fraction(_get(row, "percent_change", "change_pct", "change_percent"))
    rsi = _get_float(row, "rsi_14", "rsi", "rsi14")
    pos = _as_pct_position_fraction(_get(row, "week_52_position_pct", "position_52w_pct", "week52_position_pct"))
    pct_5d = _as_roi_fraction(_get(row, "price_change_5d"))
    vol_r = derive_volume_ratio(row)

    parts: List[Tuple[float, float]] = []

    if rsi is not None:
        x = (rsi - 55.0) / 12.0
        parts.append((0.30, _clamp(math.exp(-(x * x)), 0.0, 1.0)))

    if pct is not None:
        parts.append((0.25, _clamp((pct + 0.10) / 0.20, 0.0, 1.0)))

    if pct_5d is not None:
        parts.append((0.20, _clamp((pct_5d + 0.08) / 0.16, 0.0, 1.0)))

    if pos is not None:
        parts.append((0.15, _clamp(pos, 0.0, 1.0)))

    if vol_r is not None:
        parts.append((0.10, _clamp((vol_r - 0.5) / 1.5, 0.0, 1.0)))

    if not parts:
        return None

    wsum = sum(w for w, _ in parts)
    score_01 = sum(w * v for w, v in parts) / max(1e-9, wsum)
    return _round(100.0 * _clamp(score_01, 0.0, 1.0), 2)


def compute_risk_score(row: Mapping[str, Any]) -> Optional[float]:
    """
    Compute risk score (0-100). Higher = riskier.
    """
    vol90 = _as_fraction(_get(row, "volatility_90d"))
    dd1y = _as_fraction(_get(row, "max_drawdown_1y"))
    var1d = _as_fraction(_get(row, "var_95_1d"))
    sharpe = _get_float(row, "sharpe_1y")

    def _scale(x: Optional[float], lo: float, hi: float) -> Optional[float]:
        if x is None:
            return None
        return _clamp((x - lo) / (hi - lo), 0.0, 1.0) if hi > lo else None

    parts: List[Tuple[float, float]] = []
    if vol90 is not None:
        parts.append((0.40, _scale(vol90, 0.12, 0.70) or 0.0))
    if dd1y is not None:
        parts.append((0.35, _scale(abs(dd1y), 0.05, 0.55) or 0.0))
    if var1d is not None:
        parts.append((0.20, _scale(var1d, 0.01, 0.08) or 0.0))
    if sharpe is not None:
        sharpe_norm = _clamp(1.0 - _clamp((sharpe + 0.5) / 2.5, 0.0, 1.0), 0.0, 1.0)
        parts.append((0.05, sharpe_norm))

    if not parts:
        vol = _as_fraction(_get(row, "volatility_30d", "vol_30d"))
        beta = _get_float(row, "beta_5y", "beta")
        dd = _as_fraction(_get(row, "max_drawdown_30d", "drawdown_30d"))
        if vol is not None:
            parts.append((0.50, _scale(vol, 0.10, 0.60) or 0.0))
        if beta is not None:
            parts.append((0.30, _scale(beta, 0.60, 2.00) or 0.0))
        if dd is not None:
            parts.append((0.20, _scale(abs(dd), 0.00, 0.50) or 0.0))

    if not parts:
        return None

    wsum = sum(w for w, _ in parts)
    score_01 = sum(w * v for w, v in parts) / max(1e-9, wsum)
    return _round(100.0 * _clamp(score_01, 0.0, 1.0), 2)


def compute_opportunity_score_with_source(
    row: Mapping[str, Any],
    valuation: Optional[float],
    momentum: Optional[float],
) -> Tuple[Optional[float], str]:
    # v5.3.0 FIX: Return both score and provenance. The public
    # compute_opportunity_score() wrapper below preserves the old return shape.
    roi1 = _as_roi_fraction(_get(row, "expected_roi_1m", "expected_return_1m"))
    roi3 = _as_roi_fraction(_get(row, "expected_roi_3m", "expected_return_3m"))
    roi12 = _as_roi_fraction(_get(row, "expected_roi_12m", "expected_return_12m"))

    def _roi_norm(frac: Optional[float], cap: float) -> Optional[float]:
        if frac is None:
            return None
        return _clamp((frac + cap) / (2 * cap), 0.0, 1.0)

    parts: List[Tuple[float, float]] = []
    r3 = _roi_norm(roi3, 0.35)
    r12 = _roi_norm(roi12, 0.80)
    r1 = _roi_norm(roi1, 0.25)

    if r3 is not None:
        parts.append((0.55, r3))
    if r12 is not None:
        parts.append((0.30, r12))
    if r1 is not None:
        parts.append((0.15, r1))

    if parts:
        wsum = sum(w for w, _ in parts)
        score = _round(
            100.0 * _clamp(sum(w * v for w, v in parts) / max(1e-9, wsum), 0.0, 1.0),
            SCORE_PRECISION,
        )
        return score, "roi_based"

    if valuation is None and momentum is None:
        return None, "insufficient"

    v = (valuation if valuation is not None else 50.0) / 100.0
    m = (momentum if momentum is not None else 50.0) / 100.0
    score = _round(100.0 * _clamp(0.60 * v + 0.40 * m, 0.0, 1.0), SCORE_PRECISION)
    return score, "valuation_momentum_fallback"


def compute_opportunity_score(
    row: Mapping[str, Any],
    valuation: Optional[float],
    momentum: Optional[float],
) -> Optional[float]:
    # v5.3.0 FIX: Public API return shape preserved; provenance is available
    # through compute_opportunity_score_with_source() and compute_scores().
    score, _source = compute_opportunity_score_with_source(row, valuation, momentum)
    return score


def risk_bucket(score: Optional[float]) -> Optional[str]:
    """Map a risk score to the canonical UPPERCASE bucket.

    v5.3.0 FIX: Uses RISK_BUCKET_THRESHOLDS and accepts defensive 0-1 input
    by converting it to 0-100. The public signature is preserved.
    """
    if score is None:
        return None
    value = _safe_float(score)
    if value is None:
        return None
    if 0.0 <= abs(value) <= 1.5:
        logger.warning("[v5.3.0 BUCKET_MISMATCH] field=risk score=%s bucket=scale expected=0-100", score)
        value *= 100.0
    expected = _expected_risk_bucket(value)
    bucket = expected
    _log_bucket_mismatch("risk", value, bucket, expected)
    return bucket


def _risk_bucket(score: Optional[float]) -> Optional[str]:
    # v5.3.0 FIX: Private compatibility wrapper required by data_engine_v2 v5.71.0.
    return risk_bucket(score)


def confidence_bucket(conf01: Optional[float]) -> Optional[str]:
    """Map confidence to the canonical UPPERCASE bucket.

    v5.3.0 FIX: Accepts both 0-1 fraction and 0-100 score. The public
    signature is preserved while the implementation is now scale-independent.
    """
    if conf01 is None:
        return None
    value = _safe_float(conf01)
    if value is None:
        return None
    expected = _expected_confidence_bucket(value)
    bucket = expected
    _log_bucket_mismatch("confidence", value, bucket, expected)
    return bucket


def _confidence_bucket(value: Optional[float]) -> Optional[str]:
    # v5.3.0 FIX: Private compatibility wrapper required by data_engine_v2 v5.71.0.
    return confidence_bucket(value)


# =============================================================================
# View Derivation (preserved)
# =============================================================================

def derive_fundamental_view(
    quality: Optional[float],
    growth: Optional[float],
) -> Optional[str]:
    if quality is None and growth is None:
        return None

    q = quality if quality is not None else 50.0
    g = growth if growth is not None else 50.0

    if q < 40.0:
        return "BEARISH"
    if g < 25.0 and q < 55.0:
        return "BEARISH"
    if q >= 65.0 and g >= 60.0:
        return "BULLISH"
    if q >= 70.0 and growth is None:
        return "BULLISH"
    return "NEUTRAL"


def derive_technical_view(
    technical: Optional[float],
    momentum: Optional[float],
    rsi_label: Optional[str] = None,
) -> Optional[str]:
    if technical is None and momentum is None:
        return None

    t = technical if technical is not None else 50.0
    m = momentum if momentum is not None else 50.0

    label = (rsi_label or "").strip().lower()
    is_overbought = label.startswith("overbought")

    if t < 40.0 or m < 35.0:
        return "BEARISH"
    if t >= 65.0 and m >= 55.0:
        return "NEUTRAL" if is_overbought else "BULLISH"
    return "NEUTRAL"


def derive_risk_view(risk: Optional[float]) -> Optional[str]:
    if risk is None:
        return None
    if risk <= _CONFIG.risk_low_threshold:
        return "LOW"
    if risk <= _CONFIG.risk_moderate_threshold:
        return "MODERATE"
    return "HIGH"


def derive_value_view(
    valuation: Optional[float],
    upside_pct: Optional[float] = None,
) -> Optional[str]:
    """
    v5.2.6 NOTE: `upside_pct` is expected to be a FRACTION here.
    """
    if upside_pct is not None:
        if upside_pct > 0.20:
            return "CHEAP"
        if upside_pct < -0.10:
            return "EXPENSIVE"
        if valuation is None:
            return "FAIR"

    if valuation is None:
        return None

    if valuation >= 65.0:
        return "CHEAP"
    if valuation < 40.0:
        return "EXPENSIVE"
    return "FAIR"


# =============================================================================
# v5.2.3 — Recommendation coherence guard (preserved)
# =============================================================================

def _coherence_guard_recommendation(
    canonical_rec: str,
    roi3: Optional[float],
    confidence100: Optional[float],
    view_prefix: str,
) -> Tuple[Optional[str], Optional[str]]:
    """
    v5.2.3: Cross-check the recommendation label against the row's
    own forecasts. Returns (downgraded_rec, downgraded_reason) if
    a downgrade is warranted, or (None, None) to leave the original
    intact.
    """
    if canonical_rec not in ("BUY", "STRONG_BUY"):
        return None, None
    if roi3 is None:
        return None, None
    if roi3 >= _COHERENCE_ROI3_FLOOR_FRACTION:
        return None, None

    conf_for_guard = confidence100 if confidence100 is not None else 55.0
    if conf_for_guard >= _COHERENCE_CONFIDENCE_FLOOR:
        return None, None

    roi3_pct_disp = _round(roi3 * 100.0, 1)
    conf_pct_disp = _round(conf_for_guard, 0)

    reason = (
        f"{view_prefix} \u2192 HOLD "
        f"(coherence guard: {canonical_rec} \u2192 HOLD; "
        f"3M ROI {roi3_pct_disp}% with AI confidence {conf_pct_disp}%)"
    )
    return "HOLD", reason


# =============================================================================
# Recommendation (preserved from v5.2.4)
# =============================================================================

def compute_recommendation(
    overall: Optional[float],
    risk: Optional[float],
    confidence100: Optional[float],
    roi3: Optional[float],
    horizon: Horizon = Horizon.MONTH,
    technical: Optional[float] = None,
    momentum: Optional[float] = None,
    roi1: Optional[float] = None,
    roi12: Optional[float] = None,
    *,
    quality: Optional[float] = None,
    growth: Optional[float] = None,
    valuation: Optional[float] = None,
    fundamental_view: Optional[str] = None,
    technical_view: Optional[str] = None,
    risk_view: Optional[str] = None,
    value_view: Optional[str] = None,
    upside_pct: Optional[float] = None,
    rsi_label: Optional[str] = None,
    conviction: Optional[float] = None,
    sector_relative: Optional[float] = None,
) -> Tuple[str, str]:
    """Compute canonical recommendation and structured reason.

    v5.3.0 FIX: scoring.py is the authoritative recommendation source for
    data_engine_v2 v5.71.0. The output enum is closed and ACCUMULATE is never
    emitted. Tier intent:
      - HOLD when reliable scoring is unavailable or confidence is too low.
      - REDUCE/SELL when risk is high enough to override otherwise acceptable
        score quality.
      - STRONG_BUY/BUY require positive ROI, confidence, score, and controlled
        risk.
      - HOLD is the conservative neutral decision for mid-quality rows.
    """
    if fundamental_view is None:
        fundamental_view = derive_fundamental_view(quality, growth)
    if technical_view is None:
        technical_view = derive_technical_view(technical, momentum, rsi_label)
    if risk_view is None:
        risk_view = derive_risk_view(risk)
    if value_view is None:
        value_view = derive_value_view(valuation, upside_pct)

    view_prefix = _build_view_prefix(
        fundamental_view, technical_view, risk_view, value_view
    )

    def _reason(rec: str, prose: str) -> Tuple[str, str]:
        rec2 = normalize_recommendation_code(rec)
        return rec2, _format_recommendation_reason(rec2, prose, overall, risk, confidence100, roi3)

    # v5.3.0 FIX: None means insufficient, not fabricated neutral 50.0.
    if overall is None:
        return _reason("HOLD", "Insufficient data to score reliably.")

    c = confidence100 if confidence100 is not None else 55.0
    r = risk if risk is not None else 50.0
    o = overall
    roi = roi3 if roi3 is not None else 0.0

    # v5.3.0 FIX: Low confidence suppresses aggressive decisions first.
    if c < 35.0:
        return _reason("HOLD", f"Low confidence ({_round(c, 1)}%) prevents a reliable action signal.")

    if horizon == Horizon.DAY:
        t = technical if technical is not None else 50.0
        m = momentum if momentum is not None else 50.0
        if t >= 80 and m >= 75 and r <= 45 and c >= 55:
            return _reason("STRONG_BUY", "Short-term technical setup is strong with controlled risk.")
        if t < 35 or m < 30:
            return _reason("SELL", "Short-term technical setup has broken down.")

    # v5.3.0 FIX: Close the high-risk gap. Risk around 77 with overall below
    # the strong-buy threshold must not escape into HOLD.
    if r >= 90 and (c < 45 or o < 35):
        return _reason("STRONG_SELL", "Extreme risk and weak support require urgent exit or avoidance.")
    if r >= 85 and o < 50:
        return _reason("SELL", "Very high risk overrides the score profile.")
    if r >= 75 and o < 78:
        return _reason("REDUCE", "High risk overrides otherwise acceptable score.")

    # v5.3.0 FIX: Positive recommendations require ROI, confidence, score, and
    # controlled risk at the same time.
    if roi >= 0.25 and c >= 70 and r <= 45 and o >= 78:
        return _reason("STRONG_BUY", "High expected ROI with strong confidence and controlled risk.")
    if roi >= 0.12 and c >= 60 and r <= 55 and o >= 70:
        return _reason("BUY", "Positive expected ROI with acceptable confidence and risk.")
    if o >= 82 and c >= 60 and r <= 55:
        return _reason("BUY", "High overall score with acceptable confidence and controlled risk.")

    if o >= 65:
        return _reason("HOLD", "Score is acceptable but risk, confidence, or ROI does not justify adding exposure.")
    if o >= 50:
        return _reason("REDUCE", "Score is below preferred quality threshold.")
    return _reason("SELL", "Weak score profile does not support holding the position.")


def _recommendation(
    overall: Optional[float],
    risk: Optional[float],
    confidence100: Optional[float],
    roi3: Optional[float],
) -> Tuple[str, str]:
    # v5.3.0 FIX: Private compatibility wrapper required by data_engine_v2 v5.71.0.
    return compute_recommendation(overall, risk, confidence100, roi3)


# =============================================================================
# v5.2.7 — Priority computation (NEW)
# =============================================================================

def _compute_priority(
    reco: Optional[str],
    overall: Optional[float],
    risk: Optional[float],
    confidence100: Optional[float],
    roi3: Optional[float],
) -> str:
    """
    v5.2.7: Centralized priority bucketing. The single point of truth
    for the P1..P5 mapping so the top-line `Recommendation` column and
    the downstream `Recommendation Detail` string cannot diverge.

    Mapping (matching the priority bucketing visible in the production
    Detail strings — P2..P4 in the May 13 snapshot):

      P1 = Critical action:
             - STRONG_SELL (canonical normalizes this to SELL; would
               still resolve to P5 unless the broken-row threshold
               trips, which is the correct conservative behavior)
             - SELL on a deeply-broken row (risk >= 85 AND overall < 30)
             - REDUCE on a critically-broken row
               (risk >= 90 AND confidence < 45)
      P2 = STRONG_BUY (always), or
           High-conviction BUY (overall >= 80 AND confidence >= 70)
      P3 = BUY (normal conviction)
      P4 = HOLD (default — also the safe fallback for unknown labels)
      P5 = SELL (non-critical), or
           REDUCE (non-critical)

    Inputs are tolerant:
      - None overall/risk/confidence default to neutral midpoints
        (50.0 / 50.0 / 55.0) so the function never raises.
      - reco is normalized through `normalize_recommendation_code`
        before the lookup to absorb legacy aliases (ACCUMULATE, etc).
    """
    o = overall if overall is not None else 50.0
    r = risk if risk is not None else 50.0
    c = confidence100 if confidence100 is not None else 55.0

    canon = normalize_recommendation_code(reco) if reco else "HOLD"

    if canon == "STRONG_SELL":
        return PRIO_P1
    if canon == "STRONG_BUY":
        return PRIO_P2
    if canon == "BUY":
        if (
            o >= _PRIORITY_BUY_HIGH_CONVICTION_OVERALL_FLOOR
            and c >= _PRIORITY_BUY_HIGH_CONVICTION_CONFIDENCE_FLOOR
        ):
            return PRIO_P2
        return PRIO_P3
    if canon == "REDUCE":
        if (
            r >= _PRIORITY_REDUCE_CRITICAL_RISK_FLOOR
            and c < _PRIORITY_REDUCE_CRITICAL_CONFIDENCE_CEILING
        ):
            return PRIO_P1
        return PRIO_P5
    if canon == "SELL":
        if (
            r >= _PRIORITY_SELL_CRITICAL_RISK_FLOOR
            and o < _PRIORITY_SELL_CRITICAL_OVERALL_CEILING
        ):
            return PRIO_P1
        return PRIO_P5
    # HOLD and anything else
    return PRIO_P4


# =============================================================================
# Recommendation Normalization (preserved)
# =============================================================================

_LOCAL_RECO_ALIASES: Dict[str, str] = {
    "STRONG_BUY": "STRONG_BUY",
    "STRONGBUY": "STRONG_BUY",
    "CONVICTION_BUY": "STRONG_BUY",
    "TOP_PICK": "STRONG_BUY",
    "BUY": "BUY",
    "ACCUMULATE": "BUY",
    "ADD": "BUY",
    "OUTPERFORM": "BUY",
    "OVERWEIGHT": "BUY",
    "HOLD": "HOLD",
    "NEUTRAL": "HOLD",
    "MAINTAIN": "HOLD",
    "MARKET_PERFORM": "HOLD",
    "WATCH": "HOLD",
    "REDUCE": "REDUCE",
    "TRIM": "REDUCE",
    "UNDERWEIGHT": "REDUCE",
    "SELL": "SELL",
    "AVOID": "SELL",
    "EXIT": "SELL",
    "UNDERPERFORM": "SELL",
    "STRONG_SELL": "STRONG_SELL",
}

CANONICAL_RECOMMENDATION_CODES: Tuple[str, ...] = RECOMMENDATION_ENUM
_CANONICAL_RECO = set(CANONICAL_RECOMMENDATION_CODES)


def _normalize_key(label: Any) -> str:
    s = _safe_str(label).upper()
    if not s:
        return ""
    s = s.replace("-", "_").replace(" ", "_")
    while "__" in s:
        s = s.replace("__", "_")
    return s.strip("_")


def normalize_recommendation_code(label: Any) -> str:
    key = _normalize_key(label)
    if not key:
        return "HOLD"
    if key in _CANONICAL_RECO:
        return key

    if key in _LOCAL_RECO_ALIASES:
        return _LOCAL_RECO_ALIASES[key]

    try:
        from core.reco_normalize import normalize_recommendation as _reco_norm  # noqa: WPS433
        normalized = _reco_norm(label)
        if normalized in _CANONICAL_RECO:
            return normalized
        normalized_key = _normalize_key(normalized)
        if normalized_key in _CANONICAL_RECO:
            return normalized_key
        if normalized_key in _LOCAL_RECO_ALIASES:
            return _LOCAL_RECO_ALIASES[normalized_key]
    except Exception:
        pass

    return "HOLD"


# =============================================================================
# v5.1.0: insights_builder lazy import helper (preserved)
# =============================================================================

def _import_insights_builder():
    try:
        from core import insights_builder as _ib  # noqa: WPS433
        return _ib
    except ImportError:
        try:
            import insights_builder as _ib  # noqa: WPS433
            return _ib
        except ImportError:
            return None


# =============================================================================
# v5.2.7 — Canonical recommendation public helpers (NEW)
# =============================================================================

def derive_canonical_recommendation(
    row: Mapping[str, Any],
    *,
    settings: Any = None,
) -> Tuple[str, str, str]:
    """
    v5.2.7: Read the canonical (recommendation, reason, priority) triple
    from a row that has already been through compute_scores() (or
    equivalent — has overall/risk/confidence/views set).

    Lightweight — does NOT re-run the full compute_scores pipeline
    (no insights_builder, no sector batch pass). Use this from
    downstream engines (investment_advisor_engine, data_engine_v2)
    when the row's scores are already populated and you just need
    the canonical recommendation triple.

    Behavior
    --------
    1. If the row already carries `recommendation_source ==
       RECOMMENDATION_SOURCE_TAG`, reads the existing values. The
       priority is reconstructed if missing or corrupt.
    2. Otherwise, derives fresh values by:
         - Reading scores from the row (overall/risk/conf/roi/views)
         - Calling compute_recommendation() with those scores
         - Applying coherence guard + reason alignment
         - Computing priority via _compute_priority()

    Returns
    -------
    (recommendation, recommendation_reason, recommendation_priority_band)
        - recommendation ∈ CANONICAL_RECOMMENDATION_CODES
        - recommendation_reason: aligned to the canonical label
        - recommendation_priority_band ∈ CANONICAL_PRIORITIES (P1..P5)

    Does NOT modify the input row. For mutation, use
    apply_canonical_recommendation().
    """
    # Idempotency fast-path: row already canonically scored.
    existing_tag = _safe_str(row.get("recommendation_source"))
    if existing_tag == RECOMMENDATION_SOURCE_TAG:
        reco = normalize_recommendation_code(row.get("recommendation"))
        reason = _safe_str(row.get("recommendation_reason"))
        priority = _safe_str(row.get("recommendation_priority_band"))

        # Reconstruct priority if missing or corrupt (defensive against
        # downstream merge code that may drop the field).
        if priority not in _CANONICAL_PRIORITIES_SET:
            overall = _norm_score_0_100(row.get("overall_score"))
            risk_s = _norm_score_0_100(row.get("risk_score"))
            conf100 = _norm_score_0_100(row.get("confidence_score"))
            roi3 = _as_roi_fraction(row.get("expected_roi_3m"))
            priority = _compute_priority(reco, overall, risk_s, conf100, roi3)

        return reco, reason, priority

    # Fresh derivation — read existing scores from the row.
    overall = _norm_score_0_100(row.get("overall_score"))
    risk_s = _norm_score_0_100(row.get("risk_score"))
    conf100 = _norm_score_0_100(row.get("confidence_score"))
    if conf100 is None:
        fc01 = _norm_confidence_0_1(row.get("forecast_confidence"))
        if fc01 is not None:
            conf100 = fc01 * 100.0

    roi1 = _as_roi_fraction(row.get("expected_roi_1m"))
    roi3 = _as_roi_fraction(row.get("expected_roi_3m"))
    roi12 = _as_roi_fraction(row.get("expected_roi_12m"))

    valuation = _norm_score_0_100(row.get("valuation_score"))
    quality_s = _norm_score_0_100(row.get("quality_score"))
    growth_s = _norm_score_0_100(row.get("growth_score"))
    momentum_s = _norm_score_0_100(row.get("momentum_score"))
    technical_s = _norm_score_0_100(row.get("technical_score"))
    conviction = _norm_score_0_100(row.get("conviction_score"))
    sector_rel = _norm_score_0_100(row.get("sector_relative_score"))

    # Shape-aware upside read (v5.2.6 Phase Q).
    usp = _as_upside_fraction(row.get("upside_pct"))
    rsi_val = _get_float(row, "rsi_14", "rsi", "rsi14")
    rsi_sig = rsi_signal(rsi_val)

    # Pass through pre-set views if present (avoids re-derivation cost
    # and respects upstream view decisions). The compute_recommendation
    # helper will re-derive any view that arrives as None or "N/A".
    def _coerce_view(v: Any) -> Optional[str]:
        s = _safe_str(v)
        if not s or s.upper() == "N/A":
            return None
        return s

    fund_view = _coerce_view(row.get("fundamental_view"))
    tech_view = _coerce_view(row.get("technical_view"))
    risk_view_in = _coerce_view(row.get("risk_view"))
    value_view_in = _coerce_view(row.get("value_view"))

    horizon, _hdays = detect_horizon(settings, row)

    rec, reason = compute_recommendation(
        overall, risk_s, conf100, roi3,
        horizon=horizon,
        technical=technical_s,
        momentum=momentum_s,
        roi1=roi1,
        roi12=roi12,
        quality=quality_s,
        growth=growth_s,
        valuation=valuation,
        fundamental_view=fund_view,
        technical_view=tech_view,
        risk_view=risk_view_in,
        value_view=value_view_in,
        upside_pct=usp,
        rsi_label=rsi_sig,
        conviction=conviction,
        sector_relative=sector_rel,
    )

    canonical_rec = normalize_recommendation_code(rec)
    aligned_reason = _align_reason_to_canonical_recommendation(reason, canonical_rec)
    priority = _compute_priority(canonical_rec, overall, risk_s, conf100, roi3)

    return canonical_rec, aligned_reason, priority


def apply_canonical_recommendation(
    row: Dict[str, Any],
    *,
    settings: Any = None,
    overwrite: bool = False,
) -> Dict[str, Any]:
    """
    v5.2.7: Drop-in replacement for legacy `_compute_recommendation(row)`
    calls in data_engine_v2 and `_recommendation_from_scores(...)` calls
    in investment_advisor_engine.

    Computes the canonical (recommendation, reason, priority) triple
    via derive_canonical_recommendation() and returns a patch dict
    that the caller can merge into the row. Adds the v5.2.7 provenance
    tag so subsequent calls can short-circuit.

    Parameters
    ----------
    row : Dict[str, Any]
        The row to derive from. Not mutated by this call — the patch
        is returned for the caller to apply.
    settings : Any, optional
        Forwarded to derive_canonical_recommendation (used for
        horizon detection only — does not affect the recommendation
        math itself).
    overwrite : bool, default False
        - False (default): IDEMPOTENT mode. If the row already carries
          `recommendation_source == RECOMMENDATION_SOURCE_TAG`, returns
          an empty patch ({}). Safe to call from multiple pipeline
          stages without risk of stale recomputation.
        - True: Always recomputes. Use when you've just updated scores
          on the row and want to refresh the recommendation.

    Returns
    -------
    Dict[str, Any]
        Patch dict with keys:
            "recommendation"            — canonical 5-tier label
            "recommendation_reason"     — aligned reason text
            "recommendation_priority_band"   — P1..P5 bucket
            "recommendation_source"     — provenance tag
        Or empty dict ({}) when overwrite=False and row is already
        canonically scored.

    Example (downstream engine integration)
    ----------------------------------------
        # In investment_advisor_engine._score_and_rank_rows, replace:
        #     if _is_blank(r.get("recommendation")):
        #         r["recommendation"] = _recommendation_from_scores(...)
        # With:
        #     r.update(scoring.apply_canonical_recommendation(r))
        #
        # In data_engine_v2._compute_recommendation, replace the whole
        # body with:
        #     row.update(scoring.apply_canonical_recommendation(row))
    """
    existing_tag = _safe_str(row.get("recommendation_source"))
    if (not overwrite) and existing_tag == RECOMMENDATION_SOURCE_TAG:
        return {}

    reco, reason, priority = derive_canonical_recommendation(row, settings=settings)
    return {
        "recommendation": reco,
        "recommendation_reason": reason,
        "recommendation_priority_band": priority,
        "recommendation_source": RECOMMENDATION_SOURCE_TAG,
    }


# =============================================================================
# Main Scoring Function — v5.2.5 Phases K & L preserved + v5.2.6 Phase Q applied
#                       + v5.2.7 priority + provenance emission
# =============================================================================

def compute_scores(row: Dict[str, Any], settings: Any = None) -> Dict[str, Any]:
    """
    Score a single row.

    v5.2.7 changes (vs v5.2.6):
      - Emits `recommendation_priority_band` (P1..P5) computed by
        `_compute_priority` from the final canonical recommendation +
        the scored overall/risk/confidence/roi3 values. Same priority
        the downstream Recommendation Detail formatter should read.
      - Emits `recommendation_source = RECOMMENDATION_SOURCE_TAG`
        so downstream engines can detect this row was canonically
        scored and skip recomputation.
      - The insufficient_inputs path also gets a priority (P4) and the
        source tag — every output row carries the canonical fields.

    v5.2.6 changes (preserved):
      - Q. `compute_scores` reads the source row's `upside_pct` via
        `_as_upside_fraction` (shape-aware) in the v5.2.4 upside-
        suppression tracking block.

    v5.2.5 changes (preserved):
      - K. Engine-applied valuation clears surfaced.
      - L. last_error_class surfaced as "provider_error:<class>".

    v5.2.4 changes (preserved):
      - derive_upside_pct returns None for suspect upsides.
      - compute_valuation_score guards its upside computation.
      - derive_forecast_patch has a synthesis-ceiling guard.
      - error tracking for suspect upsides.

    [PRESERVED] All v5.2.0 / v5.2.1 / v5.2.2 / v5.2.3 / v5.2.4 / v5.2.5
    / v5.2.6 mechanics.
    """
    source = dict(row or {})
    scoring_errors: List[str] = []

    # v5.2.5 PHASE-L: surface last_error_class from providers.
    _err_raw = _get(source, "last_error_class", "lastErrorClass", "errorClass", "error_class")
    if _err_raw is not None:
        _err_class = _safe_str(_err_raw)
        if _err_class and _err_class.lower() not in {"none", "null", "nil", "nan", "n/a", "na"}:
            scoring_errors.append("provider_error:" + _err_class)

    # v5.2.5 PHASE-K: surface engine-applied valuation clears.
    if _row_engine_dropped_valuation(source):
        _src_intrinsic = _get_float(source, "intrinsic_value", "fair_value")
        if _src_intrinsic is None or _src_intrinsic <= 0:
            if "engine_dropped_valuation" not in scoring_errors:
                scoring_errors.append("engine_dropped_valuation")

    forecasts = _forecast_params_from_settings(settings)
    forecast_patch, forecast_errors = derive_forecast_patch(source, forecasts)
    scoring_errors.extend(forecast_errors)

    working = dict(source)
    working.update({k: v for k, v in forecast_patch.items() if v is not None})

    horizon, hdays = detect_horizon(settings, working)
    weights = get_weights_for_horizon(horizon, settings)

    valuation = compute_valuation_score(working)
    momentum = compute_momentum_score(working)
    quality = compute_quality_score(working)
    growth = compute_growth_score(working)
    confidence100, conf01 = compute_confidence_score(working)
    risk = compute_risk_score(working)
    # v5.3.0 FIX: Opportunity score provenance is emitted in opportunity_source.
    opportunity, opportunity_source = compute_opportunity_score_with_source(working, valuation, momentum)
    value_score = valuation

    tech_score = compute_technical_score(working)
    vol_ratio = derive_volume_ratio(working)
    drp = derive_day_range_position(working)
    usp = derive_upside_pct(working)

    # v5.2.4 + v5.2.6 Phase Q: Track scoring-layer upside suppression.
    if usp is None:
        _raw_intrinsic = _get_float(working, "intrinsic_value", "fair_value")
        _raw_price = _get_float(working, "current_price", "price", "last_price")

        reason_for_suppress = False

        if (
            _raw_intrinsic is not None and _raw_intrinsic > 0
            and _raw_price is not None and _raw_price > 0
        ):
            _raw_upside = (_raw_intrinsic - _raw_price) / _raw_price
            if _is_upside_suspect(_raw_upside):
                reason_for_suppress = True

        if not reason_for_suppress:
            # v5.2.6 Phase Q: shape-aware read of the source upside_pct.
            _supplied_usp = _as_upside_fraction(_get(source, "upside_pct"))
            if _supplied_usp is not None and _is_upside_suspect(_supplied_usp):
                reason_for_suppress = True

        if reason_for_suppress:
            if "upside_synthesis_suspect" not in scoring_errors:
                scoring_errors.append("upside_synthesis_suspect")

    rsi_val = _get_float(working, "rsi_14", "rsi", "rsi14")
    rsi_sig = rsi_signal(rsi_val)

    base_parts: List[Tuple[float, float]] = []
    if weights.w_technical > 0 and tech_score is not None:
        base_parts.append((weights.w_technical, tech_score / 100.0))
    if weights.w_valuation > 0 and valuation is not None:
        base_parts.append((weights.w_valuation, valuation / 100.0))
    if weights.w_momentum > 0 and momentum is not None:
        base_parts.append((weights.w_momentum, momentum / 100.0))
    if weights.w_quality > 0 and quality is not None:
        base_parts.append((weights.w_quality, quality / 100.0))
    if weights.w_growth > 0 and growth is not None:
        base_parts.append((weights.w_growth, growth / 100.0))
    if weights.w_opportunity > 0 and opportunity is not None:
        base_parts.append((weights.w_opportunity, opportunity / 100.0))

    overall: Optional[float] = None
    overall_raw: Optional[float] = None
    penalty_factor: Optional[float] = None
    insufficient_inputs = False

    sig_weight_total = sum(w for w, _ in base_parts)
    if base_parts and (len(base_parts) >= 2 or sig_weight_total >= 0.40):
        wsum = sig_weight_total
        base01 = sum(w * v for w, v in base_parts) / max(1e-9, wsum)
        overall_raw = _round(100.0 * _clamp(base01, 0.0, 1.0), 2)

        risk01 = (risk / 100.0) if risk is not None else 0.50
        conf01_used = conf01 if conf01 is not None else 0.55

        risk_pen = _clamp(1.0 - weights.risk_penalty_strength * (risk01 * 0.70), 0.0, 1.0)
        conf_pen = _clamp(1.0 - weights.confidence_penalty_strength * ((1.0 - conf01_used) * 0.80), 0.0, 1.0)
        penalty_factor = _round(risk_pen * conf_pen, 4)

        base01 *= (risk_pen * conf_pen)
        overall = _round(100.0 * _clamp(base01, 0.0, 1.0), 2)
    else:
        insufficient_inputs = True
        scoring_errors.append("insufficient_scoring_inputs")
        # v5.3.0 FIX: Default is nullable overall_score so missing data stays
        # visible to rankers. Env false gives emergency rollback to legacy 50.0.
        if _nullable_overall_enabled():
            overall = None
            overall_raw = None
            penalty_factor = None
        else:
            overall = 50.0
            overall_raw = 50.0
            penalty_factor = 1.0
        missing_components = [
            name for name, val in (
                ("technical_score", tech_score),
                ("valuation_score", valuation),
                ("momentum_score", momentum),
                ("quality_score", quality),
                ("growth_score", growth),
                ("opportunity_score", opportunity),
            ) if val is None
        ]
        logger.warning(
            "[v5.3.0 INSUFFICIENT] symbol=%s missing=%s",
            _safe_str(source.get("symbol") or source.get("ticker") or source.get("requested_symbol"), "UNKNOWN"),
            ",".join(missing_components),
        )

    rb = risk_bucket(risk)
    # v5.3.0 FIX: confidence_bucket is driven by final confidence_score, not
    # raw forecast_confidence. The helper accepts both 0-100 and 0-1.
    cb = confidence_bucket(confidence100)

    fundamental_view_raw = derive_fundamental_view(quality, growth)
    technical_view_raw = derive_technical_view(tech_score, momentum, rsi_sig)
    risk_view_raw = derive_risk_view(risk)
    value_view_raw = derive_value_view(valuation, usp)

    roi3 = _as_roi_fraction(working.get("expected_roi_3m"))
    roi1 = _as_roi_fraction(working.get("expected_roi_1m"))
    roi12 = _as_roi_fraction(working.get("expected_roi_12m"))

    # ---- v5.1.0: compute conviction BEFORE the recommendation -------
    conviction: Optional[float] = None
    ib = _import_insights_builder()
    if ib is not None and overall is not None:
        try:
            conviction = ib.compute_conviction_score(
                overall_score=overall,
                fundamental_view=fundamental_view_raw,
                technical_view=technical_view_raw,
                risk_view=risk_view_raw,
                value_view=value_view_raw,
                forecast_confidence=conf01,
                completeness=_completeness_factor(working),
            )
        except Exception as exc:
            logger.debug("compute_conviction_score failed: %s", exc)
            scoring_errors.append(f"conviction_failed: {type(exc).__name__}")

    if insufficient_inputs and _nullable_overall_enabled():
        # v5.3.0 FIX: Let the authoritative recommendation helper format the
        # structured insufficient-data reason.
        rec, reason = compute_recommendation(
            None, risk, confidence100, roi3,
            horizon=horizon, technical=tech_score, momentum=momentum,
            roi1=roi1, roi12=roi12, quality=quality, growth=growth,
            valuation=valuation, fundamental_view=fundamental_view_raw,
            technical_view=technical_view_raw, risk_view=risk_view_raw,
            value_view=value_view_raw, upside_pct=usp, rsi_label=rsi_sig,
            conviction=conviction, sector_relative=None,
        )
    else:
        rec, reason = compute_recommendation(
            overall, risk, confidence100, roi3,
            horizon=horizon, technical=tech_score, momentum=momentum,
            roi1=roi1, roi12=roi12,
            quality=quality,
            growth=growth,
            valuation=valuation,
            fundamental_view=fundamental_view_raw,
            technical_view=technical_view_raw,
            risk_view=risk_view_raw,
            value_view=value_view_raw,
            upside_pct=usp,
            rsi_label=rsi_sig,
            conviction=conviction,
            sector_relative=None,
        )

    canonical_rec = normalize_recommendation_code(rec)
    reason = _align_reason_to_canonical_recommendation(reason, canonical_rec)

    # v5.2.7: Centralized priority computation. Same _compute_priority
    # call that derive_canonical_recommendation/apply_canonical_recommendation
    # use, guaranteeing the top-line `Recommendation` column and the
    # downstream `Recommendation Detail` priority bucket can never
    # diverge by construction.
    priority = _compute_priority(canonical_rec, overall, risk, confidence100, roi3)

    st_signal_val = short_term_signal(tech_score, momentum, risk, horizon)
    period_label = invest_period_label(horizon, hdays)

    # ---- v5.1.0: build per-row insights ------------------------------
    top_factors_str: str = ""
    top_risks_str: str = ""
    pos_hint: str = ""
    structured_reason: str = reason

    if ib is not None and not insufficient_inputs:
        try:
            synthetic_row: Dict[str, Any] = dict(working)
            synthetic_row.update({
                "overall_score": overall,
                "valuation_score": valuation,
                "momentum_score": momentum,
                "quality_score": quality,
                "growth_score": growth,
                "value_score": value_score,
                "opportunity_score": opportunity,
                "technical_score": tech_score,
                "risk_score": risk,
                "fundamental_view": fundamental_view_raw,
                "technical_view": technical_view_raw,
                "risk_view": risk_view_raw,
                "value_view": value_view_raw,
                "forecast_confidence": conf01,
                "recommendation": canonical_rec,
                "recommendation_reason": reason,
            })

            bundle = ib.build_insights(
                synthetic_row,
                sector_scores=None,
                weights=weights.as_factor_weights_map(),
                base_reason=reason,
            )
            top_factors_str = bundle.top_factors or ""
            top_risks_str = bundle.top_risks or ""
            pos_hint = bundle.position_size_hint or ""
            structured_reason = bundle.recommendation_reason or reason
        except Exception as exc:
            logger.debug("build_insights failed for row: %s", exc)
            scoring_errors.append(f"insights_failed: {type(exc).__name__}")

    structured_reason = _align_reason_to_canonical_recommendation(
        structured_reason, canonical_rec
    )
    # v5.3.0 FIX: Insights may return legacy prose; force the final reason
    # back into the structured scoring contract unless legacy mode is enabled.
    if _structured_reason_enabled():
        structured_reason = _format_recommendation_reason(
            canonical_rec,
            structured_reason.split(":", 1)[1].split("|", 1)[0].strip()
            if structured_reason.startswith(canonical_rec + ":") else structured_reason,
            overall, risk, confidence100, roi3,
        )

    logger.info(
        "[v5.3.0 SCORE] symbol=%s overall=%s risk=%s conf=%s rec=%s",
        _safe_str(source.get("symbol") or source.get("ticker") or source.get("requested_symbol"), "UNKNOWN"),
        overall, risk, confidence100, canonical_rec,
    )

    scores = AssetScores(
        valuation_score=valuation,
        momentum_score=momentum,
        quality_score=quality,
        growth_score=growth,
        value_score=value_score,
        opportunity_score=opportunity,
        opportunity_source=opportunity_source,
        confidence_score=confidence100,
        forecast_confidence=conf01,
        confidence_bucket=cb,
        risk_score=risk,
        risk_bucket=rb,
        overall_score=overall,
        overall_score_raw=overall_raw,
        overall_penalty_factor=penalty_factor,
        technical_score=tech_score,
        rsi_signal=rsi_sig,
        short_term_signal=st_signal_val,
        day_range_position=drp,
        volume_ratio=vol_ratio,
        upside_pct=usp,
        invest_period_label=period_label,
        horizon_label=horizon.value,
        horizon_days_effective=hdays,
        fundamental_view=_view_or_na(fundamental_view_raw),
        technical_view=_view_or_na(technical_view_raw),
        risk_view=_view_or_na(risk_view_raw),
        value_view=_view_or_na(value_view_raw),
        recommendation=canonical_rec,
        recommendation_reason=structured_reason,
        forecast_price_1m=forecast_patch.get("forecast_price_1m"),
        forecast_price_3m=forecast_patch.get("forecast_price_3m"),
        forecast_price_12m=forecast_patch.get("forecast_price_12m"),
        expected_roi_1m=forecast_patch.get("expected_roi_1m"),
        expected_roi_3m=forecast_patch.get("expected_roi_3m"),
        expected_roi_12m=forecast_patch.get("expected_roi_12m"),
        expected_return_1m=forecast_patch.get("expected_return_1m"),
        expected_return_3m=forecast_patch.get("expected_return_3m"),
        expected_return_12m=forecast_patch.get("expected_return_12m"),
        expected_price_1m=forecast_patch.get("expected_price_1m"),
        expected_price_3m=forecast_patch.get("expected_price_3m"),
        expected_price_12m=forecast_patch.get("expected_price_12m"),
        sector_relative_score=None,
        conviction_score=conviction,
        top_factors=top_factors_str,
        top_risks=top_risks_str,
        position_size_hint=pos_hint,
        scoring_errors=scoring_errors,
        # v5.2.7 additions
        recommendation_priority_band=priority,
        recommendation_source=RECOMMENDATION_SOURCE_TAG,
        scoring_schema_version=__version__,
    )
    return scores.to_dict()


def score_row(row: Dict[str, Any], settings: Any = None) -> Dict[str, Any]:
    return compute_scores(row, settings=settings)


def score_quote(row: Dict[str, Any], settings: Any = None) -> Dict[str, Any]:
    return compute_scores(row, settings=settings)


def enrich_with_scores(
    row: Dict[str, Any],
    settings: Any = None,
    in_place: bool = False,
) -> Dict[str, Any]:
    target = row if in_place else dict(row or {})
    patch = compute_scores(target, settings=settings)
    target.update(patch)
    return target


class ScoringEngine:
    """Lightweight wrapper supporting object-style callers."""

    version = SCORING_VERSION

    def __init__(
        self,
        settings: Any = None,
        weights: Optional[ScoreWeights] = None,
        forecasts: Optional[ForecastParameters] = None,
    ):
        self.settings = settings
        self.weights = weights or DEFAULT_SCORING_WEIGHTS
        self.forecasts = forecasts or DEFAULT_FORECAST_PARAMETERS

    def compute_scores(self, row: Dict[str, Any]) -> Dict[str, Any]:
        return compute_scores(row, settings=self.settings)

    def enrich_with_scores(self, row: Dict[str, Any], in_place: bool = False) -> Dict[str, Any]:
        return enrich_with_scores(row, settings=self.settings, in_place=in_place)

    def apply_canonical_recommendation(
        self,
        row: Dict[str, Any],
        overwrite: bool = False,
    ) -> Dict[str, Any]:
        """v5.2.7: Object-style alias for the module-level helper."""
        return apply_canonical_recommendation(
            row, settings=self.settings, overwrite=overwrite,
        )


# =============================================================================
# Ranking + Batch Scoring (preserved from v5.2.0, with v5.2.7 priority sync)
# =============================================================================

def _rank_sort_tuple(row: Dict[str, Any], key_overall: str = "overall_score") -> Tuple[float, ...]:
    overall = _norm_score_0_100(row.get(key_overall))
    opp = _norm_score_0_100(row.get("opportunity_score"))
    conf = _norm_score_0_100(row.get("confidence_score"))
    risk = _norm_score_0_100(row.get("risk_score"))
    roi3 = _as_roi_fraction(row.get("expected_roi_3m"))
    symbol = _safe_str(row.get("symbol"), "~")
    return (
        overall if overall is not None else -1e9,
        opp if opp is not None else -1e9,
        conf if conf is not None else -1e9,
        -(risk if risk is not None else 1e9),
        roi3 if roi3 is not None else -1e9,
        symbol,
    )


def assign_rank_overall(
    rows: Sequence[Dict[str, Any]],
    key_overall: str = "overall_score",
    inplace: bool = True,
    rank_key: str = "rank_overall",
) -> List[Dict[str, Any]]:
    target = list(rows) if inplace else [dict(r or {}) for r in rows]
    indexed = list(enumerate(target))
    indexed.sort(key=lambda item: _rank_sort_tuple(item[1], key_overall=key_overall), reverse=True)
    for rank, (_, row) in enumerate(indexed, start=1):
        row[rank_key] = rank
    return target


def rank_rows_by_overall(
    rows: List[Dict[str, Any]],
    key_overall: str = "overall_score",
) -> List[Dict[str, Any]]:
    return assign_rank_overall(rows, key_overall=key_overall, inplace=True, rank_key="rank_overall")


def score_and_rank_rows(
    rows: Sequence[Dict[str, Any]],
    settings: Any = None,
    key_overall: str = "overall_score",
    inplace: bool = False,
) -> List[Dict[str, Any]]:
    """
    Score every row and rank by overall_score.

    [PRESERVED] After per-row scoring, runs a batch-level pass via
    insights_builder.enrich_rows_with_insights() to compute
    sector_relative_score and rebuild recommendation_reason with
    sector-adjusted badges.

    [PRESERVED] After insights_builder returns, each row's
    recommendation_reason is re-aligned via
    _align_reason_to_canonical_recommendation, and view fields are
    coerced via _view_or_na.

    v5.2.7: After the post-insights alignment pass, each row's
    `recommendation_priority_band` is recomputed via `_compute_priority`
    so the priority bucket reflects any change to the canonical
    recommendation from the sector-adjusted re-evaluation. The
    `recommendation_source` provenance tag is also (re)written so
    rows that came in pre-scored from compute_scores AND rows that
    were modified by insights_builder both carry the v5.2.7 tag.
    """
    prepared = list(rows) if inplace else [dict(r or {}) for r in rows]

    for row in prepared:
        try:
            row.update(compute_scores(row, settings=settings))
        except Exception as exc:
            logger.debug("score_and_rank_rows: scoring failed for row: %s", exc)
            existing_errors = row.get("scoring_errors")
            if not isinstance(existing_errors, list):
                existing_errors = []
            existing_errors.append(f"scoring_exception: {type(exc).__name__}")
            row["scoring_errors"] = existing_errors

    ib = _import_insights_builder()
    if ib is not None:
        try:
            horizon_for_weights = Horizon.MONTH
            for r in prepared:
                hl = r.get("horizon_label")
                if hl:
                    try:
                        horizon_for_weights = Horizon(hl)
                        break
                    except ValueError:
                        continue
            weights = get_weights_for_horizon(horizon_for_weights, settings)
            ib.enrich_rows_with_insights(
                prepared,
                weights=weights.as_factor_weights_map(),
                sector_key="sector",
                inplace=True,
            )
        except Exception as exc:
            logger.debug("score_and_rank_rows: batch insights failed: %s", exc)

    for row in prepared:
        rec = row.get("recommendation")
        reason = row.get("recommendation_reason")
        if rec and reason:
            canonical_rec = normalize_recommendation_code(rec)
            row["recommendation"] = canonical_rec
            row["recommendation_reason"] = _align_reason_to_canonical_recommendation(
                str(reason), canonical_rec
            )

            # v5.2.7: keep priority in sync with the (possibly re-aligned)
            # canonical recommendation, and reaffirm the provenance tag.
            # Reads the row's current overall/risk/confidence/roi3 — which
            # may have been adjusted by the batch insights pass via
            # sector_relative_score — so the priority reflects the final
            # post-batch state.
            _ov = _norm_score_0_100(row.get("overall_score"))
            _rk = _norm_score_0_100(row.get("risk_score"))
            _cf = _norm_score_0_100(row.get("confidence_score"))
            _r3 = _as_roi_fraction(row.get("expected_roi_3m"))
            row["recommendation_priority_band"] = _compute_priority(
                canonical_rec, _ov, _rk, _cf, _r3,
            )
            row["recommendation_source"] = RECOMMENDATION_SOURCE_TAG

        for view_key in ("fundamental_view", "technical_view", "risk_view", "value_view"):
            if view_key in row:
                row[view_key] = _view_or_na(row.get(view_key))

    assign_rank_overall(prepared, key_overall=key_overall, inplace=True, rank_key="rank_overall")
    return prepared


# =============================================================================
# Module Exports
# =============================================================================

__all__ = [
    "__version__",
    "SCORING_VERSION",
    "Horizon",
    "Signal",
    "RSISignal",
    "compute_scores",
    "score_row",
    "score_quote",
    "enrich_with_scores",
    "rank_rows_by_overall",
    "assign_rank_overall",
    "score_and_rank_rows",
    "AssetScores",
    "ScoreWeights",
    "ScoringWeights",
    "ForecastParameters",
    "ScoringEngine",
    "DEFAULT_WEIGHTS",
    "DEFAULT_FORECASTS",
    "DEFAULT_SCORING_WEIGHTS",
    "DEFAULT_FORECAST_PARAMETERS",
    "RECOMMENDATION_ENUM",
    "RISK_BUCKET_THRESHOLDS",
    "CONFIDENCE_BUCKET_THRESHOLDS",
    "normalize_recommendation_code",
    "CANONICAL_RECOMMENDATION_CODES",
    "detect_horizon",
    "get_weights_for_horizon",
    "compute_technical_score",
    "rsi_signal",
    "short_term_signal",
    "derive_upside_pct",
    "derive_volume_ratio",
    "derive_day_range_position",
    "invest_period_label",
    "compute_valuation_score",
    "compute_growth_score",
    "compute_momentum_score",
    "compute_quality_score",
    "compute_risk_score",
    "compute_opportunity_score",
    "compute_opportunity_score_with_source",
    "compute_confidence_score",
    "compute_recommendation",
    "risk_bucket",
    "confidence_bucket",
    "_recommendation",
    "_risk_bucket",
    "_confidence_bucket",
    "derive_fundamental_view",
    "derive_technical_view",
    "derive_risk_view",
    "derive_value_view",
    "ScoringError",
    "InvalidHorizonError",
    "MissingDataError",
    "score_views_completeness",
    # v5.2.7 additions
    "derive_canonical_recommendation",
    "apply_canonical_recommendation",
    "RECOMMENDATION_SOURCE_TAG",
    "CANONICAL_PRIORITIES",
    "PRIO_P1",
    "PRIO_P2",
    "PRIO_P3",
    "PRIO_P4",
    "PRIO_P5",
    # v5.2.9 additions
    "BUCKETS_CANONICAL",
]
