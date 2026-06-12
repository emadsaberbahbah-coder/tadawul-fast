#!/usr/bin/env python3
# core/analysis/trend_signals.py
"""
================================================================================
Trend Signals — cross-sectional sector trend computation — v1.0.0
TFB Final Execution Plan v5.0 — Phase P9
================================================================================

THE PROBLEM (P9 known issue)
----------------------------
opportunity_builder v1.0.x reads `sector_trend` / `news_trend` off each
candidate row and maps them Positive 100 / Neutral 60 / Unknown 40 /
Negative 0 (plan-fixed), with the §4.2 gates "not Negative (Unknown
passes)". But NOTHING in the live pipeline supplies those fields: the 115
schema does not carry them and the GAS pool (DT10_POOL_FIELDS) does not send
them. Result: every candidate scores trend 40/40 and the Allow Negative
News / Allow Negative Sector panel switches are dead controls.

WHAT THIS MODULE DOES
---------------------
Computes SECTOR TREND cross-sectionally from the candidate pool itself —
no external calls, no provider quota, free-tier safe, deterministic:

  cohort  = candidates sharing a normalized sector (blank/Unknown excluded)
  signal  = median(Expected ROI 12M over the cohort)            [forward]
  breadth = share of cohort with a sell-tier recommendation      [consensus]
            (SELL / STRONG SELL / REDUCE / EXIT / AVOID / UNDERPERFORM)

  classification (defaults; env-tunable):
    cohort < TFB_TREND_MIN_COHORT (3)                      -> Unknown (honest)
    median ROI <= TFB_TREND_NEG_ROI (0)  OR breadth >= 50% -> Negative
    median ROI >= TFB_TREND_POS_ROI (10) AND breadth <= 20% -> Positive
    otherwise                                               -> Neutral

`enrich_rows_with_trends(rows)` stamps 'Sector Trend' onto each row dict
ONLY where the row does not already carry a sector-trend alias — provider-
supplied values are never overwritten. Rows are mutated in place AND
returned (route convenience).

WHAT IT DELIBERATELY DOES NOT DO
--------------------------------
NEWS TREND IS NOT FABRICATED. There is no news source in the request path,
and inventing sentiment from price data would launder ignorance into
confidence (L13). News stays "Unknown" until a real news provider lands
(engine-side, future phase); this module only reports how many rows carry a
news value so the coverage is visible in meta.

ROLLOUT SAFETY
--------------
Env-gated, DEFAULT OFF (TFB house rule for behavior-changing logic):
    TFB_TREND_SIGNALS=1        enable enrichment
When disabled, enrich_rows_with_trends returns the rows UNTOUCHED with
meta {"enabled": False} — deploying this module + the route binding changes
nothing until the flag is set.

Tuning env (all optional):
    TFB_TREND_MIN_COHORT   "3"    min sector cohort for a verdict
    TFB_TREND_POS_ROI      "10"   median ROI %  >= -> Positive (w/ breadth)
    TFB_TREND_NEG_ROI      "0"    median ROI %  <= -> Negative
    TFB_TREND_NEG_BREADTH  "0.50" sell-share    >= -> Negative
    TFB_TREND_POS_BREADTH  "0.20" sell-share    <= allowed for Positive

Stdlib-only. Import-safe. Pure compute; no I/O.
================================================================================
"""

from __future__ import annotations

import os
import re
import statistics
from typing import Any, Dict, List, Mapping, Optional, Tuple

TREND_SIGNALS_VERSION = "1.0.0"

# Builder-recognized aliases (mirror opportunity_builder _FIELD_ALIASES —
# bump together if they ever change).
_SECTOR_TREND_ALIASES = ("sectortrend", "sectorsignal", "sectormomentum")
_NEWS_TREND_ALIASES = ("newstrend", "newssentiment", "newssignal")
_SECTOR_ALIASES = ("sector", "gicssector", "industrysector")
_ROI_ALIASES = ("expectedroi12m", "expectedroi", "forecastroi12m",
                "engineroi12m", "expectedroipct")
_RECO_ALIASES = ("recommendationdetail", "recommendationdetailed",
                 "recommendation", "reco", "recommendationcanonical")

_SELL_TIER = ("sell", "strongsell", "strong sell", "reduce", "exit",
              "avoid", "underperform")

# The key stamped onto rows. Normalizes to "sectortrend" -> first builder
# alias, so the builder picks it up without any change on its side.
STAMP_KEY = "Sector Trend"


def _norm_key(k: Any) -> str:
    return re.sub(r"[^a-z0-9]+", "", str(k or "").lower())


def _row_field(row: Mapping[str, Any], aliases: Tuple[str, ...]) -> Any:
    """First non-empty value among normalized-key aliases."""
    if not isinstance(row, Mapping):
        return None
    by_norm: Dict[str, Any] = {}
    for k, v in row.items():
        nk = _norm_key(k)
        if nk and nk not in by_norm:
            by_norm[nk] = v
    for a in aliases:
        v = by_norm.get(a)
        if v is None:
            continue
        s = str(v).strip()
        if s and s.lower() not in ("none", "null", "nan", "-", "n/a", "na"):
            return v
    return None


def _to_float(v: Any) -> Optional[float]:
    if v is None:
        return None
    if isinstance(v, bool):
        return None
    if isinstance(v, (int, float)):
        f = float(v)
        return f if f == f and abs(f) != float("inf") else None
    s = str(v).strip().replace(",", "").replace("%", "")
    if not s:
        return None
    try:
        f = float(s)
        return f if f == f and abs(f) != float("inf") else None
    except ValueError:
        return None


def _env_float(name: str, default: float) -> float:
    raw = os.environ.get(name, "")
    try:
        return float(raw) if str(raw).strip() else default
    except (TypeError, ValueError):
        return default


def _env_int(name: str, default: int) -> int:
    raw = os.environ.get(name, "")
    try:
        return int(str(raw).strip()) if str(raw).strip() else default
    except (TypeError, ValueError):
        return default


def trend_signals_enabled() -> bool:
    return str(os.environ.get("TFB_TREND_SIGNALS", "")).strip() in (
        "1", "true", "True", "yes", "on")


def _is_sell_tier(reco: Any) -> bool:
    token = re.sub(r"[^a-z ]+", "", str(reco or "").lower()).strip()
    if not token:
        return False
    compact = token.replace(" ", "")
    for tier in _SELL_TIER:
        if compact == tier.replace(" ", "") or token.startswith(tier):
            return True
    return False


def _norm_sector(v: Any) -> str:
    s = str(v or "").strip()
    if not s or s.lower() in ("unknown", "none", "null", "n/a", "na", "-"):
        return ""
    return s


def compute_sector_trends(rows: List[Mapping[str, Any]]) -> Dict[str, Dict[str, Any]]:
    """Per-sector cross-sectional classification.

    Returns {sector: {"trend", "median_roi_pct", "sell_breadth",
                      "cohort"}}. Sectors below the min cohort are returned
    with trend "Unknown" (visible, honest) rather than omitted.
    """
    min_cohort = max(1, _env_int("TFB_TREND_MIN_COHORT", 3))
    pos_roi = _env_float("TFB_TREND_POS_ROI", 10.0)
    neg_roi = _env_float("TFB_TREND_NEG_ROI", 0.0)
    neg_breadth = _env_float("TFB_TREND_NEG_BREADTH", 0.50)
    pos_breadth = _env_float("TFB_TREND_POS_BREADTH", 0.20)

    cohorts: Dict[str, Dict[str, List[Any]]] = {}
    for row in rows or []:
        if not isinstance(row, Mapping):
            continue
        sector = _norm_sector(_row_field(row, _SECTOR_ALIASES))
        if not sector:
            continue
        c = cohorts.setdefault(sector, {"roi": [], "sell": []})
        roi = _to_float(_row_field(row, _ROI_ALIASES))
        if roi is not None:
            c["roi"].append(roi)
        c["sell"].append(_is_sell_tier(_row_field(row, _RECO_ALIASES)))

    out: Dict[str, Dict[str, Any]] = {}
    for sector, c in cohorts.items():
        cohort_n = len(c["sell"])
        median_roi = (round(statistics.median(c["roi"]), 4)
                      if c["roi"] else None)
        breadth = (round(sum(1 for s in c["sell"] if s) / cohort_n, 4)
                   if cohort_n else None)
        if cohort_n < min_cohort or median_roi is None:
            trend = "Unknown"
        elif median_roi <= neg_roi or (breadth is not None and
                                       breadth >= neg_breadth):
            trend = "Negative"
        elif median_roi >= pos_roi and (breadth is None or
                                        breadth <= pos_breadth):
            trend = "Positive"
        else:
            trend = "Neutral"
        out[sector] = {
            "trend": trend,
            "median_roi_pct": median_roi,
            "sell_breadth": breadth,
            "cohort": cohort_n,
        }
    return out


def enrich_rows_with_trends(
    rows: List[Dict[str, Any]],
) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    """Stamp computed 'Sector Trend' onto rows lacking one. In-place + returned.

    meta: {"version", "enabled", "rows", "stamped", "already_supplied",
           "no_sector", "news_supplied", "sectors": {sector: {...}}}
    """
    meta: Dict[str, Any] = {
        "version": TREND_SIGNALS_VERSION,
        "enabled": trend_signals_enabled(),
        "rows": len(rows or []),
        "stamped": 0,
        "already_supplied": 0,
        "no_sector": 0,
        "news_supplied": 0,
        "sectors": {},
    }
    if not meta["enabled"] or not rows:
        return rows or [], meta

    sectors = compute_sector_trends(rows)
    meta["sectors"] = sectors

    for row in rows:
        if not isinstance(row, dict):
            continue
        if _row_field(row, _NEWS_TREND_ALIASES) is not None:
            meta["news_supplied"] += 1
        if _row_field(row, _SECTOR_TREND_ALIASES) is not None:
            meta["already_supplied"] += 1     # provider value wins; no stamp
            continue
        sector = _norm_sector(_row_field(row, _SECTOR_ALIASES))
        if not sector or sector not in sectors:
            meta["no_sector"] += 1
            continue
        trend = sectors[sector]["trend"]
        if trend == "Unknown":
            continue                          # stamping Unknown adds nothing
        row[STAMP_KEY] = trend
        meta["stamped"] += 1
    return rows, meta


__all__ = ["TREND_SIGNALS_VERSION", "trend_signals_enabled",
           "compute_sector_trends", "enrich_rows_with_trends", "STAMP_KEY"]
