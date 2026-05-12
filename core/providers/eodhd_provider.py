#!/usr/bin/env python3
# core/providers/eodhd_provider.py
"""
================================================================================
EODHD Provider — v4.9.1 (ENGINE PICK-ORDER FIX + PER-FAILURE PROVIDER-UNHEALTHY
                          SIGNAL + CIRCUIT BREAKER + HEALTH PROBE + SESSION
                          COUNTERS + MAY 2026 / v2.8.0 FAMILY ALIGNMENT)
================================================================================

v4.9.1 — ENGINE PICK-ORDER FIX (May 12, 2026)
-----------------------------------------------------------------------
v4.9.0 added `provider_unhealthy:eodhd` to every auth-class error patch
via `_build_error_patch_with_geo`. Post-deploy verification confirmed
those markers are NEVER appearing in production sheet rows or in
`/v1/schema/provider-health.total_observations` — even though the
provider is clearly producing the markers in its error patches.

Root cause: engine v5.61.0's `_fetch_patch` discovers a provider's
callable via `_pick_provider_callable` with this preference order:

    ("get_quote_patch", "fetch_quote", "get_quote", "fetch_patch")

eodhd_provider does NOT define `get_quote_patch`, so the engine falls
through to `fetch_quote`. The module-level `fetch_quote` is a thin
wrapper around `quote()` which **raises RuntimeError on err**:

    async def quote(symbol):
        patch, err = await client.fetch_quote(symbol)
        if err:
            raise RuntimeError(err)   # ← discards the error patch
        return patch

The engine catches the exception and returns None:

    try:
        patch = await _call_maybe_async(fn, symbol)
    except Exception as exc:
        await self.providers.record_failure(provider_name, str(exc))
        return None   # ← engine never sees the v4.9.0 marker

So the marker is correctly emitted by `_build_error_patch_with_geo`
and travels through `client.fetch_quote` → `client.fetch_quote_patch`
intact, but the engine never calls `fetch_quote_patch`. It calls the
raising variant and the patch is thrown on the floor.

This also explains the cross-provider asymmetry observed in production:
yahoo's failure warnings (`quote:SYMBOL:HTTP 403`) DO surface on the
sheet because yahoo_provider exposes `get_quote_patch` (which the
engine matches first). eodhd's failures don't surface because the
engine's first matching name (`fetch_quote`) raises instead.

v4.9.1 closes the gap with a single-line behavioral change:

  AL  NEW module-level `get_quote_patch(symbol, ...)` alias.
       Routes to the existing `fetch_quote_patch` so engine's
       `_pick_provider_callable` will match `get_quote_patch` first
       (its preferred name) and receive the error patch with the
       v4.9.0 `provider_unhealthy:eodhd` marker intact. Added to
       `__all__`. No behavioral change to any existing function —
       `fetch_quote` / `quote` / `get_quote` still raise on err
       (their historical contract is preserved).

  AM  Runtime log tags bumped `[eodhd v4.9.0]` -> `[eodhd v4.9.1]`.

  AN  Version bumps: PROVIDER_VERSION="4.9.1", UA_DEFAULT updated,
       file section header updated.

[PRESERVED — strictly] All v4.9.0 / v4.8.0 / v4.7.x / v4.6.0 behavior,
signatures, env vars, cache keys, schema fields, alias mappings, method
names, AND the v4.8.0 circuit-breaker state machine + thresholds +
half_open probe + diagnose_health() + get_provider_stats(). v4.9.1 is
PURELY ADDITIVE: a single new function `get_quote_patch` plus version
bumps. Public API surface unchanged (extension only). No removals from
__all__; one addition.

================================================================================

v4.9.0 — PER-FAILURE PROVIDER-UNHEALTHY SIGNAL (May 12, 2026)
-----------------------------------------------------------------------
v4.8.0 added a circuit breaker that opens after 8 consecutive AuthError
or IpBlocked failures and, on entering the OPEN state, emits the
cross-provider `provider_unhealthy:eodhd` marker via
`_build_circuit_open_patch()`. Engine v5.61.0+ consumes that marker
through its `_ProviderHealthRegistry` and routes around the unhealthy
primary on subsequent fetches.

In production, however, the breaker NEVER reaches its 8-consecutive
threshold. Each symbol triggers three concurrent calls in
`fetch_enriched_quote_patch` — `real-time/`, `fundamentals/`, and
`eod/` — and in many real failure modes only SOME endpoints 403 while
others return 200. `_ProviderHealth.record_success()` resets
`consecutive_auth_errors` to 0 on every successful call, so the
threshold is functionally unreachable while partial failures persist.
Live diagnostics confirm this: the engine's
`provider_unhealthy_markers.total_observations` stays at 0 even when
every row's Warnings column carries HTTP 403 markers from `eod/` calls
and downstream fields collapse silently.

v4.9.0 closes the gap by emitting the unhealthy marker on EACH
individual auth-class failure, not just when the breaker opens:

  AH  `_build_error_patch_with_geo` enhancement.
       When the error code indicates `auth_error` or `ip_blocked`, the
       function now adds `provider_unhealthy:eodhd` to the patch's
       warnings list, alongside the existing `fetch_failed:<err>` tag.
       Engine v5.61.0+'s `_observe_provider_unhealthy_markers` reads
       this marker and calls `_ProviderHealthRegistry.record_unhealthy(
       "eodhd")` with a 300s TTL (default, env-configurable via
       `ENGINE_PROVIDER_UNHEALTHY_TTL_SEC`). The registry is
       idempotent: subsequent auth failures within the TTL refresh it;
       after 300s with no auth errors the marker auto-expires and the
       engine restores eodhd to its preferred chain position.

       Granularity by error class:
         - AuthError    -> EMIT (decisive provider-broken signal)
         - IpBlocked    -> EMIT (decisive provider-broken signal)
         - RateLimited  -> do NOT emit (transient, retries handle it)
         - NotFound     -> do NOT emit (symbol-specific, not provider)
         - NetworkError -> do NOT emit (transient)
         - CircuitOpen  -> EMIT via _build_circuit_open_patch (existing
                           v4.8.0 behavior, preserved verbatim)

  AI  `fetch_quote_patch` alias fallback path.
       The fallback error patch built in `fetch_quote_patch` when
       `fetch_quote` returns empty (historically dead code in v4.8.0
       because fetch_quote always returns a non-empty patch) now also
       carries `provider_unhealthy:eodhd` when the upstream error
       indicates an auth-class failure. Preserves consistency with
       `_build_error_patch_with_geo`.

  AJ  Runtime log tags updated to `[eodhd v4.9.0]`.
       Helps operators correlate logs to the active version. Existing
       structured-diagnostics log lines from v4.8.0 are preserved
       byte-identical except for the version tag.

  AK  Version bump to 4.9.0 across PROVIDER_VERSION, UA_DEFAULT, and
       the file's section header. VERSION constant follows.

[PRESERVED — strictly] All v4.8.0 / v4.7.3 / v4.7.2 / v4.7.1 / v4.7.0 /
v4.6.0 behavior, signatures, env vars, cache keys, schema fields,
alias mappings, method names, AND the v4.8.0 circuit-breaker state
machine + thresholds + half_open probe behavior + diagnose_health() +
get_provider_stats(). v4.9.0 is purely additive: the breaker still
trips at 8-consecutive (and still emits `circuit_open` then), but
auth/IP failures now signal the engine immediately via the warning
marker WITHOUT waiting for the threshold. Public API surface
unchanged. No removals from __all__.

No new env variables in v4.9.0. The marker's TTL and demote/skip
semantics live on the engine side (v5.61.0+) and are governed by:
  - ENGINE_PROVIDER_UNHEALTHY_TTL_SEC  (default: 300)
  - ENGINE_PROVIDER_UNHEALTHY_DEMOTE   (default: 1)
  - ENGINE_PROVIDER_UNHEALTHY_SKIP     (default: 0)

================================================================================

v4.8.0 — OPERATOR-VISIBLE PROVIDER HEALTH (May 12, 2026)  [PRESERVED]
-----------------------------------------------------------------------
v4.7.3 had excellent per-request 403 body parsing (auth_error /
quota_or_rate_limit / ip_blocked classification) and cross-provider
warnings alignment, but operators had no way to detect the
"systematically broken" state where every request silently 403s.
Without that signal, the engine kept hammering EODHD for the full
~1,929-symbol refresh batch while every row fell back to Yahoo —
masking what was actually a single root-cause auth failure as
"~30 downstream catchall fields per row".

v4.8.0 added the missing surface:

  AA  NEW `_ProviderHealth` async-safe state class.
       Tracks consecutive auth/IP failures and cumulative session
       counters. Lives at module level (singleton scope, in-memory,
       reset on process restart). All mutations are guarded by an
       asyncio.Lock so concurrent fetches don't race.

       Tracked state:
         - circuit_state: "closed" | "open" | "half_open"
         - consecutive_auth_errors
         - consecutive_ip_blocks
         - last_success_at / last_failure_at / last_failure_class
         - circuit_opened_at
         - Cumulative counters: total_requests, success_count,
           auth_error_count, ip_block_count, rate_limit_count,
           network_error_count

       Method surface:
         - await begin_request()   -> True if request can proceed
         - await record_success()
         - await record_failure(error_class)
         - await snapshot()        -> dict
         - await is_open()         -> bool

  AB  NEW circuit-breaker integration in `_request_json`.
       Before each HTTP call, `_request_json` checks the health
       state via `begin_request()`. If the circuit is OPEN, returns
       immediately with `("circuit_open", "circuit_open")` — no API
       hit, no retry, no token bucket consumption. After the call,
       success/failure is recorded via `record_*` so the breaker can
       open or close itself based on actual outcomes.

       Default thresholds (env-configurable):
         - EODHD_CIRCUIT_BREAKER_ENABLE = 1
         - EODHD_CIRCUIT_AUTH_THRESHOLD = 8
           (consecutive auth_errors OR ip_blocks before opening)
         - EODHD_CIRCUIT_BACKOFF_SEC = 300  (5 minutes)

       Lifecycle:
         closed --(N consecutive AuthError|IpBlocked)--> open
         open   --(BACKOFF_SEC elapsed)----------------> half_open
         half_open --(1 success)-----------------------> closed
         half_open --(1 failure)-----------------------> open

  AC  NEW `circuit_open` warnings on patches.
       When the circuit is OPEN, every patch returned by
       fetch_quote / fetch_fundamentals / fetch_history_stats
       carries:
         - warnings: ["circuit_open", "provider_unhealthy:eodhd"]
         - error: "circuit_open"
         - last_error_class: "CircuitOpen"
         - data_quality: "MISSING"

       The engine's provider chain logic can detect this marker
       and route subsequent fetches directly to Yahoo without
       waiting for N more 403s.

  AD  NEW module-level `diagnose_health()` function.
       Probes a known-stable symbol (default AAPL.US, configurable
       via EODHD_HEALTH_PROBE_SYMBOL) and returns a structured
       health report suitable for ops dashboards or operator menu
       items.

  AE  NEW module-level `get_provider_stats()` function.
       Returns the cumulative session counters as a dict. Lightweight,
       no API call.

================================================================================

v4.7.3 — CROSS-PROVIDER ALIGNMENT (May 11, 2026)  [PRESERVED]
-----------------------------------------------------------------------
  ZA  Top-level `warnings: List[str]` on every patch.
  ZB  Currency-sanity guard on merged 52W bounds.
  ZC  `last_error_class` field on full-fail returns.
  ZD  Version bump to 4.7.3.

v4.7.2 — POST-v5.57.0 RESILIENCE & DIAGNOSTIC IMPROVEMENTS  [PRESERVED]
v4.7.1 — PROVIDER-LAYER QUIET CORRECTIONS                  [PRESERVED]
v4.7.0 — PROVIDER-LAYER FIXES FROM PRODUCTION SHEET AUDIT  [PRESERVED]

================================================================================

May 2026 / v2.8.0 family alignment
----------------------------------
This provider aligns with:
  - core/sheets/schema_registry    v2.8.0
  - core/data_engine_v2            v5.61.0 (consumes `provider_unhealthy:eodhd`
                                            warning marker via
                                            _ProviderHealthRegistry —
                                            v4.9.0 fires the marker on each
                                            auth-class failure)
  - core/scoring                   v5.2.5
  - core/reco_normalize            v7.2.0
  - core/scoring_engine            v3.4.2
  - core/insights_builder          v7.0.0
  - core/analysis/criteria_model   v3.1.0
  - core/analysis/top10_selector   v4.12.0
  - core/investment_advisor_engine v4.4.0
  - core/investment_advisor        v5.3.0
  - core/candlesticks              v1.0.0
  - yahoo_fundamentals_provider    v6.1.0
  - yahoo_chart_provider           v8.2.0
  - enriched_quote (core)          v4.3.0

================================================================================
"""

from __future__ import annotations

import asyncio
import logging
import math
import os
import random
import re
import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any, Callable, Dict, Iterable, List, Optional, Tuple, Union

import httpx

logger = logging.getLogger("core.providers.eodhd_provider")
logger.addHandler(logging.NullHandler())

PROVIDER_NAME = "eodhd"


# =============================================================================
# v4.7.0 — Currency-code canonicalization (ISSUE-A, ISSUE-B)
# (PRESERVED byte-identical in v4.8.0 and v4.9.0)
# =============================================================================
#
# EODHD returns several non-ISO currency codes that need normalization
# before downstream consumers (data_engine_v2 v5.60.0 _SUBUNIT_EXCHANGES,
# scoring.py, sheet display) can reliably reason about them.

_CURRENCY_CODE_CANONICAL: Dict[str, str] = {
    # GBP / pence
    "GBP": "GBP",
    "GBp": "GBX", "GBX": "GBX",
    # ZAR / cents
    "ZAR": "ZAR",
    "ZAc": "ZAC", "ZAC": "ZAC",
    # ILS / agorot
    "ILS": "ILS",
    "ILa": "ILA", "ILA": "ILA",
    # Kuwaiti Dinar
    "KWF": "KWD", "KD": "KWD", "KWD": "KWD",
    # Indonesian Rupiah
    "Rp": "IDR", "IDR": "IDR",
    # Malaysian Ringgit
    "RM": "MYR", "MYR": "MYR",
    # v4.7.2 YC: expanded loose-code aliases
    "EGP": "EGP", "Egp": "EGP",
    "AED": "AED", "AEd": "AED", "Dh": "AED",
    "QAR": "QAR", "Qar": "QAR", "QR": "QAR",
    "SAR": "SAR", "Sar": "SAR", "SR": "SAR",
    "JPY": "JPY", "JPy": "JPY", "Yen": "JPY",
    "CNY": "CNY", "CNy": "CNY", "RMB": "CNY", "Rmb": "CNY",
    "INR": "INR", "INr": "INR", "Rs": "INR",
    "TWD": "TWD", "TWd": "TWD", "NT$": "TWD",
    "HKD": "HKD", "HKd": "HKD", "HK$": "HKD",
    "BRL": "BRL", "BRl": "BRL", "R$": "BRL",
    "MXN": "MXN", "MXn": "MXN", "Mex$": "MXN",
}


def _canonicalize_currency_code(code: Any) -> Optional[str]:
    """v4.7.0 ISSUE-A / ISSUE-B: Map EODHD's loose currency codes to ISO."""
    s = safe_str(code)
    if not s:
        return None
    s = s.strip()
    if not s:
        return None
    if s in _CURRENCY_CODE_CANONICAL:
        return _CURRENCY_CODE_CANONICAL[s]
    su = s.upper()
    if su in _CURRENCY_CODE_CANONICAL:
        return _CURRENCY_CODE_CANONICAL[su]
    return su


# =============================================================================
# v4.7.0 — Suffix-derived geo defaults (ISSUE-C)
# (PRESERVED byte-identical in v4.8.0 and v4.9.0)
# =============================================================================

_GEO_BY_SUFFIX: Dict[str, Tuple[str, str, str]] = {
    ".L":       ("United Kingdom", "GBX", "LSE"),
    ".LSE":     ("United Kingdom", "GBX", "LSE"),
    ".LN":      ("United Kingdom", "GBX", "LSE"),
    ".JSE":     ("South Africa",   "ZAC", "JSE"),
    ".ZA":      ("South Africa",   "ZAC", "JSE"),
    ".TA":      ("Israel",         "ILA", "TASE"),
    ".TASE":    ("Israel",         "ILA", "TASE"),
    ".DE":      ("Germany",        "EUR", "XETRA"),
    ".XETRA":   ("Germany",        "EUR", "XETRA"),
    ".XETR":    ("Germany",        "EUR", "XETRA"),
    ".ETR":     ("Germany",        "EUR", "XETRA"),
    ".F":       ("Germany",        "EUR", "Frankfurt"),
    ".BE":      ("Germany",        "EUR", "Berlin"),
    ".PA":      ("France",         "EUR", "Euronext Paris"),
    ".FP":      ("France",         "EUR", "Euronext Paris"),
    ".AS":      ("Netherlands",    "EUR", "Euronext Amsterdam"),
    ".BR":      ("Belgium",        "EUR", "Euronext Brussels"),
    ".LS":      ("Portugal",       "EUR", "Euronext Lisbon"),
    ".MC":      ("Spain",          "EUR", "BME Spain"),
    ".MA":      ("Spain",          "EUR", "BME Spain"),
    ".MI":      ("Italy",          "EUR", "Borsa Italiana"),
    ".IM":      ("Italy",          "EUR", "Borsa Italiana"),
    ".VI":      ("Austria",        "EUR", "Wiener Boerse"),
    ".SW":      ("Switzerland",    "CHF", "SIX Swiss"),
    ".VX":      ("Switzerland",    "CHF", "SIX Swiss"),
    ".CO":      ("Denmark",        "DKK", "Nasdaq Copenhagen"),
    ".ST":      ("Sweden",         "SEK", "Nasdaq Stockholm"),
    ".OL":      ("Norway",         "NOK", "Oslo Bors"),
    ".HE":      ("Finland",        "EUR", "Nasdaq Helsinki"),
    ".T":       ("Japan",          "JPY", "Tokyo"),
    ".TYO":     ("Japan",          "JPY", "Tokyo"),
    ".HK":      ("Hong Kong",      "HKD", "HKEX"),
    ".HKG":     ("Hong Kong",      "HKD", "HKEX"),
    ".SS":      ("China",          "CNY", "Shanghai"),
    ".SZ":      ("China",          "CNY", "Shenzhen"),
    ".KS":      ("South Korea",    "KRW", "KOSPI"),
    ".KQ":      ("South Korea",    "KRW", "KOSDAQ"),
    ".TW":      ("Taiwan",         "TWD", "TWSE"),
    ".NS":      ("India",          "INR", "NSE India"),
    ".NSE":     ("India",          "INR", "NSE India"),
    ".BO":      ("India",          "INR", "BSE India"),
    ".BSE":     ("India",          "INR", "BSE India"),
    ".AX":      ("Australia",      "AUD", "ASX"),
    ".ASX":     ("Australia",      "AUD", "ASX"),
    ".NZ":      ("New Zealand",    "NZD", "NZX"),
    ".TO":      ("Canada",         "CAD", "TSX"),
    ".V":       ("Canada",         "CAD", "TSX-V"),
    ".SA":      ("Brazil",         "BRL", "B3 Brazil"),
    ".MX":      ("Mexico",         "MXN", "BMV"),
    ".BA":      ("Argentina",      "ARS", "BCBA"),
    ".SN":      ("Chile",          "CLP", "Bolsa de Santiago"),
    ".KW":      ("Kuwait",         "KWD", "Boursa Kuwait"),
    ".QA":      ("Qatar",          "QAR", "Qatar Stock Exchange"),
    ".AE":      ("UAE",            "AED", "ADX/DFM"),
    ".DFM":     ("UAE",            "AED", "DFM"),
    ".ADX":     ("UAE",            "AED", "ADX"),
    ".EG":      ("Egypt",          "EGP", "EGX"),
    ".SR":      ("Saudi Arabia",   "SAR", "Tadawul"),
    ".TADAWUL": ("Saudi Arabia",   "SAR", "Tadawul"),
    ".US":      ("United States",  "USD", "NYSE/NASDAQ"),
}


def _suffix_derived_geo_defaults(symbol: str) -> Dict[str, Optional[str]]:
    """v4.7.0 ISSUE-C: Resolve country / currency / exchange from symbol suffix."""
    s = safe_str(symbol)
    if not s or "." not in s:
        return {"country": None, "currency": None, "exchange": None}
    suffix = "." + s.rsplit(".", 1)[1].upper()
    geo = _GEO_BY_SUFFIX.get(suffix)
    if not geo:
        return {"country": None, "currency": None, "exchange": None}
    country, currency, exchange = geo
    return {"country": country, "currency": currency, "exchange": exchange}


# =============================================================================
# v4.7.0 — EODHD-canonical suffix mapping (ISSUE-D)
# (PRESERVED byte-identical in v4.8.0 and v4.9.0)
# =============================================================================

_EODHD_SUFFIX_CANONICAL: Dict[str, str] = {
    ".L":      ".LSE",
    ".XETR":   ".XETRA",
    ".ETR":    ".XETRA",
    ".TASE":   ".TA",
}


def _canonicalize_eodhd_suffix(symbol_uppercased: str) -> str:
    """v4.7.0 ISSUE-D: Map alias suffixes (".L" -> ".LSE") to EODHD canonical."""
    if "." not in symbol_uppercased:
        return symbol_uppercased
    base, suffix_only = symbol_uppercased.rsplit(".", 1)
    suffix_dotted = "." + suffix_only
    canonical = _EODHD_SUFFIX_CANONICAL.get(suffix_dotted)
    if canonical:
        return base + canonical
    return symbol_uppercased


# =============================================================================
# v4.7.3 ZB — 52W bounds currency-sanity guard (cross-provider alignment)
# (PRESERVED byte-identical in v4.8.0 and v4.9.0)
# =============================================================================

_PRICE_RATIO_SUSPECT_HIGH_DEFAULT = 8.0
_PRICE_RATIO_SUSPECT_LOW_DEFAULT = 0.125


def _is_suspect_price_ratio(
    ref: Optional[float],
    candidate: Optional[float],
    ratio_high: float = _PRICE_RATIO_SUSPECT_HIGH_DEFAULT,
    ratio_low: float = _PRICE_RATIO_SUSPECT_LOW_DEFAULT,
) -> bool:
    """
    Return True if `candidate` is suspiciously far from `ref` (likely a
    currency/scale mismatch -- e.g. GBX value paired with a USD reference).
    """
    if ref is None or candidate is None:
        return False
    if ref <= 0 or candidate <= 0:
        return False
    ratio = candidate / ref
    return ratio >= ratio_high or ratio <= ratio_low


def _validate_52w_bounds_merged(
    merged: Dict[str, Any],
    *,
    enabled: bool = True,
    ratio_high: float = _PRICE_RATIO_SUSPECT_HIGH_DEFAULT,
    ratio_low: float = _PRICE_RATIO_SUSPECT_LOW_DEFAULT,
) -> List[str]:
    """
    v4.7.3 ZB: Apply currency-mismatch guard to the merged 52W bounds.
    Mutates `merged` in place; returns a list of warning markers raised.
    """
    warnings: List[str] = []

    if not enabled:
        return warnings

    cp = safe_float(merged.get("current_price"))
    if cp is None or cp <= 0:
        return warnings

    hi = safe_float(merged.get("week_52_high"))
    lo = safe_float(merged.get("week_52_low"))

    if hi is not None and _is_suspect_price_ratio(cp, hi, ratio_high, ratio_low):
        warnings.append("week_52_high_unit_mismatch_dropped")
        merged["week_52_high"] = None
        merged["52w_high"] = None
        hi = None

    if lo is not None and _is_suspect_price_ratio(cp, lo, ratio_high, ratio_low):
        warnings.append("week_52_low_unit_mismatch_dropped")
        merged["week_52_low"] = None
        merged["52w_low"] = None
        lo = None

    # Sanity: high should be >= low
    if hi is not None and lo is not None and hi < lo:
        warnings.append("week_52_high_low_inverted")
        merged["week_52_high"] = lo
        merged["week_52_low"] = hi
        merged["52w_high"] = lo
        merged["52w_low"] = hi
        hi, lo = lo, hi

    # Informational: current_price should sit in the validated band
    if hi is not None and cp > hi * 1.05:
        warnings.append("current_price_outside_52w_range")
    elif lo is not None and cp < lo * 0.95:
        warnings.append("current_price_outside_52w_range")

    return warnings


# =============================================================================
# v4.9.0 AH — Per-failure provider-unhealthy detection helper
# =============================================================================
#
# Inspects an error string returned by `_request_json` and decides
# whether the failure class warrants emitting the cross-provider
# `provider_unhealthy:eodhd` warning marker.
#
# The marker is fired on ANY occurrence of an auth-class failure,
# without waiting for the circuit breaker to reach its 8-consecutive
# threshold. Engine v5.61.0+'s `_ProviderHealthRegistry` consumes the
# marker, applies its 300s TTL (idempotent — repeated firings refresh
# the same entry), and demotes/skips eodhd in subsequent chains.
#
# Granularity:
#   - "auth_error"  (HTTP 401/403 with auth body)   -> EMIT
#   - "ip_blocked"  (HTTP 401/403 with IP-block body) -> EMIT
#   - "quota_or_rate_limit" / "HTTP 429"            -> do NOT emit
#   - "HTTP 404 not_found"                          -> do NOT emit
#   - "network_error:..." / "HTTP 5xx"              -> do NOT emit
#   - "invalid_json_payload" / "bad_payload"        -> do NOT emit
#   - "circuit_open"                                -> handled separately
#                                                       by _build_circuit_open_patch

_PROVIDER_UNHEALTHY_TRIGGER_TOKENS: Tuple[str, ...] = (
    "auth_error",
    "ip_blocked",
)


def _err_indicates_provider_unhealthy(err_code: Any) -> bool:
    """
    v4.9.0 AH: Return True if `err_code` indicates an auth-class
    failure that should fire the per-failure `provider_unhealthy:eodhd`
    marker. Case-insensitive substring match.
    """
    s = safe_str(err_code)
    if not s:
        return False
    sl = s.lower()
    return any(tok in sl for tok in _PROVIDER_UNHEALTHY_TRIGGER_TOKENS)


def _build_error_patch_with_geo(
    sym_raw: str,
    sym_norm: str,
    err_code: str,
    *,
    extra_warnings: Optional[List[str]] = None,
) -> Dict[str, Any]:
    """
    v4.7.0 ISSUE-C / v4.7.3 ZA: Build a non-empty error patch that
    carries suffix-derived country/currency/exchange so the merged row
    arrives at data_engine_v2 with correct geo even when EODHD's
    endpoints failed.

    v4.7.3: also carries a structured `warnings` list with a
    fetch-failure marker plus any caller-provided field-level flags.

    v4.9.0 AH: when `err_code` indicates `auth_error` or `ip_blocked`,
    the patch also carries the cross-provider `provider_unhealthy:eodhd`
    marker. Engine v5.61.0+'s `_observe_provider_unhealthy_markers`
    extracts this from `warnings` and registers eodhd as unhealthy in
    `_ProviderHealthRegistry` immediately — without waiting for the
    circuit breaker's 8-consecutive threshold. Idempotent: repeated
    firings within the engine's TTL window (default 300s) refresh the
    same registry entry.
    """
    geo = _suffix_derived_geo_defaults(sym_raw)
    warnings_out: List[str] = [f"fetch_failed:{err_code}"]

    # v4.9.0 AH: per-failure provider-unhealthy signal.
    if _err_indicates_provider_unhealthy(err_code):
        warnings_out.append("provider_unhealthy:eodhd")

    if extra_warnings:
        for w in extra_warnings:
            if w and w not in warnings_out:
                warnings_out.append(w)
    return _clean_patch(
        {
            "symbol": sym_raw,
            "symbol_normalized": sym_norm,
            "provider": PROVIDER_NAME,
            "data_source": PROVIDER_NAME,
            "country": geo["country"],
            "currency": geo["currency"],
            "exchange": geo["exchange"],
            "error": err_code,
            "data_quality": "MISSING",
            "warnings": warnings_out,  # v4.7.3 ZA + v4.9.0 AH
            "last_updated_utc": _utc_iso(),
            "last_updated_riyadh": _riyadh_iso(),
        }
    )


# =============================================================================
# v4.9.0 — Constants and version
# =============================================================================

PROVIDER_VERSION = "4.9.1"
VERSION = PROVIDER_VERSION

DEFAULT_BASE_URL = "https://eodhistoricaldata.com/api"
UA_DEFAULT = "TFB-EODHD/4.9.1 (Render)"

try:
    import orjson  # type: ignore

    def json_loads(data: Union[str, bytes]) -> Any:
        if isinstance(data, str):
            data = data.encode("utf-8")
        return orjson.loads(data)

except Exception:
    import json  # type: ignore

    def json_loads(data: Union[str, bytes]) -> Any:
        if isinstance(data, bytes):
            data = data.decode("utf-8", errors="replace")
        return json.loads(data)

_TRUTHY = {"1", "true", "yes", "y", "on", "t", "enabled", "enable"}
_FALSY = {"0", "false", "no", "n", "off", "f", "disabled", "disable"}
_US_LIKE_RE = re.compile(r"^[A-Z0-9][A-Z0-9\-_]{0,16}$")
_KSA_RE = re.compile(r"^\d{3,6}\.SR$", re.IGNORECASE)


# =============================================================================
# Env helpers
# (PRESERVED byte-identical in v4.8.0 and v4.9.0)
# =============================================================================
def _env_str(name: str, default: str = "") -> str:
    return (os.getenv(name) or default).strip()


def _env_bool(name: str, default: bool = False) -> bool:
    raw = (os.getenv(name) or "").strip().lower()
    if not raw:
        return default
    if raw in _TRUTHY:
        return True
    if raw in _FALSY:
        return False
    return default


def _env_int(name: str, default: int, lo: Optional[int] = None, hi: Optional[int] = None) -> int:
    try:
        v = int(float((os.getenv(name) or str(default)).strip()))
    except Exception:
        v = default
    if lo is not None and v < lo:
        v = lo
    if hi is not None and v > hi:
        v = hi
    return v


def _env_float(name: str, default: float, lo: Optional[float] = None, hi: Optional[float] = None) -> float:
    try:
        v = float((os.getenv(name) or str(default)).strip())
    except Exception:
        v = default
    if lo is not None and v < lo:
        v = lo
    if hi is not None and v > hi:
        v = hi
    return v


def _token() -> str:
    return _env_str("EODHD_API_KEY") or _env_str("EODHD_API_TOKEN") or _env_str("EODHD_KEY")


def _base_url() -> str:
    return _env_str("EODHD_BASE_URL", DEFAULT_BASE_URL).rstrip("/")


def _default_exchange() -> str:
    ex = _env_str("EODHD_DEFAULT_EXCHANGE") or _env_str("EODHD_SYMBOL_SUFFIX_DEFAULT") or "US"
    return ex.strip().upper() or "US"


def _append_exchange_suffix() -> bool:
    return _env_bool("EODHD_APPEND_EXCHANGE_SUFFIX", True)


def _ksa_blocked_by_default() -> bool:
    return _env_bool("KSA_DISALLOW_EODHD", False)


def _allow_ksa_override() -> bool:
    return _env_bool("ALLOW_EODHD_KSA", False) or _env_bool("EODHD_ALLOW_KSA", False)


# v4.7.3 ZB: env helpers for the currency-sanity guard.
def _price_sanity_enabled() -> bool:
    return _env_bool("EODHD_PRICE_SANITY_GUARD", True)


def _price_ratio_high() -> float:
    v = _env_float("EODHD_PRICE_RATIO_HIGH", _PRICE_RATIO_SUSPECT_HIGH_DEFAULT)
    return v if v > 1.0 else _PRICE_RATIO_SUSPECT_HIGH_DEFAULT


def _price_ratio_low() -> float:
    v = _env_float("EODHD_PRICE_RATIO_LOW", _PRICE_RATIO_SUSPECT_LOW_DEFAULT)
    return v if 0.0 < v < 1.0 else _PRICE_RATIO_SUSPECT_LOW_DEFAULT


# =============================================================================
# v4.8.0 — Circuit-breaker env helpers
# (PRESERVED byte-identical in v4.9.0)
# =============================================================================
#
# Default thresholds chosen to be CONSERVATIVE:
#   - Threshold 8 means a legitimate batch run with sporadic 403s
#     (e.g. one symbol exceeds quota momentarily) will not trip the
#     breaker; only sustained auth failure does.
#   - Backoff 300s (5 min) means the breaker self-heals quickly enough
#     for a recovered API key / fixed quota to be detected on the
#     next refresh batch, but slow enough that we don't burn requests
#     during the failure window.
#
# v4.9.0 NOTE: the 8-consecutive threshold remains the SAME. v4.9.0
# adds a complementary per-failure signal path that fires the
# `provider_unhealthy:eodhd` marker without waiting for the breaker.
# The breaker still trips on sustained failure (its `circuit_open`
# patch + 5-minute backoff still apply); the per-failure marker
# handles the partial-failure case where the breaker can't reach
# threshold because successes keep resetting the consecutive counter.

_CIRCUIT_AUTH_THRESHOLD_DEFAULT = 8
_CIRCUIT_BACKOFF_SEC_DEFAULT = 300.0


def _circuit_breaker_enabled() -> bool:
    return _env_bool("EODHD_CIRCUIT_BREAKER_ENABLE", True)


def _circuit_auth_threshold() -> int:
    return _env_int(
        "EODHD_CIRCUIT_AUTH_THRESHOLD",
        _CIRCUIT_AUTH_THRESHOLD_DEFAULT,
        lo=1,
        hi=100,
    )


def _circuit_backoff_sec() -> float:
    return _env_float(
        "EODHD_CIRCUIT_BACKOFF_SEC",
        _CIRCUIT_BACKOFF_SEC_DEFAULT,
        lo=10.0,
        hi=3600.0,
    )


def _health_probe_symbol() -> str:
    return _env_str("EODHD_HEALTH_PROBE_SYMBOL", "AAPL.US")


def _utc_iso(dt: Optional[datetime] = None) -> str:
    d = dt or datetime.now(timezone.utc)
    if d.tzinfo is None:
        d = d.replace(tzinfo=timezone.utc)
    return d.astimezone(timezone.utc).isoformat()


def _riyadh_iso(dt: Optional[datetime] = None) -> str:
    d = dt or datetime.now(timezone.utc)
    tz = timezone(timedelta(hours=3))
    if d.tzinfo is None:
        d = d.replace(tzinfo=timezone.utc)
    return d.astimezone(tz).isoformat()


# =============================================================================
# v4.8.0 — _ProviderHealth (circuit breaker + session counters)
# (PRESERVED byte-identical in v4.9.0)
# =============================================================================
#
# Async-safe state class. Module-level singleton instance is created lazily
# on first access via _get_health(). All mutations are guarded by an
# asyncio.Lock so concurrent fetches don't race on the consecutive_*
# counters or the state machine.
#
# State machine:
#   closed     --(N consecutive AuthError|IpBlocked)--> open
#   open       --(BACKOFF_SEC elapsed)----------------> half_open
#   half_open  --(1 success)-----------------------> closed
#   half_open  --(1 failure)-----------------------> open  (resets opened_at)
#
# Cumulative counters never decrement; they reset only on process restart.
# This is by design — operators watching `total_requests / success_count`
# over time can compute success rate without us inventing a sliding
# window with its own correctness questions.

class _ProviderHealth:
    """v4.8.0 AA: async-safe provider health + circuit breaker. (Preserved v4.9.0)"""

    def __init__(self) -> None:
        self._lock = asyncio.Lock()
        # Consecutive-failure tracking (reset on first success)
        self._consecutive_auth = 0
        self._consecutive_ip = 0
        # Circuit state
        self._state: str = "closed"
        self._opened_at: Optional[float] = None
        # Last-event timestamps (monotonic seconds)
        self._last_success: Optional[float] = None
        self._last_failure: Optional[float] = None
        self._last_failure_class: Optional[str] = None
        # Cumulative session counters
        self._total_requests = 0
        self._success_count = 0
        self._auth_count = 0
        self._ip_count = 0
        self._rate_count = 0
        self._network_count = 0

    async def begin_request(self) -> Tuple[bool, str]:
        """
        v4.8.0 AB: called before each HTTP request. Returns (allow, state).
        """
        async with self._lock:
            self._total_requests += 1

            if not _circuit_breaker_enabled():
                return True, "closed"

            if self._state == "closed":
                return True, "closed"

            if self._state == "open":
                now = time.monotonic()
                backoff = _circuit_backoff_sec()
                if self._opened_at is not None and (now - self._opened_at) >= backoff:
                    self._state = "half_open"
                    return True, "half_open"
                return False, "open"

            return True, "half_open"

    async def record_success(self) -> None:
        """v4.8.0 AB: called after a successful HTTP fetch."""
        async with self._lock:
            now = time.monotonic()
            self._success_count += 1
            self._consecutive_auth = 0
            self._consecutive_ip = 0
            self._last_success = now
            if self._state in ("open", "half_open"):
                self._state = "closed"
                self._opened_at = None
                logger.info("[eodhd v4.9.1] circuit_breaker closed after successful request")

    async def record_failure(self, error_class: str) -> None:
        """
        v4.8.0 AB: called after a failed HTTP fetch with the inferred
        error class. Increments the relevant cumulative counter and
        the corresponding consecutive counter; opens the circuit if
        the threshold is crossed.
        """
        async with self._lock:
            now = time.monotonic()
            self._last_failure = now
            self._last_failure_class = error_class

            cls_lower = (error_class or "").lower()
            if cls_lower == "autherror":
                self._auth_count += 1
                self._consecutive_auth += 1
            elif cls_lower == "ipblocked":
                self._ip_count += 1
                self._consecutive_ip += 1
            elif cls_lower == "ratelimited":
                self._rate_count += 1
            elif cls_lower == "networkerror":
                self._network_count += 1

            if not _circuit_breaker_enabled():
                return

            threshold = _circuit_auth_threshold()

            if self._state == "half_open":
                self._state = "open"
                self._opened_at = now
                logger.warning(
                    "[eodhd v4.9.1] circuit_breaker re-opened: "
                    "half_open probe failed with %s", error_class,
                )
                return

            if self._state == "open":
                return

            if self._consecutive_auth >= threshold or self._consecutive_ip >= threshold:
                self._state = "open"
                self._opened_at = now
                logger.error(
                    "[eodhd v4.9.1] circuit_breaker OPEN: "
                    "consecutive_auth=%d consecutive_ip=%d threshold=%d "
                    "(EODHD provider unhealthy — verify EODHD_API_KEY env var)",
                    self._consecutive_auth, self._consecutive_ip, threshold,
                )

    async def is_open(self) -> bool:
        """v4.8.0 AB: lightweight check used by patch builders."""
        async with self._lock:
            return self._state == "open"

    async def snapshot(self) -> Dict[str, Any]:
        """
        v4.8.0 AD/AE: structured snapshot for diagnose_health() and
        get_provider_stats().
        """
        async with self._lock:
            now = time.monotonic()
            last_success_age_sec = (
                (now - self._last_success) if self._last_success is not None else None
            )
            last_failure_age_sec = (
                (now - self._last_failure) if self._last_failure is not None else None
            )
            opened_age_sec = (
                (now - self._opened_at) if self._opened_at is not None else None
            )
            return {
                "circuit_state": self._state,
                "circuit_breaker_enabled": _circuit_breaker_enabled(),
                "consecutive_auth_errors": self._consecutive_auth,
                "consecutive_ip_blocks": self._consecutive_ip,
                "last_failure_class": self._last_failure_class,
                "last_success_age_sec": (
                    round(last_success_age_sec, 2) if last_success_age_sec is not None else None
                ),
                "last_failure_age_sec": (
                    round(last_failure_age_sec, 2) if last_failure_age_sec is not None else None
                ),
                "circuit_opened_age_sec": (
                    round(opened_age_sec, 2) if opened_age_sec is not None else None
                ),
                "session_stats": {
                    "total_requests": self._total_requests,
                    "success_count": self._success_count,
                    "auth_error_count": self._auth_count,
                    "ip_block_count": self._ip_count,
                    "rate_limit_count": self._rate_count,
                    "network_error_count": self._network_count,
                },
            }


# Module-level singleton instance, created lazily.
_HEALTH: Optional[_ProviderHealth] = None
_HEALTH_LOCK = asyncio.Lock()


async def _get_health() -> _ProviderHealth:
    """v4.8.0 AA: lazy module-level singleton accessor."""
    global _HEALTH
    if _HEALTH is None:
        async with _HEALTH_LOCK:
            if _HEALTH is None:
                _HEALTH = _ProviderHealth()
    return _HEALTH


def _build_circuit_open_patch(sym_raw: str, sym_norm: str) -> Dict[str, Any]:
    """
    v4.8.0 AC: build a patch for when the circuit is open and we're
    not even attempting the API call. Carries the cross-provider
    `provider_unhealthy:eodhd` marker so the engine knows this is a
    systemic failure, not a per-symbol issue.

    v4.9.0 NOTE: this is the "circuit fully tripped" path. The v4.9.0
    per-failure path (via `_build_error_patch_with_geo`) emits the same
    cross-provider marker on each individual auth failure even when
    the breaker hasn't reached its 8-consecutive threshold. Both paths
    carry the same marker shape so the engine sees consistent input
    from either source.
    """
    geo = _suffix_derived_geo_defaults(sym_raw)
    return _clean_patch(
        {
            "symbol": sym_raw,
            "symbol_normalized": sym_norm,
            "provider": PROVIDER_NAME,
            "data_source": PROVIDER_NAME,
            "country": geo["country"],
            "currency": geo["currency"],
            "exchange": geo["exchange"],
            "error": "circuit_open",
            "data_quality": "MISSING",
            "last_error_class": "CircuitOpen",  # v4.7.3 ZC
            "warnings": ["circuit_open", "provider_unhealthy:eodhd"],  # v4.8.0 AC
            "last_updated_utc": _utc_iso(),
            "last_updated_riyadh": _riyadh_iso(),
        }
    )


# =============================================================================
# Coercion helpers
# (PRESERVED byte-identical in v4.8.0 and v4.9.0)
# =============================================================================
def safe_float(v: Any) -> Optional[float]:
    if v is None:
        return None
    try:
        if isinstance(v, (int, float)) and not isinstance(v, bool):
            x = float(v)
        else:
            s = str(v).strip().replace(",", "")
            if not s:
                return None
            if s.endswith("%"):
                s = s[:-1].strip()
                x = float(s) / 100.0
            else:
                x = float(s)
        if math.isnan(x) or math.isinf(x):
            return None
        return x
    except Exception:
        return None


def safe_int(v: Any) -> Optional[int]:
    f = safe_float(v)
    if f is None:
        return None
    try:
        return int(round(f))
    except Exception:
        return None


def safe_str(v: Any) -> Optional[str]:
    if v is None:
        return None
    try:
        s = str(v).strip()
        return s if s else None
    except Exception:
        return None


def _clean_patch(p: Dict[str, Any]) -> Dict[str, Any]:
    """
    v4.6.0 contract: drop None and empty-string values.
    v4.7.3 ZA: an empty `warnings` list is also dropped.
    """
    out: Dict[str, Any] = {}
    for k, v in (p or {}).items():
        if v is None:
            continue
        if v == "":
            continue
        if k == "warnings" and isinstance(v, list) and len(v) == 0:
            continue
        out[k] = v
    return out


def _frac_from_percentish(v: Any) -> Optional[float]:
    f = safe_float(v)
    if f is None:
        return None
    if abs(f) > 1.5:
        return f / 100.0
    return f


def _safe_div(a: Any, b: Any) -> Optional[float]:
    x = safe_float(a)
    y = safe_float(b)
    if x is None or y in (None, 0.0):
        return None
    try:
        return x / y
    except Exception:
        return None


def _first_present(*vals: Any) -> Any:
    for v in vals:
        if v is None:
            continue
        if isinstance(v, str) and not v.strip():
            continue
        return v
    return None


def _sum_present(values: Iterable[Any]) -> Optional[float]:
    total = 0.0
    seen = False
    for v in values:
        f = safe_float(v)
        if f is None:
            continue
        total += f
        seen = True
    return total if seen else None


def _merge_warnings_inplace(target: Dict[str, Any], incoming: Any) -> None:
    """
    v4.7.3 ZA: union an incoming `warnings` field into `target["warnings"]`.
    """
    if incoming is None:
        return
    if isinstance(incoming, str):
        parts = [p.strip() for p in incoming.split(";") if p and p.strip()]
    elif isinstance(incoming, (list, tuple)):
        parts = []
        for item in incoming:
            if item is None:
                continue
            try:
                s = str(item).strip()
            except Exception:
                continue
            if s and s.lower() not in {"none", "null", "nil"}:
                parts.append(s)
    else:
        return

    existing = target.get("warnings")
    if existing is None:
        merged_list: List[str] = []
    elif isinstance(existing, list):
        merged_list = list(existing)
    elif isinstance(existing, str):
        merged_list = [p.strip() for p in existing.split(";") if p and p.strip()]
    else:
        merged_list = []

    seen: set = set(merged_list)
    for p in parts:
        if p not in seen:
            seen.add(p)
            merged_list.append(p)

    if merged_list:
        target["warnings"] = merged_list


# =============================================================================
# Symbol normalization (GLOBAL, incl. international)
# (PRESERVED byte-identical in v4.8.0 and v4.9.0)
# =============================================================================
def normalize_eodhd_symbol(symbol: str) -> str:
    s = (symbol or "").strip()
    if not s:
        return ""
    s_up = s.upper()

    try:
        from core.symbols.normalize import to_eodhd_symbol as _to_eodhd_symbol  # type: ignore

        if callable(_to_eodhd_symbol):
            out = _to_eodhd_symbol(s_up, default_exchange=_default_exchange())  # type: ignore[arg-type]
            if isinstance(out, str) and out.strip():
                return _canonicalize_eodhd_suffix(out.strip().upper())
    except Exception:
        pass

    if "=" in s_up or "^" in s_up or "/" in s_up:
        return s_up

    if s_up.endswith(".SR") and _KSA_RE.match(s_up):
        return s_up

    if "." in s_up:
        base, suf = s_up.rsplit(".", 1)
        if len(suf) >= 2:
            return _canonicalize_eodhd_suffix(s_up)
        if not _append_exchange_suffix():
            return s_up
        return f"{s_up}.{_default_exchange()}"

    if not _append_exchange_suffix():
        return s_up

    if _US_LIKE_RE.match(s_up):
        return f"{s_up}.{_default_exchange()}"

    return s_up


# =============================================================================
# Nested-data helpers
# (PRESERVED byte-identical in v4.8.0 and v4.9.0)
# =============================================================================
def _listify_rows(node: Any) -> List[Dict[str, Any]]:
    if isinstance(node, list):
        rows = [r for r in node if isinstance(r, dict)]
    elif isinstance(node, dict):
        rows = []
        for k, v in node.items():
            if isinstance(v, dict):
                row = dict(v)
                row.setdefault("date", k)
                rows.append(row)
    else:
        rows = []

    def _sort_key(row: Dict[str, Any]) -> str:
        return safe_str(row.get("date")) or safe_str(row.get("filing_date")) or ""

    rows.sort(key=_sort_key, reverse=True)
    return rows


def _statement_rows(financials: Dict[str, Any], section: str, periodicity: str) -> List[Dict[str, Any]]:
    sec = (financials or {}).get(section) or {}
    if not isinstance(sec, dict):
        return []
    rows = sec.get(periodicity)
    if rows is None and periodicity == "quarterly":
        rows = sec.get("quarter")
    if rows is None and periodicity == "yearly":
        rows = sec.get("annual") or sec.get("year")
    return _listify_rows(rows)


def _pick_numeric(row: Dict[str, Any], *keys: str) -> Optional[float]:
    for k in keys:
        if k in row:
            f = safe_float(row.get(k))
            if f is not None:
                return f
    lower_map = {str(k).lower(): v for k, v in row.items()}
    for k in keys:
        f = safe_float(lower_map.get(str(k).lower()))
        if f is not None:
            return f
    return None


def _latest_numeric(rows: List[Dict[str, Any]], *keys: str) -> Optional[float]:
    for row in rows:
        v = _pick_numeric(row, *keys)
        if v is not None:
            return v
    return None


def _sum_latest_n(rows: List[Dict[str, Any]], n: int, *keys: str) -> Optional[float]:
    if not rows:
        return None
    vals: List[float] = []
    for row in rows[: max(1, n)]:
        v = _pick_numeric(row, *keys)
        if v is not None:
            vals.append(v)
    if not vals:
        return None
    return sum(vals)


def _infer_asset_class(general: Dict[str, Any], etf_data: Dict[str, Any]) -> Optional[str]:
    t = (safe_str(general.get("Type")) or safe_str(general.get("Category")) or "").strip().lower()
    if not t and etf_data:
        return "ETF"
    mapping = {
        "common stock": "Equity",
        "stock": "Equity",
        "equity": "Equity",
        "preferred stock": "Equity",
        "etf": "ETF",
        "exchange traded fund": "ETF",
        "fund": "Fund",
        "mutual fund": "Mutual Fund",
        "adr": "ADR",
        "reit": "REIT",
        "index": "Index",
        "currency": "FX",
        "forex": "FX",
        "futures": "Future",
        "future": "Future",
        "commodity": "Commodity",
        "bond": "Bond",
    }
    if t in mapping:
        return mapping[t]
    if "fund" in t and "mutual" in t:
        return "Mutual Fund"
    if "etf" in t:
        return "ETF"
    if "fund" in t:
        return "Fund"
    if "stock" in t or "equity" in t:
        return "Equity"
    if t:
        return t.title()
    return None


# =============================================================================
# Async primitives: SingleFlight + TTL Cache + TokenBucket
# (PRESERVED byte-identical in v4.8.0 and v4.9.0)
# =============================================================================
class _SingleFlight:
    def __init__(self) -> None:
        self._lock = asyncio.Lock()
        self._calls: Dict[str, asyncio.Future] = {}

    async def do(self, key: str, coro_factory: Callable[[], Any]) -> Any:
        async with self._lock:
            fut = self._calls.get(key)
            if fut is None:
                fut = asyncio.get_running_loop().create_future()
                self._calls[key] = fut
                owner = True
            else:
                owner = False

        if not owner:
            return await fut  # type: ignore

        try:
            res = await coro_factory()
            if not fut.done():
                fut.set_result(res)
            return res
        except Exception as e:
            if not fut.done():
                fut.set_exception(e)
            raise
        finally:
            async with self._lock:
                self._calls.pop(key, None)


@dataclass
class _CacheItem:
    exp: float
    val: Any


class _TTLCache:
    def __init__(self, maxsize: int, ttl_sec: float):
        self.maxsize = max(128, int(maxsize))
        self.ttl_sec = max(1.0, float(ttl_sec))
        self._d: Dict[str, _CacheItem] = {}
        self._lock = asyncio.Lock()

    async def get(self, key: str) -> Any:
        now = time.monotonic()
        async with self._lock:
            it = self._d.get(key)
            if not it:
                return None
            if it.exp > now:
                return it.val
            self._d.pop(key, None)
            return None

    async def set(self, key: str, val: Any, ttl_sec: Optional[float] = None) -> None:
        now = time.monotonic()
        ttl = self.ttl_sec if ttl_sec is None else max(1.0, float(ttl_sec))
        async with self._lock:
            if len(self._d) >= self.maxsize and key not in self._d:
                self._d.pop(next(iter(self._d.keys())), None)
            self._d[key] = _CacheItem(exp=now + ttl, val=val)


class _TokenBucket:
    def __init__(self, rate_per_sec: float, burst: float):
        self.rate = max(0.0, float(rate_per_sec))
        self.capacity = max(1.0, float(burst))
        self.tokens = self.capacity
        self.last = time.monotonic()
        self._lock = asyncio.Lock()

    async def wait(self, amount: float = 1.0) -> None:
        if self.rate <= 0:
            return
        amount = max(0.0001, float(amount))
        while True:
            async with self._lock:
                now = time.monotonic()
                elapsed = max(0.0, now - self.last)
                self.last = now
                self.tokens = min(self.capacity, self.tokens + elapsed * self.rate)
                if self.tokens >= amount:
                    self.tokens -= amount
                    return
                deficit = amount - self.tokens
                sleep_s = deficit / self.rate if self.rate > 0 else 0.25
            await asyncio.sleep(min(1.0, max(0.05, sleep_s)))


# =============================================================================
# Stats calculations from close series (no numpy)
# (PRESERVED byte-identical in v4.8.0 and v4.9.0)
# =============================================================================
def _daily_returns(closes: List[float]) -> List[float]:
    rets: List[float] = []
    for i in range(1, len(closes)):
        p0 = closes[i - 1]
        p1 = closes[i]
        if p0 and p0 > 0 and p1 and p1 > 0:
            rets.append((p1 / p0) - 1.0)
    return rets


def _stdev(x: List[float]) -> Optional[float]:
    if len(x) < 2:
        return None
    m = sum(x) / len(x)
    var = sum((v - m) ** 2 for v in x) / max(1, len(x) - 1)
    return math.sqrt(max(0.0, var))


def _max_drawdown(closes: List[float]) -> Optional[float]:
    if len(closes) < 2:
        return None
    peak = closes[0]
    mdd = 0.0
    for p in closes:
        if p > peak:
            peak = p
        if peak > 0:
            dd = (p / peak) - 1.0
            if dd < mdd:
                mdd = dd
    return mdd


def _var_95_1d(returns: List[float]) -> Optional[float]:
    if len(returns) < 20:
        return None
    xs = sorted(returns)
    idx = int(round(0.05 * (len(xs) - 1)))
    q = xs[max(0, min(len(xs) - 1, idx))]
    return abs(q) if q < 0 else 0.0


def _sharpe_1y(returns: List[float], rf_annual: float) -> Optional[float]:
    if len(returns) < 60:
        return None
    mu = sum(returns) / len(returns)
    sd = _stdev(returns)
    if sd is None or sd == 0:
        return None
    rf_daily = float(rf_annual) / 252.0
    ex = mu - rf_daily
    return (ex / sd) * math.sqrt(252.0)


def _annualized_vol(returns: List[float]) -> Optional[float]:
    sd = _stdev(returns)
    if sd is None:
        return None
    return sd * math.sqrt(252.0)


def _rsi14(closes: List[float]) -> Optional[float]:
    if len(closes) < 15:
        return None
    gains: List[float] = []
    losses: List[float] = []
    for i in range(1, len(closes)):
        delta = closes[i] - closes[i - 1]
        gains.append(max(delta, 0.0))
        losses.append(abs(min(delta, 0.0)))
    if len(gains) < 14:
        return None
    avg_gain = sum(gains[:14]) / 14.0
    avg_loss = sum(losses[:14]) / 14.0
    for i in range(14, len(gains)):
        avg_gain = ((avg_gain * 13.0) + gains[i]) / 14.0
        avg_loss = ((avg_loss * 13.0) + losses[i]) / 14.0
    if avg_loss == 0:
        return 100.0
    rs = avg_gain / avg_loss
    return 100.0 - (100.0 / (1.0 + rs))


# =============================================================================
# EODHD Client
# (v4.8.0 + v4.9.0: _request_json integrates circuit breaker; v4.9.0 emits
#  per-failure provider_unhealthy:eodhd via _build_error_patch_with_geo;
#  all other methods PRESERVED)
# =============================================================================
class EODHDClient:
    def __init__(self) -> None:
        self.api_key = _token()
        self.base_url = _base_url()

        self.timeout_sec = _env_float("EODHD_TIMEOUT_SEC", 15.0, lo=3.0, hi=120.0)
        self.retry_attempts = _env_int("EODHD_RETRY_ATTEMPTS", 4, lo=0, hi=10)
        self.retry_base_delay = _env_float("EODHD_RETRY_BASE_DELAY", 0.6, lo=0.0, hi=10.0)

        self.max_concurrency = _env_int("EODHD_MAX_CONCURRENCY", 20, lo=1, hi=100)
        self._sem = asyncio.Semaphore(self.max_concurrency)

        rps = _env_float("EODHD_RATE_LIMIT_RPS", 4.0, lo=0.0, hi=50.0)
        burst = _env_float("EODHD_RATE_LIMIT_BURST", 8.0, lo=1.0, hi=200.0)
        self._bucket = _TokenBucket(rate_per_sec=rps, burst=burst)

        self._sf = _SingleFlight()

        self.quote_cache = _TTLCache(maxsize=6000, ttl_sec=_env_float("EODHD_QUOTE_TTL_SEC", 12.0, lo=1.0, hi=600.0))
        self.fund_cache = _TTLCache(maxsize=3000, ttl_sec=_env_float("EODHD_FUND_TTL_SEC", 21600.0, lo=60.0, hi=86400.0))
        self.hist_cache = _TTLCache(maxsize=2000, ttl_sec=_env_float("EODHD_HISTORY_TTL_SEC", 1800.0, lo=60.0, hi=86400.0))

        self._client = httpx.AsyncClient(
            timeout=httpx.Timeout(self.timeout_sec),
            headers={"User-Agent": _env_str("EODHD_USER_AGENT", UA_DEFAULT)},
            limits=httpx.Limits(max_keepalive_connections=30, max_connections=60),
            http2=True,
        )

        logger.info(
            "EODHDClient v%s initialized | api_key_present=%s | rate=%s/s | burst=%s | "
            "concurrency=%s | retries=%s | sanity_guard=%s | ratio=[%s, %s] | "
            "circuit_breaker=%s threshold=%d backoff=%.0fs | "
            "per_failure_unhealthy_signal=enabled",
            PROVIDER_VERSION,
            bool(self.api_key),
            rps,
            burst,
            self.max_concurrency,
            self.retry_attempts,
            _price_sanity_enabled(),
            _price_ratio_low(),
            _price_ratio_high(),
            _circuit_breaker_enabled(),
            _circuit_auth_threshold(),
            _circuit_backoff_sec(),
        )

    def _base_params(self) -> Dict[str, str]:
        return {"api_token": self.api_key, "fmt": "json"}

    @staticmethod
    def _classify_error(err: Optional[str]) -> str:
        """
        v4.8.0 AB: map the free-form error string from _request_json into
        the structured class names the health tracker understands.
        """
        if not err:
            return "FetchError"
        lerr = str(err).lower()
        if "auth_error" in lerr:
            return "AuthError"
        if "ip_blocked" in lerr:
            return "IpBlocked"
        if "quota_or_rate_limit" in lerr or "429" in lerr:
            return "RateLimited"
        if "not_found" in lerr or "404" in lerr:
            return "NotFound"
        if "network_error" in lerr:
            return "NetworkError"
        if "bad_payload" in lerr or "invalid_json" in lerr:
            return "InvalidPayload"
        if "circuit_open" in lerr:
            return "CircuitOpen"
        return "FetchError"

    async def _request_json(self, endpoint: str, params: Optional[Dict[str, Any]] = None) -> Tuple[Optional[Any], Optional[str]]:
        """
        v4.7.3 + v4.8.0 AB: HTTP request with retry, 403 body parsing, and
        circuit-breaker integration. (PRESERVED byte-identical in v4.9.0.)

        The per-failure `provider_unhealthy:eodhd` marker emitted by
        v4.9.0 is added DOWNSTREAM of this function in
        `_build_error_patch_with_geo`, based on inspecting the error
        string returned here. _request_json itself is unchanged.
        """
        if not self.api_key:
            return None, "EODHD_API_KEY missing"

        # v4.8.0 AB: circuit-breaker check before the network call.
        health = await _get_health()
        allow, state_seen = await health.begin_request()
        if not allow:
            return None, "circuit_open"

        url = f"{self.base_url}/{endpoint.lstrip('/')}"
        p = {**self._base_params(), **(params or {})}
        last_err: Optional[str] = None

        async with self._sem:
            for attempt in range(max(1, self.retry_attempts + 1)):
                await self._bucket.wait(1.0)
                try:
                    r = await self._client.get(url, params=p)
                    sc = int(r.status_code)

                    if sc == 429:
                        ra = r.headers.get("Retry-After")
                        wait = float(ra) if ra and ra.replace(".", "", 1).isdigit() else min(15.0, 1.0 + attempt)
                        last_err = "HTTP 429"
                        await asyncio.sleep(wait)
                        continue

                    if sc in (401, 403):
                        body_hint = ""
                        try:
                            body_hint = (r.text or "")[:200]
                        except Exception:
                            body_hint = ""
                        body_low = body_hint.lower()

                        is_quota_or_rate = any(
                            k in body_low for k in (
                                "quota", "rate limit", "rate-limit", "ratelimit",
                                "exceeded", "too many", "limit reached",
                                "limit exceed", "calls per",
                            )
                        )
                        is_ip_blocked = any(
                            k in body_low for k in (
                                "ip block", "ip-block", "blocked ip", "your ip",
                                "geo block", "geographic",
                            )
                        )

                        if is_quota_or_rate and sc == 403:
                            ra = r.headers.get("Retry-After")
                            wait = float(ra) if ra and ra.replace(".", "", 1).isdigit() else min(30.0, 5.0 + 2.0 * attempt)
                            last_err = "HTTP 403 quota_or_rate_limit"
                            await health.record_failure("RateLimited")
                            await asyncio.sleep(wait)
                            continue

                        if is_ip_blocked:
                            err_str = f"HTTP {sc} ip_blocked {body_hint}".strip()
                            await health.record_failure("IpBlocked")
                            return None, err_str

                        err_str = f"HTTP {sc} auth_error {body_hint}".strip()
                        await health.record_failure("AuthError")
                        return None, err_str

                    if 500 <= sc < 600:
                        last_err = f"HTTP {sc}"
                        base = self.retry_base_delay * (2 ** (attempt - 1))
                        await asyncio.sleep(min(12.0, random.uniform(0, base + 0.25)))
                        continue

                    if sc == 404:
                        return None, "HTTP 404 not_found"

                    if sc >= 400:
                        await health.record_failure("FetchError")
                        return None, f"HTTP {sc}"

                    try:
                        data = json_loads(r.content)
                        await health.record_success()
                        return data, None
                    except Exception:
                        await health.record_failure("InvalidPayload")
                        return None, "invalid_json_payload"

                except httpx.RequestError as e:
                    last_err = f"network_error:{e.__class__.__name__}"
                    base = self.retry_base_delay * (2 ** (attempt - 1))
                    await asyncio.sleep(min(12.0, random.uniform(0, base + 0.25)))
                except Exception as e:
                    last_err = f"unexpected_error:{e.__class__.__name__}"
                    break

        final_class = self._classify_error(last_err)
        await health.record_failure(final_class)
        return None, last_err or "request_failed"

    async def fetch_quote(self, symbol: str) -> Tuple[Dict[str, Any], Optional[str]]:
        sym_raw = (symbol or "").strip().upper()
        sym = normalize_eodhd_symbol(sym_raw)
        if not sym:
            return {}, "invalid_symbol"

        if _KSA_RE.match(sym) and _ksa_blocked_by_default() and not _allow_ksa_override():
            return {}, "ksa_blocked"

        ck = f"q:{sym}"
        cached = await self.quote_cache.get(ck)
        if cached:
            return cached, None

        async def _do() -> Tuple[Dict[str, Any], Optional[str]]:
            warnings_list: List[str] = []
            data, err = await self._request_json(f"real-time/{sym}", params={})
            if err or not isinstance(data, dict):
                # v4.8.0 AC: circuit-open path
                if err == "circuit_open":
                    return _build_circuit_open_patch(sym_raw, sym), err
                # v4.9.0 AH: per-failure path — _build_error_patch_with_geo
                # adds `provider_unhealthy:eodhd` to warnings automatically
                # when the err indicates an auth-class failure.
                return _build_error_patch_with_geo(sym_raw, sym, err or "bad_payload"), err or "bad_payload"

            close = safe_float(_first_present(data.get("close"), data.get("adjusted_close"), data.get("last"), data.get("price")))
            prev = safe_float(_first_present(data.get("previousClose"), data.get("previous_close"), data.get("prev_close")))
            open_px = safe_float(_first_present(data.get("open"), data.get("dayOpen"), data.get("day_open")))
            high_px = safe_float(_first_present(data.get("high"), data.get("dayHigh"), data.get("day_high")))
            low_px = safe_float(_first_present(data.get("low"), data.get("dayLow"), data.get("day_low")))
            volume = safe_float(_first_present(data.get("volume"), data.get("shareVolume"), data.get("avgVolume")))
            chg = safe_float(_first_present(data.get("change"), data.get("price_change")))

            if chg is None and close is not None and prev not in (None, 0.0):
                chg = close - prev
            change_frac = _frac_from_percentish(_first_present(data.get("change_p"), data.get("percent_change"), data.get("changePercent")))
            if change_frac is None and close is not None and prev not in (None, 0.0):
                change_frac = (close / prev) - 1.0

            exchange = safe_str(_first_present(data.get("exchange"), data.get("fullExchangeName"), data.get("primaryExchange")))
            currency = _canonicalize_currency_code(_first_present(data.get("currency"), data.get("currency_code")))
            market_cap = safe_float(_first_present(data.get("market_cap"), data.get("marketCapitalization"), data.get("marketCapitalisation")))

            geo = _suffix_derived_geo_defaults(sym_raw)
            if not exchange and geo["exchange"]:
                exchange = geo["exchange"]
                warnings_list.append("quote_exchange_from_suffix")
            if not currency and geo["currency"]:
                currency = geo["currency"]
                warnings_list.append("quote_currency_from_suffix")

            if close is None:
                warnings_list.append("quote_current_price_missing")
            if exchange is None:
                warnings_list.append("quote_exchange_missing")
            if currency is None:
                warnings_list.append("quote_currency_missing")

            patch = _clean_patch(
                {
                    "symbol": sym_raw,
                    "symbol_normalized": sym,
                    "provider": PROVIDER_NAME,
                    "data_source": PROVIDER_NAME,
                    "name": safe_str(_first_present(data.get("name"), data.get("shortName"), data.get("longName"))),
                    "exchange": exchange,
                    "currency": currency,
                    "current_price": close,
                    "price": close,
                    "previous_close": prev,
                    "prev_close": prev,
                    "day_open": open_px,
                    "open": open_px,
                    "day_high": high_px,
                    "day_low": low_px,
                    "volume": volume,
                    "market_cap": market_cap,
                    "price_change": chg,
                    "change": chg,
                    "percent_change": change_frac,
                    "change_pct": change_frac,
                    "timestamp": safe_str(_first_present(data.get("timestamp"), data.get("date"))),
                    "warnings": warnings_list if warnings_list else None,
                    "last_updated_utc": _utc_iso(),
                    "last_updated_riyadh": _riyadh_iso(),
                }
            )
            await self.quote_cache.set(ck, patch)
            return patch, None

        return await self._sf.do(ck, _do)

    async def fetch_fundamentals(self, symbol: str) -> Tuple[Dict[str, Any], Optional[str]]:
        sym_raw = (symbol or "").strip().upper()
        sym = normalize_eodhd_symbol(sym_raw)
        if not sym:
            return {}, "invalid_symbol"

        if _KSA_RE.match(sym) and _ksa_blocked_by_default() and not _allow_ksa_override():
            return {}, "ksa_blocked"

        ck = f"f:{sym}"
        cached = await self.fund_cache.get(ck)
        if cached:
            return cached, None

        async def _do() -> Tuple[Dict[str, Any], Optional[str]]:
            warnings_list: List[str] = []
            data, err = await self._request_json(f"fundamentals/{sym}", params={})
            if err or not isinstance(data, dict):
                # v4.8.0 AC + v4.9.0 AH paths
                if err == "circuit_open":
                    return _build_circuit_open_patch(sym_raw, sym), err
                return _build_error_patch_with_geo(sym_raw, sym, err or "bad_payload"), err or "bad_payload"

            general = data.get("General") or {}
            highlights = data.get("Highlights") or {}
            valuation = data.get("Valuation") or {}
            shares = data.get("SharesStats") or {}
            tech = data.get("Technicals") or {}
            splits = data.get("SplitsDividends") or {}
            etf_data = data.get("ETF_Data") or {}
            financials = data.get("Financials") or {}

            if not general and not highlights and not financials:
                warnings_list.append("fundamentals_empty")

            income_q = _statement_rows(financials, "Income_Statement", "quarterly")
            income_y = _statement_rows(financials, "Income_Statement", "yearly")
            cash_q = _statement_rows(financials, "Cash_Flow", "quarterly")
            cash_y = _statement_rows(financials, "Cash_Flow", "yearly")
            balance_q = _statement_rows(financials, "Balance_Sheet", "quarterly")
            balance_y = _statement_rows(financials, "Balance_Sheet", "yearly")

            revenue_ttm = _sum_latest_n(income_q, 4, "totalRevenue", "revenue", "Revenue")
            gross_profit_ttm = _sum_latest_n(income_q, 4, "grossProfit", "GrossProfit")
            operating_income_ttm = _sum_latest_n(income_q, 4, "operatingIncome", "OperatingIncome")
            net_income_ttm = _sum_latest_n(income_q, 4, "netIncome", "NetIncome")
            cfo_ttm = _sum_latest_n(cash_q, 4, "totalCashFromOperatingActivities", "operatingCashFlow", "OperatingCashFlow")
            capex_ttm = _sum_latest_n(cash_q, 4, "capitalExpenditures", "CapitalExpenditure", "capitalExpenditure")
            fcf_ttm = None
            if cfo_ttm is not None and capex_ttm is not None:
                fcf_ttm = cfo_ttm - abs(capex_ttm)
            elif cfo_ttm is not None:
                fcf_ttm = cfo_ttm

            latest_rev_q = _latest_numeric(income_q, "totalRevenue", "revenue", "Revenue")
            prev_year_rev_q = None
            if len(income_q) >= 5:
                prev_year_rev_q = _pick_numeric(income_q[4], "totalRevenue", "revenue", "Revenue")
            revenue_growth_yoy = None
            if latest_rev_q is not None and prev_year_rev_q not in (None, 0.0):
                revenue_growth_yoy = (latest_rev_q / prev_year_rev_q) - 1.0
            if revenue_growth_yoy is None:
                latest_rev_y = _latest_numeric(income_y, "totalRevenue", "revenue", "Revenue")
                prev_rev_y = _pick_numeric(income_y[1], "totalRevenue", "revenue", "Revenue") if len(income_y) >= 2 else None
                if latest_rev_y is not None and prev_rev_y not in (None, 0.0):
                    revenue_growth_yoy = (latest_rev_y / prev_rev_y) - 1.0

            latest_net_q = _latest_numeric(income_q, "netIncome", "NetIncome")
            prev_year_net_q = _pick_numeric(income_q[4], "netIncome", "NetIncome") if len(income_q) >= 5 else None
            earnings_growth = None
            if latest_net_q is not None and prev_year_net_q not in (None, 0.0):
                earnings_growth = (latest_net_q / prev_year_net_q) - 1.0

            gross_margin = _safe_div(gross_profit_ttm, revenue_ttm)
            operating_margin = _safe_div(operating_income_ttm, revenue_ttm)
            profit_margin = _safe_div(net_income_ttm, revenue_ttm)

            latest_bs = balance_q[0] if balance_q else (balance_y[0] if balance_y else {})
            total_assets = _pick_numeric(latest_bs, "totalAssets", "TotalAssets")
            total_equity = _pick_numeric(
                latest_bs,
                "totalStockholderEquity",
                "totalShareholderEquity",
                "shareholdersEquity",
                "totalEquity",
                "TotalStockholderEquity",
            )
            total_debt = _first_present(
                _pick_numeric(latest_bs, "shortLongTermDebtTotal", "ShortLongTermDebtTotal"),
                _pick_numeric(latest_bs, "totalDebt", "TotalDebt"),
                _sum_present(
                    [
                        _pick_numeric(latest_bs, "shortTermDebt", "ShortTermDebt"),
                        _pick_numeric(latest_bs, "longTermDebt", "LongTermDebt"),
                    ]
                ),
            )
            debt_to_equity = _safe_div(total_debt, total_equity)
            roe = _frac_from_percentish(_first_present(highlights.get("ROE"), _safe_div(net_income_ttm, total_equity)))
            roa = _frac_from_percentish(_first_present(highlights.get("ROA"), _safe_div(net_income_ttm, total_assets)))

            shares_outstanding = safe_float(_first_present(shares.get("SharesOutstanding"), highlights.get("SharesOutstanding")))
            float_shares = safe_float(_first_present(shares.get("SharesFloat"), shares.get("FloatShares"), highlights.get("SharesFloat")))
            market_cap = safe_float(_first_present(highlights.get("MarketCapitalization"), general.get("MarketCapitalization")))

            week_52_high = safe_float(_first_present(tech.get("52WeekHigh"), tech.get("WeekHigh52"), highlights.get("52WeekHigh")))
            week_52_low = safe_float(_first_present(tech.get("52WeekLow"), tech.get("WeekLow52"), highlights.get("52WeekLow")))

            asset_class = _infer_asset_class(general, etf_data)
            country = safe_str(_first_present(general.get("CountryISO"), general.get("CountryName"), general.get("Country")))
            exchange = safe_str(_first_present(general.get("Exchange"), general.get("PrimaryExchange")))
            currency = _canonicalize_currency_code(_first_present(general.get("CurrencyCode"), general.get("Currency"), highlights.get("Currency")))

            geo = _suffix_derived_geo_defaults(sym_raw)
            if not country and geo["country"]:
                country = geo["country"]
                warnings_list.append("fundamentals_country_from_suffix")
            if not currency and geo["currency"]:
                currency = geo["currency"]
                warnings_list.append("fundamentals_currency_from_suffix")
            if not exchange and geo["exchange"]:
                exchange = geo["exchange"]
                warnings_list.append("fundamentals_exchange_from_suffix")

            sector_val = safe_str(general.get("Sector"))
            industry_val = safe_str(general.get("Industry"))
            if not industry_val:
                warnings_list.append("industry_missing_from_provider")
            if not sector_val:
                warnings_list.append("sector_missing_from_provider")

            dividend_yield = _frac_from_percentish(
                _first_present(
                    splits.get("ForwardAnnualDividendYield"),
                    highlights.get("DividendYield"),
                    splits.get("TrailingAnnualDividendYield"),
                )
            )
            payout_ratio = _frac_from_percentish(_first_present(splits.get("PayoutRatio"), highlights.get("PayoutRatio")))

            # ---- eps_ttm with v4.7.1 FIX-B fallback + v4.7.2 YD sanity bound ----
            _EPS_FALLBACK_CEILING = 500.0
            _EPS_FALLBACK_FLOOR = -200.0

            eps_ttm_final = safe_float(
                _first_present(
                    highlights.get("EarningsShare"),
                    highlights.get("DilutedEpsTTM"),
                    highlights.get("EPSEstimateCurrentYear"),
                )
            )
            if eps_ttm_final is None:
                if net_income_ttm is not None and shares_outstanding not in (None, 0.0):
                    candidate = net_income_ttm / shares_outstanding
                    if _EPS_FALLBACK_FLOOR <= candidate <= _EPS_FALLBACK_CEILING:
                        eps_ttm_final = candidate
                        warnings_list.append("eps_ttm_fallback_used")
                    else:
                        logger.debug(
                            "[eodhd v4.9.1] eps_ttm fallback rejected for %s: "
                            "computed=%.4f outside bounds [%.1f, %.1f] (likely units mismatch)",
                            sym, candidate, _EPS_FALLBACK_FLOOR, _EPS_FALLBACK_CEILING,
                        )
                        warnings_list.append("eps_ttm_fallback_rejected")
                else:
                    warnings_list.append("eps_ttm_unavailable")

            patch = _clean_patch(
                {
                    "symbol": sym_raw,
                    "symbol_normalized": sym,
                    "provider": PROVIDER_NAME,
                    "data_source": PROVIDER_NAME,
                    "name": safe_str(_first_present(general.get("Name"), general.get("ShortName"), general.get("LongName"))),
                    "exchange": exchange,
                    "currency": currency,
                    "country": country,
                    "sector": sector_val,
                    "industry": industry_val,
                    "asset_class": asset_class,
                    "asset_type": asset_class,
                    "market_cap": market_cap,
                    "enterprise_value": safe_float(_first_present(highlights.get("EnterpriseValue"), valuation.get("EnterpriseValue"))),
                    "shares_outstanding": shares_outstanding,
                    "float_shares": float_shares,
                    "shares_float": float_shares,
                    "eps_ttm": eps_ttm_final,
                    "pe_ttm": safe_float(_first_present(valuation.get("TrailingPE"), highlights.get("PERatio"))),
                    "forward_pe": safe_float(_first_present(valuation.get("ForwardPE"), highlights.get("ForwardPE"))),
                    "pb_ratio": safe_float(_first_present(valuation.get("PriceBookMRQ"), valuation.get("PriceBook"))),
                    "ps_ratio": safe_float(_first_present(valuation.get("PriceSalesTTM"), valuation.get("PriceSales"))),
                    "peg_ratio": safe_float(_first_present(valuation.get("PEGRatio"), valuation.get("PegRatio"))),
                    "ev_ebitda": safe_float(_first_present(valuation.get("EnterpriseValueEbitda"), valuation.get("EnterpriseValueEBITDA"))),
                    "pb": safe_float(_first_present(valuation.get("PriceBookMRQ"), valuation.get("PriceBook"))),
                    "ps": safe_float(_first_present(valuation.get("PriceSalesTTM"), valuation.get("PriceSales"))),
                    "peg": safe_float(_first_present(valuation.get("PEGRatio"), valuation.get("PegRatio"))),
                    "dividend_yield": dividend_yield,
                    "payout_ratio": payout_ratio,
                    "roe": roe,
                    "roa": roa,
                    "net_margin": _frac_from_percentish(_first_present(highlights.get("ProfitMargin"), profit_margin)),
                    "revenue_growth": _frac_from_percentish(_first_present(highlights.get("RevenueGrowth"), revenue_growth_yoy)),
                    "revenue_growth_yoy": _frac_from_percentish(_first_present(highlights.get("RevenueGrowth"), revenue_growth_yoy)),
                    "earnings_growth": _frac_from_percentish(_first_present(highlights.get("EarningsGrowth"), earnings_growth)),
                    "beta": safe_float(_first_present(tech.get("Beta"), highlights.get("Beta"))),
                    "beta_5y": safe_float(_first_present(tech.get("Beta"), highlights.get("Beta"))),
                    "week_52_high": week_52_high,
                    "week_52_low": week_52_low,
                    "revenue_ttm": revenue_ttm,
                    "revenue": revenue_ttm,
                    "gross_margin": _frac_from_percentish(_first_present(highlights.get("GrossMargin"), gross_margin)),
                    "operating_margin": _frac_from_percentish(_first_present(highlights.get("OperatingMargin"), operating_margin)),
                    "profit_margin": _frac_from_percentish(_first_present(highlights.get("ProfitMargin"), profit_margin)),
                    "free_cash_flow_ttm": fcf_ttm,
                    "fcf_ttm": fcf_ttm,
                    "debt_to_equity": debt_to_equity,
                    "d_e_ratio": debt_to_equity,
                    "warnings": warnings_list if warnings_list else None,
                    "last_updated_utc": _utc_iso(),
                    "last_updated_riyadh": _riyadh_iso(),
                }
            )
            await self.fund_cache.set(ck, patch)
            return patch, None

        return await self._sf.do(ck, _do)

    async def fetch_history_stats(self, symbol: str) -> Tuple[Dict[str, Any], Optional[str]]:
        sym_raw = (symbol or "").strip().upper()
        sym = normalize_eodhd_symbol(sym_raw)
        if not sym:
            return {}, "invalid_symbol"

        if _KSA_RE.match(sym) and _ksa_blocked_by_default() and not _allow_ksa_override():
            return {}, "ksa_blocked"

        ck = f"h:{sym}"
        cached = await self.hist_cache.get(ck)
        if cached:
            return cached, None

        async def _do() -> Tuple[Dict[str, Any], Optional[str]]:
            warnings_list: List[str] = []
            days = _env_int("EODHD_HISTORY_DAYS", 420, lo=60, hi=5000)
            from_date = (datetime.now(timezone.utc) - timedelta(days=days)).strftime("%Y-%m-%d")
            data, err = await self._request_json(f"eod/{sym}", params={"from": from_date})
            if err or not isinstance(data, list):
                # v4.8.0 AC + v4.9.0 AH paths
                if err == "circuit_open":
                    return _build_circuit_open_patch(sym_raw, sym), err
                return _build_error_patch_with_geo(sym_raw, sym, err or "bad_payload"), err or "bad_payload"

            rows = [r for r in data if isinstance(r, dict)]
            rows = [r for r in rows if safe_float(r.get("close")) is not None]
            rows.sort(key=lambda r: safe_str(r.get("date")) or "")

            closes: List[float] = []
            highs: List[float] = []
            lows: List[float] = []
            opens: List[float] = []
            vols: List[float] = []
            last_hist_dt: Optional[str] = None

            for row in rows:
                c = safe_float(row.get("close"))
                if c is not None:
                    closes.append(c)
                    last_hist_dt = safe_str(row.get("date")) or last_hist_dt
                h = safe_float(row.get("high"))
                if h is not None:
                    highs.append(h)
                l = safe_float(row.get("low"))
                if l is not None:
                    lows.append(l)
                o = safe_float(row.get("open"))
                if o is not None:
                    opens.append(o)
                v = safe_float(row.get("volume"))
                if v is not None:
                    vols.append(v)

            n = len(closes)
            if n == 0:
                warnings_list.append("history_empty")
                patch = _clean_patch(
                    {
                        "symbol": sym_raw,
                        "symbol_normalized": sym,
                        "provider": PROVIDER_NAME,
                        "data_source": PROVIDER_NAME,
                        "history_points": 0,
                        "history_source": PROVIDER_NAME,
                        "warnings": warnings_list,
                        "last_updated_utc": _utc_iso(),
                        "last_updated_riyadh": _riyadh_iso(),
                    }
                )
                await self.hist_cache.set(ck, patch)
                return patch, None

            if n < 2:
                warnings_list.append("history_too_short")
                patch = _clean_patch(
                    {
                        "symbol": sym_raw,
                        "symbol_normalized": sym,
                        "provider": PROVIDER_NAME,
                        "data_source": PROVIDER_NAME,
                        "history_points": n,
                        "history_source": PROVIDER_NAME,
                        "history_last_utc": last_hist_dt,
                        "warnings": warnings_list,
                        "last_updated_utc": _utc_iso(),
                        "last_updated_riyadh": _riyadh_iso(),
                    }
                )
                await self.hist_cache.set(ck, patch)
                return patch, None

            latest_row = rows[-1]
            prev_row = rows[-2] if len(rows) >= 2 else {}
            last = closes[-1]
            prev = safe_float(prev_row.get("close"))
            latest_open = safe_float(latest_row.get("open"))
            latest_high = safe_float(latest_row.get("high"))
            latest_low = safe_float(latest_row.get("low"))
            latest_vol = safe_float(latest_row.get("volume"))
            history_price_change = (last - prev) if (last is not None and prev not in (None, 0.0)) else None
            history_percent_change = ((last / prev) - 1.0) if (last is not None and prev not in (None, 0.0)) else None

            win_52 = _env_int("EODHD_HISTORY_WINDOW_52W", 252, lo=60, hi=800)
            close_win = closes[-min(win_52, len(closes)):]
            high_win = highs[-min(win_52, len(highs)):] if highs else []
            low_win = lows[-min(win_52, len(lows)):] if lows else []
            high_52 = max(high_win) if high_win else max(close_win)
            low_52 = min(low_win) if low_win else min(close_win)
            pos_52_frac = None
            if high_52 != low_52:
                pos_52_frac = (last - low_52) / (high_52 - low_52)

            # v4.7.1 FIX-A: convert fraction (0-1) to percent points (0-100)
            pos_52_pct = (pos_52_frac * 100.0) if pos_52_frac is not None else None

            avg_vol_10 = sum(vols[-10:]) / 10.0 if len(vols) >= 10 else (sum(vols) / len(vols) if vols else None)
            avg_vol_30 = sum(vols[-30:]) / 30.0 if len(vols) >= 30 else (sum(vols) / len(vols) if vols else None)

            rets_all = _daily_returns(closes)
            rets_1y = rets_all[-min(len(rets_all), 252):] if rets_all else []
            rets_90 = rets_all[-min(len(rets_all), 90):] if rets_all else []
            rets_30 = rets_all[-min(len(rets_all), 30):] if rets_all else []

            vol_30 = _annualized_vol(rets_30)
            vol_90 = _annualized_vol(rets_90)
            vol_1y = _annualized_vol(rets_1y)
            mdd_1y = _max_drawdown(closes[-min(len(closes), 252):])
            var95 = _var_95_1d(rets_1y)
            rf = _env_float("EODHD_RISK_FREE_RATE", 0.03, lo=0.0, hi=0.20)
            sharpe = _sharpe_1y(rets_1y, rf_annual=rf)
            rsi14 = _rsi14(closes[-100:])

            if n >= 60 and vol_30 is None and vol_90 is None and sharpe is None and var95 is None and mdd_1y is None:
                warnings_list.append("risk_metrics_unavailable")

            def _ret(k: int) -> Optional[float]:
                if n <= k:
                    return None
                base = closes[-1 - k]
                if not base:
                    return None
                return (last / base) - 1.0

            patch = _clean_patch(
                {
                    "symbol": sym_raw,
                    "symbol_normalized": sym,
                    "provider": PROVIDER_NAME,
                    "data_source": PROVIDER_NAME,
                    "current_price": last,
                    "price": last,
                    "previous_close": prev,
                    "prev_close": prev,
                    "day_open": latest_open,
                    "open": latest_open,
                    "day_high": latest_high,
                    "day_low": latest_low,
                    "volume": latest_vol,
                    "price_change": history_price_change,
                    "change": history_price_change,
                    "percent_change": history_percent_change,
                    "change_pct": history_percent_change,
                    "week_52_low": low_52,
                    "week_52_high": high_52,
                    "week_52_position_pct": pos_52_pct,
                    "avg_vol_10d": avg_vol_10,
                    "avg_volume_10d": avg_vol_10,
                    "avg_vol_30d": avg_vol_30,
                    "avg_volume_30d": avg_vol_30,
                    "rsi_14": rsi14,
                    "volatility_30d": vol_30,
                    "volatility_90d": vol_90,
                    "volatility_365d": vol_1y,
                    "max_drawdown_1y": mdd_1y,
                    "var_95_1d": var95,
                    "sharpe_1y": sharpe,
                    "returns_1w": _ret(5),
                    "returns_1m": _ret(21),
                    "returns_3m": _ret(63),
                    "returns_6m": _ret(126),
                    "returns_12m": _ret(252),
                    "history_points": n,
                    "history_source": PROVIDER_NAME,
                    "history_last_utc": last_hist_dt,
                    "warnings": warnings_list if warnings_list else None,
                    "last_updated_utc": _utc_iso(),
                    "last_updated_riyadh": _riyadh_iso(),
                }
            )
            patch["52w_high"] = patch.get("week_52_high")
            patch["52w_low"] = patch.get("week_52_low")
            patch["position_52w_pct"] = patch.get("week_52_position_pct")

            await self.hist_cache.set(ck, patch)
            return patch, None

        return await self._sf.do(ck, _do)

    async def fetch_enriched_quote_patch(self, symbol: str) -> Dict[str, Any]:
        now_utc = _utc_iso()
        now_riy = _riyadh_iso()
        sym_raw = (symbol or "").strip().upper()
        sym_norm = normalize_eodhd_symbol(sym_raw)

        if not sym_norm:
            return _clean_patch(
                {
                    "symbol": sym_raw,
                    "symbol_normalized": "",
                    "provider": PROVIDER_NAME,
                    "data_source": PROVIDER_NAME,
                    "data_quality": "MISSING",
                    "error": "invalid_symbol",
                    "error_detail": "normalize_eodhd_symbol returned empty",
                    "last_error_class": "InvalidSymbol",
                    "warnings": ["fetch_failed:invalid_symbol"],
                    "last_updated_utc": now_utc,
                    "last_updated_riyadh": now_riy,
                }
            )

        if not self.api_key:
            return _clean_patch(
                {
                    "symbol": sym_raw,
                    "symbol_normalized": sym_norm,
                    "provider": PROVIDER_NAME,
                    "data_source": PROVIDER_NAME,
                    "data_quality": "MISSING",
                    "error": "missing_api_key",
                    "error_detail": "EODHD_API_KEY (or EODHD_API_TOKEN/EODHD_KEY) is not set",
                    "last_error_class": "MissingApiKey",
                    "warnings": ["fetch_failed:missing_api_key"],
                    "last_updated_utc": now_utc,
                    "last_updated_riyadh": now_riy,
                }
            )

        if _KSA_RE.match(sym_norm) and _ksa_blocked_by_default() and not _allow_ksa_override():
            return _clean_patch(
                {
                    "symbol": sym_raw,
                    "symbol_normalized": sym_norm,
                    "provider": PROVIDER_NAME,
                    "data_source": PROVIDER_NAME,
                    "data_quality": "BLOCKED",
                    "error": "ksa_blocked",
                    "error_detail": "KSA_DISALLOW_EODHD=true (override with ALLOW_EODHD_KSA=1)",
                    "last_error_class": "KsaBlocked",
                    "warnings": ["fetch_failed:ksa_blocked"],
                    "last_updated_utc": now_utc,
                    "last_updated_riyadh": now_riy,
                }
            )

        # v4.8.0 AC: short-circuit when breaker is open.
        health = await _get_health()
        if await health.is_open():
            patch = _build_circuit_open_patch(sym_raw, sym_norm)
            patch["last_updated_utc"] = now_utc
            patch["last_updated_riyadh"] = now_riy
            return _clean_patch(patch)

        enable_fund = _env_bool("EODHD_ENABLE_FUNDAMENTALS", True)
        enable_hist = _env_bool("EODHD_ENABLE_HISTORY", True)

        tasks: List[asyncio.Task] = [asyncio.create_task(self.fetch_quote(sym_raw))]
        if enable_fund:
            tasks.append(asyncio.create_task(self.fetch_fundamentals(sym_raw)))
        if enable_hist:
            tasks.append(asyncio.create_task(self.fetch_history_stats(sym_raw)))

        results = await asyncio.gather(*tasks, return_exceptions=True)

        merged: Dict[str, Any] = {
            "symbol": sym_raw,
            "symbol_normalized": sym_norm,
            "provider": PROVIDER_NAME,
            "data_source": PROVIDER_NAME,
            "data_sources": [PROVIDER_NAME],
            "provider_role": "primary_global",
            "last_updated_utc": now_utc,
            "last_updated_riyadh": now_riy,
        }
        errors: List[str] = []
        last_error_class: Optional[str] = None
        circuit_open_seen = False  # v4.8.0 AC
        unhealthy_error_seen = False  # v4.9.0 AH

        for r in results:
            if isinstance(r, Exception):
                cls_name = r.__class__.__name__
                errors.append(f"exception:{cls_name}")
                last_error_class = cls_name
                continue
            try:
                patch, err = r  # type: ignore[misc]
            except Exception:
                errors.append("bad_task_result")
                last_error_class = last_error_class or "BadTaskResult"
                continue
            if err:
                errors.append(str(err))
                lerr = str(err).lower()
                if "circuit_open" in lerr:
                    circuit_open_seen = True
                    last_error_class = last_error_class or "CircuitOpen"
                elif "auth_error" in lerr:
                    unhealthy_error_seen = True  # v4.9.0 AH
                    last_error_class = last_error_class or "AuthError"
                elif "ip_blocked" in lerr:
                    unhealthy_error_seen = True  # v4.9.0 AH
                    last_error_class = last_error_class or "IpBlocked"
                elif "quota_or_rate_limit" in lerr or "429" in lerr:
                    last_error_class = last_error_class or "RateLimited"
                elif "not_found" in lerr or "404" in lerr:
                    last_error_class = last_error_class or "NotFound"
                elif "network_error" in lerr:
                    last_error_class = last_error_class or "NetworkError"
                elif "bad_payload" in lerr or "invalid_json" in lerr:
                    last_error_class = last_error_class or "InvalidPayload"
                else:
                    last_error_class = last_error_class or "FetchError"

            if isinstance(patch, dict) and patch:
                _merge_warnings_inplace(merged, patch.get("warnings"))

                for k, v in patch.items():
                    if k == "warnings":
                        continue
                    if v is None:
                        continue
                    if k not in merged or merged.get(k) in (None, "", [], {}):
                        merged[k] = v

        # ---- Cross-key consistency fills (v4.6.0 behavior) ----
        if merged.get("current_price") is None and merged.get("price") is not None:
            merged["current_price"] = merged.get("price")
        if merged.get("price") is None and merged.get("current_price") is not None:
            merged["price"] = merged.get("current_price")
        if merged.get("previous_close") is None and merged.get("prev_close") is not None:
            merged["previous_close"] = merged.get("prev_close")
        if merged.get("prev_close") is None and merged.get("previous_close") is not None:
            merged["prev_close"] = merged.get("previous_close")
        if merged.get("open") is None and merged.get("day_open") is not None:
            merged["open"] = merged.get("day_open")
        if merged.get("day_open") is None and merged.get("open") is not None:
            merged["day_open"] = merged.get("open")

        if merged.get("market_cap") is None and merged.get("shares_outstanding") is not None and merged.get("current_price") is not None:
            try:
                merged["market_cap"] = float(merged["shares_outstanding"]) * float(merged["current_price"])
            except Exception:
                pass

        if merged.get("percent_change") is None and merged.get("current_price") is not None and merged.get("previous_close") not in (None, 0.0):
            try:
                merged["percent_change"] = (float(merged["current_price"]) / float(merged["previous_close"])) - 1.0
            except Exception:
                pass
        if merged.get("price_change") is None and merged.get("current_price") is not None and merged.get("previous_close") is not None:
            try:
                merged["price_change"] = float(merged["current_price"]) - float(merged["previous_close"])
            except Exception:
                pass

        if merged.get("week_52_position_pct") is None:
            pos = _safe_div(
                (safe_float(merged.get("current_price")) or 0.0) - (safe_float(merged.get("week_52_low")) or 0.0),
                (safe_float(merged.get("week_52_high")) or 0.0) - (safe_float(merged.get("week_52_low")) or 0.0),
            )
            if pos is not None:
                merged["week_52_position_pct"] = pos * 100.0

        # ---- v4.7.3 ZB: merged-level 52W currency-sanity guard ----
        validation_warnings = _validate_52w_bounds_merged(
            merged,
            enabled=_price_sanity_enabled(),
            ratio_high=_price_ratio_high(),
            ratio_low=_price_ratio_low(),
        )
        if validation_warnings:
            _merge_warnings_inplace(merged, validation_warnings)
            if "week_52_high_unit_mismatch_dropped" in validation_warnings or \
               "week_52_low_unit_mismatch_dropped" in validation_warnings:
                if merged.get("week_52_high") is None or merged.get("week_52_low") is None:
                    merged["week_52_position_pct"] = None
                    merged["position_52w_pct"] = None

        # ---- v4.8.0 AC + v4.9.0 AH: cross-provider unhealthy signal ----
        # v4.8.0: if any of the three concurrent fetches surfaced a
        #   circuit_open error, hoist the cross-provider marker.
        # v4.9.0: also hoist when any concurrent fetch saw an auth_error
        #   or ip_blocked error. This is belt-and-suspenders alongside
        #   the per-failure marker added by _build_error_patch_with_geo
        #   on the individual error patches — guarantees the marker is
        #   present on the merged row even if `_clean_patch` or
        #   `_merge_warnings_inplace` somehow dropped a list entry.
        if circuit_open_seen:
            _merge_warnings_inplace(merged, ["circuit_open", "provider_unhealthy:eodhd"])
        elif unhealthy_error_seen:
            _merge_warnings_inplace(merged, ["provider_unhealthy:eodhd"])

        # ---- Final data-quality verdict + legacy warning field ----
        if merged.get("current_price") is None:
            merged["data_quality"] = "MISSING"
            merged["error"] = "fetch_failed"
            merged["error_detail"] = ",".join(sorted(set(errors))) if errors else "no_data"
            if last_error_class:
                merged["last_error_class"] = last_error_class
        else:
            merged["data_quality"] = "OK"
            if errors:
                merged["warning"] = "partial_sources"
                merged["info"] = {"warnings": sorted(set(errors))[:6]}

        # Alias updates after validation
        merged["change"] = merged.get("price_change")
        merged["change_pct"] = merged.get("percent_change")
        merged["52w_high"] = merged.get("week_52_high")
        merged["52w_low"] = merged.get("week_52_low")
        merged["beta_5y"] = merged.get("beta_5y") or merged.get("beta")
        merged["avg_volume_10d"] = merged.get("avg_volume_10d") or merged.get("avg_vol_10d")
        merged["avg_volume_30d"] = merged.get("avg_volume_30d") or merged.get("avg_vol_30d")
        merged["fcf_ttm"] = merged.get("fcf_ttm") or merged.get("free_cash_flow_ttm")
        merged["d_e_ratio"] = merged.get("d_e_ratio") or merged.get("debt_to_equity")

        canon_currency = _canonicalize_currency_code(merged.get("currency"))
        if canon_currency:
            merged["currency"] = canon_currency

        merged_geo = _suffix_derived_geo_defaults(sym_raw)
        if not merged.get("country") and merged_geo["country"]:
            merged["country"] = merged_geo["country"]
        if not merged.get("currency") and merged_geo["currency"]:
            merged["currency"] = merged_geo["currency"]
        if not merged.get("exchange") and merged_geo["exchange"]:
            merged["exchange"] = merged_geo["exchange"]

        return _clean_patch(merged)

    async def fetch_price_history(self, symbol: str, days: Optional[int] = None, *args: Any, **kwargs: Any) -> List[Dict[str, Any]]:
        sym = normalize_eodhd_symbol((symbol or "").strip().upper())
        if not sym:
            return []
        if _KSA_RE.match(sym) and _ksa_blocked_by_default() and not _allow_ksa_override():
            return []
        hist_days = max(30, int(days or _env_int("EODHD_HISTORY_DAYS", 420, lo=60, hi=5000)))
        from_date = (datetime.now(timezone.utc) - timedelta(days=hist_days)).strftime("%Y-%m-%d")
        data, err = await self._request_json(f"eod/{sym}", params={"from": from_date})
        if err or not isinstance(data, list):
            return []
        return [r for r in data if isinstance(r, dict)]

    async def fetch_history(self, symbol: str, days: Optional[int] = None, *args: Any, **kwargs: Any) -> List[Dict[str, Any]]:
        return await self.fetch_price_history(symbol, days=days, *args, **kwargs)

    async def fetch_ohlc_history(self, symbol: str, days: Optional[int] = None, *args: Any, **kwargs: Any) -> List[Dict[str, Any]]:
        return await self.fetch_price_history(symbol, days=days, *args, **kwargs)

    async def fetch_quote_patch(self, symbol: str, *args: Any, **kwargs: Any) -> Dict[str, Any]:
        """
        v4.9.0 AI: fallback path now emits `provider_unhealthy:eodhd`
        when the upstream err indicates an auth-class failure, mirroring
        `_build_error_patch_with_geo`. Historically dead code (fetch_quote
        always returns a non-empty patch via _build_error_patch_with_geo),
        but preserved for defensive consistency in case any future caller
        invokes this alias directly.
        """
        patch, err = await self.fetch_quote(symbol)
        if patch:
            return patch

        # v4.9.0 AI: per-failure marker in fallback path.
        warnings_out: List[str] = [f"fetch_failed:{err or 'fetch_quote_failed'}"]
        if _err_indicates_provider_unhealthy(err):
            warnings_out.append("provider_unhealthy:eodhd")

        return _clean_patch(
            {
                "symbol": (symbol or "").strip().upper(),
                "provider": PROVIDER_NAME,
                "data_source": PROVIDER_NAME,
                "error": err or "fetch_quote_failed",
                "data_quality": "MISSING",
                "warnings": warnings_out,
                "last_updated_utc": _utc_iso(),
                "last_updated_riyadh": _riyadh_iso(),
            }
        )

    async def fetch_enriched_quote(self, symbol: str, *args: Any, **kwargs: Any) -> Dict[str, Any]:
        return await self.fetch_enriched_quote_patch(symbol)

    async def aclose(self) -> None:
        try:
            await self._client.aclose()
        except Exception:
            pass


# =============================================================================
# Singleton + Engine-facing functions
# (PRESERVED byte-identical in v4.9.0; diagnose_health + get_provider_stats
#  unchanged from v4.8.0)
# =============================================================================
_INSTANCE: Optional[EODHDClient] = None
_INSTANCE_LOCK = asyncio.Lock()


async def get_client() -> EODHDClient:
    global _INSTANCE
    if _INSTANCE is None:
        async with _INSTANCE_LOCK:
            if _INSTANCE is None:
                _INSTANCE = EODHDClient()
    return _INSTANCE


async def fetch_enriched_quote_patch(symbol: str, *args: Any, **kwargs: Any) -> Dict[str, Any]:
    client = await get_client()
    return await client.fetch_enriched_quote_patch(symbol)


async def fetch_enriched_quote(symbol: str, *args: Any, **kwargs: Any) -> Dict[str, Any]:
    client = await get_client()
    return await client.fetch_enriched_quote_patch(symbol)


async def enriched_quote(symbol: str, *args: Any, **kwargs: Any) -> Dict[str, Any]:
    return await fetch_enriched_quote_patch(symbol, *args, **kwargs)


async def quote(symbol: str, *args: Any, **kwargs: Any) -> Dict[str, Any]:
    client = await get_client()
    patch, err = await client.fetch_quote(symbol)
    if err:
        raise RuntimeError(err)
    return patch


async def get_quote(symbol: str, *args: Any, **kwargs: Any) -> Dict[str, Any]:
    return await quote(symbol, *args, **kwargs)


async def fetch_quote(symbol: str, *args: Any, **kwargs: Any) -> Dict[str, Any]:
    return await quote(symbol, *args, **kwargs)


async def fetch_quote_patch(symbol: str, *args: Any, **kwargs: Any) -> Dict[str, Any]:
    client = await get_client()
    return await client.fetch_quote_patch(symbol, *args, **kwargs)


async def get_quote_patch(symbol: str, *args: Any, **kwargs: Any) -> Dict[str, Any]:
    """
    v4.9.1 AL: Module-level alias for `fetch_quote_patch`.

    Engine v5.61.0's `_fetch_patch` discovers provider callables via
    `_pick_provider_callable` with this preference order:

        ("get_quote_patch", "fetch_quote", "get_quote", "fetch_patch")

    Prior to v4.9.1, eodhd_provider did not define `get_quote_patch`,
    so the engine fell through to `fetch_quote` (which raises
    RuntimeError on err and discards the v4.9.0 `provider_unhealthy:
    eodhd` marker built by `_build_error_patch_with_geo`). This alias
    aligns eodhd_provider with the engine's preferred name so the
    engine receives the error patch with the marker intact.

    No behavioral change to `fetch_quote` / `quote` / `get_quote` —
    their historical raise-on-err contract is preserved for any
    external caller that depends on it.
    """
    return await fetch_quote_patch(symbol, *args, **kwargs)


async def fetch_history(symbol: str, days: Optional[int] = None, *args: Any, **kwargs: Any) -> List[Dict[str, Any]]:
    client = await get_client()
    return await client.fetch_history(symbol, days=days, *args, **kwargs)


async def fetch_price_history(symbol: str, days: Optional[int] = None, *args: Any, **kwargs: Any) -> List[Dict[str, Any]]:
    client = await get_client()
    return await client.fetch_price_history(symbol, days=days, *args, **kwargs)


async def fetch_ohlc_history(symbol: str, days: Optional[int] = None, *args: Any, **kwargs: Any) -> List[Dict[str, Any]]:
    client = await get_client()
    return await client.fetch_ohlc_history(symbol, days=days, *args, **kwargs)


async def fetch_quotes(symbols: List[str], *args: Any, **kwargs: Any) -> List[Dict[str, Any]]:
    client = await get_client()
    out: List[Dict[str, Any]] = []
    sem = asyncio.Semaphore(max(1, _env_int("EODHD_BATCH_CONCURRENCY", 8, lo=1, hi=32)))

    async def _one(s: str) -> Optional[Dict[str, Any]]:
        async with sem:
            try:
                return await client.fetch_enriched_quote_patch(s)
            except Exception:
                return None

    results = await asyncio.gather(*[_one(s) for s in (symbols or [])], return_exceptions=True)
    for r in results:
        if isinstance(r, dict) and r:
            out.append(r)
    return out


# =============================================================================
# v4.8.0 — diagnose_health() and get_provider_stats()
# (PRESERVED byte-identical in v4.9.0)
# =============================================================================

async def diagnose_health(probe_symbol: Optional[str] = None) -> Dict[str, Any]:
    """
    v4.8.0 AD: structured operator-visible health report for the EODHD
    provider. Probes a known-stable symbol and returns a JSON-serializable
    dict describing api key presence, circuit state, probe result, and
    cumulative session statistics.
    """
    now_utc = _utc_iso()
    now_riy = _riyadh_iso()

    probe_sym = (probe_symbol or _health_probe_symbol()).strip()
    if not probe_sym:
        probe_sym = "AAPL.US"

    health = await _get_health()
    snapshot = await health.snapshot()
    api_key_present = bool(_token())

    probe_info: Dict[str, Any] = {
        "symbol": probe_sym,
        "attempted": False,
        "fetched": False,
        "latency_ms": None,
        "error_class": None,
        "current_price": None,
        "data_quality": None,
    }

    if not api_key_present:
        probe_info["error_class"] = "MissingApiKey"
    elif snapshot.get("circuit_state") == "open":
        probe_info["error_class"] = "CircuitOpen"
    else:
        probe_info["attempted"] = True
        start_ms = time.monotonic()
        try:
            client = await get_client()
            patch = await client.fetch_enriched_quote_patch(probe_sym)
            elapsed_ms = (time.monotonic() - start_ms) * 1000.0
            probe_info["latency_ms"] = round(elapsed_ms, 2)
            if isinstance(patch, dict):
                probe_info["data_quality"] = safe_str(patch.get("data_quality"))
                price = safe_float(patch.get("current_price"))
                if price is not None:
                    probe_info["fetched"] = True
                    probe_info["current_price"] = price
                else:
                    probe_info["error_class"] = (
                        safe_str(patch.get("last_error_class")) or "FetchError"
                    )
        except Exception as e:
            elapsed_ms = (time.monotonic() - start_ms) * 1000.0
            probe_info["latency_ms"] = round(elapsed_ms, 2)
            probe_info["error_class"] = e.__class__.__name__

    snapshot_after = await health.snapshot()

    ok = bool(probe_info["fetched"]) and snapshot_after.get("circuit_state") == "closed"

    return {
        "ok": ok,
        "provider": PROVIDER_NAME,
        "provider_version": PROVIDER_VERSION,
        "timestamp_utc": now_utc,
        "timestamp_riyadh": now_riy,
        "api_key_present": api_key_present,
        "circuit_state": snapshot_after.get("circuit_state"),
        "circuit_breaker_enabled": snapshot_after.get("circuit_breaker_enabled"),
        "consecutive_auth_errors": snapshot_after.get("consecutive_auth_errors"),
        "consecutive_ip_blocks": snapshot_after.get("consecutive_ip_blocks"),
        "last_failure_class": snapshot_after.get("last_failure_class"),
        "last_success_age_sec": snapshot_after.get("last_success_age_sec"),
        "last_failure_age_sec": snapshot_after.get("last_failure_age_sec"),
        "circuit_opened_age_sec": snapshot_after.get("circuit_opened_age_sec"),
        "probe": probe_info,
        "session_stats": snapshot_after.get("session_stats"),
    }


async def get_provider_stats() -> Dict[str, Any]:
    """
    v4.8.0 AE: cumulative session counters and circuit state. No API
    call. Cheap — operators can poll this on a timer to watch provider
    behavior over time without triggering a probe.
    """
    now_utc = _utc_iso()
    health = await _get_health()
    snapshot = await health.snapshot()
    return {
        "provider": PROVIDER_NAME,
        "provider_version": PROVIDER_VERSION,
        "timestamp_utc": now_utc,
        "api_key_present": bool(_token()),
        "circuit_state": snapshot.get("circuit_state"),
        "circuit_breaker_enabled": snapshot.get("circuit_breaker_enabled"),
        "consecutive_auth_errors": snapshot.get("consecutive_auth_errors"),
        "consecutive_ip_blocks": snapshot.get("consecutive_ip_blocks"),
        "last_failure_class": snapshot.get("last_failure_class"),
        "last_success_age_sec": snapshot.get("last_success_age_sec"),
        "last_failure_age_sec": snapshot.get("last_failure_age_sec"),
        "circuit_opened_age_sec": snapshot.get("circuit_opened_age_sec"),
        "session_stats": snapshot.get("session_stats"),
    }


__all__ = [
    "PROVIDER_NAME",
    "PROVIDER_VERSION",
    "VERSION",
    "EODHDClient",
    "get_client",
    "normalize_eodhd_symbol",
    "fetch_enriched_quote_patch",
    "fetch_enriched_quote",
    "enriched_quote",
    "quote",
    "get_quote",
    "fetch_quote",
    "fetch_quote_patch",
    # v4.9.1 AL: engine pick-order alias (preferred name `get_quote_patch`).
    "get_quote_patch",
    "fetch_history",
    "fetch_price_history",
    "fetch_ohlc_history",
    "fetch_quotes",
    # v4.8.0 (preserved v4.9.0 / v4.9.1)
    "diagnose_health",
    "get_provider_stats",
]
