#!/usr/bin/env python3
# core/providers/yahoo_fundamentals_provider.py
"""
================================================================================
Yahoo Finance Fundamentals Provider -- v6.7.0
================================================================================
v6.7.0 -- YF-2 ON + TIGHT CROSS-RESPONSE BAND + DECLARED-IDENTITY ECHO
          (Fixes YF-2b / YF-3 / YF-4)
--------------------------------------------------------------------------------
WHY (live workbook audit 2026-07-13, export v39 + repo forensics): the
2026-07-12/13 sync-scale writes carried 63.6-89.2% transposed enrichment on
Global_Markets -- SAME-LOCALE crossings the v6.6.0 guards structurally pass:
GOOG served Gulfport Energy's name/EPS/PE (US<->US, Yahoo stamped the
REQUESTED ticker, price plausible). The v6.6.0 header documented exactly
this residual; today it is the live failure mode, so the residual shrinks:

YF-2b DEFAULT FLIP: TFB_FUND_PRICE_COHERENCE_GUARD now defaults ON (=0 is
the kill switch) -- same house rule as YF-1: a documented control that the
live failure class violates ships armed.

YF-3 TIGHT CROSS-RESPONSE BAND: YF-2 compared info/fast_info price against
this fetch's OWN history last close using the UNIT-CLASS thresholds
(8.0/0.125) -- the live GOOG<->Gulfport crossing sits at 2.14x and sailed
through. Two SAME-DAY responses for the SAME instrument cannot legitimately
disagree 2x (a >100% one-day move; auto-adjusted history makes split days a
non-class), so YF-2's conviction test now uses a dedicated tight band --
TFB_FUND_PRICE_BAND_HIGH (default 2.0, floor 1.25) / TFB_FUND_PRICE_BAND_LOW
(default 0.5, ceiling 0.8) -- while every OTHER consumer of the unit-class
thresholds (52W bounds, sanity guard) keeps 8.0/0.125 untouched. Same
conviction semantics: discard info AND fast_info, tag
provider_price_incoherent, fall to honest history/None. Stated honestly:
a crossing whose two instruments trade within 2x of each other still passes
here -- the engine AY-2 / route RC / sync L3b layers (all shipped
2026-07-13) own that residue via the P/E==Price/EPS invariant.

YF-4 DECLARED-IDENTITY ECHO: the patch now carries the identity Yahoo's OWN
response declared -- out["code"] = the raw symbol/underlyingSymbol Yahoo
stamped -- captured BEFORE any conviction resets info. The engine's AU-1b
raw-patch check reads exactly this key, so even with every provider-local
guard kill-switched OFF the engine gets an independent second look; a
response that declares the requested ticker echoes it back (harmless), and
canonicalization is unaffected (the engine passes normalized_symbol
explicitly). Belt-and-braces, zero behavior change on honest responses.

Version: PROVIDER_VERSION = "6.7.0". All prior WHYs preserved verbatim.
Zero functions removed; additions: _fund_price_band_high,
_fund_price_band_low.
================================================================================
(v6.6.0 header retained below)
================================================================================
Yahoo Finance Fundamentals Provider -- v6.6.0
================================================================================
v6.6.0 -- GUARD DEFAULT ON + FAST-INFO SUPPRESSION + OWN-HISTORY PRICE
          COHERENCE (Fixes YF-1 / YF-1b / YF-2)
--------------------------------------------------------------------------------
WHY (live workbook audit 2026-07-07, "Market_Share_Deepseek-V3" export v30):
fresh wrong-company names were written onto NESR.US ("Invesco Municipal
Trust"), LDI.US ("Axalta"), ROG.SW ("BKV Corporation") and 0011.HK
("BuzzFeed") with same-morning stamps -- the v6.4.0/v6.5.0 identity+locale
guard that exists precisely for this class was sitting DEFAULT OFF and
therefore did nothing in production while the damage recurred daily.

YF-1 DEFAULT FLIP: TFB_FUND_IDENTITY_GUARD now defaults ON (=0 is the kill
switch). Deliberate deviation from the OFF-first pattern, under the house
rule that a fix enforcing an already-documented, actively-violated control
may ship ON: the control is documented in this file's own v6.4.0/v6.5.0
WHYs (plus eodhd AO and engine AU-1), the violation is live today, and the
guard is conservative by construction (absent/own-symbol responses are
never touched).

YF-1b FAST-INFO SUPPRESSION: on an identity or locale mismatch the old code
discarded `info` but left `fast_info` -- fetched from the SAME crossed
session response set -- free to supply prices/52W into the patch. Both are
now suppressed together, so a convicted response set contributes nothing.

YF-2 OWN-HISTORY PRICE COHERENCE (TFB_FUND_PRICE_COHERENCE_GUARD, DEFAULT
OFF; thresholds reuse YF_PRICE_RATIO_HIGH/LOW, 8.0/0.125): the live
info/fast_info price is compared against this provider's OWN 3-month
history last close (independent HTTP; two responses crossing to the SAME
wrong instrument is vanishingly unlikely). A unit-class disagreement
convicts the whole info+fast_info set (discard + provider_price_incoherent
tag). Stated honestly: this catches fils/cents-class crossings; a
same-locale record carrying a PLAUSIBLE price with a wrong name (today's
NESR.US shape) has no in-provider invariant to test against and remains
uncaught here -- the symbol-base and locale checks (now ON) are the
operative defenses for that class, and they only catch it when Yahoo
stamps a different ticker or a foreign locale on the record. Residual risk
is documented, not hidden.

Version: PROVIDER_VERSION = "6.6.0". All prior WHYs (in this header and in
the guard-section comments below) preserved verbatim. Zero functions
removed (AST-verified).
================================================================================
(v6.3.1 header retained below)
================================================================================
Yahoo Finance Fundamentals Provider -- v6.3.1
================================================================================
v6.3.1 hotfix (over v6.3.0). Four audit fixes; output shape additive-only:

  1. (HIGH) No false default HOLD. v6.3.0's _blocking_fetch always set
     "recommendation": map_recommendation(raw_rec_key); since
     map_recommendation(None) -> "HOLD", a symbol with NO Yahoo
     recommendationKey emitted a fake HOLD that data_engine_v2 could
     capture as a provider rating (reopening the v5.77.17 corruption).
     Now: recommendation is emitted ONLY when Yahoo supplied a real
     rating key (else None -> stripped by clean_patch -> field absent).

  2. (MED) Substring precedence. map_recommendation("underperform_rating")
     returned HOLD because the generic "perform" token was tested before
     "underperform". The substring pass now iterates a length-descending
     view of the vocab (_RECO_VOCAB_BY_LEN) so longer/more-specific tokens
     win. Exact-match pass is unchanged. Same fix in extract_provider_rating.

  3. (MED/HIGH) SSOT symbol formatting for the yfinance call. The symbol
     sent to yf.Ticker came from this file's local normalize_symbol, which
     passes EODHD-style suffixes (.NSE/.XETRA/.AU/.KO) straight through --
     Yahoo can't resolve those, so international fundamentals came back
     empty. Now binds to_yahoo_symbol from the same normalize SSOT used
     for identity, with a local EODHD->Yahoo suffix-remap fallback if the
     import fails, and uses it for the ACTUAL yfinance symbol. Output
     keeps requested_symbol/symbol = canonical; provider_symbol = the
     Yahoo-formatted symbol actually queried.

  4. (OPTIONAL) Added EODHD-format .AU (ASX) and .KO (KRX) to the local
     _SUFFIX_TO_LOCALE_DEFAULTS fallback table (it already had .AX/.KS).

Purpose: Fallback fundamentals/profile source. EODHD remains primary for global
equities and yahoo_chart remains primary for Yahoo-style quote/history data.
(Header changelog condensed for working copy; behaviour preserved verbatim.)
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import math
import os
import pickle
import random
import re
import time
import zlib
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from enum import Enum
from typing import Any, Awaitable, Callable, Dict, Iterable, List, Optional, Tuple

# =============================================================================
# Logging
# =============================================================================

logger = logging.getLogger("core.providers.yahoo_fundamentals_provider")
logger.addHandler(logging.NullHandler())

# =============================================================================
# Constants
# =============================================================================

PROVIDER_NAME = "yahoo_fundamentals"
PROVIDER_VERSION = "6.7.0"
VERSION = PROVIDER_VERSION
PROVIDER_BATCH_SUPPORTED = True

_TRUTHY = {"1", "true", "yes", "y", "on", "t", "enabled", "enable"}
_FALSY = {"0", "false", "no", "n", "off", "f", "disabled", "disable"}
_KSA_CODE_RE = re.compile(r"^\d{3,6}$", re.IGNORECASE)
_ARABIC_DIGITS = str.maketrans("\u0660\u0661\u0662\u0663\u0664\u0665\u0666\u0667\u0668\u0669", "0123456789")
_K_M_B_T_RE = re.compile(r"^(-?\d+(?:\.\d+)?)([KMBT])$", re.IGNORECASE)
_K_M_B_T_MULT = {"K": 1e3, "M": 1e6, "B": 1e9, "T": 1e12}

DEFAULT_USER_AGENT_ROTATION = True
DEFAULT_PRICE_SANITY_GUARD = True
DEFAULT_RETRY_ATTEMPTS = 4
DEFAULT_PRICE_RATIO_HIGH = 8.0
DEFAULT_PRICE_RATIO_LOW = 0.125

_USER_AGENTS: Tuple[str, ...] = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 "
    "(KHTML, like Gecko) Version/17.2 Safari/605.1.15",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 "
    "Firefox/121.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36 Edg/120.0.2210.144",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_2_1) AppleWebKit/605.1.15 "
    "(KHTML, like Gecko) Version/17.2.1 Safari/605.1.15",
    "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:121.0) Gecko/20100101 "
    "Firefox/121.0",
)

_RATE_LIMIT_MARKERS: Tuple[str, ...] = (
    "403", "429", "forbidden", "rate limit", "too many requests",
    "unauthorized", "401",
)

# =============================================================================
# Optional Dependencies (prod-safe)
# =============================================================================

try:
    from redis.asyncio import Redis  # type: ignore
    _REDIS_AVAILABLE = True
except ImportError:
    Redis = None  # type: ignore[assignment]
    _REDIS_AVAILABLE = False

try:
    from prometheus_client import Counter, Gauge, Histogram  # type: ignore
    _PROMETHEUS_AVAILABLE = True
except ImportError:
    Counter = Gauge = Histogram = None  # type: ignore[assignment]
    _PROMETHEUS_AVAILABLE = False

try:
    import yfinance as yf  # type: ignore
    _HAS_YFINANCE = True
except ImportError:
    yf = None  # type: ignore[assignment]
    _HAS_YFINANCE = False

try:
    import requests  # type: ignore
    _HAS_REQUESTS = True
except ImportError:
    requests = None  # type: ignore[assignment]
    _HAS_REQUESTS = False


# =============================================================================
# v6.2.0: Optional Identity SSOT (normalize.py v5.3.0+)
# =============================================================================

_infer_symbol_metadata_external: Optional[Callable[[str], Dict[str, Any]]] = None
_to_yahoo_symbol_external: Optional[Callable[[str], str]] = None
for _norm_path in (
    "core.symbols.normalize",
    "core.normalize",
    "symbols.normalize",
    "normalize",
):
    try:
        _norm_mod_v62 = __import__(_norm_path, fromlist=["infer_symbol_metadata"])
        _fn_v62 = getattr(_norm_mod_v62, "infer_symbol_metadata", None)
        if callable(_fn_v62):
            _infer_symbol_metadata_external = _fn_v62
            # v6.3.1: bind to_yahoo_symbol from the SAME module so the
            # yfinance call uses Yahoo-format suffixes (.NS/.DE/...).
            _fn_y = getattr(_norm_mod_v62, "to_yahoo_symbol", None)
            if callable(_fn_y):
                _to_yahoo_symbol_external = _fn_y
            break
    except ImportError:
        continue
    except Exception:
        continue


# v6.3.1: local EODHD->Yahoo suffix remap, used ONLY as a fallback for the
# actual yfinance symbol when the normalize SSOT (to_yahoo_symbol) can't be
# imported. Keeps the provider self-sufficient: an import failure must not
# silently send Yahoo a suffix it can't resolve. Only the genuinely-divergent
# suffixes are listed; everything else passes through unchanged.
_EODHD_TO_YAHOO_LOCAL: Dict[str, str] = {
    "NSE": "NS",     # India NSE   -> Yahoo .NS
    "BSE": "BO",     # India BSE   -> Yahoo .BO
    "XETRA": "DE",   # Germany     -> Yahoo .DE
    "AU": "AX",      # Australia   -> Yahoo .AX
    "KO": "KS",      # Korea KOSPI -> Yahoo .KS
}


def _local_to_yahoo_symbol(norm_symbol: str) -> str:
    """Fallback EODHD->Yahoo remap (used only when the SSOT import failed)."""
    s = (norm_symbol or "").strip()
    if not s or "." not in s:
        return s
    base, _, suf = s.rpartition(".")
    if not base:
        return s
    remapped = _EODHD_TO_YAHOO_LOCAL.get(suf.upper())
    return f"{base}.{remapped}" if remapped else s


def _to_yahoo_provider_symbol(norm_symbol: str) -> str:
    """
    v6.3.1: Yahoo-format symbol for the ACTUAL yf.Ticker call.

    Prefers the normalize SSOT (to_yahoo_symbol); falls back to the local
    suffix remap above. Never returns empty -- if everything fails, the
    original normalized symbol is returned unchanged (status-quo behaviour).
    """
    s = (norm_symbol or "").strip()
    if not s:
        return s
    if _to_yahoo_symbol_external is not None:
        try:
            y = _to_yahoo_symbol_external(s)
            if y:
                return y
        except Exception:
            pass
    return _local_to_yahoo_symbol(s)


# =============================================================================
# Provider-identity guard (v6.4.0)
# =============================================================================
# WHY: yfinance's `info` is consumed without verifying it belongs to the symbol
# we asked for. For some Tadawul instruments (e.g. the sukuk 5023.SR) Yahoo
# returns a DIFFERENT company's record entirely -- in the live 2026-06-28 audit,
# 5023.SR came back carrying Apple Inc.'s identity and fundamentals (name,
# sector, $4.17T market cap, P/E, EPS) while the correct par price arrived
# separately from yahoo_chart. The fundamentals block then wrote Apple's
# identity onto the sukuk row. This guard compares the returned ticker against
# the one queried and, on a CLEAR base-ticker mismatch, discards the wrong-
# instrument `info` so the row falls back to its correct no-fundamentals state
# (price-only) instead of impersonating another company.
# SAFETY: conservative + gated. Only fires when BOTH the requested and returned
# base tickers are present AND differ; a missing/own-symbol response is left
# untouched (status-quo). Gated OFF by default (TFB_FUND_IDENTITY_GUARD) so it
# cannot blank a valid holding's fundamentals until explicitly enabled and
# verified on a live run; set the env to 1/true/on/yes to enable.
# v6.6.0 ADDENDUM: after 9 days of verified-safe semantics and a live audit
# showing daily wrong-name writes with the guard dormant, the default is now ON;
# the env is retained as the kill switch (=0). See the v6.6.0 WHY at top of file.

def _fund_identity_guard_enabled() -> bool:
    """Provider-identity guard master switch. v6.6.0: DEFAULT ON -- the
    2026-07-07 audit found fresh wrong-company names written daily while
    this guard sat dormant. Set TFB_FUND_IDENTITY_GUARD=0/false/off/no to
    disable (restores the pre-v6.6.0 unguarded behavior)."""
    return (os.getenv("TFB_FUND_IDENTITY_GUARD") or "1").strip().lower() in {"1", "true", "on", "yes"}


def _fund_price_coherence_enabled() -> bool:
    """v6.6.0 YF-2 / v6.7.0 YF-2b: own-history price-coherence guard.
    DEFAULT ON since v6.7.0 (the 2026-07-12/13 GM crossings ran with it
    dark); set TFB_FUND_PRICE_COHERENCE_GUARD=0/false/off/no to disable.
    v6.7.0 YF-3: conviction uses the TIGHT cross-response band
    (_fund_price_band_high/low, default 2.0/0.5) -- two same-day responses
    for the same instrument cannot legitimately disagree 2x."""
    return (os.getenv("TFB_FUND_PRICE_COHERENCE_GUARD") or "1").strip().lower() in {"1", "true", "on", "yes"}


def _fund_price_band_high() -> float:
    """v6.7.0 YF-3: upper conviction ratio for YF-2's info-vs-own-history
    check (default 2.0, floored at 1.25 so a mis-set env can never convict
    an ordinary daily move). Distinct from YF_PRICE_RATIO_HIGH (8.0), which
    keeps guarding the unit-class checks (52W bounds / sanity)."""
    try:
        v = float(os.getenv("TFB_FUND_PRICE_BAND_HIGH", "") or 2.0)
    except Exception:
        v = 2.0
    return v if v >= 1.25 else 1.25


def _fund_price_band_low() -> float:
    """v6.7.0 YF-3: lower conviction ratio (default 0.5, ceiling 0.8)."""
    try:
        v = float(os.getenv("TFB_FUND_PRICE_BAND_LOW", "") or 0.5)
    except Exception:
        v = 0.5
    return v if 0.0 < v <= 0.8 else 0.5


def _ticker_identity_base(s: Any) -> str:
    """Comparable base ticker: uppercase, drop the exchange suffix after the
    LAST dot, strip non-alphanumerics. '5023.SR'->'5023', 'AAPL'->'AAPL',
    'RCI.US'->'RCI', 'BRK-B'->'BRKB'. Empty string when nothing usable."""
    t = str(s if s is not None else "").strip().upper()
    if not t:
        return ""
    if "." in t:
        t = t.rsplit(".", 1)[0]
    return re.sub(r"[^A-Z0-9]", "", t)


def _identity_mismatch(requested: str, returned: Any) -> bool:
    """True iff the guard is enabled AND both base tickers are present AND they
    clearly differ. Conservative: any absent side returns False (no discard)."""
    if not _fund_identity_guard_enabled():
        return False
    req = _ticker_identity_base(requested)
    ret = _ticker_identity_base(returned)
    if not req or not ret:
        return False
    return req != ret


# =============================================================================
# v6.5.0 locale cross-check
# =============================================================================
# WHY (live 2026-06-29 audit): the v6.4.0 symbol-base guard only catches records
# where Yahoo stamps a DIFFERENT ticker on the wrong-instrument `info`. With
# TFB_FUND_IDENTITY_GUARD=1 already enabled, yf.Ticker("5023.SR").info returned
# Apple Inc.'s full record (name, Technology sector, ~$4.17T market cap,
# P/E/EPS/revenue) but carried info["symbol"] == "5023.SR" -- the REQUESTED
# ticker -- so _identity_mismatch saw base "5023" == "5023" and passed the wrong
# record straight through (that row showed NO provider_identity_mismatch warning,
# confirming the symbol-base check never fired). A symbol-vs-symbol compare can
# not detect a right-symbol / wrong-everything-else record.
#
# The robust, instrument-agnostic signal is LOCALE: an exchange-suffixed symbol
# (".SR" -> Tadawul/SAR/Saudi Arabia) must not come back denominated in another
# country's currency. Apple's record is USD/United States; the sukuk is
# SAR/Saudi Arabia. The check discards `info` only when the suffix implies a
# known locale AND the returned record clearly contradicts it.
#
# SAFETY (cannot blank a valid holding):
#   * Only symbols whose suffix is in _SUFFIX_TO_LOCALE_DEFAULTS are checked.
#     Plain US tickers, ".US", "=F"/"=X"/"^" carry no suffix-locale and are never
#     locale-checked (they still get the v6.4.0 symbol-base check).
#   * Currency is the primary signal and is decisive when present: GBp/GBX pence
#     notation is normalized to GBP, so a London ".L" stock returning GBP rather
#     than GBp is NOT a mismatch. All EUR suffixes share EUR.
#   * Country is used ONLY as a fallback when the returned record has no
#     currency, and US synonyms (USA/US/United States) are canonicalized, so a
#     US record never trips against a US expectation.
#   * Same master switch as v6.4.0 (TFB_FUND_IDENTITY_GUARD); OFF => None, no-op.
# For the current book this fires on 5023.SR (USD/US vs SAR/Saudi) ONLY; every
# real ".SR" holding returns SAR and is left untouched.

def _norm_ccy_token(x: Any) -> str:
    """Upper-case ISO currency token; collapse UK pence (GBp/GBX) to GBP so
    pence and pounds compare equal. '' when nothing usable."""
    t = re.sub(r"[^A-Z]", "", str(x if x is not None else "").strip().upper())
    if not t:
        return ""
    if t == "GBX" or t.startswith("GBP"):
        return "GBP"
    return t


_US_COUNTRY_TOKENS = {"US", "USA", "UNITEDSTATES", "UNITEDSTATESOFAMERICA"}


def _norm_country_token(x: Any) -> str:
    """Upper-case alphanumeric country token; canonicalize US synonyms. '' when
    nothing usable."""
    t = re.sub(r"[^A-Z0-9]", "", str(x if x is not None else "").strip().upper())
    if not t:
        return ""
    if t in _US_COUNTRY_TOKENS:
        return "US"
    return t


def _expected_locale_for_symbol(requested: str) -> Tuple[str, str]:
    """(expected_currency, expected_country) implied by the symbol's exchange
    suffix via _SUFFIX_TO_LOCALE_DEFAULTS, or ('', '') when the suffix carries no
    known locale (US/plain, '=F', '=X', '^', or an unrecognized suffix). Longest
    matching suffix wins."""
    s = str(requested if requested is not None else "").strip().upper()
    if not s or s.endswith("=F") or s.endswith("=X") or s.startswith("^"):
        return "", ""
    best_suffix: Optional[str] = None
    for suf in _SUFFIX_TO_LOCALE_DEFAULTS:
        if s.endswith(suf):
            if best_suffix is None or len(suf) > len(best_suffix):
                best_suffix = suf
    if best_suffix is None:
        return "", ""
    _exch, curr, country = _SUFFIX_TO_LOCALE_DEFAULTS[best_suffix]
    return _norm_ccy_token(curr), _norm_country_token(country)


def _identity_locale_mismatch(requested: str, info: Dict[str, Any]) -> Optional[str]:
    """Human-readable mismatch detail iff the guard is enabled, the requested
    symbol's suffix implies a known locale, AND the returned record clearly
    contradicts it -- decided on currency when the record has one, else on
    country. None otherwise (any absent/ambiguous field => no mismatch)."""
    if not _fund_identity_guard_enabled():
        return None
    if not isinstance(info, dict) or not info:
        return None
    exp_ccy, exp_country = _expected_locale_for_symbol(requested)
    if not exp_ccy and not exp_country:
        return None

    ret_ccy = _norm_ccy_token(_pick(info, "currency", "financialCurrency"))
    ret_country = _norm_country_token(_pick(info, "country"))

    mismatch = False
    if ret_ccy:
        # currency present -> decisive (avoids country-spelling false positives)
        mismatch = bool(exp_ccy and ret_ccy != exp_ccy)
    elif exp_country and ret_country:
        # no currency on the record -> fall back to country
        mismatch = ret_country != exp_country

    if not mismatch:
        return None
    ret_ccy_disp = safe_str(_pick(info, "currency", "financialCurrency")) or "?"
    ret_ctry_disp = safe_str(_pick(info, "country")) or "?"
    return (
        f"{str(requested).strip().upper()}:returned[{ret_ccy_disp}/{ret_ctry_disp}]"
        f"!=expected[{exp_ccy or '?'}]"
    )


# =============================================================================
# Prometheus Metrics (optional)
# =============================================================================

if _PROMETHEUS_AVAILABLE and Counter and Gauge and Histogram:
    yf_fund_requests_total = Counter(
        "yf_fund_requests_total",
        "Total Yahoo fundamentals provider requests",
        ["status"],
    )
    yf_fund_request_duration = Histogram(
        "yf_fund_request_duration_seconds",
        "Yahoo fundamentals provider request duration (seconds)",
        buckets=[0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 20.0, 30.0],
    )
    yf_fund_circuit_breaker_state = Gauge(
        "yf_fund_circuit_breaker_state",
        "Circuit breaker state (0=closed, 1=half_open, 2=open)",
    )
else:
    class _DummyMetric:
        def labels(self, *args: Any, **kwargs: Any) -> "_DummyMetric":
            return self

        def inc(self, *args: Any, **kwargs: Any) -> None:
            return None

        def observe(self, *args: Any, **kwargs: Any) -> None:
            return None

        def set(self, *args: Any, **kwargs: Any) -> None:
            return None

    yf_fund_requests_total = _DummyMetric()
    yf_fund_request_duration = _DummyMetric()
    yf_fund_circuit_breaker_state = _DummyMetric()


# =============================================================================
# Environment Helpers
# =============================================================================

def _env_str(name: str, default: str = "") -> str:
    v = os.getenv(name)
    v = str(v).strip() if v is not None else ""
    return v if v else default


def _env_int(name: str, default: int) -> int:
    try:
        v = os.getenv(name)
        return int(float(str(v).strip())) if v is not None else default
    except Exception:
        return default


def _env_float(name: str, default: float) -> float:
    try:
        v = os.getenv(name)
        return float(str(v).strip()) if v is not None else default
    except Exception:
        return default


def _env_bool(name: str, default: bool) -> bool:
    raw = (os.getenv(name) or "").strip().lower()
    if not raw:
        return default
    if raw in _FALSY:
        return False
    if raw in _TRUTHY:
        return True
    return default


def _configured() -> bool:
    return _env_bool("YF_ENABLED", True) and _HAS_YFINANCE


def _emit_warnings() -> bool:
    return _env_bool("YF_VERBOSE_WARNINGS", False)


def _timeout_sec() -> float:
    return max(5.0, _env_float("YF_TIMEOUT_SEC", 25.0))


def _fund_ttl_sec() -> float:
    return max(300.0, _env_float("YF_FUND_TTL_SEC", 21600.0))


def _err_ttl_sec() -> float:
    return max(5.0, _env_float("YF_ERROR_TTL_SEC", 20.0))


def _max_concurrency() -> int:
    return max(1, _env_int("YF_MAX_CONCURRENCY", 6))


def _rate_limit() -> float:
    return max(0.0, _env_float("YF_RATE_LIMIT_PER_SEC", 4.0))


def _cb_enabled() -> bool:
    return _env_bool("YF_CIRCUIT_BREAKER", True)


def _cb_fail_threshold() -> int:
    return max(2, _env_int("YF_CB_FAIL_THRESHOLD", 6))


def _cb_cooldown_sec() -> float:
    return max(5.0, _env_float("YF_CB_COOLDOWN_SEC", 30.0))


def _enable_redis() -> bool:
    return _env_bool("YF_ENABLE_REDIS", False) and _REDIS_AVAILABLE


def _redis_url() -> str:
    return _env_str("REDIS_URL", "redis://localhost:6379/0")


def _user_agent_rotation() -> bool:
    return _env_bool("YF_USER_AGENT_ROTATION", DEFAULT_USER_AGENT_ROTATION) and _HAS_REQUESTS


def _price_sanity_guard() -> bool:
    return _env_bool("YF_PRICE_SANITY_GUARD", DEFAULT_PRICE_SANITY_GUARD)


def _retry_attempts() -> int:
    return max(1, _env_int("YF_RETRY_ATTEMPTS", DEFAULT_RETRY_ATTEMPTS))


def _price_ratio_high() -> float:
    v = _env_float("YF_PRICE_RATIO_HIGH", DEFAULT_PRICE_RATIO_HIGH)
    return v if v > 1.0 else DEFAULT_PRICE_RATIO_HIGH


def _price_ratio_low() -> float:
    v = _env_float("YF_PRICE_RATIO_LOW", DEFAULT_PRICE_RATIO_LOW)
    return v if 0.0 < v < 1.0 else DEFAULT_PRICE_RATIO_LOW


# =============================================================================
# Pure Utility Functions
# =============================================================================

def _utc_iso(dt: Optional[datetime] = None) -> str:
    d = dt or datetime.now(timezone.utc)
    if d.tzinfo is None:
        d = d.replace(tzinfo=timezone.utc)
    return d.astimezone(timezone.utc).isoformat()


def _riyadh_iso(dt: Optional[datetime] = None) -> str:
    tz = timezone(timedelta(hours=3))
    d = dt or datetime.now(tz)
    if d.tzinfo is None:
        d = d.replace(tzinfo=tz)
    return d.astimezone(tz).isoformat()


def safe_str(x: Any) -> Optional[str]:
    if x is None:
        return None
    s = str(x).strip()
    return s if s else None


def safe_float(x: Any) -> Optional[float]:
    if x is None:
        return None
    try:
        if isinstance(x, bool):
            return None
        if isinstance(x, (int, float)):
            f = float(x)
            return None if (math.isnan(f) or math.isinf(f)) else f

        s = str(x).strip()
        if not s:
            return None
        if s.lower() in {"n/a", "na", "null", "none", "-", "--"}:
            return None

        s = s.translate(_ARABIC_DIGITS)
        s = s.replace(",", "").strip()

        if s.endswith("%"):
            s = s[:-1].strip()

        if s.startswith("(") and s.endswith(")"):
            s = "-" + s[1:-1].strip()

        m = _K_M_B_T_RE.match(s)
        if m:
            f = float(m.group(1)) * _K_M_B_T_MULT[m.group(2).upper()]
        else:
            f = float(s)

        return None if (math.isnan(f) or math.isinf(f)) else f
    except Exception:
        return None


def safe_int(x: Any) -> Optional[int]:
    f = safe_float(x)
    return int(round(f)) if f is not None else None


def _as_fraction(x: Any) -> Optional[float]:
    v = safe_float(x)
    if v is None:
        return None
    return v / 100.0 if abs(v) > 1.5 else v


def _pct_from_ratio(numerator: Any, denominator: Any) -> Optional[float]:
    a = safe_float(numerator)
    b = safe_float(denominator)
    if a is None or b is None or b == 0:
        return None
    return a / b


def clean_patch(p: Dict[str, Any]) -> Dict[str, Any]:
    out: Dict[str, Any] = {}
    for k, v in (p or {}).items():
        if v is None:
            continue
        if isinstance(v, str) and not v.strip():
            continue
        if k == "warnings" and isinstance(v, list):
            if v:
                out[k] = v
            continue
        out[k] = v
    return out


def normalize_symbol(symbol: str) -> str:
    s = (symbol or "").strip()
    if not s:
        return ""
    s = s.translate(_ARABIC_DIGITS).strip().upper()

    for prefix in ("TADAWUL:", "SAUDI:", "KSA:", "ETF:", "INDEX:"):
        if s.startswith(prefix):
            s = s.split(":", 1)[1].strip()

    for suffix in (".TADAWUL", ".SAUDI", ".KSA"):
        if s.endswith(suffix):
            s = s[: -len(suffix)].strip()

    if _KSA_CODE_RE.match(s):
        return f"{s}.SR"

    if s.endswith(".SR") and _KSA_CODE_RE.match(s[:-3]):
        return s

    return s


def _is_ksa_symbol(norm_symbol: str) -> bool:
    u = (norm_symbol or "").strip().upper()
    if u.endswith(".SR"):
        code = u[:-3]
        return bool(_KSA_CODE_RE.match(code))
    return False


# =============================================================================
# v6.3.0: Recommendation Vocabulary -- canonical 8-tier output + provider_rating
# =============================================================================

# (8tier, provider_rating)  -- single source of truth for v6.3.0
_RECO_VOCAB: Tuple[Tuple[str, str, str], ...] = (
    # --- Strong tokens (8-tier extremes) ---
    ("strong_buy",        "STRONG_BUY",   "strong_buy"),
    ("strongbuy",         "STRONG_BUY",   "strong_buy"),
    ("strong_sell",       "STRONG_SELL",  "strong_sell"),
    ("strongsell",        "STRONG_SELL",  "strong_sell"),
    ("conviction_sell",   "STRONG_SELL",  "strong_sell"),
    ("deep_sell",         "STRONG_SELL",  "strong_sell"),

    # --- ACCUMULATE tier (v8.0.0 vocabulary) ---
    ("accumulate",        "ACCUMULATE",   "accumulate"),
    ("scale_in",          "ACCUMULATE",   "accumulate"),
    ("moderate_buy",      "ACCUMULATE",   "accumulate"),
    ("mod_buy",           "ACCUMULATE",   "accumulate"),

    # --- AVOID tier (v8.0.0 vocabulary) ---
    ("avoid",             "AVOID",        "avoid"),
    ("hard_pass",         "AVOID",        "avoid"),
    ("dnb",               "AVOID",        "avoid"),
    ("do_not_buy",        "AVOID",        "avoid"),
    ("uninvestable",      "AVOID",        "avoid"),

    # --- BUY tier (broker-vocab equivalents) ---
    ("outperform",        "BUY",          "outperform"),
    ("overweight",        "BUY",          "overweight"),
    ("market_outperform", "BUY",          "outperform"),
    ("add",               "BUY",          "add"),
    ("buy",               "BUY",          "buy"),

    # --- HOLD tier ---
    ("market_perform",    "HOLD",         "market_perform"),
    ("neutral",           "HOLD",         "neutral"),
    ("hold",              "HOLD",         "hold"),
    ("equal_weight",      "HOLD",         "neutral"),
    ("perform",           "HOLD",         "hold"),       # fallback for "perform" not caught above

    # --- REDUCE tier ---
    ("underperform",      "REDUCE",       "underperform"),
    ("underweight",       "REDUCE",       "underweight"),
    ("market_underperform","REDUCE",      "underperform"),
    ("trim",              "REDUCE",       "trim"),
    ("reduce",            "REDUCE",       "reduce"),

    # --- SELL tier (after strong_sell, so substring matches don't pre-empt) ---
    ("exit",              "SELL",         "exit"),
    ("sell",              "SELL",         "sell"),
)

# v6.3.1: length-descending view of the vocab used ONLY by the substring
# pass, so longer/more-specific tokens are matched before shorter ones that
# are substrings of them ("underperform" before "perform", "market_outperform"
# before "outperform"/"perform"). The exact-match pass still iterates
# _RECO_VOCAB in declaration order.
_RECO_VOCAB_BY_LEN: Tuple[Tuple[str, str, str], ...] = tuple(
    sorted(_RECO_VOCAB, key=lambda t: len(t[0]), reverse=True)
)


def _canonicalize_raw_recommendation(rec: Optional[str]) -> str:
    if not rec:
        return ""
    s = str(rec).strip()
    if not s:
        return ""
    s = re.sub(r"([a-z])([A-Z])", r"\1_\2", s)
    s = s.lower().strip()
    for sep in (" ", "-", "/", ".", ","):
        s = s.replace(sep, "_")
    s = re.sub(r"_+", "_", s).strip("_")
    return s


def map_recommendation(rec: Optional[str]) -> str:
    if not rec:
        return "HOLD"
    canon = _canonicalize_raw_recommendation(rec)
    if not canon:
        return "HOLD"

    # First pass: exact match
    for key, out8, _provider in _RECO_VOCAB:
        if canon == key:
            return out8

    # Second pass: substring match (for compound tokens like
    # "strong_buy_with_conviction"). v6.3.1: iterate length-descending so
    # "underperform" beats "perform", "market_outperform" beats "outperform".
    for key, out8, _provider in _RECO_VOCAB_BY_LEN:
        if key in canon:
            return out8

    return "HOLD"


def extract_provider_rating(rec: Optional[str]) -> str:
    if not rec:
        return ""
    canon = _canonicalize_raw_recommendation(rec)
    if not canon:
        return ""

    for key, _out8, provider_tok in _RECO_VOCAB:
        if canon == key:
            return provider_tok

    for key, _out8, provider_tok in _RECO_VOCAB_BY_LEN:
        if key in canon:
            return provider_tok

    return canon


def _get_attr(obj: Any, *names: str) -> Any:
    if obj is None:
        return None
    for name in names:
        try:
            if isinstance(obj, dict) and name in obj:
                return obj.get(name)
            if hasattr(obj, name):
                return getattr(obj, name)
        except Exception:
            continue
    return None


def _pick(info: Dict[str, Any], *names: str) -> Any:
    for name in names:
        if name in info:
            return info.get(name)
    return None


def _coalesce(*vals: Any) -> Any:
    for v in vals:
        if v is None:
            continue
        if isinstance(v, str) and not v.strip():
            continue
        return v
    return None


# =============================================================================
# v6.2.0: Identity Defaults Table (suffix -> exchange/currency/country)
# =============================================================================

_SUFFIX_TO_LOCALE_DEFAULTS: Dict[str, Tuple[str, str, str]] = {
    # Hong Kong
    ".HK":    ("HKEX",                    "HKD", "Hong Kong"),
    # United Kingdom
    ".L":     ("LSE",                     "GBp", "United Kingdom"),
    ".LON":   ("LSE",                     "GBp", "United Kingdom"),
    ".LSE":   ("LSE",                     "GBp", "United Kingdom"),
    # Denmark
    ".CO":    ("Copenhagen",              "DKK", "Denmark"),
    # India
    ".NS":    ("NSE",                     "INR", "India"),
    ".NSE":   ("NSE",                     "INR", "India"),
    ".BO":    ("BSE",                     "INR", "India"),
    # Brazil
    ".SA":    ("B3",                      "BRL", "Brazil"),
    # Saudi Arabia
    ".SR":    ("Tadawul",                 "SAR", "Saudi Arabia"),
    # Canada
    ".TO":    ("TSX",                     "CAD", "Canada"),
    ".V":     ("TSX Venture",             "CAD", "Canada"),
    ".CN":    ("CSE",                     "CAD", "Canada"),
    ".NE":    ("NEO Exchange",            "CAD", "Canada"),
    # Germany
    ".XETRA": ("XETRA",                   "EUR", "Germany"),
    ".DE":    ("XETRA",                   "EUR", "Germany"),
    ".F":     ("Frankfurt",               "EUR", "Germany"),
    ".HM":    ("Hamburg",                 "EUR", "Germany"),
    ".MU":    ("Munich",                  "EUR", "Germany"),
    # France / Benelux / Iberia / Italy
    ".PA":    ("Euronext Paris",          "EUR", "France"),
    ".AS":    ("Euronext Amsterdam",      "EUR", "Netherlands"),
    ".MI":    ("Borsa Italiana",          "EUR", "Italy"),
    ".MC":    ("BME",                     "EUR", "Spain"),
    ".BR":    ("Euronext Brussels",       "EUR", "Belgium"),
    ".LS":    ("Euronext Lisbon",         "EUR", "Portugal"),
    ".HE":    ("Helsinki",                "EUR", "Finland"),
    ".IR":    ("Euronext Dublin",         "EUR", "Ireland"),
    # Scandinavia / Switzerland
    ".ST":    ("Stockholm",               "SEK", "Sweden"),
    ".OL":    ("Oslo",                    "NOK", "Norway"),
    ".SW":    ("SIX",                     "CHF", "Switzerland"),
    # Oceania
    ".AX":    ("ASX",                     "AUD", "Australia"),
    ".AU":    ("ASX",                     "AUD", "Australia"),   # v6.3.1: EODHD-format Australia
    ".NZ":    ("NZX",                     "NZD", "New Zealand"),
    # Japan / Korea / SE Asia
    ".T":     ("TSE",                     "JPY", "Japan"),
    ".TYO":   ("TSE",                     "JPY", "Japan"),
    ".KS":    ("KRX",                     "KRW", "South Korea"),
    ".KO":    ("KRX",                     "KRW", "South Korea"),   # v6.3.1: EODHD-format Korea
    ".KQ":    ("KOSDAQ",                  "KRW", "South Korea"),
    ".SI":    ("SGX",                     "SGD", "Singapore"),
    ".KL":    ("Bursa Malaysia",          "MYR", "Malaysia"),
    ".BK":    ("SET",                     "THB", "Thailand"),
    ".JK":    ("IDX",                     "IDR", "Indonesia"),
    # Greater China / Taiwan
    ".SS":    ("Shanghai",                "CNY", "China"),
    ".SZ":    ("Shenzhen",                "CNY", "China"),
    ".TW":    ("TWSE",                    "TWD", "Taiwan"),
    ".TWO":   ("TPEx",                    "TWD", "Taiwan"),
    # Latin America
    ".MX":    ("BMV",                     "MXN", "Mexico"),
    ".BA":    ("BCBA",                    "ARS", "Argentina"),
    # Africa
    ".JO":    ("JSE",                     "ZAR", "South Africa"),
    ".JSE":   ("JSE",                     "ZAR", "South Africa"),
    # GCC (Kuwait / Qatar / UAE)
    ".KW":    ("Boursa Kuwait",           "KWD", "Kuwait"),
    ".KSE":   ("Boursa Kuwait",           "KWD", "Kuwait"),
    ".QA":    ("QSE",                     "QAR", "Qatar"),
    ".QE":    ("QSE",                     "QAR", "Qatar"),
    ".AE":    ("DFM/ADX",                 "AED", "United Arab Emirates"),
    ".DFM":   ("DFM",                     "AED", "United Arab Emirates"),
    ".ADX":   ("ADX",                     "AED", "United Arab Emirates"),
    # MENA other
    ".EG":    ("EGX",                     "EGP", "Egypt"),
    ".EGX":   ("EGX",                     "EGP", "Egypt"),
    ".TA":    ("TASE",                    "ILS", "Israel"),
    ".TASE":  ("TASE",                    "ILS", "Israel"),
    # Emerging Europe (v4.6.0 expansion)
    ".IS":    ("BIST",                    "TRY", "Turkey"),
    ".WA":    ("GPW",                     "PLN", "Poland"),
    ".VI":    ("Wien",                    "EUR", "Austria"),
    ".PR":    ("PSE",                     "CZK", "Czech Republic"),
    ".BD":    ("BUX",                     "HUF", "Hungary"),
    ".AT":    ("ATHEX",                   "EUR", "Greece"),
    # Latin America (v4.6.0 expansion)
    ".SN":    ("Santiago",                "CLP", "Chile"),
    ".LM":    ("Lima",                    "PEN", "Peru"),
    # US sentinel (some upstream feeds attach .US to ADRs)
    ".US":    ("NASDAQ/NYSE",             "USD", "USA"),
}


def _identity_defaults_for_symbol(norm_symbol: str) -> Dict[str, Optional[str]]:
    if not norm_symbol:
        return {"exchange": None, "currency": None, "country": None, "asset_class": None}

    s = norm_symbol.strip().upper()

    if s.endswith("=F"):
        return {"exchange": "NYMEX", "currency": "USD", "country": "USA", "asset_class": "Future"}
    if s.endswith("=X"):
        return {"exchange": "CCY", "currency": None, "country": None, "asset_class": "Currency"}
    if s.startswith("^"):
        return {"exchange": "INDEX", "currency": None, "country": None, "asset_class": "Index"}

    if _infer_symbol_metadata_external is not None:
        try:
            meta = _infer_symbol_metadata_external(s)
            if isinstance(meta, dict):
                inferred_from = meta.get("inferred_from")
                if inferred_from not in (None, "", "none"):
                    ac_raw = meta.get("asset_class") or "EQUITY"
                    ac_norm = str(ac_raw).strip().upper().replace(" ", "_")
                    ac_map = {
                        "EQUITY": "Equity",
                        "ETF": "ETF",
                        "MUTUALFUND": "Mutual Fund",
                        "MUTUAL_FUND": "Mutual Fund",
                        "INDEX": "Index",
                        "CURRENCY": "Currency",
                        "FX": "Currency",
                        "CRYPTOCURRENCY": "Crypto",
                        "CRYPTO": "Crypto",
                        "FUTURE": "Future",
                        "FUTURES": "Future",
                        "OPTION": "Option",
                    }
                    asset_class = ac_map.get(ac_norm, "Equity")
                    return {
                        "exchange": meta.get("exchange"),
                        "currency": meta.get("currency"),
                        "country": meta.get("country"),
                        "asset_class": asset_class,
                    }
        except Exception:
            pass

    best_suffix: Optional[str] = None
    for suf in _SUFFIX_TO_LOCALE_DEFAULTS:
        if s.endswith(suf):
            if best_suffix is None or len(suf) > len(best_suffix):
                best_suffix = suf
    if best_suffix is not None:
        exch, curr, country = _SUFFIX_TO_LOCALE_DEFAULTS[best_suffix]
        return {
            "exchange": exch,
            "currency": curr,
            "country": country,
            "asset_class": "Equity",
        }

    if "." not in s:
        return {
            "exchange": "NASDAQ/NYSE",
            "currency": "USD",
            "country": "USA",
            "asset_class": "Equity",
        }

    return {"exchange": None, "currency": None, "country": None, "asset_class": None}


def _infer_asset_class(info: Dict[str, Any], norm_symbol: str) -> Optional[str]:
    q = safe_str(_pick(info, "quoteType", "instrumentType", "typeDisp"))
    if q:
        qn = q.strip().upper().replace(" ", "_")
        mapping = {
            "EQUITY": "Equity",
            "ETF": "ETF",
            "MUTUALFUND": "Mutual Fund",
            "MUTUAL_FUND": "Mutual Fund",
            "INDEX": "Index",
            "CURRENCY": "Currency",
            "CRYPTOCURRENCY": "Crypto",
            "FUTURE": "Future",
            "FUTURES": "Future",
            "OPTION": "Option",
        }
        if qn in mapping:
            return mapping[qn]

    defaults = _identity_defaults_for_symbol(norm_symbol)
    return defaults.get("asset_class")


# =============================================================================
# v6.1.0: User-Agent Rotation + Session Factory
# =============================================================================

def _pick_random_ua() -> str:
    return random.choice(_USER_AGENTS)


def _create_yf_session(use_rotation: bool) -> Optional[Any]:
    if not use_rotation or not _HAS_REQUESTS or requests is None:
        return None
    try:
        sess = requests.Session()
        sess.headers.update({
            "User-Agent": _pick_random_ua(),
            "Accept": (
                "text/html,application/xhtml+xml,application/xml;q=0.9,"
                "*/*;q=0.8"
            ),
            "Accept-Language": "en-US,en;q=0.9",
            "Accept-Encoding": "gzip, deflate, br",
            "DNT": "1",
            "Connection": "keep-alive",
            "Upgrade-Insecure-Requests": "1",
            "Sec-Fetch-Dest": "document",
            "Sec-Fetch-Mode": "navigate",
            "Sec-Fetch-Site": "none",
            "Sec-Fetch-User": "?1",
        })
        return sess
    except Exception as exc:
        logger.debug("yf session creation failed: %s", exc)
        return None


def _rotate_session_ua(session: Any) -> None:
    if session is None:
        return
    try:
        session.headers["User-Agent"] = _pick_random_ua()
    except Exception:
        pass


def _is_rate_limit_error(err: Optional[BaseException]) -> bool:
    if err is None:
        return False
    try:
        s = str(err).lower()
    except Exception:
        return False
    return any(m in s for m in _RATE_LIMIT_MARKERS)


def _construct_ticker(symbol: str, session: Any) -> Any:
    if yf is None:
        return None
    if session is None:
        return yf.Ticker(symbol)
    try:
        return yf.Ticker(symbol, session=session)
    except TypeError:
        return yf.Ticker(symbol)
    except Exception as exc:
        logger.debug("yf.Ticker(%s, session=...) failed (%s); retrying bare",
                     symbol, exc)
        try:
            return yf.Ticker(symbol)
        except Exception:
            return None


# =============================================================================
# v6.1.0: 52W Bounds Validation (Currency-Mismatch Guard)
# =============================================================================

def _is_suspect_price_ratio(
    ref: Optional[float],
    candidate: Optional[float],
    *,
    ratio_high: float = DEFAULT_PRICE_RATIO_HIGH,
    ratio_low: float = DEFAULT_PRICE_RATIO_LOW,
) -> bool:
    if ref is None or candidate is None:
        return False
    if ref <= 0 or candidate <= 0:
        return False
    ratio = candidate / ref
    return ratio >= ratio_high or ratio <= ratio_low


def _validate_52w_bounds(
    current_price: Optional[float],
    info_52w_high: Optional[float],
    info_52w_low: Optional[float],
    hist_52w_high: Optional[float],
    hist_52w_low: Optional[float],
    *,
    enabled: bool = True,
    ratio_high: float = DEFAULT_PRICE_RATIO_HIGH,
    ratio_low: float = DEFAULT_PRICE_RATIO_LOW,
) -> Tuple[Optional[float], Optional[float], List[str]]:
    warnings: List[str] = []

    if not enabled:
        hi = info_52w_high if info_52w_high is not None else hist_52w_high
        lo = info_52w_low if info_52w_low is not None else hist_52w_low
        return hi, lo, warnings

    cp = safe_float(current_price)
    info_hi = safe_float(info_52w_high)
    info_lo = safe_float(info_52w_low)
    hist_hi = safe_float(hist_52w_high)
    hist_lo = safe_float(hist_52w_low)

    if cp is None or cp <= 0:
        hi = info_hi if info_hi is not None else hist_hi
        lo = info_lo if info_lo is not None else hist_lo
        return hi, lo, warnings

    # --- High ---
    hi: Optional[float]
    if info_hi is not None and not _is_suspect_price_ratio(cp, info_hi,
                                                           ratio_high=ratio_high,
                                                           ratio_low=ratio_low):
        hi = info_hi
    elif info_hi is not None:
        warnings.append("week_52_high_unit_mismatch_dropped")
        if hist_hi is not None and not _is_suspect_price_ratio(cp, hist_hi,
                                                               ratio_high=ratio_high,
                                                               ratio_low=ratio_low):
            hi = hist_hi
            warnings.append("week_52_high_used_history_fallback")
        else:
            if hist_hi is not None:
                warnings.append("week_52_high_history_unit_mismatch_dropped")
            hi = None
    elif hist_hi is not None and not _is_suspect_price_ratio(cp, hist_hi,
                                                             ratio_high=ratio_high,
                                                             ratio_low=ratio_low):
        hi = hist_hi
    elif hist_hi is not None:
        warnings.append("week_52_high_history_unit_mismatch_dropped")
        hi = None
    else:
        hi = None

    # --- Low ---
    lo: Optional[float]
    if info_lo is not None and not _is_suspect_price_ratio(cp, info_lo,
                                                           ratio_high=ratio_high,
                                                           ratio_low=ratio_low):
        lo = info_lo
    elif info_lo is not None:
        warnings.append("week_52_low_unit_mismatch_dropped")
        if hist_lo is not None and not _is_suspect_price_ratio(cp, hist_lo,
                                                               ratio_high=ratio_high,
                                                               ratio_low=ratio_low):
            lo = hist_lo
            warnings.append("week_52_low_used_history_fallback")
        else:
            if hist_lo is not None:
                warnings.append("week_52_low_history_unit_mismatch_dropped")
            lo = None
    elif hist_lo is not None and not _is_suspect_price_ratio(cp, hist_lo,
                                                             ratio_high=ratio_high,
                                                             ratio_low=ratio_low):
        lo = hist_lo
    elif hist_lo is not None:
        warnings.append("week_52_low_history_unit_mismatch_dropped")
        lo = None
    else:
        lo = None

    if hi is not None and lo is not None and hi < lo:
        warnings.append("week_52_high_low_inverted")
        hi, lo = lo, hi

    if hi is not None and cp > hi * 1.05:
        warnings.append("current_price_outside_52w_range")
    elif lo is not None and cp < lo * 0.95:
        warnings.append("current_price_outside_52w_range")

    return hi, lo, warnings


def _validate_forecast_magnitude(
    forecast_price: Optional[float],
    current_price: Optional[float],
    *,
    enabled: bool = True,
    ratio_high: float = DEFAULT_PRICE_RATIO_HIGH,
    ratio_low: float = DEFAULT_PRICE_RATIO_LOW,
) -> Tuple[Optional[float], List[str]]:
    warnings: List[str] = []
    if not enabled:
        return forecast_price, warnings

    fp = safe_float(forecast_price)
    cp = safe_float(current_price)
    if fp is None or cp is None or cp <= 0:
        return fp, warnings

    if _is_suspect_price_ratio(cp, fp, ratio_high=ratio_high, ratio_low=ratio_low):
        warnings.append("forecast_magnitude_suspect")
        return None, warnings

    return fp, warnings


# =============================================================================
# Data Quality
# =============================================================================

class DataQuality(str, Enum):
    EXCELLENT = "EXCELLENT"
    HIGH = "HIGH"
    MEDIUM = "MEDIUM"
    LOW = "LOW"
    STALE = "STALE"
    ERROR = "ERROR"
    MISSING = "MISSING"


def data_quality_score(patch: Dict[str, Any]) -> Tuple[DataQuality, float]:
    score = 0.0

    if safe_str(patch.get("symbol")):
        score += 8
    if safe_str(patch.get("name")):
        score += 6
    if safe_str(patch.get("currency")):
        score += 3
    if safe_str(patch.get("exchange")):
        score += 3
    if safe_str(patch.get("asset_class")):
        score += 3

    cp = safe_float(patch.get("current_price"))
    if cp is not None and cp > 0:
        score += 10
    else:
        score -= 8

    if safe_float(patch.get("market_cap")) is not None:
        score += 8

    if safe_float(patch.get("pe_ttm")) is not None:
        score += 6
    if safe_float(patch.get("pb_ratio") or patch.get("pb")) is not None:
        score += 6
    if safe_float(patch.get("ps_ratio") or patch.get("ps")) is not None:
        score += 5
    if safe_float(patch.get("peg_ratio") or patch.get("peg")) is not None:
        score += 4
    if safe_float(patch.get("ev_ebitda")) is not None:
        score += 4

    if _as_fraction(patch.get("profit_margin") or patch.get("net_margin")) is not None:
        score += 6
    if _as_fraction(patch.get("gross_margin")) is not None:
        score += 4
    if _as_fraction(patch.get("operating_margin")) is not None:
        score += 4
    if _as_fraction(patch.get("roe")) is not None:
        score += 5

    if _as_fraction(patch.get("revenue_growth_yoy") or patch.get("revenue_growth")) is not None:
        score += 6
    if safe_float(patch.get("revenue_ttm")) is not None:
        score += 5
    if safe_float(patch.get("free_cash_flow_ttm") or patch.get("free_cashflow")) is not None:
        score += 5

    if safe_float(patch.get("target_mean_price")) is not None:
        score += 5
    if safe_int(patch.get("analyst_count")) is not None:
        score += 3
    if safe_str(patch.get("provider_rating")):
        score += 2

    score = max(0.0, min(100.0, score))

    if score >= 85:
        return DataQuality.EXCELLENT, score
    if score >= 70:
        return DataQuality.HIGH, score
    if score >= 55:
        return DataQuality.MEDIUM, score
    if score >= 35:
        return DataQuality.LOW, score
    return DataQuality.MISSING, score


# =============================================================================
# Async Primitives (lazy locks)
# =============================================================================

@dataclass(slots=True)
class CacheStats:
    hits: int = 0
    misses: int = 0
    sets: int = 0
    evictions: int = 0
    size: int = 0

    def to_dict(self) -> Dict[str, int]:
        return {
            "hits": self.hits,
            "misses": self.misses,
            "sets": self.sets,
            "evictions": self.evictions,
            "size": self.size,
        }


class AdvancedCache:
    def __init__(self, name: str, maxsize: int, ttl: float, use_redis: bool, redis_url: str):
        self.name = name
        self.maxsize = max(50, int(maxsize))
        self.ttl = float(ttl)
        self.use_redis = bool(use_redis and _REDIS_AVAILABLE and Redis)
        self.redis_url = redis_url
        self._mem: Dict[str, Tuple[Any, float]] = {}
        self._touch: Dict[str, float] = {}
        self._lock: Optional[asyncio.Lock] = None
        self.stats = CacheStats()
        self._redis: Any = None
        if self.use_redis:
            try:
                self._redis = Redis.from_url(self.redis_url, decode_responses=False)  # type: ignore
            except Exception as exc:
                logger.warning("Redis cache init failed (%s): %s", self.name, exc)
                self._redis = None
                self.use_redis = False

    def _get_lock(self) -> asyncio.Lock:
        if self._lock is None:
            self._lock = asyncio.Lock()
        return self._lock

    def _key(self, prefix: str) -> str:
        h = hashlib.sha256(prefix.encode("utf-8")).hexdigest()[:16]
        return f"yffund:{self.name}:{h}:{prefix}"

    def _evict_lru(self) -> None:
        if not self._touch:
            return
        oldest = min(self._touch.items(), key=lambda kv: kv[1])[0]
        self._mem.pop(oldest, None)
        self._touch.pop(oldest, None)
        self.stats.evictions += 1

    async def get(self, prefix: str) -> Optional[Any]:
        k = self._key(prefix)
        now = time.monotonic()
        async with self._get_lock():
            if k in self._mem:
                v, exp = self._mem[k]
                if now < exp:
                    self._touch[k] = now
                    self.stats.hits += 1
                    return v
                self._mem.pop(k, None)
                self._touch.pop(k, None)

        if self.use_redis and self._redis:
            try:
                blob = await self._redis.get(k)
                if blob is not None:
                    val = pickle.loads(zlib.decompress(blob)) if blob else {}
                    async with self._get_lock():
                        if len(self._mem) >= self.maxsize:
                            self._evict_lru()
                        self._mem[k] = (val, now + self.ttl)
                        self._touch[k] = now
                        self.stats.hits += 1
                        self.stats.size = len(self._mem)
                    return val
            except Exception:
                pass

        self.stats.misses += 1
        return None

    async def set(self, prefix: str, value: Any, ttl: Optional[float] = None) -> None:
        k = self._key(prefix)
        exp = time.monotonic() + float(ttl or self.ttl)
        now = time.monotonic()
        async with self._get_lock():
            if len(self._mem) >= self.maxsize and k not in self._mem:
                self._evict_lru()
            self._mem[k] = (value, exp)
            self._touch[k] = now
            self.stats.sets += 1
            self.stats.size = len(self._mem)

        if self.use_redis and self._redis:
            try:
                blob = zlib.compress(pickle.dumps(value))
                await self._redis.setex(k, int(ttl or self.ttl), blob)
            except Exception:
                pass

    async def close(self) -> None:
        if self.use_redis and self._redis:
            try:
                await self._redis.close()
            except Exception:
                pass
        self._redis = None

    async def size(self) -> int:
        async with self._get_lock():
            return len(self._mem)


class TokenBucket:
    def __init__(self, rate_per_sec: float):
        self.rate = max(0.0, float(rate_per_sec))
        self.capacity = max(1.0, self.rate * 2.0) if self.rate > 0 else 1.0
        self.tokens = self.capacity
        self.last = time.monotonic()
        self._lock: Optional[asyncio.Lock] = None

    def _get_lock(self) -> asyncio.Lock:
        if self._lock is None:
            self._lock = asyncio.Lock()
        return self._lock

    async def wait_and_acquire(self, tokens: float = 1.0) -> None:
        if self.rate <= 0:
            return
        need = float(tokens)
        while True:
            async with self._get_lock():
                now = time.monotonic()
                self.tokens = min(self.capacity, self.tokens + (now - self.last) * self.rate)
                self.last = now
                if self.tokens >= need:
                    self.tokens -= need
                    return
                wait = (need - self.tokens) / self.rate
            await asyncio.sleep(min(1.0, max(0.01, wait)))


class CircuitState(Enum):
    CLOSED = "closed"
    HALF_OPEN = "half_open"
    OPEN = "open"

    def to_numeric(self) -> float:
        return {CircuitState.CLOSED: 0.0, CircuitState.HALF_OPEN: 1.0, CircuitState.OPEN: 2.0}[self]


@dataclass(slots=True)
class CircuitBreakerStats:
    state: CircuitState = CircuitState.CLOSED
    failures: int = 0
    successes: int = 0
    last_failure_ts: float = 0.0
    open_until_ts: float = 0.0
    cooldown_sec: float = 30.0


class AdvancedCircuitBreaker:
    def __init__(self, fail_threshold: int, cooldown_sec: float):
        self.fail_threshold = max(1, int(fail_threshold))
        self.stats = CircuitBreakerStats(cooldown_sec=float(cooldown_sec))
        self._lock: Optional[asyncio.Lock] = None
        self._half_open_probe_used = False

    def _get_lock(self) -> asyncio.Lock:
        if self._lock is None:
            self._lock = asyncio.Lock()
        return self._lock

    async def allow_request(self) -> bool:
        if not _cb_enabled():
            return True
        async with self._get_lock():
            now = time.monotonic()
            if self.stats.state == CircuitState.CLOSED:
                yf_fund_circuit_breaker_state.set(self.stats.state.to_numeric())
                return True
            if self.stats.state == CircuitState.OPEN:
                if now >= self.stats.open_until_ts:
                    self.stats.state = CircuitState.HALF_OPEN
                    self._half_open_probe_used = False
                    yf_fund_circuit_breaker_state.set(self.stats.state.to_numeric())
                    return True
                yf_fund_circuit_breaker_state.set(self.stats.state.to_numeric())
                return False
            if not self._half_open_probe_used:
                self._half_open_probe_used = True
                yf_fund_circuit_breaker_state.set(self.stats.state.to_numeric())
                return True
            yf_fund_circuit_breaker_state.set(self.stats.state.to_numeric())
            return False

    async def on_success(self) -> None:
        if not _cb_enabled():
            return
        async with self._get_lock():
            self.stats.successes += 1
            self.stats.state = CircuitState.CLOSED
            self.stats.failures = 0
            self._half_open_probe_used = False
            yf_fund_circuit_breaker_state.set(self.stats.state.to_numeric())

    async def on_failure(self, status_code: int = 500) -> None:
        if not _cb_enabled():
            return
        async with self._get_lock():
            now = time.monotonic()
            self.stats.failures += 1
            self.stats.last_failure_ts = now
            cooldown = self.stats.cooldown_sec
            if status_code in (401, 403, 429):
                cooldown = min(300.0, cooldown * 1.5)
            if self.stats.state == CircuitState.HALF_OPEN:
                self.stats.state = CircuitState.OPEN
                self.stats.open_until_ts = now + min(300.0, cooldown * 2)
            elif self.stats.failures >= self.fail_threshold:
                self.stats.state = CircuitState.OPEN
                self.stats.open_until_ts = now + cooldown
            yf_fund_circuit_breaker_state.set(self.stats.state.to_numeric())

    def get_stats(self) -> Dict[str, Any]:
        s = self.stats
        return {
            "state": s.state.value,
            "fail_threshold": self.fail_threshold,
            "failures": s.failures,
            "successes": s.successes,
            "last_failure_ts": s.last_failure_ts,
            "open_until_ts": s.open_until_ts,
            "cooldown_sec": s.cooldown_sec,
        }


class SingleFlight:
    def __init__(self) -> None:
        self._lock: Optional[asyncio.Lock] = None
        self._futs: Dict[str, asyncio.Future] = {}

    def _get_lock(self) -> asyncio.Lock:
        if self._lock is None:
            self._lock = asyncio.Lock()
        return self._lock

    async def run(self, key: str, coro_fn: Callable[[], Awaitable[Any]]) -> Any:
        owner = False
        lock = self._get_lock()
        async with lock:
            fut = self._futs.get(key)
            if fut is None:
                fut = asyncio.get_running_loop().create_future()
                self._futs[key] = fut
                owner = True
        if not owner:
            return await fut  # type: ignore[return-value]
        try:
            res = await coro_fn()
            if not fut.done():
                fut.set_result(res)
            return res
        except Exception as exc:
            if not fut.done():
                fut.set_exception(exc)
            raise
        finally:
            async with lock:
                self._futs.pop(key, None)


# =============================================================================
# Yahoo Fundamentals Provider
# =============================================================================

@dataclass(slots=True)
class YahooFundamentalsProvider:
    name: str = PROVIDER_NAME

    timeout_sec: float = field(init=False)
    semaphore: Optional[asyncio.Semaphore] = field(init=False, default=None)
    max_concurrency: int = field(init=False)
    rate_limiter: TokenBucket = field(init=False)
    circuit_breaker: AdvancedCircuitBreaker = field(init=False)
    singleflight: SingleFlight = field(init=False)
    fund_cache: AdvancedCache = field(init=False)
    err_cache: AdvancedCache = field(init=False)
    user_agent_rotation: bool = field(init=False)
    price_sanity_guard: bool = field(init=False)
    retry_attempts: int = field(init=False)
    price_ratio_high: float = field(init=False)
    price_ratio_low: float = field(init=False)

    def __post_init__(self) -> None:
        self.timeout_sec = _timeout_sec()
        self.max_concurrency = _max_concurrency()
        self.semaphore = None
        self.rate_limiter = TokenBucket(_rate_limit())
        self.circuit_breaker = AdvancedCircuitBreaker(
            fail_threshold=_cb_fail_threshold(),
            cooldown_sec=_cb_cooldown_sec(),
        )
        self.singleflight = SingleFlight()
        self.fund_cache = AdvancedCache(
            name="fund",
            maxsize=5000,
            ttl=_fund_ttl_sec(),
            use_redis=_enable_redis(),
            redis_url=_redis_url(),
        )
        self.err_cache = AdvancedCache(
            name="error",
            maxsize=2000,
            ttl=_err_ttl_sec(),
            use_redis=_enable_redis(),
            redis_url=_redis_url(),
        )
        self.user_agent_rotation = _user_agent_rotation()
        self.price_sanity_guard = _price_sanity_guard()
        self.retry_attempts = _retry_attempts()
        self.price_ratio_high = _price_ratio_high()
        self.price_ratio_low = _price_ratio_low()

        logger.info(
            "YahooFundamentalsProvider v%s initialized | yfinance=%s | requests=%s | "
            "UA_rotation=%s | sanity_guard=%s | retries=%s | concurrency=%s | "
            "rate=%s/s | cb=%s/%ss",
            PROVIDER_VERSION,
            _HAS_YFINANCE,
            _HAS_REQUESTS,
            self.user_agent_rotation,
            self.price_sanity_guard,
            self.retry_attempts,
            self.max_concurrency,
            _rate_limit(),
            _cb_fail_threshold(),
            _cb_cooldown_sec(),
        )

    @property
    def enabled(self) -> bool:
        return _configured()

    def _get_semaphore(self) -> asyncio.Semaphore:
        if self.semaphore is None:
            self.semaphore = asyncio.Semaphore(self.max_concurrency)
        return self.semaphore

    # -- History helpers --------------------------------------------------

    def _history_rows(
        self,
        ticker: Any,
        period: str = "3mo",
        interval: str = "1d",
    ) -> List[Dict[str, Any]]:
        rows: List[Dict[str, Any]] = []
        try:
            hist = ticker.history(period=period, interval=interval, auto_adjust=False)
            if hist is None or getattr(hist, "empty", True):
                return []
            for idx, row in hist.iterrows():
                try:
                    dt = idx.to_pydatetime() if hasattr(idx, "to_pydatetime") else idx
                except Exception:
                    dt = idx
                rows.append({
                    "date": _utc_iso(dt) if isinstance(dt, datetime) else safe_str(dt),
                    "open": safe_float(row.get("Open")),
                    "high": safe_float(row.get("High")),
                    "low": safe_float(row.get("Low")),
                    "close": safe_float(row.get("Close")),
                    "volume": safe_float(row.get("Volume")),
                })
        except Exception:
            return []
        return rows

    def _history_avg_volumes(
        self,
        rows: Iterable[Dict[str, Any]],
    ) -> Tuple[Optional[float], Optional[float]]:
        vols = [safe_float(r.get("volume")) for r in rows]
        clean = [float(v) for v in vols if v is not None and v >= 0]
        if not clean:
            return None, None
        avg10 = sum(clean[-10:]) / min(10, len(clean))
        avg30 = sum(clean[-30:]) / min(30, len(clean))
        return (float(avg10), float(avg30))

    def _history_52w(
        self,
        rows: Iterable[Dict[str, Any]],
    ) -> Tuple[Optional[float], Optional[float]]:
        highs = [safe_float(r.get("high")) for r in rows]
        lows = [safe_float(r.get("low")) for r in rows]
        hs = [float(v) for v in highs if v is not None]
        ls = [float(v) for v in lows if v is not None]
        return (max(hs) if hs else None, min(ls) if ls else None)

    # -- Blocking fetch (runs in ThreadPoolExecutor) ---------------------

    def _blocking_fetch(self, norm_symbol: str) -> Dict[str, Any]:
        warnings_list: List[str] = []
        if not _HAS_YFINANCE or yf is None:
            warnings_list.append("yfinance_not_installed")
            return {
                "error": "yfinance_not_installed",
                "data_quality": DataQuality.ERROR.value,
                "warnings": warnings_list,
                "last_error_class": "ImportError",
            }

        session = _create_yf_session(self.user_agent_rotation)
        # v6.3.1: the symbol actually sent to Yahoo must be in Yahoo's suffix
        # format (.NS/.DE/.AX/.KS), not the EODHD/canonical form -- otherwise
        # international fundamentals come back empty. Output fields still report
        # the canonical norm_symbol; provider_symbol records what was queried.
        provider_symbol = _to_yahoo_provider_symbol(norm_symbol)
        max_attempts = max(1, int(self.retry_attempts))
        last_exc: Optional[BaseException] = None
        last_err_class: str = ""

        for attempt in range(max_attempts):
            try:
                t = _construct_ticker(provider_symbol, session)
                if t is None:
                    last_err_class = "RuntimeError"
                    last_exc = RuntimeError("yf_ticker_construct_failed")
                    break

                _ident_discarded = False  # v6.6.0 YF-1b
                info: Dict[str, Any] = {}
                try:
                    info = t.info or {}
                except Exception:
                    info = {}

                # v6.4.0 provider-identity guard: if Yahoo returned a different
                # instrument than requested (e.g. 5023.SR -> Apple Inc.), discard
                # the wrong-company `info` so the row does not impersonate it.
                # Conservative + gated (TFB_FUND_IDENTITY_GUARD); no-ops unless a
                # clear base-ticker mismatch is present.
                _yf4_declared = None  # v6.7.0 YF-4: Yahoo's own declared identity
                if info:
                    _returned_sym = _pick(info, "symbol", "underlyingSymbol", "quoteSymbol")
                    _yf4_declared = safe_str(_returned_sym)
                    if _identity_mismatch(provider_symbol, _returned_sym):
                        warnings_list.append(
                            f"provider_identity_mismatch:{_returned_sym}!={provider_symbol}_discarded"
                        )
                        logger.warning(
                            "Yahoo fundamentals identity mismatch: requested %s, Yahoo returned %s "
                            "-- discarding wrong-instrument info",
                            provider_symbol, _returned_sym,
                        )
                        info = {}
                        _ident_discarded = True  # v6.6.0 YF-1b

                # v6.5.0 locale cross-check: catches the harder variant where
                # Yahoo returns a wrong-instrument record but stamps the REQUESTED
                # symbol on it (so the v6.4.0 symbol-base check above cannot fire),
                # e.g. yf.Ticker("5023.SR").info returning Apple's USD/US record.
                # Decided on the returned record's currency/country vs the locale
                # implied by the symbol suffix; gated by the same env switch.
                if info:
                    _loc_detail = _identity_locale_mismatch(norm_symbol, info)
                    if _loc_detail:
                        warnings_list.append(
                            f"provider_locale_mismatch:{_loc_detail}_discarded"
                        )
                        logger.warning(
                            "Yahoo fundamentals locale mismatch for %s -- %s -- "
                            "discarding wrong-instrument info",
                            norm_symbol, _loc_detail,
                        )
                        info = {}
                        _ident_discarded = True  # v6.6.0 YF-1b

                try:
                    # v6.6.0 YF-1b: a convicted response set must not keep
                    # supplying prices through its sibling fast_info.
                    fast_info = None if _ident_discarded else getattr(t, "fast_info", None)
                except Exception:
                    fast_info = None

                history_rows = self._history_rows(t, period="3mo", interval="1d")
                hist_avg10, hist_avg30 = self._history_avg_volumes(history_rows)
                hist_52w_high, hist_52w_low = self._history_52w(history_rows)

                # v6.6.0 YF-2: own-history price coherence. Live price from
                # the info/fast_info set vs this fetch's OWN last history
                # close (independent HTTP). A unit-class disagreement
                # convicts the whole set -- discard info AND fast_info so
                # every downstream field falls to honest history/None.
                if (
                    _fund_price_coherence_enabled()
                    and not _ident_discarded
                    and history_rows
                    and (info or fast_info is not None)
                ):
                    _yf2_px = _coalesce(
                        safe_float(_get_attr(fast_info, "last_price", "lastPrice", "regularMarketPrice")),
                        safe_float(_pick(info, "currentPrice", "regularMarketPrice")),
                    )
                    _yf2_last = None
                    for _r in reversed(history_rows):
                        _c = safe_float(_r.get("close"))
                        if _c:
                            _yf2_last = _c
                            break
                    if (
                        _yf2_px is not None and _yf2_px > 0
                        and _yf2_last is not None and _yf2_last > 0
                        and _is_suspect_price_ratio(
                            _yf2_px, _yf2_last,
                            ratio_high=_fund_price_band_high(),   # v6.7.0 YF-3
                            ratio_low=_fund_price_band_low())
                    ):
                        warnings_list.append(
                            f"provider_price_incoherent:{_yf2_px}!={_yf2_last}_discarded"
                        )
                        logger.warning(
                            "Yahoo fundamentals price incoherent for %s: info=%s vs own-history=%s "
                            "-- discarding info+fast_info set",
                            norm_symbol, _yf2_px, _yf2_last,
                        )
                        info = {}
                        fast_info = None
                        _ident_discarded = True

                now_utc = _utc_iso()
                now_riy = _riyadh_iso()

                # Price fields
                current_price = _coalesce(
                    safe_float(_get_attr(fast_info, "last_price", "lastPrice", "regularMarketPrice")),
                    safe_float(_pick(info, "currentPrice", "regularMarketPrice", "navPrice")),
                )
                previous_close = _coalesce(
                    safe_float(_get_attr(fast_info, "previous_close", "previousClose", "regularMarketPreviousClose")),
                    safe_float(_pick(info, "previousClose", "regularMarketPreviousClose", "chartPreviousClose")),
                )
                open_price = _coalesce(
                    safe_float(_get_attr(fast_info, "open", "open_price", "regularMarketOpen")),
                    safe_float(_pick(info, "open", "regularMarketOpen")),
                )
                day_high = _coalesce(
                    safe_float(_get_attr(fast_info, "day_high", "dayHigh")),
                    safe_float(_pick(info, "dayHigh", "regularMarketDayHigh")),
                )
                day_low = _coalesce(
                    safe_float(_get_attr(fast_info, "day_low", "dayLow")),
                    safe_float(_pick(info, "dayLow", "regularMarketDayLow")),
                )

                info_52w_high = _coalesce(
                    safe_float(_get_attr(fast_info, "fifty_two_week_high", "fiftyTwoWeekHigh", "week52High")),
                    safe_float(_pick(info, "fiftyTwoWeekHigh", "week52High")),
                )
                info_52w_low = _coalesce(
                    safe_float(_get_attr(fast_info, "fifty_two_week_low", "fiftyTwoWeekLow", "week52Low")),
                    safe_float(_pick(info, "fiftyTwoWeekLow", "week52Low")),
                )
                week_52_high, week_52_low, bounds_warnings = _validate_52w_bounds(
                    current_price=current_price,
                    info_52w_high=info_52w_high,
                    info_52w_low=info_52w_low,
                    hist_52w_high=hist_52w_high,
                    hist_52w_low=hist_52w_low,
                    enabled=self.price_sanity_guard,
                    ratio_high=self.price_ratio_high,
                    ratio_low=self.price_ratio_low,
                )
                warnings_list.extend(bounds_warnings)

                volume = _coalesce(
                    safe_float(_get_attr(fast_info, "last_volume", "lastVolume", "regularMarketVolume")),
                    safe_float(_pick(info, "volume", "regularMarketVolume")),
                )
                market_cap = _coalesce(
                    safe_float(_get_attr(fast_info, "market_cap", "marketCap")),
                    safe_float(_pick(info, "marketCap")),
                )

                # Valuation
                pe_ttm = safe_float(_pick(info, "trailingPE"))
                pe_forward = safe_float(_pick(info, "forwardPE"))
                pb_ratio = safe_float(_pick(info, "priceToBook"))
                ps_ratio = safe_float(_pick(info, "priceToSalesTrailing12Months", "priceToSales"))
                peg_ratio = safe_float(_pick(info, "pegRatio", "trailingPegRatio"))
                ev_ebitda = safe_float(_pick(info, "enterpriseToEbitda"))
                enterprise_value = safe_float(_pick(info, "enterpriseValue"))

                # Margins
                gross_margin = _as_fraction(_pick(info, "grossMargins"))
                operating_margin = _as_fraction(_pick(info, "operatingMargins"))
                profit_margin = _as_fraction(_pick(info, "profitMargins", "netMargins"))
                roe = _as_fraction(_pick(info, "returnOnEquity"))
                roa = _as_fraction(_pick(info, "returnOnAssets"))

                # Growth
                revenue_growth_yoy = _as_fraction(_pick(info, "revenueGrowth"))
                earnings_growth_yoy = _as_fraction(_pick(info, "earningsGrowth"))
                revenue_ttm = safe_float(_pick(info, "totalRevenue", "revenueTTM"))
                free_cash_flow_ttm = safe_float(_pick(info, "freeCashflow", "freeCashFlow"))
                operating_cash_flow = safe_float(_pick(info, "operatingCashflow", "operatingCashFlow"))

                # Dividend + shares
                dividend_yield = _as_fraction(_pick(info, "dividendYield"))
                payout_ratio = _as_fraction(_pick(info, "payoutRatio"))
                eps_ttm = safe_float(_pick(info, "trailingEps"))
                eps_forward = safe_float(_pick(info, "forwardEps"))
                debt_to_equity = safe_float(_pick(info, "debtToEquity"))
                beta_5y = safe_float(_pick(info, "beta"))
                float_shares = safe_float(_pick(info, "floatShares"))
                shares_outstanding = safe_float(_pick(info, "sharesOutstanding"))

                # Average volumes (with history fallback)
                avg_volume_10d = _coalesce(
                    safe_float(_pick(info, "averageVolume10days", "averageDailyVolume10Day")),
                    hist_avg10,
                )
                avg_volume_30d = _coalesce(
                    safe_float(_pick(info, "averageVolume", "averageDailyVolume3Month")),
                    hist_avg30,
                )

                # Analyst
                target_mean_price = safe_float(_pick(info, "targetMeanPrice"))
                target_high_price = safe_float(_pick(info, "targetHighPrice"))
                target_low_price = safe_float(_pick(info, "targetLowPrice"))
                analyst_count = safe_int(_pick(info, "numberOfAnalystOpinions"))

                # v6.3.0: cascade-bridge fields.
                raw_rec_key = _pick(info, "recommendationKey")
                provider_rating = extract_provider_rating(raw_rec_key)
                provider_rating_score = safe_float(_pick(info, "recommendationMean"))
                # v6.3.1: emit a canonical recommendation ONLY when Yahoo
                # actually supplied a rating key. Otherwise leave it None so
                # clean_patch strips it -- a missing Yahoo rating must NOT
                # become a fake "HOLD" that data_engine_v2 could capture as a
                # provider override (reopening the v5.77.17 corruption).
                recommendation = map_recommendation(raw_rec_key) if provider_rating else None
                recommendation_source: Optional[str] = (
                    PROVIDER_NAME if (provider_rating or provider_rating_score is not None) else None
                )
                if not provider_rating and provider_rating_score is None:
                    warnings_list.append("provider_rating_missing")

                # Identity
                name = safe_str(_pick(info, "longName", "shortName", "displayName"))
                currency = safe_str(_pick(info, "currency", "financialCurrency"))
                exchange = safe_str(_pick(info, "fullExchangeName", "exchange", "exchangeName"))
                country = safe_str(_pick(info, "country"))
                sector = safe_str(_pick(info, "sector"))
                industry = safe_str(_pick(info, "industry"))
                asset_class = _infer_asset_class(info, norm_symbol)

                # v6.2.0: identity defaults fill
                _identity_defaults = _identity_defaults_for_symbol(norm_symbol)
                if currency is None and _identity_defaults.get("currency"):
                    currency = _identity_defaults["currency"]
                if country is None and _identity_defaults.get("country"):
                    country = _identity_defaults["country"]
                if exchange is None and _identity_defaults.get("exchange"):
                    exchange = _identity_defaults["exchange"]
                if asset_class is None and _identity_defaults.get("asset_class"):
                    asset_class = _identity_defaults["asset_class"]

                # v6.1.0: field-level missing-data warnings
                if not info:
                    warnings_list.append("info_empty")
                if not history_rows:
                    warnings_list.append("history_empty")
                if not industry:
                    warnings_list.append("industry_missing_from_provider")
                if not sector:
                    warnings_list.append("sector_missing_from_provider")
                if not currency:
                    warnings_list.append("currency_missing_from_provider")

                # Misc metrics
                book_value = safe_float(_pick(info, "bookValue"))
                current_ratio = safe_float(_pick(info, "currentRatio"))
                quick_ratio = safe_float(_pick(info, "quickRatio"))
                short_ratio = safe_float(_pick(info, "shortRatio"))
                short_percent = _as_fraction(_pick(info, "shortPercentOfFloat"))

                if gross_margin is None:
                    gross_margin = _pct_from_ratio(_pick(info, "grossProfits"), revenue_ttm)
                if operating_margin is None:
                    operating_margin = _pct_from_ratio(_pick(info, "ebitda"), revenue_ttm)

                # 52-week position (fraction in [0,1])
                week_52_position_pct: Optional[float] = None
                cp = safe_float(current_price)
                if (cp is not None and week_52_high is not None and week_52_low is not None
                        and week_52_high != week_52_low):
                    week_52_position_pct = (cp - float(week_52_low)) / (float(week_52_high) - float(week_52_low))
                    week_52_position_pct = max(0.0, min(1.0, float(week_52_position_pct)))

                # v6.1.0: forecast magnitude sanity check
                forecast_price_12m: Optional[float] = None
                expected_roi_12m: Optional[float] = None
                forecast_confidence: Optional[float] = None
                forecast_method: Optional[str] = None
                if cp is not None and cp > 0 and target_mean_price is not None and target_mean_price > 0:
                    candidate_fp = float(target_mean_price)
                    validated_fp, fc_warnings = _validate_forecast_magnitude(
                        forecast_price=candidate_fp,
                        current_price=cp,
                        enabled=self.price_sanity_guard,
                        ratio_high=self.price_ratio_high,
                        ratio_low=self.price_ratio_low,
                    )
                    warnings_list.extend(fc_warnings)
                    if validated_fp is not None:
                        forecast_price_12m = validated_fp
                        expected_roi_12m = (validated_fp / cp) - 1.0
                        forecast_method = "analyst_consensus"
                        ac = analyst_count or 1
                        forecast_confidence = min(0.95, 0.40 + (ac * 0.05))

                # Dedupe warnings while preserving order
                seen_w: set = set()
                deduped_warnings: List[str] = []
                for w in warnings_list:
                    if w and w not in seen_w:
                        seen_w.add(w)
                        deduped_warnings.append(w)

                out: Dict[str, Any] = {
                    "requested_symbol": norm_symbol,
                    "symbol": norm_symbol,
                    "provider_symbol": provider_symbol,
                    # v6.7.0 YF-4: the identity Yahoo's response DECLARED,
                    # captured before any conviction reset -- the engine's
                    # AU-1b raw-patch check reads this key, giving it an
                    # independent second look even with local guards off.
                    "code": _yf4_declared,
                    "provider": PROVIDER_NAME,
                    "data_source": PROVIDER_NAME,
                    "data_sources": [PROVIDER_NAME],
                    "provider_version": PROVIDER_VERSION,
                    "last_updated_utc": now_utc,
                    "last_updated_riyadh": now_riy,

                    # identity/profile
                    "currency": currency,
                    "name": name,
                    "exchange": exchange,
                    "country": country,
                    "sector": sector,
                    "industry": industry,
                    "asset_class": asset_class,

                    # price/liquidity (best-effort)
                    "current_price": current_price,
                    "previous_close": previous_close,
                    "open_price": open_price,
                    "day_high": day_high,
                    "day_low": day_low,
                    "week_52_high": week_52_high,
                    "week_52_low": week_52_low,
                    "week_52_position_pct": week_52_position_pct,
                    "volume": volume,
                    "market_cap": market_cap,
                    "float_shares": float_shares,
                    "avg_volume_10d": avg_volume_10d,
                    "avg_volume_30d": avg_volume_30d,
                    "beta_5y": beta_5y,

                    # fundamentals
                    "pe_ttm": pe_ttm,
                    "pe_forward": pe_forward,
                    "eps_ttm": eps_ttm,
                    "dividend_yield": dividend_yield,
                    "payout_ratio": payout_ratio,
                    "revenue_ttm": revenue_ttm,
                    "revenue_growth_yoy": revenue_growth_yoy,
                    "gross_margin": gross_margin,
                    "operating_margin": operating_margin,
                    "profit_margin": profit_margin,
                    "debt_to_equity": debt_to_equity,
                    "free_cash_flow_ttm": free_cash_flow_ttm,

                    # valuation
                    "pb_ratio": pb_ratio,
                    "ps_ratio": ps_ratio,
                    "peg_ratio": peg_ratio,
                    "ev_ebitda": ev_ebitda,
                    "enterprise_value": enterprise_value,
                    "intrinsic_value": target_mean_price,

                    # analyst/reco
                    "target_mean_price": target_mean_price,
                    "target_high_price": target_high_price,
                    "target_low_price": target_low_price,
                    "analyst_count": analyst_count,
                    "recommendation": recommendation,

                    # v6.3.0: cascade-bridge fields
                    "provider_rating": provider_rating or None,
                    "provider_rating_score": provider_rating_score,
                    "recommendation_source": recommendation_source,

                    # additional
                    "shares_outstanding": shares_outstanding,
                    "book_value": book_value,
                    "eps_forward": eps_forward,
                    "roe": roe,
                    "roa": roa,
                    "earnings_growth_yoy": earnings_growth_yoy,
                    "operating_cashflow": operating_cash_flow,
                    "free_cashflow": free_cash_flow_ttm,
                    "current_ratio": current_ratio,
                    "quick_ratio": quick_ratio,
                    "short_ratio": short_ratio,
                    "short_percent": short_percent,
                    "history_rows_3mo": len(history_rows),

                    # forecast (may be None if magnitude check dropped it)
                    "forecast_price_12m": forecast_price_12m,
                    "expected_roi_12m": expected_roi_12m,
                    "forecast_method": forecast_method,
                    "forecast_confidence": forecast_confidence,

                    # v6.1.0: structured warnings
                    "warnings": deduped_warnings,
                }

                # Legacy aliases (preserved verbatim from v6.0.0)
                out["price"] = out.get("current_price")
                out["prev_close"] = out.get("previous_close")
                out["open"] = out.get("open_price")
                out["change"] = None
                out["change_pct"] = None
                if current_price is not None and previous_close is not None:
                    change = float(current_price) - float(previous_close)
                    pct = (change / float(previous_close)) if float(previous_close) != 0 else None
                    out["price_change"] = change
                    out["percent_change"] = pct
                    out["change"] = change
                    out["change_pct"] = pct
                out["52w_high"] = out.get("week_52_high")
                out["52w_low"] = out.get("week_52_low")
                out["forward_pe"] = out.get("pe_forward")
                out["pb"] = out.get("pb_ratio")
                out["ps"] = out.get("ps_ratio")
                out["peg"] = out.get("peg_ratio")
                out["net_margin"] = out.get("profit_margin")
                out["revenue_growth"] = out.get("revenue_growth_yoy")
                out["dividend_yield_percent"] = out.get("dividend_yield")

                dq, score = data_quality_score(out)
                out["data_quality"] = dq.value
                out["data_quality_score"] = score
                return clean_patch(out)

            except Exception as exc:
                last_exc = exc
                last_err_class = type(exc).__name__

                base = min(8.0, 0.5 * (2 ** attempt))
                if _is_rate_limit_error(exc):
                    base = min(16.0, base * 2.0)
                sleep_for = base + random.uniform(0.0, base * 0.25)
                time.sleep(sleep_for)
                _rotate_session_ua(session)

        if last_exc is not None:
            logger.warning("Yahoo fundamentals fetch failed for %s after %d attempts: %s",
                           norm_symbol, max_attempts, last_exc)
            warnings_list.append(f"fetch_failed:{last_err_class}")

        return {
            "error": str(last_exc) if last_exc is not None else "unknown",
            "data_quality": DataQuality.ERROR.value,
            "warnings": warnings_list,
            "last_error_class": last_err_class or "Unknown",
        }

    # -- Async fetch API -----------------------------------------------------

    async def fetch_fundamentals(self, symbol: str) -> Dict[str, Any]:
        if not self.enabled or not symbol:
            return {}

        norm = normalize_symbol(symbol)
        if not norm:
            return {}

        cached = await self.fund_cache.get(norm)
        if cached:
            return cached

        if not await self.circuit_breaker.allow_request():
            return {}

        async def _do() -> Dict[str, Any]:
            await self.rate_limiter.wait_and_acquire()

            async with self._get_semaphore():
                loop = asyncio.get_running_loop()
                start_time = time.monotonic()
                try:
                    res = await loop.run_in_executor(None, self._blocking_fetch, norm)
                    if "error" in res and res.get("data_quality") == DataQuality.ERROR.value:
                        last_cls = (res.get("last_error_class") or "").lower()
                        is_rate_limited = any(m in last_cls for m in ("403", "429"))
                        await self.circuit_breaker.on_failure(
                            status_code=403 if is_rate_limited else 500
                        )
                        yf_fund_requests_total.labels(status="error").inc()
                        return {}
                    await self.circuit_breaker.on_success()
                    yf_fund_requests_total.labels(status="success").inc()
                    yf_fund_request_duration.observe(time.monotonic() - start_time)

                    if res.get("current_price") is not None:
                        await self.fund_cache.set(norm, res)
                    return res
                except Exception as exc:
                    await self.circuit_breaker.on_failure()
                    yf_fund_requests_total.labels(status="error").inc()
                    logger.error("Error fetching fundamentals for %s: %s", norm, exc)
                    return {}

        return await self.singleflight.run(norm, _do)

    async def fetch_fundamentals_batch(
        self,
        symbols: List[str],
        concurrency: Optional[int] = None,
    ) -> Dict[str, Dict[str, Any]]:
        if not symbols:
            return {}

        batch_cap = max(1, concurrency or self.max_concurrency)
        batch_sem = asyncio.Semaphore(batch_cap)

        async def _fetch_one(sym: str) -> Tuple[str, Dict[str, Any]]:
            async with batch_sem:
                result = await self.fetch_fundamentals(sym)
                return sym, result

        tasks = [_fetch_one(sym) for sym in symbols if sym]
        gathered = await asyncio.gather(*tasks, return_exceptions=True)

        results: Dict[str, Dict[str, Any]] = {}
        for item in gathered:
            if isinstance(item, Exception):
                continue
            sym, data = item
            if data:
                results[sym] = data
        return results

    async def close(self) -> None:
        try:
            await self.fund_cache.close()
        except Exception as exc:
            logger.debug("fund_cache close failed: %s", exc)
        try:
            await self.err_cache.close()
        except Exception as exc:
            logger.debug("err_cache close failed: %s", exc)


# =============================================================================
# Singleton Instance (lazy)
# =============================================================================

_PROVIDER_INSTANCE: Optional[YahooFundamentalsProvider] = None
_PROVIDER_LOCK: Optional[asyncio.Lock] = None


def _get_provider_lock() -> asyncio.Lock:
    global _PROVIDER_LOCK
    if _PROVIDER_LOCK is None:
        _PROVIDER_LOCK = asyncio.Lock()
    return _PROVIDER_LOCK


async def get_provider() -> YahooFundamentalsProvider:
    global _PROVIDER_INSTANCE
    if _PROVIDER_INSTANCE is not None:
        return _PROVIDER_INSTANCE
    async with _get_provider_lock():
        if _PROVIDER_INSTANCE is None:
            _PROVIDER_INSTANCE = YahooFundamentalsProvider()
    return _PROVIDER_INSTANCE


async def close_provider() -> None:
    global _PROVIDER_INSTANCE
    if _PROVIDER_INSTANCE is not None:
        await _PROVIDER_INSTANCE.close()
        _PROVIDER_INSTANCE = None


# =============================================================================
# Engine-Facing Functions
# =============================================================================

async def fetch_fundamentals_patch(symbol: str, *args: Any, **kwargs: Any) -> Dict[str, Any]:
    if args or kwargs:
        logger.debug("fetch_fundamentals_patch(%s): ignoring args=%r kwargs=%r", symbol, args, kwargs)
    provider = await get_provider()
    return await provider.fetch_fundamentals(symbol)


async def fetch_enriched_quote_patch(symbol: str, *args: Any, **kwargs: Any) -> Dict[str, Any]:
    return await fetch_fundamentals_patch(symbol, *args, **kwargs)


async def fetch_quote(symbol: str, *args: Any, **kwargs: Any) -> Dict[str, Any]:
    return await fetch_fundamentals_patch(symbol, *args, **kwargs)


async def get_quote(symbol: str, *args: Any, **kwargs: Any) -> Dict[str, Any]:
    return await fetch_fundamentals_patch(symbol, *args, **kwargs)


async def quote(symbol: str, *args: Any, **kwargs: Any) -> Dict[str, Any]:
    return await fetch_fundamentals_patch(symbol, *args, **kwargs)


async def enriched_quote(symbol: str, *args: Any, **kwargs: Any) -> Dict[str, Any]:
    return await fetch_fundamentals_patch(symbol, *args, **kwargs)


async def get_quote_patch(symbol: str, *args: Any, **kwargs: Any) -> Dict[str, Any]:
    return await fetch_fundamentals_patch(symbol, *args, **kwargs)


async def fetch_quotes(
    symbols: List[str],
    concurrency: Optional[int] = None,
    **kwargs: Any,
) -> Dict[str, Dict[str, Any]]:
    if kwargs:
        logger.debug("fetch_quotes: ignoring kwargs=%r", kwargs)
    provider = await get_provider()
    return await provider.fetch_fundamentals_batch(symbols, concurrency=concurrency)


# =============================================================================
# Module Exports
# =============================================================================

__all__ = [
    "PROVIDER_NAME",
    "PROVIDER_VERSION",
    "VERSION",
    "PROVIDER_BATCH_SUPPORTED",
    "YahooFundamentalsProvider",
    "DataQuality",
    "data_quality_score",
    "normalize_symbol",
    "map_recommendation",
    "extract_provider_rating",
    "get_provider",
    "close_provider",
    "fetch_fundamentals_patch",
    "fetch_enriched_quote_patch",
    "fetch_quote",
    "get_quote",
    "get_quote_patch",
    "quote",
    "enriched_quote",
    "fetch_quotes",
]
