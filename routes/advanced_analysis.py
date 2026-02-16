# routes/advanced_analysis.py
"""
TADAWUL FAST BRIDGE — ADVANCED ANALYSIS ROUTES (v4.6.0)
Mission Critical: PROD SAFE + BATCH-RESILIENT + ROI/FORECAST CANON + RIYADH TIME + SCORE ENRICH

Primary Sheets Contract (MUST stay stable)
- POST /v1/advanced/sheet-rows
  -> {
       status: "success" | "partial" | "error",
       headers: [...],
       rows: [...],
       error?: str,
       meta?: {...}
     }

Key Guarantees
- Never returns empty headers (falls back to safe defaults).
- Never breaks batch بسبب سهم واحد (placeholder row instead).
- Always returns rows aligned to headers length (pad/truncate).
- Canonical keys (best-effort):
    expected_roi_1m / expected_roi_3m / expected_roi_12m
    forecast_price_1m / forecast_price_3m / forecast_price_12m
    last_updated_utc / last_updated_riyadh
    forecast_updated_utc / forecast_updated_riyadh

Resilience / Scale
- Concurrency limited (ADVANCED_MAX_CONCURRENCY)
- Chunked batch calls (ADVANCED_BATCH_SIZE) with timeouts
- Per-symbol timeout (ADVANCED_PER_SYMBOL_TIMEOUT_SEC)
- Total request timeout (ADVANCED_TOTAL_TIMEOUT_SEC) – soft (returns partial)

Compatibility
- Push mode supported: body={"items":[{sheet,headers,rows},...]} caches snapshots into engine if supported.
- Compute mode: body={"tickers":[...]} or {"symbols":[...]} returns sheet-ready rows.

Auth
- Supports X-APP-TOKEN, Authorization: Bearer, and optional query token (?token=) if ALLOW_QUERY_TOKEN=1
- If no APP_TOKEN/BACKUP_APP_TOKEN/TFB_APP_TOKEN set => open-mode (no auth enforcement)

NOTE
- Heavy engine/scoring/enriched imports are lazy + cached.
"""

from __future__ import annotations

import asyncio
import inspect
import logging
import os
import re
import time
from dataclasses import is_dataclass
from datetime import datetime, timezone
from functools import lru_cache
from typing import Any, Dict, List, Optional, Sequence, Tuple, Union

from fastapi import APIRouter, Body, Header, Query, Request

# pydantic v2 preferred, v1 fallback
try:
    from pydantic import BaseModel, ConfigDict, Field  # type: ignore

    _PYDANTIC_V2 = True
except Exception:  # pragma: no cover
    from pydantic import BaseModel, Field  # type: ignore

    ConfigDict = None  # type: ignore
    _PYDANTIC_V2 = False

logger = logging.getLogger("routes.advanced_analysis")

ADVANCED_ANALYSIS_VERSION = "4.6.0"
router = APIRouter(prefix="/v1/advanced", tags=["Advanced Analysis"])

# =============================================================================
# Env helpers
# =============================================================================
_TRUTHY = {"1", "true", "yes", "y", "on", "t", "enable", "enabled"}
_FALSY = {"0", "false", "no", "n", "off", "f", "disable", "disabled"}


def _env_str(name: str, default: str = "") -> str:
    return (os.getenv(name) or default).strip()


def _env_bool(name: str, default: bool) -> bool:
    v = _env_str(name, "")
    if not v:
        return default
    vv = v.lower()
    if vv in _TRUTHY:
        return True
    if vv in _FALSY:
        return False
    return default


def _env_int(name: str, default: int, *, lo: int = -10**9, hi: int = 10**9) -> int:
    v = _env_str(name, "")
    if not v:
        return int(default)
    try:
        x = int(float(v))
        return max(lo, min(hi, x))
    except Exception:
        return int(default)


def _env_float(name: str, default: float, *, lo: float = -1e18, hi: float = 1e18) -> float:
    v = _env_str(name, "")
    if not v:
        return float(default)
    try:
        x = float(v)
        return max(lo, min(hi, x))
    except Exception:
        return float(default)


_MAX_CONCURRENCY = _env_int("ADVANCED_MAX_CONCURRENCY", 18, lo=4, hi=80)
_BATCH_SIZE = _env_int("ADVANCED_BATCH_SIZE", 250, lo=20, hi=2000)
_DEFAULT_TOP_N = _env_int("ADVANCED_DEFAULT_TOP_N", 50, lo=1, hi=500)
_PER_SYMBOL_TIMEOUT = _env_float("ADVANCED_PER_SYMBOL_TIMEOUT_SEC", 18.0, lo=3.0, hi=120.0)
_BATCH_TIMEOUT = _env_float("ADVANCED_BATCH_TIMEOUT_SEC", 40.0, lo=5.0, hi=180.0)
_TOTAL_TIMEOUT = _env_float("ADVANCED_TOTAL_TIMEOUT_SEC", 110.0, lo=8.0, hi=300.0)
_ALLOW_QUERY_TOKEN = _env_bool("ALLOW_QUERY_TOKEN", False)

# =============================================================================
# Time (Riyadh)
# =============================================================================
@lru_cache(maxsize=1)
def _riyadh_tz():
    try:
        from zoneinfo import ZoneInfo  # py3.9+

        return ZoneInfo("Asia/Riyadh")
    except Exception:
        # KSA is UTC+3 (no DST)
        return timezone.utc  # fallback; we still provide iso, but not ideal


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _riyadh_now_iso() -> str:
    try:
        return datetime.now(_riyadh_tz()).isoformat(timespec="seconds")
    except Exception:
        return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _to_riyadh_iso(utc_any: Any) -> str:
    if utc_any is None or utc_any == "":
        return ""
    try:
        s = str(utc_any).strip().replace("Z", "+00:00")
        dt = datetime.fromisoformat(s)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(_riyadh_tz()).isoformat(timespec="seconds")
    except Exception:
        return ""


# =============================================================================
# Normalization (symbols)
# =============================================================================
def _fallback_normalize(raw: str) -> str:
    s = (raw or "").strip().upper()
    if not s:
        return ""
    if s.startswith("TADAWUL:"):
        s = s.split(":", 1)[1].strip()
    if s.endswith(".TADAWUL"):
        s = s.replace(".TADAWUL", "")
    if any(ch in s for ch in ("^", "=", "/")):
        return s
    if s.isdigit():
        return f"{s}.SR"
    return s


@lru_cache(maxsize=1)
def _get_normalizer():
    # Prefer stable central normalizer if present; else v2 engine normalizer; else fallback.
    try:
        from core.symbols.normalize import normalize_symbol as ns  # type: ignore

        return ns
    except Exception:
        try:
            from core.data_engine_v2 import normalize_symbol as ns2  # type: ignore

            return ns2
        except Exception:
            return _fallback_normalize


def _normalize_any(raw: str) -> str:
    try:
        ns = _get_normalizer()
        out = ns(raw)  # type: ignore[misc]
        out = (out or "").strip().upper()
        return out or _fallback_normalize(raw)
    except Exception:
        return _fallback_normalize(raw)


def _dedupe_symbols(raw: Sequence[str]) -> List[str]:
    seen = set()
    out: List[str] = []
    for x in raw or []:
        s = _normalize_any(str(x or ""))
        if not s or s in seen:
            continue
        seen.add(s)
        out.append(s)
    return out


# =============================================================================
# Auth
# =============================================================================
@lru_cache(maxsize=1)
def _allowed_tokens() -> List[str]:
    toks: List[str] = []
    for k in ("APP_TOKEN", "BACKUP_APP_TOKEN", "TFB_APP_TOKEN"):
        v = _env_str(k, "")
        if v:
            toks.append(v)
    # de-dupe stable
    out: List[str] = []
    seen = set()
    for t in toks:
        if t not in seen:
            seen.add(t)
            out.append(t)
    return out


def _extract_bearer(authorization: Optional[str]) -> str:
    a = (authorization or "").strip()
    if not a:
        return ""
    parts = a.split()
    if len(parts) == 2 and parts[0].lower() == "bearer":
        return parts[1].strip()
    return ""


def _auth_ok(x_app_token: Optional[str], authorization: Optional[str], query_token: Optional[str]) -> bool:
    allowed = _allowed_tokens()
    if not allowed:
        return True  # open-mode

    tok = (x_app_token or "").strip()
    if tok and tok in allowed:
        return True

    btok = _extract_bearer(authorization)
    if btok and btok in allowed:
        return True

    if _ALLOW_QUERY_TOKEN:
        qtok = (query_token or "").strip()
        if qtok and qtok in allowed:
            return True

    return False


# =============================================================================
# Lazy imports (schemas / enriched quote / scoring)
# =============================================================================
@lru_cache(maxsize=1)
def _schemas_default_headers_59() -> Optional[List[str]]:
    try:
        from core.schemas import DEFAULT_HEADERS_59  # type: ignore

        return list(DEFAULT_HEADERS_59)
    except Exception:
        return None


@lru_cache(maxsize=1)
def _schema_headers_for_sheet_fn():
    try:
        from core.schemas import get_headers_for_sheet  # type: ignore

        return get_headers_for_sheet
    except Exception:
        return None


@lru_cache(maxsize=1)
def _enriched_quote_class():
    try:
        from core.enriched_quote import EnrichedQuote  # type: ignore

        return EnrichedQuote
    except Exception:
        return None


@lru_cache(maxsize=1)
def _enriched_headers_59() -> Optional[List[str]]:
    try:
        from core.enriched_quote import ENRICHED_HEADERS_59  # type: ignore

        return list(ENRICHED_HEADERS_59)
    except Exception:
        return None


@lru_cache(maxsize=1)
def _scoring_enricher():
    try:
        from core.scoring_engine import enrich_with_scores  # type: ignore

        return enrich_with_scores
    except Exception:
        return None


# =============================================================================
# Canonicalization utilities (ROI / Forecast / Time)
# =============================================================================
def _hkey(x: Any) -> str:
    s = str(x or "").strip().lower()
    if not s:
        return ""
    # keep only alnum
    return "".join(ch for ch in s if ch.isalnum())


def _build_key_index(d: Dict[str, Any]) -> Dict[str, Any]:
    km: Dict[str, Any] = {}
    for k, v in (d or {}).items():
        ks = str(k or "").strip()
        if not ks:
            continue
        km.setdefault(_hkey(ks), v)
        alt = (
            ks.replace("%", "pct")
            .replace("(", "")
            .replace(")", "")
            .replace("/", "_")
            .replace("-", "_")
            .replace(" ", "_")
        )
        km.setdefault(_hkey(alt), v)
    return km


def _safe_float(x: Any) -> Optional[float]:
    try:
        if x is None or x == "":
            return None
        return float(x)
    except Exception:
        return None


def _ratio_to_percent_if_ratio(x: Any) -> Any:
    """
    If looks like ratio (-1..1), convert to percent. Otherwise keep as-is.
    """
    f = _safe_float(x)
    if f is None:
        return x
    if -1.0 <= f <= 1.0:
        return f * 100.0
    return f


_CANON_ROI_KEYS = {
    "expected_roi_1m": [
        "expected_roi_1m",
        "expected_roi_pct_1m",
        "expected_roi_percent_1m",
        "expected_roi_(1m)",
        "expected_roi_%_(1m)",
        "expectedroi1m",
        "expectedroipct1m",
        "expected_roi_30d",
        "expected_roi_pct_30d",
    ],
    "expected_roi_3m": [
        "expected_roi_3m",
        "expected_roi_pct_3m",
        "expected_roi_percent_3m",
        "expectedroi3m",
        "expectedroipct3m",
        "expected_roi_90d",
        "expected_roi_pct_90d",
    ],
    "expected_roi_12m": [
        "expected_roi_12m",
        "expected_roi_pct_12m",
        "expected_roi_percent_12m",
        "expectedroi12m",
        "expectedroipct12m",
        "expected_roi_1y",
        "expected_roi_pct_1y",
    ],
}

_CANON_FC_KEYS = {
    "forecast_price_1m": ["forecast_price_1m", "forecastprice1m", "target_price_1m", "forecast_price_30d"],
    "forecast_price_3m": ["forecast_price_3m", "forecastprice3m", "target_price_3m", "forecast_price_90d"],
    "forecast_price_12m": ["forecast_price_12m", "forecastprice12m", "target_price_12m", "forecast_price_1y"],
}


def _canonize_roi_and_forecast(u: Dict[str, Any]) -> Dict[str, Any]:
    if not isinstance(u, dict) or not u:
        return u
    km = _build_key_index(u)

    for canon, variants in _CANON_ROI_KEYS.items():
        if u.get(canon) not in (None, "", "NA", "N/A"):
            continue
        found = None
        for vk in variants:
            vv = km.get(_hkey(vk))
            if vv not in (None, "", "NA", "N/A"):
                found = vv
                break
        if found is not None:
            u[canon] = _ratio_to_percent_if_ratio(found)

    for canon, variants in _CANON_FC_KEYS.items():
        if u.get(canon) not in (None, "", "NA", "N/A"):
            continue
        found = None
        for vk in variants:
            vv = km.get(_hkey(vk))
            if vv not in (None, "", "NA", "N/A"):
                found = vv
                break
        if found is not None:
            u[canon] = found

    return u


def _inject_times(u: Dict[str, Any]) -> Dict[str, Any]:
    if not isinstance(u, dict):
        return u

    if not u.get("last_updated_utc"):
        u["last_updated_utc"] = _utc_now_iso()
    if not u.get("last_updated_riyadh"):
        u["last_updated_riyadh"] = _to_riyadh_iso(u.get("last_updated_utc")) or _riyadh_now_iso()

    # forecast_updated_utc may exist in multiple variants
    if not u.get("forecast_updated_utc"):
        if u.get("forecast_updated"):
            u["forecast_updated_utc"] = str(u.get("forecast_updated"))
        elif u.get("forecast_updated_at"):
            u["forecast_updated_utc"] = str(u.get("forecast_updated_at"))
        else:
            u["forecast_updated_utc"] = u.get("last_updated_utc")

    if not u.get("forecast_updated_riyadh"):
        u["forecast_updated_riyadh"] = _to_riyadh_iso(u.get("forecast_updated_utc")) or _riyadh_now_iso()

    return u


# =============================================================================
# Engine resolution + probing
# =============================================================================
async def _maybe_await(x: Any) -> Any:
    if inspect.isawaitable(x):
        return await x
    return x


_ENGINE: Optional[Any] = None
_ENGINE_LOCK = asyncio.Lock()


async def _resolve_engine(request: Request) -> Optional[Any]:
    # (1) app.state.engine
    st = getattr(request.app, "state", None)
    if st is not None:
        eng = getattr(st, "engine", None)
        if eng is not None:
            return eng

    # (2) cached
    global _ENGINE
    if _ENGINE is not None:
        return _ENGINE

    # (3) lazy get_engine()
    async with _ENGINE_LOCK:
        if _ENGINE is not None:
            return _ENGINE
        try:
            from core.data_engine_v2 import get_engine  # type: ignore

            _ENGINE = await _maybe_await(get_engine())
            return _ENGINE
        except Exception:
            return None


def _engine_capabilities(engine: Any) -> Dict[str, bool]:
    if engine is None:
        return {}
    names = [
        # single quote
        "get_enriched_quote",
        "fetch_enriched_quote",
        "get_unified_quote",
        "get_quote_unified",
        "get_quote",
        "fetch_quote",
        # batch
        "get_enriched_quotes",
        "get_enriched_quotes_batch",
        "get_quotes_batch",
        "fetch_quotes_batch",
        "get_unified_quotes",
        "get_quotes",
        # cache setters
        "set_cached_sheet_snapshot",
        "set_sheet_snapshot",
        "cache_sheet_snapshot",
    ]
    return {n: callable(getattr(engine, n, None)) for n in names}


async def _engine_get_quote(engine: Any, symbol: str, *, mode: str = "") -> Any:
    if engine is None:
        raise RuntimeError("Engine unavailable")

    for name in (
        "get_enriched_quote",
        "fetch_enriched_quote",
        "get_unified_quote",
        "get_quote_unified",
        "get_quote",
        "fetch_quote",
    ):
        fn = getattr(engine, name, None)
        if callable(fn):
            if mode:
                try:
                    return await _maybe_await(fn(symbol, mode=mode))
                except TypeError:
                    return await _maybe_await(fn(symbol))
            return await _maybe_await(fn(symbol))

    raise RuntimeError("Engine has no supported single-quote method")


async def _engine_get_quotes_batch(engine: Any, symbols: List[str], *, mode: str = "") -> Optional[Union[List[Any], Dict[str, Any]]]:
    if engine is None:
        return None

    for name in (
        "get_enriched_quotes_batch",
        "get_enriched_quotes",
        "get_quotes_batch",
        "fetch_quotes_batch",
        "get_unified_quotes",
        "get_quotes",
    ):
        fn = getattr(engine, name, None)
        if callable(fn):
            try:
                if mode:
                    try:
                        out = await _maybe_await(fn(symbols, mode=mode))
                    except TypeError:
                        out = await _maybe_await(fn(symbols))
                else:
                    out = await _maybe_await(fn(symbols))
                if isinstance(out, (list, dict)):
                    return out
                return None
            except Exception:
                return None

    return None


# =============================================================================
# Models
# =============================================================================
class _ExtraIgnore(BaseModel):
    if _PYDANTIC_V2:
        model_config = ConfigDict(extra="ignore")
    else:  # pragma: no cover
        class Config:  # type: ignore
            extra = "ignore"


class PushSheetItem(_ExtraIgnore):
    sheet: str
    headers: List[str] = Field(default_factory=list)
    rows: List[List[Any]] = Field(default_factory=list)


class PushSheetRowsRequest(_ExtraIgnore):
    items: List[PushSheetItem] = Field(default_factory=list)
    universe_rows: Optional[int] = None
    source: Optional[str] = None


class AdvancedSheetRequest(_ExtraIgnore):
    tickers: List[str] = Field(default_factory=list)
    symbols: List[str] = Field(default_factory=list)
    top_n: Optional[int] = Field(default=None, ge=1, le=500)
    sheet_name: Optional[str] = None
    sheetName: Optional[str] = None  # compatibility
    include_meta: Optional[bool] = True
    mode: Optional[str] = None  # payload mode hint
    # optional: allow caller to override headers (advanced clients only)
    headers: Optional[List[str]] = None


# =============================================================================
# Headers + row mapping
# =============================================================================
def _resolve_sheet_name(req: AdvancedSheetRequest) -> str:
    return (req.sheet_name or req.sheetName or "").strip()


def _get_headers(sheet_name: str, override: Optional[List[str]] = None) -> List[str]:
    # (0) override from request
    if isinstance(override, list) and override:
        h = [str(x).strip() for x in override if str(x).strip()]
        if h:
            return h

    # (1) schemas.get_headers_for_sheet(sheet_name)
    fn = _schema_headers_for_sheet_fn()
    if callable(fn) and sheet_name:
        try:
            hh = fn(sheet_name)  # type: ignore[misc]
            if isinstance(hh, list) and hh:
                h2 = [str(x).strip() for x in hh if str(x).strip()]
                if h2:
                    return h2
        except Exception:
            pass

    # (2) core.enriched_quote default 59
    eh = _enriched_headers_59()
    if isinstance(eh, list) and eh:
        return [str(x).strip() for x in eh if str(x).strip()]

    # (3) core.schemas default 59
    sh = _schemas_default_headers_59()
    if isinstance(sh, list) and sh:
        return [str(x).strip() for x in sh if str(x).strip()]

    # (4) minimal fallback (never empty)
    return [
        "Symbol",
        "Name",
        "Price",
        "Forecast Price (1M)",
        "Expected ROI % (1M)",
        "Forecast Price (3M)",
        "Expected ROI % (3M)",
        "Forecast Price (12M)",
        "Expected ROI % (12M)",
        "Risk Score",
        "Overall Score",
        "Recommendation",
        "Error",
        "Data Source",
        "Data Quality",
        "Last Updated (UTC)",
        "Last Updated (Riyadh)",
    ]


def _pad_row(row: List[Any], n: int) -> List[Any]:
    if len(row) == n:
        return row
    if len(row) < n:
        return row + [None] * (n - len(row))
    return row[:n]


def _safe_dict_from_any(obj: Any) -> Dict[str, Any]:
    if obj is None:
        return {}
    if isinstance(obj, dict):
        return dict(obj)

    try:
        md = getattr(obj, "model_dump", None)
        if callable(md):
            d = md()
            if isinstance(d, dict):
                return dict(d)
    except Exception:
        pass

    try:
        dd = getattr(obj, "dict", None)
        if callable(dd):
            d = dd()
            if isinstance(d, dict):
                return dict(d)
    except Exception:
        pass

    try:
        if is_dataclass(obj):
            return dict(getattr(obj, "__dict__", {}) or {})
    except Exception:
        pass

    try:
        od = getattr(obj, "__dict__", None)
        if isinstance(od, dict):
            return dict(od)
    except Exception:
        pass

    return {"value": str(obj)}


def _enrich_scores_best_effort(u: Dict[str, Any]) -> Dict[str, Any]:
    fn = _scoring_enricher()
    if callable(fn):
        try:
            out = fn(u)
            if isinstance(out, dict):
                return out
        except Exception:
            pass
    return u


def _finalize_unified(u: Dict[str, Any], requested_symbol: str) -> Dict[str, Any]:
    sym = (requested_symbol or "").strip().upper()
    if sym:
        u.setdefault("symbol", sym)
        u.setdefault("requested_symbol", sym)

    u = _enrich_scores_best_effort(u)
    u = _canonize_roi_and_forecast(u)
    u = _inject_times(u)

    # default metadata
    u.setdefault("data_source", "advanced_analysis")
    u.setdefault("data_quality", "PARTIAL" if u.get("price") or u.get("current_price") else "MISSING")
    u.setdefault("recommendation", "HOLD")

    # common score fallback
    if u.get("overall_score") is None and u.get("opportunity_score") is not None:
        u["overall_score"] = u.get("opportunity_score")

    return u


def _row_fallback(headers: List[str], u: Dict[str, Any]) -> List[Any]:
    """
    Robust fallback mapper for any header set.
    - Builds tolerant key index
    - Maps common sheet labels to canonical keys
    """
    km = _build_key_index(u)

    def pick(*keys: str) -> Any:
        for k in keys:
            v = km.get(_hkey(k))
            if v is not None and v != "":
                return v
        return None

    out: List[Any] = []
    for h in headers:
        hk = _hkey(h)

        # High-value explicit mappings (sheet-style headers)
        if hk in ("symbol", "ticker"):
            out.append(u.get("symbol") or u.get("requested_symbol"))
            continue

        if "forecastprice" in hk and "1m" in hk:
            out.append(u.get("forecast_price_1m"))
            continue
        if "forecastprice" in hk and "3m" in hk:
            out.append(u.get("forecast_price_3m"))
            continue
        if "forecastprice" in hk and ("12m" in hk or "1y" in hk):
            out.append(u.get("forecast_price_12m"))
            continue

        if "expectedroi" in hk and "1m" in hk:
            out.append(u.get("expected_roi_1m"))
            continue
        if "expectedroi" in hk and "3m" in hk:
            out.append(u.get("expected_roi_3m"))
            continue
        if "expectedroi" in hk and ("12m" in hk or "1y" in hk):
            out.append(u.get("expected_roi_12m"))
            continue

        if hk in ("lastupdatedutc", "lastupdated(utc)"):
            out.append(u.get("last_updated_utc"))
            continue
        if hk in ("lastupdatedriyadh", "lastupdated(riyadh)"):
            out.append(u.get("last_updated_riyadh"))
            continue

        # Generic mapping by normalized key
        v = km.get(hk)
        if v is None:
            # try common variants if not found
            if hk == _hkey("Price"):
                v = pick("current_price", "last_price", "price")
            elif hk == _hkey("Change"):
                v = pick("price_change", "change")
            elif hk == _hkey("Change %"):
                v = pick("percent_change", "change_percent", "change_pct")
            elif hk == _hkey("Error"):
                v = pick("error")
            elif hk == _hkey("Recommendation"):
                v = pick("recommendation")
        out.append(v)

    return _pad_row(out, len(headers))


def _map_to_row(headers: List[str], unified: Dict[str, Any]) -> List[Any]:
    EnrichedQuote = _enriched_quote_class()
    if EnrichedQuote is not None:
        try:
            eq = EnrichedQuote.from_unified(unified)  # type: ignore[attr-defined]
            to_row = getattr(eq, "to_row", None)
            if callable(to_row):
                row = to_row(headers)
                if isinstance(row, list):
                    return _pad_row(row, len(headers))
        except Exception:
            pass
    return _row_fallback(headers, unified)


def _placeholder(symbol: str, err: str) -> Dict[str, Any]:
    sym = (symbol or "").strip().upper() or "UNKNOWN"
    return _finalize_unified(
        {
            "symbol": sym,
            "error": err,
            "data_quality": "MISSING",
            "data_source": "advanced_analysis",
            "recommendation": "HOLD",
        },
        requested_symbol=sym,
    )


# =============================================================================
# Routes
# =============================================================================
@router.get("/health")
async def advanced_health(request: Request) -> Dict[str, Any]:
    eng = await _resolve_engine(request)
    caps = _engine_capabilities(eng)
    return {
        "status": "ok",
        "version": ADVANCED_ANALYSIS_VERSION,
        "engine": type(eng).__name__ if eng else "none",
        "capabilities": {k: v for k, v in caps.items() if v},
        "max_concurrency": _MAX_CONCURRENCY,
        "batch_size": _BATCH_SIZE,
        "timeouts": {
            "per_symbol_sec": _PER_SYMBOL_TIMEOUT,
            "batch_sec": _BATCH_TIMEOUT,
            "total_sec": _TOTAL_TIMEOUT,
        },
        "time_utc": _utc_now_iso(),
        "time_riyadh": _riyadh_now_iso(),
    }


@router.get("/inspect", include_in_schema=False)
async def advanced_inspect(request: Request) -> Dict[str, Any]:
    eng = await _resolve_engine(request)
    return {
        "status": "ok",
        "version": ADVANCED_ANALYSIS_VERSION,
        "engine": {
            "present": bool(eng),
            "type": type(eng).__name__ if eng else "none",
            "capabilities": _engine_capabilities(eng),
        },
        "auth": {
            "open_mode": (len(_allowed_tokens()) == 0),
            "allow_query_token": bool(_ALLOW_QUERY_TOKEN),
        },
        "time_utc": _utc_now_iso(),
        "time_riyadh": _riyadh_now_iso(),
    }


@router.post("/sheet-rows")
async def advanced_sheet_rows(
    request: Request,
    body: Dict[str, Any] = Body(...),
    mode: str = Query(default="", description="Optional mode hint (e.g., extended)"),
    token: Optional[str] = Query(default=None, description="Optional token (only if ALLOW_QUERY_TOKEN=1)"),
    x_app_token: Optional[str] = Header(default=None, alias="X-APP-TOKEN"),
    authorization: Optional[str] = Header(default=None, alias="Authorization"),
) -> Dict[str, Any]:
    """
    (A) PUSH MODE:
      body={"items":[{sheet,headers,rows},...]}

    (B) COMPUTE MODE:
      body={"tickers":[...]} or {"symbols":[...]}
      returns {status, headers, rows, meta?}
    """
    rid = getattr(getattr(request, "state", None), "request_id", None) or "n/a"
    t_start = time.perf_counter()

    if not _auth_ok(x_app_token, authorization, token):
        return {"status": "error", "error": "Unauthorized", "version": ADVANCED_ANALYSIS_VERSION, "request_id": rid}

    engine = await _resolve_engine(request)

    # -------------------------------------------------------------------------
    # (A) PUSH MODE
    # -------------------------------------------------------------------------
    if isinstance(body.get("items"), list):
        try:
            push = PushSheetRowsRequest.model_validate(body) if _PYDANTIC_V2 else PushSheetRowsRequest.parse_obj(body)

            if engine is None:
                return {
                    "status": "error",
                    "error": "Engine unavailable (cannot cache snapshot)",
                    "version": ADVANCED_ANALYSIS_VERSION,
                    "request_id": rid,
                }

            setters = [
                getattr(engine, "set_cached_sheet_snapshot", None),
                getattr(engine, "set_sheet_snapshot", None),
                getattr(engine, "cache_sheet_snapshot", None),
            ]
            setter = next((fn for fn in setters if callable(fn)), None)
            if not callable(setter):
                return {
                    "status": "error",
                    "error": "Engine does not support snapshot caching",
                    "version": ADVANCED_ANALYSIS_VERSION,
                    "request_id": rid,
                }

            written: List[Dict[str, Any]] = []
            for it in push.items:
                try:
                    await _maybe_await(setter(it.sheet, it.headers, it.rows))
                    written.append({"sheet": it.sheet, "rows": len(it.rows), "status": "cached"})
                except Exception as e:
                    written.append({"sheet": it.sheet, "rows": len(it.rows), "status": "error", "error": str(e)})

            st = "success" if all(w.get("status") == "cached" for w in written) else "partial"
            return {
                "status": st,
                "written": written,
                "version": ADVANCED_ANALYSIS_VERSION,
                "request_id": rid,
                "meta": {
                    "duration_ms": round((time.perf_counter() - t_start) * 1000.0, 1),
                    "time_riyadh": _riyadh_now_iso(),
                },
            }
        except Exception as e:
            return {
                "status": "error",
                "error": str(e),
                "version": ADVANCED_ANALYSIS_VERSION,
                "request_id": rid,
            }

    # -------------------------------------------------------------------------
    # (B) COMPUTE MODE
    # -------------------------------------------------------------------------
    try:
        req = AdvancedSheetRequest.model_validate(body) if _PYDANTIC_V2 else AdvancedSheetRequest.parse_obj(body)
        payload_mode = (req.mode or "").strip()
        effective_mode = (mode or payload_mode or "").strip()

        raw_syms = list(req.tickers or []) + list(req.symbols or [])
        symbols = _dedupe_symbols(raw_syms)

        if not symbols:
            return {
                "status": "error",
                "error": "No tickers/symbols provided",
                "version": ADVANCED_ANALYSIS_VERSION,
                "request_id": rid,
            }

        top_n = int(req.top_n or _DEFAULT_TOP_N)
        top_n = max(1, min(500, top_n))
        symbols = symbols[:top_n]

        sheet_name = _resolve_sheet_name(req)
        headers = _get_headers(sheet_name, override=req.headers)
        if not headers:
            headers = _get_headers("", override=None)  # final safety

        # Soft total timeout management (returns partial if exceeded)
        deadline = t_start + float(_TOTAL_TIMEOUT)

        if engine is None:
            # Always return writeable structure
            rows = []
            for s in symbols:
                ph = _placeholder(s, "Engine unavailable")
                rows.append(_map_to_row(headers, ph))
            return {
                "status": "error",
                "error": "Engine unavailable",
                "headers": headers,
                "rows": rows,
                "version": ADVANCED_ANALYSIS_VERSION,
                "request_id": rid,
                "meta": {
                    "requested": len(symbols),
                    "errors": len(symbols),
                    "engine": "none",
                    "mode": effective_mode,
                    "sheet": sheet_name,
                    "duration_ms": round((time.perf_counter() - t_start) * 1000.0, 1),
                    "time_riyadh": _riyadh_now_iso(),
                },
            }

        errors = 0
        warnings: List[str] = []

        # Helper: convert engine output into finalized dict
        def finalize_obj(obj: Any, sym: str) -> Dict[str, Any]:
            d = _safe_dict_from_any(obj)
            return _finalize_unified(d, requested_symbol=sym)

        # Chunked fetch strategy:
        # - Try batch per chunk (fast)
        # - If batch missing/unreliable, fallback per-symbol concurrently
        results_by_symbol: Dict[str, Dict[str, Any]] = {}

        async def fetch_chunk_batch(chunk: List[str]) -> bool:
            """
            Returns True if batch produced usable results for this chunk.
            """
            nonlocal errors
            try:
                remaining = deadline - time.perf_counter()
                if remaining <= 0:
                    warnings.append("total_timeout_exceeded_before_batch")
                    return False

                out = await asyncio.wait_for(
                    _engine_get_quotes_batch(engine, chunk, mode=effective_mode),
                    timeout=min(_BATCH_TIMEOUT, max(3.0, remaining)),
                )
                if out is None:
                    return False

                # dict: symbol -> payload
                if isinstance(out, dict):
                    for s in chunk:
                        obj = out.get(s) or out.get(str(s).upper()) or out.get(str(s).strip())
                        if obj is None:
                            results_by_symbol[s] = _placeholder(s, "Engine batch missing item")
                            errors += 1
                        else:
                            results_by_symbol[s] = finalize_obj(obj, s)
                    return True

                # list: try align
                if isinstance(out, list) and len(out) == len(chunk):
                    for i, s in enumerate(chunk):
                        obj = out[i]
                        if isinstance(obj, Exception):
                            results_by_symbol[s] = _placeholder(s, str(obj))
                            errors += 1
                        else:
                            results_by_symbol[s] = finalize_obj(obj, s)
                    return True

                return False
            except asyncio.TimeoutError:
                warnings.append("batch_timeout")
                return False
            except Exception as e:
                warnings.append(f"batch_error:{e}")
                return False

        sem = asyncio.Semaphore(_MAX_CONCURRENCY)

        async def fetch_one(sym: str) -> None:
            nonlocal errors
            async with sem:
                try:
                    remaining = deadline - time.perf_counter()
                    if remaining <= 0:
                        results_by_symbol[sym] = _placeholder(sym, "total_timeout")
                        errors += 1
                        return
                    obj = await asyncio.wait_for(
                        _engine_get_quote(engine, sym, mode=effective_mode),
                        timeout=min(_PER_SYMBOL_TIMEOUT, max(3.0, remaining)),
                    )
                    results_by_symbol[sym] = finalize_obj(obj, sym)
                    # count as error if payload contains error string
                    if str(results_by_symbol[sym].get("error") or "").strip():
                        errors += 1
                except asyncio.TimeoutError:
                    results_by_symbol[sym] = _placeholder(sym, "timeout")
                    errors += 1
                except Exception as e:
                    results_by_symbol[sym] = _placeholder(sym, str(e))
                    errors += 1

        # Process in chunks
        for i in range(0, len(symbols), _BATCH_SIZE):
            chunk = symbols[i : i + _BATCH_SIZE]

            # Respect total timeout
            if time.perf_counter() >= deadline:
                warnings.append("total_timeout_exceeded_midway")
                for s in chunk:
                    if s not in results_by_symbol:
                        results_by_symbol[s] = _placeholder(s, "total_timeout")
                        errors += 1
                continue

            used_batch = await fetch_chunk_batch(chunk)

            if not used_batch:
                # per-symbol fallback for the chunk
                await asyncio.gather(*[fetch_one(s) for s in chunk])

        # Build rows in original requested order
        rows: List[List[Any]] = []
        for s in symbols:
            u = results_by_symbol.get(s) or _placeholder(s, "missing_result")
            rows.append(_map_to_row(headers, u))

        status = "success"
        if errors > 0 and errors < len(symbols):
            status = "partial"
        if errors >= len(symbols):
            status = "error"

        meta: Dict[str, Any] = {
            "version": ADVANCED_ANALYSIS_VERSION,
            "engine": type(engine).__name__,
            "mode": effective_mode,
            "sheet": sheet_name,
            "requested": len(symbols),
            "errors": errors,
            "warnings": warnings[:25],
            "request_id": rid,
            "duration_ms": round((time.perf_counter() - t_start) * 1000.0, 1),
            "time_utc": _utc_now_iso(),
            "time_riyadh": _riyadh_now_iso(),
            "limits": {
                "max_concurrency": _MAX_CONCURRENCY,
                "batch_size": _BATCH_SIZE,
                "per_symbol_timeout_sec": _PER_SYMBOL_TIMEOUT,
                "batch_timeout_sec": _BATCH_TIMEOUT,
                "total_timeout_sec": _TOTAL_TIMEOUT,
            },
        }

        out: Dict[str, Any] = {
            "status": status,
            "headers": headers,
            "rows": [_pad_row(r, len(headers)) for r in rows],
            "version": ADVANCED_ANALYSIS_VERSION,
        }

        if req.include_meta is not False:
            out["meta"] = meta

        if status != "success":
            out["error"] = f"{errors} rows had errors"

        return out

    except Exception as e:
        logger.exception("Advanced sheet-rows failure")
        return {
            "status": "error",
            "error": f"Computation failure: {str(e)}",
            "version": ADVANCED_ANALYSIS_VERSION,
            "request_id": rid,
        }


__all__ = ["router"]
