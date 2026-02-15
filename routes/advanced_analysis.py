# routes/advanced_analysis.py
"""
TADAWUL FAST BRIDGE — ADVANCED ANALYSIS ROUTES (v4.2.0)
PROD SAFE + ROI KEY ALIGNMENT + RIYADH LOCALIZATION + SCORING INTEGRATION (HARDENED)

Primary contract (Sheets controller expects this shape)
- POST /v1/advanced/sheet-rows
  -> { status: "success|partial|error", headers: [...], rows: [...], error?: str, meta?: {...} }

Key alignment guarantees
- Canonical ROI keys:
    expected_roi_1m, expected_roi_3m, expected_roi_12m
- Canonical forecast price keys:
    forecast_price_1m, forecast_price_3m, forecast_price_12m
- Riyadh timestamps injected:
    last_updated_riyadh, forecast_updated_riyadh

Resilience
- One bad symbol never breaks the batch (placeholder rows)
- Concurrency limited (env ADVANCED_MAX_CONCURRENCY)
- Engine method probing supports multiple versions (DataEngine v2+ evolutions)
"""

from __future__ import annotations

import asyncio
import inspect
import logging
import os
from datetime import datetime, timezone, timedelta
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

ADVANCED_ANALYSIS_VERSION = "4.2.0"
router = APIRouter(prefix="/v1/advanced", tags=["Advanced Analysis"])


# =============================================================================
# Settings / Limits
# =============================================================================
_TRUTHY = {"1", "true", "yes", "y", "on", "t"}


def _env_int(name: str, default: int) -> int:
    v = (os.getenv(name) or "").strip()
    if not v:
        return int(default)
    try:
        return int(float(v))
    except Exception:
        return int(default)


def _env_bool(name: str, default: bool) -> bool:
    v = (os.getenv(name) or "").strip().lower()
    if not v:
        return default
    return v in _TRUTHY


_MAX_CONCURRENCY = max(4, min(50, _env_int("ADVANCED_MAX_CONCURRENCY", 18)))
_DEFAULT_TOP_N = max(1, min(500, _env_int("ADVANCED_DEFAULT_TOP_N", 50)))
_ALLOW_QUERY_TOKEN = _env_bool("ALLOW_QUERY_TOKEN", False)


# =============================================================================
# Core Shims (Safe Imports)
# =============================================================================
try:
    from core.config import get_settings  # type: ignore
except Exception:  # pragma: no cover
    def get_settings():  # type: ignore
        return None


try:
    from core.schemas import DEFAULT_HEADERS_59 as _SCHEMAS_DEFAULT_59  # type: ignore
    from core.schemas import get_headers_for_sheet as _get_headers_for_sheet  # type: ignore

    _SCHEMAS_OK = True
except Exception:  # pragma: no cover
    _SCHEMAS_DEFAULT_59 = None
    _get_headers_for_sheet = None
    _SCHEMAS_OK = False


# =============================================================================
# Normalization & Localization Utilities
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
def _get_normalizer() -> Any:
    # Prefer stable core normalizer if present; else v2 engine normalize; else fallback.
    try:
        from core.symbols.normalize import normalize_symbol as _NS  # type: ignore

        return _NS
    except Exception:
        try:
            from core.data_engine_v2 import normalize_symbol as _NS2  # type: ignore

            return _NS2
        except Exception:
            return _fallback_normalize


def _normalize_any(raw: str) -> str:
    try:
        ns = _get_normalizer()
        out = ns(raw)  # type: ignore
        out = (out or "").strip().upper()
        return out or _fallback_normalize(raw)
    except Exception:
        return _fallback_normalize(raw)


_RIYADH_TZ = timezone(timedelta(hours=3))


def _riyadh_iso_now() -> str:
    return datetime.now(_RIYADH_TZ).isoformat()


def _to_riyadh_iso(utc_iso: Optional[str]) -> str:
    if not utc_iso:
        return ""
    try:
        dt = datetime.fromisoformat(str(utc_iso).replace("Z", "+00:00"))
        return dt.astimezone(_RIYADH_TZ).isoformat()
    except Exception:
        return ""


# =============================================================================
# Header-key normalization (robust mapping)
# =============================================================================
def _hkey(x: Any) -> str:
    s = str(x or "").strip().lower()
    if not s:
        return ""
    out = []
    for ch in s:
        if ch.isalnum():
            out.append(ch)
    return "".join(out)


def _build_key_index(d: Dict[str, Any]) -> Dict[str, Any]:
    """
    Build a tolerant lookup: for each dict key, store multiple normalized forms.
    """
    km: Dict[str, Any] = {}
    for k, v in (d or {}).items():
        ks = str(k or "").strip()
        if not ks:
            continue
        hk = _hkey(ks)
        if hk and hk not in km:
            km[hk] = v

        # common alternates: underscores / spaces / percent symbols etc.
        alt = ks.replace("%", "pct").replace("(", "").replace(")", "").replace("/", "_").replace("-", "_")
        hk2 = _hkey(alt)
        if hk2 and hk2 not in km:
            km[hk2] = v
    return km


# =============================================================================
# ROI / Forecast Canonicalization
# =============================================================================
_CANON_ROI_KEYS = {
    # canonical -> possible variants
    "expected_roi_1m": [
        "expected_roi_1m",
        "expected_roi_pct_1m",
        "expected_roi_percent_1m",
        "expectedroi1m",
        "expectedroipct1m",
        "expected_roi_(1m)",
        "expected_roi_%_(1m)",
        "expected_roi%(1m)",
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
    """
    Ensure canonical keys exist when possible by copying from known variants.
    Never deletes existing values.
    """
    if not isinstance(u, dict) or not u:
        return u

    km = _build_key_index(u)

    # ROI
    for canon, variants in _CANON_ROI_KEYS.items():
        if canon in u and u.get(canon) not in (None, "", "NA", "N/A"):
            continue
        found = None
        for vkey in variants:
            vv = km.get(_hkey(vkey))
            if vv not in (None, "", "NA", "N/A"):
                found = vv
                break
        if found is not None:
            u[canon] = found

    # Forecast Price
    for canon, variants in _CANON_FC_KEYS.items():
        if canon in u and u.get(canon) not in (None, "", "NA", "N/A"):
            continue
        found = None
        for vkey in variants:
            vv = km.get(_hkey(vkey))
            if vv not in (None, "", "NA", "N/A"):
                found = vv
                break
        if found is not None:
            u[canon] = found

    return u


def _inject_times(u: Dict[str, Any]) -> Dict[str, Any]:
    """
    Ensure utc and riyadh timestamps exist (best-effort).
    """
    if not isinstance(u, dict):
        return u

    # last_updated_utc
    if not u.get("last_updated_utc"):
        u["last_updated_utc"] = datetime.now(timezone.utc).isoformat()

    u["last_updated_riyadh"] = _to_riyadh_iso(u.get("last_updated_utc")) or _riyadh_iso_now()

    # forecast_updated_utc might exist; if not, align with last_updated_utc
    if not u.get("forecast_updated_utc"):
        # do not overwrite if engine already sets forecast_updated (alt name)
        if u.get("forecast_updated"):
            u["forecast_updated_utc"] = str(u.get("forecast_updated"))
        else:
            u["forecast_updated_utc"] = u.get("last_updated_utc")

    u["forecast_updated_riyadh"] = _to_riyadh_iso(u.get("forecast_updated_utc")) or _riyadh_iso_now()

    return u


# =============================================================================
# Scoring (Lazy Imports)
# =============================================================================
@lru_cache(maxsize=1)
def _try_import_scoring_enricher() -> Optional[Any]:
    """
    Prefer scoring_engine.enrich_with_scores if available.
    """
    try:
        from core.scoring_engine import enrich_with_scores  # type: ignore

        return enrich_with_scores
    except Exception:
        return None


def _enrich_scores_best_effort(unified: Any) -> Any:
    fn = _try_import_scoring_enricher()
    if callable(fn):
        try:
            return fn(unified)
        except Exception:
            return unified
    return unified


@lru_cache(maxsize=1)
def _try_import_enriched_quote_class() -> Optional[Any]:
    try:
        from core.enriched_quote import EnrichedQuote  # type: ignore

        return EnrichedQuote
    except Exception:
        return None


async def _maybe_await(x: Any) -> Any:
    if inspect.isawaitable(x):
        return await x
    return x


# =============================================================================
# Auth (X-APP-TOKEN / Authorization Bearer / optional query token)
# =============================================================================
@lru_cache(maxsize=1)
def _allowed_tokens() -> List[str]:
    tokens: List[str] = []
    for k in ("APP_TOKEN", "BACKUP_APP_TOKEN", "TFB_APP_TOKEN"):
        v = (os.getenv(k) or "").strip()
        if v:
            tokens.append(v)
    # de-dupe stable
    out: List[str] = []
    seen = set()
    for t in tokens:
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

    # priority: X-APP-TOKEN -> Authorization -> optional query token
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
# Engine Resolution (robust probing)
# =============================================================================
_ENGINE: Optional[Any] = None
_ENGINE_LOCK = asyncio.Lock()


async def _resolve_engine(request: Request) -> Optional[Any]:
    # 1) app.state.engine (preferred)
    st = getattr(request.app, "state", None)
    if st is not None:
        eng = getattr(st, "engine", None)
        if eng is not None:
            return eng

    # 2) cached singleton
    global _ENGINE
    if _ENGINE is not None:
        return _ENGINE

    # 3) lazy import + resolve
    async with _ENGINE_LOCK:
        if _ENGINE is not None:
            return _ENGINE
        try:
            from core.data_engine_v2 import get_engine  # type: ignore

            _ENGINE = await _maybe_await(get_engine())
            return _ENGINE
        except Exception:
            return None


async def _engine_get_quote(engine: Any, symbol: str, *, mode: str = "") -> Any:
    """
    Probe for engine methods across versions.
    Returns either a dict-like unified quote OR any object that can be converted later.
    """
    if engine is None:
        raise RuntimeError("Engine unavailable")

    # Prefer enriched quote method variants
    for name in (
        "get_enriched_quote",
        "fetch_enriched_quote",
        "get_quote_unified",
        "get_unified_quote",
        "get_quote",
        "fetch_quote",
    ):
        fn = getattr(engine, name, None)
        if callable(fn):
            try:
                # Some engines accept mode=... or options; try best-effort
                if mode:
                    try:
                        return await _maybe_await(fn(symbol, mode=mode))
                    except TypeError:
                        return await _maybe_await(fn(symbol))
                return await _maybe_await(fn(symbol))
            except Exception:
                raise

    raise RuntimeError("Engine has no supported quote method")


async def _engine_get_quotes_batch(engine: Any, symbols: List[str], *, mode: str = "") -> Optional[List[Any]]:
    """
    Optional batch method probing. Returns list aligned to symbols if possible.
    If unavailable, returns None.
    """
    if engine is None:
        return None

    for name in (
        "get_enriched_quotes_batch",
        "get_quotes_batch",
        "fetch_quotes_batch",
        "get_unified_quotes",
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
                # Normalize: dict(sym->obj) OR list
                if isinstance(out, dict):
                    aligned: List[Any] = []
                    for s in symbols:
                        aligned.append(out.get(s) or out.get(str(s).upper()) or out.get(str(s).strip()))
                    return aligned
                if isinstance(out, list):
                    # If equal length, assume aligned; else fall back to None
                    if len(out) == len(symbols):
                        return out
                return None
            except Exception:
                return None

    return None


# =============================================================================
# Request/Response Models
# =============================================================================
class _ExtraIgnore(BaseModel):
    if _PYDANTIC_V2:
        model_config = ConfigDict(extra="ignore")
    else:
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
    mode: Optional[str] = None  # payload mode hint (also supported as query)


# =============================================================================
# Placeholder / Safety
# =============================================================================
def _make_placeholder(symbol: str, err: str) -> Dict[str, Any]:
    sym = (symbol or "").strip().upper()
    return {
        "symbol": sym,
        "error": err,
        "data_source": "advanced_analysis",
        "data_quality": "MISSING",
        "recommendation": "HOLD",
        "last_updated_utc": datetime.now(timezone.utc).isoformat(),
        "last_updated_riyadh": _riyadh_iso_now(),
        "forecast_updated_utc": datetime.now(timezone.utc).isoformat(),
        "forecast_updated_riyadh": _riyadh_iso_now(),
    }


def _resolve_sheet_name(req: AdvancedSheetRequest) -> str:
    sn = (req.sheet_name or req.sheetName or "").strip()
    return sn


def _dedupe_symbols(raw: Sequence[str]) -> List[str]:
    seen = set()
    out: List[str] = []
    for t in raw or []:
        s = _normalize_any(str(t or ""))
        if not s or s in seen:
            continue
        seen.add(s)
        out.append(s)
    return out


def _get_headers(sheet_name: str) -> List[str]:
    # Prefer schema resolver, else default headers
    try:
        if callable(_get_headers_for_sheet) and sheet_name:
            hh = _get_headers_for_sheet(sheet_name)  # type: ignore
            if isinstance(hh, list) and hh:
                return [str(x).strip() for x in hh if str(x).strip()]
    except Exception:
        pass

    if _SCHEMAS_DEFAULT_59 is not None:
        try:
            return list(_SCHEMAS_DEFAULT_59)  # type: ignore
        except Exception:
            pass

    # absolute fallback
    return [
        "Symbol",
        "Name",
        "Price",
        "Change",
        "Change %",
        "Market",
        "Currency",
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


def _row_from_dict(headers: List[str], d: Dict[str, Any]) -> List[Any]:
    """
    Generic tolerant mapper for when EnrichedQuote is unavailable.
    Maps by normalized header-key against normalized dict key index.
    """
    km = _build_key_index(d)

    # Provide canonical synonyms so header mapping succeeds even if headers are "sheet style"
    # (e.g., "Expected ROI % (1M)" should find expected_roi_1m)
    canon = dict(d)
    canon = _canonize_roi_and_forecast(canon)
    canon = _inject_times(canon)

    # rebuild after canon
    km2 = _build_key_index(canon)
    if km2:
        km.update({k: v for k, v in km2.items() if k not in km})

    out: List[Any] = [None] * len(headers)

    for i, h in enumerate(headers):
        hk = _hkey(h)

        # Common header-key remaps
        if hk in ("symbol", "ticker"):
            out[i] = canon.get("symbol") or canon.get("ticker") or canon.get("requested_symbol")
            continue
        if hk in ("lastupdatedutc", "lastupdated(utc)", "lastupdatedutc)"):
            out[i] = canon.get("last_updated_utc")
            continue
        if hk in ("lastupdatedriyadh", "lastupdated(riyadh)"):
            out[i] = canon.get("last_updated_riyadh")
            continue

        v = km.get(hk)

        # Extra: map expected roi / forecast price by common sheet labels
        if v is None:
            if "expectedroi" in hk and "1m" in hk:
                v = canon.get("expected_roi_1m")
            elif "expectedroi" in hk and "3m" in hk:
                v = canon.get("expected_roi_3m")
            elif "expectedroi" in hk and ("12m" in hk or "1y" in hk):
                v = canon.get("expected_roi_12m")
            elif "forecastprice" in hk and "1m" in hk:
                v = canon.get("forecast_price_1m")
            elif "forecastprice" in hk and "3m" in hk:
                v = canon.get("forecast_price_3m")
            elif "forecastprice" in hk and ("12m" in hk or "1y" in hk):
                v = canon.get("forecast_price_12m")

        out[i] = v

    # Ensure Symbol at least
    if out and (out[0] in (None, "", "NA", "N/A")):
        # find symbol column
        sym = canon.get("symbol") or canon.get("requested_symbol") or "UNKNOWN"
        try:
            sym_idx = next((j for j, h in enumerate(headers) if _hkey(h) in ("symbol", "ticker")), 0)
        except Exception:
            sym_idx = 0
        out[sym_idx] = sym

    return out


def _to_unified_dict(obj: Any, requested_symbol: str) -> Dict[str, Any]:
    """
    Convert whatever engine returns into a unified dict.
    - dict -> use it
    - pydantic-like -> .model_dump() / .dict()
    - object with __dict__ -> shallow copy
    Then enforce canonical keys + timestamps + scoring.
    """
    sym = (requested_symbol or "").strip().upper()

    d: Dict[str, Any] = {}
    try:
        if isinstance(obj, dict):
            d = dict(obj)
        else:
            # pydantic v2
            md = getattr(obj, "model_dump", None)
            if callable(md):
                d0 = md()
                if isinstance(d0, dict):
                    d = dict(d0)
            if not d:
                # pydantic v1
                dd = getattr(obj, "dict", None)
                if callable(dd):
                    d0 = dd()
                    if isinstance(d0, dict):
                        d = dict(d0)
            if not d:
                # generic
                od = getattr(obj, "__dict__", None)
                if isinstance(od, dict):
                    d = dict(od)
    except Exception:
        d = {}

    # minimal identity fields
    if "symbol" not in d and sym:
        d["symbol"] = sym
    if "requested_symbol" not in d and sym:
        d["requested_symbol"] = sym

    # scoring best-effort
    d2 = _enrich_scores_best_effort(d)
    if isinstance(d2, dict):
        d = d2

    d = _canonize_roi_and_forecast(d)
    d = _inject_times(d)

    # recommendation standard
    if not d.get("recommendation"):
        d["recommendation"] = "HOLD"

    return d


# =============================================================================
# Routes
# =============================================================================
@router.get("/health")
async def advanced_health(request: Request) -> Dict[str, Any]:
    eng = await _resolve_engine(request)
    return {
        "status": "ok",
        "version": ADVANCED_ANALYSIS_VERSION,
        "engine": type(eng).__name__ if eng else "none",
        "max_concurrency": _MAX_CONCURRENCY,
        "riyadh_time": _riyadh_iso_now(),
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
    Two modes:
    (A) Push mode: body has "items" -> engine cache snapshot setter
    (B) Compute mode: body has "symbols/tickers" -> returns {headers, rows}
    """
    if not _auth_ok(x_app_token, authorization, token):
        return {
            "status": "error",
            "error": "Unauthorized",
            "version": ADVANCED_ANALYSIS_VERSION,
        }

    engine = await _resolve_engine(request)

    # ------------------------------------------------------------------
    # (A) PUSH MODE
    # ------------------------------------------------------------------
    if isinstance(body.get("items"), list):
        try:
            push = PushSheetRowsRequest.model_validate(body) if _PYDANTIC_V2 else PushSheetRowsRequest.parse_obj(body)

            if engine is None:
                return {
                    "status": "error",
                    "error": "Engine unavailable (cannot cache snapshot)",
                    "version": ADVANCED_ANALYSIS_VERSION,
                }

            written: List[Dict[str, Any]] = []
            # Probe setter methods (different engine versions)
            setters = [
                getattr(engine, "set_cached_sheet_snapshot", None),
                getattr(engine, "set_sheet_snapshot", None),
                getattr(engine, "cache_sheet_snapshot", None),
            ]
            setter = next((fn for fn in setters if callable(fn)), None)

            if not callable(setter):
                return {
                    "status": "error",
                    "error": "Engine does not support snapshot caching (missing set_cached_sheet_snapshot)",
                    "version": ADVANCED_ANALYSIS_VERSION,
                }

            for it in push.items:
                try:
                    await _maybe_await(setter(it.sheet, it.headers, it.rows))
                    written.append({"sheet": it.sheet, "rows": len(it.rows), "status": "cached"})
                except Exception as e:
                    written.append({"sheet": it.sheet, "rows": len(it.rows), "status": "error", "error": str(e)})

            st = "success"
            if any(w.get("status") == "error" for w in written):
                st = "partial"

            return {
                "status": st,
                "written": written,
                "version": ADVANCED_ANALYSIS_VERSION,
                "riyadh_time": _riyadh_iso_now(),
            }
        except Exception as e:
            return {"status": "error", "error": str(e), "version": ADVANCED_ANALYSIS_VERSION}

    # ------------------------------------------------------------------
    # (B) COMPUTE MODE
    # ------------------------------------------------------------------
    try:
        req = AdvancedSheetRequest.model_validate(body) if _PYDANTIC_V2 else AdvancedSheetRequest.parse_obj(body)

        payload_mode = (req.mode or "").strip()
        effective_mode = (mode or payload_mode or "").strip()

        raw = list(req.tickers or []) + list(req.symbols or [])
        symbols = _dedupe_symbols(raw)

        if not symbols:
            return {
                "status": "error",
                "error": "No tickers provided",
                "version": ADVANCED_ANALYSIS_VERSION,
            }

        top_n = int(req.top_n or _DEFAULT_TOP_N)
        top_n = max(1, min(500, top_n))
        symbols = symbols[:top_n]

        sheet_name = _resolve_sheet_name(req)
        headers = _get_headers(sheet_name)

        if engine is None:
            # return structured error payload that Sheets service can write
            return {
                "status": "error",
                "error": "Engine unavailable",
                "version": ADVANCED_ANALYSIS_VERSION,
                "headers": headers or ["Symbol", "Error"],
                "rows": [[s, "Engine unavailable"] + [None] * max(0, (len(headers) - 2)) for s in symbols],
                "meta": {
                    "count": len(symbols),
                    "mode": effective_mode,
                    "sheet": sheet_name,
                    "riyadh_time": _riyadh_iso_now(),
                },
            }

        # 1) Try batch method if available
        batch_out = await _engine_get_quotes_batch(engine, symbols, mode=effective_mode)

        results: List[Dict[str, Any]] = []
        errors = 0

        if isinstance(batch_out, list) and len(batch_out) == len(symbols):
            for i, obj in enumerate(batch_out):
                sym_in = symbols[i]
                try:
                    if isinstance(obj, Exception):
                        raise obj
                    unified = _to_unified_dict(obj, sym_in)
                    results.append(unified)
                except Exception as e:
                    errors += 1
                    results.append(_make_placeholder(sym_in, str(e)))
        else:
            # 2) Fallback: concurrent per-symbol calls with a semaphore
            sem = asyncio.Semaphore(_MAX_CONCURRENCY)

            async def _one(sym: str) -> Dict[str, Any]:
                async with sem:
                    try:
                        obj = await _engine_get_quote(engine, sym, mode=effective_mode)
                        return _to_unified_dict(obj, sym)
                    except Exception as e:
                        return _make_placeholder(sym, str(e))

            raw_results = await asyncio.gather(*[_one(s) for s in symbols], return_exceptions=False)
            for r in raw_results:
                if isinstance(r, dict) and r.get("error"):
                    errors += 1
                results.append(r if isinstance(r, dict) else _make_placeholder("UNKNOWN", "Unexpected result"))

        # Mapping to rows
        EnrichedQuote = _try_import_enriched_quote_class()
        rows: List[List[Any]] = []

        for u in results:
            try:
                if EnrichedQuote and isinstance(u, dict):
                    # Prefer canonical EnrichedQuote mapper if present
                    try:
                        eq = EnrichedQuote.from_unified(u)  # type: ignore
                    except Exception:
                        # fallback constructor patterns
                        eq = EnrichedQuote(**u)  # type: ignore

                    to_row = getattr(eq, "to_row", None)
                    if callable(to_row):
                        rows.append(to_row(headers))  # type: ignore
                    else:
                        rows.append(_row_from_dict(headers, u))
                else:
                    rows.append(_row_from_dict(headers, u if isinstance(u, dict) else {}))
            except Exception as e:
                sym = str(u.get("symbol") or u.get("requested_symbol") or "ERROR")
                rows.append(_row_from_dict(headers, _make_placeholder(sym, f"Row map failed: {e}")))

        status = "success"
        if errors > 0:
            status = "partial"

        meta: Dict[str, Any] = {
            "version": ADVANCED_ANALYSIS_VERSION,
            "engine": type(engine).__name__,
            "mode": effective_mode,
            "sheet": sheet_name,
            "requested": len(symbols),
            "errors": errors,
            "riyadh_time": _riyadh_iso_now(),
        }

        out: Dict[str, Any] = {
            "status": status,
            "headers": headers,
            "rows": rows,
            "version": ADVANCED_ANALYSIS_VERSION,
        }

        if req.include_meta is not False:
            out["meta"] = meta

        if status != "success":
            out["error"] = f"{errors} rows had errors"

        return out

    except Exception as e:
        logger.exception("Advanced Route Failure")
        return {
            "status": "error",
            "error": f"Computation failure: {str(e)}",
            "version": ADVANCED_ANALYSIS_VERSION,
        }


__all__ = ["router"]
