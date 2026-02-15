# core/legacy_service.py
"""
core/legacy_service.py
------------------------------------------------------------
Compatibility shim (quiet + useful) — v1.9.0
(PROD SAFE + ADVANCED MAPPING + SNAPSHOT BRIDGE + SMART % SCALING)

Goals
- Provide a stable legacy router that NEVER breaks app startup.
- Never raise outward from endpoints (always HTTP 200 with a JSON body).
- Best-effort engine discovery:
    1) request.app.state.engine
    2) core.data_engine_v2.get_engine() (singleton) if available
    3) core.data_engine_v2.DataEngineV2/DataEngine temp
    4) core.data_engine.DataEngine() temp
- Support BOTH async and sync engine method implementations.
- Accept both {"symbols":[...]} and {"tickers":[...]} payload shapes.
- Batch-first; if batch is missing/fails, fallback per-symbol with bounded concurrency.

v1.9.0 Improvements
- ✅ Snapshot Bridge: /sheet-rows will best-effort call engine.set_cached_sheet_snapshot(...)
  so your Investment Advisor can consume cached pages reliably.
- ✅ Smarter method fallback: if a method exists but fails, it tries the next.
- ✅ Percent scaling: if header contains "%", auto-convert ratios (0.12 -> 12) safely.
- ✅ Stable master headers fallback: if schemas missing, returns a rich set (forecasts, scores, technicals).
- ✅ Better ordering/alignment in batch responses (list/dict/single item).
- ✅ Always fills Last Updated UTC/Riyadh if missing.
- ✅ Adds lightweight diagnostics in responses (engine_source + method).

Notes
- This file must remain import-safe (no heavy imports at module level).
"""

from __future__ import annotations

import asyncio
import importlib
import inspect
import os
import traceback
from datetime import datetime, timezone, timedelta
from typing import Any, Dict, List, Optional, Sequence, Tuple

from fastapi import APIRouter, Query, Request
from fastapi.encoders import jsonable_encoder
from pydantic import BaseModel
from starlette.responses import JSONResponse

VERSION = "1.9.0"

_TRUTHY = {"1", "true", "yes", "y", "on", "t", "enable", "enabled", "ok"}


def _env_bool(name: str, default: bool = False) -> bool:
    v = os.getenv(name)
    if v is None:
        return default
    return str(v).strip().lower() in _TRUTHY


def _env_int(name: str, default: int) -> int:
    try:
        v = int(str(os.getenv(name, "")).strip() or default)
        return v if v > 0 else default
    except Exception:
        return default


def _env_float(name: str, default: float) -> float:
    try:
        v = float(str(os.getenv(name, "")).strip() or default)
        return v if v > 0 else default
    except Exception:
        return default


ENABLE_EXTERNAL_LEGACY_ROUTER = _env_bool("ENABLE_EXTERNAL_LEGACY_ROUTER", False)
LOG_EXTERNAL_IMPORT_FAILURE = _env_bool("LOG_EXTERNAL_LEGACY_IMPORT_FAILURE", False)
DEBUG_ERRORS = _env_bool("DEBUG_ERRORS", False)

LEGACY_CONCURRENCY = max(1, min(25, _env_int("LEGACY_CONCURRENCY", 8)))
LEGACY_TIMEOUT_SEC = max(3.0, min(90.0, _env_float("LEGACY_TIMEOUT_SEC", 25.0)))
LEGACY_MAX_SYMBOLS = max(50, min(5000, _env_int("LEGACY_MAX_SYMBOLS", 2500)))

_external_loaded_from: Optional[str] = None


def _utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _riyadh_iso() -> str:
    tz = timezone(timedelta(hours=3))
    return datetime.now(tz).isoformat()


def _safe_mod_file(mod: Any) -> str:
    try:
        return str(getattr(mod, "__file__", "") or "")
    except Exception:
        return ""


def _looks_like_this_file(path: str) -> bool:
    p = (path or "").replace("\\", "/")
    return p.endswith("/core/legacy_service.py")


def _try_import_external_router() -> Optional[APIRouter]:
    """
    Optional override:
      - legacy_service.router
      - routes.legacy_service.router
    Guard against circular import.
    """
    global _external_loaded_from

    try:
        mod = importlib.import_module("legacy_service")
        if _looks_like_this_file(_safe_mod_file(mod)):
            raise RuntimeError("circular import: legacy_service points to core.legacy_service")
        r = getattr(mod, "router", None)
        if isinstance(r, APIRouter):
            _external_loaded_from = "legacy_service"
            return r
        raise RuntimeError("legacy_service.router missing/not APIRouter")
    except Exception as exc1:
        try:
            mod2 = importlib.import_module("routes.legacy_service")
            if _looks_like_this_file(_safe_mod_file(mod2)):
                raise RuntimeError("circular import: routes.legacy_service points to core.legacy_service")
            r2 = getattr(mod2, "router", None)
            if isinstance(r2, APIRouter):
                _external_loaded_from = "routes.legacy_service"
                return r2
            raise RuntimeError("routes.legacy_service.router missing/not APIRouter")
        except Exception as exc2:
            if LOG_EXTERNAL_IMPORT_FAILURE:
                try:
                    print(
                        "External legacy router not importable. Using internal router. "
                        f"errors=[{exc1.__class__.__name__}] / [{exc2.__class__.__name__}]"
                    )
                except Exception:
                    pass
            return None


router: APIRouter = APIRouter(prefix="/v1/legacy", tags=["legacy_compat"])


# ---------------------------------------------------------------------
# Models
# ---------------------------------------------------------------------
class SymbolsIn(BaseModel):
    symbols: List[str] = []
    tickers: List[str] = []

    def normalized(self) -> List[str]:
        items = self.symbols or self.tickers or []
        out: List[str] = []
        seen = set()
        for x in items[:LEGACY_MAX_SYMBOLS]:
            s = str(x or "").strip()
            if not s:
                continue
            su = s.upper()
            if su in seen:
                continue
            seen.add(su)
            out.append(s)
        return out


class SheetRowsIn(BaseModel):
    symbols: List[str] = []
    tickers: List[str] = []
    sheet_name: str = ""
    sheetName: str = ""


# ---------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------
def _safe_err(e: BaseException) -> str:
    msg = str(e).strip()
    return msg or e.__class__.__name__


async def _maybe_await(x: Any) -> Any:
    try:
        if inspect.isawaitable(x):
            return await x
    except Exception:
        pass
    return x


def _normalize_symbol_best_effort(sym: str) -> str:
    s = (sym or "").strip()
    if not s:
        return ""
    # conservative normalization: .SA -> .SR and uppercase
    su = s.upper().replace(" ", "")
    if su.endswith(".SA"):
        su = su[:-3] + ".SR"
    try:
        from core.data_engine_v2 import normalize_symbol  # type: ignore

        out = normalize_symbol(su)
        return str(out or "").strip() or su
    except Exception:
        return su


async def _close_engine_best_effort(engine: Any) -> None:
    if engine is None:
        return
    try:
        aclose = getattr(engine, "aclose", None)
        if callable(aclose):
            await _maybe_await(aclose())
            return
    except Exception:
        pass
    try:
        close = getattr(engine, "close", None)
        if callable(close):
            close()
    except Exception:
        pass


async def _get_engine_best_effort(request: Request) -> Tuple[Optional[Any], str, bool]:
    # 1) already attached
    try:
        eng = getattr(request.app.state, "engine", None)
    except Exception:
        eng = None
    if eng is not None:
        return eng, "app.state.engine", False

    # 2) v2 singleton getter
    try:
        from core.data_engine_v2 import get_engine as v2_get_engine  # type: ignore

        maybe_eng2 = v2_get_engine()
        eng2 = await _maybe_await(maybe_eng2)
        if eng2 is not None:
            try:
                request.app.state.engine = eng2
            except Exception:
                pass
            return eng2, "core.data_engine_v2.get_engine(singleton)", False
    except Exception:
        pass

    # 3) v2 temp engine (support multiple class names)
    try:
        mod = importlib.import_module("core.data_engine_v2")
        V2Engine = getattr(mod, "DataEngineV2", None) or getattr(mod, "DataEngine", None)
        if V2Engine is not None:
            eng3 = V2Engine()
            return eng3, "core.data_engine_v2.(DataEngineV2/DataEngine)(temp)", True
    except Exception:
        pass

    # 4) v1 temp engine
    try:
        from core.data_engine import DataEngine as V1Engine  # type: ignore

        eng4 = V1Engine()
        return eng4, "core.data_engine.DataEngine(temp)", True
    except Exception:
        return None, "none", False


async def _call_engine_method(
    engine: Any,
    method_names: Sequence[str],
    *args,
    **kwargs,
) -> Tuple[Optional[Any], str]:
    """
    Tries methods in order:
    - If method missing: continue
    - If method exists but fails: continue to next (keep last error)
    Returns: (result or None, used_method_or_error)
    """
    last_err = "missing"
    for name in method_names:
        fn = getattr(engine, name, None)
        if not callable(fn):
            continue
        try:
            res = fn(*args, **kwargs)
            res2 = await _maybe_await(res)
            return res2, name
        except Exception as e:
            last_err = f"{name} failed: {_safe_err(e)}"
            continue
    return None, last_err


def _sheet_name_from(payload: SheetRowsIn) -> str:
    return (payload.sheet_name or payload.sheetName or "").strip()


def _headers_master_default() -> List[str]:
    # A generous default covering the common "59+ columns" universe.
    return [
        # Identity
        "Rank", "Symbol", "Origin", "Name", "Sector", "Sub Sector", "Market", "Currency", "Listing Date",
        # Prices
        "Price", "Prev Close", "Change", "Change %", "Day High", "Day Low", "52W High", "52W Low", "52W Position %",
        # Liquidity
        "Volume", "Avg Vol 30D", "Value Traded", "Turnover %", "Shares Outstanding", "Free Float %", "Market Cap",
        "Free Float Mkt Cap", "Liquidity Score",
        # Fundamentals
        "EPS (TTM)", "Forward EPS", "P/E (TTM)", "Forward P/E", "P/B", "P/S", "EV/EBITDA",
        "Dividend Yield", "Dividend Rate", "Payout Ratio", "ROE", "ROA", "Net Margin", "EBITDA Margin",
        "Revenue Growth", "Net Income Growth", "Beta",
        # Technicals & Scores
        "Volatility 30D", "RSI 14", "Fair Value", "Upside %", "Valuation Label",
        "Value Score", "Quality Score", "Momentum Score", "Opportunity Score", "Risk Score", "Overall Score",
        "Recommendation", "Rec Badge",
        # Forecasts
        "Forecast Price (1M)", "Expected ROI % (1M)",
        "Forecast Price (3M)", "Expected ROI % (3M)",
        "Forecast Price (12M)", "Expected ROI % (12M)",
        "Forecast Confidence", "Forecast Updated (UTC)", "Forecast Updated (Riyadh)",
        # Metadata
        "News Score", "Data Quality", "Data Source", "Error", "Last Updated (UTC)", "Last Updated (Riyadh)",
    ]


def _headers_fallback(sheet_name: str) -> List[str]:
    try:
        from core.schemas import get_headers_for_sheet  # type: ignore

        if callable(get_headers_for_sheet) and sheet_name:
            h = get_headers_for_sheet(sheet_name)  # type: ignore
            if isinstance(h, list) and h:
                return [str(x) for x in h]
    except Exception:
        pass

    return _headers_master_default()


def _quote_dict(q: Any) -> Dict[str, Any]:
    if isinstance(q, dict):
        return q
    try:
        if hasattr(q, "model_dump"):
            return q.model_dump()  # type: ignore
        if hasattr(q, "dict"):
            return q.dict()  # type: ignore
        return dict(getattr(q, "__dict__", {}) or {})
    except Exception:
        return {}


def _extract_symbol_from_quote(q: Any) -> str:
    d = _quote_dict(q)
    s = d.get("symbol_normalized") or d.get("symbol") or d.get("ticker") or d.get("requested_symbol") or ""
    return str(s or "").strip().upper()


def _looks_like_percent_header(h: str) -> bool:
    s = (h or "").strip()
    if not s:
        return False
    if "%" in s:
        return True
    # common percent-ish fields without %
    s2 = s.lower()
    return s2.endswith("yield") or "margin" in s2 or s2.endswith("growth")


def _to_float_best(x: Any) -> Optional[float]:
    if x is None:
        return None
    if isinstance(x, bool):
        return None
    if isinstance(x, (int, float)):
        try:
            return float(x)
        except Exception:
            return None
    try:
        s = str(x).strip().replace(",", "")
        if not s or s.lower() in {"na", "n/a", "null", "none", "-", "—"}:
            return None
        if s.endswith("%"):
            s = s[:-1].strip()
        return float(s)
    except Exception:
        return None


def _coerce_for_header(header: str, val: Any) -> Any:
    """
    Smart coercion:
    - If header is percent-ish and value looks like ratio (-2..2), scale to percent (x100).
    - Keep non-numeric values as-is.
    """
    if val is None:
        return None

    # strings might be dates, keep if not numeric
    f = _to_float_best(val)
    if f is None:
        return val

    if _looks_like_percent_header(header):
        # ratio -> percent
        if -2.0 <= f <= 2.0:
            return f * 100.0
        return f

    return f


def _quote_to_row(q: Any, headers: List[str]) -> List[Any]:
    d = _quote_dict(q)

    def g(*keys: str) -> Any:
        for k in keys:
            if k in d:
                return d.get(k)
        return None

    # Advanced mapping (best-effort / stable)
    mapped: Dict[str, Any] = {
        # Identity
        "Rank": g("rank", "market_rank"),
        "Symbol": g("symbol_normalized", "symbol", "ticker", "requested_symbol"),
        "Origin": g("origin", "exchange", "source_sheet"),
        "Name": g("name", "company_name", "long_name", "short_name"),
        "Sector": g("sector", "sector_name"),
        "Sub Sector": g("sub_sector", "industry"),
        "Market": g("market", "market_region"),
        "Currency": g("currency", "currency_code"),
        "Listing Date": g("listing_date", "ipo_date"),

        # Prices
        "Price": g("current_price", "last_price", "price", "close"),
        "Prev Close": g("previous_close", "prev_close", "prior_close"),
        "Change": g("price_change", "change"),
        "Change %": g("percent_change", "change_pct", "change_percent"),
        "Day High": g("day_high", "high"),
        "Day Low": g("day_low", "low"),
        "52W High": g("week_52_high", "high_52w"),
        "52W Low": g("week_52_low", "low_52w"),
        "52W Position %": g("position_52w_percent", "position_52w"),

        # Liquidity
        "Volume": g("volume"),
        "Avg Vol 30D": g("avg_volume_30d", "avg_vol"),
        "Value Traded": g("value_traded", "turnover_value"),
        "Turnover %": g("turnover_percent"),
        "Shares Outstanding": g("shares_outstanding"),
        "Free Float %": g("free_float", "free_float_percent"),
        "Market Cap": g("market_cap"),
        "Free Float Mkt Cap": g("free_float_market_cap"),
        "Liquidity Score": g("liquidity_score"),

        # Fundamentals
        "EPS (TTM)": g("eps_ttm", "eps"),
        "Forward EPS": g("forward_eps"),
        "P/E (TTM)": g("pe_ttm", "pe"),
        "Forward P/E": g("forward_pe"),
        "P/B": g("pb", "price_to_book"),
        "P/S": g("ps", "price_to_sales"),
        "EV/EBITDA": g("ev_ebitda"),
        "Dividend Yield": g("dividend_yield"),
        "Dividend Rate": g("dividend_rate"),
        "Payout Ratio": g("payout_ratio"),
        "ROE": g("roe"),
        "ROA": g("roa"),
        "Net Margin": g("net_margin", "profit_margin"),
        "EBITDA Margin": g("ebitda_margin"),
        "Revenue Growth": g("revenue_growth"),
        "Net Income Growth": g("net_income_growth"),
        "Beta": g("beta"),

        # Technicals & Scores
        "Volatility 30D": g("volatility_30d", "volatility (30d)", "volatility_30d_ratio"),
        "RSI 14": g("rsi_14", "rsi (14)"),
        "Fair Value": g("fair_value", "intrinsic_value"),
        "Upside %": g("upside_percent"),
        "Valuation Label": g("valuation_label"),
        "Value Score": g("value_score"),
        "Quality Score": g("quality_score"),
        "Momentum Score": g("momentum_score"),
        "Opportunity Score": g("opportunity_score"),
        "Risk Score": g("risk_score"),
        "Overall Score": g("overall_score"),
        "Recommendation": g("recommendation"),
        "Rec Badge": g("rec_badge"),

        # Forecasts
        "Forecast Price (1M)": g("forecast_price_1m", "target_price_1m"),
        "Expected ROI % (1M)": g("expected_roi_1m", "roi_1m"),
        "Forecast Price (3M)": g("forecast_price_3m", "target_price_3m"),
        "Expected ROI % (3M)": g("expected_roi_3m", "roi_3m"),
        "Forecast Price (12M)": g("forecast_price_12m", "target_price_12m"),
        "Expected ROI % (12M)": g("expected_roi_12m", "roi_12m"),
        "Forecast Confidence": g("forecast_confidence"),
        "Forecast Updated (UTC)": g("forecast_updated_utc"),
        "Forecast Updated (Riyadh)": g("forecast_updated_riyadh"),

        # Metadata
        "News Score": g("news_score", "news_sentiment", "sentiment_score", "news_boost"),
        "Data Quality": g("data_quality"),
        "Data Source": g("data_source", "source", "provider"),
        "Error": g("error"),

        "Last Updated (UTC)": g("last_updated_utc", "as_of_utc"),
        "Last Updated (Riyadh)": g("last_updated_riyadh"),
    }

    # Ensure updated timestamps
    if not mapped.get("Last Updated (UTC)"):
        mapped["Last Updated (UTC)"] = _utc_iso()
    if not mapped.get("Last Updated (Riyadh)"):
        mapped["Last Updated (Riyadh)"] = _riyadh_iso()

    row: List[Any] = []
    for h in headers:
        # 1) exact mapped
        if h in mapped:
            v = mapped[h]
        else:
            # 2) case-insensitive match in mapped keys
            h_low = (h or "").strip().lower()
            v = None
            for k, vv in mapped.items():
                if (k or "").strip().lower() == h_low:
                    v = vv
                    break

            # 3) raw dict fallbacks
            if v is None:
                v = d.get(h)
                if v is None:
                    v = d.get(h_low.replace(" ", "_"))

        row.append(_coerce_for_header(h, v))

    return row


def _items_to_ordered_list(items: Any, symbols: List[str]) -> List[Any]:
    if items is None:
        return []

    if isinstance(items, list):
        sym_map: Dict[str, Any] = {}
        for it in items:
            k = _extract_symbol_from_quote(it)
            if k and k not in sym_map:
                sym_map[k] = it
        if sym_map:
            ordered = [sym_map.get(str(s or "").strip().upper()) for s in symbols]
            if not all(x is None for x in ordered):
                return ordered
        return items

    if isinstance(items, dict):
        mp: Dict[str, Any] = {}
        for k, v in items.items():
            kk = str(k or "").strip().upper()
            if kk:
                mp[kk] = v
            s2 = _extract_symbol_from_quote(v)
            if s2 and s2 not in mp:
                mp[s2] = v

        ordered: List[Any] = []
        for s in symbols:
            su = str(s or "").strip().upper()
            ordered.append(mp.get(su))

        if all(x is None for x in ordered):
            return list(items.values())
        return ordered

    if len(symbols) == 1:
        return [items]

    return []


async def _try_cache_snapshot(engine: Any, sheet_name: str, headers: List[str], rows: List[List[Any]], meta: Dict[str, Any]) -> None:
    """
    Bridge: store snapshot for later use by Investment Advisor universe fetch.
    No-op if engine doesn't support it.
    """
    if engine is None or not sheet_name or not headers:
        return
    fn = getattr(engine, "set_cached_sheet_snapshot", None)
    if callable(fn):
        try:
            res = fn(sheet_name, headers, rows, meta)  # type: ignore[misc]
            await _maybe_await(res)
        except Exception:
            pass


# ---------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------
@router.get("/health", summary="Legacy compatibility health")
async def legacy_health(request: Request):
    eng = getattr(request.app.state, "engine", None)
    info: Dict[str, Any] = {
        "ok": True,
        "status": "ok",
        "router": "core.legacy_service",
        "version": VERSION,
        "mode": "internal",
        "engine_present": eng is not None,
        "external_router_enabled": ENABLE_EXTERNAL_LEGACY_ROUTER,
        "legacy_concurrency": LEGACY_CONCURRENCY,
        "legacy_timeout_sec": LEGACY_TIMEOUT_SEC,
        "legacy_max_symbols": LEGACY_MAX_SYMBOLS,
    }

    try:
        from core.data_engine_v2 import ENGINE_VERSION as V2_ENGINE_VERSION  # type: ignore

        info["engine_version"] = V2_ENGINE_VERSION
    except Exception:
        info["engine_version"] = "unknown"

    if _external_loaded_from:
        info["external_loaded_from"] = _external_loaded_from

    if eng is not None:
        try:
            info["engine_class"] = eng.__class__.__name__
            info["engine_module"] = eng.__class__.__module__
        except Exception:
            pass

    try:
        e2, src, should_close = await _get_engine_best_effort(request)
        info["engine_resolve_source"] = src
        info["engine_resolved"] = bool(e2 is not None)
        info["engine_temp_should_close"] = bool(should_close)
        if should_close and e2 is not None:
            await _close_engine_best_effort(e2)
    except Exception:
        info["engine_resolve_source"] = "error"
        info["engine_resolved"] = False

    return info


@router.get("/quote", summary="Legacy quote endpoint (UnifiedQuote)")
async def legacy_quote(
    request: Request,
    symbol: str = Query(..., min_length=1),
    debug: int = Query(0, description="Set 1 to include traceback (or enable DEBUG_ERRORS=1)"),
):
    dbg = DEBUG_ERRORS or bool(debug)
    raw = (symbol or "").strip()
    sym = _normalize_symbol_best_effort(raw)

    eng = None
    src = "none"
    should_close = False

    try:
        eng, src, should_close = await _get_engine_best_effort(request)
        if eng is None:
            out = {
                "status": "error",
                "symbol": raw,
                "symbol_normalized": sym,
                "data_quality": "MISSING",
                "data_source": "none",
                "error": "Legacy engine not available (no working provider).",
            }
            return JSONResponse(status_code=200, content=jsonable_encoder(out))

        q, used = await _call_engine_method(eng, ("get_quote", "get_enriched_quote"), sym)
        if q is None:
            out = {
                "status": "error",
                "symbol": raw,
                "symbol_normalized": sym,
                "data_quality": "MISSING",
                "data_source": "none",
                "error": f"Engine call failed (source={src}, detail={used}).",
            }
            if dbg:
                out["engine_source"] = src
                out["method_detail"] = used
            return JSONResponse(status_code=200, content=jsonable_encoder(out))

        return JSONResponse(status_code=200, content=jsonable_encoder(q))

    except Exception as e:
        out: Dict[str, Any] = {
            "status": "error",
            "symbol": raw,
            "symbol_normalized": sym,
            "data_quality": "MISSING",
            "data_source": "none",
            "error": _safe_err(e),
        }
        if dbg:
            out["traceback"] = traceback.format_exc()[:8000]
            out["engine_source"] = src
        return JSONResponse(status_code=200, content=jsonable_encoder(out))

    finally:
        if should_close and eng is not None:
            try:
                await _close_engine_best_effort(eng)
            except Exception:
                pass


@router.post("/quotes", summary="Legacy batch quotes endpoint (list[UnifiedQuote])")
async def legacy_quotes(
    request: Request,
    payload: SymbolsIn,
    debug: int = Query(0, description="Set 1 to include traceback (or enable DEBUG_ERRORS=1)"),
):
    dbg = DEBUG_ERRORS or bool(debug)

    raw_symbols = payload.normalized()
    if not raw_symbols:
        return JSONResponse(status_code=200, content=jsonable_encoder([]))

    symbols = [_normalize_symbol_best_effort(s) for s in raw_symbols]

    eng = None
    src = "none"
    should_close = False

    try:
        eng, src, should_close = await _get_engine_best_effort(request)
        if eng is None:
            out = [
                {"status": "error", "symbol": s, "data_quality": "MISSING", "data_source": "none",
                 "error": "Legacy engine not available (no working provider)."}
                for s in raw_symbols
            ]
            return JSONResponse(status_code=200, content=jsonable_encoder(out))

        # try batch
        try:
            items, used = await asyncio.wait_for(
                _call_engine_method(eng, ("get_quotes", "get_enriched_quotes"), symbols),
                timeout=LEGACY_TIMEOUT_SEC,
            )
        except asyncio.TimeoutError:
            items, used = None, "get_quotes timeout"

        if items is not None:
            ordered = _items_to_ordered_list(items, symbols)
            if ordered:
                return JSONResponse(status_code=200, content=jsonable_encoder(ordered))
            if isinstance(items, list):
                return JSONResponse(status_code=200, content=jsonable_encoder(items))
            items = None

        # per-symbol fallback
        sem = asyncio.Semaphore(LEGACY_CONCURRENCY)

        async def _one(sym_i: str, raw_i: str) -> Any:
            async with sem:
                try:
                    q_i, used_i = await asyncio.wait_for(
                        _call_engine_method(eng, ("get_quote", "get_enriched_quote"), sym_i),
                        timeout=LEGACY_TIMEOUT_SEC,
                    )
                    if q_i is None:
                        return {
                            "status": "error",
                            "symbol": raw_i,
                            "symbol_normalized": sym_i,
                            "data_quality": "MISSING",
                            "data_source": "none",
                            "error": f"Engine call failed (source={src}, detail={used_i}).",
                        }
                    return q_i
                except asyncio.TimeoutError:
                    return {"status": "error", "symbol": raw_i, "symbol_normalized": sym_i,
                            "data_quality": "MISSING", "data_source": "none", "error": "timeout"}
                except Exception as ee:
                    return {"status": "error", "symbol": raw_i, "symbol_normalized": sym_i,
                            "data_quality": "MISSING", "data_source": "none", "error": _safe_err(ee)}

        results = await asyncio.gather(*[_one(symbols[i], raw_symbols[i]) for i in range(len(symbols))])
        if dbg:
            results.append({"debug": True, "engine_source": src, "fallback": "per_symbol"})
        return JSONResponse(status_code=200, content=jsonable_encoder(results))

    except Exception as e:
        out = [{"status": "error", "symbol": s, "data_quality": "MISSING", "data_source": "none", "error": _safe_err(e)} for s in raw_symbols]
        if dbg:
            out.append({"debug": True, "traceback": traceback.format_exc()[:8000], "engine_source": src})
        return JSONResponse(status_code=200, content=jsonable_encoder(out))

    finally:
        if should_close and eng is not None:
            try:
                await _close_engine_best_effort(eng)
            except Exception:
                pass


@router.post("/sheet-rows", summary="Legacy sheet-rows helper (headers + rows)")
async def legacy_sheet_rows(
    request: Request,
    payload: SheetRowsIn,
    debug: int = Query(0, description="Set 1 to include traceback (or enable DEBUG_ERRORS=1)"),
):
    dbg = DEBUG_ERRORS or bool(debug)

    eng = None
    src = "none"
    should_close = False

    try:
        symbols_in = SymbolsIn(symbols=payload.symbols or [], tickers=payload.tickers or [])
        raw_symbols = symbols_in.normalized()
        if not raw_symbols:
            return JSONResponse(
                status_code=200,
                content=jsonable_encoder({"status": "skipped", "headers": [], "rows": [], "error": "No symbols provided"}),
            )

        symbols = [_normalize_symbol_best_effort(s) for s in raw_symbols]

        sheet_name = _sheet_name_from(payload)
        headers = _headers_fallback(sheet_name)

        eng, src, should_close = await _get_engine_best_effort(request)
        if eng is None:
            rows = [[s, None, None, None, None, None, None, None, None, None, "MISSING", "none", "Legacy engine not available"] for s in raw_symbols]
            return JSONResponse(
                status_code=200,
                content=jsonable_encoder({"status": "error", "headers": headers, "rows": rows, "error": "Legacy engine not available", "engine_source": src}),
            )

        # batch quotes first (preferred)
        try:
            items, used = await asyncio.wait_for(
                _call_engine_method(eng, ("get_quotes", "get_enriched_quotes"), symbols),
                timeout=LEGACY_TIMEOUT_SEC,
            )
        except asyncio.TimeoutError:
            items, used = None, "get_quotes timeout"

        if items is None:
            # per symbol
            sem = asyncio.Semaphore(LEGACY_CONCURRENCY)

            async def _one(sym_i: str) -> Any:
                async with sem:
                    try:
                        q_i, _used_i = await asyncio.wait_for(
                            _call_engine_method(eng, ("get_quote", "get_enriched_quote"), sym_i),
                            timeout=LEGACY_TIMEOUT_SEC,
                        )
                        return q_i
                    except Exception:
                        return None

            items = await asyncio.gather(*[_one(s) for s in symbols])
            used = "per_symbol_fallback"

        ordered_items = _items_to_ordered_list(items, symbols)

        if not isinstance(ordered_items, list) or not ordered_items:
            rows = [[s, None, None, None, None, None, None, None, None, None, "MISSING", "none", f"Engine returned non-list (detail={used})"] for s in raw_symbols]
            out = {"status": "error", "headers": headers, "rows": rows, "error": "Engine returned non-list", "engine_source": src, "method": used}
            if dbg:
                out["debug"] = True
            return JSONResponse(status_code=200, content=jsonable_encoder(out))

        rows = [_quote_to_row(q, headers) if q is not None else _quote_to_row({}, headers) for q in ordered_items]

        # Snapshot Bridge: cache this sheet page for later (Investment Advisor universe)
        snap_meta = {
            "router": "core.legacy_service",
            "version": VERSION,
            "engine_source": src,
            "method": used,
            "cached_at_utc": _utc_iso(),
        }
        await _try_cache_snapshot(eng, sheet_name, headers, rows, snap_meta)

        out = {
            "status": "success",
            "headers": headers,
            "rows": rows,
            "count": len(rows),
            "engine_source": src,
            "method": used,
        }
        if dbg:
            out["debug"] = True
            out["sheet_name"] = sheet_name
        return JSONResponse(status_code=200, content=jsonable_encoder(out))

    except Exception as e:
        out: Dict[str, Any] = {"status": "error", "headers": _headers_master_default(), "rows": [], "error": _safe_err(e)}
        if dbg:
            out["traceback"] = traceback.format_exc()[:8000]
            out["engine_source"] = src
        return JSONResponse(status_code=200, content=jsonable_encoder(out))

    finally:
        if should_close and eng is not None:
            try:
                await _close_engine_best_effort(eng)
            except Exception:
                pass


# ---------------------------------------------------------------------
# Optional external override router
# ---------------------------------------------------------------------
if ENABLE_EXTERNAL_LEGACY_ROUTER:
    ext = _try_import_external_router()
    if ext is not None:
        router = ext  # type: ignore


def get_router() -> APIRouter:
    return router


__all__ = ["router", "get_router", "VERSION"]
