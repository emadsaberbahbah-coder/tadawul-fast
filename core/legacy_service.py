"""
core/legacy_service.py
------------------------------------------------------------
Compatibility shim (quiet + useful) — v1.8.0 (PROD SAFE + ADVANCED MAPPING)

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

v1.8.0 Improvements:
- ✅ Expanded Header Mapping: Supports all 59 columns (Forecasts, ROI, Scores, Technicals).
- ✅ Riyadh Time Support: Explicitly maps Riyadh timestamps.
- ✅ Robust Fallbacks: Better handling of missing engine methods.
"""

from __future__ import annotations

import asyncio
import importlib
import inspect
import os
import traceback
from typing import Any, Dict, List, Optional, Sequence, Tuple

from fastapi import APIRouter, Query, Request
from fastapi.encoders import jsonable_encoder
from pydantic import BaseModel
from starlette.responses import JSONResponse

VERSION = "1.8.0"

_TRUTHY = {"1", "true", "yes", "y", "on", "t"}


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

LEGACY_CONCURRENCY = max(1, min(25, _env_int("LEGACY_CONCURRENCY", 8)))
LEGACY_TIMEOUT_SEC = max(3.0, min(90.0, _env_float("LEGACY_TIMEOUT_SEC", 25.0)))

_external_loaded_from: Optional[str] = None


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
            # If routes.legacy_service is only a shim (expected), do NOT treat it as external override.
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
        for x in items:
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
    try:
        from core.data_engine_v2 import normalize_symbol  # type: ignore

        out = normalize_symbol(s)
        return str(out or "").strip() or s
    except Exception:
        return s


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


async def _call_engine_method(engine: Any, method_names: Sequence[str], *args, **kwargs) -> Tuple[Optional[Any], str]:
    for name in method_names:
        fn = getattr(engine, name, None)
        if callable(fn):
            try:
                res = fn(*args, **kwargs)
                res2 = await _maybe_await(res)
                return res2, name
            except Exception as e:
                return None, f"{name} failed: {_safe_err(e)}"
    return None, "missing"


def _sheet_name_from(payload: SheetRowsIn) -> str:
    return (payload.sheet_name or payload.sheetName or "").strip()


def _headers_fallback(sheet_name: str) -> List[str]:
    try:
        from core.schemas import get_headers_for_sheet  # type: ignore

        if callable(get_headers_for_sheet) and sheet_name:
            h = get_headers_for_sheet(sheet_name)  # type: ignore
            if isinstance(h, list) and h:
                return [str(x) for x in h]
    except Exception:
        pass

    # Fallback to a generous default if schemas missing
    return [
        "Symbol",
        "Name",
        "Market",
        "Currency",
        "Price",
        "Change",
        "Change %",
        "Volume",
        "Market Cap",
        "P/E (TTM)",
        "Data Quality",
        "Data Source",
        "Error",
    ]


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
    s = d.get("symbol_normalized") or d.get("symbol") or d.get("ticker") or ""
    return str(s or "").strip().upper()


def _quote_to_row(q: Any, headers: List[str]) -> List[Any]:
    d = _quote_dict(q)

    # Robust getter to handle multiple keys and safe string/float handling
    def g(*keys: str) -> Any:
        for k in keys:
            if k in d:
                return d.get(k)
        return None
    
    # Advanced mapping (Aligned with v5.8.0 Sheet Controller)
    mapped: Dict[str, Any] = {
        # Identity
        "Rank": g("rank", "market_rank"),
        "Symbol": g("symbol", "symbol_normalized", "ticker"),
        "Origin": g("origin", "exchange"),
        "Name": g("name", "company_name"),
        "Sector": g("sector", "sector_name"),
        "Sub Sector": g("sub_sector", "industry"),
        "Market": g("market", "market_region"),
        "Currency": g("currency", "currency_code"),
        "Listing Date": g("listing_date", "ipo_date"),

        # Prices
        "Price": g("current_price", "last_price", "price"),
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
        "Value Traded": g("value_traded"),
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
        "Volatility 30D": g("volatility_30d"),
        "RSI 14": g("rsi_14"),
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
        "Data Quality": g("data_quality"),
        "Data Source": g("data_source", "source", "provider"),
        "Error": g("error"),
        "Last Updated (UTC)": g("last_updated_utc", "as_of_utc"),
        "Last Updated (Riyadh)": g("last_updated_riyadh"),
    }

    row: List[Any] = []
    for h in headers:
        # Case-insensitive lookup in mapped dict
        found = False
        target_val = None
        
        # 1. Exact match
        if h in mapped:
            target_val = mapped[h]
            found = True
        else:
            # 2. Case-insensitive match
            h_low = h.lower().strip()
            for k, v in mapped.items():
                if k.lower().strip() == h_low:
                    target_val = v
                    found = True
                    break
        
        # 3. Direct lookup in quote dict (fallback)
        if not found:
             target_val = d.get(h) or d.get(h.lower().replace(" ", "_"))

        row.append(target_val)
        
    return row


def _items_to_ordered_list(items: Any, symbols: List[str]) -> List[Any]:
    if items is None:
        return []

    if isinstance(items, list):
        # attempt to align list by symbol if list items have symbol fields
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
    }

    try:
        from core.data_engine_v2 import ENGINE_VERSION  # type: ignore

        info["engine_version"] = ENGINE_VERSION
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
    dbg = _env_bool("DEBUG_ERRORS", False) or bool(debug)
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
                "error": f"Engine call failed (source={src}, method={used}).",
            }
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
    dbg = _env_bool("DEBUG_ERRORS", False) or bool(debug)

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
                {"status": "error", "symbol": s, "data_quality": "MISSING", "data_source": "none", "error": "Legacy engine not available (no working provider)."}
                for s in raw_symbols
            ]
            return JSONResponse(status_code=200, content=jsonable_encoder(out))

        items, used = await _call_engine_method(eng, ("get_quotes", "get_enriched_quotes"), symbols)
        if items is not None:
            ordered = _items_to_ordered_list(items, symbols)
            if ordered:
                return JSONResponse(status_code=200, content=jsonable_encoder(ordered))
            if isinstance(items, list):
                return JSONResponse(status_code=200, content=jsonable_encoder(items))
            items = None

        sem = asyncio.Semaphore(LEGACY_CONCURRENCY)

        async def _one(sym_i: str, raw_i: str) -> Any:
            async with sem:
                try:
                    q_i, used_i = await asyncio.wait_for(
                        _call_engine_method(eng, ("get_quote", "get_enriched_quote"), sym_i),
                        timeout=LEGACY_TIMEOUT_SEC,
                    )
                    if q_i is None:
                        return {"status": "error", "symbol": raw_i, "data_quality": "MISSING", "data_source": "none", "error": f"Engine call failed (source={src}, method={used_i})."}
                    return q_i
                except asyncio.TimeoutError:
                    return {"status": "error", "symbol": raw_i, "data_quality": "MISSING", "data_source": "none", "error": "timeout"}
                except Exception as ee:
                    return {"status": "error", "symbol": raw_i, "data_quality": "MISSING", "data_source": "none", "error": _safe_err(ee)}

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
    dbg = _env_bool("DEBUG_ERRORS", False) or bool(debug)

    eng = None
    src = "none"
    should_close = False

    try:
        symbols_in = SymbolsIn(symbols=payload.symbols or [], tickers=payload.tickers or [])
        raw_symbols = symbols_in.normalized()
        if not raw_symbols:
            return JSONResponse(status_code=200, content=jsonable_encoder({"status": "skipped", "headers": [], "rows": [], "error": "No symbols provided"}))

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

        items, used = await _call_engine_method(eng, ("get_quotes", "get_enriched_quotes"), symbols)

        if items is None:
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
            rows = [[s, None, None, None, None, None, None, None, None, None, "MISSING", "none", f"Engine returned non-list (method={used})"] for s in raw_symbols]
            return JSONResponse(
                status_code=200,
                content=jsonable_encoder({"status": "error", "headers": headers, "rows": rows, "error": "Engine returned non-list", "engine_source": src, "method": used}),
            )

        rows = [_quote_to_row(q, headers) if q is not None else _quote_to_row({}, headers) for q in ordered_items]
        return JSONResponse(
            status_code=200,
            content=jsonable_encoder({"status": "success", "headers": headers, "rows": rows, "count": len(rows), "engine_source": src, "method": used}),
        )

    except Exception as e:
        out: Dict[str, Any] = {"status": "error", "headers": [], "rows": [], "error": _safe_err(e)}
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


__all__ = ["router", "get_router"]
