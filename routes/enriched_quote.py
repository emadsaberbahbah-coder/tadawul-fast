# routes/enriched_quote.py
"""
routes/enriched_quote.py
------------------------------------------------------------
Enriched Quote Router – PROD SAFE + DEBUG + KSA FUNDAMENTALS SUPPLEMENT

✅ Always returns HTTP 200 with a status field.
✅ Uses app.state.engine when available; safe fallbacks.
✅ Supports debug=1 (adds a debug block, never leaks tokens).
✅ KSA fundamentals supplement via Yahoo quoteSummary when enabled:
   - ENABLE_YAHOO_FUNDAMENTALS_KSA=true  (default true in your main.py)
   - Fills: market_cap, shares_outstanding, eps_ttm, pe_ttm, pb, dividend_yield, roe, roa
✅ Batch endpoint: /v1/enriched/quotes?symbols=AAPL,1120.SR
"""

from __future__ import annotations

import json
import os
import time
import traceback
from importlib import import_module
from typing import Any, Dict, List, Optional, Tuple

from fastapi import APIRouter, Request

ROUTE_VERSION = "5.0.0"

router = APIRouter(prefix="/v1/enriched", tags=["enriched"])

_TRUTHY = {"1", "true", "yes", "y", "on", "t"}


def _truthy(v: Any) -> bool:
    return str(v or "").strip().lower() in _TRUTHY


def _clamp(s: Any, n: int = 4000) -> str:
    txt = (str(s) if s is not None else "").strip()
    if not txt:
        return ""
    return txt if len(txt) <= n else (txt[: n - 12] + " ...TRUNC...")


def _to_dict(x: Any) -> Dict[str, Any]:
    if x is None:
        return {}
    if isinstance(x, dict):
        return x
    # pydantic v2
    md = getattr(x, "model_dump", None)
    if callable(md):
        try:
            return md()
        except Exception:
            pass
    # pydantic v1
    d = getattr(x, "dict", None)
    if callable(d):
        try:
            return d()
        except Exception:
            pass
    # best effort
    try:
        return dict(x)  # type: ignore[arg-type]
    except Exception:
        return {"value": str(x)}


def _get_env(name: str, default: Optional[str] = None) -> Optional[str]:
    v = os.getenv(name)
    if v is None:
        v = os.getenv(name.upper())
    if v is None:
        return default
    s = str(v).strip()
    return s or default


def _get_auth_token_from_request(req: Request) -> Optional[str]:
    # common patterns: X-API-Key or Authorization: Bearer <token>
    h = req.headers
    x = (h.get("x-api-key") or h.get("X-API-Key") or "").strip()
    if x:
        return x
    auth = (h.get("authorization") or h.get("Authorization") or "").strip()
    if auth.lower().startswith("bearer "):
        return auth.split(" ", 1)[1].strip()
    return None


def _auth_required(req: Request) -> bool:
    # Prefer Settings if available
    s = getattr(req.app.state, "settings", None)
    for k in ("require_auth", "REQUIRE_AUTH"):
        try:
            v = getattr(s, k, None) if s is not None else None
            if v is not None:
                return _truthy(v) if isinstance(v, str) else bool(v)
        except Exception:
            pass
    return _truthy(_get_env("REQUIRE_AUTH", "false"))


def _check_auth(req: Request) -> Tuple[bool, Optional[str]]:
    if not _auth_required(req):
        return True, None

    token = _get_auth_token_from_request(req)
    if not token:
        return False, "Missing API token"

    valid = []
    for k in ("APP_TOKEN", "TFB_APP_TOKEN", "BACKUP_APP_TOKEN"):
        vv = (_get_env(k) or "").strip()
        if vv:
            valid.append(vv)

    if not valid:
        # If require_auth is true but no tokens are set, fail closed
        return False, "Auth is enabled but no valid tokens are configured"

    if token in valid:
        return True, None

    return False, "Invalid API token"


def _get_engine(req: Request) -> Tuple[Optional[Any], str, Optional[str]]:
    # 1) preferred: app.state.engine
    try:
        eng = getattr(req.app.state, "engine", None)
        if eng is not None:
            return eng, "app.state.engine", None
    except Exception:
        pass

    # 2) try v2 engine module
    for mod_path in ("core.data_engine_v2", "data_engine_v2", "core.data_engine"):
        try:
            mod = import_module(mod_path)
            Engine = getattr(mod, "DataEngine", None)
            if Engine is not None:
                return Engine(), mod_path + ".DataEngine()", None
        except Exception:
            last = traceback.format_exc()
            continue

    return None, "none", _clamp(last if "last" in locals() else "Engine import failed", 8000)


def _call_engine_quote(eng: Any, symbol: str, debug: bool) -> Dict[str, Any]:
    # support multiple method names across versions
    for m in ("get_enriched_quote", "enriched_quote", "quote", "get_quote"):
        fn = getattr(eng, m, None)
        if callable(fn):
            try:
                # some engines accept debug kw
                return _to_dict(fn(symbol=symbol, debug=debug))  # type: ignore[misc]
            except TypeError:
                return _to_dict(fn(symbol))  # type: ignore[misc]
    raise RuntimeError("Engine has no quote method")


def _call_engine_quotes(eng: Any, symbols: List[str], debug: bool) -> List[Dict[str, Any]]:
    for m in ("get_enriched_quotes", "enriched_quotes", "quotes", "get_quotes"):
        fn = getattr(eng, m, None)
        if callable(fn):
            try:
                out = fn(symbols=symbols, debug=debug)  # type: ignore[misc]
            except TypeError:
                out = fn(symbols)  # type: ignore[misc]
            if isinstance(out, dict) and "items" in out and isinstance(out["items"], list):
                return [(_to_dict(x)) for x in out["items"]]
            if isinstance(out, list):
                return [(_to_dict(x)) for x in out]
            return [_to_dict(out)]
    # fallback: loop single
    items: List[Dict[str, Any]] = []
    for s in symbols:
        try:
            items.append(_call_engine_quote(eng, s, debug))
        except Exception as e:
            items.append(
                {
                    "status": "error",
                    "symbol": s,
                    "error": f"engine_error: {e}",
                }
            )
    return items


# -----------------------------------------------------------------------------
# Yahoo fundamentals supplement (KSA)
# -----------------------------------------------------------------------------
_YF_CACHE: Dict[str, Tuple[float, Dict[str, Any]]] = {}
_YF_TTL_SEC = 6 * 60 * 60  # 6 hours


def _yf_get_json(url: str, timeout: float = 12.0) -> Dict[str, Any]:
    # Prefer httpx, fallback to requests, then urllib
    try:
        import httpx  # type: ignore

        with httpx.Client(timeout=timeout, headers={"User-Agent": "Mozilla/5.0"}) as c:
            r = c.get(url)
            r.raise_for_status()
            return r.json()
    except Exception:
        pass

    try:
        import requests  # type: ignore

        r = requests.get(url, timeout=timeout, headers={"User-Agent": "Mozilla/5.0"})
        r.raise_for_status()
        return r.json()
    except Exception:
        pass

    import urllib.request

    req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        raw = resp.read().decode("utf-8", errors="replace")
        return json.loads(raw)


def _pick_raw(node: Any) -> Optional[float]:
    # yahoo returns {"raw": x, "fmt": "..."} style
    if node is None:
        return None
    if isinstance(node, (int, float)):
        return float(node)
    if isinstance(node, dict):
        v = node.get("raw", None)
        if isinstance(v, (int, float)):
            return float(v)
    return None


def _yahoo_fundamentals(symbol: str) -> Tuple[Optional[Dict[str, Any]], Optional[str]]:
    sym = (symbol or "").strip()
    if not sym:
        return None, "empty symbol"

    now = time.time()
    cached = _YF_CACHE.get(sym)
    if cached and (now - cached[0]) < _YF_TTL_SEC:
        return cached[1], None

    # quoteSummary endpoint
    modules = "price,defaultKeyStatistics,summaryDetail,financialData"
    url = f"https://query2.finance.yahoo.com/v10/finance/quoteSummary/{sym}?modules={modules}"

    try:
        js = _yf_get_json(url)
        res = (((js or {}).get("quoteSummary") or {}).get("result") or [])
        if not res:
            return None, "yahoo quoteSummary empty result"
        obj = res[0] or {}

        price = obj.get("price") or {}
        dks = obj.get("defaultKeyStatistics") or {}
        sd = obj.get("summaryDetail") or {}
        fd = obj.get("financialData") or {}

        out: Dict[str, Any] = {
            "market_cap": _pick_raw(price.get("marketCap")) or _pick_raw(dks.get("marketCap")),
            "shares_outstanding": _pick_raw(dks.get("sharesOutstanding")) or _pick_raw(price.get("sharesOutstanding")),
            "eps_ttm": _pick_raw(dks.get("trailingEps")),
            "pe_ttm": _pick_raw(dks.get("trailingPE")) or _pick_raw(sd.get("trailingPE")),
            "pb": _pick_raw(dks.get("priceToBook")),
            "dividend_yield": _pick_raw(sd.get("dividendYield")),
            "roe": _pick_raw(fd.get("returnOnEquity")),
            "roa": _pick_raw(fd.get("returnOnAssets")),
        }

        # strip None
        out = {k: v for k, v in out.items() if v is not None}

        _YF_CACHE[sym] = (now, out)
        return out, None
    except Exception as e:
        return None, f"yahoo_fundamentals_error: {_clamp(e, 800)}"


def _needs_ksa_fundamentals(d: Dict[str, Any]) -> bool:
    if not isinstance(d, dict):
        return False
    if str(d.get("market") or "").upper() != "KSA":
        return False
    # only if major fields missing
    keys = ["market_cap", "shares_outstanding", "eps_ttm", "pe_ttm", "pb", "dividend_yield", "roe", "roa"]
    missing = 0
    for k in keys:
        if d.get(k, None) in (None, "", 0):
            missing += 1
    return missing >= 4  # avoid calling yahoo if only 1 thing missing


def _supplement_ksa_fundamentals_if_enabled(d: Dict[str, Any]) -> Tuple[Dict[str, Any], Optional[str], bool]:
    enabled = _truthy(_get_env("ENABLE_YAHOO_FUNDAMENTALS_KSA", "true"))
    if not enabled:
        return d, None, False

    if not _needs_ksa_fundamentals(d):
        return d, None, False

    sym = str(d.get("symbol") or "").strip()
    if not sym:
        return d, "missing symbol for fundamentals supplement", False

    fx, err = _yahoo_fundamentals(sym)
    if not fx:
        return d, err or "no fundamentals returned", False

    # fill only missing fields
    for k, v in fx.items():
        if d.get(k, None) in (None, "", 0):
            d[k] = v

    # mark source
    src = str(d.get("data_source") or "").strip()
    if "yahoo_fundamentals" not in src.lower():
        d["data_source"] = (src + ",yahoo_fundamentals").strip(",") if src else "yahoo_fundamentals"

    return d, None, True


def _normalize_symbols_csv(s: str) -> List[str]:
    parts = [p.strip() for p in (s or "").split(",") if p.strip()]
    # de-dup preserve
    out: List[str] = []
    seen = set()
    for x in parts:
        if x not in seen:
            seen.add(x)
            out.append(x)
    return out


@router.get("/quote")
async def enriched_quote(request: Request, symbol: str, debug: int = 0) -> Dict[str, Any]:
    ok, err = _check_auth(request)
    if not ok:
        return {"status": "error", "symbol": symbol, "error": err}

    dbg = bool(debug)
    eng, eng_src, eng_err = _get_engine(request)

    if eng is None:
        out = {"status": "error", "symbol": symbol, "error": "engine_unavailable"}
        if dbg:
            out["debug"] = {
                "route_version": ROUTE_VERSION,
                "engine_source": eng_src,
                "engine_error": eng_err,
            }
        return out

    try:
        out = _call_engine_quote(eng, symbol, dbg)
        if not isinstance(out, dict) or not out:
            out = {"status": "error", "symbol": symbol, "error": "null_response"}
        # KSA supplement
        sup_err = None
        sup_used = False
        try:
            out, sup_err, sup_used = _supplement_ksa_fundamentals_if_enabled(out)
            if sup_err:
                # append warning safely
                cur = str(out.get("error") or "").strip()
                warn = f"warning: {sup_err}"
                out["error"] = (cur + " | " + warn).strip(" |") if cur else warn
        except Exception:
            pass

        if dbg:
            out["debug"] = {
                "route_version": ROUTE_VERSION,
                "engine_source": eng_src,
                "engine_type": type(eng).__name__,
                "engine_version": getattr(eng, "ENGINE_VERSION", None) or getattr(eng, "engine_version", None),
                "supplement_used": sup_used,
            }
        return out
    except Exception:
        tb = traceback.format_exc()
        out = {"status": "error", "symbol": symbol, "error": "engine_exception"}
        if dbg:
            out["debug"] = {
                "route_version": ROUTE_VERSION,
                "engine_source": eng_src,
                "traceback": _clamp(tb, 8000),
            }
        return out


@router.get("/quotes")
async def enriched_quotes(request: Request, symbols: str, debug: int = 0) -> Dict[str, Any]:
    ok, err = _check_auth(request)
    if not ok:
        return {"status": "error", "count": 0, "items": [], "error": err}

    dbg = bool(debug)
    sym_list = _normalize_symbols_csv(symbols)
    if not sym_list:
        return {"status": "error", "count": 0, "items": [], "error": "No symbols provided"}

    eng, eng_src, eng_err = _get_engine(request)
    if eng is None:
        out = {"status": "error", "count": 0, "items": [], "error": "engine_unavailable"}
        if dbg:
            out["debug"] = {"route_version": ROUTE_VERSION, "engine_source": eng_src, "engine_error": eng_err}
        return out

    items = _call_engine_quotes(eng, sym_list, dbg)

    # KSA supplement per item
    sup_count = 0
    for it in items:
        try:
            it2, sup_err, sup_used = _supplement_ksa_fundamentals_if_enabled(it)
            if sup_used:
                sup_count += 1
            if sup_err:
                cur = str(it2.get("error") or "").strip()
                warn = f"warning: {sup_err}"
                it2["error"] = (cur + " | " + warn).strip(" |") if cur else warn
        except Exception:
            continue

    out = {"status": "success", "count": len(items), "items": items}
    if dbg:
        out["debug"] = {
            "route_version": ROUTE_VERSION,
            "engine_source": eng_src,
            "engine_type": type(eng).__name__,
            "supplement_used_count": sup_count,
        }
    return out
