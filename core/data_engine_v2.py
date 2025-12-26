# core/data_engine_v2.py
"""
core/data_engine_v2.py
===============================================================
UNIFIED DATA ENGINE (v2.7.1) — KSA-SAFE + PROD SAFE + ROUTER FRIENDLY

Goals
- Never crash app startup (no network at import time).
- Provide DataEngine used by routes/enriched_quote.py and analysis routers.
- Provider routing:
    GLOBAL: from ENABLED_PROVIDERS / PROVIDERS (default handled by main.py)
    KSA:    from KSA_PROVIDERS
- Best-effort: if a provider module/function is missing, skip safely.

Expected provider modules (optional)
- core.providers.finnhub_provider
- core.providers.tadawul_provider
- core.providers.yahoo_chart_provider   (if you have it)
- core.providers.argaam_provider        (if you have it)
- core.providers.eodhd_provider         (if you have it)
- core.providers.fmp_provider           (if you have it)

Return
- UnifiedQuote-like dict (aligned keys):
  symbol, market, currency, name,
  current_price, previous_close, open, day_high, day_low,
  price_change, percent_change, volume,
  market_cap, shares_outstanding, free_float, free_float_market_cap,
  eps_ttm, pe_ttm, pb, ps, dividend_yield, roe, roa, beta,
  data_source, data_quality, last_updated_utc, error
"""

from __future__ import annotations

import asyncio
import importlib
import logging
import math
import os
import re
from datetime import datetime, timezone
from typing import Any, Awaitable, Callable, Dict, List, Optional, Tuple

from cachetools import TTLCache

logger = logging.getLogger("core.data_engine_v2")

ENGINE_VERSION = "2.7.1"

_ARABIC_DIGITS = str.maketrans("٠١٢٣٤٥٦٧٨٩", "0123456789")


def _utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _safe_str(x: Any) -> Optional[str]:
    if x is None:
        return None
    s = str(x).strip()
    return s if s else None


def _safe_float(val: Any) -> Optional[float]:
    if val is None:
        return None
    try:
        if isinstance(val, (int, float)):
            f = float(val)
            if math.isnan(f) or math.isinf(f):
                return None
            return f

        s = str(val).strip()
        if not s or s in {"-", "—", "N/A", "NA", "null", "None"}:
            return None

        s = s.translate(_ARABIC_DIGITS)
        s = s.replace("٬", ",").replace("٫", ".")
        s = s.replace("%", "").replace(",", "").replace("+", "").strip()

        if s.startswith("(") and s.endswith(")"):
            s = "-" + s[1:-1].strip()

        m = re.match(r"^(-?\d+(\.\d+)?)([KMB])$", s, re.IGNORECASE)
        mult = 1.0
        if m:
            num = m.group(1)
            suf = m.group(3).upper()
            mult = 1_000.0 if suf == "K" else 1_000_000.0 if suf == "M" else 1_000_000_000.0
            s = num

        f = float(s) * mult
        if math.isnan(f) or math.isinf(f):
            return None
        return f
    except Exception:
        return None


def _pct_if_fraction(x: Any) -> Optional[float]:
    v = _safe_float(x)
    if v is None:
        return None
    return v * 100.0 if abs(v) <= 1.0 else v


def _parse_list_env(name: str, fallback: str = "") -> List[str]:
    raw = (os.getenv(name) or "").strip()
    if not raw and fallback:
        raw = fallback
    if not raw:
        return []
    return [p.strip().lower() for p in raw.split(",") if p.strip()]


def _normalize_symbol(symbol: str) -> str:
    s = (symbol or "").strip().upper()
    if not s:
        return ""
    # digits -> KSA .SR
    if s.isdigit():
        return f"{s}.SR"
    # normalize raw KSA codes like 1120 -> 1120.SR
    if re.fullmatch(r"\d{3,6}", s):
        return f"{s}.SR"
    return s


def _is_ksa(symbol: str) -> bool:
    s = (symbol or "").strip().upper()
    return s.endswith(".SR") or s.isdigit() or bool(re.fullmatch(r"\d{3,6}(\.SR)?", s))


def _merge_patch(dst: Dict[str, Any], src: Dict[str, Any]) -> None:
    # Keep existing non-null; fill missing with src non-null
    for k, v in (src or {}).items():
        if v is None:
            continue
        if k not in dst or dst.get(k) is None or dst.get(k) == "":
            dst[k] = v


async def _call_async(fn: Callable[..., Any], *args: Any, **kwargs: Any) -> Any:
    out = fn(*args, **kwargs)
    if asyncio.iscoroutine(out):
        return await out
    return out


async def _try_provider_call(
    module_name: str,
    fn_names: List[str],
    *args: Any,
    **kwargs: Any,
) -> Tuple[Dict[str, Any], Optional[str]]:
    """
    Tries importing module and calling the first existing function in fn_names.
    Expects (patch, err) return OR patch dict alone.
    """
    try:
        mod = importlib.import_module(module_name)
    except Exception as e:
        return {}, f"{module_name}: import failed ({e})"

    fn = None
    used = None
    for n in fn_names:
        if hasattr(mod, n):
            fn = getattr(mod, n)
            used = n
            break

    if fn is None or not callable(fn):
        return {}, f"{module_name}: no callable in {fn_names}"

    try:
        res = await _call_async(fn, *args, **kwargs)
        if isinstance(res, tuple) and return_two(res):
            patch, err = res
            return (patch or {}), err
        if isinstance(res, dict):
            return res, None
        return {}, f"{module_name}.{used}: unexpected return type"
    except Exception as e:
        return {}, f"{module_name}.{used}: call failed ({e})"


def return_two(x: Any) -> bool:
    try:
        return isinstance(x, tuple) and len(x) == 2
    except Exception:
        return False


def _score_quality(p: Dict[str, Any]) -> Optional[int]:
    # Simple heuristic – safe even if fields missing
    roe = _safe_float(p.get("roe"))
    margin = _safe_float(p.get("net_margin"))
    debt = _safe_float(p.get("debt_to_equity"))
    score = 50
    if roe is not None:
        score += 10 if roe >= 10 else 0
        score += 10 if roe >= 15 else 0
    if margin is not None:
        score += 5 if margin >= 10 else 0
        score += 5 if margin >= 20 else 0
    if debt is not None:
        score += 5 if debt <= 1.0 else 0
        score -= 5 if debt >= 2.0 else 0
    return int(max(0, min(100, score)))


def _score_value(p: Dict[str, Any]) -> Optional[int]:
    pe = _safe_float(p.get("pe_ttm"))
    pb = _safe_float(p.get("pb"))
    score = 50
    if pe is not None:
        score += 10 if pe <= 15 else 0
        score -= 10 if pe >= 30 else 0
    if pb is not None:
        score += 5 if pb <= 2 else 0
        score -= 5 if pb >= 6 else 0
    return int(max(0, min(100, score)))


def _score_momentum(p: Dict[str, Any]) -> Optional[int]:
    chg = _safe_float(p.get("percent_change"))
    score = 50
    if chg is not None:
        score += 20 if chg >= 2 else 0
        score -= 20 if chg <= -2 else 0
    return int(max(0, min(100, score)))


def _score_risk(p: Dict[str, Any]) -> Optional[int]:
    beta = _safe_float(p.get("beta"))
    score = 35
    if beta is not None:
        score += 10 if beta >= 1.2 else 0
        score -= 10 if beta <= 0.8 else 0
    return int(max(0, min(100, score)))


class DataEngine:
    """
    Engine used by app.state.engine (preferred by routers).
    """

    def __init__(self) -> None:
        self.providers_global = _parse_list_env("ENABLED_PROVIDERS") or _parse_list_env("PROVIDERS")
        self.providers_ksa = _parse_list_env("KSA_PROVIDERS")

        # short cache to avoid hammering providers
        ttl = 10
        try:
            ttl = int((os.getenv("ENGINE_CACHE_TTL_SEC") or "10").strip())
            if ttl < 3:
                ttl = 3
        except Exception:
            ttl = 10

        self._cache: TTLCache = TTLCache(maxsize=8000, ttl=ttl)

        logger.info(
            "DataEngineV2 init v%s | GLOBAL=%s | KSA=%s | cache_ttl=%ss",
            ENGINE_VERSION,
            ",".join(self.providers_global) if self.providers_global else "(none)",
            ",".join(self.providers_ksa) if self.providers_ksa else "(none)",
            ttl,
        )

    async def aclose(self) -> None:
        # Best-effort provider client close hooks
        for mod_name, closer in [
            ("core.providers.finnhub_provider", "aclose_finnhub_client"),
            ("core.providers.tadawul_provider", "aclose_tadawul_client"),
        ]:
            try:
                mod = importlib.import_module(mod_name)
                fn = getattr(mod, closer, None)
                if callable(fn):
                    await _call_async(fn)
            except Exception:
                continue

    async def get_enriched_quote(self, symbol: str, *, enrich: bool = True) -> Dict[str, Any]:
        sym = _normalize_symbol(symbol)
        if not sym:
            return {"status": "error", "symbol": symbol, "error": "empty symbol"}

        cache_key = f"q::{sym}::{int(bool(enrich))}"
        hit = self._cache.get(cache_key)
        if isinstance(hit, dict) and hit:
            return dict(hit)

        is_ksa = _is_ksa(sym)
        providers = self.providers_ksa if is_ksa else self.providers_global

        out: Dict[str, Any] = {
            "status": "ok",
            "symbol": sym,
            "market": "KSA" if is_ksa else None,
            "data_source": None,
            "data_quality": None,
            "last_updated_utc": _utc_iso(),
            "error": None,
        }

        if not providers:
            out["status"] = "error"
            out["error"] = "No providers configured"
            return out

        errors: List[str] = []
        source_used: List[str] = []

        for p in providers:
            p = (p or "").strip().lower()
            if not p:
                continue

            patch: Dict[str, Any] = {}
            err: Optional[str] = None

            # --- KSA providers ---
            if p == "tadawul":
                patch, err = await _try_provider_call(
                    "core.providers.tadawul_provider",
                    ["fetch_quote_and_fundamentals_patch", "fetch_fundamentals_patch", "fetch_quote_patch"],
                    sym,
                )

            elif p == "argaam":
                patch, err = await _try_provider_call(
                    "core.providers.argaam_provider",
                    ["fetch_quote_patch", "fetch_quote_and_fundamentals_patch", "fetch_enriched_patch"],
                    sym,
                )

            elif p == "yahoo_chart":
                patch, err = await _try_provider_call(
                    "core.providers.yahoo_chart_provider",
                    ["fetch_quote_patch", "fetch_quote_and_enrichment_patch", "fetch_enriched_quote_patch"],
                    sym,
                )

            # --- Global providers ---
            elif p == "finnhub":
                api_key = os.getenv("FINNHUB_API_KEY")
                patch, err = await _try_provider_call(
                    "core.providers.finnhub_provider",
                    ["fetch_quote_and_enrichment_patch", "fetch_quote_patch"],
                    sym,
                    api_key,
                )

            elif p == "eodhd":
                patch, err = await _try_provider_call(
                    "core.providers.eodhd_provider",
                    ["fetch_quote_and_fundamentals_patch", "fetch_enriched_patch", "fetch_quote_patch"],
                    sym,
                )

            elif p == "fmp":
                patch, err = await _try_provider_call(
                    "core.providers.fmp_provider",
                    ["fetch_quote_and_fundamentals_patch", "fetch_enriched_patch", "fetch_quote_patch"],
                    sym,
                )

            else:
                errors.append(f"{p}: unknown provider")
                continue

            if patch:
                _merge_patch(out, patch)
                source_used.append(p)

            if err:
                errors.append(f"{p}: {err}")

            # Stop early if we already have core price + identity
            if out.get("current_price") is not None and (out.get("name") or out.get("market_cap") is not None):
                break

        # Scoring (best-effort)
        out["quality_score"] = _score_quality(out)
        out["value_score"] = _score_value(out)
        out["momentum_score"] = _score_momentum(out)
        out["risk_score"] = _score_risk(out)

        # Simple opportunity score
        qs = _safe_float(out.get("quality_score")) or 50
        vs = _safe_float(out.get("value_score")) or 50
        ms = _safe_float(out.get("momentum_score")) or 50
        rs = _safe_float(out.get("risk_score")) or 35
        out["opportunity_score"] = int(max(0, min(100, round((0.35 * qs) + (0.35 * vs) + (0.30 * ms) - (0.15 * rs)))))

        out["data_source"] = ",".join(source_used) if source_used else (out.get("data_source") or None)

        # Data quality
        if out.get("current_price") is None:
            out["status"] = "error"
            out["data_quality"] = "BAD"
            out["error"] = " | ".join(errors) if errors else "No price returned"
        else:
            out["data_quality"] = "OK" if out.get("market_cap") is not None or out.get("name") else "PARTIAL"
            if errors and out.get("error") is None:
                out["error"] = " | ".join(errors)

        # Cache
        self._cache[cache_key] = dict(out)
        return out

    async def get_enriched_quotes(self, symbols: List[str], *, enrich: bool = True) -> List[Dict[str, Any]]:
        if not symbols:
            return []
        tasks = [self.get_enriched_quote(s, enrich=enrich) for s in symbols]
        res = await asyncio.gather(*tasks, return_exceptions=True)
        out: List[Dict[str, Any]] = []
        for i, r in enumerate(res):
            if isinstance(r, Exception):
                out.append({"status": "error", "symbol": symbols[i], "error": str(r)})
            else:
                out.append(r)
        return out


# Convenience module-level (legacy-style)
_ENGINE_SINGLETON: Optional[DataEngine] = None
_LOCK = asyncio.Lock()


async def get_engine() -> DataEngine:
    global _ENGINE_SINGLETON
    if _ENGINE_SINGLETON is None:
        async with _LOCK:
            if _ENGINE_SINGLETON is None:
                _ENGINE_SINGLETON = DataEngine()
    return _ENGINE_SINGLETON


async def get_enriched_quote(symbol: str) -> Dict[str, Any]:
    e = await get_engine()
    return await e.get_enriched_quote(symbol, enrich=True)


async def get_enriched_quotes(symbols: List[str]) -> List[Dict[str, Any]]:
    e = await get_engine()
    return await e.get_enriched_quotes(symbols, enrich=True)
