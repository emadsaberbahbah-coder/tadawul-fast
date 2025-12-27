# core/data_engine_v2.py  (FULL REPLACEMENT)
"""
core/data_engine_v2.py
===============================================================
UNIFIED DATA ENGINE (v2.8.0) — KSA-SAFE + PROD SAFE + ROUTER FRIENDLY

Goals
- Never crash app startup (no network at import time).
- Provide DataEngine used by routes/enriched_quote.py and analysis routers.
- Provider routing:
    GLOBAL: ENABLED_PROVIDERS or PROVIDERS (comma-separated)
    KSA:    KSA_PROVIDERS (comma-separated)
- If provider list is missing, uses safe defaults:
    GLOBAL: eodhd -> finnhub -> fmp
    KSA:    tadawul -> argaam -> yahoo_chart
- Best-effort: if a provider module/function is missing, skip safely.

Env (optional)
- ENABLED_PROVIDERS / PROVIDERS      e.g. "eodhd,finnhub,fmp"
- KSA_PROVIDERS                      e.g. "tadawul,argaam,yahoo_chart"
- PRIMARY_PROVIDER                   e.g. "eodhd" (forces first in GLOBAL list)
- PRIMARY_KSA_PROVIDER               e.g. "tadawul" (forces first in KSA list)
- ENGINE_CACHE_TTL_SEC               default: 10 (min: 3)

Expected provider modules (optional)
- core.providers.eodhd_provider
- core.providers.finnhub_provider
- core.providers.fmp_provider
- core.providers.tadawul_provider
- core.providers.argaam_provider
- core.providers.yahoo_chart_provider

Provider callable conventions (engine will try many names)
- fetch_quote_patch(symbol) -> dict OR (dict, err)
- fetch_enriched_quote_patch(symbol) -> dict OR (dict, err)
- fetch_quote_and_enrichment_patch(symbol) -> dict OR (dict, err)
- KSA-only (legacy names):
    fetch_quote_and_fundamentals_patch(symbol) -> (dict, err)

Return
- UnifiedQuote-like dict (aligned keys):
  symbol, market, currency, name,
  current_price, previous_close, open, day_high, day_low,
  price_change, percent_change, volume, value_traded,
  high_52w, low_52w, position_52w_percent,
  market_cap, shares_outstanding, free_float, free_float_market_cap,
  eps_ttm, pe_ttm, pb, ps, dividend_yield, roe, roa, beta,
  data_source, data_quality, last_updated_utc, error,
  quality_score, value_score, momentum_score, risk_score, opportunity_score
"""

from __future__ import annotations

import asyncio
import importlib
import logging
import math
import os
import re
from datetime import datetime, timezone
from typing import Any, Callable, Dict, List, Optional, Tuple

from cachetools import TTLCache

logger = logging.getLogger("core.data_engine_v2")

ENGINE_VERSION = "2.8.0"

_ARABIC_DIGITS = str.maketrans("٠١٢٣٤٥٦٧٨٩", "0123456789")


# -----------------------------------------------------------------------------
# Helpers
# -----------------------------------------------------------------------------
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


def normalize_symbol(symbol: str) -> str:
    """
    Public normalizer (used by legacy adapter too).
    """
    s = (symbol or "").strip().upper()
    if not s:
        return ""
    if s.startswith("TADAWUL:"):
        s = s.split(":", 1)[1].strip()
    if s.endswith(".TADAWUL"):
        s = s.replace(".TADAWUL", "")

    # digits -> KSA .SR
    if s.isdigit():
        return f"{s}.SR"
    if re.fullmatch(r"\d{3,6}", s):
        return f"{s}.SR"

    return s


def _is_ksa(symbol: str) -> bool:
    s = (symbol or "").strip().upper()
    return s.endswith(".SR") or s.isdigit() or bool(re.fullmatch(r"\d{3,6}(\.SR)?", s))


def _pos_52w(cur: Optional[float], lo: Optional[float], hi: Optional[float]) -> Optional[float]:
    if cur is None or lo is None or hi is None:
        return None
    if hi == lo:
        return None
    try:
        return (cur - lo) / (hi - lo) * 100.0
    except Exception:
        return None


def _merge_patch(dst: Dict[str, Any], src: Dict[str, Any]) -> None:
    """
    Merge policy:
    - never overwrite a non-empty dst value
    - always ignore None from src
    - ignore noisy provider status fields
    """
    if not isinstance(src, dict):
        return
    for k, v in src.items():
        if v is None:
            continue
        if k in {"status"}:
            continue
        if k not in dst or dst.get(k) is None or dst.get(k) == "":
            dst[k] = v


async def _call_async(fn: Callable[..., Any], *args: Any, **kwargs: Any) -> Any:
    out = fn(*args, **kwargs)
    if asyncio.iscoroutine(out):
        return await out
    return out


def _is_patch_tuple(res: Any) -> bool:
    return isinstance(res, tuple) and len(res) == 2


async def _try_provider_call(
    module_name: str,
    fn_names: List[str],
    *args: Any,
    **kwargs: Any,
) -> Tuple[Dict[str, Any], Optional[str]]:
    """
    Tries importing module and calling the first existing function in fn_names.
    Accepts:
      - dict
      - (dict, err)
    """
    try:
        mod = importlib.import_module(module_name)
    except Exception as e:
        return {}, f"import failed: {e.__class__.__name__}"

    fn = None
    used = None
    for n in fn_names:
        if hasattr(mod, n):
            candidate = getattr(mod, n)
            if callable(candidate):
                fn = candidate
                used = n
                break

    if fn is None:
        return {}, f"no callable in {fn_names}"

    try:
        res = await _call_async(fn, *args, **kwargs)

        if _is_patch_tuple(res):
            patch, err = res
            if isinstance(patch, dict):
                return patch, err
            return {}, err or f"{used}: returned non-dict patch"

        if isinstance(res, dict):
            return res, None

        return {}, f"{used}: unexpected return type ({type(res).__name__})"
    except Exception as e:
        return {}, f"{used}: call failed ({e.__class__.__name__})"


# -----------------------------------------------------------------------------
# Scoring (lightweight heuristics)
# -----------------------------------------------------------------------------
def _score_quality(p: Dict[str, Any]) -> int:
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


def _score_value(p: Dict[str, Any]) -> int:
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


def _score_momentum(p: Dict[str, Any]) -> int:
    chg = _safe_float(p.get("percent_change"))
    score = 50
    if chg is not None:
        score += 20 if chg >= 2 else 0
        score -= 20 if chg <= -2 else 0
    return int(max(0, min(100, score)))


def _score_risk(p: Dict[str, Any]) -> int:
    beta = _safe_float(p.get("beta"))
    score = 35
    if beta is not None:
        score += 10 if beta >= 1.2 else 0
        score -= 10 if beta <= 0.8 else 0
    return int(max(0, min(100, score)))


def _finalize_derived(out: Dict[str, Any]) -> None:
    cur = _safe_float(out.get("current_price"))
    prev = _safe_float(out.get("previous_close"))
    vol = _safe_float(out.get("volume"))

    if out.get("price_change") is None and cur is not None and prev is not None:
        out["price_change"] = cur - prev

    if out.get("percent_change") is None and cur is not None and prev not in (None, 0.0):
        out["percent_change"] = (cur - prev) / prev * 100.0

    if out.get("value_traded") is None and cur is not None and vol is not None:
        out["value_traded"] = cur * vol

    if out.get("position_52w_percent") is None:
        out["position_52w_percent"] = _pos_52w(
            cur,
            _safe_float(out.get("low_52w")),
            _safe_float(out.get("high_52w")),
        )

    mc = _safe_float(out.get("market_cap"))
    ff = _safe_float(out.get("free_float"))
    if out.get("free_float_market_cap") is None and mc is not None and ff is not None:
        out["free_float_market_cap"] = mc * (ff / 100.0)


def _compute_data_quality(out: Dict[str, Any]) -> str:
    cur = _safe_float(out.get("current_price"))
    if cur is None:
        return "BAD"

    # FULL if typical dashboard trading fields exist
    must = ["previous_close", "day_high", "day_low", "volume"]
    have_trading = all(_safe_float(out.get(k)) is not None for k in must)

    have_identity = bool((out.get("name") or "").strip())
    have_mcap = _safe_float(out.get("market_cap")) is not None

    if have_trading and (have_identity or have_mcap):
        return "FULL"
    if have_trading:
        return "OK"
    return "PARTIAL"


def _reorder_primary(items: List[str], primary: Optional[str]) -> List[str]:
    if not items:
        return items
    p = (primary or "").strip().lower()
    if not p:
        return items
    if p in items:
        return [p] + [x for x in items if x != p]
    return items


# -----------------------------------------------------------------------------
# DataEngine
# -----------------------------------------------------------------------------
class DataEngine:
    """
    Engine used by app.state.engine (preferred by routers).
    """

    def __init__(self) -> None:
        global_list = _parse_list_env("ENABLED_PROVIDERS") or _parse_list_env("PROVIDERS")
        ksa_list = _parse_list_env("KSA_PROVIDERS")

        # Safe defaults if nothing configured
        if not global_list:
            global_list = ["eodhd", "finnhub", "fmp"]
        if not ksa_list:
            ksa_list = ["tadawul", "argaam", "yahoo_chart"]

        # Force primary first if set
        global_list = _reorder_primary(global_list, os.getenv("PRIMARY_PROVIDER"))
        ksa_list = _reorder_primary(ksa_list, os.getenv("PRIMARY_KSA_PROVIDER"))

        self.providers_global = global_list
        self.providers_ksa = ksa_list

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
        """
        Best-effort provider client close hooks.
        """
        for mod_name, closer in [
            ("core.providers.tadawul_provider", "aclose_tadawul_client"),
            ("core.providers.argaam_provider", "aclose_argaam_client"),
        ]:
            try:
                mod = importlib.import_module(mod_name)
                fn = getattr(mod, closer, None)
                if callable(fn):
                    await _call_async(fn)
            except Exception:
                continue

    async def get_quote(self, symbol: str) -> Dict[str, Any]:
        return await self.get_enriched_quote(symbol, enrich=False)

    async def get_quotes(self, symbols: List[str]) -> List[Dict[str, Any]]:
        return await self.get_enriched_quotes(symbols, enrich=False)

    async def get_enriched_quote(self, symbol: str, *, enrich: bool = True) -> Dict[str, Any]:
        sym = normalize_symbol(symbol)
        if not sym:
            return {"status": "error", "symbol": str(symbol or ""), "error": "empty symbol", "data_quality": "BAD"}

        cache_key = f"q::{sym}::{int(bool(enrich))}"
        hit = self._cache.get(cache_key)
        if isinstance(hit, dict) and hit:
            return dict(hit)

        is_ksa = _is_ksa(sym)
        providers = self.providers_ksa if is_ksa else self.providers_global

        out: Dict[str, Any] = {
            "status": "success",
            "symbol": sym,
            "market": "KSA" if is_ksa else "GLOBAL",
            "currency": "SAR" if is_ksa else "",
            "name": "",
            "data_source": "",
            "data_quality": "",
            "last_updated_utc": _utc_iso(),
            "error": "",
        }

        if not providers:
            out["status"] = "error"
            out["data_quality"] = "BAD"
            out["error"] = "No providers configured"
            return out

        errors: List[str] = []
        source_used: List[str] = []

        # Choose function preference order based on enrich flag
        def _fnset(*names: str) -> List[str]:
            # prefer enriched or quote first depending on enrich flag
            if enrich:
                preferred = []
                for n in names:
                    if "enriched" in n or "enrichment" in n or "fundamentals" in n:
                        preferred.append(n)
                for n in names:
                    if n not in preferred:
                        preferred.append(n)
                return preferred
            return list(names)

        for p in providers:
            p = (p or "").strip().lower()
            if not p:
                continue

            patch: Dict[str, Any] = {}
            err: Optional[str] = None

            # ---------------- KSA providers ----------------
            if p == "tadawul":
                patch, err = await _try_provider_call(
                    "core.providers.tadawul_provider",
                    _fnset(
                        "fetch_quote_and_fundamentals_patch",
                        "fetch_quote_and_enrichment_patch",
                        "fetch_enriched_quote_patch",
                        "fetch_fundamentals_patch",
                        "fetch_quote_patch",
                    ),
                    sym,
                )

            elif p == "argaam":
                patch, err = await _try_provider_call(
                    "core.providers.argaam_provider",
                    _fnset(
                        "fetch_quote_patch",
                        "fetch_quote_and_fundamentals_patch",
                        "fetch_quote_and_enrichment_patch",
                        "fetch_enriched_quote_patch",
                    ),
                    sym,
                )

            elif p == "yahoo_chart":
                patch, err = await _try_provider_call(
                    "core.providers.yahoo_chart_provider",
                    _fnset(
                        "fetch_quote_patch",
                        "fetch_quote_and_enrichment_patch",
                        "fetch_enriched_quote_patch",
                        "fetch_quote",   # many versions export this
                        "get_quote",     # alias in many versions
                    ),
                    sym,
                )

            # ---------------- GLOBAL providers ----------------
            elif p == "eodhd":
                patch, err = await _try_provider_call(
                    "core.providers.eodhd_provider",
                    _fnset(
                        "fetch_quote_and_enrichment_patch",
                        "fetch_enriched_quote_patch",
                        "fetch_quote_and_fundamentals_patch",  # legacy
                        "fetch_quote_patch",
                    ),
                    sym,
                )

            elif p == "finnhub":
                # finnhub_provider reads FINNHUB_API_KEY internally (do NOT pass token as argument)
                patch, err = await _try_provider_call(
                    "core.providers.finnhub_provider",
                    _fnset(
                        "fetch_quote_and_enrichment_patch",
                        "fetch_enriched_quote_patch",
                        "fetch_quote_patch",
                    ),
                    sym,
                )

            elif p == "fmp":
                patch, err = await _try_provider_call(
                    "core.providers.fmp_provider",
                    _fnset(
                        "fetch_quote_and_fundamentals_patch",
                        "fetch_quote_and_enrichment_patch",
                        "fetch_enriched_quote_patch",
                        "fetch_quote_patch",
                    ),
                    sym,
                )

            else:
                errors.append(f"{p}: unknown provider")
                continue

            if patch:
                _merge_patch(out, patch)
                source_used.append(p)

            if err:
                # keep provider errors compact (avoid misleading giant traces)
                errors.append(f"{p}: {err}")

            # Stop early if we have core quote + some identity/fundamental richness
            if _safe_float(out.get("current_price")) is not None and (
                (out.get("name") or "").strip() or _safe_float(out.get("market_cap")) is not None
            ):
                break

        # Fill derived fields and scores
        _finalize_derived(out)

        out["quality_score"] = _score_quality(out)
        out["value_score"] = _score_value(out)
        out["momentum_score"] = _score_momentum(out)
        out["risk_score"] = _score_risk(out)

        qs = _safe_float(out.get("quality_score")) or 50.0
        vs = _safe_float(out.get("value_score")) or 50.0
        ms = _safe_float(out.get("momentum_score")) or 50.0
        rs = _safe_float(out.get("risk_score")) or 35.0
        out["opportunity_score"] = int(
            max(0, min(100, round((0.35 * qs) + (0.35 * vs) + (0.30 * ms) - (0.15 * rs))))
        )

        out["data_source"] = ",".join(source_used) if source_used else (out.get("data_source") or "")
        out["data_quality"] = _compute_data_quality(out)

        if out["data_quality"] == "BAD":
            out["status"] = "error"
            out["error"] = " | ".join(errors) if errors else (out.get("error") or "No price returned")
        else:
            # If we have partial success but some providers failed, keep as warning text
            if errors and not (out.get("error") or "").strip():
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
                out.append(
                    {
                        "status": "error",
                        "symbol": normalize_symbol(symbols[i]) or str(symbols[i] or ""),
                        "market": "UNKNOWN",
                        "data_quality": "BAD",
                        "last_updated_utc": _utc_iso(),
                        "error": f"{r.__class__.__name__}: {r}",
                    }
                )
            else:
                out.append(r)
        return out


# -----------------------------------------------------------------------------
# Convenience module-level (legacy-style)
# -----------------------------------------------------------------------------
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
