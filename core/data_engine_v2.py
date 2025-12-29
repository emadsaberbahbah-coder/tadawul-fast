# core/data_engine_v2.py  (FULL REPLACEMENT)
"""
core/data_engine_v2.py
===============================================================
UNIFIED DATA ENGINE (v2.7.8) — KSA-SAFE + PROD SAFE + ROUTER FRIENDLY

✅ v2.7.8 improvements vs v2.7.7 (fix KSA fundamentals visibility + reliability)
- ✅ KSA price stays on yahoo_chart (stable).
- ✅ Yahoo Fundamentals supplement is now DEBUGGABLE:
    - prefers calling yahoo_fundamentals() (full dict includes "error")
    - if returns empty fundamentals, emits warning (so we can see why)
- ✅ Fundamentals cache (FUNDAMENTALS_TTL_SEC, default 21600s = 6h)
- ✅ Better supplement trigger:
    - runs when core fundamentals are missing (market_cap/pe/pb/div_yield/roe/roa/shares)
- ✅ Derivations:
    - compute shares_outstanding or market_cap if one exists + current_price
    - compute free_float_market_cap when market_cap + free_float exist
- ✅ PROD SAFE: never throws to callers; health stays up.

Return shape
- Dict aligned to UnifiedQuote keys (safe for Sheets)
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

ENGINE_VERSION = "2.7.8"

_ARABIC_DIGITS = str.maketrans("٠١٢٣٤٥٦٧٨٩", "0123456789")
_TRUTHY = {"1", "true", "yes", "y", "on", "t"}
_FALSEY = {"0", "false", "no", "n", "off", "f"}


# ---------------------------------------------------------------------------
# Pydantic (best-effort)
# ---------------------------------------------------------------------------
try:
    from pydantic import BaseModel, ConfigDict, Field
except Exception:  # pragma: no cover
    class BaseModel:  # type: ignore
        def __init__(self, **kwargs):
            self.__dict__.update(kwargs)

        def model_dump(self, *a, **k):
            return dict(self.__dict__)

    def Field(default=None, **kwargs):  # type: ignore
        return default

    def ConfigDict(**kwargs):  # type: ignore
        return dict(kwargs)


# ---------------------------------------------------------------------------
# UnifiedQuote schema (for router schema-fill)
# ---------------------------------------------------------------------------
class UnifiedQuote(BaseModel):
    model_config = ConfigDict(populate_by_name=True, extra="ignore")

    # Identity
    symbol: str
    name: Optional[str] = None
    market: Optional[str] = None
    exchange: Optional[str] = None
    currency: Optional[str] = None

    # Price / Trading
    current_price: Optional[float] = None
    previous_close: Optional[float] = None
    open: Optional[float] = None
    day_high: Optional[float] = None
    day_low: Optional[float] = None

    week_52_high: Optional[float] = None
    week_52_low: Optional[float] = None
    position_52w_percent: Optional[float] = None

    volume: Optional[float] = None
    avg_volume_30d: Optional[float] = None
    value_traded: Optional[float] = None

    price_change: Optional[float] = None
    percent_change: Optional[float] = None

    # Fundamentals (optional)
    market_cap: Optional[float] = None
    shares_outstanding: Optional[float] = None
    free_float: Optional[float] = None  # percent (0..100) per your AAPL output
    free_float_market_cap: Optional[float] = None

    eps_ttm: Optional[float] = None
    pe_ttm: Optional[float] = None
    pb: Optional[float] = None
    ps: Optional[float] = None
    dividend_yield: Optional[float] = None  # fraction (0.0037 = 0.37%)
    roe: Optional[float] = None
    roa: Optional[float] = None
    beta: Optional[float] = None

    # Scores
    quality_score: Optional[int] = None
    value_score: Optional[int] = None
    momentum_score: Optional[int] = None
    risk_score: Optional[int] = None
    opportunity_score: Optional[int] = None

    # Provenance
    data_source: Optional[str] = None
    data_quality: Optional[str] = None
    last_updated_utc: Optional[str] = None
    error: Optional[str] = None

    # Router helpers (optional)
    symbol_input: Optional[str] = None
    symbol_normalized: Optional[str] = None

    def finalize(self) -> "UnifiedQuote":
        cur = _safe_float(self.current_price)
        prev = _safe_float(self.previous_close)
        vol = _safe_float(self.volume)

        if self.price_change is None and cur is not None and prev is not None:
            self.price_change = cur - prev

        if self.percent_change is None and cur is not None and prev not in (None, 0.0):
            try:
                self.percent_change = (cur - prev) / prev * 100.0
            except Exception:
                pass

        if self.value_traded is None and cur is not None and vol is not None:
            self.value_traded = cur * vol

        if self.position_52w_percent is None:
            hi = _safe_float(self.week_52_high)
            lo = _safe_float(self.week_52_low)
            if cur is not None and hi is not None and lo is not None and hi != lo:
                self.position_52w_percent = (cur - lo) / (hi - lo) * 100.0

        if not self.last_updated_utc:
            self.last_updated_utc = _utc_iso()

        if self.error is None:
            self.error = ""

        return self


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def _utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _env_bool(name: str, default: bool = False) -> bool:
    raw = (os.getenv(name) or "").strip().lower()
    if not raw:
        return default
    if raw in _FALSEY:
        return False
    if raw in _TRUTHY:
        return True
    return default


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


def _parse_list_env(name: str, fallback: str = "") -> List[str]:
    raw = (os.getenv(name) or "").strip()
    if not raw and fallback:
        raw = fallback
    if not raw:
        return []
    return [p.strip().lower() for p in raw.split(",") if p.strip()]


def _is_ksa(symbol: str) -> bool:
    s = (symbol or "").strip().upper()
    return s.endswith(".SR") or s.isdigit() or bool(re.fullmatch(r"\d{3,6}(\.SR)?", s))


def normalize_symbol(symbol: str) -> str:
    s = (symbol or "").strip().upper()
    if not s:
        return ""
    if s.startswith("TADAWUL:"):
        s = s.split(":", 1)[1].strip()
    if s.endswith(".TADAWUL"):
        s = s.replace(".TADAWUL", "")
    if s.isdigit() or re.fullmatch(r"\d{3,6}", s):
        return f"{s}.SR"
    return s


def _merge_patch(dst: Dict[str, Any], src: Dict[str, Any]) -> None:
    for k, v in (src or {}).items():
        if v is None:
            continue
        if k not in dst or dst.get(k) is None or dst.get(k) == "":
            dst[k] = v


async def _call_maybe_async(fn: Callable[..., Any], *args: Any, **kwargs: Any) -> Any:
    out = fn(*args, **kwargs)
    if asyncio.iscoroutine(out):
        return await out
    return out


def _is_tuple2(x: Any) -> bool:
    try:
        return isinstance(x, tuple) and len(x) == 2
    except Exception:
        return False


async def _try_provider_call(
    module_name: str,
    fn_names: List[str],
    *args: Any,
    **kwargs: Any,
) -> Tuple[Dict[str, Any], Optional[str]]:
    try:
        mod = importlib.import_module(module_name)
    except Exception as e:
        return {}, f"{module_name}: import failed ({e})"

    fn = None
    used = None
    for n in fn_names:
        if hasattr(mod, n):
            cand = getattr(mod, n)
            if callable(cand):
                fn = cand
                used = n
                break

    if fn is None:
        return {}, f"{module_name}: no callable in {fn_names}"

    try:
        res = await _call_maybe_async(fn, *args, **kwargs)
        if _is_tuple2(res):
            patch, err = res
            return (patch or {}) if isinstance(patch, dict) else {}, err
        if isinstance(res, dict):
            return res, None
        return {}, f"{module_name}.{used}: unexpected return type"
    except Exception as e:
        return {}, f"{module_name}.{used}: call failed ({e})"


def _apply_derived(out: Dict[str, Any]) -> None:
    cur = _safe_float(out.get("current_price"))
    prev = _safe_float(out.get("previous_close"))
    vol = _safe_float(out.get("volume"))

    if out.get("price_change") is None and cur is not None and prev is not None:
        out["price_change"] = cur - prev

    if out.get("percent_change") is None and cur is not None and prev not in (None, 0.0):
        try:
            out["percent_change"] = (cur - prev) / prev * 100.0
        except Exception:
            pass

    if out.get("value_traded") is None and cur is not None and vol is not None:
        out["value_traded"] = cur * vol

    if out.get("position_52w_percent") is None:
        hi = _safe_float(out.get("week_52_high"))
        lo = _safe_float(out.get("week_52_low"))
        if cur is not None and hi is not None and lo is not None and hi != lo:
            out["position_52w_percent"] = (cur - lo) / (hi - lo) * 100.0


def _apply_fundamental_derivations(out: Dict[str, Any]) -> None:
    """
    Helpful derivations once we have price + either market_cap or shares_outstanding.
    Keeps everything best-effort; never throws.
    """
    try:
        cur = _safe_float(out.get("current_price"))
        mc = _safe_float(out.get("market_cap"))
        sh = _safe_float(out.get("shares_outstanding"))

        if cur is not None and mc is not None and sh is None and cur != 0:
            out["shares_outstanding"] = mc / cur

        if cur is not None and sh is not None and mc is None:
            out["market_cap"] = sh * cur

        ff = _safe_float(out.get("free_float"))
        mc2 = _safe_float(out.get("market_cap"))
        if mc2 is not None and ff is not None and out.get("free_float_market_cap") is None:
            # free_float is treated as percent (0..100) per your AAPL example.
            out["free_float_market_cap"] = mc2 * (ff / 100.0)
    except Exception:
        return


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


def _quality_label(out: Dict[str, Any]) -> str:
    must = ["current_price", "previous_close", "day_high", "day_low", "volume"]
    ok = all(_safe_float(out.get(k)) is not None for k in must)
    if _safe_float(out.get("current_price")) is None:
        return "BAD"
    return "FULL" if ok else "PARTIAL"


def _dedup_preserve(items: List[str]) -> List[str]:
    out: List[str] = []
    seen = set()
    for x in items:
        s = (x or "").strip()
        if not s:
            continue
        if s in seen:
            continue
        seen.add(s)
        out.append(s)
    return out


def _tadawul_configured() -> bool:
    tq = (os.getenv("TADAWUL_QUOTE_URL") or "").strip()
    tf = (os.getenv("TADAWUL_FUNDAMENTALS_URL") or "").strip()
    return bool(tq or tf)


def _has_any_fundamentals(out: Dict[str, Any]) -> bool:
    for k in (
        "market_cap",
        "pe_ttm",
        "eps_ttm",
        "dividend_yield",
        "pb",
        "ps",
        "shares_outstanding",
        "free_float",
        "roe",
        "roa",
        "beta",
    ):
        if _safe_float(out.get(k)) is not None:
            return True
    return False


def _has_core_fundamentals(out: Dict[str, Any]) -> bool:
    """
    Core fundamentals that matter for your dashboard scoring.
    If none exist, we should try Yahoo fundamentals supplement.
    """
    for k in ("market_cap", "shares_outstanding", "pe_ttm", "pb", "dividend_yield", "roe", "roa"):
        if _safe_float(out.get(k)) is not None:
            return True
    return False


def _normalize_warning_prefix(msg: str) -> str:
    m = (msg or "").strip()
    if not m:
        return ""
    low = m.lower()
    if low.startswith("warning:"):
        return m[len("warning:") :].strip()
    return m


# ---------------------------------------------------------------------------
# DataEngine
# ---------------------------------------------------------------------------
class DataEngine:
    """
    Engine used by app.state.engine (preferred by routers).
    """

    def __init__(self) -> None:
        self.providers_global = _parse_list_env("ENABLED_PROVIDERS") or _parse_list_env("PROVIDERS")
        self.providers_ksa = _parse_list_env("KSA_PROVIDERS")

        # ✅ Fundamentals supplement toggles (default ON for KSA)
        self.enable_yahoo_fundamentals_ksa = _env_bool("ENABLE_YAHOO_FUNDAMENTALS_KSA", True)
        self.enable_yahoo_fundamentals_global = _env_bool("ENABLE_YAHOO_FUNDAMENTALS_GLOBAL", False)

        # Quote cache TTL
        ttl = 10
        try:
            ttl = int((os.getenv("ENGINE_CACHE_TTL_SEC") or "10").strip())
            if ttl < 3:
                ttl = 3
        except Exception:
            ttl = 10
        self._cache: TTLCache = TTLCache(maxsize=8000, ttl=ttl)

        # Fundamentals cache TTL (long)
        fttl = 21600
        try:
            fttl = int((os.getenv("FUNDAMENTALS_TTL_SEC") or "21600").strip())
            if fttl < 300:
                fttl = 300
        except Exception:
            fttl = 21600
        self._fund_cache: TTLCache = TTLCache(maxsize=12000, ttl=fttl)

        logger.info(
            "DataEngineV2 init v%s | GLOBAL=%s | KSA=%s | quote_cache_ttl=%ss | fund_cache_ttl=%ss | "
            "yahoo_fund_ksa=%s yahoo_fund_global=%s",
            ENGINE_VERSION,
            ",".join(self.providers_global) if self.providers_global else "(none)",
            ",".join(self.providers_ksa) if self.providers_ksa else "(none)",
            ttl,
            fttl,
            self.enable_yahoo_fundamentals_ksa,
            self.enable_yahoo_fundamentals_global,
        )

    async def aclose(self) -> None:
        for mod_name, closer in [
            ("core.providers.finnhub_provider", "aclose_finnhub_client"),
            ("core.providers.tadawul_provider", "aclose_tadawul_client"),
        ]:
            try:
                mod = importlib.import_module(mod_name)
                fn = getattr(mod, closer, None)
                if callable(fn):
                    await _call_maybe_async(fn)
            except Exception:
                continue

    def _providers_for(self, sym: str) -> List[str]:
        is_ksa = _is_ksa(sym)

        if is_ksa:
            if self.providers_ksa:
                return list(self.providers_ksa)
            safe = [p for p in (self.providers_global or []) if p in {"tadawul", "argaam", "yahoo_chart"}]
            return safe

        return list(self.providers_global or [])

    async def _yahoo_fundamentals_supplement(self, sym: str) -> Tuple[Dict[str, Any], Optional[str]]:
        """
        Returns (clean_patch, warning_or_error).
        Uses fundamentals cache.
        """
        cache_key = f"fund::{sym}"
        hit = self._fund_cache.get(cache_key)
        if isinstance(hit, dict) and hit:
            # cached patch, no warning
            return dict(hit), None

        # Prefer yahoo_fundamentals() first because it includes "error" in returned dict
        patch2, err2 = await _try_provider_call(
            "core.providers.yahoo_fundamentals_provider",
            [
                "yahoo_fundamentals",        # returns full dict including "error"
                "fetch_fundamentals_patch",  # returns filtered patch only
                "fetch_patch",
                "fetch_fundamentals",
            ],
            sym,
        )

        # If yahoo_fundamentals() was used, patch2 includes many keys; we only keep what UnifiedQuote expects.
        clean = {
            k: patch2.get(k)
            for k in (
                "currency",
                "market_cap",
                "shares_outstanding",
                "pe_ttm",
                "pb",
                "dividend_yield",
                "roe",
                "roa",
            )
            if patch2.get(k) is not None
        }

        # Pull error from either the engine err2 or the provider dict field (important!)
        msg = (err2 or "") or str(patch2.get("error") or "").strip()
        msg = msg.strip() if msg else ""

        # If nothing returned and no error, still warn (this was your silent failure in v2.7.7)
        if not clean and not msg:
            msg = "yahoo fundamentals returned no usable fields"

        # Cache only when we got something useful (avoid caching empties forever)
        if clean:
            self._fund_cache[cache_key] = dict(clean)

        return clean, msg or None

    async def get_enriched_quote(self, symbol: str, *, enrich: bool = True) -> Dict[str, Any]:
        sym = normalize_symbol(symbol)
        if not sym:
            q = UnifiedQuote(symbol=str(symbol or ""), error="empty symbol").finalize()
            out0 = q.model_dump() if hasattr(q, "model_dump") else dict(q.__dict__)
            out0["status"] = "error"
            out0["data_quality"] = "BAD"
            return out0

        cache_key = f"q::{sym}::{int(bool(enrich))}"
        hit = self._cache.get(cache_key)
        if isinstance(hit, dict) and hit:
            return dict(hit)

        is_ksa = _is_ksa(sym)
        providers = self._providers_for(sym)

        out: Dict[str, Any] = UnifiedQuote(
            symbol=sym,
            market="KSA" if is_ksa else "GLOBAL",
            last_updated_utc=_utc_iso(),
            error="",
            symbol_input=str(symbol or ""),
            symbol_normalized=sym,
        ).model_dump()

        if not providers:
            out["status"] = "error"
            out["data_quality"] = "BAD"
            out["error"] = "No providers configured"
            self._cache[cache_key] = dict(out)
            return out

        warnings: List[str] = []
        source_used: List[str] = []

        for p in providers:
            p = (p or "").strip().lower()
            if not p:
                continue

            # KSA safety: never call global-only providers for KSA unless explicitly in KSA providers list
            if is_ksa and p in {"eodhd", "fmp", "finnhub"} and (p not in (self.providers_ksa or [])):
                warnings.append(f"{p}: skipped for KSA")
                continue

            # Auto-skip tadawul if not configured
            if p == "tadawul" and not _tadawul_configured():
                warnings.append("tadawul: skipped (missing TADAWUL_QUOTE_URL/TADAWUL_FUNDAMENTALS_URL)")
                continue

            patch: Dict[str, Any] = {}
            err: Optional[str] = None

            if p == "tadawul":
                patch, err = await _try_provider_call(
                    "core.providers.tadawul_provider",
                    ["fetch_quote_and_fundamentals_patch", "fetch_fundamentals_patch", "fetch_quote_patch"],
                    sym,
                )

            elif p == "argaam":
                patch, err = await _try_provider_call(
                    "core.providers.argaam_provider",
                    [
                        "fetch_quote_patch",
                        "fetch_quote_and_fundamentals_patch",
                        "fetch_enriched_patch",
                        "fetch_enriched_quote_patch",
                    ],
                    sym,
                )

            elif p == "yahoo_chart":
                patch, err = await _try_provider_call(
                    "core.providers.yahoo_chart_provider",
                    ["fetch_quote_patch", "fetch_quote_and_enrichment_patch", "fetch_enriched_quote_patch", "fetch_quote"],
                    sym,
                )

            elif p == "finnhub":
                api_key = os.getenv("FINNHUB_API_KEY")
                patch, err = await _try_provider_call(
                    "core.providers.finnhub_provider",
                    ["fetch_quote_and_enrichment_patch", "fetch_enriched_quote_patch", "fetch_quote_patch", "fetch_quote"],
                    sym,
                    api_key,
                )

            elif p == "eodhd":
                if enrich:
                    fn_list = [
                        "fetch_enriched_quote_patch",
                        "fetch_quote_and_fundamentals_patch",
                        "fetch_enriched_patch",
                        "fetch_quote_patch",
                        "fetch_quote",
                    ]
                else:
                    fn_list = ["fetch_quote_patch", "fetch_quote"]
                patch, err = await _try_provider_call("core.providers.eodhd_provider", fn_list, sym)

            elif p == "fmp":
                patch, err = await _try_provider_call(
                    "core.providers.fmp_provider",
                    [
                        "fetch_quote_and_fundamentals_patch",
                        "fetch_enriched_quote_patch",
                        "fetch_enriched_patch",
                        "fetch_quote_patch",
                        "fetch_quote",
                    ],
                    sym,
                )

            else:
                warnings.append(f"{p}: unknown provider")
                continue

            if patch:
                _merge_patch(out, patch)
                source_used.append(p)

            if err:
                low = str(err).strip().lower()
                if low.startswith(f"{p}:"):
                    warnings.append(str(err).strip())
                else:
                    warnings.append(f"{p}: {str(err).strip()}")

            _apply_derived(out)
            _apply_fundamental_derivations(out)

            # Stop rule:
            has_price_now = _safe_float(out.get("current_price")) is not None
            if has_price_now:
                if not enrich:
                    break
                # If we already have ANY fundamentals from a provider, we can stop.
                # (Supplement step later may still run if core fundamentals missing.)
                if _has_any_fundamentals(out):
                    break

        # ✅ Post-loop: Yahoo Fundamentals supplement (fix KSA fundamentals when core fundamentals missing)
        try:
            has_price = _safe_float(out.get("current_price")) is not None
            needs_core_fund = enrich and has_price and (not _has_core_fundamentals(out))
            allow = (is_ksa and self.enable_yahoo_fundamentals_ksa) or ((not is_ksa) and self.enable_yahoo_fundamentals_global)

            if needs_core_fund and allow:
                clean, msg = await self._yahoo_fundamentals_supplement(sym)

                if clean:
                    _merge_patch(out, clean)
                    source_used.append("yahoo_fundamentals")

                if msg:
                    warnings.append(f"yahoo_fundamentals: {msg}")

                _apply_derived(out)
                _apply_fundamental_derivations(out)

        except Exception:
            pass  # PROD SAFE

        # Scoring
        out["quality_score"] = _score_quality(out)
        out["value_score"] = _score_value(out)
        out["momentum_score"] = _score_momentum(out)
        out["risk_score"] = _score_risk(out)

        qs = float(out.get("quality_score") or 50)
        vs = float(out.get("value_score") or 50)
        ms = float(out.get("momentum_score") or 50)
        rs = float(out.get("risk_score") or 35)
        out["opportunity_score"] = int(max(0, min(100, round((0.35 * qs) + (0.35 * vs) + (0.30 * ms) - (0.15 * rs)))))

        out["data_source"] = ",".join(_dedup_preserve(source_used)) if source_used else (out.get("data_source") or None)
        out["data_quality"] = _quality_label(out)

        # Final status + error normalization
        has_price = _safe_float(out.get("current_price")) is not None

        base_err = str(out.get("error") or "").strip()
        parts: List[str] = []
        if base_err:
            parts.append(_normalize_warning_prefix(base_err))
        parts += [_normalize_warning_prefix(w) for w in warnings]
        parts = _dedup_preserve([p for p in parts if p])

        if not has_price:
            out["status"] = "error"
            out["error"] = " | ".join(parts) if parts else "No price returned"
        else:
            out["status"] = "success"
            out["error"] = "" if not parts else ("warning: " + " | ".join(parts))

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
                q = UnifiedQuote(symbol=normalize_symbol(symbols[i]) or str(symbols[i] or ""), error=str(r)).finalize()
                d = q.model_dump() if hasattr(q, "model_dump") else dict(q.__dict__)
                d["status"] = "error"
                d["data_quality"] = "BAD"
                out.append(d)
            else:
                out.append(r)
        return out

    # Aliases expected by some legacy routers/services
    async def get_quote(self, symbol: str) -> Dict[str, Any]:
        return await self.get_enriched_quote(symbol, enrich=True)

    async def get_quotes(self, symbols: List[str]) -> List[Dict[str, Any]]:
        return await self.get_enriched_quotes(symbols, enrich=True)


# Convenience module-level singleton (legacy-style)
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


# legacy aliases
async def get_quote(symbol: str) -> Dict[str, Any]:
    return await get_enriched_quote(symbol)


async def get_quotes(symbols: List[str]) -> List[Dict[str, Any]]:
    return await get_enriched_quotes(symbols)
