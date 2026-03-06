#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
core/investment_advisor_engine.py
================================================================================
Investment Advisor Engine — v5.0.0
================================================================================
CANONICAL | CONTEXT-PROPAGATION | HORIZON-AWARE | IMPORT-SAFE

Why this revision
- Fix missing advisory-context propagation into final canonical output.
- Ensure these fields are always populated whenever possible:
    - recommendation_reason
    - horizon_days
    - invest_period_label
- Normalize criteria / request payload into a stable internal contract.
- Provide a safe engine layer between routes and core/investment_advisor.py
- Stay tolerant to repo differences (different engine/data providers).

Design principles
- No network calls at import-time
- Best-effort compatibility with multiple backend engine styles
- Sync/async safe helper methods
- Canonical output enrichment
- Never crash because a provider method is absent

Expected usage
- from core.investment_advisor_engine import InvestmentAdvisorEngine
- engine = InvestmentAdvisorEngine(data_engine=..., quote_engine=...)
- result = engine.run(payload)

Primary public methods
- run(payload)
- get_sheet_rows(...)
- get_cached_sheet_snapshot(...)
- get_cached_multi_sheet_snapshots(...)
- get_enriched_quotes_batch(...)
================================================================================
"""

from __future__ import annotations

import asyncio
import inspect
import json
import logging
import math
import os
import threading
import traceback
from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone, timedelta
from typing import Any, Dict, List, Optional, Tuple, Union

# -----------------------------------------------------------------------------
# Optional high-performance JSON
# -----------------------------------------------------------------------------
try:
    import orjson  # type: ignore

    def json_dumps(obj: Any) -> str:
        return orjson.dumps(obj, default=str).decode("utf-8")

    def json_loads(data: Union[str, bytes]) -> Any:
        if isinstance(data, str):
            data = data.encode("utf-8")
        return orjson.loads(data)

except Exception:

    def json_dumps(obj: Any) -> str:
        return json.dumps(obj, ensure_ascii=False, default=str)

    def json_loads(data: Union[str, bytes]) -> Any:
        if isinstance(data, (bytes, bytearray)):
            data = data.decode("utf-8", errors="replace")
        return json.loads(data)


logger = logging.getLogger("core.investment_advisor_engine")

RIYADH_TZ = timezone(timedelta(hours=3))
_TRUTHY = {"1", "true", "yes", "y", "on", "t", "ok", "enabled", "active"}

DEFAULT_SOURCE_PAGES = [
    "Market_Leaders",
    "Global_Markets",
    "Mutual_Funds",
    "Commodities_FX",
    "My_Portfolio",
]

# -----------------------------------------------------------------------------
# Optional imports
# -----------------------------------------------------------------------------
try:
    from core.investment_advisor import run_investment_advisor  # type: ignore

    _HAS_CORE_ADVISOR = True
except Exception:
    run_investment_advisor = None  # type: ignore
    _HAS_CORE_ADVISOR = False

try:
    from core.sheets.page_catalog import normalize_page_name as _normalize_page_name  # type: ignore

    _HAS_PAGE_CATALOG = True
except Exception:
    _HAS_PAGE_CATALOG = False

    def _normalize_page_name(name: str, allow_output_pages: bool = True) -> str:  # type: ignore
        return str(name or "").strip()

try:
    from core.sheets.schema_registry import get_sheet_spec as _get_sheet_spec  # type: ignore

    _HAS_SCHEMA = True
except Exception:
    _HAS_SCHEMA = False

    def _get_sheet_spec(sheet_name: str) -> Any:  # type: ignore
        raise KeyError("schema_registry unavailable")


# -----------------------------------------------------------------------------
# Utilities
# -----------------------------------------------------------------------------
def _truthy(v: Any) -> bool:
    if isinstance(v, bool):
        return v
    if v is None:
        return False
    return str(v).strip().lower() in _TRUTHY


def _safe_str(v: Any, default: str = "") -> str:
    try:
        return str(v).strip() if v is not None else default
    except Exception:
        return default


def _to_float(v: Any) -> Optional[float]:
    if v is None:
        return None
    try:
        if isinstance(v, (int, float)) and not isinstance(v, bool):
            f = float(v)
        else:
            s = str(v).strip().replace(",", "")
            if not s or s.lower() in {"na", "n/a", "none", "null"}:
                return None
            if s.endswith("%"):
                f = float(s[:-1].strip()) / 100.0
            else:
                f = float(s)
        if math.isnan(f) or math.isinf(f):
            return None
        return f
    except Exception:
        return None


def _to_int(v: Any) -> Optional[int]:
    f = _to_float(v)
    return int(f) if f is not None else None


def _as_ratio(v: Any) -> Optional[float]:
    f = _to_float(v)
    if f is None:
        return None
    if abs(f) > 1.5:
        return f / 100.0
    return f


def _norm_key(s: Any) -> str:
    return " ".join(_safe_str(s).lower().split())


def _snake_like(header: str) -> str:
    s = _safe_str(header)
    s = s.strip().replace("%", " pct").replace("/", " ")
    out = []
    prev_us = False
    for ch in s:
        if ch.isalnum():
            out.append(ch.lower())
            prev_us = False
        else:
            if not prev_us:
                out.append("_")
                prev_us = True
    res = "".join(out).strip("_")
    while "__" in res:
        res = res.replace("__", "_")
    return res


def _iso_utc(dt: datetime) -> str:
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc).isoformat()


def _iso_riyadh(dt: datetime) -> str:
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(RIYADH_TZ).isoformat()


def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


def _parse_iso_dt(v: Any) -> Optional[datetime]:
    s = _safe_str(v)
    if not s:
        return None
    try:
        if s.endswith("Z"):
            s = s[:-1] + "+00:00"
        dt = datetime.fromisoformat(s)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except Exception:
        return None


def _clean_reason_text(v: Any) -> str:
    s = _safe_str(v)
    if not s:
        return ""
    s = " ".join(s.split())
    return s.strip(" ;,.-")


def _pick(d: Dict[str, Any], *keys: str) -> Any:
    if not isinstance(d, dict):
        return None
    for k in keys:
        if k in d:
            return d.get(k)
    nmap = d.get("_nmap")
    if not isinstance(nmap, dict):
        nmap = {_norm_key(k): k for k in d.keys() if k != "_nmap"}
        d["_nmap"] = nmap
    for k in keys:
        nk = _norm_key(k)
        if nk in nmap:
            return d.get(nmap[nk])
    return None


def _normalize_symbol(symbol: Any) -> str:
    s = _safe_str(symbol).upper().replace(" ", "")
    if s.startswith("TADAWUL:"):
        s = s.split(":", 1)[1]
    if s.endswith(".SA"):
        s = s[:-3] + ".SR"
    return s


def _canonical_symbol(symbol: Any) -> str:
    s = _normalize_symbol(symbol)
    if not s:
        return ""
    if s.endswith(".SR") and s[:-3].isdigit():
        return f"KSA:{s[:-3]}"
    if s.isdigit():
        return f"KSA:{s}"
    if "." in s:
        return f"GLOBAL:{s.split('.', 1)[0]}"
    if s.startswith("^"):
        return f"IDX:{s}"
    return f"GLOBAL:{s}"


# -----------------------------------------------------------------------------
# Sync/async bridge
# -----------------------------------------------------------------------------
def _in_running_loop() -> bool:
    try:
        loop = asyncio.get_running_loop()
        return bool(loop.is_running())
    except Exception:
        return False


def _run_coro_in_thread(coro: Any, timeout: float = 20.0) -> Tuple[Optional[Any], Optional[BaseException], bool]:
    box: Dict[str, Any] = {"result": None, "error": None}

    def _worker():
        try:
            box["result"] = asyncio.run(asyncio.wait_for(coro, timeout=timeout))
        except BaseException as e:  # noqa
            box["error"] = e

    t = threading.Thread(target=_worker, daemon=True)
    t.start()
    t.join(timeout + 0.25)
    if t.is_alive():
        return None, None, True
    return box["result"], box["error"], False


def _safe_call(obj: Any, method_name: str, *args: Any, timeout: float = 20.0, **kwargs: Any) -> Tuple[Optional[Any], Optional[str]]:
    if obj is None:
        return None, "target_none"
    fn = getattr(obj, method_name, None)
    if not callable(fn):
        return None, "method_not_found"

    try:
        out = fn(*args, **kwargs)
        if inspect.isawaitable(out):
            if _in_running_loop():
                res, err, to = _run_coro_in_thread(out, timeout=timeout)
                if to:
                    return None, f"timeout:{timeout}s"
                if err is not None:
                    return None, f"exception:{type(err).__name__}:{err}"
                return res, None
            try:
                return asyncio.run(asyncio.wait_for(out, timeout=timeout)), None
            except asyncio.TimeoutError:
                return None, f"timeout:{timeout}s"
        return out, None
    except Exception as e:
        return None, f"exception:{type(e).__name__}:{e}"


# -----------------------------------------------------------------------------
# Criteria model
# -----------------------------------------------------------------------------
@dataclass(slots=True)
class AdvisorCriteria:
    sources: List[str] = field(default_factory=lambda: ["ALL"])
    tickers: Optional[List[str]] = None

    risk_profile: str = "moderate"
    risk_bucket: str = ""
    confidence_bucket: str = ""

    invest_amount: float = 0.0
    currency: str = "SAR"
    top_n: int = 20
    allocation_strategy: str = "maximum_sharpe"

    invest_period_days: Optional[int] = None
    invest_period_label: str = ""
    horizon_days: Optional[int] = None

    required_roi_1m: Optional[float] = None
    required_roi_3m: Optional[float] = None
    required_roi_12m: Optional[float] = None

    min_price: Optional[float] = None
    max_price: Optional[float] = None
    min_liquidity_score: float = 25.0
    min_advisor_score: Optional[float] = None

    exclude_sectors: Optional[List[str]] = None
    allowed_markets: Optional[List[str]] = None

    use_row_updated_at: bool = True
    debug: bool = False

    raw: Dict[str, Any] = field(default_factory=dict)

    def to_payload(self) -> Dict[str, Any]:
        out: Dict[str, Any] = {
            "sources": self.sources,
            "tickers": self.tickers,
            "risk_profile": self.risk_profile,
            "risk_bucket": self.risk_bucket,
            "confidence_bucket": self.confidence_bucket,
            "invest_amount": self.invest_amount,
            "currency": self.currency,
            "top_n": self.top_n,
            "allocation_strategy": self.allocation_strategy,
            "required_roi_1m": self.required_roi_1m,
            "required_roi_3m": self.required_roi_3m,
            "required_roi_12m": self.required_roi_12m,
            "min_price": self.min_price,
            "max_price": self.max_price,
            "min_liquidity_score": self.min_liquidity_score,
            "min_advisor_score": self.min_advisor_score,
            "exclude_sectors": self.exclude_sectors,
            "allowed_markets": self.allowed_markets,
            "use_row_updated_at": self.use_row_updated_at,
            "debug": self.debug,
            "horizon_days": self.horizon_days,
            "invest_period_days": self.invest_period_days,
            "invest_period_label": self.invest_period_label,
        }
        out.update(self.raw or {})
        return out


# -----------------------------------------------------------------------------
# Horizon helpers
# -----------------------------------------------------------------------------
def _normalize_sources(raw_sources: Any) -> List[str]:
    if isinstance(raw_sources, str):
        raw_list = [raw_sources]
    elif isinstance(raw_sources, list):
        raw_list = [str(x) for x in raw_sources if x]
    else:
        raw_list = ["ALL"]

    out: List[str] = []
    seen = set()

    for s in raw_list:
        txt = _safe_str(s)
        if not txt:
            continue
        if txt.upper() == "ALL":
            for page in DEFAULT_SOURCE_PAGES:
                if page not in seen:
                    out.append(page)
                    seen.add(page)
            continue
        try:
            page = _normalize_page_name(txt, allow_output_pages=False) if _HAS_PAGE_CATALOG else txt
        except Exception:
            page = txt
        if page and page not in seen:
            out.append(page)
            seen.add(page)

    return out or list(DEFAULT_SOURCE_PAGES)


def _normalize_tickers(raw: Any) -> Optional[List[str]]:
    if raw is None:
        return None
    if isinstance(raw, str):
        vals = [x for x in raw.replace(",", " ").split() if x.strip()]
        return vals or None
    if isinstance(raw, list):
        vals = [_safe_str(x) for x in raw if _safe_str(x)]
        return vals or None
    return None


def _period_label_from_days(days: Optional[int]) -> str:
    if days is None:
        return ""
    d = int(days)
    if d <= 0:
        return ""
    if d <= 31:
        return "1M"
    if d <= 92:
        return "3M"
    if d <= 183:
        return "6M"
    if d <= 366:
        return "12M"
    return f"{d}D"


def _infer_horizon_days(payload: Dict[str, Any]) -> Tuple[Optional[int], str]:
    """
    Priority:
    1) explicit horizon_days
    2) explicit invest_period_days / investment_period_days / period_days
    3) invest_period_label
    4) if ROI target fields supplied -> infer nearest canonical horizon
    default -> 90 days / 3M
    """
    explicit = (
        _to_int(payload.get("horizon_days"))
        or _to_int(payload.get("invest_period_days"))
        or _to_int(payload.get("investment_period_days"))
        or _to_int(payload.get("period_days"))
        or _to_int(payload.get("days"))
    )
    if explicit and explicit > 0:
        return explicit, _period_label_from_days(explicit)

    label = _safe_str(
        payload.get("invest_period_label")
        or payload.get("investment_period_label")
        or payload.get("period_label")
        or payload.get("horizon_label")
    ).upper()

    label_map = {
        "1M": 30,
        "30D": 30,
        "30": 30,
        "3M": 90,
        "90D": 90,
        "90": 90,
        "6M": 180,
        "180D": 180,
        "180": 180,
        "12M": 365,
        "1Y": 365,
        "365D": 365,
        "365": 365,
    }
    if label in label_map:
        return label_map[label], _period_label_from_days(label_map[label])

    if payload.get("required_roi_1m") is not None:
        return 30, "1M"
    if payload.get("required_roi_3m") is not None:
        return 90, "3M"
    if payload.get("required_roi_12m") is not None:
        return 365, "12M"

    return 90, "3M"


def _build_criteria(payload: Dict[str, Any]) -> AdvisorCriteria:
    horizon_days, invest_period_label = _infer_horizon_days(payload or {})
    return AdvisorCriteria(
        sources=_normalize_sources((payload or {}).get("sources")),
        tickers=_normalize_tickers((payload or {}).get("tickers") or (payload or {}).get("symbols")),
        risk_profile=_safe_str((payload or {}).get("risk_profile") or "moderate").lower() or "moderate",
        risk_bucket=_safe_str((payload or {}).get("risk_bucket") or (payload or {}).get("risk")),
        confidence_bucket=_safe_str((payload or {}).get("confidence_bucket") or (payload or {}).get("confidence")),
        invest_amount=_to_float((payload or {}).get("invest_amount")) or 0.0,
        currency=_safe_str((payload or {}).get("currency") or "SAR").upper() or "SAR",
        top_n=max(1, min(200, _to_int((payload or {}).get("top_n")) or 20)),
        allocation_strategy=_safe_str((payload or {}).get("allocation_strategy") or "maximum_sharpe").lower(),
        invest_period_days=horizon_days,
        invest_period_label=invest_period_label,
        horizon_days=horizon_days,
        required_roi_1m=_as_ratio((payload or {}).get("required_roi_1m")),
        required_roi_3m=_as_ratio((payload or {}).get("required_roi_3m")),
        required_roi_12m=_as_ratio((payload or {}).get("required_roi_12m")),
        min_price=_to_float((payload or {}).get("min_price")),
        max_price=_to_float((payload or {}).get("max_price")),
        min_liquidity_score=_to_float((payload or {}).get("min_liquidity_score")) or 25.0,
        min_advisor_score=_to_float((payload or {}).get("min_advisor_score")),
        exclude_sectors=(payload or {}).get("exclude_sectors"),
        allowed_markets=(payload or {}).get("allowed_markets"),
        use_row_updated_at=_truthy((payload or {}).get("use_row_updated_at", True)),
        debug=_truthy((payload or {}).get("debug", False)),
        raw=dict(payload or {}),
    )


# -----------------------------------------------------------------------------
# Reason synthesis
# -----------------------------------------------------------------------------
def _fmt_pct(r: Optional[float], digits: int = 1) -> Optional[str]:
    if r is None:
        return None
    try:
        return f"{float(r) * 100:.{digits}f}%"
    except Exception:
        return None


def _fmt_num(v: Optional[float], digits: int = 1) -> Optional[str]:
    if v is None:
        return None
    try:
        return f"{float(v):.{digits}f}"
    except Exception:
        return None


def _build_fallback_recommendation_reason(item: Dict[str, Any], criteria: AdvisorCriteria) -> str:
    rec = _safe_str(item.get("recommendation") or "HOLD").upper()
    current_price = _to_float(item.get("current_price") or item.get("price"))
    fp1 = _to_float(item.get("forecast_price_1m"))
    fp3 = _to_float(item.get("forecast_price_3m"))
    fp12 = _to_float(item.get("forecast_price_12m"))
    roi1 = _as_ratio(item.get("expected_roi_1m"))
    roi3 = _as_ratio(item.get("expected_roi_3m"))
    roi12 = _as_ratio(item.get("expected_roi_12m"))
    overall = _to_float(item.get("overall_score"))
    confidence = _to_float(item.get("confidence_score"))
    risk = _to_float(item.get("risk_score"))
    valuation = _to_float(item.get("valuation_score"))
    momentum = _to_float(item.get("momentum_score"))
    quality = _to_float(item.get("quality_score"))
    opportunity = _to_float(item.get("opportunity_score"))
    liquidity = _to_float(item.get("liquidity_score"))

    horizon = criteria.horizon_days or 90
    if horizon <= 31:
        horizon_label = "1M"
        chosen_roi = roi1 if roi1 is not None else roi3
        chosen_fp = fp1 if fp1 is not None else fp3
    elif horizon <= 92:
        horizon_label = "3M"
        chosen_roi = roi3 if roi3 is not None else roi12
        chosen_fp = fp3 if fp3 is not None else fp12
    else:
        horizon_label = criteria.invest_period_label or _period_label_from_days(horizon) or "12M"
        chosen_roi = roi12 if roi12 is not None else roi3
        chosen_fp = fp12 if fp12 is not None else fp3

    parts: List[str] = []

    if rec == "BUY":
        if chosen_roi is not None and chosen_roi > 0:
            parts.append(f"expected {horizon_label} upside is {_fmt_pct(chosen_roi)}")
        if chosen_fp is not None and current_price is not None and chosen_fp > current_price:
            parts.append(f"forecast price {_fmt_num(chosen_fp, 2)} is above current price {_fmt_num(current_price, 2)}")
        if confidence is not None and confidence >= 70:
            parts.append(f"confidence is high at {_fmt_num(confidence, 0)}/100")
        if overall is not None and overall >= 65:
            parts.append(f"overall score is strong at {_fmt_num(overall, 0)}/100")
        if valuation is not None and valuation >= 60:
            parts.append(f"valuation is supportive at {_fmt_num(valuation, 0)}/100")
        if quality is not None and quality >= 60:
            parts.append(f"quality is supportive at {_fmt_num(quality, 0)}/100")
        if momentum is not None and momentum >= 60:
            parts.append(f"momentum is positive at {_fmt_num(momentum, 0)}/100")
        if opportunity is not None and opportunity >= 60:
            parts.append(f"opportunity score is favorable at {_fmt_num(opportunity, 0)}/100")
        if risk is not None and risk <= 45:
            parts.append(f"risk remains contained at {_fmt_num(risk, 0)}/100")

    elif rec == "SELL":
        if chosen_roi is not None and chosen_roi < 0:
            parts.append(f"expected {horizon_label} return is negative at {_fmt_pct(chosen_roi)}")
        if chosen_fp is not None and current_price is not None and chosen_fp < current_price:
            parts.append(f"forecast price {_fmt_num(chosen_fp, 2)} is below current price {_fmt_num(current_price, 2)}")
        if risk is not None and risk >= 65:
            parts.append(f"risk is elevated at {_fmt_num(risk, 0)}/100")
        if overall is not None and overall <= 40:
            parts.append(f"overall score is weak at {_fmt_num(overall, 0)}/100")
        if momentum is not None and momentum <= 40:
            parts.append(f"momentum is weak at {_fmt_num(momentum, 0)}/100")
        if confidence is not None and confidence <= 40:
            parts.append(f"confidence is low at {_fmt_num(confidence, 0)}/100")

    else:
        if chosen_roi is not None:
            if abs(chosen_roi) <= 0.03:
                parts.append(f"expected {horizon_label} move is limited at {_fmt_pct(chosen_roi)}")
            elif chosen_roi > 0:
                parts.append(f"upside exists but remains moderate at {_fmt_pct(chosen_roi)}")
            else:
                parts.append(f"downside exists but remains moderate at {_fmt_pct(chosen_roi)}")
        if overall is not None:
            parts.append(f"overall score is balanced at {_fmt_num(overall, 0)}/100")
        if risk is not None and 40 <= risk <= 60:
            parts.append(f"risk is moderate at {_fmt_num(risk, 0)}/100")
        if confidence is not None and 45 <= confidence <= 69:
            parts.append(f"confidence is moderate at {_fmt_num(confidence, 0)}/100")

    if liquidity is not None:
        if liquidity >= 65:
            parts.append(f"liquidity is healthy at {_fmt_num(liquidity, 0)}/100")
        elif liquidity <= 25:
            parts.append(f"liquidity is weak at {_fmt_num(liquidity, 0)}/100")

    if not parts:
        if rec == "BUY":
            return f"BUY because the {horizon_label} risk-return profile is favorable."
        if rec == "SELL":
            return f"SELL because the {horizon_label} risk-return profile is unfavorable."
        return f"HOLD because the {horizon_label} risk-return profile is balanced."

    return f"{rec} because " + ", ".join(parts[:4]) + "."


# -----------------------------------------------------------------------------
# Canonical output enrichment
# -----------------------------------------------------------------------------
def _ensure_items_shape(result: Dict[str, Any]) -> List[Dict[str, Any]]:
    items = result.get("items")
    if isinstance(items, list) and all(isinstance(x, dict) for x in items):
        return items

    headers = result.get("headers") or []
    rows = result.get("rows") or []
    if isinstance(headers, list) and isinstance(rows, list):
        out: List[Dict[str, Any]] = []
        for r in rows:
            if isinstance(r, (list, tuple)):
                d: Dict[str, Any] = {}
                for i, h in enumerate(headers):
                    key = _snake_like(_safe_str(h))
                    d[key] = r[i] if i < len(r) else None
                out.append(d)
        return out
    return []


def _enrich_item_with_context(item: Dict[str, Any], criteria: AdvisorCriteria, meta: Dict[str, Any]) -> Dict[str, Any]:
    out = dict(item or {})

    out["symbol"] = _normalize_symbol(out.get("symbol") or out.get("ticker") or out.get("code"))
    out["canonical"] = out.get("canonical") or _canonical_symbol(out.get("symbol"))
    out["invest_period_label"] = _safe_str(out.get("invest_period_label")) or criteria.invest_period_label
    out["horizon_days"] = _to_int(out.get("horizon_days")) or criteria.horizon_days
    out["invest_period_days"] = _to_int(out.get("invest_period_days")) or criteria.invest_period_days or criteria.horizon_days

    if not out.get("last_updated_utc"):
        now_dt = _now_utc()
        out["last_updated_utc"] = _iso_utc(now_dt)
        out["last_updated_riyadh"] = _iso_riyadh(now_dt)
    elif not out.get("last_updated_riyadh"):
        parsed = _parse_iso_dt(out.get("last_updated_utc")) or _now_utc()
        out["last_updated_riyadh"] = _iso_riyadh(parsed)

    reason = _clean_reason_text(out.get("recommendation_reason"))
    if not reason:
        reason = _build_fallback_recommendation_reason(out, criteria)
    out["recommendation_reason"] = reason

    out["advisor_context"] = {
        "horizon_days": out.get("horizon_days"),
        "invest_period_label": out.get("invest_period_label"),
        "risk_profile": criteria.risk_profile,
        "risk_bucket": criteria.risk_bucket,
        "confidence_bucket": criteria.confidence_bucket,
        "allocation_strategy": criteria.allocation_strategy,
    }

    return out


def _enrich_result(result: Dict[str, Any], criteria: AdvisorCriteria) -> Dict[str, Any]:
    out = dict(result or {})
    items = _ensure_items_shape(out)
    meta = dict(out.get("meta") or {})

    enriched_items = [_enrich_item_with_context(x, criteria, meta) for x in items]
    out["items"] = enriched_items

    headers = out.get("headers")
    rows = out.get("rows")

    if isinstance(headers, list) and isinstance(rows, list):
        hdr_index = {str(h): i for i, h in enumerate(headers)}
        need_new_headers = []
        for h in ["Recommendation Reason", "Horizon Days", "Invest Period Label"]:
            if h not in hdr_index:
                need_new_headers.append(h)

        if need_new_headers:
            headers = list(headers) + need_new_headers
            hdr_index = {str(h): i for i, h in enumerate(headers)}
            new_rows: List[List[Any]] = []
            for idx, r in enumerate(rows):
                rr = list(r) if isinstance(r, (list, tuple)) else []
                while len(rr) < len(headers):
                    rr.append(None)
                item = enriched_items[idx] if idx < len(enriched_items) else {}
                rr[hdr_index["Recommendation Reason"]] = item.get("recommendation_reason")
                rr[hdr_index["Horizon Days"]] = item.get("horizon_days")
                rr[hdr_index["Invest Period Label"]] = item.get("invest_period_label")
                new_rows.append(rr)
            out["headers"] = headers
            out["rows"] = new_rows

    meta["engine_version"] = "5.0.0"
    meta["core_advisor_available"] = bool(_HAS_CORE_ADVISOR)
    meta["criteria"] = {
        "sources": criteria.sources,
        "tickers": criteria.tickers,
        "risk_profile": criteria.risk_profile,
        "risk_bucket": criteria.risk_bucket,
        "confidence_bucket": criteria.confidence_bucket,
        "invest_amount": criteria.invest_amount,
        "currency": criteria.currency,
        "top_n": criteria.top_n,
        "allocation_strategy": criteria.allocation_strategy,
        "horizon_days": criteria.horizon_days,
        "invest_period_days": criteria.invest_period_days,
        "invest_period_label": criteria.invest_period_label,
        "required_roi_1m": criteria.required_roi_1m,
        "required_roi_3m": criteria.required_roi_3m,
        "required_roi_12m": criteria.required_roi_12m,
    }
    meta["context_propagation"] = {
        "recommendation_reason": True,
        "horizon_days": True,
        "invest_period_label": True,
    }
    out["meta"] = meta
    return out


# -----------------------------------------------------------------------------
# Engine class
# -----------------------------------------------------------------------------
class InvestmentAdvisorEngine:
    """
    Thin but intelligent engine layer:
    - normalizes advisory request/criteria
    - delegates to data_engine / quote_engine where available
    - calls core.investment_advisor.run_investment_advisor(...)
    - enriches final canonical output with advisory context
    """

    def __init__(
        self,
        *,
        data_engine: Any = None,
        quote_engine: Any = None,
        cache_engine: Any = None,
        settings: Optional[Dict[str, Any]] = None,
    ) -> None:
        self.data_engine = data_engine
        self.quote_engine = quote_engine or data_engine
        self.cache_engine = cache_engine or data_engine
        self.settings = dict(settings or {})

    # ------------------------------------------------------------------
    # engine compatibility methods used by core/investment_advisor.py
    # ------------------------------------------------------------------
    def get_cached_multi_sheet_snapshots(self, sheet_names: List[str]) -> Dict[str, Dict[str, Any]]:
        sheet_names = [str(x) for x in (sheet_names or []) if str(x).strip()]
        out: Dict[str, Dict[str, Any]] = {}

        targets = [self.cache_engine, self.data_engine]
        for target in targets:
            if target is None:
                continue

            for method in ["get_cached_multi_sheet_snapshots", "get_multi_sheet_snapshots"]:
                res, err = _safe_call(target, method, sheet_names)
                if err is None and isinstance(res, dict):
                    for k, v in res.items():
                        if isinstance(v, dict):
                            out[str(k)] = v
                    if out:
                        return out

        for s in sheet_names:
            one = self.get_cached_sheet_snapshot(s)
            if isinstance(one, dict):
                out[s] = one
        return out

    def get_cached_sheet_snapshot(self, sheet_name: str) -> Optional[Dict[str, Any]]:
        targets = [self.cache_engine, self.data_engine]
        for target in targets:
            if target is None:
                continue
            for method in ["get_cached_sheet_snapshot", "get_sheet_snapshot", "read_cached_sheet_snapshot"]:
                res, err = _safe_call(target, method, sheet_name)
                if err is None and isinstance(res, dict):
                    return res
        return None

    def get_sheet_rows(
        self,
        sheet: Optional[str] = None,
        limit: int = 5000,
        offset: int = 0,
        mode: Optional[str] = None,
        body: Optional[Dict[str, Any]] = None,
        **kwargs: Any,
    ) -> Dict[str, Any]:
        page = sheet or kwargs.get("sheet_name") or kwargs.get("page") or kwargs.get("name") or ""
        page = _normalize_page_name(page, allow_output_pages=True) if page else page

        targets = [self.data_engine]
        method_candidates = [
            "get_sheet_rows",
            "sheet_rows",
            "build_sheet_rows",
            "get_sheet",
            "fetch_sheet",
            "get_rows",
        ]

        for target in targets:
            if target is None:
                continue
            for method in method_candidates:
                variants = [
                    ((), {"sheet": page, "limit": limit, "offset": offset, "mode": mode, "body": body or {}}),
                    ((), {"sheet": page, "limit": limit, "offset": offset}),
                    ((), {"page": page, "limit": limit, "offset": offset}),
                    ((page,), {}),
                ]
                for args, kw in variants:
                    res, err = _safe_call(target, method, *args, **{k: v for k, v in kw.items() if v is not None})
                    if err is None and isinstance(res, dict):
                        return res

        return {"headers": [], "rows": [], "rows_matrix": [], "meta": {"ok": False, "sheet": page, "error": "sheet rows unavailable"}}

    def get_enriched_quotes_batch(self, symbols: List[str]) -> Dict[str, Dict[str, Any]]:
        syms = [_normalize_symbol(x) for x in (symbols or []) if _normalize_symbol(x)]
        if not syms:
            return {}

        targets = [self.quote_engine, self.data_engine]
        methods = [
            "get_enriched_quotes_batch",
            "get_quotes_batch",
            "fetch_quotes_batch",
            "batch_quotes",
        ]
        for target in targets:
            if target is None:
                continue
            for method in methods:
                for args, kwargs in [
                    ((), {"symbols": syms}),
                    ((syms,), {}),
                    ((), {"tickers": syms}),
                ]:
                    res, err = _safe_call(target, method, *args, **kwargs)
                    if err is None and isinstance(res, dict):
                        out: Dict[str, Dict[str, Any]] = {}
                        for k, v in res.items():
                            if isinstance(v, dict):
                                out[_normalize_symbol(k)] = dict(v)
                        if out:
                            return out
        return {}

    # ------------------------------------------------------------------
    # primary runner
    # ------------------------------------------------------------------
    def run(self, payload: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        payload = dict(payload or {})
        criteria = _build_criteria(payload)

        forwarded_payload = criteria.to_payload()
        forwarded_payload.setdefault("advisor_data_mode", payload.get("advisor_data_mode") or payload.get("data_mode") or "live_quotes")

        if not _HAS_CORE_ADVISOR or run_investment_advisor is None:
            return {
                "headers": [],
                "rows": [],
                "items": [],
                "meta": {
                    "ok": False,
                    "engine_version": "5.0.0",
                    "error": "core.investment_advisor.run_investment_advisor unavailable",
                    "criteria": asdict(criteria),
                },
            }

        result = run_investment_advisor(forwarded_payload, engine=self)
        if not isinstance(result, dict):
            return {
                "headers": [],
                "rows": [],
                "items": [],
                "meta": {
                    "ok": False,
                    "engine_version": "5.0.0",
                    "error": "advisor returned invalid response type",
                    "criteria": asdict(criteria),
                },
            }

        enriched = _enrich_result(result, criteria)
        return enriched

    # ------------------------------------------------------------------
    # optional async wrapper
    # ------------------------------------------------------------------
    async def run_async(self, payload: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        return self.run(payload)

    # ------------------------------------------------------------------
    # convenience health/debug snapshot
    # ------------------------------------------------------------------
    def health_snapshot(self) -> Dict[str, Any]:
        return {
            "ok": True,
            "engine_version": "5.0.0",
            "core_advisor_available": bool(_HAS_CORE_ADVISOR),
            "page_catalog": bool(_HAS_PAGE_CATALOG),
            "schema_registry": bool(_HAS_SCHEMA),
            "data_engine": type(self.data_engine).__name__ if self.data_engine is not None else None,
            "quote_engine": type(self.quote_engine).__name__ if self.quote_engine is not None else None,
            "cache_engine": type(self.cache_engine).__name__ if self.cache_engine is not None else None,
            "timestamp_utc": _iso_utc(_now_utc()),
        }


# -----------------------------------------------------------------------------
# module-level convenience function
# -----------------------------------------------------------------------------
def run_investment_advisor_engine(payload: Optional[Dict[str, Any]] = None, *, data_engine: Any = None, quote_engine: Any = None, cache_engine: Any = None) -> Dict[str, Any]:
    engine = InvestmentAdvisorEngine(
        data_engine=data_engine,
        quote_engine=quote_engine,
        cache_engine=cache_engine,
    )
    return engine.run(payload or {})


__all__ = [
    "InvestmentAdvisorEngine",
    "AdvisorCriteria",
    "run_investment_advisor_engine",
]
