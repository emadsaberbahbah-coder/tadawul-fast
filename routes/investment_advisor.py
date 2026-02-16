# routes/investment_advisor.py
"""
routes/investment_advisor.py
------------------------------------------------------------
Tadawul Fast Bridge — Investment Advisor Routes (GOOGLE SHEETS SAFE)
FULL REPLACEMENT — v2.4.0 (Config-Aware Auth + Payload Hardening + Diagnostics + Gains)

Endpoints
- GET  /v1/advisor/health
- GET  /v1/advisor/options
- POST /v1/advisor/recommendations
- POST /v1/advisor/run                 (alias)

Primary goals
- ✅ PROD SAFE: never crash on missing optional modules (config/core/engine).
- ✅ Sheets safe: headers NEVER empty on ok/error; rows are rectangular.
- ✅ Auth: open if no tokens configured; supports dynamic header name (routes.config.AUTH_HEADER_NAME)
        + Authorization: Bearer <token>
        + optional ?token=... when ALLOW_QUERY_TOKEN=1
- ✅ Payload hardening: accepts many shapes (GAS, JSON, strings, nested criteria, percent-ish).
- ✅ Diagnostics: snapshot cache hit stats by source + filtering summaries.
- ✅ Optional post-processing:
        - compute Allocated Amount (if missing) from Allocation % + invest_amount
        - compute Expected Gain/Loss (1M/3M) when possible (adds columns at END only)

Canonical alignment
- required_roi_1m / required_roi_3m accepted as percent or ratio; sent to core as ratio (0..1).
- expected_roi_1m/3m/12m and forecast_price_1m/3m/12m are preserved if returned by core.

Environment (safe defaults)
- ADVISOR_TIMEOUT_SEC (default 60) [best-effort; only enforced for awaitables]
- ADVISOR_MAX_TICKERS (default 2500)
"""

from __future__ import annotations

import asyncio
import inspect
import logging
import os
import time
from datetime import datetime, timezone, timedelta
from functools import lru_cache
from typing import Any, Dict, List, Optional, Sequence, Tuple

from fastapi import APIRouter, Body, Query, Request

logger = logging.getLogger("routes.investment_advisor")

ADVISOR_ROUTE_VERSION = "2.4.0"
router = APIRouter(prefix="/v1/advisor", tags=["investment_advisor"])

_TRUTHY = {"1", "true", "yes", "y", "on", "t"}
_RIYADH_TZ = timezone(timedelta(hours=3))

# Canonical/default pages (must match GAS tab names)
DEFAULT_SOURCES = ["Market_Leaders", "Global_Markets", "Mutual_Funds", "Commodities_FX"]

# Sheets-safe default headers (minimum stable contract)
TT_ADVISOR_DEFAULT_HEADERS: List[str] = [
    "Rank",
    "Symbol",
    "Origin",
    "Name",
    "Market",
    "Currency",
    "Price",
    "Advisor Score",
    "Action",
    "Allocation %",
    "Allocation Amount",
    "Expected ROI % (1M)",
    "Forecast Price (1M)",
    "Expected ROI % (3M)",
    "Forecast Price (3M)",
    "Expected ROI % (12M)",
    "Forecast Price (12M)",
    "Risk Bucket",
    "Confidence Bucket",
    "Reason (Explain)",
    "Data Source",
    "Data Quality",
    "Last Updated (UTC)",
    "Last Updated (Riyadh)",
]

# Computed columns (we ONLY add at end, never reorder existing headers)
ALLOC_AMOUNT_COL = "Allocated Amount (SAR)"
GAIN_COL_1M = "Expected Gain/Loss 1M (SAR)"
GAIN_COL_3M = "Expected Gain/Loss 3M (SAR)"


# =============================================================================
# Time helpers
# =============================================================================
def _utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _riyadh_iso(utc_iso: Optional[str] = None) -> str:
    try:
        if utc_iso:
            s = str(utc_iso).replace("Z", "+00:00")
            dt = datetime.fromisoformat(s)
        else:
            dt = datetime.now(timezone.utc)
        return dt.astimezone(_RIYADH_TZ).isoformat()
    except Exception:
        return datetime.now(_RIYADH_TZ).isoformat()


# =============================================================================
# Config/Auth integration (routes.config preferred)
# =============================================================================
@lru_cache(maxsize=1)
def _routes_config_mod():
    try:
        import routes.config as rc  # type: ignore

        return rc
    except Exception:
        return None


def _truthy(v: Any) -> bool:
    return str(v or "").strip().lower() in _TRUTHY


def _allowed_tokens_env() -> List[str]:
    toks: List[str] = []
    for k in ("APP_TOKEN", "BACKUP_APP_TOKEN", "TFB_APP_TOKEN"):
        v = (os.getenv(k) or "").strip()
        if v:
            toks.append(v)
    # de-dupe preserve order
    out: List[str] = []
    seen = set()
    for t in toks:
        if t not in seen:
            seen.add(t)
            out.append(t)
    return out


def _open_mode() -> bool:
    """
    Prefer routes.config.allowed_tokens when available; else env-only.
    """
    rc = _routes_config_mod()
    try:
        if rc is not None and callable(getattr(rc, "allowed_tokens", None)):
            return len(rc.allowed_tokens() or []) == 0  # type: ignore[attr-defined]
    except Exception:
        pass
    return len(_allowed_tokens_env()) == 0


def _auth_header_name() -> str:
    rc = _routes_config_mod()
    name = None
    if rc is not None:
        name = getattr(rc, "AUTH_HEADER_NAME", None)
        if not name:
            try:
                gs = getattr(rc, "get_settings", None)
                if callable(gs):
                    s = gs()
                    name = getattr(s, "auth_header_name", None)
            except Exception:
                name = None
    # env override as last-mile
    env_name = (os.getenv("AUTH_HEADER_NAME") or "").strip()
    if env_name:
        return env_name
    return str(name or "X-APP-TOKEN").strip() or "X-APP-TOKEN"


def _auth_ok_request(request: Request, query_token: Optional[str]) -> bool:
    """
    Uses routes.config.auth_ok_request if available; else env-only.
    Open mode if no tokens configured.
    """
    rc = _routes_config_mod()

    # Preferred: centralized config helper
    if rc is not None and callable(getattr(rc, "auth_ok_request", None)):
        try:
            x_app = request.headers.get(_auth_header_name()) or request.headers.get("X-APP-TOKEN")
            authorization = request.headers.get("Authorization")
            return bool(rc.auth_ok_request(x_app_token=x_app, authorization=authorization, query_token=query_token))  # type: ignore[attr-defined]
        except Exception:
            pass

    # Fallback: env-only
    allowed = _allowed_tokens_env()
    if not allowed:
        return True

    # Extract token locally
    token = (request.headers.get(_auth_header_name()) or request.headers.get("X-APP-TOKEN") or "").strip()
    if token and token in set(allowed):
        return True

    auth = (request.headers.get("Authorization") or "").strip()
    if auth.lower().startswith("bearer "):
        bt = auth.split(" ", 1)[1].strip()
        if bt and bt in set(allowed):
            return True

    if _truthy(os.getenv("ALLOW_QUERY_TOKEN", "")):
        qt = (query_token or "").strip()
        if qt and qt in set(allowed):
            return True

    return False


# =============================================================================
# Engine resolution + snapshot probes
# =============================================================================
async def _maybe_await(x: Any) -> Any:
    if inspect.isawaitable(x):
        return await x
    return x


async def _resolve_engine(request: Request) -> Any:
    # 1) app.state.engine
    try:
        st = getattr(request.app, "state", None)
        eng = getattr(st, "engine", None) if st else None
        if eng:
            return eng
    except Exception:
        pass

    # 2) singleton (best-effort)
    try:
        from core.data_engine_v2 import get_engine  # type: ignore

        eng2 = get_engine()
        return await _maybe_await(eng2)
    except Exception:
        return None


def _probe_engine_snapshots(engine: Any, sources: List[str]) -> Tuple[Dict[str, Dict[str, Any]], int]:
    """
    Returns (stats_map, hit_count)
    - Works with engines exposing get_cached_sheet_snapshot(name)
    - stats per source: {hit, rows, headers, updated_utc, updated_riyadh}
    """
    stats: Dict[str, Dict[str, Any]] = {}
    if not sources:
        return stats, 0

    getter = getattr(engine, "get_cached_sheet_snapshot", None) if engine else None
    if not callable(getter):
        for s in sources:
            stats[s] = {"hit": False}
        return stats, 0

    variants = {
        "Market_Leaders": ["Market_Leaders", "MARKET_LEADERS"],
        "Global_Markets": ["Global_Markets", "GLOBAL_MARKETS"],
        "Mutual_Funds": ["Mutual_Funds", "MUTUAL_FUNDS"],
        "Commodities_FX": ["Commodities_FX", "COMMODITIES_FX"],
    }

    hit_count = 0
    for s in sources:
        ok = False
        snap_best: Optional[Dict[str, Any]] = None

        cand = variants.get(s, [s, str(s).upper(), str(s).lower()])
        for c in cand:
            try:
                snap = getter(c)
                if isinstance(snap, dict) and isinstance(snap.get("rows"), list):
                    snap_best = snap
                    ok = True
                    break
            except Exception:
                continue

        if ok and snap_best:
            rows_len = len(snap_best.get("rows") or [])
            headers_len = len(snap_best.get("headers") or [])
            updated_utc = snap_best.get("updated_utc") or snap_best.get("last_updated_utc") or snap_best.get("updatedAtUtc")
            stats[s] = {
                "hit": True,
                "rows": rows_len,
                "headers": headers_len,
                "updated_utc": str(updated_utc or ""),
                "updated_riyadh": _riyadh_iso(str(updated_utc)) if updated_utc else "",
            }
            hit_count += 1
        else:
            stats[s] = {"hit": False}

    return stats, hit_count


def _advisor_timeout_sec() -> float:
    try:
        v = float(os.getenv("ADVISOR_TIMEOUT_SEC", "60") or "60")
        return max(10.0, min(240.0, v))
    except Exception:
        return 60.0


def _advisor_max_tickers() -> int:
    # Prefer routes.config settings.ai_max_tickers when available
    rc = _routes_config_mod()
    try:
        if rc is not None and callable(getattr(rc, "get_settings", None)):
            s = rc.get_settings()  # type: ignore[attr-defined]
            v = getattr(s, "ai_max_tickers", None)
            if isinstance(v, int) and v > 0:
                return max(100, min(20000, v))
    except Exception:
        pass

    # Env fallback
    try:
        v2 = int(float(os.getenv("ADVISOR_MAX_TICKERS", "2500") or "2500"))
        return max(100, min(20000, v2))
    except Exception:
        return 2500


# =============================================================================
# Normalizers (payload hardening)
# =============================================================================
def _safe_float(v: Any) -> Optional[float]:
    if v is None:
        return None
    if isinstance(v, bool):
        return None
    if isinstance(v, (int, float)):
        return float(v)
    try:
        s = str(v).strip().replace(",", "")
        if s == "":
            return None
        # allow "SAR 10000"
        parts = s.split()
        if len(parts) == 2 and parts[0].isalpha():
            s = parts[1]
        if s.endswith("%"):
            s = s[:-1]
        return float(s)
    except Exception:
        return None


def _safe_int(v: Any) -> Optional[int]:
    if v is None:
        return None
    if isinstance(v, bool):
        return None
    if isinstance(v, int):
        return v
    try:
        return int(float(str(v).strip()))
    except Exception:
        return None


def _clamp_int(v: Any, default: int, lo: int, hi: int) -> int:
    x = _safe_int(v)
    if x is None:
        x = default
    return max(lo, min(hi, x))


def _key_norm(k: Any) -> str:
    s = str(k or "").strip().lower()
    if not s:
        return ""
    # normalize spaces/underscores/dashes/punct into nothing for loose matching
    out = []
    for ch in s:
        if ch.isalnum():
            out.append(ch)
    return "".join(out)


def _flatten_criteria(body: Dict[str, Any]) -> Dict[str, Any]:
    """
    Merge nested criteria blocks into top-level (without overwriting explicit top-level keys).
    Common keys: criteria, advisor_criteria, filters, params
    """
    if not isinstance(body, dict):
        return {}
    out = dict(body)

    for k in ("criteria", "advisor_criteria", "advisorCriteria", "filters", "params"):
        v = body.get(k)
        if isinstance(v, dict):
            for kk, vv in v.items():
                if kk not in out:
                    out[kk] = vv
    return out


def _get_any(m: Dict[str, Any], *names: str) -> Any:
    """
    Case/format-insensitive getter.
    """
    if not m:
        return None
    km = {_key_norm(k): k for k in m.keys()}
    for n in names:
        nk = _key_norm(n)
        if nk in km:
            return m.get(km[nk])
    return None


def _normalize_sources(v: Any) -> List[str]:
    if v is None:
        return list(DEFAULT_SOURCES)

    if isinstance(v, str):
        s = v.strip()
        if not s or s.upper() == "ALL":
            return list(DEFAULT_SOURCES)
        parts = [p.strip() for p in s.split(",") if p.strip()]
        return parts or list(DEFAULT_SOURCES)

    if isinstance(v, list):
        parts = [str(x).strip() for x in v if str(x or "").strip()]
        if len(parts) == 1 and parts[0].upper() == "ALL":
            return list(DEFAULT_SOURCES)
        return parts or list(DEFAULT_SOURCES)

    return list(DEFAULT_SOURCES)


def _normalize_bucket(v: Any, kind: str) -> str:
    """
    kind: "risk" or "confidence"
    Accepts many labels. Returns canonical:
      risk -> Low/Moderate/High
      confidence -> Low/Medium/High
    """
    s = str(v or "").strip().lower()
    if not s:
        return "Moderate" if kind == "risk" else "High"

    # Common combined strings like "Low risk–High confidence"
    if "risk" in s or "confidence" in s:
        if kind == "risk":
            if "low" in s:
                return "Low"
            if "moder" in s or "med" in s:
                return "Moderate"
            if "high" in s:
                return "High"
            return "Moderate"
        else:
            if "low" in s:
                return "Low"
            if "med" in s or "moder" in s:
                return "Medium"
            if "high" in s:
                return "High"
            return "High"

    # Direct
    if kind == "risk":
        if s.startswith("l"):
            return "Low"
        if s.startswith("h"):
            return "High"
        if "med" in s or "moder" in s:
            return "Moderate"
        return "Moderate"

    # confidence
    if s.startswith("l"):
        return "Low"
    if s.startswith("h"):
        return "High"
    if "med" in s or "moder" in s:
        return "Medium"
    return "High"


def _normalize_tickers(v: Any) -> List[str]:
    if v is None:
        return []

    items: List[str] = []
    if isinstance(v, str):
        s = v.strip()
        if not s:
            return []
        s = s.replace(",", " ")
        items = [p.strip() for p in s.split() if p.strip()]
    elif isinstance(v, list):
        for it in v:
            s = str(it or "").strip()
            if s:
                s = s.replace(",", " ")
                items.extend([p.strip() for p in s.split() if p.strip()])
    else:
        s = str(v).strip()
        if s:
            items = [s]

    seen = set()
    out: List[str] = []
    for t in items:
        u = t.strip().upper()
        if not u or u in ("SYMBOL", "TICKER"):
            continue
        if u.startswith("#"):
            continue
        if u in seen:
            continue
        seen.add(u)
        out.append(u)
    return out


def _normalize_list(v: Any) -> Optional[List[str]]:
    if v is None:
        return None
    if isinstance(v, list):
        out = [str(x).strip() for x in v if str(x or "").strip()]
        return out or None
    if isinstance(v, str):
        s = v.strip()
        if not s:
            return None
        parts = [p.strip() for p in s.split(",") if p.strip()]
        return parts or None
    return None


def _percent_to_ratio(v: Any, *, default_ratio: float) -> float:
    """
    Accepts:
      0.10 -> 0.10
      10   -> 0.10 (assumed percent)
      "10%" -> 0.10
      "0.1" -> 0.10
    """
    x = _safe_float(v)
    if x is None:
        return default_ratio
    if x > 1.0:
        return float(x) / 100.0
    if x < 0.0:
        return default_ratio
    return float(x)


def _normalize_payload(body: Dict[str, Any]) -> Dict[str, Any]:
    """
    Produces payload keys expected by core.investment_advisor.run_investment_advisor
    """
    raw0 = dict(body or {})
    raw = _flatten_criteria(raw0)

    sources = _normalize_sources(
        _get_any(raw, "sources", "data_sources", "dataSources", "pages", "selected_sources", "selectedSources", "source")
    )

    risk = _normalize_bucket(_get_any(raw, "risk", "risk_bucket", "riskBucket", "risk level"), "risk")
    confidence = _normalize_bucket(_get_any(raw, "confidence", "confidence_bucket", "confidenceBucket", "confidence level"), "confidence")

    tickers = _normalize_tickers(_get_any(raw, "tickers", "symbols", "universe", "ticker_list", "tickers_list", "selected_tickers"))
    max_tickers = _advisor_max_tickers()
    if len(tickers) > max_tickers:
        tickers = tickers[:max_tickers]

    # Invest amount
    amount = _get_any(raw, "invest_amount", "amount", "amount_to_invest", "amount to be invest", "amount_to_be_invest", "investAmount")
    invest_amount = _safe_float(amount) or 0.0

    # top_n
    top_n = _get_any(raw, "top_n", "max_items", "number_of_stocks", "number of stock to be reported", "numberOfStocks", "limit")
    top_n_i = _clamp_int(top_n, default=10, lo=1, hi=500)

    include_news = _get_any(raw, "include_news", "includeNews", "use_news", "useNews", "news", "with_news")
    include_news_b = True if include_news is None else (_truthy(include_news) if isinstance(include_news, str) else bool(include_news))

    # required ROI (many aliases)
    req_1m_ratio = _percent_to_ratio(
        _get_any(raw, "required_roi_1m", "required_return_1m", "target_roi_1m", "required rate of return 1 month", "requiredroi1m"),
        default_ratio=0.05,
    )
    req_3m_ratio = _percent_to_ratio(
        _get_any(raw, "required_roi_3m", "required_return_3m", "target_roi_3m", "required rate of return 3 month", "requiredroi3m"),
        default_ratio=0.10,
    )

    currency = str(_get_any(raw, "currency", "ccy") or "SAR").strip().upper() or "SAR"

    # Filters (pass-through if present)
    min_price = _safe_float(_get_any(raw, "min_price", "minPrice"))
    max_price = _safe_float(_get_any(raw, "max_price", "maxPrice"))
    exclude_sectors = _normalize_list(_get_any(raw, "exclude_sectors", "excludeSectors", "exclude_sectors_csv"))
    liquidity_min = _safe_float(_get_any(raw, "liquidity_min", "min_liquidity", "minLiquidity"))
    max_position_pct = _safe_float(_get_any(raw, "max_position_pct", "maxPositionPct"))
    min_position_pct = _safe_float(_get_any(raw, "min_position_pct", "minPositionPct"))
    as_of_utc = _get_any(raw, "as_of_utc", "asOfUtc")

    payload: Dict[str, Any] = {
        "sources": sources,
        "risk": risk,
        "confidence": confidence,
        "required_roi_1m": req_1m_ratio,
        "required_roi_3m": req_3m_ratio,
        "top_n": top_n_i,
        "invest_amount": float(invest_amount or 0.0),
        "include_news": bool(include_news_b),
        "currency": currency,
        "as_of_utc": as_of_utc,
        "min_price": min_price,
        "max_price": max_price,
        "exclude_sectors": exclude_sectors,
        "liquidity_min": liquidity_min,
        "max_position_pct": max_position_pct,
        "min_position_pct": min_position_pct,
        # tickers override (restrict universe)
        "tickers": tickers if tickers else None,
        # diagnostics (safe extras)
        "_diag_tickers_count": len(tickers),
        "_diag_tickers_sample": tickers[:50],
        "_diag_sources_count": len(sources or []),
        "_diag_max_tickers": max_tickers,
    }
    return payload


# =============================================================================
# Response safety + computed allocations + gains
# =============================================================================
def _rectangularize(headers: List[Any], rows: List[Any]) -> Tuple[List[str], List[List[Any]]]:
    h = [str(x).strip() for x in (headers or []) if str(x).strip()]
    if not h:
        h = list(TT_ADVISOR_DEFAULT_HEADERS)

    out_rows: List[List[Any]] = []
    w = len(h)
    for r in (rows or []):
        rr = list(r) if isinstance(r, list) else []
        if len(rr) < w:
            rr.extend([""] * (w - len(rr)))
        elif len(rr) > w:
            rr = rr[:w]
        out_rows.append(rr)

    return h, out_rows


def _find_col(headers: List[str], needles: Sequence[str]) -> Optional[int]:
    hlow = [str(x or "").strip().lower() for x in headers]
    for i, hh in enumerate(hlow):
        for n in needles:
            if n in hh:
                return i
    return None


def _parse_percent_cell(x: Any) -> Optional[float]:
    """
    Returns ratio (0..1) if possible.
    Accepts:
      10, "10", "10%", "0.1"
    """
    v = _safe_float(x)
    if v is None:
        return None
    if v > 1.0:
        return v / 100.0
    if v < 0.0:
        return None
    return v


def _maybe_add_alloc_amount(headers: List[str], rows: List[List[Any]], *, invest_amount: float, currency: str) -> Tuple[List[str], List[List[Any]], Dict[str, Any]]:
    """
    If Allocation Amount is missing but Allocation % exists and invest_amount>0,
    append Allocated Amount (CUR) column and compute per row.
    """
    meta: Dict[str, Any] = {"allocated_amount_added": False}
    if not headers or not rows or not invest_amount or invest_amount <= 0:
        return headers, rows, meta

    # If any allocation amount column already exists, do nothing
    idx_alloc_amt = _find_col(headers, ["allocation amount", "allocated amount"])
    if idx_alloc_amt is not None:
        return headers, rows, meta

    idx_alloc_pct = _find_col(headers, ["allocation %", "allocation percent", "allocation pct"])
    if idx_alloc_pct is None:
        return headers, rows, meta

    new_headers = list(headers)
    col_name = f"Allocated Amount ({currency})"
    new_headers.append(col_name)

    total_alloc = 0.0
    new_rows: List[List[Any]] = []
    for r in rows:
        rr = list(r)
        pct = _parse_percent_cell(rr[idx_alloc_pct])
        amt = ""
        if pct is not None:
            amt_v = float(invest_amount) * float(pct)
            amt = round(amt_v, 2)
            total_alloc += float(amt_v)
        rr.append(amt)
        new_rows.append(rr)

    meta.update({"allocated_amount_added": True, "total_allocated_amount": round(total_alloc, 2), "allocated_amount_col": col_name})
    return new_headers, new_rows, meta


def _maybe_add_gain_columns(headers: List[str], rows: List[List[Any]], *, currency: str, invest_amount: float = 0.0) -> Tuple[List[str], List[List[Any]], Dict[str, Any]]:
    """
    Compute gains if possible. Adds columns at END only (does not reorder).
    Priority for allocation base:
      1) Allocation Amount / Allocated Amount column (any recognized)
      2) Allocation % * invest_amount (if invest_amount>0)
    """
    meta: Dict[str, Any] = {"gains_added": False}

    if not headers or not rows:
        return headers, rows, meta

    idx_alloc_amt = _find_col(headers, ["allocation amount", "allocated amount"])
    idx_alloc_pct = _find_col(headers, ["allocation %", "allocation percent", "allocation pct"])

    # Detect ROI columns
    idx_roi_1m = _find_col(headers, ["expected roi % (1m)", "expected roi (1m)", "expected return (1m)", "expected roi 1m"])
    idx_roi_3m = _find_col(headers, ["expected roi % (3m)", "expected roi (3m)", "expected return (3m)", "expected roi 3m"])

    if idx_roi_1m is None and idx_roi_3m is None:
        return headers, rows, meta

    # already present?
    has_1m = any(str(h).strip().lower() == GAIN_COL_1M.lower() for h in headers)
    has_3m = any(str(h).strip().lower() == GAIN_COL_3M.lower() for h in headers)

    add_1m = not has_1m
    add_3m = not has_3m
    if not add_1m and not add_3m:
        return headers, rows, meta

    # Ensure we have an allocation base
    can_compute_alloc = idx_alloc_amt is not None or (idx_alloc_pct is not None and invest_amount and invest_amount > 0)
    if not can_compute_alloc:
        return headers, rows, meta

    new_headers = list(headers)
    if add_1m:
        new_headers.append(GAIN_COL_1M)
    if add_3m:
        new_headers.append(GAIN_COL_3M)

    total_alloc = 0.0
    total_gain_1m = 0.0
    total_gain_3m = 0.0

    new_rows: List[List[Any]] = []
    for r in rows:
        rr = list(r)

        # allocation base
        alloc = 0.0
        if idx_alloc_amt is not None:
            alloc = _safe_float(rr[idx_alloc_amt]) or 0.0
        elif idx_alloc_pct is not None and invest_amount and invest_amount > 0:
            pct = _parse_percent_cell(rr[idx_alloc_pct])
            alloc = float(invest_amount) * float(pct or 0.0)

        total_alloc += alloc

        g1 = ""
        g3 = ""
        if idx_roi_1m is not None:
            roi1 = _parse_percent_cell(rr[idx_roi_1m])
            if roi1 is not None:
                g1v = alloc * roi1
                g1 = round(g1v, 2)
                total_gain_1m += float(g1v)

        if idx_roi_3m is not None:
            roi3 = _parse_percent_cell(rr[idx_roi_3m])
            if roi3 is not None:
                g3v = alloc * roi3
                g3 = round(g3v, 2)
                total_gain_3m += float(g3v)

        if add_1m:
            rr.append(g1)
        if add_3m:
            rr.append(g3)

        new_rows.append(rr)

    meta.update(
        {
            "gains_added": True,
            "currency": currency,
            "total_allocated": round(total_alloc, 2),
            "total_expected_gain_1m": round(total_gain_1m, 2),
            "total_expected_gain_3m": round(total_gain_3m, 2),
        }
    )
    return new_headers, new_rows, meta


def _items_to_rows_by_headers(items: List[Dict[str, Any]], headers: List[str]) -> List[List[Any]]:
    """
    If core returns items but not rows, build rows using loose header matching.
    """
    def hk(x: Any) -> str:
        s = str(x or "").strip().lower()
        return "".join([c for c in s if c.isalnum()])

    alias = {
        "symbol": ["symbol", "ticker"],
        "price": ["price", "currentprice", "current_price"],
        "advisorscore": ["advisor_score", "advisorscore", "score", "overall_score"],
        "allocation%": ["allocation_pct", "allocation%", "allocation_percent", "weight", "target_weight"],
        "allocationamount": ["allocation_amount", "allocated_amount", "allocated_amount_sar", "allocatedamount"],
        "riskbucket": ["risk_bucket", "riskbucket"],
        "confidencebucket": ["confidence_bucket", "confidencebucket"],
        "reason(explain)": ["reason", "explain", "rationale"],
        "datasource": ["data_source", "source", "origin"],
        "dataquality": ["data_quality", "quality"],
        "lastupdated(utc)": ["last_updated_utc", "updated_utc", "lastupdatedutc"],
        "lastupdated(riyadh)": ["last_updated_riyadh", "updated_riyadh", "lastupdatedriyadh"],
        "expectedroi%(1m)": ["expected_roi_1m", "expected_return_1m", "expectedroi1m"],
        "expectedroi%(3m)": ["expected_roi_3m", "expected_return_3m", "expectedroi3m"],
        "forecastprice(1m)": ["forecast_price_1m", "expected_price_1m", "target_price_1m"],
        "forecastprice(3m)": ["forecast_price_3m", "expected_price_3m", "target_price_3m"],
    }

    # precompute header->candidate keys
    hkeys = [hk(h) for h in headers]
    rows: List[List[Any]] = []
    for it in items or []:
        d = it if isinstance(it, dict) else {}
        km = {hk(k): v for k, v in d.items()}
        row: List[Any] = []
        for h, hh in zip(headers, hkeys):
            val = None
            # direct
            if hh in km:
                val = km.get(hh)
            else:
                # alias bundle
                cands = alias.get(hh, [])
                for c in cands:
                    if hk(c) in km:
                        val = km.get(hk(c))
                        break
            row.append(val if val is not None else "")
        rows.append(row)
    return rows


def _envelope(status: str, *, headers: List[str], rows: List[List[Any]], error: Optional[str], meta: Dict[str, Any], extra: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    base: Dict[str, Any] = {
        "status": status,
        "headers": headers,
        "rows": rows,
        "version": ADVISOR_ROUTE_VERSION,
        "time_utc": _utc_iso(),
        "time_riyadh": _riyadh_iso(),
        "meta": meta or {},
    }
    if error is not None:
        base["error"] = error
    if extra and isinstance(extra, dict):
        base.update(extra)
    return base


# =============================================================================
# Endpoints
# =============================================================================
@router.get("/health")
@router.get("/ping")
async def advisor_health(request: Request) -> Dict[str, Any]:
    engine = await _resolve_engine(request)
    engine_name = type(engine).__name__ if engine is not None else "none"
    engine_version = None
    if engine is not None:
        engine_version = getattr(engine, "ENGINE_VERSION", None) or getattr(engine, "engine_version", None)

    rc = _routes_config_mod()
    config_mask = None
    try:
        if rc is not None and callable(getattr(rc, "mask_settings_dict", None)):
            config_mask = rc.mask_settings_dict()  # type: ignore[attr-defined]
    except Exception:
        config_mask = None

    return _envelope(
        "ok",
        headers=["key", "value"],
        rows=[
            ["module", "routes.investment_advisor"],
            ["route_version", ADVISOR_ROUTE_VERSION],
            ["engine", engine_name],
            ["engine_version", str(engine_version or "")],
            ["auth_header", _auth_header_name()],
            ["open_mode", str(_open_mode())],
            ["default_sources", ",".join(DEFAULT_SOURCES)],
            ["default_headers_len", str(len(TT_ADVISOR_DEFAULT_HEADERS))],
            ["advisor_timeout_sec", str(_advisor_timeout_sec())],
            ["advisor_max_tickers", str(_advisor_max_tickers())],
            ["time_utc", _utc_iso()],
            ["time_riyadh", _riyadh_iso()],
        ],
        error=None,
        meta={"config_mask": config_mask},
    )


@router.get("/options")
async def advisor_options() -> Dict[str, Any]:
    """
    Handy for building dropdowns / UI on GAS side.
    """
    return {
        "status": "ok",
        "version": ADVISOR_ROUTE_VERSION,
        "auth_header": _auth_header_name(),
        "defaults": {
            "sources": list(DEFAULT_SOURCES),
            "risk": ["Low", "Moderate", "High"],
            "confidence": ["Low", "Medium", "High"],
            "currency": ["SAR", "USD", "EUR"],
            "top_n": 10,
            "required_roi_1m": "5%",
            "required_roi_3m": "10%",
        },
        "notes": {
            "required_roi_fields": "Accepts percent or ratio. Examples: 10, '10%', 0.1",
            "tickers": "Accepts list OR string with comma/space separators",
            "computed_columns": f"May append '{ALLOC_AMOUNT_COL}', '{GAIN_COL_1M}', '{GAIN_COL_3M}' when possible.",
        },
        "time_utc": _utc_iso(),
        "time_riyadh": _riyadh_iso(),
    }


async def _run_core(request: Request, payload: Dict[str, Any], *, debug: int = 0) -> Dict[str, Any]:
    # Lazy import (prod safe)
    try:
        from core.investment_advisor import run_investment_advisor  # type: ignore
    except Exception as exc:
        logger.exception("Advisor core import failed: %s", exc)
        h, r = _rectangularize(list(TT_ADVISOR_DEFAULT_HEADERS), [])
        return _envelope(
            "error",
            headers=h,
            rows=r,
            error=f"Advisor core missing: {exc}",
            meta={"route_version": ADVISOR_ROUTE_VERSION},
        )

    engine = await _resolve_engine(request)

    # Snapshot diagnostics (helps explain 0 rows issues)
    sources = payload.get("sources") or []
    sources_list = sources if isinstance(sources, list) else []
    snap_stats, snap_hit_count = _probe_engine_snapshots(engine, sources_list)

    # Execute
    started = time.time()
    timeout_sec = _advisor_timeout_sec()

    try:
        # Support sync OR async core
        res_obj = run_investment_advisor(payload, engine=engine)  # type: ignore
        if inspect.isawaitable(res_obj):
            result = await asyncio.wait_for(res_obj, timeout=timeout_sec)
        else:
            result = res_obj

        if not isinstance(result, dict):
            h, r = _rectangularize(list(TT_ADVISOR_DEFAULT_HEADERS), [])
            return _envelope(
                "error",
                headers=h,
                rows=r,
                error="Advisor core returned non-dict result",
                meta={"route_version": ADVISOR_ROUTE_VERSION},
            )

        headers = result.get("headers") if isinstance(result.get("headers"), list) else []
        rows = result.get("rows") if isinstance(result.get("rows"), list) else []
        items = result.get("items") if isinstance(result.get("items"), list) else None
        meta_core = result.get("meta") if isinstance(result.get("meta"), dict) else {}

        # If rows empty but items exist, build rows
        if (not rows) and items and isinstance(items, list) and headers:
            try:
                rows = _items_to_rows_by_headers([x for x in items if isinstance(x, dict)], [str(h) for h in headers])
            except Exception:
                rows = rows or []

        # Always rectangular + never empty headers
        headers2, rows2 = _rectangularize(headers, rows)

        currency = str(payload.get("currency") or "SAR").strip().upper() or "SAR"
        invest_amount = float(payload.get("invest_amount") or 0.0)

        # Optional: add allocated amount from allocation % (if missing)
        headers3, rows3, alloc_meta = _maybe_add_alloc_amount(headers2, rows2, invest_amount=invest_amount, currency=currency)

        # Optional: gains computation (adds columns at end only)
        headers4, rows4, gains_meta = _maybe_add_gain_columns(headers3, rows3, currency=currency, invest_amount=invest_amount)

        processing_ms = round((time.time() - started) * 1000, 2)

        # Route-level diagnostics meta
        meta_out = dict(meta_core or {})
        meta_out.update(
            {
                "route_version": ADVISOR_ROUTE_VERSION,
                "processing_time_ms": processing_ms,
                "timeout_sec": timeout_sec,
                "engine_present": bool(engine is not None),
                "engine_type": type(engine).__name__ if engine is not None else "none",
                "sheet_cache_stats": snap_stats,
                "sheet_cache_hit_count": snap_hit_count,
                "diag_tickers_count": payload.get("_diag_tickers_count", 0),
                "diag_tickers_sample": payload.get("_diag_tickers_sample", []),
                "sources": sources_list,
                "risk": payload.get("risk"),
                "confidence": payload.get("confidence"),
                "required_roi_1m_ratio": payload.get("required_roi_1m"),
                "required_roi_3m_ratio": payload.get("required_roi_3m"),
                "invest_amount": invest_amount,
                "include_news": bool(payload.get("include_news")),
                "filters": {
                    "min_price": payload.get("min_price"),
                    "max_price": payload.get("max_price"),
                    "exclude_sectors": payload.get("exclude_sectors"),
                    "liquidity_min": payload.get("liquidity_min"),
                    "max_position_pct": payload.get("max_position_pct"),
                    "min_position_pct": payload.get("min_position_pct"),
                },
                "rows_count": len(rows4 or []),
                "headers_count": len(headers4 or []),
            }
        )
        meta_out.update(alloc_meta or {})
        meta_out.update(gains_meta or {})

        # Determine status from core meta if present
        status = "ok"
        core_ok = meta_core.get("ok") if isinstance(meta_core, dict) else None
        if core_ok is False:
            status = "error"

        # If core produced zero rows, keep status ok but include an explanation in meta (non-breaking)
        if status == "ok" and len(rows4 or []) == 0:
            meta_out.setdefault("note", "No rows returned after filtering. Check sheet_cache_stats + required ROI + risk/confidence filters.")

        # Keep error text stable if core says error
        err_text = None
        if status == "error":
            err_text = str(meta_core.get("error") or "Advisor core reported ok=false")

        extra = {}
        if items is not None:
            # Optional convenience field; does not affect Sheets writing
            extra["count"] = len(items) if isinstance(items, list) else 0

        if int(debug or 0):
            extra["debug"] = {
                "auth_header": _auth_header_name(),
                "open_mode": _open_mode(),
                "max_tickers": _advisor_max_tickers(),
            }

        return _envelope(status, headers=headers4, rows=rows4, error=err_text, meta=meta_out, extra=extra)

    except asyncio.TimeoutError:
        h, r = _rectangularize(list(TT_ADVISOR_DEFAULT_HEADERS), [])
        return _envelope(
            "error",
            headers=h,
            rows=r,
            error=f"Advisor execution timed out after {timeout_sec}s",
            meta={
                "route_version": ADVISOR_ROUTE_VERSION,
                "timeout_sec": timeout_sec,
                "engine_present": bool(engine is not None),
                "engine_type": type(engine).__name__ if engine is not None else "none",
                "sheet_cache_stats": snap_stats,
                "sheet_cache_hit_count": snap_hit_count,
            },
        )
    except Exception as exc:
        logger.exception("Advisor core execution failed: %s", exc)
        h, r = _rectangularize(list(TT_ADVISOR_DEFAULT_HEADERS), [])
        meta_out = {
            "route_version": ADVISOR_ROUTE_VERSION,
            "engine_present": bool(engine is not None),
            "engine_type": type(engine).__name__ if engine is not None else "none",
            "sheet_cache_stats": snap_stats,
            "sheet_cache_hit_count": snap_hit_count,
        }
        if int(debug or 0):
            meta_out["debug_error"] = str(exc)
        return _envelope("error", headers=h, rows=r, error=f"Advisor execution failed: {exc}", meta=meta_out)


@router.post("/recommendations")
async def advisor_recommendations(
    request: Request,
    body: Dict[str, Any] = Body(default_factory=dict),
    token: Optional[str] = Query(default=None, description="(optional) query token if ALLOW_QUERY_TOKEN=1"),
    debug: int = Query(0, description="debug=1 adds extra diagnostics"),
) -> Dict[str, Any]:
    # Sheets-safe: HTTP 200 always
    if not _auth_ok_request(request, token):
        h, r = _rectangularize(list(TT_ADVISOR_DEFAULT_HEADERS), [])
        return _envelope(
            "error",
            headers=h,
            rows=r,
            error="Unauthorized: invalid or missing token.",
            meta={"route_version": ADVISOR_ROUTE_VERSION, "auth_header": _auth_header_name(), "open_mode": _open_mode()},
        )

    payload = _normalize_payload(body or {})

    # Log minimal summary (no secrets)
    try:
        logger.info(
            "[Advisor] /recommendations sources=%s top_n=%s invest=%s tickers=%s",
            ",".join(payload.get("sources") or []),
            payload.get("top_n"),
            payload.get("invest_amount"),
            payload.get("_diag_tickers_count"),
        )
    except Exception:
        pass

    return await _run_core(request, payload, debug=debug)


@router.post("/run")
async def advisor_run(
    request: Request,
    body: Dict[str, Any] = Body(default_factory=dict),
    token: Optional[str] = Query(default=None, description="(optional) query token if ALLOW_QUERY_TOKEN=1"),
    debug: int = Query(0, description="debug=1 adds extra diagnostics"),
) -> Dict[str, Any]:
    """
    Alias endpoint for compatibility: POST /v1/advisor/run
    """
    if not _auth_ok_request(request, token):
        h, r = _rectangularize(list(TT_ADVISOR_DEFAULT_HEADERS), [])
        return _envelope(
            "error",
            headers=h,
            rows=r,
            error="Unauthorized: invalid or missing token.",
            meta={"route_version": ADVISOR_ROUTE_VERSION, "auth_header": _auth_header_name(), "open_mode": _open_mode()},
        )

    payload = _normalize_payload(body or {})

    try:
        logger.info(
            "[Advisor] /run sources=%s top_n=%s invest=%s tickers=%s",
            ",".join(payload.get("sources") or []),
            payload.get("top_n"),
            payload.get("invest_amount"),
            payload.get("_diag_tickers_count"),
        )
    except Exception:
        pass

    return await _run_core(request, payload, debug=debug)


__all__ = ["router"]
