"""
Tadawul Fast Bridge — Investment Advisor Core (GOOGLE SHEETS SAFE)
File: core/investment_advisor.py

FULL REPLACEMENT — v1.4.0 (ADVANCED + STABLE OUTPUT + TICKERS OVERRIDE)

Public contract:
  run_investment_advisor(payload_dict, engine=...) -> {"headers":[...], "rows":[...], "meta":{...}}

v1.4.0 Enhancements:
- ✅ Tickers Override: payload may include tickers/symbols to restrict universe (even if sources=ALL).
- ✅ Stable Schema: headers are ALWAYS present (GAS-safe).
- ✅ Adds 12M columns: Forecast Price (12M), Expected ROI % (12M), Expected Gain/Loss (12M).
- ✅ Expected Gain/Loss: computed from Allocation Amount * ROI (1M/3M/12M).
- ✅ Include-News Switch: news impact applied ONLY if include_news=True.
- ✅ Liquidity Guard: uses Liquidity Score OR proxy from Value Traded / Volume if available.
- ✅ Volatility-aware penalty: stronger penalty for Low risk profile.
- ✅ Allocation Constraints: optional caps + min allocation threshold with redistribution.
- ✅ Riyadh Localization: outputs Last Updated (Riyadh) column.
- ✅ Better fallback scoring if Overall Score missing (composite factors).

Notes:
- Expects sheet snapshots to be cached via engine.set_cached_sheet_snapshot()
  after each sheet refresh (Market_Leaders, Global_Markets, Mutual_Funds, Commodities_FX).
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple, Set
import math
import time
from datetime import datetime, timezone, timedelta

TT_ADVISOR_CORE_VERSION = "1.4.0"

DEFAULT_SOURCES = ["Market_Leaders", "Global_Markets", "Mutual_Funds", "Commodities_FX"]

# ---------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------
_TRUTHY = {"1", "true", "yes", "y", "on", "t", "enable", "enabled", "ok"}


def _truthy(v: Any) -> bool:
    if isinstance(v, bool):
        return v
    return str(v or "").strip().lower() in _TRUTHY


def _norm_key(k: Any) -> str:
    s = str(k or "").strip().lower()
    return " ".join(s.split())


def _safe_str(x: Any) -> str:
    return "" if x is None else str(x).strip()


def _norm_symbol(x: Any) -> str:
    s = _safe_str(x).upper().replace(" ", "")
    # common normalization
    if s.endswith(".SA"):  # sometimes appears
        s = s[:-3] + ".SR"
    return s


def _to_float(x: Any) -> Optional[float]:
    if x is None:
        return None
    if isinstance(x, (int, float)) and not isinstance(x, bool):
        f = float(x)
        if math.isnan(f) or math.isinf(f):
            return None
        return f
    s = str(x).strip().replace(",", "")
    if s in ("", "NA", "N/A", "null", "None", "-", "—", "none"):
        return None
    s = s.replace("%", "")
    try:
        f = float(s)
        if math.isnan(f) or math.isinf(f):
            return None
        return f
    except Exception:
        return None


def _to_int(x: Any) -> Optional[int]:
    f = _to_float(x)
    if f is None:
        return None
    try:
        return int(round(f))
    except Exception:
        return None


def _as_ratio(x: Any) -> Optional[float]:
    """
    Accepts percent-ish or ratio and returns ratio:
      - 0.10 => 0.10
      - 10   => 0.10
      - "10%" => 0.10
      - "0.10" => 0.10
    """
    f = _to_float(x)
    if f is None:
        return None
    if f > 1.5:
        return f / 100.0
    return f


def _norm_bucket(x: Any) -> str:
    if x is None:
        return ""
    s = str(x).strip().lower()
    if not s:
        return ""
    # risk
    if s in ("low", "low risk", "low-risk", "conservative"):
        return "Low"
    if s in ("moderate", "medium", "mid", "balanced"):
        return "Moderate"
    if s in ("high", "high risk", "high-risk", "aggressive", "growth"):
        return "High"
    if s in ("very high", "very-high", "speculative"):
        return "Very High"
    # confidence
    if s in ("high confidence", "high-conf", "highconf"):
        return "High"
    if s in ("moderate confidence", "medium confidence", "mid confidence"):
        return "Moderate"
    if s in ("low confidence", "low-conf", "lowconf"):
        return "Low"
    return str(x).strip().title()


def _get_any(row: Dict[str, Any], *names: str) -> Any:
    """
    Case/space-insensitive key lookup for dict rows.
    Adds a cached normalized-key map in row["_nmap"] for speed.
    """
    if not row:
        return None

    for n in names:
        if n in row:
            return row.get(n)

    nmap = row.get("_nmap")
    if not isinstance(nmap, dict):
        nmap = {}
        for k in row.keys():
            if k == "_nmap":
                continue
            nk = _norm_key(k)
            if nk and nk not in nmap:
                nmap[nk] = k
        row["_nmap"] = nmap

    for n in names:
        nk = _norm_key(n)
        k0 = nmap.get(nk)
        if k0 is not None:
            return row.get(k0)

    return None


def _rows_to_dicts(headers: List[Any], rows: List[Any], sheet_name: str, limit: int) -> List[Dict[str, Any]]:
    h = [str(x).strip() for x in (headers or [])]
    out: List[Dict[str, Any]] = []
    for i, r in enumerate(rows or []):
        if i >= limit:
            break
        if not isinstance(r, list):
            continue
        d = {h[j]: (r[j] if j < len(r) else None) for j in range(len(h))}
        d["_Sheet"] = sheet_name
        out.append(d)
    return out


def _parse_iso_to_dt(v: Any) -> Optional[datetime]:
    s = _safe_str(v)
    if not s:
        return None
    try:
        # allow both with/without tz
        dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except Exception:
        return None


def _iso_utc(dt: datetime) -> str:
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc).isoformat()


def _iso_riyadh(dt: datetime) -> str:
    tz_riyadh = timezone(timedelta(hours=3))
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(tz_riyadh).isoformat()


# ---------------------------------------------------------------------
# Universe fetch (ENGINE snapshot cache first)
# ---------------------------------------------------------------------
def _engine_get_snapshot(engine: Any, sheet_name: str) -> Optional[Dict[str, Any]]:
    if engine is None:
        return None
    fn = getattr(engine, "get_cached_sheet_snapshot", None)
    if callable(fn):
        try:
            snap = fn(sheet_name)
            return snap if isinstance(snap, dict) else None
        except Exception:
            return None
    return None


def _engine_get_multi_snapshots(engine: Any, sheet_names: List[str]) -> Dict[str, Dict[str, Any]]:
    if engine is None:
        return {}
    fn = getattr(engine, "get_cached_multi_sheet_snapshots", None)
    if callable(fn):
        try:
            out = fn(sheet_names)
            if isinstance(out, dict):
                return {str(k): v for k, v in out.items() if isinstance(v, dict)}
        except Exception:
            return {}
    out2: Dict[str, Dict[str, Any]] = {}
    for s in sheet_names or []:
        snap = _engine_get_snapshot(engine, s)
        if snap:
            out2[str(s)] = snap
    return out2


def _try_get_universe_rows(
    sources: List[str],
    *,
    max_rows_per_source: int = 5000,
    engine: Any = None,
) -> Tuple[List[Dict[str, Any]], Dict[str, Any], Optional[str]]:
    meta: Dict[str, Any] = {
        "sources": sources,
        "engine": type(engine).__name__ if engine is not None else None,
        "items": [],
        "mode": "engine_snapshot",
    }

    normalized_sources = [s.strip() for s in (sources or []) if isinstance(s, str) and s.strip()]
    if not normalized_sources:
        normalized_sources = ["ALL"]
    if "ALL" in [s.upper() for s in normalized_sources]:
        normalized_sources = list(DEFAULT_SOURCES)

    if engine is None:
        return [], meta, "Missing engine instance (routes must pass engine=app.state.engine)."

    snaps = _engine_get_multi_snapshots(engine, normalized_sources)
    out_rows: List[Dict[str, Any]] = []

    for sheet in normalized_sources:
        snap = snaps.get(sheet) or _engine_get_snapshot(engine, sheet)
        if not snap:
            meta["items"].append({"sheet": sheet, "cached": False, "rows": 0, "headers": 0})
            continue

        headers = snap.get("headers") or []
        rows = snap.get("rows") or []
        meta["items"].append(
            {
                "sheet": sheet,
                "cached": True,
                "rows": len(rows) if isinstance(rows, list) else 0,
                "headers": len(headers) if isinstance(headers, list) else 0,
                "cached_at_utc": snap.get("cached_at_utc"),
            }
        )
        if isinstance(headers, list) and isinstance(rows, list):
            out_rows.extend(_rows_to_dicts(headers, rows, sheet_name=sheet, limit=max_rows_per_source))

    if not out_rows:
        return [], meta, (
            "No cached sheet snapshots found. Ensure your sheet routes call "
            "engine.set_cached_sheet_snapshot(sheet_name, headers, rows, meta) "
            "after fetching each page."
        )

    return out_rows, meta, None


# ---------------------------------------------------------------------
# Scoring model
# ---------------------------------------------------------------------
@dataclass
class Candidate:
    symbol: str
    name: str
    sheet: str
    market: str
    sector: str
    currency: str
    price: Optional[float]
    risk_bucket: str
    confidence_bucket: str

    forecast_1m: Optional[float]
    exp_roi_1m: Optional[float]  # ratio
    forecast_3m: Optional[float]
    exp_roi_3m: Optional[float]  # ratio
    forecast_12m: Optional[float]
    exp_roi_12m: Optional[float]  # ratio

    overall_score: Optional[float]
    risk_score: Optional[float]
    momentum_score: Optional[float]
    value_score: Optional[float]
    quality_score: Optional[float]

    news_score: Optional[float] = None
    liquidity_score: Optional[float] = None
    volatility: Optional[float] = None
    trend_signal: str = ""

    volume: Optional[float] = None
    value_traded: Optional[float] = None

    advisor_score: float = 0.0
    reason: str = ""


def _extract_candidate(row: Dict[str, Any]) -> Optional[Candidate]:
    symbol = _norm_symbol(_get_any(row, "Symbol", "Fund Symbol", "Ticker", "Code", "Fund Code"))
    if not symbol or symbol == "SYMBOL":
        return None

    name = _safe_str(_get_any(row, "Name", "Company Name", "Fund Name", "Instrument", "Long Name", "Short Name"))
    sheet = _safe_str(_get_any(row, "_Sheet", "Origin")) or ""
    sector = _safe_str(_get_any(row, "Sector", "Industry", "Category"))

    market = _safe_str(_get_any(row, "Market")) or ("KSA" if symbol.endswith(".SR") else "GLOBAL")
    currency = _safe_str(_get_any(row, "Currency")) or ""

    price = _to_float(_get_any(row, "Price", "Last", "Close", "NAV per Share", "NAV", "Last Price"))

    risk_bucket = _norm_bucket(_get_any(row, "Risk Bucket", "Risk", "Risk Level") or "")
    confidence_bucket = _norm_bucket(_get_any(row, "Confidence Bucket", "Confidence") or "")

    forecast_1m = _to_float(_get_any(row, "Forecast Price (1M)", "Forecast Price 1M", "Forecast 1M", "forecast_price_1m", "expected_price_1m"))
    exp_roi_1m = _as_ratio(_get_any(row, "Expected ROI % (1M)", "Expected ROI 1M", "ROI 1M", "expected_roi_1m", "expected_return_1m"))

    forecast_3m = _to_float(_get_any(row, "Forecast Price (3M)", "Forecast Price 3M", "Forecast 3M", "forecast_price_3m", "expected_price_3m"))
    exp_roi_3m = _as_ratio(_get_any(row, "Expected ROI % (3M)", "Expected ROI 3M", "ROI 3M", "expected_roi_3m", "expected_return_3m"))

    forecast_12m = _to_float(_get_any(row, "Forecast Price (12M)", "Forecast Price 12M", "Forecast 12M", "forecast_price_12m", "expected_price_12m"))
    exp_roi_12m = _as_ratio(_get_any(row, "Expected ROI % (12M)", "Expected ROI 12M", "ROI 12M", "expected_roi_12m", "expected_return_12m"))

    overall_score = _to_float(_get_any(row, "Overall Score", "Opportunity Score", "Score", "overall_score"))
    risk_score = _to_float(_get_any(row, "Risk Score", "risk_score"))
    momentum_score = _to_float(_get_any(row, "Momentum Score", "momentum_score"))
    value_score = _to_float(_get_any(row, "Value Score", "value_score"))
    quality_score = _to_float(_get_any(row, "Quality Score", "quality_score"))

    news_score = _to_float(_get_any(row, "News Score", "News Sentiment", "Sentiment Score", "news_boost"))
    liquidity_score = _to_float(_get_any(row, "Liquidity Score", "liquidity_score"))
    volatility = _to_float(_get_any(row, "Volatility (30D)", "Volatility 30D", "volatility_30d"))

    trend_signal = _safe_str(_get_any(row, "Trend Signal", "trend_signal")).upper()

    volume = _to_float(_get_any(row, "Volume", "Avg Volume", "Avg Volume 10D", "volume"))
    value_traded = _to_float(_get_any(row, "Value Traded", "Value", "Turnover", "value_traded"))

    return Candidate(
        symbol=symbol,
        name=name,
        sheet=sheet,
        market=market,
        sector=sector,
        currency=currency,
        price=price,
        risk_bucket=risk_bucket,
        confidence_bucket=confidence_bucket,
        forecast_1m=forecast_1m,
        exp_roi_1m=exp_roi_1m,
        forecast_3m=forecast_3m,
        exp_roi_3m=exp_roi_3m,
        forecast_12m=forecast_12m,
        exp_roi_12m=exp_roi_12m,
        overall_score=overall_score,
        risk_score=risk_score,
        momentum_score=momentum_score,
        value_score=value_score,
        quality_score=quality_score,
        news_score=news_score,
        liquidity_score=liquidity_score,
        volatility=volatility,
        trend_signal=trend_signal,
        volume=volume,
        value_traded=value_traded,
    )


def _liquidity_proxy_score(c: Candidate) -> Optional[float]:
    """
    If liquidity_score missing, attempt a rough proxy from value_traded or volume.
    Returns 0..100 (approx), or None.
    """
    if c.liquidity_score is not None:
        return float(c.liquidity_score)

    # Prefer value_traded (often in local currency units)
    vt = c.value_traded
    if vt is not None and vt > 0:
        # log scaling: 10k -> low, 10m -> high
        x = math.log10(max(1.0, vt))
        # map roughly: 4..8 -> 20..95
        score = (x - 4.0) / (8.0 - 4.0) * 75.0 + 20.0
        return max(0.0, min(100.0, score))

    vol = c.volume
    if vol is not None and vol > 0:
        x = math.log10(max(1.0, vol))
        # map roughly: 3..7 -> 20..95
        score = (x - 3.0) / (7.0 - 3.0) * 75.0 + 20.0
        return max(0.0, min(100.0, score))

    return None


def _passes_filters(
    c: Candidate,
    risk: str,
    confidence: str,
    req_roi_1m: Optional[float],
    req_roi_3m: Optional[float],
    *,
    min_price: Optional[float] = None,
    max_price: Optional[float] = None,
    exclude_sectors: Optional[List[str]] = None,
    liquidity_min: float = 25.0,
) -> Tuple[bool, str]:
    # Risk filter (if user asked for risk, require candidate risk bucket known)
    if risk:
        user_rb = _norm_bucket(risk)
        cand_rb = _norm_bucket(c.risk_bucket)
        if not cand_rb:
            return False, "Missing risk bucket"
        allowed = {user_rb}
        if user_rb == "Moderate":
            allowed.add("Low")
        if user_rb == "High":
            allowed.update({"Moderate", "Low"})
        if user_rb == "Very High":
            allowed.update({"High", "Moderate", "Low"})
        if cand_rb not in allowed:
            return False, f"Risk mismatch (Req: {user_rb}, Got: {cand_rb})"

    # Confidence filter (if user asked, require known + minimum level)
    if confidence:
        user_cb = _norm_bucket(confidence)
        cand_cb = _norm_bucket(c.confidence_bucket)
        if not cand_cb:
            return False, "Missing confidence bucket"
        min_levels = {"High": 3, "Moderate": 2, "Low": 1}
        if min_levels.get(cand_cb, 0) < min_levels.get(user_cb, 0):
            return False, "Confidence too low"

    if req_roi_1m is not None:
        if c.exp_roi_1m is None or c.exp_roi_1m < req_roi_1m:
            return False, "ROI 1M < Target"
    if req_roi_3m is not None:
        if c.exp_roi_3m is None or c.exp_roi_3m < req_roi_3m:
            return False, "ROI 3M < Target"

    if min_price is not None and c.price is not None and c.price < min_price:
        return False, "Price too low"
    if max_price is not None and c.price is not None and c.price > max_price:
        return False, "Price too high"

    if exclude_sectors and c.sector:
        if any(s.lower() in c.sector.lower() for s in exclude_sectors):
            return False, f"Sector excluded: {c.sector}"

    # Liquidity Safety Guard (direct or proxy)
    liq = _liquidity_proxy_score(c)
    if liq is not None and liq < float(liquidity_min):
        return False, "Low Liquidity"

    return True, ""


def _fallback_base_score(c: Candidate) -> float:
    """
    If overall_score missing, compute a conservative composite from factors.
    """
    parts: List[float] = []
    for v in (c.quality_score, c.momentum_score, c.value_score):
        if v is not None:
            parts.append(float(v))
    if parts:
        # normalize to similar 0..100 scale (assumes factors are 0..100 already)
        return max(0.0, min(100.0, sum(parts) / len(parts)))
    return 50.0


def _compute_advisor_score(
    c: Candidate,
    req_roi_1m: Optional[float],
    req_roi_3m: Optional[float],
    risk_profile: str,
    *,
    include_news: bool = True,
) -> Tuple[float, str]:
    base = float(c.overall_score) if c.overall_score is not None else _fallback_base_score(c)
    score = float(base)
    reasons: List[str] = []

    # 1) ROI Uplift (diminishing, capped)
    if c.exp_roi_1m is not None:
        thr = req_roi_1m if req_roi_1m is not None else 0.0
        uplift = max(0.0, (c.exp_roi_1m - thr)) * 100.0
        score += min(18.0, uplift * 0.45)
        if uplift > 1.0:
            reasons.append(f"ROI1M={c.exp_roi_1m:.1%}")

    if c.exp_roi_3m is not None:
        thr = req_roi_3m if req_roi_3m is not None else 0.0
        uplift = max(0.0, (c.exp_roi_3m - thr)) * 100.0
        score += min(22.0, uplift * 0.38)
        if uplift > 1.0:
            reasons.append(f"ROI3M={c.exp_roi_3m:.1%}")

    # Optional small credit for strong 12M ROI (if present)
    if c.exp_roi_12m is not None:
        bonus = max(0.0, min(8.0, c.exp_roi_12m * 12.0))  # e.g., 30% -> 3.6 pts
        if bonus > 1.0:
            score += bonus
            reasons.append("ROI12M+")

    # 2) Factor boosts (quality/momentum/value)
    for label, val, cap, weight in (
        ("Quality", c.quality_score, 7.5, 0.075),
        ("Momentum", c.momentum_score, 7.5, 0.075),
        ("Value", c.value_score, 7.5, 0.075),
    ):
        if val is not None and val > 60:
            boost = min(cap, (float(val) - 50.0) * weight)
            score += boost
            reasons.append(f"{label}++")

    # 3) Risk penalty based on profile
    risk_tol = _norm_bucket(risk_profile)
    risk_val = float(c.risk_score) if c.risk_score is not None else 50.0

    if risk_tol == "Low":
        penalty_mult = 0.55
    elif risk_tol == "Moderate":
        penalty_mult = 0.22
    elif risk_tol in ("High", "Very High"):
        penalty_mult = 0.06
    else:
        penalty_mult = 0.22

    if risk_val > 50:
        penalty = (risk_val - 50.0) * penalty_mult
        score -= penalty
        if penalty > 2.0:
            reasons.append(f"Risk-{int(round(penalty))}")

    # 4) Volatility penalty (stronger for Low/Moderate)
    if c.volatility is not None:
        vol = float(c.volatility)
        # Accept both ratio-like (0.25) and percent-like (25)
        if vol > 1.5:
            vol = vol / 100.0
        if vol > 0:
            if risk_tol == "Low":
                score -= min(10.0, vol * 30.0)  # 0.30 -> -9
                reasons.append("Vol-")
            elif risk_tol == "Moderate":
                score -= min(6.0, vol * 18.0)
            # High risk: no penalty

    # 5) News intelligence integration (ONLY if enabled)
    if include_news and c.news_score is not None and c.news_score != 0:
        ns = float(c.news_score)
        if abs(ns) <= 10:
            score += ns
            if ns > 1:
                reasons.append("News+")
            elif ns < -1:
                reasons.append("News-")
        else:
            nb = (ns - 50.0) * 0.1
            score += nb
            if nb > 1:
                reasons.append("News+")

    # 6) Trend intelligence
    if c.trend_signal == "UPTREND":
        score += 4.5
        reasons.append("TrendUp")
    elif c.trend_signal == "DOWNTREND":
        score -= 4.5
        reasons.append("TrendDown")

    # 7) Liquidity mild boost/penalty
    liq = _liquidity_proxy_score(c)
    if liq is not None:
        if liq >= 80:
            score += 1.5
        elif liq < 35:
            score -= 2.5

    # clamp
    score = max(0.0, min(100.0, score))
    if c.price is not None and c.price <= 0:
        score = 0.0

    return score, "; ".join(reasons[:6])


# ---------------------------------------------------------------------
# Allocation (with caps + min allocation + redistribution)
# ---------------------------------------------------------------------
def _allocate_amount(
    ranked: List[Candidate],
    total_amount: float,
    top_n: int,
    *,
    max_position_pct: float = 0.35,
    min_position_pct: float = 0.02,
) -> List[Dict[str, Any]]:
    picks = ranked[: max(0, int(top_n))]
    if not picks:
        return []

    total_amount = float(total_amount or 0.0)
    if total_amount <= 0:
        return [{"symbol": c.symbol, "weight": 0.0, "amount": 0.0} for c in picks]

    # Conviction weights (score^2) -> normalize
    raw = [math.pow(max(1.0, float(c.advisor_score)), 2) for c in picks]
    ssum = sum(raw) or 1.0
    w = [r / ssum for r in raw]

    # Apply max cap then redistribute remainder
    max_position_pct = float(max(0.05, min(1.0, max_position_pct)))
    min_position_pct = float(max(0.0, min(0.20, min_position_pct)))

    def _cap_and_redistribute(weights: List[float]) -> List[float]:
        weights = [max(0.0, float(x)) for x in weights]
        total = sum(weights) or 1.0
        weights = [x / total for x in weights]

        # cap
        capped = [min(max_position_pct, x) for x in weights]
        used = sum(capped)
        remainder = 1.0 - used

        # redistribute to those under cap proportionally to their original weight
        if remainder > 1e-9:
            room_idx = [i for i, x in enumerate(capped) if x < max_position_pct - 1e-9]
            if room_idx:
                room_total = sum(weights[i] for i in room_idx) or 1.0
                for i in room_idx:
                    add = remainder * (weights[i] / room_total)
                    capped[i] = min(max_position_pct, capped[i] + add)

        # renormalize
        total2 = sum(capped) or 1.0
        return [x / total2 for x in capped]

    w = _cap_and_redistribute(w)

    # Floor tiny allocations then redistribute again
    if min_position_pct > 0:
        kept_idx = [i for i, x in enumerate(w) if x >= min_position_pct]
        if kept_idx:
            w2 = [0.0] * len(w)
            kept_sum = sum(w[i] for i in kept_idx) or 1.0
            for i in kept_idx:
                w2[i] = w[i] / kept_sum
            w = _cap_and_redistribute(w2)

    out: List[Dict[str, Any]] = []
    for c, ww in zip(picks, w):
        amt = total_amount * ww
        out.append({"symbol": c.symbol, "weight": ww, "amount": amt})
    return out


# ---------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------
def run_investment_advisor(payload: Dict[str, Any], *, engine: Any = None) -> Dict[str, Any]:
    """
    Router should call: run_investment_advisor(payload, engine=app.state.engine)
    """
    t0 = time.time()

    # GAS-safe stable headers (never empty)
    headers = [
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
        "Forecast Price (1M)",
        "Expected ROI % (1M)",
        "Expected Gain/Loss (1M)",
        "Forecast Price (3M)",
        "Expected ROI % (3M)",
        "Expected Gain/Loss (3M)",
        "Forecast Price (12M)",
        "Expected ROI % (12M)",
        "Expected Gain/Loss (12M)",
        "Risk Bucket",
        "Confidence Bucket",
        "Reason (Explain)",
        "Data Source",
        "Data Quality",
        "Last Updated (UTC)",
        "Last Updated (Riyadh)",
    ]

    try:
        sources = payload.get("sources") or ["ALL"]
        if isinstance(sources, str):
            sources = [sources]
        sources = [str(s).strip() for s in sources if str(s).strip()] or ["ALL"]

        # Optional tickers override (restrict to these symbols)
        tickers_in = payload.get("tickers") or payload.get("symbols")
        tickers: List[str] = []
        if isinstance(tickers_in, str):
            # allow "AAPL,MSFT 1120.SR"
            split = tickers_in.replace(",", " ").split()
            tickers = [_norm_symbol(x) for x in split if _safe_str(x)]
        elif isinstance(tickers_in, list):
            tickers = [_norm_symbol(x) for x in tickers_in if _safe_str(x)]
        tickers = [t for t in tickers if t]
        tickers_set: Optional[Set[str]] = set(tickers) if tickers else None

        risk = _norm_bucket(payload.get("risk") or "")
        confidence = _norm_bucket(payload.get("confidence") or "")

        req_roi_1m = _as_ratio(payload.get("required_roi_1m"))
        req_roi_3m = _as_ratio(payload.get("required_roi_3m"))

        top_n = _to_int(payload.get("top_n")) or 10
        top_n = max(1, min(200, top_n))

        invest_amount = _to_float(payload.get("invest_amount")) or 0.0
        currency = _safe_str(payload.get("currency") or "SAR").upper() or "SAR"
        include_news = bool(_truthy(payload.get("include_news", True)))

        # Extra filters / controls
        min_price = _to_float(payload.get("min_price"))
        max_price = _to_float(payload.get("max_price"))
        exclude_sectors = payload.get("exclude_sectors")
        if not isinstance(exclude_sectors, list):
            exclude_sectors = None

        liquidity_min = _to_float(payload.get("liquidity_min"))
        liquidity_min = float(liquidity_min) if liquidity_min is not None else 25.0

        max_position_pct = _as_ratio(payload.get("max_position_pct"))
        if max_position_pct is None:
            max_position_pct = 0.35
        min_position_pct = _as_ratio(payload.get("min_position_pct"))
        if min_position_pct is None:
            min_position_pct = 0.02

        # "as_of_utc" can be provided by caller (for reproducible timestamps)
        as_of_dt = _parse_iso_to_dt(payload.get("as_of_utc")) or datetime.now(timezone.utc)

        universe_rows, fetch_meta, fetch_err = _try_get_universe_rows(sources, engine=engine)

        candidates: List[Candidate] = []
        dropped = {"no_symbol": 0, "ticker_not_requested": 0, "filter": 0, "bad_row": 0}

        for r in universe_rows:
            try:
                c = _extract_candidate(r)
                if c is None:
                    dropped["no_symbol"] += 1
                    continue

                if tickers_set is not None and c.symbol not in tickers_set:
                    dropped["ticker_not_requested"] += 1
                    continue

                ok, _why = _passes_filters(
                    c,
                    risk,
                    confidence,
                    req_roi_1m,
                    req_roi_3m,
                    min_price=min_price,
                    max_price=max_price,
                    exclude_sectors=exclude_sectors,
                    liquidity_min=liquidity_min,
                )
                if not ok:
                    dropped["filter"] += 1
                    continue

                c.advisor_score, c.reason = _compute_advisor_score(
                    c, req_roi_1m, req_roi_3m, risk, include_news=include_news
                )
                candidates.append(c)
            except Exception:
                dropped["bad_row"] += 1

        # Sort: Advisor score, then 3M ROI, then 1M ROI
        candidates.sort(
            key=lambda x: (x.advisor_score, (x.exp_roi_3m or 0.0), (x.exp_roi_1m or 0.0)),
            reverse=True,
        )

        allocations = _allocate_amount(
            candidates,
            invest_amount,
            top_n,
            max_position_pct=float(max_position_pct),
            min_position_pct=float(min_position_pct),
        )
        alloc_map = {a["symbol"]: a for a in allocations}

        rows: List[List[Any]] = []
        for i, c in enumerate(candidates[:top_n], start=1):
            alloc = alloc_map.get(c.symbol, {"weight": 0.0, "amount": 0.0})
            w = float(alloc.get("weight", 0.0) or 0.0)
            amt = float(alloc.get("amount", 0.0) or 0.0)

            # ROI % values (for sheet display)
            exp1_pct = (c.exp_roi_1m * 100.0) if c.exp_roi_1m is not None else None
            exp3_pct = (c.exp_roi_3m * 100.0) if c.exp_roi_3m is not None else None
            exp12_pct = (c.exp_roi_12m * 100.0) if c.exp_roi_12m is not None else None

            # Expected gain/loss based on allocation amount
            gl1 = (amt * c.exp_roi_1m) if (c.exp_roi_1m is not None) else None
            gl3 = (amt * c.exp_roi_3m) if (c.exp_roi_3m is not None) else None
            gl12 = (amt * c.exp_roi_12m) if (c.exp_roi_12m is not None) else None

            # Action logic
            if c.advisor_score >= 82:
                action = "STRONG BUY"
            elif c.advisor_score >= 67:
                action = "BUY"
            elif c.advisor_score >= 52:
                action = "HOLD"
            else:
                action = "REDUCE"

            # Data quality
            quality = "FULL"
            if c.price is None:
                quality = "PARTIAL"
            if c.exp_roi_1m is None and c.exp_roi_3m is None:
                quality = "PARTIAL"

            rows.append(
                [
                    i,
                    c.symbol,
                    c.sheet,
                    c.name,
                    c.market,
                    c.currency or currency,
                    c.price,
                    round(c.advisor_score, 2),
                    action,
                    round(w * 100.0, 2),
                    round(amt, 2),
                    c.forecast_1m,
                    exp1_pct,
                    round(gl1, 2) if isinstance(gl1, (int, float)) else gl1,
                    c.forecast_3m,
                    exp3_pct,
                    round(gl3, 2) if isinstance(gl3, (int, float)) else gl3,
                    c.forecast_12m,
                    exp12_pct,
                    round(gl12, 2) if isinstance(gl12, (int, float)) else gl12,
                    c.risk_bucket,
                    c.confidence_bucket,
                    c.reason,
                    "engine_snapshot",
                    quality,
                    _iso_utc(as_of_dt),
                    _iso_riyadh(as_of_dt),
                ]
            )

        meta = {
            "ok": True,
            "core_version": TT_ADVISOR_CORE_VERSION,
            "include_news": include_news,
            "criteria": {
                "sources": sources,
                "tickers_override": tickers if tickers else None,
                "risk": risk,
                "confidence": confidence,
                "required_roi_1m_ratio": req_roi_1m,
                "required_roi_3m_ratio": req_roi_3m,
                "top_n": top_n,
                "invest_amount": invest_amount,
                "currency": currency,
                "min_price": min_price,
                "max_price": max_price,
                "exclude_sectors": exclude_sectors,
                "liquidity_min": liquidity_min,
                "max_position_pct": float(max_position_pct),
                "min_position_pct": float(min_position_pct),
                "as_of_utc": _iso_utc(as_of_dt),
            },
            "fetch": fetch_meta,
            "counts": {
                "universe_rows": len(universe_rows),
                "candidates_after_filters": len(candidates),
                "returned_rows": len(rows),
                "dropped": dropped,
            },
            "runtime_ms": int((time.time() - t0) * 1000),
        }

        if fetch_err:
            meta["warning"] = fetch_err
        if not rows and not fetch_err:
            meta["warning"] = "No candidates matched your filters (Risk/Confidence/ROI/Liquidity) or universe is empty."

        return {"headers": headers, "rows": rows, "meta": meta}

    except Exception as exc:
        meta = {
            "ok": False,
            "core_version": TT_ADVISOR_CORE_VERSION,
            "error": str(exc),
            "runtime_ms": int((time.time() - t0) * 1000),
        }
        return {"headers": headers, "rows": [], "meta": meta}


__all__ = ["run_investment_advisor", "TT_ADVISOR_CORE_VERSION"]
