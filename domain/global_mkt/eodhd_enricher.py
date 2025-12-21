# domain/global_mkt/eodhd_enricher.py
from __future__ import annotations

import json
import math
import os
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urlencode
from urllib.request import Request, urlopen


EODHD_BASE = os.getenv("EODHD_BASE_URL", "https://eodhd.com/api")
EODHD_KEY = os.getenv("EODHD_API_KEY", "").strip()
HTTP_TIMEOUT = float(os.getenv("HTTP_TIMEOUT", "15"))


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _http_get_json(url: str) -> Any:
    req = Request(url, headers={"User-Agent": "TFB/5.0"})
    with urlopen(req, timeout=HTTP_TIMEOUT) as resp:
        raw = resp.read().decode("utf-8", errors="replace")
    return json.loads(raw) if raw else None


def _safe_num(v: Any) -> Optional[float]:
    try:
        if v is None or v == "":
            return None
        return float(v)
    except Exception:
        return None


def _pct(a: Optional[float], b: Optional[float]) -> Optional[float]:
    # (a - b)/b
    if a is None or b is None or b == 0:
        return None
    return (a - b) / b


def _stdev(xs: List[float]) -> Optional[float]:
    if len(xs) < 2:
        return None
    m = sum(xs) / len(xs)
    var = sum((x - m) ** 2 for x in xs) / (len(xs) - 1)
    return math.sqrt(var)


def _sma(values: List[float], window: int) -> Optional[float]:
    if len(values) < window or window <= 0:
        return None
    return sum(values[-window:]) / window


def _rsi(values: List[float], period: int = 14) -> Optional[float]:
    if len(values) < period + 1:
        return None
    gains = []
    losses = []
    for i in range(-period, 0):
        diff = values[i] - values[i - 1]
        if diff >= 0:
            gains.append(diff)
        else:
            losses.append(-diff)
    avg_gain = sum(gains) / period
    avg_loss = sum(losses) / period
    if avg_loss == 0:
        return 100.0
    rs = avg_gain / avg_loss
    return 100.0 - (100.0 / (1.0 + rs))


def _max_drawdown(prices: List[float]) -> Optional[float]:
    if not prices:
        return None
    peak = prices[0]
    mdd = 0.0
    for p in prices:
        if p > peak:
            peak = p
        dd = (p - peak) / peak
        if dd < mdd:
            mdd = dd
    return mdd  # negative value (e.g., -0.32)


def _eodhd_real_time(symbol: str) -> Dict[str, Any]:
    # /api/real-time/{symbol}?api_token=...&fmt=json
    q = urlencode({"api_token": EODHD_KEY, "fmt": "json"})
    url = f"{EODHD_BASE}/real-time/{symbol}?{q}"
    data = _http_get_json(url) or {}
    return data if isinstance(data, dict) else {}


def _eodhd_fundamentals(symbol: str) -> Dict[str, Any]:
    # /api/fundamentals/{symbol}?api_token=...&fmt=json
    q = urlencode({"api_token": EODHD_KEY, "fmt": "json"})
    url = f"{EODHD_BASE}/fundamentals/{symbol}?{q}"
    data = _http_get_json(url) or {}
    return data if isinstance(data, dict) else {}


def _eodhd_eod_history(symbol: str, days: int = 370) -> List[Dict[str, Any]]:
    # /api/eod/{symbol}?api_token=...&fmt=json&from=YYYY-MM-DD
    # We'll pull ~1Y+ to compute 52W + 1Y returns.
    if days < 30:
        days = 30
    to_dt = datetime.now(timezone.utc).date()
    from_dt = (datetime.now(timezone.utc).date()).fromordinal(to_dt.toordinal() - days)
    q = urlencode(
        {
            "api_token": EODHD_KEY,
            "fmt": "json",
            "from": from_dt.isoformat(),
            "to": to_dt.isoformat(),
            "period": "d",
        }
    )
    url = f"{EODHD_BASE}/eod/{symbol}?{q}"
    data = _http_get_json(url) or []
    if isinstance(data, list):
        return [x for x in data if isinstance(x, dict)]
    return []


def _extract_fundamentals(f: Dict[str, Any]) -> Dict[str, Any]:
    # EODHD fundamentals is nested; we defensively extract common fields
    gen = f.get("General") or {}
    highlights = f.get("Highlights") or {}
    valuation = f.get("Valuation") or {}
    shares = f.get("SharesStats") or {}
    tech = f.get("Technicals") or {}
    splits = f.get("SplitsDividends") or {}
    analyst = f.get("AnalystRatings") or {}

    return {
        "company_name": gen.get("Name"),
        "exchange": gen.get("Exchange"),
        "mic": gen.get("Code"),
        "country": gen.get("CountryName") or gen.get("CountryISO"),
        "sector": gen.get("Sector"),
        "industry": gen.get("Industry"),
        "currency": gen.get("CurrencyCode"),
        "isin": gen.get("ISIN"),
        "cusip": gen.get("CUSIP"),
        "website": gen.get("WebURL"),
        "beta": _safe_num(highlights.get("Beta")),
        "eps_ttm": _safe_num(highlights.get("EarningsShare")),
        "revenue_ttm": _safe_num(highlights.get("RevenueTTM")),
        "revenue_growth_yoy": _safe_num(highlights.get("RevenueGrowth")),
        "gross_margin": _safe_num(highlights.get("GrossMarginTTM")),
        "operating_margin": _safe_num(highlights.get("OperatingMarginTTM")),
        "net_margin": _safe_num(highlights.get("ProfitMargin")),
        "roe": _safe_num(highlights.get("ReturnOnEquityTTM")),
        "roa": _safe_num(highlights.get("ReturnOnAssetsTTM")),
        "market_cap": _safe_num(highlights.get("MarketCapitalization")),
        "pe_ttm": _safe_num(valuation.get("TrailingPE")),
        "pe_forward": _safe_num(valuation.get("ForwardPE")),
        "pb": _safe_num(valuation.get("PriceBookMRQ")),
        "ps": _safe_num(valuation.get("PriceSalesTTM")),
        "peg": _safe_num(valuation.get("PEGRatio")),
        "enterprise_value": _safe_num(valuation.get("EnterpriseValue")),
        "ev_ebitda": _safe_num(valuation.get("EnterpriseValueEbitda")),
        "shares_outstanding": _safe_num(shares.get("SharesOutstanding")),
        "float_shares": _safe_num(shares.get("SharesFloat")),
        "dividend_yield": _safe_num(splits.get("ForwardAnnualDividendYield")),
        "dividend_rate": _safe_num(splits.get("ForwardAnnualDividendRate")),
        "payout_ratio": _safe_num(splits.get("PayoutRatio")),
        "ex_dividend_date": splits.get("ExDividendDate"),
        "analyst_target_price": _safe_num(analyst.get("TargetPrice")),
        "next_earnings_date": gen.get("NextEarningsDate") or "",
    }


def enrich_symbol(symbol: str) -> Dict[str, Any]:
    if not EODHD_KEY:
        raise ValueError("EODHD_API_KEY is missing in Render env vars.")

    rt = _eodhd_real_time(symbol)
    f = _eodhd_fundamentals(symbol)
    h = _eodhd_eod_history(symbol, days=380)

    out: Dict[str, Any] = {}
    out["symbol"] = symbol

    # --- Real-time snapshot (defensive keys)
    last = _safe_num(rt.get("close") or rt.get("price") or rt.get("last"))
    prev = _safe_num(rt.get("previousClose") or rt.get("previous_close") or rt.get("prev_close"))
    opn = _safe_num(rt.get("open"))
    high = _safe_num(rt.get("high"))
    low = _safe_num(rt.get("low"))
    bid = _safe_num(rt.get("bid"))
    ask = _safe_num(rt.get("ask"))
    vol = _safe_num(rt.get("volume"))
    mc = _safe_num(rt.get("marketCap") or rt.get("market_cap"))

    out.update(
        {
            "last_price": last,
            "previous_close": prev,
            "open": opn,
            "day_high": high,
            "day_low": low,
            "bid": bid,
            "ask": ask,
            "spread": (ask - bid) if (ask is not None and bid is not None) else None,
            "change": (last - prev) if (last is not None and prev is not None) else None,
            "change_percent": _pct(last, prev),
            "volume": vol,
            "market_cap": mc,
        }
    )

    # --- Fundamentals extraction
    out.update(_extract_fundamentals(f))

    # --- History-derived metrics
    closes: List[float] = []
    for row in h:
        c = _safe_num(row.get("close"))
        if c is not None:
            closes.append(c)

    if closes:
        # 52w range from last ~252 trading days (approx)
        last_252 = closes[-252:] if len(closes) >= 252 else closes[:]
        out["high_52w"] = max(last_252) if last_252 else None
        out["low_52w"] = min(last_252) if last_252 else None
        if out.get("high_52w") and out.get("low_52w") and last is not None:
            hi = float(out["high_52w"])
            lo = float(out["low_52w"])
            out["position_52w_percent"] = (last - lo) / (hi - lo) if (hi - lo) != 0 else None

        # returns based on trading-day approximations
        def _ret(n: int) -> Optional[float]:
            if len(closes) <= n:
                return None
            return _pct(closes[-1], closes[-1 - n])

        out["return_1w"] = _ret(5)
        out["return_1m"] = _ret(21)
        out["return_3m"] = _ret(63)
        out["return_6m"] = _ret(126)
        out["return_1y"] = _ret(252)

        # ytd return: approximate using first close in current year
        try:
            year = datetime.now(timezone.utc).year
            # history has dates; find first date in year
            first_y = None
            for row in h:
                d = row.get("date")
                if not d:
                    continue
                if str(d).startswith(str(year)):
                    c = _safe_num(row.get("close"))
                    if c is not None:
                        first_y = c
                        break
            out["ytd_return"] = _pct(closes[-1], first_y) if first_y else None
        except Exception:
            out["ytd_return"] = None

        # daily returns for vol
        rets: List[float] = []
        for i in range(1, len(closes)):
            if closes[i - 1] != 0:
                rets.append((closes[i] - closes[i - 1]) / closes[i - 1])

        # annualize stdev from daily returns (sqrt(252))
        if len(rets) >= 30:
            r30 = rets[-30:]
            v30 = _stdev(r30)
            out["volatility_30d"] = (v30 * math.sqrt(252)) if v30 is not None else None
        else:
            out["volatility_30d"] = None

        if len(rets) >= 90:
            r90 = rets[-90:]
            v90 = _stdev(r90)
            out["volatility_90d"] = (v90 * math.sqrt(252)) if v90 is not None else None
        else:
            out["volatility_90d"] = None

        out["max_drawdown_1y"] = _max_drawdown(last_252)

        # indicators
        out["sma_20"] = _sma(closes, 20)
        out["sma_50"] = _sma(closes, 50)
        out["sma_200"] = _sma(closes, 200)
        out["rsi_14"] = _rsi(closes, 14)

    # --- Expectations
    tp = out.get("analyst_target_price")
    if tp is not None and last is not None and last != 0:
        out["upside_to_target_percent"] = (tp - last) / last
    else:
        out["upside_to_target_percent"] = None

    # --- Simple scoring (deterministic, Phase-1.5)
    # value: lower PE better, PB better
    pe = out.get("pe_ttm")
    pb = out.get("pb")
    value_score = 0.0
    if pe is not None and pe > 0:
        value_score += max(0.0, (30.0 - pe) / 30.0)
    if pb is not None and pb > 0:
        value_score += max(0.0, (5.0 - pb) / 5.0)
    value_score = min(1.0, value_score / 2.0) if value_score > 0 else None

    # quality: ROE + margins
    roe = out.get("roe")
    nm = out.get("net_margin")
    quality_score = 0.0
    if roe is not None:
        quality_score += min(1.0, max(0.0, roe / 0.25))  # 25% ROE => 1
    if nm is not None:
        quality_score += min(1.0, max(0.0, nm / 0.20))   # 20% margin => 1
    quality_score = min(1.0, quality_score / 2.0) if quality_score > 0 else None

    # momentum: 3m + 1y
    r3 = out.get("return_3m")
    r1y = out.get("return_1y")
    momentum_score = 0.0
    if r3 is not None:
        momentum_score += min(1.0, max(0.0, r3 / 0.30))   # 30% => 1
    if r1y is not None:
        momentum_score += min(1.0, max(0.0, r1y / 0.50))  # 50% => 1
    momentum_score = min(1.0, momentum_score / 2.0) if momentum_score > 0 else None

    # risk: volatility (lower is better)
    vol30 = out.get("volatility_30d")
    risk_score = None
    if vol30 is not None and vol30 > 0:
        # 20% annual vol => good; 60% => bad
        risk_score = 1.0 - min(1.0, max(0.0, (vol30 - 0.20) / 0.40))

    out["value_score"] = value_score
    out["quality_score"] = quality_score
    out["momentum_score"] = momentum_score
    out["risk_score"] = risk_score

    # opportunity: upside + blended scores - risk penalty
    upside = out.get("upside_to_target_percent") or 0.0
    vs = out.get("value_score") or 0.0
    qs = out.get("quality_score") or 0.0
    ms = out.get("momentum_score") or 0.0
    rs = out.get("risk_score") if out.get("risk_score") is not None else 0.5

    opp = (0.45 * upside) + (0.20 * vs) + (0.20 * qs) + (0.15 * ms)
    opp = opp * (0.6 + 0.4 * rs)  # penalize high risk
    out["opportunity_score"] = opp

    out["recommendation"] = "BUY" if opp >= 0.15 else ("HOLD" if opp >= 0.05 else "WATCH")
    out["rationale"] = "Auto-scored from EODHD snapshot + fundamentals + 1Y history."

    # Expected ROI (simple baseline; Phase-2 will improve)
    out["expected_roi_30d"] = out.get("return_1m")
    out["expected_roi_90d"] = out.get("return_3m")
    out["expected_roi_365d"] = out.get("return_1y")

    out["last_updated"] = _utc_now_iso()
    out["data_source"] = "EODHD"
    out["data_quality"] = "ENRICHED"

    return out


def enrich_many(symbols: List[str]) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    rows: List[Dict[str, Any]] = []
    errors: List[Dict[str, Any]] = []
    for s in symbols:
        sym = str(s or "").strip().upper()
        if not sym:
            continue
        try:
            rows.append(enrich_symbol(sym))
        except Exception as e:
            errors.append({"symbol": sym, "error": str(e)})
    return rows, errors
