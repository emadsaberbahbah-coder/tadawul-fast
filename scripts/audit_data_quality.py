#!/usr/bin/env python3
# scripts/audit_data_quality.py
"""
audit_data_quality.py
===========================================================
TADAWUL FAST BRIDGE – DATA & STRATEGY AUDITOR (v2.1.0)
===========================================================
PROD SAFE + ADVANCED DIAGNOSTICS + ENGINE-AWARE BATCHING

Purpose
- Scans symbols across dashboard pages (Market_Leaders, Global_Markets, etc.).
- Detects "Zombie" tickers (stale data, zero price, provider errors, delisted-like).
- Validates technical integrity (history depth for MA/RSI/MACD/Volatility).
- "Reality Check": compares AI Trend / Recommendation vs realized returns (1W).
- Generates:
  - Clean-up report (what to remove/fix)
  - Provider health snapshot (what is failing)
  - System self-diagnosis (rates, hotspots, top pages)

Usage
  python scripts/audit_data_quality.py
  python scripts/audit_data_quality.py --keys Market_Leaders Global_Markets
  python scripts/audit_data_quality.py --json-out audit_report.json
  python scripts/audit_data_quality.py --csv-out audit_report.csv
  python scripts/audit_data_quality.py --refresh 1 --concurrency 16
  python scripts/audit_data_quality.py --strict 1   (exit non-zero if CRITICAL found)

Notes
- PROD SAFE: If optional dependencies (engine/symbols_reader) are missing, exits 0 unless --strict 1.
- Engine aware:
  - Prefers engine.get_enriched_quotes (batch) if available
  - Falls back to per-symbol fetching with concurrency control
- GAS aligned: treats expected ROI fields as percent-ish in unified quote dict.
"""

from __future__ import annotations

import argparse
import asyncio
import csv
import json
import logging
import os
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple, Union


# =============================================================================
# Path & Logging
# =============================================================================
def _ensure_project_root_on_path() -> None:
    try:
        here = os.path.dirname(os.path.abspath(__file__))
        parent = os.path.dirname(here)
        for p in (here, parent):
            if p and p not in sys.path:
                sys.path.insert(0, p)
    except Exception:
        pass


_ensure_project_root_on_path()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("TFB.Audit")


# =============================================================================
# Deferred Imports (Resilient)
# =============================================================================
@dataclass
class _Deps:
    ok: bool
    settings: Any
    symbols_reader: Any
    get_engine: Any
    err: Optional[str] = None


def _load_deps() -> _Deps:
    settings = None
    symbols_reader = None
    get_engine = None
    err = None

    # settings
    try:
        from env import settings as _settings  # type: ignore
        settings = _settings
    except Exception:
        settings = None

    # symbols_reader
    try:
        import symbols_reader as _sr  # type: ignore
        symbols_reader = _sr
    except Exception:
        symbols_reader = None

    # engine
    try:
        from core.data_engine_v2 import get_engine as _get_engine  # type: ignore
        get_engine = _get_engine
    except Exception as e:
        get_engine = None
        err = f"core.data_engine_v2.get_engine import failed: {e}"

    ok = bool(symbols_reader is not None and get_engine is not None)
    return _Deps(ok=ok, settings=settings, symbols_reader=symbols_reader, get_engine=get_engine, err=err)


DEPS = _load_deps()


# =============================================================================
# Version + Thresholds
# =============================================================================
AUDIT_VERSION = "2.1.0"

DEFAULT_THRESHOLDS: Dict[str, Any] = {
    # Freshness
    "stale_hours": 72,                 # older than 3 days
    "hard_stale_hours": 168,           # older than 7 days => CRITICAL
    # Price sanity
    "min_price": 0.01,                 # <= considered zero
    "max_abs_change_pct": 60.0,        # extreme daily move suspicion
    # Forecast confidence (if 0..1 ratio or 0..100 percent handled)
    "min_confidence_pct": 30.0,
    # Technical requirements (minimum history points)
    "min_hist_macd": 35,               # MACD(26)+signal(9)
    "min_hist_rsi": 20,                # RSI(14) needs >14, plus buffer
    "min_hist_vol30": 35,              # volatility 30D
    "min_hist_ma50": 55,
    "min_hist_ma200": 210,
    # Strategy reality checks
    "trend_tolerance_pct": 4.0,        # move against trend triggers note
    "mom_divergence_drop_pct": 5.0,    # MACD bullish but -5% 1W
    "aggressive_roi_1m_pct": 20.0,     # high forecast in 1M
    "low_vol_pct": 10.0,               # low volatility threshold
    "risk_overest_score": 85.0,        # high risk but low vol
}

DEFAULT_PAGES = ["Market_Leaders", "Global_Markets", "Mutual_Funds", "Commodities_FX"]


# =============================================================================
# Utilities
# =============================================================================
def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _riyadh_iso(utc_iso: Optional[str] = None) -> str:
    tz = timezone(timedelta(hours=3))
    try:
        if utc_iso:
            dt = datetime.fromisoformat(str(utc_iso).replace("Z", "+00:00"))
        else:
            dt = datetime.now(timezone.utc)
        return dt.astimezone(tz).isoformat()
    except Exception:
        return datetime.now(tz).isoformat()


def _parse_iso_time(iso_str: Optional[str]) -> Optional[datetime]:
    if not iso_str:
        return None
    try:
        return datetime.fromisoformat(str(iso_str).replace("Z", "+00:00"))
    except Exception:
        return None


def _age_hours(utc_iso: Optional[str]) -> Optional[float]:
    dt = _parse_iso_time(utc_iso)
    if not dt:
        return None
    delta = datetime.now(timezone.utc) - dt.astimezone(timezone.utc)
    return delta.total_seconds() / 3600.0


def _safe_float(x: Any, default: Optional[float] = None) -> Optional[float]:
    if x is None or x == "":
        return default
    if isinstance(x, (int, float)) and not isinstance(x, bool):
        return float(x)
    try:
        s = str(x).strip().replace(",", "")
        if not s:
            return default
        if s.endswith("%"):
            s = s[:-1]
        return float(s)
    except Exception:
        return default


def _safe_int(x: Any, default: int = 0) -> int:
    try:
        return int(float(str(x).strip()))
    except Exception:
        return default


def _coerce_confidence_pct(x: Any) -> float:
    """
    Accepts:
      - 0..1 ratios
      - 0..100 percent
      - strings
    Returns 0..100
    """
    v = _safe_float(x, default=0.0) or 0.0
    if 0.0 <= v <= 1.0:
        return v * 100.0
    return v


def _norm_symbol(sym: Any) -> str:
    s = ("" if sym is None else str(sym)).strip().upper()
    if not s:
        return ""
    s = s.replace("TADAWUL:", "").replace(".TADAWUL", "")
    if s.endswith(".SA"):
        s = s[:-3] + ".SR"
    if s.isdigit():
        return f"{s}.SR"
    return s


def _dedupe_preserve(items: Iterable[str]) -> List[str]:
    out: List[str] = []
    seen = set()
    for x in items:
        if not x:
            continue
        if x in seen:
            continue
        seen.add(x)
        out.append(x)
    return out


def _unwrap_tuple(x: Any) -> Any:
    if isinstance(x, tuple) and len(x) >= 1:
        return x[0]
    return x


async def _maybe_await(x: Any) -> Any:
    import inspect
    if inspect.isawaitable(x):
        return await x
    return x


# =============================================================================
# Audit Logic
# =============================================================================
def _is_stale(last_updated_utc: Optional[str], thresholds: Dict[str, Any]) -> Tuple[bool, bool, Optional[float]]:
    """
    Returns: (stale, hard_stale, age_hours)
    """
    age = _age_hours(last_updated_utc)
    if age is None:
        return True, True, None
    stale = age > float(thresholds["stale_hours"])
    hard = age > float(thresholds["hard_stale_hours"])
    return stale, hard, age


def _infer_history_points(q: Dict[str, Any]) -> int:
    hp = _safe_int(q.get("history_points") or 0, 0)
    if hp > 0:
        return hp
    # Try alternate hints
    for k in ("history_len", "history_count", "prices_count"):
        v = _safe_int(q.get(k) or 0, 0)
        if v > 0:
            return v
    # If history array exists
    h = q.get("history")
    if isinstance(h, list):
        return len(h)
    return 0


def _required_history_points(q: Dict[str, Any], thresholds: Dict[str, Any]) -> int:
    """
    If the engine computed MA200/MA50/RSI/Volatility fields, ensure history_points supports them.
    Otherwise, apply a base requirement for MACD (common in your engine).
    """
    req = int(thresholds["min_hist_macd"])

    # If the quote includes these computed fields, enforce higher requirement.
    if q.get("ma200") is not None or q.get("MA200") is not None:
        req = max(req, int(thresholds["min_hist_ma200"]))
    if q.get("ma50") is not None or q.get("MA50") is not None:
        req = max(req, int(thresholds["min_hist_ma50"]))
    if q.get("volatility_30d") is not None or q.get("volatility (30d)") is not None:
        req = max(req, int(thresholds["min_hist_vol30"]))
    if q.get("rsi_14") is not None or q.get("RSI (14)") is not None or q.get("rsi") is not None:
        req = max(req, int(thresholds["min_hist_rsi"]))

    return req


def _perform_strategy_review(q: Dict[str, Any], thresholds: Dict[str, Any]) -> List[str]:
    """
    Advanced Self-Review: Checks if Technicals/AI match Reality.
    Returns list of strategy notes (not data integrity issues).
    """
    notes: List[str] = []

    trend_sig = str(q.get("trend_signal") or q.get("trend") or "NEUTRAL").strip().upper()
    rec = str(q.get("recommendation") or "HOLD").strip().upper()

    ret_1w = _safe_float(q.get("returns_1w") or q.get("Returns 1W %") or 0.0, 0.0) or 0.0
    vol_30d = _safe_float(q.get("volatility_30d") or q.get("Volatility (30D)") or 0.0, 0.0) or 0.0
    exp_roi_1m = _safe_float(q.get("expected_roi_1m") or q.get("Expected ROI % (1M)") or 0.0, 0.0) or 0.0
    exp_roi_3m = _safe_float(q.get("expected_roi_3m") or q.get("Expected ROI % (3M)") or 0.0, 0.0) or 0.0
    macd_hist = _safe_float(q.get("macd_hist") or q.get("MACD Hist") or 0.0, 0.0) or 0.0

    tol = float(thresholds["trend_tolerance_pct"])
    if trend_sig == "UPTREND" and ret_1w < -tol:
        notes.append("TREND_BREAK_BEAR")
    elif trend_sig == "DOWNTREND" and ret_1w > tol:
        notes.append("TREND_BREAK_BULL")

    if macd_hist > 0 and ret_1w < -float(thresholds["mom_divergence_drop_pct"]):
        notes.append("MOM_DIVERGENCE_BEAR")

    if exp_roi_1m > float(thresholds["aggressive_roi_1m_pct"]) and vol_30d < float(thresholds["low_vol_pct"]):
        notes.append("AGGRESSIVE_FORECAST_LOW_VOL")

    risk_score = _safe_float(q.get("risk_score") or q.get("Risk Score") or 50.0, 50.0) or 50.0
    if risk_score > float(thresholds["risk_overest_score"]) and vol_30d < float(thresholds["low_vol_pct"]):
        notes.append("RISK_OVEREST_LOW_VOL")

    # Recommendation plausibility checks
    if rec == "BUY" and exp_roi_1m < 0:
        notes.append("REC_BUY_BUT_NEG_ROI_1M")
    if rec == "SELL" and exp_roi_1m > 5:
        notes.append("REC_SELL_BUT_POS_ROI_1M")

    # Consistency between 1M and 3M
    if exp_roi_1m > 0 and exp_roi_3m < 0:
        notes.append("ROI_INCONSISTENT_1M_POS_3M_NEG")

    return notes


def _audit_quote(symbol: str, q: Dict[str, Any], thresholds: Dict[str, Any]) -> Dict[str, Any]:
    """
    Audits data integrity AND technical sufficiency + strategy divergence.
    Returns a normalized report dict.
    """
    issues: List[str] = []
    notes: List[str] = []

    # Core fields
    price = _safe_float(q.get("current_price") or q.get("price") or q.get("Price"), None)
    last_upd = q.get("last_updated_utc") or q.get("Last Updated (UTC)") or q.get("last_updated")

    # 1) Price Integrity
    if price is None or price < float(thresholds["min_price"]):
        issues.append("ZERO_PRICE")

    # 2) Freshness
    stale, hard_stale, age_h = _is_stale(str(last_upd) if last_upd else None, thresholds)
    if stale:
        issues.append("STALE_DATA")
    if hard_stale:
        issues.append("HARD_STALE_DATA")

    # 3) Technical Integrity
    hist_pts = _infer_history_points(q)
    req_hist = _required_history_points(q, thresholds)
    if hist_pts < req_hist:
        issues.append(f"INSUFFICIENT_HISTORY ({hist_pts}<{req_hist})")

    # 4) Provider error / engine error
    err = q.get("error") or q.get("Error")
    if err:
        issues.append("PROVIDER_ERROR")

    # 5) Outlier daily move (sanity)
    chg_pct = _safe_float(q.get("percent_change") or q.get("change_pct") or q.get("Change %"), None)
    if chg_pct is not None and abs(chg_pct) > float(thresholds["max_abs_change_pct"]):
        issues.append("EXTREME_DAILY_MOVE")

    # 6) Confidence sanity (if exists)
    conf_pct = _coerce_confidence_pct(q.get("forecast_confidence") or q.get("Forecast Confidence") or 0)
    if conf_pct > 0 and conf_pct < float(thresholds["min_confidence_pct"]):
        issues.append("LOW_CONFIDENCE")

    # 7) Zombie heuristics
    dq = str(q.get("data_quality") or q.get("Data Quality") or "").strip().upper()
    name = str(q.get("name") or q.get("Name") or "").strip()
    if ("ZERO_PRICE" in issues and "STALE_DATA" in issues) or (dq in ("MISSING", "BAD", "BROKEN")):
        issues.append("ZOMBIE_TICKER")
    if not name and dq in ("MISSING", "PARTIAL") and stale:
        issues.append("MISSING_METADATA")

    # Strategy notes (only if quote has enough signals)
    try:
        notes = _perform_strategy_review(q, thresholds)
    except Exception:
        notes = []

    # Severity
    status = "OK"
    if "PROVIDER_ERROR" in issues or "ZERO_PRICE" in issues or "HARD_STALE_DATA" in issues or "ZOMBIE_TICKER" in issues:
        status = "CRITICAL"
    elif issues:
        status = "WARNING"
    elif notes:
        status = "REVIEW"

    return {
        "symbol": symbol,
        "status": status,
        "issues": issues,
        "strategy_notes": notes,
        "price": price,
        "change_pct": chg_pct,
        "age_hours": round(age_h, 2) if isinstance(age_h, (int, float)) else None,
        "last_updated_utc": str(last_upd) if last_upd else None,
        "last_updated_riyadh": _riyadh_iso(str(last_upd)) if last_upd else _riyadh_iso(),
        "source": str(q.get("data_source") or q.get("Data Source") or "unknown"),
        "data_quality": dq or "UNKNOWN",
        "confidence_pct": round(conf_pct, 1),
        "history_points": hist_pts,
        "required_history_points": req_hist,
        "error": (str(err)[:200] if err else None),
    }


# =============================================================================
# Engine Fetching (Batch-first, fallback concurrent singles)
# =============================================================================
def _supports_param(fn: Any, name: str) -> bool:
    try:
        import inspect
        sig = inspect.signature(fn)
        return name in sig.parameters
    except Exception:
        return False


async def _call_engine_batch(engine: Any, symbols: List[str], refresh: bool) -> Dict[str, Dict[str, Any]]:
    """
    Returns map: {symbol -> quote_dict}
    Handles list/dict returns and tuple payloads.
    """
    if engine is None:
        return {s: {"error": "NO_ENGINE"} for s in symbols}

    fn = getattr(engine, "get_enriched_quotes", None)
    if not callable(fn):
        return {}

    try:
        kwargs = {}
        if _supports_param(fn, "refresh"):
            kwargs["refresh"] = refresh
        res = await _maybe_await(fn(symbols, **kwargs))
    except Exception as e:
        logger.warning("Batch fetch failed: %s", e)
        return {}

    res = _unwrap_tuple(res)

    out: Dict[str, Dict[str, Any]] = {}
    if isinstance(res, dict):
        # normalize keys
        for k, v in res.items():
            kk = _norm_symbol(k)
            vv = _unwrap_tuple(v)
            out[kk or str(k)] = vv if isinstance(vv, dict) else (vv.__dict__ if hasattr(vv, "__dict__") else {})
        # ensure requested symbols exist in map
        for s in symbols:
            out.setdefault(_norm_symbol(s), {})
        return out

    if isinstance(res, list):
        # order-based
        for i, s in enumerate(symbols):
            item = res[i] if i < len(res) else {}
            item = _unwrap_tuple(item)
            d = item if isinstance(item, dict) else (item.__dict__ if hasattr(item, "__dict__") else {})
            out[_norm_symbol(s)] = d
        return out

    # unknown format
    for s in symbols:
        out[_norm_symbol(s)] = {"error": "INVALID_BATCH_FORMAT"}
    return out


async def _call_engine_single(engine: Any, symbol: str, refresh: bool) -> Dict[str, Any]:
    if engine is None:
        return {"error": "NO_ENGINE"}

    fn = getattr(engine, "get_enriched_quote", None) or getattr(engine, "get_quote", None)
    if not callable(fn):
        return {"error": "ENGINE_NO_QUOTE_METHOD"}

    try:
        kwargs = {}
        if _supports_param(fn, "refresh"):
            kwargs["refresh"] = refresh
        res = await _maybe_await(fn(symbol, **kwargs))
        res = _unwrap_tuple(res)
        return res if isinstance(res, dict) else (res.__dict__ if hasattr(res, "__dict__") else {})
    except Exception as e:
        return {"error": str(e)}


async def _fetch_quotes(engine: Any, symbols: List[str], *, refresh: bool, concurrency: int) -> Dict[str, Dict[str, Any]]:
    """
    Fetch quotes for symbols using batch-first strategy.
    """
    symbols = [_norm_symbol(s) for s in symbols if _norm_symbol(s)]
    symbols = _dedupe_preserve(symbols)

    # 1) Batch
    batch_map = await _call_engine_batch(engine, symbols, refresh=refresh)
    if batch_map:
        return batch_map

    # 2) Fallback concurrent singles
    sem = asyncio.Semaphore(max(1, int(concurrency)))

    async def one(s: str) -> Tuple[str, Dict[str, Any]]:
        async with sem:
            d = await _call_engine_single(engine, s, refresh=refresh)
            return s, d

    pairs = await asyncio.gather(*[one(s) for s in symbols], return_exceptions=True)

    out: Dict[str, Dict[str, Any]] = {}
    for i, p in enumerate(pairs):
        if isinstance(p, Exception):
            s = symbols[i]
            out[s] = {"error": str(p)}
        else:
            s, d = p
            out[s] = d if isinstance(d, dict) else {"error": "BAD_SINGLE_FORMAT"}
    return out


# =============================================================================
# Page symbol loading
# =============================================================================
def _get_spreadsheet_id(args: argparse.Namespace) -> Optional[str]:
    sid = (args.sheet_id or "").strip()
    if sid:
        return sid

    # Try env settings
    if DEPS.settings is not None:
        try:
            sid = str(getattr(DEPS.settings, "default_spreadsheet_id", "") or "").strip()
            if sid:
                return sid
        except Exception:
            pass

    sid = (os.getenv("DEFAULT_SPREADSHEET_ID") or "").strip()
    return sid or None


def _get_target_keys(args: argparse.Namespace) -> List[str]:
    if args.keys:
        return [str(k).strip() for k in args.keys if str(k).strip()]

    # prefer PAGE_REGISTRY if present
    sr = DEPS.symbols_reader
    if sr is not None:
        try:
            reg = getattr(sr, "PAGE_REGISTRY", None)
            if isinstance(reg, dict) and reg:
                return list(reg.keys())
        except Exception:
            pass

    return list(DEFAULT_PAGES)


def _read_page_symbols(page_key: str, sid: str) -> List[str]:
    sr = DEPS.symbols_reader
    if sr is None:
        return []
    try:
        sym_data = sr.get_page_symbols(page_key, spreadsheet_id=sid)
        if isinstance(sym_data, dict):
            # common patterns
            for k in ("all", "symbols", "tickers", "items"):
                v = sym_data.get(k)
                if isinstance(v, list):
                    return [_norm_symbol(x) for x in v if _norm_symbol(x)]
            # fallback: values
            vals = []
            for v in sym_data.values():
                if isinstance(v, list):
                    vals.extend(v)
            return [_norm_symbol(x) for x in vals if _norm_symbol(x)]
        if isinstance(sym_data, list):
            return [_norm_symbol(x) for x in sym_data if _norm_symbol(x)]
    except Exception as e:
        logger.warning("   Skipping %s: %s", page_key, e)
    return []


# =============================================================================
# Reporting
# =============================================================================
def _print_table(rows: List[Dict[str, Any]], *, limit: int = 400) -> None:
    if not rows:
        return

    # fixed columns
    cols = ["symbol", "origin_page", "status", "source", "data_quality", "age_hours", "issues", "strategy_notes"]
    header = f"{'SYMBOL':<12} | {'PAGE':<16} | {'STATUS':<9} | {'SRC':<10} | {'DQ':<8} | {'AGE(h)':<7} | DETAILS"
    print("\n" + header)
    print("-" * min(140, max(100, len(header) + 20)))

    for i, r in enumerate(rows[:limit]):
        sym = str(r.get("symbol") or "")[:12]
        page = str(r.get("origin_page") or "")[:16]
        status = str(r.get("status") or "")[:9]
        src = str(r.get("source") or "")[:10]
        dq = str(r.get("data_quality") or "")[:8]
        age = r.get("age_hours")
        age_s = f"{age:.1f}" if isinstance(age, (int, float)) else "?"
        details = []
        for x in (r.get("issues") or []):
            details.append(str(x))
        for x in (r.get("strategy_notes") or []):
            details.append(str(x))
        d = ", ".join(details)[:200]
        print(f"{sym:<12} | {page:<16} | {status:<9} | {src:<10} | {dq:<8} | {age_s:<7} | {d}")

    if len(rows) > limit:
        print(f"... ({len(rows) - limit} more rows not shown)")


def _summarize(reports: List[Dict[str, Any]]) -> Dict[str, Any]:
    sev = {"OK": 0, "REVIEW": 0, "WARNING": 0, "CRITICAL": 0}
    issue_counts: Dict[str, int] = {}
    note_counts: Dict[str, int] = {}
    page_counts: Dict[str, int] = {}
    provider_counts: Dict[str, int] = {}

    for r in reports:
        s = str(r.get("status") or "OK").upper()
        sev[s] = sev.get(s, 0) + 1

        page = str(r.get("origin_page") or "UNKNOWN")
        page_counts[page] = page_counts.get(page, 0) + (1 if s != "OK" else 0)

        src = str(r.get("source") or "unknown")
        provider_counts[src] = provider_counts.get(src, 0) + (1 if s != "OK" else 0)

        for it in (r.get("issues") or []):
            k = str(it)
            issue_counts[k] = issue_counts.get(k, 0) + 1
        for it in (r.get("strategy_notes") or []):
            k = str(it)
            note_counts[k] = note_counts.get(k, 0) + 1

    top_issues = sorted(issue_counts.items(), key=lambda x: x[1], reverse=True)[:12]
    top_notes = sorted(note_counts.items(), key=lambda x: x[1], reverse=True)[:12]
    top_pages = sorted(page_counts.items(), key=lambda x: x[1], reverse=True)[:12]
    top_providers = sorted(provider_counts.items(), key=lambda x: x[1], reverse=True)[:12]

    total = max(1, len(reports))
    critical_rate = (sev.get("CRITICAL", 0) / total) * 100.0
    warn_rate = (sev.get("WARNING", 0) / total) * 100.0
    review_rate = (sev.get("REVIEW", 0) / total) * 100.0

    return {
        "severity_counts": sev,
        "rates_pct": {
            "critical": round(critical_rate, 2),
            "warning": round(warn_rate, 2),
            "review": round(review_rate, 2),
        },
        "top_issues": top_issues,
        "top_strategy_notes": top_notes,
        "top_pages_with_alerts": top_pages,
        "top_providers_with_alerts": top_providers,
    }


def _write_json(path: str, payload: Dict[str, Any]) -> None:
    with open(path, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2, ensure_ascii=False)


def _write_csv(path: str, rows: List[Dict[str, Any]]) -> None:
    if not rows:
        # still create file with headers
        headers = ["symbol", "origin_page", "status", "source", "data_quality", "age_hours", "issues", "strategy_notes", "last_updated_utc", "confidence_pct", "history_points", "required_history_points", "error"]
        with open(path, "w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=headers)
            w.writeheader()
        return

    headers = [
        "symbol",
        "origin_page",
        "status",
        "source",
        "data_quality",
        "age_hours",
        "confidence_pct",
        "history_points",
        "required_history_points",
        "last_updated_utc",
        "issues",
        "strategy_notes",
        "error",
    ]
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=headers)
        w.writeheader()
        for r in rows:
            row = dict(r)
            row["issues"] = ", ".join([str(x) for x in (row.get("issues") or [])])
            row["strategy_notes"] = ", ".join([str(x) for x in (row.get("strategy_notes") or [])])
            w.writerow({k: row.get(k) for k in headers})


# =============================================================================
# Audit Runner
# =============================================================================
async def _run_audit(
    *,
    keys: List[str],
    sid: str,
    thresholds: Dict[str, Any],
    refresh: bool,
    concurrency: int,
    max_symbols_per_page: int,
    include_ok: bool,
) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    # Engine init
    try:
        engine = await _maybe_await(DEPS.get_engine())
        if not engine:
            return [], {"error": "Failed to initialize Data Engine."}
    except Exception as e:
        return [], {"error": f"Engine initialization error: {e}"}

    logger.info("🚀 Starting Data Quality & Strategy Audit (v%s)", AUDIT_VERSION)

    all_reports: List[Dict[str, Any]] = []
    stats = {
        "audit_version": AUDIT_VERSION,
        "total_assets": 0,
        "pages_scanned": 0,
        "alerts": 0,
        "critical": 0,
        "warning": 0,
        "review": 0,
        "ok": 0,
        "trend_breaks": 0,
        "data_holes": 0,
        "provider_errors": 0,
        "aggressive_forecasts": 0,
    }

    for key in keys:
        key = str(key).strip()
        if not key:
            continue

        logger.info("🔍 Scanning Page: %s", key)

        symbols = _read_page_symbols(key, sid)
        symbols = [s for s in symbols if s]
        symbols = _dedupe_preserve(symbols)

        if max_symbols_per_page > 0 and len(symbols) > max_symbols_per_page:
            symbols = symbols[:max_symbols_per_page]

        if not symbols:
            logger.info("   -> Empty page.")
            continue

        stats["pages_scanned"] += 1

        # Fetch (refresh optional)
        logger.info("   -> Fetching %d symbols (refresh=%s, concurrency=%d)...", len(symbols), refresh, concurrency)
        quotes_map = await _fetch_quotes(engine, symbols, refresh=refresh, concurrency=concurrency)

        page_alerts = 0
        for sym in symbols:
            stats["total_assets"] += 1
            q = quotes_map.get(sym) or {}
            if not isinstance(q, dict):
                q = {}

            report = _audit_quote(sym, q, thresholds)
            report["origin_page"] = key

            status = report["status"]
            if status == "OK":
                stats["ok"] += 1
            elif status == "REVIEW":
                stats["review"] += 1
            elif status == "WARNING":
                stats["warning"] += 1
            elif status == "CRITICAL":
                stats["critical"] += 1

            # Aggregate stats
            for note in report.get("strategy_notes", []):
                if "TREND_BREAK" in str(note):
                    stats["trend_breaks"] += 1
                if "AGGRESSIVE" in str(note):
                    stats["aggressive_forecasts"] += 1

            for issue in report.get("issues", []):
                s_issue = str(issue)
                if "INSUFFICIENT_HISTORY" in s_issue or "ZERO_PRICE" in s_issue:
                    stats["data_holes"] += 1
                if "PROVIDER_ERROR" in s_issue:
                    stats["provider_errors"] += 1

            # Collect
            if include_ok or status != "OK":
                if status != "OK":
                    page_alerts += 1
                    stats["alerts"] += 1
                all_reports.append(report)

        logger.info("   -> Alerts on %s: %d", key, page_alerts)

    return all_reports, stats


# =============================================================================
# Main
# =============================================================================
def main() -> None:
    parser = argparse.ArgumentParser(description="TFB Data Quality & Strategy Auditor")
    parser.add_argument("--keys", nargs="*", default=None, help="Specific pages to audit (default: all registered pages)")
    parser.add_argument("--sheet-id", dest="sheet_id", help="Override Spreadsheet ID")
    parser.add_argument("--json-out", help="Save full report to JSON file")
    parser.add_argument("--csv-out", help="Save alerts to CSV file")
    parser.add_argument("--refresh", type=int, default=1, help="1=force refresh from providers, 0=allow cache (default: 1)")
    parser.add_argument("--concurrency", type=int, default=12, help="Concurrency for per-symbol fallback fetching (default: 12)")
    parser.add_argument("--max-per-page", type=int, default=0, help="Limit symbols per page (0=unlimited)")
    parser.add_argument("--include-ok", type=int, default=0, help="1=include OK rows in output payload (default: 0)")
    parser.add_argument("--strict", type=int, default=0, help="1=exit non-zero if CRITICAL issues exist or deps missing (default: 0)")
    parser.add_argument("--stale-hours", type=int, default=None, help="Override stale_hours threshold")
    parser.add_argument("--hard-stale-hours", type=int, default=None, help="Override hard_stale_hours threshold")
    args = parser.parse_args()

    strict = bool(int(args.strict or 0))

    # Dependency readiness
    if not DEPS.ok:
        msg = f"Audit skipped (environment not ready). Missing deps. Details: {DEPS.err or 'symbols_reader/get_engine missing'}"
        if strict:
            logger.error(msg)
            sys.exit(2)
        logger.warning(msg)
        sys.exit(0)

    sid = _get_spreadsheet_id(args)
    if not sid:
        msg = "No Spreadsheet ID found (check --sheet-id, env settings.default_spreadsheet_id, or DEFAULT_SPREADSHEET_ID)."
        if strict:
            logger.error(msg)
            sys.exit(2)
        logger.warning(msg)
        sys.exit(0)

    keys = _get_target_keys(args)
    if not keys:
        msg = "No pages found to audit (keys empty)."
        if strict:
            logger.error(msg)
            sys.exit(2)
        logger.warning(msg)
        sys.exit(0)

    # thresholds (allow overrides)
    thresholds = dict(DEFAULT_THRESHOLDS)
    if args.stale_hours is not None:
        thresholds["stale_hours"] = int(args.stale_hours)
    if args.hard_stale_hours is not None:
        thresholds["hard_stale_hours"] = int(args.hard_stale_hours)

    refresh = bool(int(args.refresh or 0))
    concurrency = max(1, min(40, int(args.concurrency or 12)))
    max_per_page = max(0, int(args.max_per_page or 0))
    include_ok = bool(int(args.include_ok or 0))

    # Run
    try:
        reports, stats = asyncio.run(
            _run_audit(
                keys=keys,
                sid=sid,
                thresholds=thresholds,
                refresh=refresh,
                concurrency=concurrency,
                max_symbols_per_page=max_per_page,
                include_ok=include_ok,
            )
        )
    except Exception as e:
        logger.critical("Audit run crashed: %s", e)
        sys.exit(1)

    # If include_ok=0 => reports are only alerts; else may include OK
    alerts_only = [r for r in reports if str(r.get("status") or "OK") != "OK"]

    # Summary
    if include_ok:
        summary = _summarize(reports)
        total_seen = len(reports)
    else:
        # still summarize full stats from alerts subset to rank top issues
        summary = _summarize(alerts_only)
        total_seen = len(alerts_only)

    # Console output
    if not alerts_only:
        logger.info("\n✅ AUDIT COMPLETE: No alerts found. System looks healthy.")
    else:
        logger.info("\n=== ⚠️  AUDIT REPORT: %d ITEMS REQUIRING ATTENTION ===", len(alerts_only))
        _print_table(alerts_only, limit=600)

    # System diagnosis
    total_assets = int(stats.get("total_assets") or 0) or 1
    crit = int(stats.get("critical") or 0)
    warn = int(stats.get("warning") or 0)
    review = int(stats.get("review") or 0)

    logger.info("\n🧠 SYSTEM DIAGNOSIS (v%s):", AUDIT_VERSION)
    logger.info("   Pages scanned: %s", stats.get("pages_scanned"))
    logger.info("   Assets analyzed: %s", total_assets)
    logger.info("   CRITICAL: %d | WARNING: %d | REVIEW: %d | OK: %d", crit, warn, review, int(stats.get("ok") or 0))
    logger.info("   Trend breaks: %d | Aggressive forecasts: %d", int(stats.get("trend_breaks") or 0), int(stats.get("aggressive_forecasts") or 0))
    logger.info("   Data holes: %d | Provider errors: %d", int(stats.get("data_holes") or 0), int(stats.get("provider_errors") or 0))

    # Top lists
    if alerts_only:
        logger.info("   Top Issues: %s", summary.get("top_issues"))
        logger.info("   Top Strategy Notes: %s", summary.get("top_strategy_notes"))
        logger.info("   Pages with most alerts: %s", summary.get("top_pages_with_alerts"))
        logger.info("   Providers with most alerts: %s", summary.get("top_providers_with_alerts"))

    # Save outputs
    payload = {
        "audit_time_utc": _utc_now_iso(),
        "audit_time_riyadh": _riyadh_iso(),
        "audit_version": AUDIT_VERSION,
        "spreadsheet_id": sid,
        "keys": keys,
        "thresholds": thresholds,
        "run_config": {
            "refresh": refresh,
            "concurrency": concurrency,
            "max_per_page": max_per_page,
            "include_ok": include_ok,
            "strict": strict,
        },
        "stats": stats,
        "summary": summary,
        "alerts": alerts_only,
        "reports": reports if include_ok else None,
    }

    if args.json_out:
        try:
            _write_json(args.json_out, payload)
            logger.info("📄 JSON report saved to %s", args.json_out)
        except Exception as e:
            logger.error("Failed to write JSON: %s", e)

    if args.csv_out:
        try:
            _write_csv(args.csv_out, alerts_only)
            logger.info("📄 CSV alerts saved to %s", args.csv_out)
        except Exception as e:
            logger.error("Failed to write CSV: %s", e)

    # Exit code policy (strict)
    if strict:
        if crit > 0:
            sys.exit(3)
        # If deps missing earlier we already exited 2
    sys.exit(0)


if __name__ == "__main__":
    main()
