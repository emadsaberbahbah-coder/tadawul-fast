#!/usr/bin/env python3
# scripts/track_performance.py
"""
track_performance.py
===========================================================
TADAWUL FAST BRIDGE – PERFORMANCE BENCHMARKER (v2.0.0)
===========================================================
PROD SAFE — ADVANCED ROI PROOF + WIN-RATE DIAGNOSTICS

What it does
- Records BUY / STRONG BUY (or your chosen signals) from a source sheet (default: Market_Scan)
  into a dedicated Performance_Log tab (Row 5 headers, Row 6 data).
- Audits existing log entries:
  - Updates Current Price + Unrealized ROI %
  - Automatically marks entries as MATURED when Target Date is reached
  - Computes Realized ROI % and Win/Loss
- Writes a compact summary block at the top of Performance_Log (A1:H3).

Key upgrades vs v1.0.0
- Header-driven parsing (no fragile column indexes)
- Multi-horizon tracking (1W / 1M / 3M / 12M) with per-horizon target dates
- Idempotent recording (prevents duplicates by unique key)
- Engine-first quotes fetch with HTTP fallback to /v1/enriched/quotes
- Deployment-safe behavior (optional STRICT mode)
- Riyadh time everywhere

Usage
  python scripts/track_performance.py --record
  python scripts/track_performance.py --audit
  python scripts/track_performance.py --record --source-tab Market_Scan --horizons 1M 3M
  python scripts/track_performance.py --audit --refresh 0
  STRICT=1 python scripts/track_performance.py --record   (fail if deps missing)

Notes
- Expects your project wrapper google_sheets_service.py to expose read_range/write_range/append_rows
  (falls back to direct Sheets API if get_sheets_service() exists).
- Quotes: prefers core.data_engine_v2.get_engine(). If missing, calls backend:
    GET {BACKEND_BASE_URL}/v1/enriched/quotes?tickers=AAA&tickers=BBB
  using X-APP-TOKEN if APP_TOKEN is set.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import os
import sys
import time
import urllib.parse
import urllib.request
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Iterable, List, Optional, Tuple


# =============================================================================
# Path & Dependency Setup
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

LOG_FORMAT = "%(asctime)s | %(levelname)s | %(message)s"
logging.basicConfig(level=logging.INFO, format=LOG_FORMAT, datefmt="%H:%M:%S")
logger = logging.getLogger("PerformanceTrack")

SCRIPT_VERSION = "2.0.0"

STRICT = str(os.getenv("STRICT") or "").strip().lower() in {"1", "true", "yes", "y", "on"}

# Deferred Imports (prod safe)
settings = None
sheets = None
get_engine = None

try:
    from env import settings as _settings  # type: ignore
    settings = _settings
except Exception:
    settings = None

try:
    import google_sheets_service as _sheets  # type: ignore
    sheets = _sheets
except Exception as e:
    sheets = None
    if STRICT:
        logger.error("Critical: google_sheets_service import failed: %s", e)
        raise SystemExit(1)

try:
    from core.data_engine_v2 import get_engine as _get_engine  # type: ignore
    get_engine = _get_engine
except Exception:
    get_engine = None
    # Not fatal; we have HTTP fallback


# =============================================================================
# Constants & Schema
# =============================================================================
PERF_SHEET_NAME = "Performance_Log"

# Headers written on Row 5; data begins Row 6
PERF_HEADERS: List[str] = [
    "Key",
    "Date Recorded (Riyadh)",
    "Symbol",
    "Origin Tab",
    "Recommendation",
    "Horizon",
    "Target Days",
    "Target Date (Riyadh)",
    "Entry Price",
    "Expected ROI % (Horizon)",
    "Forecast Price (Horizon)",
    "Advisor Score",
    "Risk Bucket",
    "Confidence Bucket",
    "Status",
    "Current Price",
    "Unrealized ROI %",
    "Realized ROI %",
    "Accuracy",
    "Last Updated (Riyadh)",
    "Notes",
]

DEFAULT_SOURCE_TAB = "Market_Scan"
DEFAULT_SOURCE_RANGE_DATA = "A6:ZZ500"
DEFAULT_SOURCE_RANGE_HEADERS = "A5:ZZ5"

# Which recommendations to track
DEFAULT_TRACK_RECOS = {"BUY", "STRONG BUY", "STRONGBUY"}

# Horizon mapping (days)
HORIZON_DAYS = {
    "1W": 7,
    "1M": 30,
    "3M": 90,
    "12M": 365,
}

# Common column name variants we might see in source sheets
SOURCE_COL_ALIASES = {
    "symbol": {"symbol", "ticker"},
    "recommendation": {"recommendation", "action"},
    "price": {"price", "current price", "entry price"},
    "advisor_score": {"advisor score", "overall score", "opportunity score"},
    "risk_bucket": {"risk bucket", "risk"},
    "confidence_bucket": {"confidence bucket", "confidence"},
    # ROI / forecast by horizon
    "roi_1m": {"expected roi % (1m)", "expected roi 1m", "expected_return_1m", "expected roi 1m %"},
    "roi_3m": {"expected roi % (3m)", "expected roi 3m", "expected_return_3m", "expected roi 3m %"},
    "roi_12m": {"expected roi % (12m)", "expected roi 12m", "expected_return_12m", "expected roi 12m %"},
    "roi_1w": {"expected roi % (1w)", "expected roi 1w", "expected_return_1w"},
    "fp_1m": {"forecast price (1m)", "forecast_price_1m", "target price (1m)", "expected_price_1m"},
    "fp_3m": {"forecast price (3m)", "forecast_price_3m", "target price (3m)", "expected_price_3m"},
    "fp_12m": {"forecast price (12m)", "forecast_price_12m", "target price (12m)", "expected_price_12m"},
    "fp_1w": {"forecast price (1w)", "forecast_price_1w", "target price (1w)", "expected_price_1w"},
}


# =============================================================================
# Time / Parsing helpers
# =============================================================================
def _riyadh_tz() -> timezone:
    return timezone(timedelta(hours=3))


def _riyadh_now() -> datetime:
    return datetime.now(_riyadh_tz())


def _riyadh_date_str(dt: Optional[datetime] = None) -> str:
    d = dt or _riyadh_now()
    return d.strftime("%Y-%m-%d")


def _riyadh_iso() -> str:
    return _riyadh_now().isoformat(timespec="seconds")


def _to_float(x: Any) -> float:
    try:
        if x is None:
            return 0.0
        s = str(x).strip()
        if not s:
            return 0.0
        s = s.replace(",", "").replace("%", "")
        return float(s)
    except Exception:
        return 0.0


def _pct_from_any(x: Any) -> float:
    """
    Accepts 0.12 or 12 or '12%' -> returns percent as 12.0
    """
    v = _to_float(x)
    if abs(v) <= 1.5 and v != 0.0:
        return v * 100.0
    return v


def _safe_str(x: Any) -> str:
    return str(x).strip() if x is not None else ""


def _norm_symbol(sym: str) -> str:
    s = _safe_str(sym).upper()
    if s.startswith("TADAWUL:"):
        s = s.split(":", 1)[1].strip().upper()
    if s.endswith(".TADAWUL"):
        s = s.replace(".TADAWUL", "")
    # Keep as-is; your normalizer exists server-side
    return s


def _parse_yyyy_mm_dd(s: str) -> Optional[datetime]:
    try:
        return datetime.strptime(s.strip(), "%Y-%m-%d")
    except Exception:
        return None


# =============================================================================
# Google Sheets wrappers (use project wrapper if present; fallback to API)
# =============================================================================
def _get_sheet_id_from_settings(cli: Optional[str]) -> str:
    if cli and cli.strip():
        return cli.strip()
    sid = ""
    if settings is not None:
        sid = str(getattr(settings, "default_spreadsheet_id", "") or "").strip()
    if not sid:
        sid = str(os.getenv("DEFAULT_SPREADSHEET_ID") or "").strip()
    return sid


def _sheets_service() -> Any:
    if sheets and hasattr(sheets, "get_sheets_service") and callable(getattr(sheets, "get_sheets_service")):
        return sheets.get_sheets_service()
    raise RuntimeError("google_sheets_service.get_sheets_service() not available")


def _read_range(spreadsheet_id: str, a1: str) -> List[List[Any]]:
    if sheets and hasattr(sheets, "read_range") and callable(getattr(sheets, "read_range")):
        return sheets.read_range(spreadsheet_id, a1)
    # Fallback direct API
    svc = _sheets_service()
    resp = svc.spreadsheets().values().get(spreadsheetId=spreadsheet_id, range=a1).execute()
    return resp.get("values") or []


def _write_range(spreadsheet_id: str, a1: str, values: List[List[Any]]) -> None:
    if sheets and hasattr(sheets, "write_range") and callable(getattr(sheets, "write_range")):
        sheets.write_range(spreadsheet_id, a1, values)
        return
    svc = _sheets_service()
    body = {"values": values}
    svc.spreadsheets().values().update(
        spreadsheetId=spreadsheet_id, range=a1, valueInputOption="RAW", body=body
    ).execute()


def _append_rows(spreadsheet_id: str, a1: str, rows: List[List[Any]]) -> None:
    if sheets and hasattr(sheets, "append_rows") and callable(getattr(sheets, "append_rows")):
        sheets.append_rows(spreadsheet_id, a1, rows)
        return
    svc = _sheets_service()
    body = {"values": rows}
    svc.spreadsheets().values().append(
        spreadsheetId=spreadsheet_id,
        range=a1,
        valueInputOption="RAW",
        insertDataOption="INSERT_ROWS",
        body=body,
    ).execute()


def _safe_a1(sheet_name: str, a1: str) -> str:
    name = (sheet_name or "").replace("'", "''")
    return f"'{name}'!{a1}"


def _ensure_perf_sheet_ready(spreadsheet_id: str) -> None:
    """
    Ensures Performance_Log exists and headers exist in Row 5.
    Writes a small summary block area at top as well (A1:H3).
    """
    if sheets is None:
        # We can still try API if available
        try:
            svc = _sheets_service()
        except Exception:
            if STRICT:
                raise
            logger.warning("Sheets service not available; skipping sheet init.")
            return
    else:
        svc = None
        try:
            svc = _sheets_service()
        except Exception:
            svc = None

    # Create sheet if missing (best-effort)
    try:
        if svc is not None:
            meta = svc.spreadsheets().get(spreadsheetId=spreadsheet_id).execute()
            titles = [s["properties"]["title"] for s in (meta.get("sheets") or [])]
            if PERF_SHEET_NAME not in titles:
                req = {"requests": [{"addSheet": {"properties": {"title": PERF_SHEET_NAME}}}]}
                svc.spreadsheets().batchUpdate(spreadsheetId=spreadsheet_id, body=req).execute()
                logger.info("Created missing tab: %s", PERF_SHEET_NAME)
    except Exception as e:
        # Not fatal; may already exist or permissions limited
        logger.warning("Sheet create check warning: %s", e)

    # Ensure headers in row 5
    header_a1 = _safe_a1(PERF_SHEET_NAME, f"A5:{_col_letter(len(PERF_HEADERS))}5")
    try:
        existing = _read_range(spreadsheet_id, header_a1)
        has_any = bool(existing and any(str(c).strip() for c in (existing[0] if existing else [])))
        if not has_any:
            _write_range(spreadsheet_id, header_a1, [PERF_HEADERS])
            logger.info("Initialized headers in %s row 5.", PERF_SHEET_NAME)
    except Exception as e:
        if STRICT:
            raise
        logger.warning("Header init warning: %s", e)

    # Summary block (A1:H3) — best effort
    try:
        summary = [
            [f"TFB Performance Benchmark (v{SCRIPT_VERSION})", "", "", "", "Last Run (Riyadh)", _riyadh_iso(), "", ""],
            ["Active", "", "Matured", "", "Win Rate (Matured)", "", "Avg ROI (Matured)", ""],
            ["", "", "", "", "", "", "", ""],
        ]
        _write_range(spreadsheet_id, _safe_a1(PERF_SHEET_NAME, "A1:H3"), summary)
    except Exception:
        pass


def _col_letter(n: int) -> str:
    """
    1 -> A, 26 -> Z, 27 -> AA
    """
    n = int(n)
    if n <= 0:
        return "A"
    out = []
    while n > 0:
        n, r = divmod(n - 1, 26)
        out.append(chr(ord("A") + r))
    return "".join(reversed(out))


# =============================================================================
# Source Parsing (header-driven)
# =============================================================================
def _lower_clean(s: str) -> str:
    return " ".join(str(s or "").strip().lower().split())


def _build_header_map(headers_row: List[Any]) -> Dict[str, int]:
    hm: Dict[str, int] = {}
    for i, h in enumerate(headers_row or []):
        key = _lower_clean(h)
        if key and key not in hm:
            hm[key] = i
    return hm


def _find_col(hm: Dict[str, int], aliases: Iterable[str]) -> Optional[int]:
    for a in aliases:
        k = _lower_clean(a)
        if k in hm:
            return hm[k]
    return None


def _extract_from_row(row: List[Any], idx: Optional[int]) -> Any:
    if idx is None:
        return None
    if idx < 0 or idx >= len(row):
        return None
    return row[idx]


def _roi_key_for_horizon(h: str) -> str:
    h = h.upper()
    if h == "1W":
        return "roi_1w"
    if h == "1M":
        return "roi_1m"
    if h == "3M":
        return "roi_3m"
    if h == "12M":
        return "roi_12m"
    return "roi_1m"


def _fp_key_for_horizon(h: str) -> str:
    h = h.upper()
    if h == "1W":
        return "fp_1w"
    if h == "1M":
        return "fp_1m"
    if h == "3M":
        return "fp_3m"
    if h == "12M":
        return "fp_12m"
    return "fp_1m"


@dataclass(frozen=True)
class Candidate:
    symbol: str
    reco: str
    entry_price: float
    origin_tab: str
    advisor_score: float
    risk_bucket: str
    confidence_bucket: str
    roi_by_h: Dict[str, float]
    fp_by_h: Dict[str, float]


def _read_candidates_from_source(spreadsheet_id: str, tab: str, limit: int = 200) -> List[Candidate]:
    """
    Reads headers (row 5) + data (row 6..), and extracts candidates.
    """
    headers = _read_range(spreadsheet_id, _safe_a1(tab, DEFAULT_SOURCE_RANGE_HEADERS))
    if not headers or not headers[0]:
        logger.warning("Source tab '%s': headers not found at row 5.", tab)
        return []

    hm = _build_header_map(headers[0])

    i_symbol = _find_col(hm, SOURCE_COL_ALIASES["symbol"])
    i_reco = _find_col(hm, SOURCE_COL_ALIASES["recommendation"])
    i_price = _find_col(hm, SOURCE_COL_ALIASES["price"])
    i_score = _find_col(hm, SOURCE_COL_ALIASES["advisor_score"])
    i_risk = _find_col(hm, SOURCE_COL_ALIASES["risk_bucket"])
    i_conf = _find_col(hm, SOURCE_COL_ALIASES["confidence_bucket"])

    roi_cols = {
        "1W": _find_col(hm, SOURCE_COL_ALIASES["roi_1w"]),
        "1M": _find_col(hm, SOURCE_COL_ALIASES["roi_1m"]),
        "3M": _find_col(hm, SOURCE_COL_ALIASES["roi_3m"]),
        "12M": _find_col(hm, SOURCE_COL_ALIASES["roi_12m"]),
    }
    fp_cols = {
        "1W": _find_col(hm, SOURCE_COL_ALIASES["fp_1w"]),
        "1M": _find_col(hm, SOURCE_COL_ALIASES["fp_1m"]),
        "3M": _find_col(hm, SOURCE_COL_ALIASES["fp_3m"]),
        "12M": _find_col(hm, SOURCE_COL_ALIASES["fp_12m"]),
    }

    data = _read_range(spreadsheet_id, _safe_a1(tab, DEFAULT_SOURCE_RANGE_DATA))
    if not data:
        return []

    out: List[Candidate] = []
    for row in data[: max(1, int(limit))]:
        sym = _norm_symbol(_safe_str(_extract_from_row(row, i_symbol)))
        if not sym or sym == "SYMBOL":
            continue
        reco = _safe_str(_extract_from_row(row, i_reco)).strip().upper()
        entry_price = _to_float(_extract_from_row(row, i_price))
        score = _to_float(_extract_from_row(row, i_score))
        risk = _safe_str(_extract_from_row(row, i_risk))
        conf = _safe_str(_extract_from_row(row, i_conf))

        roi_by_h: Dict[str, float] = {}
        fp_by_h: Dict[str, float] = {}
        for h in ["1W", "1M", "3M", "12M"]:
            roi_by_h[h] = _pct_from_any(_extract_from_row(row, roi_cols.get(h)))
            fp_by_h[h] = _to_float(_extract_from_row(row, fp_cols.get(h)))

        out.append(
            Candidate(
                symbol=sym,
                reco=reco,
                entry_price=entry_price,
                origin_tab=tab,
                advisor_score=score,
                risk_bucket=risk,
                confidence_bucket=conf,
                roi_by_h=roi_by_h,
                fp_by_h=fp_by_h,
            )
        )
    return out


# =============================================================================
# Quotes fetching (Engine first; HTTP fallback)
# =============================================================================
async def _fetch_quotes_engine(symbols: List[str], refresh: bool) -> Dict[str, Dict[str, Any]]:
    if get_engine is None:
        return {}

    try:
        eng = await get_engine()
    except Exception as e:
        logger.warning("Engine init failed; fallback to HTTP. (%s)", e)
        return {}

    if not eng:
        return {}

    # Prefer batch
    try:
        if hasattr(eng, "get_enriched_quotes"):
            res = await eng.get_enriched_quotes(symbols, refresh=refresh)
            out: Dict[str, Dict[str, Any]] = {}
            if isinstance(res, list):
                for s, item in zip(symbols, res):
                    if isinstance(item, tuple):
                        item = item[0]
                    out[_norm_symbol(s)] = item if isinstance(item, dict) else {}
            elif isinstance(res, dict):
                for k, v in res.items():
                    out[_norm_symbol(k)] = v if isinstance(v, dict) else {}
            return out
    except Exception as e:
        logger.warning("Engine batch quotes failed; fallback HTTP. (%s)", e)

    # Fallback single
    out2: Dict[str, Dict[str, Any]] = {}
    for s in symbols:
        try:
            if hasattr(eng, "get_enriched_quote"):
                item = await eng.get_enriched_quote(s, refresh=refresh)
            elif hasattr(eng, "get_quote"):
                item = await eng.get_quote(s, refresh=refresh)
            else:
                item = None
            if isinstance(item, tuple):
                item = item[0]
            out2[_norm_symbol(s)] = item if isinstance(item, dict) else {}
        except Exception:
            out2[_norm_symbol(s)] = {}
    return out2


def _fetch_quotes_http(symbols: List[str], timeout: int = 40) -> Dict[str, Dict[str, Any]]:
    base = (os.getenv("BACKEND_BASE_URL") or "http://127.0.0.1:8000").rstrip("/")
    token = (os.getenv("APP_TOKEN") or "").strip()

    # GET /v1/enriched/quotes?tickers=A&tickers=B...
    qs = urllib.parse.urlencode([("tickers", s) for s in symbols], doseq=True)
    url = f"{base}/v1/enriched/quotes?{qs}"

    headers = {
        "User-Agent": f"TFB-PerformanceTrack/{SCRIPT_VERSION}",
    }
    if token:
        headers["X-APP-TOKEN"] = token

    try:
        req = urllib.request.Request(url, headers=headers, method="GET")
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            raw = resp.read().decode("utf-8")
        parsed = json.loads(raw)
        items = parsed.get("items") or parsed.get("results") or []
        out: Dict[str, Dict[str, Any]] = {}
        if isinstance(items, list):
            for it in items:
                if isinstance(it, dict):
                    sym = _norm_symbol(it.get("symbol") or it.get("ticker") or "")
                    if sym:
                        out[sym] = it
        return out
    except Exception as e:
        logger.error("HTTP quote fetch failed: %s", e)
        return {}


async def _quotes_map(symbols: List[str], refresh: bool) -> Dict[str, Dict[str, Any]]:
    symbols = [_norm_symbol(s) for s in symbols if _norm_symbol(s)]
    if not symbols:
        return {}
    # Try engine first
    m = await _fetch_quotes_engine(symbols, refresh=refresh)
    if m:
        return m
    # HTTP fallback
    return _fetch_quotes_http(symbols)


def _quote_price(q: Dict[str, Any]) -> float:
    return _to_float(q.get("current_price") or q.get("price") or q.get("last") or q.get("close"))


# =============================================================================
# Performance Log Reading / Writing
# =============================================================================
def _read_existing_keys(spreadsheet_id: str, max_rows: int = 2000) -> Tuple[List[List[Any]], Dict[str, int]]:
    """
    Reads existing Performance_Log rows and returns:
      - rows (A6:... up to max_rows)
      - key_index map (key -> row_offset_index within the returned rows)
    """
    # Columns length = PERF_HEADERS
    rng = _safe_a1(PERF_SHEET_NAME, f"A6:{_col_letter(len(PERF_HEADERS))}{5 + max_rows}")
    rows = _read_range(spreadsheet_id, rng) or []
    km: Dict[str, int] = {}
    for idx, r in enumerate(rows):
        k = _safe_str(r[0]) if len(r) > 0 else ""
        if k and k not in km:
            km[k] = idx
    return rows, km


def _build_log_key(symbol: str, horizon: str, date_recorded: str) -> str:
    # Stable, short-ish key
    return f"{_norm_symbol(symbol)}|{horizon.upper()}|{date_recorded}"


def _make_log_row(c: Candidate, horizon: str, date_recorded: str) -> List[Any]:
    h = horizon.upper()
    days = HORIZON_DAYS.get(h, 30)
    target_date = (_riyadh_now() + timedelta(days=days)).strftime("%Y-%m-%d")

    exp_roi = c.roi_by_h.get(h, 0.0)
    fp = c.fp_by_h.get(h, 0.0)

    key = _build_log_key(c.symbol, h, date_recorded)

    return [
        key,
        date_recorded,
        c.symbol,
        c.origin_tab,
        c.reco,
        h,
        days,
        target_date,
        c.entry_price,
        round(exp_roi, 4),
        fp,
        c.advisor_score,
        c.risk_bucket,
        c.confidence_bucket,
        "ACTIVE",
        c.entry_price,  # current price initial = entry
        0.0,            # unrealized
        "",             # realized
        "PENDING",
        _riyadh_iso(),
        "",
    ]


def _pad_row(row: List[Any], width: int) -> List[Any]:
    r = list(row)
    if len(r) < width:
        r.extend([""] * (width - len(r)))
    elif len(r) > width:
        r = r[:width]
    return r


# =============================================================================
# Summary Writing
# =============================================================================
def _write_summary_block(
    spreadsheet_id: str,
    *,
    active: int,
    matured: int,
    wins: int,
    losses: int,
    avg_roi_matured: float,
) -> None:
    wr = (wins / (wins + losses)) * 100.0 if (wins + losses) > 0 else 0.0
    block = [
        [f"TFB Performance Benchmark (v{SCRIPT_VERSION})", "", "", "", "Last Run (Riyadh)", _riyadh_iso(), "", ""],
        ["Active", active, "Matured", matured, "Win Rate (Matured)", f"{wr:.1f}%", "Avg ROI (Matured)", f"{avg_roi_matured:.2f}%"],
        ["Wins", wins, "Losses", losses, "", "", "", ""],
    ]
    try:
        _write_range(spreadsheet_id, _safe_a1(PERF_SHEET_NAME, "A1:H3"), block)
    except Exception:
        pass


# =============================================================================
# Commands
# =============================================================================
async def record_recommendations(
    spreadsheet_id: str,
    *,
    source_tabs: List[str],
    horizons: List[str],
    track_recos: set,
    limit: int,
) -> None:
    logger.info("Recording recommendations | tabs=%s | horizons=%s", ",".join(source_tabs), ",".join(horizons))

    _ensure_perf_sheet_ready(spreadsheet_id)

    # Existing keys for idempotency
    _, existing_keys = _read_existing_keys(spreadsheet_id, max_rows=4000)

    date_rec = _riyadh_date_str()

    # Gather candidates across tabs
    candidates: List[Candidate] = []
    for tab in source_tabs:
        try:
            cs = _read_candidates_from_source(spreadsheet_id, tab, limit=limit)
            candidates.extend(cs)
        except Exception as e:
            logger.warning("Source read failed for '%s': %s", tab, e)

    if not candidates:
        logger.warning("No candidates found in source tabs.")
        return

    # Filter: reco + symbol
    keep: List[Candidate] = []
    for c in candidates:
        r = c.reco.replace("_", " ").strip().upper()
        if r in track_recos:
            keep.append(c)

    if not keep:
        logger.info("No matching recommendations found (%s).", ",".join(sorted(track_recos)))
        return

    to_append: List[List[Any]] = []
    for c in keep:
        for h in horizons:
            hh = h.upper()
            if hh not in HORIZON_DAYS:
                continue
            key = _build_log_key(c.symbol, hh, date_rec)
            if key in existing_keys:
                continue
            to_append.append(_make_log_row(c, hh, date_rec))

    if not to_append:
        logger.info("Nothing new to record (all keys already exist).")
        return

    logger.info("Appending %d new tracking rows to %s...", len(to_append), PERF_SHEET_NAME)
    # Append starting A6; wrapper should find end, else API append will do
    _append_rows(spreadsheet_id, _safe_a1(PERF_SHEET_NAME, "A6"), to_append)
    logger.info("✅ Record complete.")


async def audit_existing_logs(
    spreadsheet_id: str,
    *,
    refresh: bool,
    max_rows: int,
) -> None:
    logger.info("Auditing performance logs... (refresh=%s)", refresh)

    _ensure_perf_sheet_ready(spreadsheet_id)

    rows, _ = _read_existing_keys(spreadsheet_id, max_rows=max_rows)
    if not rows:
        logger.info("Performance_Log is empty.")
        _write_summary_block(spreadsheet_id, active=0, matured=0, wins=0, losses=0, avg_roi_matured=0.0)
        return

    width = len(PERF_HEADERS)
    rows = [_pad_row(r, width) for r in rows]

    # Collect active symbols
    symbols: List[str] = []
    for r in rows:
        sym = _norm_symbol(_safe_str(r[2]))
        status = _safe_str(r[14]).upper()
        if sym and status in {"ACTIVE", "MATURED"}:
            symbols.append(sym)
    symbols = list(dict.fromkeys(symbols))
    if not symbols:
        logger.info("No symbols to audit.")
        _write_summary_block(spreadsheet_id, active=0, matured=0, wins=0, losses=0, avg_roi_matured=0.0)
        return

    # Fetch current quotes
    logger.info("Fetching current prices for %d symbols...", len(symbols))
    qmap = await _quotes_map(symbols, refresh=refresh)

    now_riyadh = _riyadh_now()
    wins = 0
    losses = 0
    matured = 0
    active = 0
    matured_roi_sum = 0.0
    matured_roi_count = 0

    # Update rows in memory
    for r in rows:
        sym = _norm_symbol(_safe_str(r[2]))
        if not sym:
            continue

        status = _safe_str(r[14]).upper() or "ACTIVE"
        horizon = _safe_str(r[5]).upper()
        entry_price = _to_float(r[8])
        target_date_s = _safe_str(r[7])
        target_dt = _parse_yyyy_mm_dd(target_date_s)

        q = qmap.get(sym) or {}
        current_price = _quote_price(q)

        # Always update current/unrealized if possible
        if current_price > 0 and entry_price > 0:
            unreal = ((current_price / entry_price) - 1.0) * 100.0
            r[15] = round(current_price, 6)
            r[16] = round(unreal, 4)
        r[19] = _riyadh_iso()

        # Maturity check
        is_mature = False
        if target_dt is not None:
            # Compare in local time at midnight -> treat "target date" as day boundary
            is_mature = now_riyadh.date() >= target_dt.date()

        # If already matured keep matured; if target reached => mature it
        if status == "ACTIVE":
            if is_mature:
                r[14] = "MATURED"
                status = "MATURED"
            else:
                active += 1

        if status == "MATURED":
            matured += 1
            # Compute realized ROI if possible
            if current_price > 0 and entry_price > 0:
                realized = ((current_price / entry_price) - 1.0) * 100.0
                r[17] = round(realized, 4)
                # Accuracy
                if realized > 0:
                    r[18] = "WIN ✅"
                    wins += 1
                else:
                    r[18] = "LOSS ❌"
                    losses += 1
                matured_roi_sum += float(realized)
                matured_roi_count += 1
            else:
                # keep pending if cannot price
                if not _safe_str(r[18]):
                    r[18] = "PENDING"

    avg_roi_matured = (matured_roi_sum / matured_roi_count) if matured_roi_count else 0.0

    # Write back
    rng = _safe_a1(PERF_SHEET_NAME, f"A6:{_col_letter(width)}{5 + max_rows}")
    try:
        _write_range(spreadsheet_id, rng, rows)
        logger.info("✅ Audit write-back complete.")
    except Exception as e:
        logger.error("❌ Failed writing audit updates: %s", e)
        if STRICT:
            raise

    _write_summary_block(
        spreadsheet_id,
        active=active,
        matured=matured,
        wins=wins,
        losses=losses,
        avg_roi_matured=avg_roi_matured,
    )

    wr = (wins / (wins + losses)) * 100.0 if (wins + losses) > 0 else 0.0
    logger.info("Win Rate (matured): %.1f%% | avg ROI: %.2f%% | active=%d | matured=%d", wr, avg_roi_matured, active, matured)


# =============================================================================
# Main
# =============================================================================
def main() -> int:
    ap = argparse.ArgumentParser(description="TFB Performance Benchmarker (Advanced)")
    ap.add_argument("--record", action="store_true", help="Record current signals into Performance_Log")
    ap.add_argument("--audit", action="store_true", help="Update existing logs with current prices + realized ROI")
    ap.add_argument("--sheet-id", help="Override Spreadsheet ID")
    ap.add_argument("--source-tab", action="append", help="Source tab name (repeatable). Default: Market_Scan")
    ap.add_argument("--horizons", nargs="*", default=["1M"], help="Horizons to track: 1W 1M 3M 12M")
    ap.add_argument("--track-recos", nargs="*", default=None, help="Recommendations to track (e.g. BUY STRONG_BUY)")
    ap.add_argument("--limit", type=int, default=200, help="Max rows to read from each source tab")
    ap.add_argument("--max-log-rows", type=int, default=2000, help="Max Performance_Log rows to audit/write back")
    ap.add_argument("--refresh", type=int, default=1, help="Refresh quotes (1) or use cached (0)")
    args = ap.parse_args()

    if not args.record and not args.audit:
        logger.info("No action specified. Use --record and/or --audit.")
        return 0

    sid = _get_sheet_id_from_settings(args.sheet_id)
    if not sid:
        logger.error("No Spreadsheet ID found. Use --sheet-id or set DEFAULT_SPREADSHEET_ID.")
        return 1

    # Track recos
    track_recos = set(DEFAULT_TRACK_RECOS)
    if args.track_recos:
        track_recos = {str(x).replace("_", " ").strip().upper() for x in args.track_recos if str(x).strip()}

    # Horizons
    horizons = [str(h).strip().upper() for h in (args.horizons or []) if str(h).strip()]
    horizons = [h for h in horizons if h in HORIZON_DAYS]
    if not horizons:
        horizons = ["1M"]

    source_tabs = args.source_tab or [DEFAULT_SOURCE_TAB]

    refresh = bool(int(args.refresh or 0))

    # If sheets wrapper is missing and strict is off: skip safely
    if sheets is None and not STRICT:
        logger.warning("google_sheets_service not available; skipping to avoid breaking CI/CD.")
        return 0

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        if args.record:
            loop.run_until_complete(
                record_recommendations(
                    sid,
                    source_tabs=source_tabs,
                    horizons=horizons,
                    track_recos=track_recos,
                    limit=max(10, int(args.limit)),
                )
            )
        if args.audit:
            loop.run_until_complete(
                audit_existing_logs(
                    sid,
                    refresh=refresh,
                    max_rows=max(50, int(args.max_log_rows)),
                )
            )
    finally:
        loop.close()

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
