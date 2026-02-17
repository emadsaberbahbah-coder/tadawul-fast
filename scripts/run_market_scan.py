#!/usr/bin/env python3
# scripts/run_market_scan.py
"""
run_market_scan.py
===========================================================
TADAWUL FAST BRIDGE – AI MARKET SCANNER (v2.4.0)
===========================================================
PRODUCTION-HARDENED + SHEETS-SAFE + BACKEND-SAFE

What this script does
1) Aggregates symbols across selected dashboards (KSA/Global/Leaders/Portfolio etc.)
2) Calls backend batch analysis endpoint (/v1/analysis/quotes) in resilient chunks
3) Normalizes + scores + ranks using a stable composite ranking (Opportunity/ROI/Risk/Quality)
4) Writes a "Top Opportunities" grid to a destination tab (default: Market_Scan)
5) Produces a JSON report (optional) for auditability

Key upgrades vs v1.6.1
- ✅ Preflight: backend health + route checks + token-mode awareness
- ✅ Resilient chunking: configurable chunk size + retries + backoff
- ✅ Strong normalization: de-dupe, strip placeholders, cap symbol count, preserve origin
- ✅ Safety rule: never write if headers are empty or rows are zero unexpectedly (prevents blank dashboards)
- ✅ Ranking v2: deterministic composite score with tie-breakers + missing-safe defaults
- ✅ Output QA: pads/truncates rows to header width; inserts Riyadh timestamps
- ✅ Optional modes: --no-sheet, --dry-run, --keys, --top, --max-symbols
- ✅ Cleaner exit codes: 0 success, 1 fatal config/preflight, 2 partial/failure

Exit Codes
- 0: Success
- 1: Fatal (config/preflight)
- 2: Partial/Failure (backend returned no results, sheet write failed, etc.)

Usage
- python scripts/run_market_scan.py
- python scripts/run_market_scan.py --keys MARKET_LEADERS GLOBAL_MARKETS
- python scripts/run_market_scan.py --top 75 --sheet-name Market_Scan
- python scripts/run_market_scan.py --no-sheet
- python scripts/run_market_scan.py --json-out market_scan_report.json
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import re
import sys
import time
import urllib.error
import urllib.request
from dataclasses import dataclass
from datetime import datetime, timezone, timedelta
from typing import Any, Dict, List, Optional, Sequence, Tuple

# =============================================================================
# Version & Logging
# =============================================================================
SCRIPT_VERSION = "2.4.0"
LOG_FORMAT = "%(asctime)s | %(levelname)s | %(message)s"
DATE_FORMAT = "%H:%M:%S"

logging.basicConfig(level=logging.INFO, format=LOG_FORMAT, datefmt=DATE_FORMAT)
logger = logging.getLogger("MarketScan")

# =============================================================================
# Path & Dependency Alignment
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

# Deferred Imports for Resilience (do NOT break CI/CD)
try:
    from env import settings  # type: ignore
    import symbols_reader  # type: ignore
    import google_sheets_service as sheets_service  # type: ignore
except ImportError as e:
    logger.warning("Optional dependencies missing: %s", e)
    logger.warning("Market scan skipped (environment not ready).")
    sys.exit(0)

# =============================================================================
# Constants / Schema
# =============================================================================
_TRUTHY = {"1", "true", "yes", "y", "on", "t"}

DEFAULT_SCAN_KEYS = ["KSA_TADAWUL", "GLOBAL_MARKETS", "MARKET_LEADERS"]

# Destination grid starts at A5; row 5 = first row for headers
DEFAULT_START_CELL = "A5"

SCAN_HEADERS: List[str] = [
    "Rank",
    "Symbol",
    "Origin",
    "Name",
    "Market",
    "Currency",
    "Price",
    "Change %",
    "Opportunity Score",
    "Overall Score",
    "Recommendation",
    "Rec Badge",
    "Reasoning",
    "Fair Value",
    "Upside %",
    "Expected ROI % (1M)",
    "Expected ROI % (3M)",
    "Expected ROI % (12M)",
    "Forecast Confidence",
    "RSI 14",
    "MA200",
    "Volatility (30D)",
    "Risk Score",
    "Quality Score",
    "Momentum Score",
    "Data Quality",
    "Last Updated (UTC)",
    "Last Updated (Riyadh)",
    "Error",
]

_SYMBOL_BAD = {"", "SYMBOL", "TICKER", "N/A", "NA", "NONE", "NULL"}
_A1_CELL_RE = re.compile(r"^\$?[A-Za-z]+\$?\d+$")

# =============================================================================
# Time Helpers
# =============================================================================
def _now_utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat()

def _to_riyadh_iso(utc_iso: Optional[str]) -> str:
    if not utc_iso:
        return ""
    try:
        dt = datetime.fromisoformat(str(utc_iso).replace("Z", "+00:00"))
        tz = timezone(timedelta(hours=3))
        return dt.astimezone(tz).isoformat()
    except Exception:
        return ""

# =============================================================================
# Config Helpers
# =============================================================================
def _truthy(v: Any) -> bool:
    return str(v or "").strip().lower() in _TRUTHY

def _safe_int(x: Any, default: int) -> int:
    try:
        return int(str(x).strip())
    except Exception:
        return default

def _safe_float(x: Any, default: float) -> float:
    try:
        return float(str(x).strip())
    except Exception:
        return default

def _get_spreadsheet_id(cli_id: Optional[str]) -> str:
    if cli_id:
        return cli_id.strip()
    sid = (getattr(settings, "default_spreadsheet_id", None) or "").strip()
    if sid:
        return sid
    return (os.getenv("DEFAULT_SPREADSHEET_ID") or "").strip()

def _backend_base_url() -> str:
    env_url = (os.getenv("BACKEND_BASE_URL") or "").strip()
    if env_url:
        return env_url.rstrip("/")
    s_url = (getattr(settings, "backend_base_url", None) or "").strip()
    if s_url:
        return s_url.rstrip("/")
    return "http://127.0.0.1:8000"

def _canon_key(user_key: str) -> str:
    k = str(user_key or "").strip().upper().replace("-", "_").replace(" ", "_")
    aliases = {
        "KSA": "KSA_TADAWUL",
        "TADAWUL": "KSA_TADAWUL",
        "GLOBAL": "GLOBAL_MARKETS",
        "LEADERS": "MARKET_LEADERS",
        "PORTFOLIO": "MY_PORTFOLIO",
        "INSIGHTS": "INSIGHTS_ANALYSIS",
    }
    return aliases.get(k, k)

def _validate_a1_cell(a1: str) -> str:
    s = (a1 or "").strip()
    if not s:
        return DEFAULT_START_CELL
    if not _A1_CELL_RE.match(s):
        raise ValueError(f"Invalid A1 cell: {a1}")
    return s

def _read_token() -> str:
    # Open mode if empty; backend should accept when no token configured
    return (os.getenv("APP_TOKEN") or "").strip()

# =============================================================================
# HTTP Helpers (urllib only)
# =============================================================================
def _http_get_json(url: str, timeout_sec: float = 10.0) -> Tuple[Optional[Dict[str, Any]], Optional[str], int]:
    try:
        req = urllib.request.Request(
            url,
            method="GET",
            headers={"User-Agent": f"TFB-MarketScan/{SCRIPT_VERSION}"},
        )
        with urllib.request.urlopen(req, timeout=timeout_sec) as resp:
            code = int(resp.getcode() or 0)
            raw = resp.read()
            if not raw:
                return None, "Empty response body", code
            try:
                data = json.loads(raw.decode("utf-8", errors="replace"))
                return data, None, code
            except Exception:
                return None, "Non-JSON response", code
    except urllib.error.HTTPError as e:
        return None, f"HTTPError {getattr(e, 'code', 0)}", int(getattr(e, "code", 0) or 0)
    except Exception as e:
        return None, str(e), 0

def _http_post_json(
    url: str,
    payload: Dict[str, Any],
    *,
    headers: Dict[str, str],
    timeout_sec: float,
) -> Tuple[Optional[Dict[str, Any]], Optional[str], int]:
    try:
        data = json.dumps(payload).encode("utf-8")
        req = urllib.request.Request(url, data=data, headers=headers, method="POST")
        with urllib.request.urlopen(req, timeout=timeout_sec) as resp:
            code = int(resp.getcode() or 0)
            raw = resp.read()
            if not raw:
                return None, "Empty response body", code
            try:
                parsed = json.loads(raw.decode("utf-8", errors="replace"))
                return parsed, None, code
            except Exception:
                return None, "Non-JSON response", code
    except urllib.error.HTTPError as e:
        code = int(getattr(e, "code", 0) or 0)
        try:
            raw = e.read().decode("utf-8", errors="replace")  # type: ignore[attr-defined]
            return {"status": "error", "error": raw}, f"HTTPError {code}", code
        except Exception:
            return None, f"HTTPError {code}", code
    except Exception as e:
        return None, str(e), 0

# =============================================================================
# Preflight
# =============================================================================
def _preflight(backend_url: str, *, timeout_sec: float) -> bool:
    logger.info("--- Preflight ---")
    ok = False
    for path in ("/readyz", "/health", "/v1/analysis/health"):
        data, err, code = _http_get_json(f"{backend_url}{path}", timeout_sec=timeout_sec)
        if code == 200 and isinstance(data, dict):
            logger.info("✅ Backend OK (%s)", path)
            ok = True
            break
        if err:
            logger.debug("Preflight path %s error: %s", path, err)

    if not ok:
        logger.error("❌ Backend unreachable/unhealthy at %s", backend_url)
        return False

    # Sheets service basic init
    try:
        sheets_service.get_sheets_service()
        logger.info("✅ Google Sheets API: Connected")
    except Exception as e:
        logger.error("❌ Google Sheets API: Connection Failed: %s", e)
        return False

    return True

# =============================================================================
# Symbol Discovery
# =============================================================================
def _normalize_symbol(s: Any) -> Optional[str]:
    t = str(s or "").strip()
    if not t:
        return None
    u = t.upper()
    if u in _SYMBOL_BAD:
        return None
    if u.startswith("#"):
        return None
    return u

def _read_symbols_from_keys(
    sid: str,
    keys: Sequence[str],
    *,
    max_symbols: int = 0,
) -> Tuple[List[str], Dict[str, str], Dict[str, int]]:
    """
    Returns: (symbols_unique, origin_map, per_key_counts)
    origin_map stores FIRST key where symbol was seen.
    """
    all_syms: List[str] = []
    origin_map: Dict[str, str] = {}
    per_key_counts: Dict[str, int] = {}

    for raw_k in keys:
        k = _canon_key(raw_k)
        try:
            sym_data = symbols_reader.get_page_symbols(k, spreadsheet_id=sid)
            symbols = sym_data.get("all") if isinstance(sym_data, dict) else sym_data
            count = 0
            if symbols:
                for s in symbols:
                    u = _normalize_symbol(s)
                    if not u:
                        continue
                    all_syms.append(u)
                    if u not in origin_map:
                        origin_map[u] = k
                    count += 1
            per_key_counts[k] = count
        except Exception as e:
            logger.warning("Could not read symbols for %s: %s", k, e)
            per_key_counts[k] = 0

    # de-dupe preserve order
    uniq = list(dict.fromkeys(all_syms))

    if max_symbols > 0 and len(uniq) > max_symbols:
        uniq = uniq[:max_symbols]

    return uniq, origin_map, per_key_counts

# =============================================================================
# Backend Analysis (chunked)
# =============================================================================
def _sleep_backoff(attempt: int, base: float, cap: float) -> None:
    delay = min(cap, base * (2 ** max(0, attempt - 1)))
    delay = delay + (0.05 * attempt)
    time.sleep(delay)

def _call_analysis_batch_chunked(
    symbols: List[str],
    *,
    backend_url: str,
    timeout_sec: float,
    chunk_size: int,
    retries: int,
    backoff_base: float,
    backoff_cap: float,
    mode: str,
) -> List[Dict[str, Any]]:
    if not symbols:
        return []

    url = f"{backend_url}/v1/analysis/quotes"
    token = _read_token()

    headers = {
        "Content-Type": "application/json",
        "User-Agent": f"TFB-MarketScan/{SCRIPT_VERSION}",
    }
    if token:
        headers["X-APP-TOKEN"] = token

    out: List[Dict[str, Any]] = []
    chunks = [symbols[i : i + chunk_size] for i in range(0, len(symbols), chunk_size)]

    for ci, chunk in enumerate(chunks, start=1):
        payload = {
            "tickers": chunk,
            "symbols": chunk,  # resilient dual key
            "mode": mode,
        }

        ok = False
        last_err = None
        attempts = max(1, retries + 1)

        for a in range(1, attempts + 1):
            parsed, err, code = _http_post_json(url, payload, headers=headers, timeout_sec=timeout_sec)
            if code == 200 and isinstance(parsed, dict):
                items = parsed.get("results") or parsed.get("items") or []
                if isinstance(items, list):
                    out.extend([x for x in items if isinstance(x, dict)])
                    ok = True
                    break
                last_err = "Backend returned non-list results/items"
            else:
                last_err = err or f"HTTP {code}"

            if a < attempts:
                _sleep_backoff(a, base=backoff_base, cap=backoff_cap)

        if not ok:
            logger.warning("Chunk %d/%d failed (%d symbols): %s", ci, len(chunks), len(chunk), last_err or "unknown")

    return out

# =============================================================================
# Ranking
# =============================================================================
def _to_float(x: Any, default: float = 0.0) -> float:
    try:
        if x is None:
            return default
        if isinstance(x, (int, float)):
            return float(x)
        s = str(x).replace(",", "").strip()
        if not s:
            return default
        return float(s)
    except Exception:
        return default

def _pct_norm(v: Any) -> float:
    """
    Normalize ROI-like value:
    - 0.12 => 12.0
    - 12.0 => 12.0
    """
    f = _to_float(v, 0.0)
    if abs(f) <= 1.5:
        return f * 100.0
    return f

def _ranking_key(item: Dict[str, Any]) -> Tuple[float, float, float, float, float]:
    """
    Composite ranking:
    1) opportunity_score (desc)
    2) expected_roi_3m (desc)
    3) expected_roi_1m (desc)
    4) overall_score (desc)
    5) risk_score (asc)  -> implemented by negating risk so higher key means lower risk
    """
    opp = _to_float(item.get("opportunity_score"), 0.0)
    roi3 = _pct_norm(item.get("expected_roi_3m") or item.get("expected_return_3m"))
    roi1 = _pct_norm(item.get("expected_roi_1m") or item.get("expected_return_1m"))
    overall = _to_float(item.get("overall_score"), 0.0)
    risk = _to_float(item.get("risk_score"), 50.0)
    return (opp, roi3, roi1, overall, -risk)

# =============================================================================
# Row Mapping
# =============================================================================
def _g(item: Dict[str, Any], *keys: str) -> Any:
    for k in keys:
        if k in item and item[k] is not None:
            return item[k]
    return None

def _map_to_row(item: Dict[str, Any], rank: int, origin: str) -> List[Any]:
    utc = _g(item, "last_updated_utc") or _now_utc_iso()
    return [
        rank,
        _g(item, "symbol") or "",
        origin,
        _g(item, "name") or "",
        _g(item, "market", "market_region") or "",
        _g(item, "currency") or "",
        _g(item, "price", "current_price") or "",
        _pct_norm(_g(item, "change_pct", "percent_change")) if _g(item, "change_pct", "percent_change") is not None else "",
        _g(item, "opportunity_score") or "",
        _g(item, "overall_score") or "",
        _g(item, "recommendation") or "",
        _g(item, "rec_badge") or "",
        _g(item, "scoring_reason", "reason", "reasoning") or "",
        _g(item, "fair_value") or "",
        _pct_norm(_g(item, "upside_percent", "upside_pct")) if _g(item, "upside_percent", "upside_pct") is not None else "",
        _pct_norm(_g(item, "expected_roi_1m", "expected_return_1m")) if _g(item, "expected_roi_1m", "expected_return_1m") is not None else "",
        _pct_norm(_g(item, "expected_roi_3m", "expected_return_3m")) if _g(item, "expected_roi_3m", "expected_return_3m") is not None else "",
        _pct_norm(_g(item, "expected_roi_12m", "expected_return_12m")) if _g(item, "expected_roi_12m", "expected_return_12m") is not None else "",
        _g(item, "forecast_confidence") or "",
        _g(item, "rsi_14", "rsi14", "rsi (14)", "rsi_14d") or "",
        _g(item, "ma200", "ma_200") or "",
        _g(item, "volatility_30d", "volatility (30d)") or "",
        _g(item, "risk_score") or "",
        _g(item, "quality_score") or "",
        _g(item, "momentum_score") or "",
        _g(item, "data_quality") or "",
        utc,
        _to_riyadh_iso(str(utc)),
        _g(item, "error") or "",
    ]

def _pad_or_trim_row(row: List[Any], width: int) -> List[Any]:
    r = list(row)
    if len(r) < width:
        r.extend([""] * (width - len(r)))
    elif len(r) > width:
        r = r[:width]
    return r

# =============================================================================
# Sheet Writing (safe)
# =============================================================================
def _write_grid_safe(
    sid: str,
    sheet_name: str,
    start_cell: str,
    grid: List[List[Any]],
) -> int:
    """
    Expects grid includes header row as first row (we will write starting at start_cell).
    Returns a best-effort "cells updated" (depends on your sheets_service implementation).
    """
    if not grid or not isinstance(grid, list):
        raise ValueError("Empty grid")

    if not grid[0] or not isinstance(grid[0], list):
        raise ValueError("Grid missing header row")

    headers = grid[0]
    if isinstance(headers, list) and len(headers) == 0:
        raise ValueError("Unsafe: empty headers")

    # Ensure every row matches header width
    w = len(headers)
    fixed = [headers]
    for r in grid[1:]:
        fixed.append(_pad_or_trim_row(r if isinstance(r, list) else [], w))

    # Prefer chunked writer if available
    fn = getattr(sheets_service, "write_grid_chunked", None)
    if callable(fn):
        return int(fn(sid, sheet_name, start_cell, fixed) or 0)

    # Fallback: try generic writer if exists
    fn2 = getattr(sheets_service, "write_grid", None)
    if callable(fn2):
        return int(fn2(sid, sheet_name, start_cell, fixed) or 0)

    raise RuntimeError("google_sheets_service missing write_grid_chunked/write_grid")

# =============================================================================
# Main
# =============================================================================
def main(argv: Optional[List[str]] = None) -> int:
    ap = argparse.ArgumentParser(description="TFB AI Market Scanner (production-safe)")
    ap.add_argument("--sheet-id", help="Target Spreadsheet ID")
    ap.add_argument("--keys", nargs="*", help="Registry keys to scan (e.g. KSA GLOBAL LEADERS)")
    ap.add_argument("--top", default="50", help="Number of opportunities to save (default: 50)")
    ap.add_argument("--sheet-name", default="Market_Scan", help="Destination tab name")
    ap.add_argument("--start-cell", default=DEFAULT_START_CELL, help="Top-left cell for output grid (default: A5)")
    ap.add_argument("--no-sheet", action="store_true", help="Print results only (do not write to sheet)")
    ap.add_argument("--dry-run", action="store_true", help="Only read symbols and print counts (no backend call)")
    ap.add_argument("--json-out", help="Save JSON report to file")
    ap.add_argument("--no-preflight", action="store_true", help="Skip backend/sheets checks")
    ap.add_argument("--max-symbols", default="1000", help="Cap unique symbols to scan (default: 1000)")
    ap.add_argument("--chunk-size", default="150", help="Backend chunk size (default: 150)")
    ap.add_argument("--timeout", default="75", help="Backend timeout seconds (default: 75)")
    ap.add_argument("--retries", default="1", help="Retries per chunk (default: 1)")
    ap.add_argument("--backoff-base", default="1.2", help="Backoff base seconds (default: 1.2)")
    ap.add_argument("--backoff-cap", default="8.0", help="Backoff cap seconds (default: 8.0)")
    ap.add_argument("--mode", default="comprehensive", help="Backend analysis mode (default: comprehensive)")
    args = ap.parse_args(argv)

    sid = _get_spreadsheet_id(args.sheet_id)
    if not sid:
        logger.error("Configuration Error: No Spreadsheet ID found.")
        return 1

    try:
        start_cell = _validate_a1_cell(args.start_cell)
    except Exception as e:
        logger.error("Invalid --start-cell: %s", e)
        return 1

    top_n = max(1, min(500, _safe_int(args.top, 50)))
    max_symbols = max(10, min(5000, _safe_int(args.max_symbols, 1000)))
    chunk_size = max(10, min(500, _safe_int(args.chunk_size, 150)))
    timeout_sec = max(10.0, min(180.0, _safe_float(args.timeout, 75.0)))
    retries = max(0, min(5, _safe_int(args.retries, 1)))
    backoff_base = max(0.2, min(10.0, _safe_float(args.backoff_base, 1.2)))
    backoff_cap = max(backoff_base, min(60.0, _safe_float(args.backoff_cap, 8.0)))
    mode = str(args.mode or "comprehensive").strip() or "comprehensive"

    backend_url = _backend_base_url()

    # Keys
    keys_in = args.keys or DEFAULT_SCAN_KEYS
    scan_keys = [_canon_key(k) for k in keys_in]

    if not args.no_preflight and not _preflight(backend_url, timeout_sec=10.0):
        return 1

    logger.info("Market Scan v%s | backend=%s | sheet=%s", SCRIPT_VERSION, backend_url, sid)
    logger.info("Scan Keys: %s", ", ".join(scan_keys))

    # 1) Discover symbols
    symbols, origin_map, per_key_counts = _read_symbols_from_keys(sid, scan_keys, max_symbols=max_symbols)
    logger.info("Symbols discovered (per key): %s", ", ".join([f"{k}={per_key_counts.get(k,0)}" for k in scan_keys]))
    logger.info("Unique symbols to scan: %d (cap=%d)", len(symbols), max_symbols)

    if args.dry_run:
        return 0

    if not symbols:
        logger.error("No symbols found. Ensure dashboards have tickers (usually Column B).")
        return 2

    # 2) Backend batch analysis (chunked)
    t0 = time.perf_counter()
    results = _call_analysis_batch_chunked(
        symbols,
        backend_url=backend_url,
        timeout_sec=timeout_sec,
        chunk_size=chunk_size,
        retries=retries,
        backoff_base=backoff_base,
        backoff_cap=backoff_cap,
        mode=mode,
    )
    elapsed = time.perf_counter() - t0
    logger.info("Backend returned %d items in %.2fs", len(results), elapsed)

    if not results:
        logger.error("Backend returned 0 results. (Possible: engine issue, provider issue, auth issue)")
        return 2

    # 3) Rank + take top N
    ranked = sorted(results, key=_ranking_key, reverse=True)[:top_n]

    # 4) Build grid
    grid: List[List[Any]] = [list(SCAN_HEADERS)]
    for i, item in enumerate(ranked, start=1):
        sym = str(item.get("symbol") or "").upper()
        origin = origin_map.get(sym, "SCAN")
        grid.append(_map_to_row(item, i, origin))

    # 5) Console / Sheet output
    if args.no_sheet:
        for r in grid[: min(len(grid), 10)]:
            logger.info("%s", r)
        logger.info("No-sheet mode: built %d rows (including header).", len(grid))
    else:
        try:
            logger.info("Writing Top %d opportunities to tab '%s' at %s", top_n, args.sheet_name, start_cell)
            updated = _write_grid_safe(sid, args.sheet_name, start_cell, grid)
            logger.info("✅ Sheet write complete. cells_updated=%s", updated)
        except Exception as e:
            logger.error("❌ Sheet write failed: %s", e)
            return 2

    # 6) Optional JSON report
    if args.json_out:
        try:
            report = {
                "version": SCRIPT_VERSION,
                "time_utc": _now_utc_iso(),
                "time_riyadh": _to_riyadh_iso(_now_utc_iso()),
                "backend": backend_url,
                "scan_keys": scan_keys,
                "symbols_total": len(symbols),
                "results_total": len(results),
                "top_n": top_n,
                "duration_sec": round(elapsed, 3),
                "items": ranked,  # keep raw dicts for downstream use
            }
            with open(args.json_out, "w", encoding="utf-8") as f:
                json.dump(report, f, indent=2, ensure_ascii=False)
            logger.info("JSON report written to %s", args.json_out)
        except Exception as e:
            logger.warning("Failed to write JSON report: %s", e)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
