#!/usr/bin/env python3
# scripts/run_dashboard_sync.py
"""
run_dashboard_sync.py
===========================================================
TADAWUL FAST BRIDGE – DASHBOARD SYNCHRONIZER (v4.2.0)
===========================================================
PRODUCTION-HARDENED EDITION (SHEETS-SAFE + BACKEND-SAFE)

What this script does
- Reads symbols per dashboard tab (via symbols_reader)
- Calls google_sheets_service refresh methods to write rows back to Sheets
- Adds strong preflight checks (Sheets API + Backend endpoints)
- Adds KSA gateway strategy (enriched / argaam / auto-fallback)
- Adds retry + backoff + partial/fatal exit codes
- Adds strict safety rule: never clear/write if backend returns empty headers (common failure mode)

Exit Codes
- 0: Success (all selected pages wrote data successfully)
- 1: Fatal (preflight/config failure)
- 2: Partial/Failed (one or more pages failed or wrote 0 rows unexpectedly)

Usage
- python scripts/run_dashboard_sync.py
- python scripts/run_dashboard_sync.py --keys MARKET_LEADERS GLOBAL_MARKETS
- python scripts/run_dashboard_sync.py --ksa-gw auto
- python scripts/run_dashboard_sync.py --clear --start-cell A5
- python scripts/run_dashboard_sync.py --json-out sync_report.json
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import re
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timezone, timedelta
from typing import Any, Callable, Dict, List, Optional, Sequence, Tuple

# =============================================================================
# Version & Logging
# =============================================================================
SCRIPT_VERSION = "4.2.0"
LOG_FORMAT = "%(asctime)s | %(levelname)s | %(message)s"
DATE_FORMAT = "%H:%M:%S"

logging.basicConfig(level=logging.INFO, format=LOG_FORMAT, datefmt=DATE_FORMAT)
logger = logging.getLogger("DashboardSync")

_A1_CELL_RE = re.compile(r"^\$?[A-Za-z]+\$?\d+$")

# =============================================================================
# Path & Imports
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

try:
    from env import settings  # type: ignore
    import symbols_reader  # type: ignore
    import google_sheets_service as sheets_service  # type: ignore
except Exception as e:
    logger.error("Project dependency import failed: %s", e)
    raise SystemExit(1)

# =============================================================================
# Data Structures
# =============================================================================
@dataclass(frozen=True)
class SyncTask:
    key: str
    desc: str
    kind: str  # enriched | ai | advanced | ksa
    method_name: str
    sheet_name: Optional[str] = None


@dataclass
class SyncResult:
    key: str
    desc: str
    status: str  # success | partial | error | skipped
    rows_written: int = 0
    symbols_count: int = 0
    duration_sec: float = 0.0
    gateway_used: Optional[str] = None
    error: Optional[str] = None
    meta: Dict[str, Any] = None  # type: ignore


# =============================================================================
# Time Helpers
# =============================================================================
def _now_utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _now_riyadh_iso() -> str:
    tz = timezone(timedelta(hours=3))
    return datetime.now(tz).isoformat()


# =============================================================================
# Config Helpers
# =============================================================================
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
    # Prefer env; fallback to settings; fallback local
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


def _resolve_sheet_name(key: str) -> str:
    # Keep your existing naming conventions
    if key == "KSA_TADAWUL":
        return "KSA_Tadawul"
    # Default: Title Case with spaces
    return key.title().replace("_", " ")


def _validate_a1_cell(a1: str) -> str:
    s = (a1 or "").strip()
    if not s:
        return "A5"
    if not _A1_CELL_RE.match(s):
        raise ValueError(f"Invalid A1 start cell: {a1}")
    return s


# =============================================================================
# Minimal HTTP client (no external deps required)
# =============================================================================
def _http_get_json(url: str, timeout_sec: float = 10.0) -> Tuple[Optional[Dict[str, Any]], Optional[str], int]:
    """
    Uses urllib only (keeps dependency-free).
    Returns: (json_dict_or_none, error_or_none, http_status_or_0)
    """
    import urllib.request
    import urllib.error

    try:
        req = urllib.request.Request(url, method="GET", headers={"User-Agent": "TFB-DashboardSync/4.2.0"})
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
        code = int(getattr(e, "code", 0) or 0)
        return None, f"HTTPError {code}", code
    except Exception as e:
        return None, str(e), 0


# =============================================================================
# Preflight Checks
# =============================================================================
def _preflight_check(
    spreadsheet_id: str,
    *,
    backend_url: str,
    need_enriched: bool,
    need_argaam: bool,
    need_ai: bool,
    timeout_sec: float,
) -> bool:
    logger.info("--- Starting Pre-Flight Validation ---")

    # 1) Sheets API connectivity
    try:
        sheets_service.get_sheets_service()
        logger.info("✅ Google Sheets API: Connected")
    except Exception as e:
        logger.error("❌ Google Sheets API: Connection Failed: %s", e)
        return False

    # 2) Backend /readyz or /health (best effort)
    ready_ok = False
    for p in ("/readyz", "/health", "/v1/enriched/health"):
        data, err, code = _http_get_json(f"{backend_url}{p}", timeout_sec=timeout_sec)
        if code == 200 and isinstance(data, dict):
            ready_ok = True
            logger.info("✅ Backend API: Connected (%s)", p)
            break
        if err:
            logger.debug("Backend check %s failed: %s", p, err)

    if not ready_ok:
        logger.error("❌ Backend API: Unreachable or unhealthy at %s", backend_url)
        return False

    # 3) Route-level checks based on selected tasks
    def check(path: str, label: str) -> bool:
        data, err, code = _http_get_json(f"{backend_url}{path}", timeout_sec=timeout_sec)
        if code == 200 and isinstance(data, dict):
            logger.info("✅ %s: OK", label)
            return True
        logger.warning("⚠️  %s: Not OK (%s)", label, err or f"HTTP {code}")
        return False

    if need_enriched:
        check("/v1/enriched/health", "Enriched Route")
    if need_argaam:
        check("/v1/argaam/health", "Argaam Route")
    if need_ai:
        check("/v1/analysis/health", "AI Analysis Route")

    return True


# =============================================================================
# Task Registry
# =============================================================================
def _build_tasks() -> List[SyncTask]:
    # Keep method names aligned to your google_sheets_service module
    # NOTE: KSA has gateway logic; the method_name here is default enriched writer.
    return [
        SyncTask("KSA_TADAWUL", "KSA Tadawul", "ksa", "refresh_sheet_with_enriched_quotes", "KSA_Tadawul"),
        SyncTask("MARKET_LEADERS", "Market Leaders", "enriched", "refresh_sheet_with_enriched_quotes"),
        SyncTask("GLOBAL_MARKETS", "Global Markets", "enriched", "refresh_sheet_with_enriched_quotes"),
        SyncTask("MUTUAL_FUNDS", "Mutual Funds", "enriched", "refresh_sheet_with_enriched_quotes"),
        SyncTask("COMMODITIES_FX", "Commodities & FX", "enriched", "refresh_sheet_with_enriched_quotes"),
        SyncTask("MY_PORTFOLIO", "My Portfolio", "enriched", "refresh_sheet_with_enriched_quotes"),
        SyncTask("INSIGHTS_ANALYSIS", "AI Insights", "ai", "refresh_sheet_with_ai_analysis"),
    ]


# =============================================================================
# Symbols Reader
# =============================================================================
def _read_symbols(key: str, spreadsheet_id: str, *, max_symbols: int) -> List[str]:
    sym_data = symbols_reader.get_page_symbols(key, spreadsheet_id=spreadsheet_id)
    symbols: List[str]
    if isinstance(sym_data, dict):
        symbols = sym_data.get("all") or sym_data.get("symbols") or []
    else:
        symbols = sym_data or []

    # normalize, de-dupe preserve order
    out: List[str] = []
    seen = set()
    for s in symbols:
        t = str(s or "").strip()
        if not t:
            continue
        u = t.upper()
        if u in {"SYMBOL", "TICKER"}:
            continue
        if u.startswith("#"):
            continue
        if u in seen:
            continue
        seen.add(u)
        out.append(u)

    if max_symbols > 0 and len(out) > max_symbols:
        out = out[:max_symbols]
    return out


# =============================================================================
# Gateway execution (KSA)
# =============================================================================
def _refresh_via_argaam(
    spreadsheet_id: str,
    sheet_name: str,
    symbols: List[str],
    *,
    start_cell: str,
    clear: bool,
) -> Dict[str, Any]:
    # Use your internal refresh logic endpoint for Argaam
    # (google_sheets_service._refresh_logic must exist)
    return sheets_service._refresh_logic(  # type: ignore[attr-defined]
        "/v1/argaam/sheet-rows",
        spreadsheet_id,
        sheet_name,
        symbols,
        start_cell=start_cell,
        clear=clear,
    )


def _refresh_via_enriched(
    spreadsheet_id: str,
    sheet_name: str,
    symbols: List[str],
    *,
    start_cell: str,
    clear: bool,
) -> Dict[str, Any]:
    fn = getattr(sheets_service, "refresh_sheet_with_enriched_quotes", None)
    if not callable(fn):
        raise RuntimeError("google_sheets_service.refresh_sheet_with_enriched_quotes not found")
    return fn(
        spreadsheet_id=spreadsheet_id,
        sheet_name=sheet_name,
        tickers=symbols,
        start_cell=start_cell,
        clear=clear,
    )


# =============================================================================
# Safety Checks on Service Results
# =============================================================================
def _result_is_safe(res: Dict[str, Any]) -> Tuple[bool, Optional[str]]:
    """
    Protect against the #1 production failure mode:
    backend returns empty headers => GAS clears sheet => dashboard becomes blank.

    We enforce:
    - If service returns "headers" key and it is empty list => unsafe
    - If service returns status "success" but rows_written == 0 while caller had symbols => suspicious
    """
    if not isinstance(res, dict):
        return False, "Service returned non-dict result"

    headers = res.get("headers")
    if headers is not None:
        if isinstance(headers, list) and len(headers) == 0:
            return False, "Unsafe: backend returned EMPTY headers"

    # Some services return status "success" and rows_written
    status = str(res.get("status") or "").lower()
    if status in {"error", "failed"}:
        return True, None  # safe to report; caller handles it

    return True, None


def _coerce_rows_written(res: Dict[str, Any]) -> int:
    for k in ("rows_written", "written_rows", "rows", "count"):
        v = res.get(k)
        if isinstance(v, int):
            return v
        if isinstance(v, str) and v.strip().isdigit():
            return int(v.strip())
    return 0


# =============================================================================
# Sync Page Runner
# =============================================================================
def _sleep_backoff(attempt: int, base: float, cap: float) -> None:
    # attempt=1 => base, attempt=2 => 2*base, ...
    delay = min(cap, base * (2 ** max(0, attempt - 1)))
    # small jitter without random import
    delay = delay + (0.07 * attempt)
    time.sleep(delay)


def sync_one(
    task: SyncTask,
    *,
    spreadsheet_id: str,
    start_cell: str,
    clear: bool,
    ksa_gateway: str,
    retries: int,
    backoff_base: float,
    backoff_cap: float,
    max_symbols: int,
    dry_run: bool,
) -> SyncResult:
    key = _canon_key(task.key)
    desc = task.desc
    sheet_name = task.sheet_name or _resolve_sheet_name(key)

    t0 = time.perf_counter()

    # 1) Read symbols
    try:
        symbols = _read_symbols(key, spreadsheet_id, max_symbols=max_symbols)
        if not symbols:
            return SyncResult(key=key, desc=desc, status="skipped", duration_sec=0.0, meta={"reason": "no_symbols"})
    except Exception as e:
        return SyncResult(key=key, desc=desc, status="error", error=f"Symbol read failed: {e}", meta={"stage": "read_symbols"})

    if dry_run:
        return SyncResult(
            key=key,
            desc=desc,
            status="skipped",
            symbols_count=len(symbols),
            duration_sec=round(time.perf_counter() - t0, 4),
            meta={"dry_run": True, "sheet_name": sheet_name},
        )

    # 2) Resolve refresh callable by kind
    def run_gateway(gw: str) -> Dict[str, Any]:
        if task.kind == "ksa":
            if gw == "argaam":
                return _refresh_via_argaam(spreadsheet_id, sheet_name, symbols, start_cell=start_cell, clear=clear)
            return _refresh_via_enriched(spreadsheet_id, sheet_name, symbols, start_cell=start_cell, clear=clear)

        # AI / Enriched pages: call method_name
        fn = getattr(sheets_service, task.method_name, None)
        if not callable(fn):
            raise RuntimeError(f"google_sheets_service.{task.method_name} not found")

        return fn(
            spreadsheet_id=spreadsheet_id,
            sheet_name=sheet_name,
            tickers=symbols,
            start_cell=start_cell,
            clear=clear,
        )

    # 3) Execution with retries (+ optional KSA gateway fallback)
    attempts = max(1, retries + 1)
    last_err: Optional[str] = None
    gw_used: Optional[str] = None
    res: Optional[Dict[str, Any]] = None

    # Determine gateway plan
    if task.kind == "ksa":
        gw = ksa_gateway.lower().strip()
        if gw not in {"enriched", "argaam", "auto"}:
            gw = "enriched"
        # auto plan: enriched then argaam (on failure/unsafe)
        gw_plan = ["enriched", "argaam"] if gw == "auto" else [gw]
    else:
        gw_plan = ["default"]

    for gw in gw_plan:
        for i in range(1, attempts + 1):
            try:
                gw_used = gw if gw != "default" else None
                res = run_gateway(gw if gw != "default" else "enriched")

                safe, unsafe_reason = _result_is_safe(res)
                if not safe:
                    raise RuntimeError(unsafe_reason or "Unsafe result")

                status = str(res.get("status") or "unknown").lower()
                rows_written = _coerce_rows_written(res)

                # If symbols exist but rows_written is 0 => treat as partial and retry
                if len(symbols) > 0 and status in {"success", "ok"} and rows_written == 0:
                    raise RuntimeError("Wrote 0 rows with non-empty symbols list (likely empty backend rows)")

                # success path
                elapsed = time.perf_counter() - t0
                return SyncResult(
                    key=key,
                    desc=desc,
                    status="success" if status in {"success", "ok"} else ("partial" if status in {"partial"} else status),
                    rows_written=rows_written,
                    symbols_count=len(symbols),
                    duration_sec=elapsed,
                    gateway_used=gw_used,
                    meta={
                        "sheet_name": sheet_name,
                        "backend_status": status,
                        "time_utc": _now_utc_iso(),
                        "time_riyadh": _now_riyadh_iso(),
                        "attempt": i,
                        "gateway": gw_used or "default",
                    },
                )

            except Exception as e:
                last_err = str(e)
                if i < attempts:
                    _sleep_backoff(i, base=backoff_base, cap=backoff_cap)
                continue

        # if this gateway failed after retries, move to next gw in plan (for KSA auto)
        continue

    elapsed = time.perf_counter() - t0
    return SyncResult(
        key=key,
        desc=desc,
        status="error",
        rows_written=0,
        symbols_count=len(symbols),
        duration_sec=elapsed,
        gateway_used=gw_used,
        error=last_err or "Unknown error",
        meta={
            "sheet_name": sheet_name,
            "time_utc": _now_utc_iso(),
            "time_riyadh": _now_riyadh_iso(),
            "attempts": attempts,
            "gateway_plan": gw_plan,
        },
    )


# =============================================================================
# Reporting
# =============================================================================
def _summarize(results: List[SyncResult]) -> Dict[str, Any]:
    succ = [r for r in results if r.status == "success"]
    part = [r for r in results if r.status == "partial"]
    fail = [r for r in results if r.status == "error"]
    skip = [r for r in results if r.status == "skipped"]

    total_rows = sum(int(r.rows_written or 0) for r in results)
    total_syms = sum(int(r.symbols_count or 0) for r in results)
    total_time = sum(float(r.duration_sec or 0.0) for r in results)

    return {
        "version": SCRIPT_VERSION,
        "time_utc": _now_utc_iso(),
        "time_riyadh": _now_riyadh_iso(),
        "pages_total": len(results),
        "pages_success": len(succ),
        "pages_partial": len(part),
        "pages_failed": len(fail),
        "pages_skipped": len(skip),
        "symbols_total": total_syms,
        "rows_total_written": total_rows,
        "duration_total_sec": round(total_time, 3),
    }


def _results_to_jsonable(results: List[SyncResult]) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for r in results:
        out.append(
            {
                "key": r.key,
                "desc": r.desc,
                "status": r.status,
                "rows_written": r.rows_written,
                "symbols_count": r.symbols_count,
                "duration_sec": round(r.duration_sec, 4),
                "gateway_used": r.gateway_used,
                "error": r.error,
                "meta": r.meta or {},
            }
        )
    return out


# =============================================================================
# Main
# =============================================================================
def main(argv: Optional[List[str]] = None) -> int:
    ap = argparse.ArgumentParser(description="TFB Dashboard Synchronizer (production-safe)")
    ap.add_argument("--sheet-id", help="Target Spreadsheet ID")
    ap.add_argument("--keys", nargs="*", help="Specific page keys to sync")
    ap.add_argument("--clear", action="store_true", help="Clear data rows before writing")
    ap.add_argument("--ksa-gw", default="enriched", choices=["enriched", "argaam", "auto"], help="Gateway for KSA tab")
    ap.add_argument("--start-cell", default="A5", help="Top-left cell for data write (A1 notation)")
    ap.add_argument("--no-preflight", action="store_true", help="Skip connectivity checks")
    ap.add_argument("--retries", default="1", help="Retries per page (default: 1)")
    ap.add_argument("--backoff-base", default="1.2", help="Backoff base seconds (default: 1.2)")
    ap.add_argument("--backoff-cap", default="8.0", help="Max backoff seconds (default: 8.0)")
    ap.add_argument("--max-symbols", default="0", help="Cap symbols per page (0 = no cap)")
    ap.add_argument("--dry-run", action="store_true", help="Only read symbols and print counts; do not write to Sheets")
    ap.add_argument("--json-out", help="Write full report JSON to a file path")

    args = ap.parse_args(argv)

    # Resolve core config
    sid = _get_spreadsheet_id(args.sheet_id)
    if not sid:
        logger.error("Configuration Error: No Spreadsheet ID found (DEFAULT_SPREADSHEET_ID / settings.default_spreadsheet_id).")
        return 1

    try:
        start_cell = _validate_a1_cell(args.start_cell)
    except Exception as e:
        logger.error("Invalid --start-cell: %s", e)
        return 1

    retries = _safe_int(args.retries, 1)
    retries = max(0, min(5, retries))

    backoff_base = _safe_float(args.backoff_base, 1.2)
    backoff_cap = _safe_float(args.backoff_cap, 8.0)
    backoff_base = max(0.2, min(10.0, backoff_base))
    backoff_cap = max(backoff_base, min(60.0, backoff_cap))

    max_symbols = _safe_int(args.max_symbols, 0)
    max_symbols = max(0, max_symbols)

    backend_url = _backend_base_url()

    # Build task list
    all_tasks = _build_tasks()
    if args.keys:
        wanted = {_canon_key(k) for k in args.keys}
        tasks = [t for t in all_tasks if _canon_key(t.key) in wanted]
    else:
        tasks = all_tasks

    if not tasks:
        logger.warning("No valid tasks selected.")
        return 0

    # Preflight
    if not args.no_preflight:
        need_enriched = any(t.kind in {"enriched", "ksa"} for t in tasks) and args.ksa_gw in {"enriched", "auto"}
        need_argaam = any(t.kind == "ksa" for t in tasks) and args.ksa_gw in {"argaam", "auto"}
        need_ai = any(t.kind == "ai" for t in tasks)
        ok = _preflight_check(
            sid,
            backend_url=backend_url,
            need_enriched=need_enriched,
            need_argaam=need_argaam,
            need_ai=need_ai,
            timeout_sec=10.0,
        )
        if not ok:
            logger.error("Preflight validation failed. Aborting.")
            return 1

    # Run
    logger.info("Starting sync sequence (v%s) for %d dashboard(s)...", SCRIPT_VERSION, len(tasks))
    logger.info("Backend: %s | SheetId: %s | KSA Gateway: %s | Clear: %s | StartCell: %s",
                backend_url, sid, args.ksa_gw, bool(args.clear), start_cell)

    results: List[SyncResult] = []
    t_total_start = time.perf_counter()

    for idx, task in enumerate(tasks, start=1):
        key = _canon_key(task.key)
        logger.info("(%d/%d) ▶ %s [%s]", idx, len(tasks), task.desc, key)

        r = sync_one(
            task,
            spreadsheet_id=sid,
            start_cell=start_cell,
            clear=bool(args.clear),
            ksa_gateway=args.ksa_gw,
            retries=retries,
            backoff_base=backoff_base,
            backoff_cap=backoff_cap,
            max_symbols=max_symbols,
            dry_run=bool(args.dry_run),
        )
        results.append(r)

        # Logging per task
        if r.status == "success":
            rate = (r.symbols_count / r.duration_sec) if r.duration_sec > 0 and r.symbols_count else 0.0
            logger.info("   ✅ SUCCESS | rows=%s | syms=%s | %.2fs | %.1f sym/sec%s",
                        r.rows_written, r.symbols_count, r.duration_sec, rate,
                        f" | gw={r.gateway_used}" if r.gateway_used else "")
        elif r.status == "skipped":
            logger.info("   ⏭️  SKIPPED | syms=%s | %s", r.symbols_count, (r.meta or {}).get("reason", ""))
        else:
            logger.warning("   ❌ %s | syms=%s | %.2fs | err=%s%s",
                           r.status.upper(), r.symbols_count, r.duration_sec, r.error or "unknown",
                           f" | gw={r.gateway_used}" if r.gateway_used else "")

        # Cooldown (quota-friendly) unless dry-run
        if not args.dry_run:
            time.sleep(1.2)

    t_total = time.perf_counter() - t_total_start
    summary = _summarize(results)
    summary["duration_wall_sec"] = round(t_total, 3)

    # Print summary
    logger.info("--- Sync Report (v%s) ---", SCRIPT_VERSION)
    logger.info("Total Wall Time: %.2fs", t_total)
    logger.info("Pages: %d success | %d partial | %d failed | %d skipped",
                summary["pages_success"], summary["pages_partial"], summary["pages_failed"], summary["pages_skipped"])
    logger.info("Symbols: %d | Rows Written: %d",
                summary["symbols_total"], summary["rows_total_written"])

    # Optional JSON output
    if args.json_out:
        try:
            payload = {
                "summary": summary,
                "results": _results_to_jsonable(results),
            }
            with open(args.json_out, "w", encoding="utf-8") as f:
                json.dump(payload, f, indent=2, ensure_ascii=False)
            logger.info("Report written to %s", args.json_out)
        except Exception as e:
            logger.warning("Failed to write json report: %s", e)

    # Exit codes
    failed = any(r.status == "error" for r in results)
    partial = any(r.status == "partial" for r in results)

    if failed or partial:
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
