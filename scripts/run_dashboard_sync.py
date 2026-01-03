#!/usr/bin/env python3
"""
run_dashboard_sync.py
===========================================================
TADAWUL FAST BRIDGE – DASHBOARD SYNCHRONIZER (v2.8.1)
===========================================================

What this script does
1) Connects to your Google Spreadsheet (DEFAULT_SPREADSHEET_ID or --sheet-id).
2) Reads symbols from configured tabs via `symbols_reader`.
3) Calls backend endpoints (enriched / analysis / advanced / argaam) through `google_sheets_service`.
4) Writes updated {headers, rows} back into the target sheets (chunked, Sheets-safe).

v2.8.1 changes (safe hardening)
- ✅ Handles symbols_reader outputs that are {tickers:[...]} or {symbols:[...]} cleanly.
- ✅ More robust key resolution (accepts page registry key variants).
- ✅ Value-input validation hardened.
- ✅ Health-check won’t crash on non-JSON bodies.

Exit code
- 2 if failures exist, else 0
"""

from __future__ import annotations

import argparse
import inspect
import json
import logging
import os
import re
import sys
import time
import urllib.error
import urllib.request
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Callable, Dict, List, Optional, Sequence, Tuple

# =============================================================================
# Version / Logging
# =============================================================================

SCRIPT_VERSION = "2.8.1"

LOG_FORMAT = "%(asctime)s | %(levelname)s | %(message)s"
DATE_FORMAT = "%H:%M:%S"

logging.basicConfig(level=logging.INFO, format=LOG_FORMAT, datefmt=DATE_FORMAT)
logger = logging.getLogger("DashboardSync")

_A1_CELL_RE = re.compile(r"^\$?[A-Za-z]+\$?\d+$")


# =============================================================================
# Path safety (allow running from subfolders)
# =============================================================================
def _ensure_project_root_on_path() -> None:
    """
    Ensure imports work even if running from a subdirectory.
    Adds the directory containing this file (and its parent) to sys.path.
    """
    try:
        here = os.path.dirname(os.path.abspath(__file__))
        parent = os.path.dirname(here)
        if here and here not in sys.path:
            sys.path.insert(0, here)
        if parent and parent not in sys.path:
            sys.path.insert(0, parent)
    except Exception:
        pass


_ensure_project_root_on_path()


# =============================================================================
# Imports (project)
# =============================================================================
try:
    from env import settings  # type: ignore
    import symbols_reader  # type: ignore
    import google_sheets_service as sheets_service  # type: ignore
except Exception as e:
    logger.error("Import failed: %s", e)
    logger.error("Tip: run from project root where env.py / symbols_reader.py exist.")
    sys.exit(1)


# =============================================================================
# Internal types
# =============================================================================
RefreshFunc = Callable[..., Dict[str, Any]]


@dataclass(frozen=True)
class SyncTask:
    key: str
    method: RefreshFunc
    desc: str
    kind: str = "enriched"  # enriched | ai | advanced | ksa


# =============================================================================
# Helpers
# =============================================================================
def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _get_spreadsheet_id(cli_sheet_id: Optional[str] = None) -> str:
    if cli_sheet_id and str(cli_sheet_id).strip():
        return str(cli_sheet_id).strip()

    sid = (getattr(settings, "default_spreadsheet_id", None) or "").strip()
    if not sid:
        sid = (os.getenv("DEFAULT_SPREADSHEET_ID", "") or "").strip()
    return sid


def _validate_start_cell(cell: str) -> str:
    c = (cell or "").strip()
    if not c:
        return "A5"
    if ":" in c:
        c = c.split(":", 1)[0].strip()
    if not _A1_CELL_RE.match(c):
        logger.warning("Invalid start-cell '%s' -> using A5", cell)
        return "A5"
    return c.replace("$", "").upper()


def _parse_keys_list(keys: Optional[Sequence[str]]) -> List[str]:
    if not keys:
        return []
    out: List[str] = []
    for x in keys:
        if x is None:
            continue
        s = str(x).strip()
        if not s:
            continue
        parts = [p.strip() for p in s.split(",") if p.strip()]
        out.extend(parts)
    seen = set()
    final: List[str] = []
    for k in out:
        ku = k.strip().upper()
        if not ku:
            continue
        if ku in seen:
            continue
        seen.add(ku)
        final.append(ku)
    return final


def _resolve_sheet_name(task_key: str) -> Optional[str]:
    """
    Prefer symbols_reader.PAGE_REGISTRY[key].sheet_name if available.
    Fall back to env settings.* fields.
    If still missing: fallback to key itself (safe last resort).
    """
    key = (task_key or "").strip().upper()
    if not key:
        return None

    # 1) symbols_reader registry (preferred)
    try:
        reg = getattr(symbols_reader, "PAGE_REGISTRY", None)
        if isinstance(reg, dict):
            # accept common key variants
            cfg = (
                reg.get(key)
                or reg.get(key.lower())
                or reg.get(key.title())
                or reg.get(key.replace(" ", "_"))
            )
            if cfg is not None:
                if isinstance(cfg, dict):
                    nm = (cfg.get("sheet_name") or cfg.get("tab") or cfg.get("name") or "").strip()
                    if nm:
                        return nm
                nm = (getattr(cfg, "sheet_name", None) or getattr(cfg, "tab_name", None) or "").strip()
                if nm:
                    return nm
    except Exception:
        pass

    # 2) env settings fallbacks
    candidates = {
        "KSA_TADAWUL": getattr(settings, "sheet_ksa_tadawul", None),
        "GLOBAL_MARKETS": getattr(settings, "sheet_global_markets", None),
        "MUTUAL_FUNDS": getattr(settings, "sheet_mutual_funds", None),
        "COMMODITIES_FX": getattr(settings, "sheet_commodities_fx", None),
        "MY_PORTFOLIO": getattr(settings, "sheet_my_portfolio", None),
        "MARKET_LEADERS": getattr(settings, "sheet_market_leaders", None),
        "INSIGHTS_ANALYSIS": getattr(settings, "sheet_insights_analysis", None),
        "INVESTMENT_ADVISOR": getattr(settings, "sheet_investment_advisor", None),
        "ADVANCED_ANALYSIS": getattr(settings, "sheet_advanced_analysis", None),
        "ECONOMIC_CALENDAR": getattr(settings, "sheet_economic_calendar", None),
        "INVESTMENT_INCOME": getattr(settings, "sheet_investment_income", None),
    }
    nm = candidates.get(key)
    nm = nm.strip() if isinstance(nm, str) else ""
    if nm:
        return nm

    # 3) last resort
    return key


def _symbols_reader_call(key: str) -> Any:
    """
    Support multiple symbols_reader APIs without breaking.
    """
    fn = getattr(symbols_reader, "get_page_symbols", None)
    if callable(fn):
        return fn(key)

    for name in ("get_symbols_for_page", "get_symbols", "read_symbols_for_page"):
        fn2 = getattr(symbols_reader, name, None)
        if callable(fn2):
            return fn2(key)

    raise RuntimeError("symbols_reader has no supported symbol function (expected get_page_symbols).")


def _read_symbols_for_key(task_key: str) -> Tuple[List[str], Dict[str, Any]]:
    """
    Returns (symbols, meta).

    Supports:
    - dict: {"all":[...], "ksa":[...], "global":[...], "meta":{...}}
    - dict: {"tickers":[...]} or {"symbols":[...]}
    - list: [...]
    """
    key = (task_key or "").strip().upper()
    data = _symbols_reader_call(key)

    if isinstance(data, list):
        syms = [str(x).strip() for x in data if str(x).strip()]
        return syms, {"count": len(syms), "source_shape": "list"}

    if not isinstance(data, dict):
        raise RuntimeError(f"Unexpected symbols_reader output type for {key}: {type(data)}")

    all_syms = data.get("all")
    ksa_syms = data.get("ksa")
    glob_syms = data.get("global")

    if all_syms is None:
        all_syms = data.get("tickers") or data.get("symbols") or []

    chosen = all_syms
    if key == "KSA_TADAWUL" and isinstance(ksa_syms, list) and ksa_syms:
        chosen = ksa_syms
    elif key == "GLOBAL_MARKETS" and isinstance(glob_syms, list) and glob_syms:
        chosen = glob_syms

    syms = [str(x).strip() for x in (chosen or []) if str(x).strip()]

    meta_in = data.get("meta") or {}
    meta: Dict[str, Any] = {
        "count": len(syms),
        "all_count": len(all_syms) if isinstance(all_syms, list) else None,
        "ksa_count": len(ksa_syms) if isinstance(ksa_syms, list) else None,
        "global_count": len(glob_syms) if isinstance(glob_syms, list) else None,
        "source_shape": "dict",
        "chosen_list": ("ksa" if chosen is ksa_syms else ("global" if chosen is glob_syms else "all/tickers")),
    }
    if isinstance(meta_in, dict):
        meta["symbols_reader"] = {
            k: meta_in.get(k)
            for k in ("status", "version", "symbol_col", "method", "best_score", "header_row_used")
            if k in meta_in
        }

    return syms, meta


def _select_tasks(args: argparse.Namespace, all_tasks: List[SyncTask]) -> List[SyncTask]:
    wanted = _parse_keys_list(args.keys)
    if wanted:
        wset = set(wanted)
        return [t for t in all_tasks if t.key.upper() in wset]

    if args.ksa:
        return [t for t in all_tasks if t.key == "KSA_TADAWUL"]
    if args.global_markets:
        return [t for t in all_tasks if t.key == "GLOBAL_MARKETS"]
    if args.portfolio:
        return [t for t in all_tasks if t.key == "MY_PORTFOLIO"]
    if args.insights:
        return [t for t in all_tasks if t.key == "INSIGHTS_ANALYSIS"]
    if args.advanced:
        return [t for t in all_tasks if t.key in ("INVESTMENT_ADVISOR", "ADVANCED_ANALYSIS") or t.kind == "advanced"]

    return list(all_tasks)


def _call_refresh(
    fn: Callable[..., Any],
    *,
    spreadsheet_id: str,
    sheet_name: str,
    tickers: List[str],
    start_cell: str,
    clear: bool,
    value_input: str,
) -> Dict[str, Any]:
    """
    Signature-aware refresh call.
    """
    if not callable(fn):
        return {"status": "error", "error": "refresh function not callable"}

    try:
        sig = inspect.signature(fn)
        params = sig.parameters

        kw: Dict[str, Any] = {}

        if "spreadsheet_id" in params:
            kw["spreadsheet_id"] = spreadsheet_id
        elif "sid" in params:
            kw["sid"] = spreadsheet_id
        elif "sheet_id" in params:
            kw["sheet_id"] = spreadsheet_id

        if "sheet_name" in params:
            kw["sheet_name"] = sheet_name
        elif "sheet" in params:
            kw["sheet"] = sheet_name
        elif "tab" in params:
            kw["tab"] = sheet_name

        if "tickers" in params:
            kw["tickers"] = tickers
        elif "symbols" in params:
            kw["symbols"] = tickers

        if "start_cell" in params:
            kw["start_cell"] = start_cell
        if "clear" in params:
            kw["clear"] = clear
        if "value_input" in params:
            kw["value_input"] = value_input

        if kw:
            out = fn(**kw)
            return out if isinstance(out, dict) else {"status": "success", "result": out}

    except Exception:
        pass

    try:
        out = fn(
            spreadsheet_id,
            sheet_name,
            tickers,
            start_cell=start_cell,
            clear=clear,
            value_input=value_input,
        )
        return out if isinstance(out, dict) else {"status": "success", "result": out}
    except TypeError:
        try:
            out = fn(spreadsheet_id, sheet_name, tickers, clear=clear)
            return out if isinstance(out, dict) else {"status": "success", "result": out}
        except Exception as e:
            return {"status": "error", "error": str(e)}
    except Exception as e:
        return {"status": "error", "error": str(e)}


def _ksa_refresh_method(
    *,
    spreadsheet_id: str,
    sheet_name: str,
    tickers: List[str],
    start_cell: str,
    clear: bool,
    value_input: str,
    ksa_gateway: str,
) -> Dict[str, Any]:
    """
    KSA page can be fetched from:
    - "enriched" gateway: refresh_sheet_with_enriched_quotes (default)
    - "argaam" gateway:  /v1/argaam/sheet-rows if sheets_service exposes _refresh_logic
    """
    gw = (ksa_gateway or "enriched").strip().lower()

    if gw == "argaam" and hasattr(sheets_service, "_refresh_logic"):
        try:
            return sheets_service._refresh_logic(  # type: ignore
                "/v1/argaam/sheet-rows",
                spreadsheet_id,
                sheet_name,
                tickers,
                start_cell=start_cell,
                clear=clear,
                value_input=value_input,
            )
        except Exception as e:
            logger.warning("KSA argaam gateway failed; falling back to enriched. err=%s", e)

    return _call_refresh(
        sheets_service.refresh_sheet_with_enriched_quotes,
        spreadsheet_id=spreadsheet_id,
        sheet_name=sheet_name,
        tickers=tickers,
        start_cell=start_cell,
        clear=clear,
        value_input=value_input,
    )


# =============================================================================
# Optional backend health check (urllib)
# =============================================================================
def _backend_base_url_guess() -> str:
    try:
        fn = getattr(sheets_service, "_backend_base_url", None)
        if callable(fn):
            return str(fn()).rstrip("/")
    except Exception:
        pass

    u = (os.getenv("BACKEND_BASE_URL", "") or "").strip().rstrip("/")
    return u or "http://127.0.0.1:8000"


def _http_get_json(url: str, timeout_sec: float = 15.0) -> Tuple[int, Optional[Dict[str, Any]], str]:
    req = urllib.request.Request(
        url,
        method="GET",
        headers={
            "Accept": "application/json, text/plain;q=0.9, */*;q=0.8",
            "User-Agent": f"TadawulFastBridge-DashboardSync/{SCRIPT_VERSION}",
        },
    )
    try:
        with urllib.request.urlopen(req, timeout=timeout_sec) as resp:  # type: ignore
            raw = resp.read().decode("utf-8", errors="replace")
            code = int(getattr(resp, "status", 0) or 200)
            try:
                data = json.loads(raw)
                return code, (data if isinstance(data, dict) else None), raw[:400]
            except Exception:
                return code, None, raw[:400]
    except urllib.error.HTTPError as e:
        try:
            raw = e.read().decode("utf-8", errors="replace")  # type: ignore
        except Exception:
            raw = str(e)
        return int(getattr(e, "code", 0) or 0), None, raw[:400]
    except Exception as e:
        return 0, None, str(e)[:400]


def _health_check() -> Dict[str, Any]:
    base = _backend_base_url_guess()
    endpoints = [
        "/v1/enriched/health",
        "/v1/analysis/health",
        "/v1/advanced/health",
        "/v1/argaam/health",
    ]
    out: Dict[str, Any] = {"base": base, "checks": []}
    for ep in endpoints:
        url = f"{base}{ep}"
        code, data, preview = _http_get_json(url, timeout_sec=15.0)
        out["checks"].append(
            {
                "endpoint": ep,
                "http": code,
                "ok": bool(200 <= code < 300 and isinstance(data, dict)),
                "status": (data or {}).get("status") if isinstance(data, dict) else None,
                "preview": preview,
            }
        )
    return out


# =============================================================================
# Default task map (aligned with your endpoints)
# =============================================================================
def _build_sync_map() -> List[SyncTask]:
    tasks: List[SyncTask] = [
        SyncTask("KSA_TADAWUL", sheets_service.refresh_sheet_with_enriched_quotes, "KSA Tadawul Market", "ksa"),
        SyncTask("GLOBAL_MARKETS", sheets_service.refresh_sheet_with_enriched_quotes, "Global Markets", "enriched"),
        SyncTask("MUTUAL_FUNDS", sheets_service.refresh_sheet_with_enriched_quotes, "Mutual Funds", "enriched"),
        SyncTask("COMMODITIES_FX", sheets_service.refresh_sheet_with_enriched_quotes, "Commodities & FX", "enriched"),
        SyncTask("MY_PORTFOLIO", sheets_service.refresh_sheet_with_enriched_quotes, "My Portfolio", "enriched"),
        SyncTask("MARKET_LEADERS", sheets_service.refresh_sheet_with_enriched_quotes, "Market Leaders", "enriched"),
        SyncTask("INSIGHTS_ANALYSIS", sheets_service.refresh_sheet_with_ai_analysis, "Insights & AI Analysis", "ai"),
    ]

    if hasattr(sheets_service, "refresh_sheet_with_advanced_analysis"):
        tasks.append(
            SyncTask(
                "INVESTMENT_ADVISOR",
                sheets_service.refresh_sheet_with_advanced_analysis,
                "Investment Advisor (Advanced)",
                "advanced",
            )
        )
        tasks.append(
            SyncTask(
                "ADVANCED_ANALYSIS",
                sheets_service.refresh_sheet_with_advanced_analysis,
                "Advanced Analysis (Scoreboard)",
                "advanced",
            )
        )

    return tasks


# =============================================================================
# Core sync logic
# =============================================================================
def sync_page(
    task: SyncTask,
    *,
    spreadsheet_id: str,
    start_cell: str = "A5",
    clear: bool = False,
    dry_run: bool = False,
    max_tickers: Optional[int] = None,
    ksa_gateway: str = "enriched",
    value_input: str = "RAW",
    progress_idx: int = 1,
    progress_total: int = 1,
) -> Dict[str, Any]:
    key = task.key
    desc = task.desc

    pct = (progress_idx / max(1, progress_total)) * 100.0
    logger.info("=== Progress: %s/%s (%.1f%%) | Syncing: %s [%s] ===", progress_idx, progress_total, pct, desc, key)

    sheet_name = _resolve_sheet_name(key)
    if not sheet_name:
        msg = f"Sheet name not found for key={key}"
        logger.error(msg)
        return {"status": "error", "key": key, "desc": desc, "error": msg}

    try:
        symbols, meta = _read_symbols_for_key(key)
    except Exception as e:
        logger.exception("Failed reading symbols for %s", key)
        return {"status": "error", "key": key, "desc": desc, "sheet": sheet_name, "error": str(e)}

    if not symbols:
        logger.warning("No symbols found for %s. Skipping.", key)
        return {
            "status": "skipped",
            "key": key,
            "desc": desc,
            "sheet": sheet_name,
            "reason": "No symbols",
            "symbols_meta": meta,
            "timestamp_utc": _utc_now_iso(),
        }

    if max_tickers and max_tickers > 0 and len(symbols) > max_tickers:
        symbols = symbols[:max_tickers]
        meta["trimmed_to"] = int(max_tickers)

    logger.info("Sheet='%s' | Symbols=%s (chosen=%s)", sheet_name, len(symbols), meta.get("chosen_list"))

    start_cell = _validate_start_cell(start_cell)

    vi = (value_input or "RAW").strip().upper()
    if vi not in ("RAW", "USER_ENTERED"):
        logger.warning("Invalid value_input '%s' -> using RAW", vi)
        vi = "RAW"

    if dry_run:
        logger.info(
            "[DRY RUN] Would update sheet='%s' start_cell=%s clear=%s value_input=%s",
            sheet_name,
            start_cell,
            clear,
            vi,
        )
        return {
            "status": "dry_run",
            "key": key,
            "desc": desc,
            "sheet": sheet_name,
            "count_symbols": len(symbols),
            "start_cell": start_cell,
            "clear": bool(clear),
            "value_input": vi,
            "symbols_meta": meta,
            "timestamp_utc": _utc_now_iso(),
        }

    start_t = time.time()
    try:
        if task.kind == "ksa":
            result = _ksa_refresh_method(
                spreadsheet_id=spreadsheet_id,
                sheet_name=sheet_name,
                tickers=symbols,
                start_cell=start_cell,
                clear=clear,
                value_input=vi,
                ksa_gateway=ksa_gateway,
            )
        else:
            result = _call_refresh(
                task.method,
                spreadsheet_id=spreadsheet_id,
                sheet_name=sheet_name,
                tickers=symbols,
                start_cell=start_cell,
                clear=clear,
                value_input=vi,
            )
    except Exception as e:
        logger.exception("Update failed for %s", key)
        return {"status": "error", "key": key, "desc": desc, "sheet": sheet_name, "error": str(e)}

    duration = time.time() - start_t
    status = str((result or {}).get("status") or "unknown").lower()

    if status in ("success", "partial"):
        logger.info(
            "✅ %s: rows=%s cells=%s time=%.2fs (backend=%s)",
            status.upper(),
            (result or {}).get("rows_written"),
            (result or {}).get("cells_updated"),
            duration,
            (result or {}).get("backend_status"),
        )
    elif status in ("skipped",):
        logger.info("⏭️  SKIPPED: %s", (result or {}).get("reason") or (result or {}).get("error") or "")
    else:
        logger.error("❌ %s failed: %s", key, result)

    out = dict(result or {})
    out.update(
        {
            "key": key,
            "desc": desc,
            "sheet": sheet_name,
            "count_symbols": len(symbols),
            "start_cell": start_cell,
            "clear": bool(clear),
            "value_input": vi,
            "duration_sec": round(duration, 3),
            "timestamp_utc": _utc_now_iso(),
            "symbols_meta": meta,
            "ksa_gateway": ksa_gateway if task.kind == "ksa" else None,
            "progress": {"idx": progress_idx, "total": progress_total, "pct": round(pct, 2)},
        }
    )
    return out


# =============================================================================
# CLI / Main
# =============================================================================
def main() -> None:
    parser = argparse.ArgumentParser(description="Synchronize Dashboard Sheets")
    parser.add_argument("--sheet-id", default=None, help="Override Spreadsheet ID (otherwise uses DEFAULT_SPREADSHEET_ID)")
    parser.add_argument("--dry-run", action="store_true", help="Read symbols but do not write data")
    parser.add_argument("--clear", action="store_true", help="Clear old values first (safe range) before writing")
    parser.add_argument("--start-cell", default="A5", help="Top-left cell where headers should be written (default A5)")
    parser.add_argument("--sleep", type=float, default=2.0, help="Seconds to sleep between pages (default 2.0)")
    parser.add_argument("--max-tickers", type=int, default=0, help="Cap tickers per page (0 = no cap)")
    parser.add_argument("--json-out", default=None, help="Write full results to a JSON file")

    parser.add_argument(
        "--value-input",
        default="RAW",
        choices=["RAW", "USER_ENTERED", "raw", "user_entered"],
        help="Sheets write mode: RAW (default) or USER_ENTERED",
    )

    parser.add_argument("--health", action="store_true", help="Run backend health checks before syncing")

    parser.add_argument(
        "--ksa-gateway",
        default="enriched",
        choices=["enriched", "argaam"],
        help="KSA fetch gateway: enriched (default) or argaam (strict) if supported",
    )

    parser.add_argument("--ksa", action="store_true", help="Sync only KSA Tadawul")
    parser.add_argument("--global", dest="global_markets", action="store_true", help="Sync only Global Markets")
    parser.add_argument("--portfolio", action="store_true", help="Sync only My Portfolio")
    parser.add_argument("--insights", action="store_true", help="Sync only Insights/AI")
    parser.add_argument("--advanced", action="store_true", help="Sync only Advanced pages (if configured)")

    parser.add_argument("--keys", nargs="*", default=None, help="Run only these PAGE_REGISTRY keys (space or comma-separated)")
    parser.add_argument("--log-level", default="INFO", help="DEBUG, INFO, WARNING, ERROR")

    args = parser.parse_args()

    try:
        logging.getLogger().setLevel(getattr(logging, str(args.log_level).upper(), logging.INFO))
    except Exception:
        pass

    sid = _get_spreadsheet_id(args.sheet_id)
    if not sid:
        logger.error("DEFAULT_SPREADSHEET_ID is not set in env.py or environment variables (or pass --sheet-id).")
        sys.exit(1)

    value_input = str(args.value_input or "RAW").strip().upper()

    if args.health:
        hc = _health_check()
        logger.info("=== Backend Health Check ===")
        logger.info("Base: %s", hc.get("base"))
        for c in hc.get("checks", []):
            logger.info(" - %s | http=%s | ok=%s | status=%s", c.get("endpoint"), c.get("http"), c.get("ok"), c.get("status"))

    sync_map = _build_sync_map()
    tasks = _select_tasks(args, sync_map)
    if not tasks:
        logger.warning("No matching tasks found.")
        return

    max_tickers = int(args.max_tickers or 0) or None
    start_cell = _validate_start_cell(str(args.start_cell or "A5"))

    logger.info(
        "Dashboard Sync v%s | pages=%s | dry_run=%s | clear=%s | start_cell=%s | ksa_gateway=%s | value_input=%s",
        SCRIPT_VERSION,
        len(tasks),
        args.dry_run,
        args.clear,
        start_cell,
        args.ksa_gateway,
        value_input,
    )
    logger.info("Target Spreadsheet ID: %s", sid)

    t0 = time.time()

    results: List[Dict[str, Any]] = []
    failures = 0
    partials = 0
    successes = 0
    skipped = 0

    total = len(tasks)

    for idx, task in enumerate(tasks, start=1):
        r = sync_page(
            task,
            spreadsheet_id=sid,
            start_cell=start_cell,
            clear=bool(args.clear),
            dry_run=bool(args.dry_run),
            max_tickers=max_tickers,
            ksa_gateway=str(args.ksa_gateway or "enriched"),
            value_input=value_input,
            progress_idx=idx,
            progress_total=total,
        )
        results.append(r)

        st = str(r.get("status") or "").lower()
        if st in ("error", "failed"):
            failures += 1
        elif st == "partial":
            partials += 1
        elif st == "success":
            successes += 1
        elif st in ("skipped", "dry_run"):
            skipped += 1

        if not args.dry_run and idx < total:
            try:
                time.sleep(max(0.0, float(args.sleep)))
            except Exception:
                pass

    total_sec = time.time() - t0
    logger.info(
        "=== Dashboard Sync Complete === success=%s partial=%s skipped=%s failures=%s total=%s time=%.2fs",
        successes,
        partials,
        skipped,
        failures,
        total,
        total_sec,
    )

    if args.json_out:
        try:
            path = str(args.json_out).strip()
            payload = {
                "version": SCRIPT_VERSION,
                "timestamp_utc": _utc_now_iso(),
                "spreadsheet_id": sid,
                "tasks_total": total,
                "summary": {
                    "success": successes,
                    "partial": partials,
                    "skipped": skipped,
                    "failures": failures,
                    "duration_sec": round(total_sec, 3),
                },
                "results": results,
            }
            with open(path, "w", encoding="utf-8") as f:
                json.dump(payload, f, ensure_ascii=False, indent=2)
            logger.info("Saved JSON results to: %s", path)
        except Exception as e:
            logger.warning("Failed to write --json-out file: %s", e)

    if failures:
        sys.exit(2)


if __name__ == "__main__":
    main()
