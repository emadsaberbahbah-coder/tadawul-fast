```python
# run_dashboard_sync.py  (FULL REPLACEMENT)
"""
run_dashboard_sync.py
===========================================================
TADAWUL FAST BRIDGE – DASHBOARD SYNCHRONIZER (v2.7.0)
===========================================================

What this script does
1) Connects to your Google Spreadsheet (DEFAULT_SPREADSHEET_ID or --sheet-id).
2) Reads symbols from configured tabs via `symbols_reader`.
3) Calls backend endpoints (enriched / analysis / advanced) through `google_sheets_service`.
4) Writes updated {headers, rows} back into the target sheets (chunked, Sheets-safe).

Key upgrades (v2.7.0)
- FIX: Compatible with google_sheets_service signatures (spreadsheet_id vs sid)
- Robust refresh invocation via signature-aware dispatcher (no more TypeError mismatch)
- Better symbol selection (prefers {ksa/global} lists when present for KSA/Global pages)
- Clearer progress + totals + run duration
- Never crashes whole run (best-effort per page), exit code non-zero only if failures

Usage
  python run_dashboard_sync.py
  python run_dashboard_sync.py --dry-run
  python run_dashboard_sync.py --ksa
  python run_dashboard_sync.py --global
  python run_dashboard_sync.py --portfolio
  python run_dashboard_sync.py --insights
  python run_dashboard_sync.py --keys KSA_TADAWUL GLOBAL_MARKETS
  python run_dashboard_sync.py --clear --start-cell A5 --sleep 2
  python run_dashboard_sync.py --ksa-gateway argaam
  python run_dashboard_sync.py --json-out sync_results.json

Notes
- Run from project root (where env.py / symbols_reader.py exist).
- Continues best-effort if one page fails.
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
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Callable, Dict, List, Optional, Sequence, Tuple

# =============================================================================
# Version / Logging
# =============================================================================

SCRIPT_VERSION = "2.7.0"

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
    # allow "A5:ZZ" (we take the left side)
    if ":" in c:
        c = c.split(":", 1)[0].strip()
    if not _A1_CELL_RE.match(c):
        logger.warning("Invalid start-cell '%s' -> using A5", cell)
        return "A5"
    return c.replace("$", "").upper()


def _resolve_sheet_name(task_key: str) -> Optional[str]:
    """
    Prefer symbols_reader.PAGE_REGISTRY[key].sheet_name if available.
    Fall back to env settings.* fields.
    """
    key = (task_key or "").strip().upper()

    # 1) symbols_reader registry (preferred)
    try:
        reg = getattr(symbols_reader, "PAGE_REGISTRY", None)
        if isinstance(reg, dict):
            cfg = reg.get(key) or reg.get(key.lower()) or reg.get(key.title())
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
        "ECONOMIC_CALENDAR": getattr(settings, "sheet_economic_calendar", None),
        "INVESTMENT_INCOME": getattr(settings, "sheet_investment_income", None),
    }
    nm = candidates.get(key)
    nm = nm.strip() if isinstance(nm, str) else ""
    return nm or None


def _read_symbols_for_key(task_key: str) -> Tuple[List[str], Dict[str, Any]]:
    """
    Returns (symbols, meta).

    Supports:
    - enhanced symbols_reader output: {"all":[...], "ksa":[...], "global":[...], "meta":{...}}
    - older shapes: {"tickers":[...]} or list

    Selection policy:
    - KSA_TADAWUL prefers "ksa" list if present and non-empty
    - GLOBAL_MARKETS prefers "global" list if present and non-empty
    - Others: prefer "all" then fallback to tickers/symbols
    """
    key = (task_key or "").strip().upper()
    try:
        data = symbols_reader.get_page_symbols(key)  # type: ignore
    except Exception as e:
        raise RuntimeError(f"symbols_reader.get_page_symbols failed for {key}: {e}")

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

    # page-specific preference
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
        "chosen_list": (
            "ksa" if chosen is ksa_syms else ("global" if chosen is glob_syms else "all/tickers")
        ),
    }
    if isinstance(meta_in, dict):
        meta["symbols_reader"] = {
            k: meta_in.get(k)
            for k in ("status", "version", "symbol_col", "method", "best_score", "header_row_used")
            if k in meta_in
        }

    return syms, meta


def _select_tasks(args: argparse.Namespace, all_tasks: List[SyncTask]) -> List[SyncTask]:
    # 1) Explicit keys
    if args.keys:
        wanted = {k.strip().upper() for k in args.keys if k and k.strip()}
        return [t for t in all_tasks if t.key.upper() in wanted]

    # 2) Convenience flags
    if args.ksa:
        return [t for t in all_tasks if t.key == "KSA_TADAWUL"]
    if args.global_markets:
        return [t for t in all_tasks if t.key == "GLOBAL_MARKETS"]
    if args.portfolio:
        return [t for t in all_tasks if t.key == "MY_PORTFOLIO"]
    if args.insights:
        return [t for t in all_tasks if t.key == "INSIGHTS_ANALYSIS"]

    # 3) Default all
    return list(all_tasks)


def _call_refresh(
    fn: Callable[..., Any],
    *,
    spreadsheet_id: str,
    sheet_name: str,
    tickers: List[str],
    start_cell: str,
    clear: bool,
) -> Dict[str, Any]:
    """
    Signature-aware refresh call:
    Handles variations like:
      - refresh_sheet_with_enriched_quotes(spreadsheet_id, sheet_name, tickers, ...)
      - refresh_sheet_with_enriched_quotes(sid=..., sheet_name=..., tickers=..., ...)
      - functions that don't accept start_cell
    """
    if not callable(fn):
        return {"status": "error", "error": "refresh function not callable"}

    # Best-effort use inspect.signature (safe fallback if it fails)
    try:
        sig = inspect.signature(fn)
        params = sig.parameters

        kw: Dict[str, Any] = {}

        # spreadsheet id
        if "spreadsheet_id" in params:
            kw["spreadsheet_id"] = spreadsheet_id
        elif "sid" in params:
            kw["sid"] = spreadsheet_id
        elif "sheet_id" in params:
            kw["sheet_id"] = spreadsheet_id

        # sheet name
        if "sheet_name" in params:
            kw["sheet_name"] = sheet_name
        elif "sheet" in params:
            kw["sheet"] = sheet_name
        elif "tab" in params:
            kw["tab"] = sheet_name

        # tickers/symbols
        if "tickers" in params:
            kw["tickers"] = tickers
        elif "symbols" in params:
            kw["symbols"] = tickers

        # optional args
        if "start_cell" in params:
            kw["start_cell"] = start_cell
        if "clear" in params:
            kw["clear"] = clear

        # If function requires positional spreadsheet_id/sheet_name/tickers (rare),
        # but we couldn't detect names, fall back to positional below.
        if kw:
            out = fn(**kw)
            return out if isinstance(out, dict) else {"status": "success", "result": out}

    except Exception:
        pass

    # Fallback attempts (positional + optional kwargs)
    try:
        out = fn(spreadsheet_id, sheet_name, tickers, start_cell=start_cell, clear=clear)
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
    ksa_gateway: str,
) -> Dict[str, Any]:
    """
    KSA page can be fetched from:
    - "enriched" gateway: /v1/enriched/sheet-rows (default)
    - "argaam" gateway:  /v1/argaam/sheet-rows (strict KSA normalization) if sheets_service exposes _refresh_logic
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
            )
        except Exception as e:
            logger.warning("KSA argaam gateway failed; falling back to enriched. err=%s", e)

    # default: enriched quotes endpoint
    return _call_refresh(
        sheets_service.refresh_sheet_with_enriched_quotes,
        spreadsheet_id=spreadsheet_id,
        sheet_name=sheet_name,
        tickers=tickers,
        start_cell=start_cell,
        clear=clear,
    )


# =============================================================================
# Default task map (aligned with your endpoints)
# =============================================================================
def _build_sync_map() -> List[SyncTask]:
    return [
        SyncTask("KSA_TADAWUL", sheets_service.refresh_sheet_with_enriched_quotes, "KSA Tadawul Market", "ksa"),
        SyncTask("GLOBAL_MARKETS", sheets_service.refresh_sheet_with_enriched_quotes, "Global Markets", "enriched"),
        SyncTask("MUTUAL_FUNDS", sheets_service.refresh_sheet_with_enriched_quotes, "Mutual Funds", "enriched"),
        SyncTask("COMMODITIES_FX", sheets_service.refresh_sheet_with_enriched_quotes, "Commodities & FX", "enriched"),
        SyncTask("MY_PORTFOLIO", sheets_service.refresh_sheet_with_enriched_quotes, "My Portfolio", "enriched"),
        SyncTask("MARKET_LEADERS", sheets_service.refresh_sheet_with_enriched_quotes, "Market Leaders", "enriched"),
        SyncTask("INSIGHTS_ANALYSIS", sheets_service.refresh_sheet_with_ai_analysis, "Insights & AI Analysis", "ai"),
        # Optional (uncomment if your backend supports it for a page):
        # SyncTask("ADVANCED_ANALYSIS", sheets_service.refresh_sheet_with_advanced_analysis, "Advanced Analysis", "advanced"),
    ]


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
    progress_pct: Optional[float] = None,
) -> Dict[str, Any]:
    key = task.key
    desc = task.desc

    ptxt = f" | progress={progress_pct:.1f}%" if isinstance(progress_pct, (int, float)) else ""
    logger.info("--- Syncing: %s (%s)%s ---", desc, key, ptxt)

    sheet_name = _resolve_sheet_name(key)
    if not sheet_name:
        msg = f"Sheet name not found for key={key} (PAGE_REGISTRY or settings.* missing)"
        logger.error(msg)
        return {"status": "error", "key": key, "desc": desc, "error": msg}

    # Read symbols
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

    logger.info("Symbols: %s (chosen=%s)", len(symbols), meta.get("chosen_list"))

    start_cell = _validate_start_cell(start_cell)

    if dry_run:
        logger.info("[DRY RUN] Would update sheet='%s' start_cell=%s clear=%s", sheet_name, start_cell, clear)
        return {
            "status": "dry_run",
            "key": key,
            "desc": desc,
            "sheet": sheet_name,
            "count_symbols": len(symbols),
            "start_cell": start_cell,
            "clear": bool(clear),
            "symbols_meta": meta,
            "timestamp_utc": _utc_now_iso(),
        }

    # Perform update
    start_t = time.time()
    try:
        if task.kind == "ksa":
            result = _ksa_refresh_method(
                spreadsheet_id=spreadsheet_id,
                sheet_name=sheet_name,
                tickers=symbols,
                start_cell=start_cell,
                clear=clear,
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
            "duration_sec": round(duration, 3),
            "timestamp_utc": _utc_now_iso(),
            "symbols_meta": meta,
            "ksa_gateway": ksa_gateway if task.kind == "ksa" else None,
            "progress_pct": round(progress_pct, 2) if isinstance(progress_pct, (int, float)) else None,
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

    # KSA mode choice
    parser.add_argument(
        "--ksa-gateway",
        default="enriched",
        choices=["enriched", "argaam"],
        help="KSA fetch gateway: enriched (default) or argaam (strict) if supported",
    )

    # Convenience flags
    parser.add_argument("--ksa", action="store_true", help="Sync only KSA Tadawul")
    parser.add_argument("--global", dest="global_markets", action="store_true", help="Sync only Global Markets")
    parser.add_argument("--portfolio", action="store_true", help="Sync only My Portfolio")
    parser.add_argument("--insights", action="store_true", help="Sync only Insights/AI")

    # Explicit keys
    parser.add_argument("--keys", nargs="*", default=None, help="Run only these PAGE_REGISTRY keys (space-separated)")

    # Logging
    parser.add_argument("--log-level", default="INFO", help="DEBUG, INFO, WARNING, ERROR")

    args = parser.parse_args()

    # Apply log level
    try:
        logging.getLogger().setLevel(getattr(logging, str(args.log_level).upper(), logging.INFO))
    except Exception:
        pass

    sid = _get_spreadsheet_id(args.sheet_id)
    if not sid:
        logger.error("DEFAULT_SPREADSHEET_ID is not set in env.py or environment variables (or pass --sheet-id).")
        sys.exit(1)

    sync_map = _build_sync_map()
    tasks = _select_tasks(args, sync_map)
    if not tasks:
        logger.warning("No matching tasks found.")
        return

    max_tickers = int(args.max_tickers or 0) or None
    start_cell = _validate_start_cell(str(args.start_cell or "A5"))

    logger.info(
        "Dashboard Sync v%s | pages=%s | dry_run=%s | clear=%s | start_cell=%s | ksa_gateway=%s",
        SCRIPT_VERSION,
        len(tasks),
        args.dry_run,
        args.clear,
        start_cell,
        args.ksa_gateway,
    )
    logger.info("Target Spreadsheet ID: %s", sid)

    t0 = time.time()

    results: List[Dict[str, Any]] = []
    failures = 0
    partials = 0
    successes = 0
    skipped = 0

    for idx, task in enumerate(tasks, start=1):
        progress_pct = (idx / max(1, len(tasks))) * 100.0
        logger.info("(%s/%s) %s | %.1f%%", idx, len(tasks), task.desc, progress_pct)

        r = sync_page(
            task,
            spreadsheet_id=sid,
            start_cell=start_cell,
            clear=bool(args.clear),
            dry_run=bool(args.dry_run),
            max_tickers=max_tickers,
            ksa_gateway=str(args.ksa_gateway or "enriched"),
            progress_pct=progress_pct,
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

        if not args.dry_run and idx < len(tasks):
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
        len(tasks),
        total_sec,
    )

    # Write JSON output if requested
    if args.json_out:
        try:
            path = str(args.json_out).strip()
            payload = {
                "version": SCRIPT_VERSION,
                "timestamp_utc": _utc_now_iso(),
                "spreadsheet_id": sid,
                "tasks_total": len(tasks),
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

    # Exit code: non-zero if any failure (useful in CI / scheduled jobs)
    if failures:
        sys.exit(2)


if __name__ == "__main__":
    main()
```
