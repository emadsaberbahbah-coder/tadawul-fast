# run_dashboard_sync.py
"""
run_dashboard_sync.py
===========================================================
TADAWUL FAST BRIDGE – DASHBOARD SYNCHRONIZER (v2.3.0)
===========================================================

What this script does
1) Connects to your Google Spreadsheet (DEFAULT_SPREADSHEET_ID).
2) Reads symbols from configured tabs via `symbols_reader`.
3) Calls backend endpoints (enriched / analysis / advanced) through `google_sheets_service`.
4) Writes updated {headers, rows} back into the target sheets (chunked, Sheets-safe).

Usage
  python run_dashboard_sync.py
  python run_dashboard_sync.py --dry-run
  python run_dashboard_sync.py --ksa
  python run_dashboard_sync.py --global
  python run_dashboard_sync.py --portfolio
  python run_dashboard_sync.py --insights
  python run_dashboard_sync.py --keys KSA_TADAWUL GLOBAL_MARKETS
  python run_dashboard_sync.py --clear --start-cell A5 --sleep 2

Notes
- Designed to be run from project root, but will self-fix sys.path if possible.
- Never crashes the whole run due to one page; continues best-effort.
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Callable, Dict, List, Optional, Sequence, Tuple

# =============================================================================
# Version / Logging
# =============================================================================

SCRIPT_VERSION = "2.3.0"

LOG_FORMAT = "%(asctime)s | %(levelname)s | %(message)s"
DATE_FORMAT = "%H:%M:%S"

logging.basicConfig(level=logging.INFO, format=LOG_FORMAT, datefmt=DATE_FORMAT)
logger = logging.getLogger("DashboardSync")


# =============================================================================
# Path safety (allow running from subfolders)
# =============================================================================

def _ensure_project_root_on_path() -> None:
    """
    Try to ensure imports work even if the user runs this script from a subdirectory.
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
    # Optional: override endpoint behavior in future
    kind: str = "enriched"  # enriched | ai | advanced


# =============================================================================
# Default task map (aligned with your endpoints)
# =============================================================================

SYNC_MAP: List[SyncTask] = [
    SyncTask("KSA_TADAWUL", sheets_service.refresh_sheet_with_enriched_quotes, "KSA Tadawul Market", "enriched"),
    SyncTask("GLOBAL_MARKETS", sheets_service.refresh_sheet_with_enriched_quotes, "Global Markets", "enriched"),
    SyncTask("MUTUAL_FUNDS", sheets_service.refresh_sheet_with_enriched_quotes, "Mutual Funds", "enriched"),
    SyncTask("COMMODITIES_FX", sheets_service.refresh_sheet_with_enriched_quotes, "Commodities & FX", "enriched"),
    SyncTask("MY_PORTFOLIO", sheets_service.refresh_sheet_with_enriched_quotes, "My Portfolio", "enriched"),
    SyncTask("MARKET_LEADERS", sheets_service.refresh_sheet_with_enriched_quotes, "Market Leaders", "enriched"),
    SyncTask("INSIGHTS_ANALYSIS", sheets_service.refresh_sheet_with_ai_analysis, "Insights & AI Analysis", "ai"),
    # Optional future page:
    # SyncTask("ADVANCED_ANALYSIS", sheets_service.refresh_sheet_with_advanced_analysis, "Advanced Analysis", "advanced"),
]


# =============================================================================
# Helpers
# =============================================================================

def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _get_spreadsheet_id() -> str:
    sid = (getattr(settings, "default_spreadsheet_id", None) or "").strip()
    if not sid:
        # fallback to env var just in case
        sid = (os.getenv("DEFAULT_SPREADSHEET_ID", "") or "").strip()
    return sid


def _safe_int(v: Any, default: int) -> int:
    try:
        return int(v)
    except Exception:
        return default


def _resolve_sheet_name(task_key: str) -> Optional[str]:
    """
    Prefer symbols_reader.PAGE_REGISTRY[key].sheet_name if available.
    Fall back to settings.* (if you use those), otherwise None.
    """
    # 1) symbols_reader registry (preferred)
    try:
        reg = getattr(symbols_reader, "PAGE_REGISTRY", None)
        if reg and task_key in reg:
            cfg = reg.get(task_key)
            # cfg might be an object or dict
            if isinstance(cfg, dict):
                name = (cfg.get("sheet_name") or cfg.get("tab") or cfg.get("name") or "").strip()
                if name:
                    return name
            name = (getattr(cfg, "sheet_name", None) or getattr(cfg, "tab_name", None) or "").strip()
            if name:
                return name
    except Exception:
        pass

    # 2) env settings fallbacks (best-effort)
    # Use the same names you referenced in your other routes/scripts.
    candidates = {
        "KSA_TADAWUL": getattr(settings, "sheet_ksa_tadawul", None),
        "GLOBAL_MARKETS": getattr(settings, "sheet_global_markets", None),
        "MUTUAL_FUNDS": getattr(settings, "sheet_mutual_funds", None),
        "COMMODITIES_FX": getattr(settings, "sheet_commodities_fx", None),
        "MY_PORTFOLIO": getattr(settings, "sheet_my_portfolio", None),
        "MARKET_LEADERS": getattr(settings, "sheet_market_leaders", None),
        "INSIGHTS_ANALYSIS": getattr(settings, "sheet_insights_analysis", None),
    }
    name = (candidates.get(task_key) or "").strip()
    return name or None


def _read_symbols_for_key(task_key: str) -> Tuple[List[str], Dict[str, Any]]:
    """
    Returns (all_symbols, meta) where meta is debug info (ksa/global counts, etc.).
    Supports multiple shapes of symbols_reader output safely.
    """
    data = {}
    try:
        data = symbols_reader.get_page_symbols(task_key)  # type: ignore
    except Exception as e:
        raise RuntimeError(f"symbols_reader.get_page_symbols failed for {task_key}: {e}")

    # Accept list output too
    if isinstance(data, list):
        syms = [str(x).strip() for x in data if str(x).strip()]
        return syms, {"count": len(syms), "source_shape": "list"}

    if not isinstance(data, dict):
        raise RuntimeError(f"Unexpected symbols_reader output type for {task_key}: {type(data)}")

    all_syms = data.get("all")
    if all_syms is None:
        # Some implementations might use "tickers"
        all_syms = data.get("tickers") or data.get("symbols") or []

    all_syms = [str(x).strip() for x in (all_syms or []) if str(x).strip()]

    # counts (best-effort)
    ksa = data.get("ksa") or []
    glob = data.get("global") or []
    meta = {
        "count": len(all_syms),
        "ksa_count": len(ksa) if isinstance(ksa, list) else None,
        "global_count": len(glob) if isinstance(glob, list) else None,
        "source_shape": "dict",
    }
    return all_syms, meta


def _select_tasks(args: argparse.Namespace) -> List[SyncTask]:
    # 1) Explicit keys
    if args.keys:
        wanted = set([k.strip().upper() for k in args.keys if k and k.strip()])
        return [t for t in SYNC_MAP if t.key.upper() in wanted]

    # 2) Convenience flags
    if args.ksa:
        return [t for t in SYNC_MAP if t.key == "KSA_TADAWUL"]
    if args.global_markets:
        return [t for t in SYNC_MAP if t.key == "GLOBAL_MARKETS"]
    if args.portfolio:
        return [t for t in SYNC_MAP if t.key == "MY_PORTFOLIO"]
    if args.insights:
        return [t for t in SYNC_MAP if t.key == "INSIGHTS_ANALYSIS"]

    # 3) Default all
    return list(SYNC_MAP)


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
) -> Dict[str, Any]:
    key = task.key
    desc = task.desc

    logger.info("--- Syncing: %s (%s) ---", desc, key)

    # Resolve sheet name
    sheet_name = _resolve_sheet_name(key)
    if not sheet_name:
        msg = f"Sheet name not found for key={key} (PAGE_REGISTRY or settings.* missing)"
        logger.error(msg)
        return {"status": "error", "key": key, "error": msg}

    # Read symbols
    try:
        symbols, meta = _read_symbols_for_key(key)
    except Exception as e:
        logger.exception("Failed reading symbols for %s", key)
        return {"status": "error", "key": key, "sheet": sheet_name, "error": str(e)}

    if not symbols:
        logger.warning("No symbols found for %s. Skipping.", key)
        return {"status": "skipped", "key": key, "sheet": sheet_name, "reason": "No symbols"}

    if max_tickers and max_tickers > 0 and len(symbols) > max_tickers:
        symbols = symbols[:max_tickers]
        meta["trimmed_to"] = max_tickers

    logger.info("Symbols: %s (meta=%s)", len(symbols), meta)

    if dry_run:
        logger.info("[DRY RUN] Would update sheet='%s' start_cell=%s clear=%s", sheet_name, start_cell, clear)
        return {"status": "dry_run", "key": key, "sheet": sheet_name, "count": len(symbols)}

    # Perform update
    start_t = time.time()
    try:
        result = task.method(
            sid=spreadsheet_id,
            sheet_name=sheet_name,
            tickers=symbols,
            start_cell=start_cell,
            clear=clear,
        )
    except TypeError:
        # Backward compatibility: older google_sheets_service versions may not accept start_cell
        try:
            result = task.method(
                sid=spreadsheet_id,
                sheet_name=sheet_name,
                tickers=symbols,
                clear=clear,
            )
        except Exception as e:
            logger.exception("Update failed for %s (compat path)", key)
            return {"status": "error", "key": key, "sheet": sheet_name, "error": str(e)}
    except Exception as e:
        logger.exception("Update failed for %s", key)
        return {"status": "error", "key": key, "sheet": sheet_name, "error": str(e)}

    duration = time.time() - start_t
    status = (result or {}).get("status")

    if status in ("success", "partial"):
        logger.info("✅ %s: rows=%s cells=%s time=%.2fs (backend=%s)",
                    status.upper(),
                    (result or {}).get("rows_written"),
                    (result or {}).get("cells_updated"),
                    duration,
                    (result or {}).get("backend_status"))
    else:
        logger.error("❌ FAILED: %s", result)

    # Always return a rich meta dict
    out = dict(result or {})
    out.update({
        "key": key,
        "desc": desc,
        "sheet": sheet_name,
        "duration_sec": round(duration, 3),
        "timestamp_utc": _utc_now_iso(),
    })
    return out


# =============================================================================
# CLI / Main
# =============================================================================

def main() -> None:
    parser = argparse.ArgumentParser(description="Synchronize Dashboard Sheets")
    parser.add_argument("--dry-run", action="store_true", help="Read symbols but do not write data")
    parser.add_argument("--clear", action="store_true", help="Clear old values first (safe range) before writing")
    parser.add_argument("--start-cell", default="A5", help="Top-left cell where headers should be written (default A5)")
    parser.add_argument("--sleep", type=float, default=2.0, help="Seconds to sleep between pages (default 2.0)")
    parser.add_argument("--max-tickers", type=int, default=0, help="Cap tickers per page (0 = no cap)")

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

    sid = _get_spreadsheet_id()
    if not sid:
        logger.error("DEFAULT_SPREADSHEET_ID is not set in env.py or environment variables.")
        sys.exit(1)

    tasks = _select_tasks(args)
    if not tasks:
        logger.warning("No matching tasks found.")
        return

    max_tickers = int(args.max_tickers or 0) or None

    logger.info("Dashboard Sync v%s | pages=%s | dry_run=%s | clear=%s | start_cell=%s",
                SCRIPT_VERSION, len(tasks), args.dry_run, args.clear, args.start_cell)
    logger.info("Target Spreadsheet ID: %s", sid)

    results: List[Dict[str, Any]] = []
    failures = 0

    for idx, task in enumerate(tasks, start=1):
        logger.info("(%s/%s) %s", idx, len(tasks), task.desc)
        r = sync_page(
            task,
            spreadsheet_id=sid,
            start_cell=str(args.start_cell or "A5"),
            clear=bool(args.clear),
            dry_run=bool(args.dry_run),
            max_tickers=max_tickers,
        )
        results.append(r)
        if (r.get("status") or "").lower() in ("error", "failed"):
            failures += 1

        if not args.dry_run and idx < len(tasks):
            try:
                time.sleep(max(0.0, float(args.sleep)))
            except Exception:
                pass

    logger.info("=== Dashboard Sync Complete === (failures=%s/%s)", failures, len(tasks))

    # Exit code: non-zero if any failure (useful in CI / scheduled jobs)
    if failures:
        sys.exit(2)


if __name__ == "__main__":
    main()
