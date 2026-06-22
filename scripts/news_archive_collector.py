#!/usr/bin/env python3
# =============================================================================
# TADAWUL FAST BRIDGE -- NEWS / SENTIMENT ARCHIVE COLLECTOR  (v1.0.0)
# -----------------------------------------------------------------------------
# WHY THIS EXISTS (Strategy items (b) news->sector and (c) theme rotation):
#   news_intelligence produces per-symbol sentiment / confidence / news_boost /
#   article_velocity / emerging_topics in REAL TIME and then throws it away. You
#   cannot backtest "negative sector news precedes sector underperformance" or
#   "an emerging theme precedes rotation" without a HISTORICAL series of those
#   signals. None exists, because nothing is persisted.
#
#   This collector is the forward-collection CLOCK that fixes that -- exactly the
#   same idea as Performance_Log. It runs daily, snapshots the news signal for a
#   stable universe, and APPENDS one row per symbol per day to a new
#   `News_Archive` tab. After it has accumulated, (b)/(c) become registered
#   hypotheses that run through the SAME run_backtest gate already deployed in
#   track_performance.py -- and only influence a recommendation if they pass.
#
# WHAT IT IS NOT:
#   - It does NOT modify news_intelligence (that is request-path; archiving
#     belongs OFF the request path, here, like every other TFB cron).
#   - It does NOT forecast anything. It records signals so they can be tested.
#   - Its value accrues over MONTHS as the archive fills. Starting it now is the
#     point: every day not collected is a day of history you never get back.
#
# DESIGN:
#   - APPEND-ONLY (never overwrites): a forward clock; each day is preserved.
#   - Headers at row 5, data from row 6 (matches Performance_Log convention).
#   - Idempotent per day: re-running on the same date skips symbols already
#     archived for that Riyadh date (no duplicate rows).
#   - Stable universe (env NEWS_ARCHIVE_UNIVERSE) so each symbol has a
#     continuous series -- better for backtesting than a rotating Top_10.
#   - news_intelligence uses Google-News RSS: NO provider API key required;
#     only GOOGLE_SHEETS_CREDENTIALS + a spreadsheet id are needed.
# =============================================================================

from __future__ import annotations

import argparse
import asyncio
import base64
import json
import logging
import os
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

SCRIPT_VERSION = "1.0.0"
SCRIPT_NAME = "news_archive_collector"

# --- optional gspread (sheet write); script still runs in --dry-run without it
try:
    import gspread  # type: ignore
    from google.oauth2 import service_account  # type: ignore

    GSPREAD_AVAILABLE = True
except Exception:  # pragma: no cover
    gspread = None  # type: ignore
    service_account = None  # type: ignore
    GSPREAD_AVAILABLE = False

LOG_FORMAT = "%(asctime)s | %(levelname)8s | %(name)s | %(message)s"
DATE_FORMAT = "%Y-%m-%d %H:%M:%S"
logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO").strip().upper(),
    format=LOG_FORMAT,
    datefmt=DATE_FORMAT,
)
logger = logging.getLogger("NewsArchive")

_RIYADH_TZ = timezone(timedelta(hours=3))


# -----------------------------------------------------------------------------
# Path + env helpers
# -----------------------------------------------------------------------------
def _ensure_project_root_on_path() -> None:
    """Add this script's dir and its parent to sys.path so the canonical
    `core.*` / bare engine modules import in the cron runner (mirrors
    track_performance.py)."""
    try:
        script_dir = Path(__file__).parent.absolute()
        project_root = script_dir.parent
        for p in (script_dir, project_root):
            ps = str(p)
            if ps and ps not in sys.path:
                sys.path.insert(0, ps)
    except Exception:
        pass


_ensure_project_root_on_path()


def _env_bool(name: str, default: bool = False) -> bool:
    raw = (os.getenv(name, "") or "").strip().lower()
    if not raw:
        return default
    return raw in ("1", "true", "yes", "on", "y", "t")


def _env_int(name: str, default: int, lo: Optional[int] = None, hi: Optional[int] = None) -> int:
    try:
        raw = (os.getenv(name, "") or "").strip()
        v = int(float(raw)) if raw else default
    except Exception:
        v = default
    if lo is not None and v < lo:
        v = lo
    if hi is not None and v > hi:
        v = hi
    return v


def _env_csv(name: str) -> Optional[List[str]]:
    raw = (os.getenv(name, "") or "").strip()
    if not raw:
        return None
    parts = [p.strip() for p in raw.replace(";", ",").split(",")]
    return [p for p in parts if p]


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _riyadh_now() -> datetime:
    return datetime.now(_RIYADH_TZ)


def _riyadh_date() -> str:
    return _riyadh_now().strftime("%Y-%m-%d")


def _safe_float(x: Any, default: float = 0.0) -> float:
    try:
        if x is None or x == "":
            return default
        return float(x)
    except Exception:
        return default


def _col_to_a1(col: int) -> str:
    s = ""
    n = col
    while n > 0:
        n, r = divmod(n - 1, 26)
        s = chr(65 + r) + s
    return s


def _a1_range(c0: int, r0: int, c1: int, r1: int) -> str:
    return f"{_col_to_a1(c0)}{r0}:{_col_to_a1(c1)}{r1}"


# -----------------------------------------------------------------------------
# Minimal retry/backoff (full-jitter), copied small to keep this standalone
# -----------------------------------------------------------------------------
class _Backoff:
    def __init__(self, base: float = 0.5, cap: float = 8.0, tries: int = 5):
        self.base, self.cap, self.tries = base, cap, tries

    def run(self, fn):
        import random

        last = None
        for i in range(self.tries):
            try:
                return fn()
            except Exception as e:  # pragma: no cover - network/sheets timing
                last = e
                if i == self.tries - 1:
                    break
                time.sleep(min(self.cap, self.base * (2 ** i)) * (0.5 + random.random() / 2.0))
        if last:
            raise last


# -----------------------------------------------------------------------------
# News engine import (lazy, tolerant of layout)
# -----------------------------------------------------------------------------
def _import_news_module() -> Optional[Any]:
    for path in ("core.news_intelligence", "news_intelligence"):
        try:
            return __import__(path, fromlist=["batch_news_intelligence"])
        except Exception:
            continue
    return None


# -----------------------------------------------------------------------------
# Archive store (append-only)
# -----------------------------------------------------------------------------
class NewsArchiveStore:
    SHEET_DEFAULT = "News_Archive"
    START_ROW = 5  # headers at row 5
    DATA_ROW0 = 6  # data from row 6
    HEADERS = [
        "Date (Riyadh)",
        "Symbol",
        "Sector",
        "Sentiment",
        "Confidence",
        "News Boost",
        "Article Velocity",
        "Articles Analyzed",
        "Emerging Topics",
        "Sources Used",
        "Collected (UTC)",
        "Engine Version",
    ]

    def __init__(self, spreadsheet_id: str, sheet_name: str = ""):
        self.spreadsheet_id = spreadsheet_id
        self.sheet_name = sheet_name or self.SHEET_DEFAULT
        self.backoff = _Backoff()
        self.gc = None
        self.sheet = None
        self.ws = None
        self._init_sheet()

    def _load_creds(self) -> Optional[Any]:
        raw = (os.getenv("GOOGLE_SHEETS_CREDENTIALS") or os.getenv("GOOGLE_CREDENTIALS") or "").strip()
        if not raw:
            return None
        s = raw
        if not s.startswith("{"):
            try:
                dec = base64.b64decode(s).decode("utf-8", errors="replace").strip()
                if dec.startswith("{"):
                    s = dec
            except Exception:
                pass
        try:
            obj = json.loads(s)
            if isinstance(obj, dict) and service_account is not None:
                return service_account.Credentials.from_service_account_info(
                    obj, scopes=["https://www.googleapis.com/auth/spreadsheets"]
                )
        except Exception:
            return None
        return None

    def _init_sheet(self) -> None:
        if not GSPREAD_AVAILABLE or gspread is None:
            return
        try:
            creds = self._load_creds()
            self.gc = gspread.authorize(creds) if creds else gspread.service_account()
            self.sheet = self.gc.open_by_key(self.spreadsheet_id)
            try:
                self.ws = self.sheet.worksheet(self.sheet_name)
            except Exception:
                self.ws = self.sheet.add_worksheet(title=self.sheet_name, rows=20000, cols=30)
            self.backoff.run(self._ensure_headers)
        except Exception as e:
            logger.error("NewsArchiveStore init failed: %s", e)
            self.gc = self.sheet = self.ws = None

    def _ensure_headers(self) -> None:
        if not self.ws:
            return
        try:
            existing = self.ws.row_values(self.START_ROW)
        except Exception:
            existing = []
        if [str(x).strip() for x in existing[: len(self.HEADERS)]] != self.HEADERS:
            rng = _a1_range(1, self.START_ROW, len(self.HEADERS), self.START_ROW)
            self.backoff.run(lambda: self.ws.update(values=[self.HEADERS], range_name=rng))

    def is_available(self) -> bool:
        return self.ws is not None

    def existing_keys_for_date(self, date_str: str) -> set:
        """Return {symbol} already archived for date_str (idempotency guard)."""
        if not self.ws:
            return set()
        try:
            rows = self.ws.get_all_values()
        except Exception:
            return set()
        out: set = set()
        for row in rows[self.DATA_ROW0 - 1:]:
            if len(row) >= 2 and str(row[0]).strip() == date_str:
                out.add(str(row[1]).strip().upper())
        return out

    def append(self, rows: List[List[Any]]) -> bool:
        if not self.ws or not rows:
            return False
        try:
            self.backoff.run(lambda: self.ws.append_rows(rows, value_input_option="RAW"))
            return True
        except Exception as e:
            logger.error("News_Archive append failed: %s", e)
            return False


# -----------------------------------------------------------------------------
# Universe
# -----------------------------------------------------------------------------
def resolve_universe(cli_universe: Optional[List[str]]) -> List[str]:
    """Stable universe to archive. CLI > env NEWS_ARCHIVE_UNIVERSE > default.
    A STABLE list (not the rotating Top_10) gives each symbol a continuous
    series, which is what a backtest needs. Expand freely over time -- new
    symbols simply start their series from the day they are added."""
    if cli_universe:
        return [s.strip().upper() for s in cli_universe if s.strip()]
    env = _env_csv("NEWS_ARCHIVE_UNIVERSE")
    if env:
        return [s.strip().upper() for s in env if s.strip()]
    # Default: liquid KSA sector representatives + a few US mega-caps so themes
    # and cross-market rotation are both visible from day one.
    return [
        "2222.SR", "1120.SR", "1180.SR", "2010.SR", "7010.SR", "1211.SR",
        "4013.SR", "1010.SR", "2350.SR", "4002.SR", "1150.SR", "2280.SR",
        "AAPL.US", "MSFT.US", "NVDA.US", "JPM.US",
    ]


def _row_from_result(r: Dict[str, Any], date_str: str) -> Optional[List[Any]]:
    sym = str(r.get("symbol") or "").strip().upper()
    if not sym:
        return None
    topics = r.get("emerging_topics") or []
    if isinstance(topics, list):
        topics_s = ", ".join(str(t) for t in topics[:5])
    else:
        topics_s = str(topics)
    sources = r.get("sources_used") or []
    if isinstance(sources, list):
        sources_s = ", ".join(str(s) for s in sources[:6])
    else:
        sources_s = str(sources)
    return [
        date_str,
        sym,
        "",  # Sector -- resolved from the symbol->sector map at backtest time
        round(_safe_float(r.get("sentiment")), 4),
        round(_safe_float(r.get("confidence")), 4),
        round(_safe_float(r.get("news_boost")), 4),
        round(_safe_float(r.get("article_velocity")), 4),
        int(_safe_float(r.get("articles_analyzed"))),
        topics_s,
        sources_s,
        _utc_now().isoformat(),
        str(r.get("version") or ""),
    ]


# -----------------------------------------------------------------------------
# Collect
# -----------------------------------------------------------------------------
async def collect(args: argparse.Namespace) -> int:
    news = _import_news_module()
    if news is None or not hasattr(news, "batch_news_intelligence"):
        logger.error("news_intelligence not importable -- cannot collect")
        return 1

    universe = resolve_universe(args.universe)
    date_str = _riyadh_date()
    logger.info("collecting news sentiment for %d symbols (date %s)", len(universe), date_str)

    items = [{"symbol": s} for s in universe]
    try:
        batch = await news.batch_news_intelligence(items, include_articles=False)
    except Exception as e:
        logger.error("batch_news_intelligence failed: %s", e)
        return 1

    results = list(getattr(batch, "items", None) or [])
    meta = getattr(batch, "meta", {}) or {}
    print(
        "[news_archive] fetched %d/%d (status=%s, errors=%s)"
        % (len(results), len(universe), meta.get("status"), meta.get("errors"))
    )

    rows: List[List[Any]] = []
    for r in results:
        row = _row_from_result(r, date_str)
        if row:
            rows.append(row)

    # show a compact preview
    for row in rows[:8]:
        print(
            "  %-9s sent=%+.3f conf=%.2f boost=%+.3f vel=%.2f topics=[%s]"
            % (row[1], row[3], row[4], row[5], row[6], (row[8] or "")[:48])
        )
    if len(rows) > 8:
        print("  ... (%d more)" % (len(rows) - 8))

    if args.dry_run:
        print("[news_archive] DRY RUN -- %d rows NOT written" % len(rows))
        return 0

    sheet_id = args.sheet_id or os.getenv("DEFAULT_SPREADSHEET_ID") or os.getenv("TRACK_SHEET_ID")
    if not sheet_id:
        logger.error("no spreadsheet id (use --sheet-id or DEFAULT_SPREADSHEET_ID)")
        return 1

    store = NewsArchiveStore(sheet_id, os.getenv("NEWS_ARCHIVE_SHEET", "News_Archive"))
    if not store.is_available():
        logger.error("News_Archive sheet unavailable (gspread/creds?) -- nothing written")
        return 1

    # idempotency: skip symbols already archived for today
    already = store.existing_keys_for_date(date_str)
    fresh = [row for row in rows if str(row[1]).strip().upper() not in already]
    skipped = len(rows) - len(fresh)
    if skipped:
        print("[news_archive] %d already archived for %s -- skipping" % (skipped, date_str))

    if not fresh:
        print("[news_archive] nothing new to write for %s" % date_str)
        return 0

    ok = store.append(fresh)
    print("[news_archive] %s %d rows to %s" % ("wrote" if ok else "FAILED writing", len(fresh), store.sheet_name))
    return 0 if ok else 1


def create_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description=f"TFB News/Sentiment Archive Collector v{SCRIPT_VERSION}")
    p.add_argument("--sheet-id", default=os.getenv("DEFAULT_SPREADSHEET_ID") or os.getenv("TRACK_SHEET_ID") or None,
                   help="Spreadsheet ID (also DEFAULT_SPREADSHEET_ID / TRACK_SHEET_ID env).")
    p.add_argument("--universe", nargs="+", default=None,
                   help="Symbols to archive (space/comma separated). Default: env NEWS_ARCHIVE_UNIVERSE or built-in list.")
    p.add_argument("--dry-run", action="store_true", default=_env_bool("NEWS_ARCHIVE_DRY_RUN", False),
                   help="Fetch + print sentiment but DO NOT write the sheet.")
    p.add_argument("--verbose", "-v", action="store_true", default=_env_bool("NEWS_ARCHIVE_VERBOSE", False),
                   help="Verbose logs.")
    return p


def main() -> int:
    args = create_parser().parse_args()
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)
    if sys.platform == "win32":
        try:
            asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())  # type: ignore[attr-defined]
        except Exception:
            pass
    try:
        return asyncio.run(collect(args))
    except KeyboardInterrupt:
        return 130
    except Exception as e:
        logger.exception("Fatal: %s", e)
        return 1


__all__ = [
    "SCRIPT_VERSION",
    "NewsArchiveStore",
    "resolve_universe",
    "collect",
    "create_parser",
    "main",
]

if __name__ == "__main__":
    raise SystemExit(main())
