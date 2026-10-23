#!/usr/bin/env python3
# =============================================================================
# TADAWUL FAST BRIDGE -- NEWS / SENTIMENT ARCHIVE COLLECTOR  (v1.3.2)
# -----------------------------------------------------------------------------
# WHY THIS EXISTS (Strategy items (b) news->sector and (c) theme rotation):
#   news_intelligence produces per-symbol sentiment / confidence / news_boost /
#   article_velocity / emerging_topics on request. You
#   cannot backtest "negative sector news precedes sector underperformance" or
#   "an emerging theme precedes rotation" without a HISTORICAL series of those
#   signals. This collector preserves forward observations for that testing.
#
#   This collector is the forward-collection CLOCK that fixes that -- exactly the
#   same idea as Performance_Log. It retains a stable baseline and can add
#   bounded, source-validated portfolio/research identities plus rotating
#   eligible/market samples. It APPENDS at most one row per symbol per Riyadh
#   day to `News_Archive`; repeat collections retain fresh evidence artifacts
#   without replacing that day's earlier row. Archived context supports later
#   hypothesis tests; collection alone does not promote a recommendation.
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
#   - CLI/env selections are subject to explicit reviewed legacy-symbol exclusions.
#     No successor is automatically selected. Adaptive mode retains the baseline, gives
#     source-validated portfolio/research identities priority, and rotates a
#     bounded eligible sample with a broader-market sample when room remains.
#   - At most 60 symbol analyses per batch, not 60 provider HTTP requests:
#     configured sources/retries can add requests. The news batch has a
#     600-second timeout; the workflow has a separate 20-minute outer limit.
#   - Scheduled collection slots are 17:00/23:00 Riyadh. They do not change
#     the one-row-per-symbol-per-day archive grain or promise full-market news.
#   - Complete OK/EMPTY acquisitions alone enter the archive. PARTIAL, ERROR,
#     UNAVAILABLE and UNKNOWN remain explicit and retryable in evidence.
#   - Source publication recency (72 hours) validates identity discovery metadata;
#     it is not quote freshness, article recency or investment eligibility.
#   - news_intelligence uses Google-News RSS and configured feeds: no provider API key required;
#     only GOOGLE_SHEETS_CREDENTIALS + a spreadsheet id are needed.
# =============================================================================
# v1.3.2 (2026-10-24) -- STACK INTEGRATION: VERIFIED ARCHIVE READ SHAPES
# WHY: preserve PR #785's unknown/header/schema-state refusal together with
# main's later identity/acquisition truth, created-only initialization and
# non-retried append. A malformed daily-key grid is unavailable, never empty;
# successful batch metadata cannot certify an omitted requested symbol.
# Existing populated or blank archives are never relabelled. No financial,
# selection, scoring or acquisition retry policy is changed.
# =============================================================================
# v1.1.1 (2026-10-17) -- CONFIRMED ARCHIVE STATE BEFORE APPEND
# WHY: failed idempotency reads must not become empty archives, and missing or
# incompatible historical headers must not be overwritten. Preserve canonical
# grid/row shape checks and unknown-state refusal through the later collector.
# =============================================================================
# v1.3.0 (2026-10-23) -- BOUNDED ADAPTIVE NEWS SELECTION
#   Add exact-symbol/company-name discovery, portfolio/research priority,
#   eligible and broader rotation, explicit acquisition/selection evidence,
#   and read-only dry-run discovery. No scoring weights or gates are changed.
# =============================================================================
# v1.1.0 (2026-08-11) -- WRITE-TIME SECTOR JOIN (ENV-GATED, DEFAULT OFF)
# -----------------------------------------------------------------------------
# WHY: every archived row since 2026-06-22 carries a blank Sector -- the v1.0.0
# design deferred the symbol->sector resolution to backtest time (see the
# _row_from_result comment). That deferral makes every future consumer repeat
# the join, and a sector-at-collection snapshot is HISTORICALLY truer: a
# symbol's sector label can change (reclassification, ticker reuse), and the
# backtest wants the label AS OF the observation date, which only write time
# can capture.
# WHAT: after the idempotency filter and before the append, when
# TFB_NEWS_ARCHIVE_SECTOR_JOIN=1 the collector reads Symbol+Sector by header
# from the four pool pages of the SAME workbook it is writing to and fills
# each fresh row's Sector cell from that map. FAIL-OPEN at every step: page
# read errors, an empty map, or an unknown symbol leave Sector blank exactly
# as v1.0.0 wrote it -- the join can never block or corrupt collection.
# ENV: TFB_NEWS_ARCHIVE_SECTOR_JOIN (unset/0 => byte-identical v1.0.0 rows).
# This script runs in the GitHub Actions sync context, so the flag belongs in
# the WORKFLOW env (TFB ENV-placement rule), not the Render dashboard.
# ZERO functions removed. Additions: _env_sector_join_enabled,
# _sector_map_from_values, NewsArchiveStore.load_symbol_sector_map.
# =============================================================================

from __future__ import annotations

import argparse
import asyncio
import base64
import hashlib
import json
import logging
import os
import re
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

SCRIPT_VERSION = "1.3.2"
SCRIPT_NAME = "news_archive_collector"

# --- optional gspread (sheet write); script still runs in --dry-run without it
try:
    import gspread  # type: ignore
    from gspread.exceptions import WorksheetNotFound as _WorksheetNotFound  # type: ignore
    from google.oauth2 import service_account  # type: ignore

    GSPREAD_AVAILABLE = True
except Exception:  # pragma: no cover
    gspread = None  # type: ignore
    service_account = None  # type: ignore
    GSPREAD_AVAILABLE = False
    class _WorksheetNotFound(Exception):
        """Unavailable-client placeholder; never inferred from other errors."""

LOG_FORMAT = "%(asctime)s | %(levelname)8s | %(name)s | %(message)s"
DATE_FORMAT = "%Y-%m-%d %H:%M:%S"
logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO").strip().upper(),
    format=LOG_FORMAT,
    datefmt=DATE_FORMAT,
)
logger = logging.getLogger("NewsArchive")

_RIYADH_TZ = timezone(timedelta(hours=3))

# v1.1.0: the four pool pages whose Symbol+Sector columns feed the write-time
# sector join. Resolve columns by their headers: canonical Sector is col G,
# while col C is Asset Class and must never be archived as a sector.
_SECTOR_JOIN_PAGES = ("Market_Leaders", "Global_Markets",
                      "Commodities_FX", "Mutual_Funds")


def _env_sector_join_enabled() -> bool:
    """v1.1.0 [SECTOR JOIN] kill-switch -- DEFAULT OFF. Unset/0 keeps the
    archive rows byte-identical to v1.0.0 (Sector blank)."""
    return (os.getenv("TFB_NEWS_ARCHIVE_SECTOR_JOIN", "0") or "0").strip().lower() \
        in ("1", "true", "yes", "on")


def _sector_map_from_values(values_by_page: Dict[str, List[List[Any]]]) -> Dict[str, str]:
    """{UPPER(symbol): sector} from per-page header-bearing A1:H grids.
    First page wins on duplicates (Market_Leaders is authoritative for .SR).
    Blank symbols/sectors are skipped; never raises."""
    out: Dict[str, str] = {}
    for page in _SECTOR_JOIN_PAGES:
        try:
            grid = values_by_page.get(page) or []
            if not grid:
                continue
            headers = [str(value or "").strip().lower() for value in grid[0]]
            if headers.count("symbol") != 1 or headers.count("sector") != 1:
                continue
            symbol_col, sector_col = headers.index("symbol"), headers.index("sector")
            for row in grid[1:]:
                if not row:
                    continue
                sym = str(row[symbol_col] or "").strip().upper() if len(row) > symbol_col else ""
                sec = str(row[sector_col] or "").strip() if len(row) > sector_col else ""
                if sym and sec and sym not in out:
                    out[sym] = sec
        except Exception:
            continue
    return out


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
class NewsArchiveStateError(RuntimeError):
    """Archive state could not be confirmed safely before an append."""


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

    def __init__(self, spreadsheet_id: str, sheet_name: str = "", *, read_only: bool = False):
        self.spreadsheet_id = spreadsheet_id
        self.sheet_name = sheet_name or self.SHEET_DEFAULT
        self.read_only = read_only
        self.backoff = _Backoff()
        self.gc = None
        self.sheet = None
        self.ws = None
        self.init_error_code = None
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
            self.init_error_code = "archive_client_unavailable"
            return
        try:
            creds = self._load_creds()
            self.gc = gspread.authorize(creds) if creds else gspread.service_account()
            self.sheet = self.gc.open_by_key(self.spreadsheet_id)
            if self.read_only:
                return
            created = False
            try:
                self.ws = self.sheet.worksheet(self.sheet_name)
            except _WorksheetNotFound:
                self.ws = self.sheet.add_worksheet(title=self.sheet_name, rows=20000, cols=30)
                created = True
            except Exception:
                raise RuntimeError("archive_lookup_failed") from None
            self._ensure_headers(created=created)
        except Exception as e:
            self.init_error_code = (str(e) if str(e) in {
                "archive_lookup_failed", "archive_header_read_failed", "archive_header_unverified"}
                else "archive_init_failed")
            logger.error("NewsArchiveStore initialization withheld (%s)", self.init_error_code)
            self.gc = self.sheet = self.ws = None

    @staticmethod
    def _header_cells(row: Any) -> list[str]:
        if not isinstance(row, list):
            raise NewsArchiveStateError("archive_header_unverified")
        cells = [str(value if value is not None else "").strip() for value in row]
        while cells and not cells[-1]:
            cells.pop()
        return cells

    def _header_matches(self, values: List[Any]) -> bool:
        # Main's case-insensitive canonical-header compatibility is retained;
        # malformed response shapes are never interpreted as a blank header.
        try:
            header = self._header_cells(values)
        except NewsArchiveStateError:
            return False
        return [value.casefold() for value in header] == [value.casefold() for value in self.HEADERS]

    def _read_archive_values(self) -> list[list[Any]]:
        if self.ws is None:
            raise NewsArchiveStateError("archive_key_read_failed")
        try:
            rows = self.backoff.run(lambda: self.ws.get_all_values())
        except Exception:
            raise NewsArchiveStateError("archive_key_read_failed") from None
        if not isinstance(rows, list) or any(not isinstance(row, list) for row in rows):
            raise NewsArchiveStateError("archive_key_read_failed")
        return rows

    def _ensure_headers(self, *, created: bool = False) -> None:
        """Initialize a proven new sheet; verify existing sheets without writes."""
        if not self.ws:
            raise RuntimeError("archive_header_unverified")
        if created:
            rng = _a1_range(1, self.START_ROW, len(self.HEADERS), self.START_ROW)
            # Fixed-range initialization is idempotent, unlike row append.
            self.backoff.run(lambda: self.ws.update(values=[self.HEADERS], range_name=rng))
        try:
            existing = self.backoff.run(lambda: self.ws.row_values(self.START_ROW))
        except Exception:
            raise RuntimeError("archive_header_read_failed") from None
        if not self._header_matches(existing):
            raise RuntimeError("archive_header_unverified")

    def load_symbol_sector_map(self) -> Dict[str, str]:
        """v1.1.0: Symbol->Sector from the pool pages of THIS workbook.
        FAIL-OPEN: any per-page error skips that page; total failure returns
        {} and the caller leaves Sector blank (v1.0.0 behavior)."""
        values_by_page: Dict[str, List[List[Any]]] = {}
        if not self.sheet:
            return {}
        for page in _SECTOR_JOIN_PAGES:
            try:
                ws = self.sheet.worksheet(page)
                values_by_page[page] = self.backoff.run(
                    lambda w=ws: w.get_values("A1:H")) or []
            except Exception as e:
                logger.warning("sector join: page %s unreadable (%s)", page, e)
        return _sector_map_from_values(values_by_page)

    def load_news_source_context(self) -> Dict[str, Any]:
        """Read bounded identity columns only; no quantities, cash or orders.

        Publication metadata and read timestamps are separate evidence. These
        are source-validated names, not an independent market identity audit.
        A failed source read is retained as incomplete discovery.
        """
        pages = (*_SECTOR_JOIN_PAGES, "My_Portfolio", "Top_10_Investments", "Investable Symbol")
        observed: Dict[str, Any] = {}
        identities: Dict[str, str] = {}
        ambiguous: set = set()
        pools: List[str] = []
        portfolio: List[str] = []
        candidates: List[str] = []
        eligible: List[str] = []
        complete = True
        invalid_identity_rows = 0
        unknown_identities: List[str] = []
        from core.news_identity_lifecycle import load_news_lifecycle
        lifecycle, lifecycle_registry = load_news_lifecycle(now=_utc_now())
        lifecycle_exclusions: Dict[str, Any] = {}

        def publication() -> Dict[str, Any]:
            if self.sheet is None:
                raise RuntimeError("source_unavailable")
            grid = self.backoff.run(lambda: self.sheet.worksheet("_Status").get_values(
                "A1:D20", value_render_option="UNFORMATTED_VALUE"))
            if not grid or [str(v).strip().lower() for v in grid[0][:3]] != ["page", "last updated", "status"]:
                raise RuntimeError("status_header_unknown")
            result = {}
            for row in grid[1:]:
                if row and str(row[0]) in pages:
                    page = str(row[0])
                    if page in result:
                        raise RuntimeError("status_page_ambiguous")
                    stamp = _source_publication_time(row[1] if len(row) > 1 else None)
                    status = str(row[2] if len(row) > 2 else "").strip().upper()
                    result[page] = {"published_utc": stamp, "status": status,
                                    "run": _source_run(row[3] if len(row) > 3 else "",
                                                       research=page == "Top_10_Investments")}
            return result

        try:
            before = publication()
        except Exception:
            before = {}
            complete = False
        for page in pages:
            started = _utc_now().isoformat()
            entry: Dict[str, Any] = {"page": page, "read_started_utc": started}
            try:
                if self.sheet is None:
                    raise RuntimeError("source_unavailable")
                ws = self.sheet.worksheet(page)
                limit = 120 if page == "Top_10_Investments" else 10001
                row_limit = min(max(1, int(ws.row_count)), limit)
                if page == "Top_10_Investments":
                    # The decision cockpit keeps monetary KPIs above row 18.
                    # Discover the Rank header below that control panel, then
                    # read only its bounded Rank/Symbol/Name block.
                    discovery_range = f"A18:A{min(80, int(ws.row_count))}"
                    labels = self.backoff.run(lambda: ws.get_values(discovery_range)) or []
                    header_offset = next((i for i, row in enumerate(labels)
                                          if row and str(row[0]).strip().lower() == "rank"), None)
                    if header_offset is None:
                        raise RuntimeError("research_identity_header_unknown")
                    header_row = 18 + header_offset
                    source_range = f"A{header_row}:C{min(header_row + 22, int(ws.row_count))}"
                    entry["header_discovery_range"] = discovery_range
                else:
                    source_range = f"A1:B{row_limit}"
                grid = self.backoff.run(lambda w=ws, r=source_range: w.get_values(r)) or []
                diagnostics: Dict[str, Any] = {}
                pairs, header_ok, block_complete = _identity_pairs(
                    grid, research=page == "Top_10_Investments", diagnostics=diagnostics,
                    lifecycle=lifecycle)
                for excluded in diagnostics.get("lifecycle_exclusions", []):
                    previous = lifecycle_exclusions.get(excluded["symbol"])
                    if previous is None or (previous["status"] == "VERIFIED_LEGACY"
                                            and excluded["status"] != "VERIFIED_LEGACY"):
                        lifecycle_exclusions[excluded["symbol"]] = excluded
                source_ok = header_ok and block_complete
                # row_count is allocated capacity, not populated extent. With
                # the canonical contiguous writer, only a populated capped
                # boundary leaves a possible unread tail. Spare grid rows do
                # not make an otherwise complete identity read incomplete.
                boundary_populated = (len(grid) >= row_limit and bool(grid) and
                                      any(str(value or "").strip() for value in grid[-1]))
                capped = (page != "Top_10_Investments" and
                          int(ws.row_count) > limit and boundary_populated)
                if capped:
                    source_ok = False
                entry.update({"range": source_range, "header_verified": header_ok,
                              "status": "OK" if source_ok else "INCOMPLETE",
                              "named_rows": len(pairs), "read_cap_reached": capped,
                              **diagnostics})
                invalid_identity_rows += diagnostics.get("invalid_identity_rows", 0)
                unknown_identities.extend(diagnostics.get("unknown_identity_symbols") or [])
                complete = complete and source_ok
                conflicts = []
                for symbol, name in pairs:
                    previous = identities.get(symbol)
                    if previous and " ".join(previous.casefold().split()) != " ".join(name.casefold().split()):
                        ambiguous.add(symbol)
                        conflicts.append(symbol)
                    else:
                        identities[symbol] = name
                    if page in _SECTOR_JOIN_PAGES:
                        pools.append(symbol)
                    elif page == "My_Portfolio":
                        portfolio.append(symbol)
                    elif page == "Investable Symbol":
                        eligible.append(symbol)
                    else:
                        candidates.append(symbol)
                entry["conflicting_identity_rows"] = len(conflicts)
                if page != "My_Portfolio":
                    entry["conflicting_identity_symbols"] = sorted(set(conflicts))[:20]
                else:
                    entry.pop("unknown_identity_symbols", None)
                    entry.pop("lifecycle_exclusions", None)
            except Exception:
                entry.update({"status": "UNAVAILABLE", "error_code": "identity_source_read_failed"})
                complete = False
            entry["read_finished_utc"] = _utc_now().isoformat()
            observed[page] = entry
        try:
            after = publication()
        except Exception:
            after = {}
            complete = False
        for page in pages:
            publication_page = "Global_Markets" if page == "Investable Symbol" else page
            meta = after.get(publication_page) or {}
            stable = bool(before.get(publication_page)) and before.get(publication_page) == meta
            stamp = meta.get("published_utc")
            fresh = bool(stamp) and 0 <= (_utc_now() - datetime.fromisoformat(stamp)).total_seconds() <= 72 * 3600
            usable = meta.get("status") in {"OK", "SUCCESS"}
            observed[page].update({"publication": meta, "publication_stable": stable,
                                   "publication_recent": fresh})
            if page == "Investable Symbol":
                observed[page]["publication_source"] = "Global_Markets (derived identity list)"
            complete = complete and stable and fresh and usable and bool(meta.get("run"))
        for symbol in ambiguous:
            identities.pop(symbol, None)
        identity_complete = invalid_identity_rows == 0 and not ambiguous and lifecycle_registry["status"] == "VALID"
        return {"identities": identities, "ambiguous_symbols": sorted(ambiguous),
                # Public identity diagnostics are global, never attributed to
                # a portfolio role or accompanied by positions/cash/orders.
                "unknown_identity_symbols": _unique_symbols(unknown_identities + sorted(ambiguous))[:20],
                "pool": _unique_symbols(pools), "portfolio_membership": _unique_symbols(portfolio),
                "research": _unique_symbols(candidates), "eligible": _unique_symbols(eligible),
                "source_read_complete": bool(complete), "identity_coverage_complete": identity_complete,
                "complete": bool(complete and identity_complete),
                "lifecycle_registry": lifecycle_registry,
                "lifecycle_exclusions": list(lifecycle_exclusions.values()),
                "read_scope": "Bounded identity windows under the contiguous canonical writer contract; not a full allocated-grid or sparse late-row audit.",
                "sources": list(observed.values())}

    def is_available(self) -> bool:
        return self.ws is not None

    def existing_keys_for_date(self, date_str: str) -> set:
        """Read proven daily keys; unavailable reads are not an empty archive."""
        rows = self._read_archive_values()
        header_row = self.DATA_ROW0 - 2
        if not self._header_matches(rows[header_row] if len(rows) > header_row else []):
            raise RuntimeError("archive_header_unverified")
        out: set = set()
        for row in rows[self.DATA_ROW0 - 1:]:
            if len(row) >= 2 and str(row[0]).strip() == date_str:
                out.add(str(row[1]).strip().upper())
        return out

    def append(self, rows: List[List[Any]]) -> bool:
        if not self.ws or not rows:
            return False
        try:
            # Append is not idempotent: a timeout may follow a committed write.
            # Do not blindly retry it. The next invocation must re-read keys.
            self.ws.append_rows(rows, value_input_option="RAW")
            return True
        except Exception:
            logger.error("News_Archive append outcome unknown; re-read keys before retry")
            return False


# -----------------------------------------------------------------------------
# Universe
# -----------------------------------------------------------------------------
def _unique_symbols(values: List[Any]) -> List[str]:
    out: List[str] = []
    for value in values:
        for token in re.split(r"[\s,;]+", str(value or "").strip().upper()):
            if token and token not in out:
                out.append(token)
    return out


def _source_publication_time(value: Any) -> Optional[str]:
    """Normalize source publication metadata without confusing it with read time."""
    try:
        text = str(value or "").strip()
        if re.fullmatch(r"\d{5}(?:\.\d+)?", text):
            stamp = datetime(1899, 12, 30, tzinfo=_RIYADH_TZ) + timedelta(days=float(text))
        else:
            stamp = datetime.fromisoformat(text.replace("Z", "+00:00"))
            if stamp.tzinfo is None:
                stamp = stamp.replace(tzinfo=_RIYADH_TZ)
        return stamp.astimezone(timezone.utc).isoformat()
    except (ValueError, TypeError, OverflowError):
        return None


def _source_run(message: Any, *, research: bool = False) -> Optional[str]:
    """Accept one explicit source-run receipt, not 'Last run YYYY-MM-DD'."""
    text = str(message or "")
    tokens = (re.findall(r"\bsource readiness VERIFIED\s*\(\s*run\s+([^)]+)\)", text)
              if research else re.findall(r"\brun=([^\s,;|)]+)", text))
    if len(tokens) != 1 or not re.fullmatch(r"[1-9]\d*", tokens[0].strip()):
        return None
    return tokens[0].strip()


def _identity_pairs(grid: List[List[Any]], *, research: bool = False,
                    diagnostics: Optional[Dict[str, Any]] = None,
                    lifecycle: Optional[Dict[str, Any]] = None):
    """Read canonical identity columns or the first research-selected block.

    A portfolio row proves membership only, not a positive holding. Research
    membership includes grace rows and does not assert executable eligibility.
    """
    start = 0
    if research:
        start = next((i for i, row in enumerate(grid)
                      if [str(v).strip().lower() for v in row[:3]] == ["rank", "symbol", "name"]), -1)
        if start < 0:
            return [], False, False
        symbol_col, name_col = 1, 2
    else:
        if not grid or [str(v).strip().lower() for v in grid[0][:2]] != ["symbol", "name"]:
            return [], False, False
        symbol_col, name_col = 0, 1
    pairs = []
    ended = not research
    invalid = 0
    unknown = []
    excluded = []
    for row in grid[start + 1:]:
        symbol = str(row[symbol_col] or "").strip().upper() if len(row) > symbol_col else ""
        name = str(row[name_col] or "").strip() if len(row) > name_col else ""
        if research and ((not symbol and not name) or symbol == "SYMBOL"):
            ended = True
            break
        if not symbol and not name:
            continue
        valid = (symbol not in {"SYMBOL", "TICKER", "NAME", "RANK"}
                 and bool(re.fullmatch(r"[A-Z0-9^][A-Z0-9.^=_-]{0,40}", symbol)))
        if valid and symbol in (lifecycle or {}):
            record = dict(lifecycle[symbol])
            if name and " ".join(name.casefold().split()) != " ".join(record["historical_name"].casefold().split()):
                record.update(status="UNKNOWN", reason="current_caption_conflict")
            excluded.append(record)
            if record["status"] != "VERIFIED_LEGACY":
                invalid += 1
                unknown.append(symbol)
            continue
        if (valid and name and len(name) <= 300 and any(char.isalpha() for char in name)
                and not re.search(r"[\x00-\x1f]", name)
                and name.casefold() not in {symbol.casefold(), "unknown", "n/a", "none", "—"}):
            pairs.append((symbol, name))
        else:
            invalid += 1
            if valid:
                unknown.append(symbol)
    if diagnostics is not None:
        diagnostics["invalid_identity_rows"] = invalid
        diagnostics["unknown_identity_symbols"] = _unique_symbols(unknown)[:20]
        if excluded:
            diagnostics["lifecycle_excluded_rows"] = len(excluded)
            diagnostics["lifecycle_exclusions"] = excluded
    return pairs, True, ended


def _select_news_universe(args: argparse.Namespace, context: Optional[Dict[str, Any]],
                          date_str: str):
    """Bound provider work while preserving exact-symbol identities and overrides.

    CLI determines the requested set, subject to lifecycle exclusions. With adaptive mode off, env remains the historical
    override. In explicitly enabled adaptive mode, env is the stable baseline
    augmented by current source identities and a repeatable daily pool slice.
    """
    cli = _unique_symbols(args.universe or [])
    from core.news_identity_lifecycle import load_news_lifecycle
    lifecycle, lifecycle_registry = load_news_lifecycle(now=_utc_now())
    adaptive = bool(getattr(args, "adaptive", False)) and not cli
    budget = max(1, min(60, int(getattr(args, "max_symbols", 60))))
    baseline = _unique_symbols(resolve_universe(None))
    known = (context or {}).get("identities") or {}
    pool = sorted(_unique_symbols((context or {}).get("pool") or []))
    eligible = sorted(_unique_symbols((context or {}).get("eligible") or []))
    slot = 0 if _utc_now().astimezone(_RIYADH_TZ).hour < 23 else 1
    slot_ordinal = datetime.fromisoformat(date_str).date().toordinal() * 2 + slot
    total_requested = 0
    rotation_count = 0
    eligible_rotation_count = 0
    broad_rotation_count = 0
    legacy_priority = []
    if cli:
        mode, proposed = "cli", cli
    elif adaptive:
        mode = "adaptive"
        priority = _unique_symbols((context or {}).get("portfolio_membership", []) +
                                   (context or {}).get("research", []) + baseline)
        legacy_priority = [symbol for symbol in priority if symbol in lifecycle]
        priority = [symbol for symbol in priority if symbol not in lifecycle]
        eligible_remaining = [symbol for symbol in eligible
                              if symbol not in priority and symbol in known and symbol not in lifecycle]
        room = max(0, budget - len(priority))
        start = slot_ordinal * max(1, room) % max(1, len(eligible_remaining))
        rotation = (eligible_remaining[start:] + eligible_remaining[:start])[:room]
        eligible_rotation_count = len(rotation)
        spare = room - len(rotation)
        remaining = [symbol for symbol in pool if symbol not in priority and symbol not in rotation
                     and symbol in known and symbol not in lifecycle]
        broad_start = slot_ordinal * max(1, spare) % max(1, len(remaining))
        broad = (remaining[broad_start:] + remaining[:broad_start])[:spare]
        rotation += broad
        broad_rotation_count = len(broad)
        rotation_count = len(rotation)
        proposed = priority + rotation
    else:
        mode, proposed = ("env" if _env_csv("NEWS_ARCHIVE_UNIVERSE") else "static"), baseline
    requested_before_lifecycle = len(proposed) + len(legacy_priority)
    excluded = {record["symbol"]: record for record in
                (context or {}).get("lifecycle_exclusions", [])}
    for symbol in proposed + legacy_priority:
        if symbol in lifecycle and symbol not in excluded:
            excluded[symbol] = lifecycle[symbol]
    proposed = [symbol for symbol in proposed if symbol not in lifecycle]
    total_requested = len(proposed)
    universe = proposed[:budget]
    missing = [symbol for symbol in universe if adaptive and symbol not in known]
    items = [{"symbol": symbol, **({"company_name": known[symbol]} if adaptive else {})}
             for symbol in universe if symbol not in missing]
    if lifecycle_registry["status"] != "VALID":
        items = []
    complete = bool(universe) and not missing and total_requested <= budget
    complete = (complete and lifecycle_registry["status"] == "VALID"
                and all(record["status"] == "VERIFIED_LEGACY" for record in excluded.values()))
    if adaptive:
        complete = complete and bool((context or {}).get("complete"))
    evidence = {"mode": mode, "adaptive_enabled": adaptive, "analysis_cap": budget,
                "budget_note": "Cap applies to symbol analyses, not provider HTTP requests; sources and retries add requests.",
                "scope": "bounded selected set; not full-market news coverage",
                "selected_count": len(universe), "queried_count": len(items),
                "missing_identity_symbols": missing, "omitted_priority_count": max(0, total_requested - budget),
                "unknown_identity_symbols": (context or {}).get("unknown_identity_symbols", []) if adaptive else [],
                "rotation_count": rotation_count, "pool_identity_count": len(pool),
                "eligible_pool_identity_count": len(eligible),
                "eligible_rotation_count": eligible_rotation_count,
                "broader_rotation_count": broad_rotation_count,
                "rotation_slot": f"{date_str}/{'17:00' if slot == 0 else '23:00'} Asia/Riyadh",
                "discovery_complete": bool(complete),
                "source_read_complete": (context or {}).get("source_read_complete") if adaptive else None,
                "discovery_identity_coverage_complete": (context or {}).get("identity_coverage_complete") if adaptive else None,
                "identity_read_scope": (context or {}).get("read_scope") if adaptive else None,
                "cohort_sha256": hashlib.sha256("\n".join(universe).encode()).hexdigest(),
                "sources": (context or {}).get("sources", []) if adaptive else []}
    evidence.update(lifecycle_registry=lifecycle_registry,
                    requested_before_lifecycle=requested_before_lifecycle,
                    lifecycle_exclusions=list(excluded.values()),
                    lifecycle_excluded_count=len(excluded),
                    interpretation="Legacy symbols are excluded from current-symbol news only; source rows/history remain. Successors require separate identity validation; no automatic remapping.")
    return universe, items, evidence


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
        "",  # Sector -- backtest-time resolution by default; v1.1.0 fills it
            # at write time when TFB_NEWS_ARCHIVE_SECTOR_JOIN=1 (see collect())
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


def _collection_status(result: Dict[str, Any]) -> str:
    return str((result.get("collection_evidence") or {}).get("status") or "UNKNOWN").upper()


def _collection_report(universe: List[str], results: List[Dict[str, Any]],
                       date_str: str) -> Dict[str, Any]:
    """Bounded acquisition evidence, separate from the unchanged archive schema.

    Preserve public article metadata only, never snippets, feed URLs or raw
    exceptions. Missing acquisition evidence must remain UNKNOWN, not neutral.
    """
    by_symbol = {str(r.get("symbol") or "").strip().upper(): r for r in results}
    observations = []
    for symbol in universe:
        result = by_symbol.get(symbol, {})
        provenance = []
        for article in (result.get("articles") or [])[:5]:
            provenance.append({k: article.get(k) for k in (
                "title", "url", "source_domain", "transport_domain",
                "publisher_url", "published_utc", "crawled_utc") if article.get(k)})
        observations.append({
            "symbol": symbol, "status": _collection_status(result),
            "articles_analyzed": result.get("articles_analyzed", 0),
            "cached": bool(result.get("cached", False)),
            "collection_evidence": result.get("collection_evidence") or {},
            "article_provenance": provenance,
        })
    counts: Dict[str, int] = {}
    for observation in observations:
        status = observation["status"]
        counts[status] = counts.get(status, 0) + 1
    complete = all(o["status"] in {"OK", "EMPTY"} for o in observations)
    return {"schema": "tfb.news-acquisition.v1", "collector_version": SCRIPT_VERSION,
            "date_riyadh": date_str, "generated_utc": _utc_now().isoformat(),
            "requested": len(universe), "returned": len(results),
            "complete": complete, "status_counts": counts, "symbols": observations,
            "interpretation": "News context only; collection success is not prediction accuracy."}


def _write_collection_report(path: Optional[str], report: Dict[str, Any]) -> None:
    if path:
        target = Path(path)
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(json.dumps(report, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")


def _validated_news_results(raw, items):
    """Only exact, uniquely returned queried symbols may reach the archive."""
    from collections import Counter
    requested = {item["symbol"] for item in items}
    counts = Counter()
    matched = []
    unexpected = malformed = 0
    for result in raw:
        if not isinstance(result, dict) or not isinstance(result.get("symbol"), str):
            malformed += 1
            continue
        symbol = result["symbol"].strip().upper()
        if symbol not in requested:
            unexpected += 1
            continue
        counts[symbol] += 1
        matched.append({**result, "symbol": symbol})
    duplicates = sum(count > 1 for count in counts.values())
    accepted = [result for result in matched if counts[result["symbol"]] == 1]
    return accepted, {"raw_returned": len(raw), "accepted_returned": len(accepted),
                      "unexpected_count": unexpected, "malformed_count": malformed,
                      "duplicate_symbol_count": duplicates,
                      "complete": not (unexpected or malformed or duplicates)}


def _finish_collection_report(args: argparse.Namespace, report: Dict[str, Any],
                              status: str, *, error_code: Optional[str] = None,
                              attempted_rows: int = 0, accepted_rows: int = 0,
                              skipped_rows: int = 0) -> None:
    """Keep acquisition evidence and emit bounded nonfinancial adoption counts."""
    write = {"status": status, "attempted_rows": attempted_rows,
             "accepted_rows": accepted_rows, "skipped_rows": skipped_rows}
    if error_code:
        write["error_code"] = error_code
        report["complete"] = False
    report["archive_write"] = write
    _write_collection_report(getattr(args, "evidence_json", None), report)
    selection = report.get("selection") or {}
    counts = {key: int(value) for key, value in (report.get("status_counts") or {}).items()
              if key in {"OK", "EMPTY", "PARTIAL", "ERROR", "UNAVAILABLE", "UNKNOWN"}}
    summary = {"schema": "tfb.news-acquisition-summary.v1",
               "selection": {key: selection.get(key) for key in (
                   "mode", "analysis_cap", "selected_count", "queried_count",
                   "rotation_count", "source_read_complete",
                   "discovery_identity_coverage_complete", "discovery_complete")},
               "selected_acquisition_complete": bool(report.get("selected_acquisition_complete")),
               "complete": bool(report.get("complete")), "status_counts": counts,
               "archive_write": write}
    print("[news_archive] evidence " + json.dumps(summary, sort_keys=True))


# -----------------------------------------------------------------------------
# Collect
# -----------------------------------------------------------------------------
async def collect(args: argparse.Namespace) -> int:
    news = _import_news_module()
    if news is None or not hasattr(news, "batch_news_intelligence"):
        logger.error("news_intelligence not importable -- cannot collect")
        return 1

    date_str = _riyadh_date()
    adaptive = bool(getattr(args, "adaptive", False)) and not _unique_symbols(args.universe or [])
    context = None
    sheet_id = args.sheet_id or os.getenv("DEFAULT_SPREADSHEET_ID") or os.getenv("TRACK_SHEET_ID")
    if adaptive:
        context = {"complete": False, "identities": {}, "sources": []}
        if sheet_id:
            source_store = NewsArchiveStore(sheet_id, os.getenv("NEWS_ARCHIVE_SHEET", "News_Archive"), read_only=True)
            context = source_store.load_news_source_context()
    universe, items, selection = _select_news_universe(args, context, date_str)
    logger.info("collecting news sentiment for %d symbols (date %s)", len(universe), date_str)

    try:
        timeout = max(30, min(600, int(getattr(args, "timeout_seconds", 600))))
        batch = await asyncio.wait_for(news.batch_news_intelligence(items, include_articles=True), timeout=timeout)
    except Exception as e:
        code = "news_batch_timeout" if isinstance(e, (TimeoutError, asyncio.TimeoutError)) else "news_batch_failed"
        failed = _collection_report(universe, [], date_str)
        failed.update({"selection": selection, "selected_acquisition_complete": False,
                       "complete": False, "error_code": code})
        _finish_collection_report(args, failed, "NOT_ATTEMPTED", error_code=code)
        logger.error("batch_news_intelligence failed (%s)", code)
        return 1

    results, response_validation = _validated_news_results(list(getattr(batch, "items", None) or []), items)
    report = _collection_report(universe, results, date_str)
    report["response_validation"] = response_validation
    report["complete"] = report["complete"] and response_validation["complete"]
    report["selection"] = selection
    report["selected_acquisition_complete"] = report["complete"]
    report["complete"] = report["complete"] and selection["discovery_complete"]
    _write_collection_report(getattr(args, "evidence_json", None), report)
    incomplete = not report["complete"]
    meta = getattr(batch, "meta", {}) or {}
    print(
        "[news_archive] fetched %d/%d (status=%s, errors=%s)"
        % (len(results), len(universe), meta.get("status"), meta.get("errors"))
    )

    rows: List[List[Any]] = []
    for r in results:
        if _collection_status(r) not in {"OK", "EMPTY"}:
            # Do not lock in a failed fetch as a neutral daily sample. Its key
            # stays unarchived, so a subsequent run can recover that symbol.
            continue
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
        _finish_collection_report(args, report, "DRY_RUN")
        return 1 if incomplete else 0

    if not sheet_id:
        logger.error("no spreadsheet id (use --sheet-id or DEFAULT_SPREADSHEET_ID)")
        _finish_collection_report(args, report, "WITHHELD", error_code="archive_target_missing")
        return 1

    store = NewsArchiveStore(sheet_id, os.getenv("NEWS_ARCHIVE_SHEET", "News_Archive"))
    if not store.is_available():
        logger.error("News_Archive sheet unavailable (gspread/creds?) -- nothing written")
        init_code = getattr(store, "init_error_code", None)
        code = init_code if init_code in {
            "archive_client_unavailable", "archive_lookup_failed", "archive_header_read_failed",
            "archive_header_unverified", "archive_init_failed"} else "archive_unavailable"
        _finish_collection_report(args, report, "WITHHELD", error_code=code)
        return 1

    # idempotency: skip symbols already archived for today
    try:
        already = store.existing_keys_for_date(date_str)
    except Exception as exc:
        code = (str(exc) if str(exc) in {"archive_key_read_failed", "archive_header_unverified"}
                else "archive_key_read_failed")
        logger.error("News_Archive verification withheld (%s)", code)
        _finish_collection_report(args, report, "WITHHELD", error_code=code)
        return 1
    fresh = [row for row in rows if str(row[1]).strip().upper() not in already]
    skipped = len(rows) - len(fresh)
    if skipped:
        print("[news_archive] %d already archived for %s -- skipping" % (skipped, date_str))

    if not fresh:
        print("[news_archive] nothing new to write for %s" % date_str)
        _finish_collection_report(args, report,
                                  "SKIPPED_EXISTING" if rows else "NO_ELIGIBLE_ROWS",
                                  skipped_rows=skipped)
        return 1 if incomplete else 0

    # v1.1.0 [SECTOR JOIN] -- env-gated, fail-open; see header WHY.
    if _env_sector_join_enabled():
        try:
            smap = store.load_symbol_sector_map()
        except Exception as e:
            logger.warning("sector join failed (%s) -- Sector left blank", e)
            smap = {}
        if smap:
            filled = 0
            for row in fresh:
                sec = smap.get(str(row[1]).strip().upper(), "")
                if sec:
                    row[2] = sec
                    filled += 1
            print("[news_archive] sector join: %d/%d fresh rows filled (map: %d symbols)"
                  % (filled, len(fresh), len(smap)))
        else:
            print("[news_archive] sector join armed but map empty -- Sector left blank (fail-open)")

    ok = store.append(fresh)
    _finish_collection_report(args, report, "APPEND_ACCEPTED" if ok else "UNKNOWN",
                              error_code=None if ok else "archive_append_outcome_unknown",
                              attempted_rows=len(fresh), accepted_rows=len(fresh) if ok else 0,
                              skipped_rows=skipped)
    print("[news_archive] %s %d rows to %s" % ("wrote" if ok else "UNKNOWN write outcome for", len(fresh), store.sheet_name))
    return 0 if ok and not incomplete else 1


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
    p.add_argument("--evidence-json", default=None,
                   help="Write per-symbol acquisition status and bounded public article provenance JSON.")
    p.add_argument("--adaptive", action="store_true", default=_env_bool("TFB_NEWS_ARCHIVE_ADAPTIVE", False),
                   help="Augment the env/default baseline with source-validated portfolio/research names and a rotating pool; CLI --universe remains absolute.")
    p.add_argument("--max-symbols", type=int, default=60,
                   help="Maximum unique news analyses per run (clamped to 1..60); omitted or unknown identities are reported.")
    p.add_argument("--timeout-seconds", type=int, default=600,
                   help="Whole news batch time budget (clamped to 30..600 seconds, independent of source retries).")
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
