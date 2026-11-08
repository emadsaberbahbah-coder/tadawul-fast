#!/usr/bin/env python3
# scripts/run_corporate_events.py
"""
================================================================================
TFB Corporate Events - v1.0.0 (the Corporate_Events tab; workflow-scoped)
================================================================================
v1.0.0 (2026-11-08) -- THE CORPORATE-EVENT REGISTER
WHY (owner, 8 Nov 2026, after the week of 2-6 Nov: "how we can avoid that in
  the future" / "let's do the requirement and implement it"): the board
  seated INDV.US on 2 Nov, the day its merger with Supernus closed; on 3 Nov
  the combined company traded as SUPN, went ex an $8.13 special dividend
  (22 % of the price) and slid another 7 % by 6 Nov. Nothing in the pool,
  the pages or the ticket guard knew that a corporate action was in flight:
  the earnings guard reads Calendar_Events, the ex-dividend guard reads
  Dividend_Events (and EODHD showed that ex-date a day late), and a merger
  closing, a change of control or a name / ticker change had no row
  anywhere. SEC EDGAR publishes exactly these as 8-K items the day they are
  filed, and the repository already reads EDGAR for the news gate.
WHAT: for every US symbol harvested from the decision pages (the same
  harvest as run_calendar_sync: TFB_CORP_PAGES, default Top_10_Investments,
  My_Portfolio) the job reads the issuer's recent EDGAR submissions and
  keeps the 8-K / 8-K/A filings of the last TFB_CORP_LOOKBACK_DAYS (default
  45) whose items name a corporate action:
    2.01 acquisition or disposition completed  -> merger_completed
    5.01 change in control                     -> change_in_control
    5.03 articles / bylaws amended (a name or ticker change rides this item)
                                               -> charter_change
    3.03 material modification of rights       -> rights_modified
    1.03 bankruptcy or receivership            -> bankruptcy
    3.01 delisting or listing-rule notice      -> delisting_notice
    4.02 non-reliance on prior financials      -> restatement
  and writes one row per (symbol, filing) to the Corporate_Events tab,
  newest first: Symbol | Event | Event Date | Filed At (UTC) | Form | Items |
  Source | URL | Updated At (Riyadh). The event date is the filing date (an
  8-K is due within four business days of the event). A symbol with no such
  filing has no row. The tab is replaced whole (bounded at
  MAX_ROWS = 2000); a prior tab wider or taller than the bound is preserved
  and the run fails closed. Non-US listings have no EDGAR record and are
  skipped (Tadawul / CMA announcements are a later lane).
  The Top-10 cockpit's ticket guard (16_Decision_Top10 v1.27.0,
  DT10_GUARD_CORP_DAYS) reads the tab and holds an executable ticket whose
  symbol has an event dated within the window. NOTHING here decides,
  sizes or places anything: the tab is evidence.
GATES: the job runs only from .github/workflows/corporate_events.yml
  (schedule + manual dispatch; a manual dispatch writes only with
  write=true). TFB_SEC_USER_AGENT (the EDGAR contact the news gate already
  uses) is required for --write: without it the run exits 2 and writes
  nothing - a silent empty tab would read as "no corporate action".
  TFB_CORP_EVENTS_SHEET (default Corporate_Events), TFB_CORP_PAGES,
  TFB_CORP_LOOKBACK_DAYS (1..120), TFB_CORP_TIMEOUT_S (5..60, default 20).
ROLLBACK: disable the workflow (or stop dispatching it); the cockpit knob
  DT10_GUARD_CORP_DAYS absent = no hold. Nothing else reads the tab.
NOT THIS TAB: `_Corporate_Actions` (core/corporate_actions.py v1.0.0) is the
  operator-confirmed split / rename ledger that repairs Performance_Log
  prices; it is untouched. Corporate_Events is a read-only register of
  filed events for the ticket guard.
--------------------------------------------------------------------------------
"""
from __future__ import annotations

import argparse
import datetime as _dt
import json
import os
import sys
import time
from typing import Any, Dict, List, Optional, Sequence, Tuple

__version__ = "1.0.0"

_REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _REPO not in sys.path:
    sys.path.insert(0, _REPO)

UTC = _dt.timezone.utc
RIYADH = _dt.timezone(_dt.timedelta(hours=3))

EDGAR_TICKERS = "https://www.sec.gov/files/company_tickers.json"
EDGAR_SUBMISSIONS = "https://data.sec.gov/submissions/CIK{cik:010d}.json"

# 8-K item -> (event, label)
CORP_ITEMS: Dict[str, Tuple[str, str]] = {
    "2.01": ("merger_completed", "acquisition or disposition completed"),
    "5.01": ("change_in_control", "change in control"),
    "5.03": ("charter_change", "articles or bylaws amended (name / ticker change)"),
    "3.03": ("rights_modified", "material modification of rights"),
    "1.03": ("bankruptcy", "bankruptcy or receivership"),
    "3.01": ("delisting_notice", "delisting or listing-rule notice"),
    "4.02": ("restatement", "non-reliance on prior financials"),
}
# the order rows keep when one filing names several items (the gravest first)
EVENT_ORDER = ("bankruptcy", "delisting_notice", "restatement", "merger_completed",
               "change_in_control", "rights_modified", "charter_change")
HEADERS = ["Symbol", "Event", "Event Date", "Filed At (UTC)", "Form", "Items", "Source", "URL",
           "Updated At (Riyadh)"]
MAX_ROWS = 2000
DEFAULT_LOOKBACK_DAYS = 45
DEFAULT_PAGES = "Top_10_Investments,My_Portfolio"
DEFAULT_SHEET = "Corporate_Events"


def _out(msg: str) -> None:
    sys.stdout.write(msg + "\n")
    sys.stdout.flush()


def _env(name: str, default: str = "") -> str:
    return (os.environ.get(name) or default).strip()


def lookback_days() -> int:
    raw = _env("TFB_CORP_LOOKBACK_DAYS", str(DEFAULT_LOOKBACK_DAYS))
    try:
        n = int(raw)
    except ValueError:
        return DEFAULT_LOOKBACK_DAYS
    return min(max(n, 1), 120)


def timeout_s() -> int:
    raw = _env("TFB_CORP_TIMEOUT_S", "20")
    try:
        n = int(raw)
    except ValueError:
        return 20
    return min(max(n, 5), 60)


# ----------------------------------------------------------------------------- #
# Pure helpers (offline-tested)
# ----------------------------------------------------------------------------- #
def us_ticker(symbol: str) -> Optional[str]:
    """The EDGAR ticker of a US listing: 'BRK.B.US' -> 'BRK-B', 'AAPL' -> 'AAPL';
    None for any other venue (a suffix that is not .US)."""
    s = (symbol or "").strip().upper()
    if not s:
        return None
    if s.endswith(".US"):
        s = s[:-3]
    elif "." in s:
        return None
    return s.replace(".", "-") or None


def parse_ticker_map(payload: Any) -> Dict[str, int]:
    out: Dict[str, int] = {}
    rows = payload.values() if isinstance(payload, dict) else (payload or [])
    for row in rows:
        try:
            out[str(row["ticker"]).upper()] = int(row["cik_str"])
        except (KeyError, TypeError, ValueError):
            continue
    return out


def _filed_at(row_date: str, accept: str) -> Optional[_dt.datetime]:
    for s, fmt in ((accept, "%Y-%m-%dT%H:%M:%S.%fZ"), (accept, "%Y-%m-%dT%H:%M:%SZ"),
                   (row_date, "%Y-%m-%d")):
        try:
            return _dt.datetime.strptime(s, fmt).replace(tzinfo=UTC)
        except (TypeError, ValueError):
            continue
    return None


def events_from_submissions(payload: Any, symbol: str, cik: int,
                            since: _dt.datetime) -> List[Dict[str, Any]]:
    """The corporate-action filings of one issuer since `since`: one record per
    8-K / 8-K/A filing that names at least one CORP_ITEMS item, with the
    gravest event first in `event` and every matching item in `items`."""
    rec = (((payload or {}).get("filings") or {}).get("recent") or {}) if isinstance(payload, dict) else {}
    forms = rec.get("form") or []
    out: List[Dict[str, Any]] = []
    for i, form in enumerate(forms):
        if form not in ("8-K", "8-K/A"):
            continue

        def col(name: str) -> str:
            vals = rec.get(name) or []
            return str(vals[i]) if i < len(vals) and vals[i] is not None else ""

        when = _filed_at(col("filingDate"), col("acceptanceDateTime"))
        if when is None or when < since:
            continue
        items = [x.strip() for x in col("items").split(",") if x.strip()]
        hits = [x for x in items if x in CORP_ITEMS]
        if not hits:
            continue
        events = sorted({CORP_ITEMS[x][0] for x in hits}, key=EVENT_ORDER.index)
        acc, doc = col("accessionNumber"), col("primaryDocument")
        url = (f"https://www.sec.gov/Archives/edgar/data/{cik}/{acc.replace('-', '')}/{doc}"
               if acc and doc else "")
        out.append({"symbol": symbol, "event": events[0], "events": events,
                    "event_date": col("filingDate") or when.strftime("%Y-%m-%d"),
                    "filed_at": when, "form": form, "items": hits, "url": url})
    return out


def build_rows(events: Sequence[Dict[str, Any]], now: _dt.datetime) -> List[List[str]]:
    """Tab rows, newest filing first, then symbol; one row per filing; bounded."""
    stamp = now.astimezone(RIYADH).strftime("%Y-%m-%d %H:%M")
    ordered = sorted(events, key=lambda e: (e["filed_at"], e["symbol"]), reverse=True)
    rows: List[List[str]] = []
    seen = set()
    for e in ordered:
        key = (e["symbol"], e["form"], e["filed_at"].isoformat(), tuple(e["items"]))
        if key in seen:
            continue
        seen.add(key)
        rows.append([e["symbol"], "+".join(e["events"]), e["event_date"],
                     e["filed_at"].strftime("%Y-%m-%d %H:%M"), e["form"], ",".join(e["items"]),
                     "sec.gov EDGAR submissions", e["url"], stamp])
    return rows[:MAX_ROWS - 1]


# ----------------------------------------------------------------------------- #
# Network (CI only)
# ----------------------------------------------------------------------------- #
class Edgar:
    """Reads EDGAR with the declared contact; 2 consecutive failures open the
    breaker and the remaining symbols are skipped (reported, not hidden)."""

    def __init__(self, user_agent: str, timeout: int, fetch=None, sleep=None):
        self.hdr = {"User-Agent": user_agent, "Accept-Encoding": "gzip, deflate"}
        self.timeout = timeout
        self.fetch = fetch or self._fetch
        self.sleep = sleep or time.sleep
        self.failures: List[str] = []
        self.skipped: List[str] = []
        self.reached = 0

    def _fetch(self, url: str) -> Any:
        import requests  # local import: CI only
        r = requests.get(url, headers=self.hdr, timeout=self.timeout)
        r.raise_for_status()
        return r.json()

    def collect(self, symbols: Sequence[str], since: _dt.datetime) -> List[Dict[str, Any]]:
        try:
            tmap = parse_ticker_map(self.fetch(EDGAR_TICKERS))
        except Exception as exc:  # noqa: BLE001 - reported
            self.failures.append(f"ticker map: {type(exc).__name__}")
            return []
        out: List[Dict[str, Any]] = []
        streak = 0
        for sym in symbols:
            tick = us_ticker(sym)
            if not tick:
                continue
            cik = tmap.get(tick) or tmap.get(tick.replace("-", "."))
            if not cik:
                continue
            if streak >= 2:
                self.skipped.append(sym)
                continue
            self.sleep(0.15)
            try:
                payload = self.fetch(EDGAR_SUBMISSIONS.format(cik=cik))
            except Exception as exc:  # noqa: BLE001 - reported
                self.failures.append(f"{sym}: {type(exc).__name__}")
                streak += 1
                continue
            streak = 0
            self.reached += 1
            out.extend(events_from_submissions(payload, sym, cik, since))
        return out


# ----------------------------------------------------------------------------- #
# Workbook
# ----------------------------------------------------------------------------- #
def write_tab(book, tab: str, rows: List[List[str]]) -> int:
    """Replace the tab whole (header + rows + blank fill); fail closed on a
    prior tab beyond the bound. Returns a process exit code."""
    width = len(HEADERS)
    last_col = chr(ord("A") + width - 1)
    ws = None
    prior_extent = 0
    try:
        ws = book.worksheet(tab)
        rows_alloc = getattr(ws, "row_count", None)
        if isinstance(rows_alloc, int) and rows_alloc > MAX_ROWS + 1:
            _out(f"ERROR prior {tab} exceeds the bounded table ({rows_alloc} rows) - preserving tab")
            return 2
        prior_extent = rows_alloc if isinstance(rows_alloc, int) and rows_alloc > 0 else 0
    except Exception as e:  # noqa: BLE001
        if type(e).__name__ != "WorksheetNotFound":
            _out(f"ERROR prior {tab} unreadable ({type(e).__name__}) - preserving tab")
            return 2
        ws = None
    try:
        extent = max(200, len(rows) + 1, prior_extent)
        if ws is None:
            ws = book.add_worksheet(title=tab, rows=extent, cols=width)
        else:
            rows_alloc = getattr(ws, "row_count", None)
            cols_alloc = getattr(ws, "col_count", None)
            if isinstance(rows_alloc, int) and rows_alloc < extent:
                ws.resize(rows=extent)
            if isinstance(cols_alloc, int) and cols_alloc < width:
                ws.resize(cols=width)
        values = [list(HEADERS)] + rows + [[""] * width for _ in range(extent - len(rows) - 1)]
        ws.update(values=values, range_name=f"A1:{last_col}{extent}", value_input_option="RAW")
        try:
            ws.freeze(rows=1)
        except Exception:  # noqa: BLE001
            pass
        _out(f"wrote {len(rows)} rows -> {tab}")
        return 0
    except Exception as e:  # noqa: BLE001
        _out(f"ERROR publication unconfirmed: {type(e).__name__}; read back before retrying")
        return 3


def main(argv: Optional[Sequence[str]] = None, *, book=None, edgar: Optional[Edgar] = None,
         now: Optional[_dt.datetime] = None) -> int:
    ap = argparse.ArgumentParser(description="Write the Corporate_Events tab from EDGAR 8-K items.")
    g = ap.add_mutually_exclusive_group()
    g.add_argument("--write", action="store_true", help="write the tab (CI mode)")
    g.add_argument("--dry-run", action="store_true", help="print the rows only")
    args = ap.parse_args(list(argv) if argv is not None else None)
    write = bool(args.write) and not args.dry_run
    now = now or _dt.datetime.now(UTC)
    ua = _env("TFB_SEC_USER_AGENT")
    if not ua:
        _out("ERROR: TFB_SEC_USER_AGENT not set - EDGAR needs a declared contact; nothing written")
        return 2
    tab = _env("TFB_CORP_EVENTS_SHEET", DEFAULT_SHEET) or DEFAULT_SHEET
    pages = [p.strip() for p in _env("TFB_CORP_PAGES", DEFAULT_PAGES).split(",") if p.strip()]

    from scripts.run_calendar_sync import harvest_symbols, _open_book  # noqa: PLC0415
    if book is None:
        try:
            book = _open_book()
        except Exception as e:  # noqa: BLE001
            _out(f"ERROR: cannot open workbook: {type(e).__name__}")
            return 2
    symbols: List[str] = []
    for p in pages:
        try:
            vals = book.worksheet(p).get("A1:DZ2000")
        except Exception as e:  # noqa: BLE001
            _out(f"WARN: cannot read page {p}: {type(e).__name__}")
            continue
        got = harvest_symbols(vals)
        _out(f"page {p}: {len(got)} symbols")
        symbols += [s for s in got if s not in symbols]
    us = [s for s in symbols if us_ticker(s)]
    _out(f"decision symbols: {len(symbols)} | US listings: {len(us)} | lookback {lookback_days()}d")
    if not us:
        _out("no US listing in scope - nothing to read")
        rows: List[List[str]] = []
    else:
        edgar = edgar or Edgar(ua, timeout_s())
        since = now - _dt.timedelta(days=lookback_days())
        events = edgar.collect(us, since)
        rows = build_rows(events, now)
        _out(f"edgar: {edgar.reached} issuers read, {len(events)} corporate-action filing(s), "
             f"{len(rows)} row(s)" + (f" | failures {len(edgar.failures)}: {'; '.join(edgar.failures[:5])}"
                                     if edgar.failures else "")
             + (f" | skipped after the breaker: {len(edgar.skipped)}" if edgar.skipped else ""))
        for r in rows[:20]:
            _out("  " + " | ".join(r[:6]))
    if not write:
        _out("dry run - nothing written")
        return 0
    if edgar is not None and (edgar.skipped or (edgar.failures and edgar.reached == 0)):
        _out("ERROR: EDGAR read incomplete - a partial table would read as 'no corporate action'; tab preserved")
        return 2
    return write_tab(book, tab, rows)


if __name__ == "__main__":
    sys.exit(main())
