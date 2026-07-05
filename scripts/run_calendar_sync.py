#!/usr/bin/env python3
# scripts/run_calendar_sync.py
"""
================================================================================
Calendar Sync — v1.0.0 (F1 wiring, Option B: sheet-as-bus, off request path)
================================================================================
NEW script (owner greenlight 2026-07-05: "go with the recommended one").

WHAT THIS DOES
    Once a day (GitHub Actions: calendar_sync.yml), for the DECISION symbols:
      1. harvests symbols from the pages in TFB_CALENDAR_PAGES
         (default: Top_10_Investments + My_Portfolio),
      2. calls core.providers.calendar_provider.fetch_event_context_sync()
         (EODHD earnings + ex-dividend calendars; KSA symbols honestly None),
      3. REPLACES the Calendar_Events tab with one row per symbol:
         Symbol | Next Earnings Date | Days To Earnings | Next Ex-Div Date |
         Days To ExDiv | Updated At (Riyadh) | Source
    track_performance.py v6.15.0 reads this tab and merges the two date keys
    into the Top10 rows BEFORE building Signal_History snapshots — which is
    how the v6.14.0 "Days To Earnings"/"Days To ExDiv" columns fill.

WHY OPTION B (vs in-process cache on the backend)
    * The Top10 build runs ON the request path behind the ~100s Render edge
      timeout; live calendar calls there would risk it. This job runs entirely
      off that path — the backend and every route are UNTOUCHED (zero Render
      changes for this feature).
    * Calendar facts change at most daily; a daily sheet write is the honest
      cadence, and the sheet doubles as a human-auditable view of exactly
      which event data the system is conditioning on.
    * Scope is deliberately Top_10 + My_Portfolio (~tens of symbols): the
      ex-div endpoint is per-symbol, so widening to Market_Leaders (~1,100)
      means ~1,100 extra calls/day. Widen later via TFB_CALENDAR_PAGES once
      wanted — the code already supports it.

FAIL-SAFETY
    Provider disabled / key missing / any fetch error -> the tab still gets a
    full symbol list with blank dates and a Source note, so the tracker merge
    degrades to no-op instead of breaking. Sheet errors log and exit non-zero
    (the workflow surfaces red) but never raise tracebacks at the user.

ENV
    DEFAULT_SPREADSHEET_ID / SPREADSHEET_ID   production workbook id
    GOOGLE_SHEETS_CREDENTIALS                 service-account JSON (or base64)
    EODHD_API_KEY (+ TFB_CALENDAR_ENABLED=1)  consumed by calendar_provider
    TFB_CALENDAR_PAGES     default "Top_10_Investments,My_Portfolio"
    TFB_CALENDAR_SHEET     default "Calendar_Events"

USAGE
    python scripts/run_calendar_sync.py --dry-run    # print table, write nothing
    python scripts/run_calendar_sync.py --write      # write the tab (CI mode)
================================================================================
"""
from __future__ import annotations

import argparse
import base64
import datetime as _dt
import json
import os
import sys
from typing import Any, Dict, List, Optional
from zoneinfo import ZoneInfo

__version__ = "1.0.0"
_RIYADH = ZoneInfo("Asia/Riyadh")

HEADERS = ["Symbol", "Next Earnings Date", "Days To Earnings",
           "Next Ex-Div Date", "Days To ExDiv", "Updated At (Riyadh)", "Source"]


def _out(msg: str) -> None:
    sys.stderr.write(f"[calendar_sync v{__version__}] {msg}\n")


def _env(name: str, default: str = "") -> str:
    return (os.getenv(name) or default).strip()


def _today_riyadh() -> _dt.date:
    return _dt.datetime.now(_RIYADH).date()


def _days_until(iso: Optional[str]) -> Any:
    if not iso:
        return ""
    try:
        d = _dt.datetime.strptime(str(iso)[:10], "%Y-%m-%d").date()
        return (d - _today_riyadh()).days
    except Exception:
        return ""


# ----------------------------------------------------------------------------- #
# Google Sheets (gspread) — mirrors track_performance's best-effort loader
# ----------------------------------------------------------------------------- #
def _credentials():
    from google.oauth2 import service_account  # local import: CI only
    raw = (_env("GOOGLE_SHEETS_CREDENTIALS") or _env("GOOGLE_CREDENTIALS"))
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
        info = json.loads(s)
        return service_account.Credentials.from_service_account_info(
            info, scopes=["https://www.googleapis.com/auth/spreadsheets"])
    except Exception as e:
        _out(f"ERROR: credentials unusable: {e}")
        return None


def _open_book():
    import gspread  # local import: CI only
    creds = _credentials()
    gc = gspread.authorize(creds) if creds else gspread.service_account()
    sid = _env("DEFAULT_SPREADSHEET_ID") or _env("SPREADSHEET_ID")
    if not sid:
        raise RuntimeError("DEFAULT_SPREADSHEET_ID / SPREADSHEET_ID not set")
    return gc.open_by_key(sid)


# ----------------------------------------------------------------------------- #
# Pure helpers (unit-tested offline)
# ----------------------------------------------------------------------------- #
def harvest_symbols(values: List[List[Any]]) -> List[str]:
    """Symbols from a raw sheet matrix: locate a header row containing
    'Symbol', then collect that column below it. Filters headers repeated in
    body, blanks, and cell values with spaces (labels, notes)."""
    out: List[str] = []
    col: Optional[int] = None
    for row in values or []:
        cells = [str(c or "").strip() for c in row]
        if col is None:
            for i, c in enumerate(cells):
                if c.lower() == "symbol":
                    col = i
                    break
            continue
        v = cells[col] if col < len(cells) else ""
        if not v or " " in v or v.lower() == "symbol":
            continue
        out.append(v.upper())
    seen: set = set()
    return [s for s in out if not (s in seen or seen.add(s))]


def build_rows(symbols: List[str],
               ctx: Dict[str, Dict[str, Optional[str]]],
               source: str) -> List[List[Any]]:
    stamp = _dt.datetime.now(_RIYADH).strftime("%Y-%m-%d %H:%M")
    rows: List[List[Any]] = []
    for s in symbols:
        c = ctx.get(s) or {}
        e, x = c.get("next_earnings_date"), c.get("next_ex_div_date")
        rows.append([s, e or "", _days_until(e), x or "", _days_until(x),
                     stamp, source])
    return rows


# ----------------------------------------------------------------------------- #
# Main
# ----------------------------------------------------------------------------- #
def main(argv: Optional[List[str]] = None) -> int:
    ap = argparse.ArgumentParser(description="Write the Calendar_Events tab.")
    g = ap.add_mutually_exclusive_group()
    g.add_argument("--write", action="store_true", help="write the tab (CI mode)")
    g.add_argument("--dry-run", action="store_true", help="print table only")
    args = ap.parse_args(argv)
    write = bool(args.write) and not args.dry_run

    pages = [p.strip() for p in _env(
        "TFB_CALENDAR_PAGES", "Top_10_Investments,My_Portfolio").split(",") if p.strip()]
    tab = _env("TFB_CALENDAR_SHEET", "Calendar_Events")

    try:
        book = _open_book()
    except Exception as e:
        _out(f"ERROR: cannot open workbook: {e}")
        return 2

    symbols: List[str] = []
    for p in pages:
        try:
            vals = book.worksheet(p).get("A1:DZ2000")
        except Exception as e:
            _out(f"WARN: cannot read page {p}: {e}")
            continue
        got = harvest_symbols(vals)
        _out(f"page {p}: {len(got)} symbols")
        symbols += [s for s in got if s not in symbols]
    if not symbols:
        _out("ERROR: no symbols harvested — nothing to do")
        return 2
    _out(f"decision symbols total: {len(symbols)}")

    source = f"eodhd calendar via calendar_provider"
    try:
        from core.providers.calendar_provider import (  # noqa: PLC0415
            fetch_event_context_sync, is_enabled, __version__ as pv)
        if not is_enabled():
            _out("provider disabled (TFB_CALENDAR_ENABLED!=1 or key missing) "
                 "— writing blank dates")
            ctx, source = {}, "provider disabled — blank dates"
        else:
            ctx = fetch_event_context_sync(symbols)
            source = f"eodhd via calendar_provider v{pv}"
    except Exception as e:
        _out(f"WARN: provider failed ({e}) — writing blank dates")
        ctx, source = {}, f"provider error — blank dates"

    rows = build_rows(symbols, ctx, source)
    filled = sum(1 for r in rows if r[1] or r[3])
    _out(f"rows: {len(rows)} | with at least one date: {filled}")

    if not write:
        for r in rows[:15]:
            _out("  " + " | ".join(str(c) for c in r))
        _out("dry-run — nothing written (pass --write to publish)")
        return 0

    try:
        try:
            ws = book.worksheet(tab)
        except Exception:
            ws = book.add_worksheet(title=tab, rows=1000, cols=len(HEADERS))
        ws.update(values=[HEADERS], range_name="A1")
        ws.batch_clear([f"A2:G{max(1000, len(rows) + 1)}"])
        ws.update(values=rows, range_name=f"A2:G{len(rows) + 1}",
                  value_input_option="RAW")
        try:
            ws.freeze(rows=1)
        except Exception:
            pass
        _out(f"wrote {len(rows)} rows -> {tab}")
        return 0
    except Exception as e:
        _out(f"ERROR: sheet write failed: {e}")
        return 3


if __name__ == "__main__":
    raise SystemExit(main())
