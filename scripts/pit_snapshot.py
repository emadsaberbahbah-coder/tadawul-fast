#!/usr/bin/env python3
# scripts/pit_snapshot.py
"""
================================================================================
PIT Snapshot — v1.0.0 (W-6, Execution Plan v2.1; the evidence-preservation stopgap)
================================================================================
NEW script (window whitelist W-6, scheduled Jul 22–24; adopted 2026-07-20).

WHY
    "Every unarchived day is evidence lost forever" — the 2026-07-20 lesson
    that blocked backtesting the engine's own scores: fundamentals and engine
    outputs are overwritten in place on every refresh, so no point-in-time
    record exists to test yesterday's claim against tomorrow's outcome. The
    structural fix is the elevated PIT store / Postgres (post-S-1); THIS is
    the stopgap that starts the archive TODAY, append-only, decision symbols
    only, entirely off the request path (GitHub Actions, sheet-as-bus — the
    same Option-B doctrine as calendar_sync).

WHAT IT DOES (daily via .github/workflows/pit_snapshot.yml, ~23:10 Riyadh,
after the US close so each day's row set carries full-day data)
    1. Harvests the DECISION symbol set: Top_10_Investments (selected +
       qualified) ∪ My_Portfolio ∪ Shadow_Board (typically ~30–70 symbols).
    2. Looks each symbol up in the enriched pages (Market_Leaders,
       Global_Markets, Mutual_Funds — header-tolerant aliases) and captures a
       fixed field set: price, market cap, P/E, P/B, EPS, dividend yield,
       debt ratio, 12M expected ROI, value score, recommendation, provider,
       and the row's own Last Updated stamp (provenance travels with the
       snapshot).
    3. APPENDS one row per (date, symbol) to `_PIT_Fundamentals` — never
       updates, never deletes; a re-run on the same day skips existing keys
       (duplicate-proof), so the tab is a true point-in-time ledger.
    4. Writes a [PIT-SNAPSHOT] line to _Run_Log with written/skipped/missing
       counts — silent failure impossible; the seven-gate audit reads it.

GROWTH HONESTY
    ~50 symbols × 16 columns × 365 days ≈ 300k cells/year — comfortably
    inside Sheets limits for the stopgap's lifetime; the Postgres store
    inherits this tab as its seed corpus.

ENV
    DEFAULT_SPREADSHEET_ID / SPREADSHEET_ID    production workbook id
    GOOGLE_SHEETS_CREDENTIALS(_B64) | GOOGLE_APPLICATION_CREDENTIALS
    TFB_PIT_SHEET            default "_PIT_Fundamentals"
    TFB_PIT_SYMBOL_PAGES     default "Top_10_Investments,My_Portfolio,Shadow_Board"
    TFB_PIT_SOURCE_PAGES     default "Market_Leaders,Global_Markets,Mutual_Funds"

USAGE
    python scripts/pit_snapshot.py --dry-run     # print rows, write nothing
    python scripts/pit_snapshot.py --write       # append (CI mode)
    python scripts/pit_snapshot.py --selftest    # offline logic harness
================================================================================
"""
from __future__ import annotations

import argparse
import base64
import datetime as _dt
import json
import os
import re
import sys
from typing import Any, Dict, List, Optional, Tuple
from zoneinfo import ZoneInfo

__version__ = "1.0.0"
_RIYADH = ZoneInfo("Asia/Riyadh")

HEADERS = ["Snapshot Date", "Symbol", "Source Page", "Price", "Market Cap",
           "P/E", "P/B", "EPS", "Dividend Yield %", "Debt Ratio %",
           "Expected ROI 12M %", "Value Score", "Recommendation",
           "Data Provider", "Row Last Updated", "Captured At (UTC)"]

# field -> normalized header aliases (normalize: lowercase, alnum only)
_ALIASES: Dict[str, Tuple[str, ...]] = {
    "price": ("currentprice", "price", "lastprice", "priceusd", "pricenative"),
    "market_cap": ("marketcap", "marketcapitalization", "mktcap"),
    "pe": ("peratio", "pe", "pettm", "trailingpe", "priceearnings"),
    "pb": ("pbratio", "pb", "pricebook", "pricetobook"),
    "eps": ("eps", "epsttm", "earningspershare"),
    "div_yield": ("dividendyield", "dividendyieldpct", "divyield",
                  "dividendyieldttm"),
    "debt_ratio": ("debttoequity", "debtequity", "debtratio", "debtmcap",
                   "debttomarketcap", "debtmcappct"),
    "roi_12m": ("expectedroi12m", "expectedroi", "forecastroi12m",
                "engineroi12m", "expectedroipct"),
    "value_score": ("valuescore", "overallscore", "score", "compositescore"),
    "recommendation": ("recommendation", "recommendationdetail", "reco",
                       "recommendationcanonical"),
    "provider": ("dataprovider", "provider", "primaryprovider"),
    "last_updated": ("lastupdatedutc", "lastupdated", "asof", "timestamp"),
}
_FIELD_ORDER = ("price", "market_cap", "pe", "pb", "eps", "div_yield",
                "debt_ratio", "roi_12m", "value_score", "recommendation",
                "provider", "last_updated")


def _out(msg: str) -> None:
    sys.stderr.write(f"[pit_snapshot v{__version__}] {msg}\n")


def _env(name: str, default: str = "") -> str:
    return (os.getenv(name) or default).strip()


def _norm(h: Any) -> str:
    return re.sub(r"[^a-z0-9]", "", str(h or "").lower())


def _today_riyadh() -> str:
    return _dt.datetime.now(_RIYADH).date().isoformat()


# --------------------------------------------------------------------------- #
# Pure helpers (offline selftest)                                             #
# --------------------------------------------------------------------------- #
def harvest_symbols(values: List[List[Any]]) -> List[str]:
    """Symbols from a raw sheet matrix (same contract as calendar_sync):
    find the header row containing 'Symbol', collect below, drop labels."""
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


def index_page(values: List[List[Any]]) -> Dict[str, Dict[str, Any]]:
    """{SYMBOL: {field: value}} for one enriched page, header-tolerant.
    The header row is the one containing 'Symbol'; every alias resolves to
    its column once; rows keyed by upper-cased symbol (first wins)."""
    header_i: Optional[int] = None
    sym_col: Optional[int] = None
    for i, row in enumerate(values or []):
        cells = [_norm(c) for c in row]
        if "symbol" in cells:
            header_i = i
            sym_col = cells.index("symbol")
            colmap: Dict[str, int] = {}
            for f, aliases in _ALIASES.items():
                for a in aliases:
                    if a in cells:
                        colmap[f] = cells.index(a)
                        break
            break
    out: Dict[str, Dict[str, Any]] = {}
    if header_i is None or sym_col is None:
        return out
    for row in values[header_i + 1:]:
        cells = [str(c).strip() if c is not None else "" for c in row]
        sym = (cells[sym_col] if sym_col < len(cells) else "").upper()
        if not sym or " " in sym or sym == "SYMBOL":
            continue
        if sym in out:
            continue
        rec: Dict[str, Any] = {}
        for f, ci in colmap.items():
            rec[f] = cells[ci] if ci < len(cells) else ""
        out[sym] = rec
    return out


def build_rows(date_str: str, symbols: List[str],
               pages: List[Tuple[str, Dict[str, Dict[str, Any]]]],
               existing_keys: set) -> Tuple[List[List[Any]], int, int]:
    """(rows_to_append, skipped_dup, missing). First page holding the symbol
    wins (page order = precedence). A symbol on no page still gets a row —
    the ABSENCE at t is itself point-in-time evidence — with Source '-'."""
    captured = _dt.datetime.now(_dt.timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    rows: List[List[Any]] = []
    skipped = missing = 0
    for s in symbols:
        if (date_str, s) in existing_keys:
            skipped += 1
            continue
        src, rec = "-", {}
        for pname, idx in pages:
            if s in idx:
                src, rec = pname, idx[s]
                break
        if src == "-":
            missing += 1
        rows.append([date_str, s, src]
                    + [rec.get(f, "") for f in _FIELD_ORDER[:-1]]
                    + [rec.get("last_updated", ""), captured])
    return rows, skipped, missing


def existing_keys_for(values: List[List[Any]], date_str: str) -> set:
    """(date, symbol) keys already present for date_str (duplicate guard)."""
    keys: set = set()
    for row in values or []:
        if not row or len(row) < 2:
            continue
        d = str(row[0]).strip()[:10]
        if d == date_str:
            keys.add((d, str(row[1]).strip().upper()))
    return keys


# --------------------------------------------------------------------------- #
# Sheets I/O (CI-only imports; mirrors calendar_sync)                         #
# --------------------------------------------------------------------------- #
def _credentials():
    from google.oauth2 import service_account
    path = _env("GOOGLE_APPLICATION_CREDENTIALS")
    scopes = ["https://www.googleapis.com/auth/spreadsheets"]
    if path and os.path.exists(path):
        return service_account.Credentials.from_service_account_file(
            path, scopes=scopes)
    raw = (_env("GOOGLE_SHEETS_CREDENTIALS")
           or _env("GOOGLE_SHEETS_CREDENTIALS_B64")
           or _env("GOOGLE_CREDENTIALS"))
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
    info = json.loads(s)
    return service_account.Credentials.from_service_account_info(
        info, scopes=scopes)


def _open_book():
    import gspread
    creds = _credentials()
    gc = gspread.authorize(creds) if creds else gspread.service_account()
    sid = _env("DEFAULT_SPREADSHEET_ID") or _env("SPREADSHEET_ID")
    if not sid:
        raise RuntimeError("DEFAULT_SPREADSHEET_ID / SPREADSHEET_ID not set")
    return gc.open_by_key(sid)


def append_run_log(book, status: str, message: str,
                   details: Dict[str, Any]) -> None:
    try:
        book.worksheet("_Run_Log").append_row(
            [_dt.datetime.now(_dt.timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
             "INFO" if status == "OK" else "ERROR", "pit_snapshot",
             _env("TFB_PIT_SHEET", "_PIT_Fundamentals"), status, message,
             "", "", "", json.dumps(details)[:900]],
            value_input_option="RAW")
    except Exception as e:  # noqa: BLE001
        _out(f"WARN: _Run_Log append failed (outcome unaffected): {e}")


# --------------------------------------------------------------------------- #
# Main                                                                        #
# --------------------------------------------------------------------------- #
def main(argv: Optional[List[str]] = None) -> int:
    ap = argparse.ArgumentParser(description="Append the daily PIT snapshot.")
    g = ap.add_mutually_exclusive_group()
    g.add_argument("--write", action="store_true")
    g.add_argument("--dry-run", action="store_true")
    g.add_argument("--selftest", action="store_true")
    args = ap.parse_args(argv)
    if args.selftest:
        return _selftest()
    write = bool(args.write) and not args.dry_run

    sym_pages = [p.strip() for p in _env(
        "TFB_PIT_SYMBOL_PAGES",
        "Top_10_Investments,My_Portfolio,Shadow_Board").split(",") if p.strip()]
    src_pages = [p.strip() for p in _env(
        "TFB_PIT_SOURCE_PAGES",
        "Market_Leaders,Global_Markets,Mutual_Funds").split(",") if p.strip()]
    tab = _env("TFB_PIT_SHEET", "_PIT_Fundamentals")
    today = _today_riyadh()

    try:
        book = _open_book()
    except Exception as e:  # noqa: BLE001
        _out(f"ERROR: cannot open workbook: {e}")
        return 2

    symbols: List[str] = []
    for p in sym_pages:
        try:
            vals = book.worksheet(p).get("A1:DZ2000")
        except Exception as e:  # noqa: BLE001
            _out(f"WARN: cannot read symbol page {p}: {e}")
            continue
        got = harvest_symbols(vals)
        _out(f"symbol page {p}: {len(got)}")
        symbols += [s for s in got if s not in symbols]
    if not symbols:
        msg = f"[PIT-SNAPSHOT v{__version__}] FAIL no decision symbols harvested"
        _out("ERROR: " + msg)
        append_run_log(book, "FAIL", msg, {})
        return 2

    indexed: List[Tuple[str, Dict[str, Dict[str, Any]]]] = []
    for p in src_pages:
        try:
            vals = book.worksheet(p).get("A1:EZ8000")
            idx = index_page(vals)
            indexed.append((p, idx))
            _out(f"source page {p}: {len(idx)} rows indexed")
        except Exception as e:  # noqa: BLE001
            _out(f"WARN: cannot index source page {p}: {e}")

    try:
        ws = book.worksheet(tab)
        existing = ws.get("A1:B100000")
    except Exception:
        ws = None
        existing = []
    keys = existing_keys_for(existing[1:] if existing else [], today)

    rows, skipped, missing = build_rows(today, symbols, indexed, keys)
    _out(f"date={today} symbols={len(symbols)} to_write={len(rows)} "
         f"skipped_dup={skipped} missing_on_pages={missing}")

    if not write:
        for r in rows[:12]:
            _out("  " + " | ".join(str(c) for c in r[:8]))
        _out("dry-run — nothing written (pass --write to append)")
        return 0

    try:
        if ws is None:
            ws = book.add_worksheet(title=tab, rows=2000, cols=len(HEADERS))
            ws.update(values=[HEADERS], range_name="A1")
            try:
                ws.freeze(rows=1)
            except Exception:
                pass
        if rows:
            ws.append_rows(rows, value_input_option="RAW")
        msg = (f"[PIT-SNAPSHOT v{__version__}] date={today} "
               f"written={len(rows)} skipped_dup={skipped} "
               f"missing={missing} symbols={len(symbols)}")
        _out(msg)
        append_run_log(book, "OK", msg,
                       {"written": len(rows), "skipped": skipped,
                        "missing": missing})
        return 0
    except Exception as e:  # noqa: BLE001
        msg = f"[PIT-SNAPSHOT v{__version__}] FAIL append: {e}"
        _out("ERROR: " + msg)
        append_run_log(book, "FAIL", msg, {"stage": "append"})
        return 3


# --------------------------------------------------------------------------- #
# Offline selftest                                                            #
# --------------------------------------------------------------------------- #
def _selftest() -> int:
    checks: List[Tuple[str, bool]] = []
    page = [["junk", "", ""],
            ["Symbol", "Name", "Current Price", "P/E Ratio", "Market Cap",
             "Expected ROI 12M", "Recommendation", "Last Updated (UTC)"],
            ["EXE.US", "Expand", "86.95", "12.4", "6.1B", "18.2", "BUY",
             "2026-07-21T20:05:00Z"],
            ["EXE.US", "dup ignored", "1", "", "", "", "", ""],
            ["", "", "", "", "", "", "", ""],
            ["4030.SR", "Bahri", "33.08", "9.1", "13B", "35", "BUY",
             "2026-07-21 14:34"]]
    idx = index_page(page)
    checks.append(("index: alias resolution + first-row-wins dedup",
                   idx["EXE.US"]["price"] == "86.95"
                   and idx["EXE.US"]["pe"] == "12.4"
                   and idx["EXE.US"]["roi_12m"] == "18.2"
                   and idx["4030.SR"]["last_updated"] == "2026-07-21 14:34"
                   and len(idx) == 2))
    syms = harvest_symbols([["Control", ""], ["Symbol", "Name"],
                            ["EXE.US", "x"], ["Grace note", ""],
                            ["4030.SR", "y"], ["EXE.US", "again"]])
    checks.append(("harvest: dedup + label filtering",
                   syms == ["EXE.US", "4030.SR"]))
    rows, skipped, missing = build_rows(
        "2026-07-22", ["EXE.US", "4030.SR", "GHOST.XX"],
        [("Market_Leaders", idx)], {("2026-07-22", "4030.SR")})
    checks.append(("rows: dup skipped, ghost recorded as absence evidence",
                   skipped == 1 and missing == 1 and len(rows) == 2
                   and rows[0][:4] == ["2026-07-22", "EXE.US",
                                       "Market_Leaders", "86.95"]
                   and rows[1][2] == "-"))
    checks.append(("rows: width matches header",
                   all(len(r) == len(HEADERS) for r in rows)))
    keys = existing_keys_for(
        [["2026-07-22", "exe.us"], ["2026-07-21", "OLD.US"], [""]],
        "2026-07-22")
    checks.append(("dup keys: date-scoped, case-normalized",
                   keys == {("2026-07-22", "EXE.US")}))
    checks.append(("precedence: first page wins",
                   build_rows("2026-07-23", ["EXE.US"],
                              [("A", {"EXE.US": {"price": "1"}}),
                               ("B", {"EXE.US": {"price": "2"}})],
                              set())[0][0][3] == "1"))
    passed = sum(1 for _, ok in checks if ok)
    for name, ok in checks:
        print(("PASS " if ok else "FAIL ") + name)
    print(f"[pit_snapshot v{__version__}] SELFTEST {passed}/{len(checks)}")
    return 0 if passed == len(checks) else 1


if __name__ == "__main__":
    raise SystemExit(main())
