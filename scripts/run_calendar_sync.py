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
import re
import sys
from typing import Any, Dict, List, Optional
from zoneinfo import ZoneInfo

# v1.1.0 (2026-07-22): STICKY DATES — replace-mode amnesia cured.
# EVIDENCE: EXE.US (earnings 2026-07-28, known since Monday) was stripped
# from the harvest pages by Tuesday's provider-429 wave; the full-tab
# REPLACE then erased its known FUTURE date, and Wednesday's selected EXE
# ticket rendered with no ⚠ tag — a protective layer silently lost to an
# unrelated page incident. FIX (merge semantics, three rules):
#   1. FILL:      a harvested symbol whose fetch returns no date inherits
#                 its prior FUTURE date (source column: "... +carried").
#   2. RESURRECT: a symbol missing from today's harvest but holding a
#                 prior FUTURE date keeps its row (the EXE case).
#   3. EXPIRE:    past dates are never carried — they die naturally.
# Fresh provider data always wins; carried rows are visibly marked; the
# summary line reports carried=/resurrected= for the audit chain.
# v1.1.1 (2026-07-23): TICKER-SHAPE GUARD — the pit_snapshot v1.0.1 lesson,
# ported. EVIDENCE (2026-07-22 evening audit of the production workbook):
# Calendar_Events carried section-leak rows in its Symbol column — FORECAST,
# COUNT, 402, 1298, VERSABANK, … — harvested off the Top_10 cockpit's
# multi-section layout exactly the way pit_snapshot's first live run proved
# (11 of 21 "symbols" were artifacts). Two leak paths existed here:
#   (1) harvest_symbols() filtered only blanks/spaces, so any section title
#       or bare count under a later Symbol column entered the universe and
#       burned a per-symbol provider call;
#   (2) WORSE — parse_prior() accepted ANY symbol holding a future date, so
#       the v1.1.0 RESURRECT rule made junk rows IMMORTAL: once a leak row
#       acquired a future date it would be carried forever, surviving every
#       REPLACE.
# FIX: every real decision symbol in this system carries a venue suffix
# (the pit v1.0.1 invariant); both the harvest and the prior-tab reader now
# accept ONLY ticker-shaped tokens (^[A-Z0-9]{1,8}\.[A-Z]{1,4}$ — verbatim
# the pit regex, kept identical ON PURPOSE so the two harvesters share one
# invariant). Existing contamination self-purges on the first run: junk
# can no longer harvest OR resurrect, and the tab is REPLACE-written, so
# the 2026-07-22 leak rows die without operator cleanup. Dropped counts are
# reported (junk_dropped= / prior_junk_purged=) for the audit chain. Note
# the shape excludes '='/'-' classes (GC=F, BRK-B): futures and unsuffixed
# share-classes are not calendar decision symbols today; widen the regex in
# BOTH scripts together if that invariant ever changes. Kill-switch:
# TFB_CALENDAR_TICKER_GUARD=0 restores v1.1.0 byte-identically (junk
# passes again). ZERO functions removed; additions: _ticker_guard_enabled,
# _is_ticker_shaped. Selftest grows 5 -> 9 with the exact production leak
# fixture.
__version__ = "1.1.1"
_RIYADH = ZoneInfo("Asia/Riyadh")

HEADERS = ["Symbol", "Next Earnings Date", "Days To Earnings",
           "Next Ex-Div Date", "Days To ExDiv", "Updated At (Riyadh)", "Source"]


def _out(msg: str) -> None:
    sys.stderr.write(f"[calendar_sync v{__version__}] {msg}\n")


def _env(name: str, default: str = "") -> str:
    return (os.getenv(name) or default).strip()


# v1.1.1: the pit_snapshot v1.0.1 ticker shape — one invariant, two scripts.
_TICKER_RE = re.compile(r"^[A-Z0-9]{1,8}\.[A-Z]{1,4}$")


def _ticker_guard_enabled() -> bool:
    """v1.1.1 kill-switch — DEFAULT ON. TFB_CALENDAR_TICKER_GUARD=0
    restores the v1.1.0 unguarded harvest/prior byte-identically."""
    return (_env("TFB_CALENDAR_TICKER_GUARD", "1") or "1").strip().lower() \
        not in ("0", "false", "off", "no")


def _is_ticker_shaped(sym: str) -> bool:
    """True when `sym` matches the venue-suffixed decision-symbol shape."""
    return bool(_TICKER_RE.match((sym or "").strip().upper()))


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
    dropped: List[str] = []
    harvest_symbols.last_dropped = dropped  # v1.1.1: telemetry, reset per call
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
        # v1.1.1: ticker-shape guard — section titles (FORECAST), counts
        # (402, 1298) and bare names (VERSABANK) are not venue-suffixed;
        # they never enter the universe or burn a provider call.
        if _ticker_guard_enabled() and not _is_ticker_shaped(v):
            dropped.append(v.upper())
            continue
        out.append(v.upper())
    harvest_symbols.last_dropped = dropped
    seen: set = set()
    return [s for s in out if not (s in seen or seen.add(s))]


def parse_prior(values: List[List[Any]]) -> Dict[str, Dict[str, str]]:
    """v1.1.0: {SYM: {"e": date, "x": date}} from the existing tab —
    FUTURE dates only (rule 3: the past is never carried)."""
    out: Dict[str, Dict[str, str]] = {}
    if not values:
        return out
    hdr_i, cs, ce, cx = -1, -1, -1, -1
    for i, row in enumerate(values[:5]):
        low = [str(c or "").strip().lower() for c in row]
        if "symbol" in low:
            hdr_i, cs = i, low.index("symbol")
            for j, h in enumerate(low):
                if "next earnings" in h:
                    ce = j
                elif "ex-div" in h or "ex div" in h:
                    cx = j
            break
    if hdr_i < 0 or cs < 0:
        return out
    parse_prior.junk_purged = 0  # v1.1.1: telemetry, reset per call
    for row in values[hdr_i + 1:]:
        sym = str(row[cs] if cs < len(row) else "").strip().upper()
        if not sym or sym == "SYMBOL":
            continue
        # v1.1.1: junk must not be immortal — a leak row holding a future
        # date would otherwise RESURRECT forever (rule 2). Purged rows die
        # on this run's REPLACE write.
        if _ticker_guard_enabled() and not _is_ticker_shaped(sym):
            parse_prior.junk_purged += 1
            continue
        rec: Dict[str, str] = {}
        for key, cix in (("e", ce), ("x", cx)):
            d = str(row[cix] if 0 <= cix < len(row) else "").strip()[:10]
            du = _days_until(d)
            if d and du is not None and du >= 0:
                rec[key] = d
        if rec:
            out[sym] = rec
    return out


def apply_sticky(symbols: List[str],
                 ctx: Dict[str, Dict[str, Optional[str]]],
                 prior: Dict[str, Dict[str, str]]
                 ) -> Tuple[List[str], Dict[str, Dict[str, Optional[str]]],
                            set, int, int]:
    """v1.1.0 merge: (symbols_out, ctx_out, carried_syms, n_fill, n_resur).
    Fresh provider data ALWAYS wins; prior fills blanks (rule 1) and
    resurrects vanished symbols (rule 2); only future dates exist in
    `prior` by construction (rule 3)."""
    ctx_out = {s: dict(ctx.get(s) or {}) for s in symbols}
    carried: set = set()
    n_fill = 0
    for s in symbols:
        p = prior.get(s)
        if not p:
            continue
        c = ctx_out[s]
        touched = False
        if not c.get("next_earnings_date") and p.get("e"):
            c["next_earnings_date"] = p["e"]
            touched = True
        if not c.get("next_ex_div_date") and p.get("x"):
            c["next_ex_div_date"] = p["x"]
            touched = True
        if touched:
            carried.add(s)
            n_fill += 1
    symbols_out = list(symbols)
    n_res = 0
    for s, p in prior.items():
        if s in ctx_out or not p.get("e"):
            continue
        symbols_out.append(s)
        ctx_out[s] = {"next_earnings_date": p.get("e"),
                      "next_ex_div_date": p.get("x")}
        carried.add(s)
        n_res += 1
    return symbols_out, ctx_out, carried, n_fill, n_res


def build_rows(symbols: List[str],
               ctx: Dict[str, Dict[str, Optional[str]]],
               source: str,
               carried: Optional[set] = None) -> List[List[Any]]:
    stamp = _dt.datetime.now(_RIYADH).strftime("%Y-%m-%d %H:%M")
    carried = carried or set()
    rows: List[List[Any]] = []
    for s in symbols:
        c = ctx.get(s) or {}
        e, x = c.get("next_earnings_date"), c.get("next_ex_div_date")
        row_src = (source + " +carried") if s in carried else source
        rows.append([s, e or "", _days_until(e), x or "", _days_until(x),
                     stamp, row_src])
    return rows


# ----------------------------------------------------------------------------- #
# Main
# ----------------------------------------------------------------------------- #
def main(argv: Optional[List[str]] = None) -> int:
    ap = argparse.ArgumentParser(description="Write the Calendar_Events tab.")
    g = ap.add_mutually_exclusive_group()
    g.add_argument("--write", action="store_true", help="write the tab (CI mode)")
    g.add_argument("--dry-run", action="store_true", help="print table only")
    g.add_argument("--selftest", action="store_true",
                   help="offline logic harness (no network)")
    args = ap.parse_args(argv)
    if getattr(args, "selftest", False):
        return _selftest()
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
        _dropped = list(getattr(harvest_symbols, "last_dropped", []) or [])
        _out(f"page {p}: {len(got)} symbols"
             + (f" | junk_dropped={len(_dropped)}"
                f" ({', '.join(_dropped[:6])}{'…' if len(_dropped) > 6 else ''})"
                if _dropped else ""))
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

    # v1.1.0: sticky merge against the tab's prior state (read once, early).
    prior: Dict[str, Dict[str, str]] = {}
    try:
        prior = parse_prior(book.worksheet(tab).get("A1:H400"))
    except Exception as e:
        _out(f"WARN: prior-tab read failed ({e}) — no carry this run")
    symbols, ctx, carried, n_fill, n_res = apply_sticky(symbols, ctx, prior)
    rows = build_rows(symbols, ctx, source, carried)
    filled = sum(1 for r in rows if r[1] or r[3])
    _out(f"rows: {len(rows)} | with at least one date: {filled} | "
         f"carried={n_fill} resurrected={n_res} | "
         f"prior_junk_purged={getattr(parse_prior, 'junk_purged', 0)}")

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




# ----------------------------------------------------------------------------- #
# v1.1.0 offline selftest
# ----------------------------------------------------------------------------- #
def _selftest() -> int:
    today = _dt.datetime.now(_RIYADH).date()
    fut = (today + _dt.timedelta(days=6)).isoformat()
    fut2 = (today + _dt.timedelta(days=13)).isoformat()
    past = (today - _dt.timedelta(days=2)).isoformat()
    tab = [["Symbol", "Next Earnings Date", "Days To Earnings",
            "Next Ex-Div Date", "Days To ExDiv", "Updated At (Riyadh)",
            "Source"],
           ["EXE.US", fut, 6, "", "", "x", "eodhd"],
           ["OLD.US", past, -2, "", "", "x", "eodhd"],
           ["MRP.US", fut2, 13, past, -2, "x", "eodhd"]]
    prior = parse_prior(tab)
    checks = []
    checks.append(("prior: future kept, past expired (rows and fields)",
                   "EXE.US" in prior and "OLD.US" not in prior
                   and prior["MRP.US"].get("e") == fut2
                   and "x" not in prior["MRP.US"]))
    syms, ctx, carried, nf, nr = apply_sticky(
        ["MRP.US", "NEW.US"],
        {"MRP.US": {"next_earnings_date": ""},
         "NEW.US": {"next_earnings_date": fut}},
        prior)
    checks.append(("fill: harvested blank inherits prior future",
                   ctx["MRP.US"]["next_earnings_date"] == fut2
                   and "MRP.US" in carried and nf == 1))
    checks.append(("resurrect: vanished EXE returns carried",
                   "EXE.US" in syms and ctx["EXE.US"]["next_earnings_date"] == fut
                   and "EXE.US" in carried and nr == 1))
    checks.append(("fresh wins: provider date untouched",
                   ctx["NEW.US"]["next_earnings_date"] == fut
                   and "NEW.US" not in carried))
    rows = build_rows(syms, ctx, "eodhd v1.2.0", carried)
    by = {r[0]: r for r in rows}
    checks.append(("rows: carried marked, days computed",
                   by["EXE.US"][6].endswith("+carried")
                   and by["EXE.US"][2] == 6
                   and by["NEW.US"][6] == "eodhd v1.2.0"))
    # --- v1.1.1: the exact 2026-07-22 production leak, both guard states ---
    leak_page = [["Rank", "Symbol", "Name"],
                 ["1", "1050.SR", "BSF"],
                 ["2", "RCI.US", "Rogers"],
                 ["", "FORECAST", ""],
                 ["", "COUNT", ""],
                 ["", "402", ""],
                 ["", "1298", ""],
                 ["", "VERSABANK", ""],
                 ["3", "0405.HK", "Yuexiu"]]
    os.environ.pop("TFB_CALENDAR_TICKER_GUARD", None)
    got_on = harvest_symbols(leak_page)
    checks.append(("guard ON: leak tokens dropped, real symbols kept",
                   got_on == ["1050.SR", "RCI.US", "0405.HK"]
                   and sorted(harvest_symbols.last_dropped)
                   == ["1298", "402", "COUNT", "FORECAST", "VERSABANK"]))
    junk_tab = [["Symbol", "Next Earnings Date", "Days To Earnings",
                 "Next Ex-Div Date", "Days To ExDiv", "Updated At (Riyadh)",
                 "Source"],
                ["EXE.US", fut, 6, "", "", "x", "eodhd"],
                ["FORECAST", fut, 6, "", "", "x", "eodhd"],
                ["402", fut2, 13, "", "", "x", "eodhd"]]
    p_on = parse_prior(junk_tab)
    checks.append(("guard ON: junk never resurrects (immortality cured)",
                   list(p_on) == ["EXE.US"] and parse_prior.junk_purged == 2))
    os.environ["TFB_CALENDAR_TICKER_GUARD"] = "0"
    got_off = harvest_symbols(leak_page)
    p_off = parse_prior(junk_tab)
    checks.append(("guard OFF: v1.1.0 verbatim (junk passes both paths)",
                   "FORECAST" in got_off and "402" in got_off
                   and "FORECAST" in p_off and "402" in p_off
                   and parse_prior.junk_purged == 0))
    os.environ.pop("TFB_CALENDAR_TICKER_GUARD", None)
    checks.append(("shape edge cases: suffix required, 8+dot+4 bound",
                   _is_ticker_shaped("1050.SR") and _is_ticker_shaped("0405.HK")
                   and _is_ticker_shaped("MPHASIS.NS")
                   and not _is_ticker_shaped("FICO")
                   and not _is_ticker_shaped("GC=F")
                   and not _is_ticker_shaped("BRK-B")
                   and not _is_ticker_shaped("TOOLONGNAME.US")))
    passed = sum(1 for _, ok in checks if ok)
    for name, ok in checks:
        print(("PASS " if ok else "FAIL ") + name)
    print(f"[calendar_sync v{__version__}] SELFTEST {passed}/{len(checks)}")
    return 0 if passed == len(checks) else 1


if __name__ == "__main__":
    raise SystemExit(main())
