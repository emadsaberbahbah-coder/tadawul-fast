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
from typing import Any, Dict, List, Optional, Tuple
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
# v1.1.2: validate sticky dates per field, retain either future event, and
# preserve a carried record's original source/as-of instead of stamping it now.
# v1.1.3: retain canonical hyphenated/class symbols, read the bounded full
# prior event table, and label missing event facts without inventing an as-of.
# v1.1.4: replace the complete header/body/tail in one atomic values request.
# A transport failure before commit no longer leaves known events erased;
# lost acknowledgement after commit leaves the complete new table, not a mix.
# v1.2.0 (2026-10-27): LAST REPORT DATE (program register A4, the
# post-earnings quarantine data lane). WHY: FRST.US was bought on 26 Oct two
# sessions after a 22% EPS miss (Q3 reported 22 Oct after the close; -4.1%
# and -3.7% on heavy volume the next sessions). This tab only knew its NEXT
# report (2027-01-28), so neither the cockpit's earnings tag nor the owner's
# manual earnings rule - both forward-only - could see that the name had just
# reported and missed. WHAT: TFB_CAL_LAST_REPORT = off (DEFAULT: the
# 7-column table, the A1:G read and write and every log line are v1.1.4 byte
# for byte) | on. When on, core.providers.calendar_provider v1.3.0
# fetch_last_earnings_sync() adds, per symbol, the LATEST report date in the
# last TFB_CALENDAR_DAYS_BACK days (default 21, 1..120) and its EPS surprise
# %, and three columns are appended AFTER Source: Last Earnings Date | Days
# Since Earnings | EPS Surprise %. Every existing reader is unaffected (the
# GAS cockpit reads A:G; the tracker and the brief read by header name).
# FILL-ONLY CARRY: a harvested symbol whose fresh fetch has no report keeps a
# prior row's last report while it is still inside the window (a provider 429
# must not erase a quarantine); a carried report never resurrects a symbol
# and expires with the window. The prior read widens to A1:J only when the
# tab already has 10 columns; the tab grows to 10 columns before the write.
# One summary line: last_report: with=n fresh=f carried=c window=Nd.
# Rollback: unset TFB_CAL_LAST_REPORT; the next run writes A1:G again and
# blanks H:J in the same atomic update (review fix below).
# Review fix (Codex P2 on PR #850): with the lane off the publication is
# A:G only, so rows could move under H:J values an earlier ON run left
# behind, and a re-arm inside the window would carry another row's report
# (FRST's -22.2% landing on ZTO). When the lane is off and the tab still
# holds the last-report block (>= 10 columns AND H1:J1 equal to the three
# last-report headers), the off run now writes A1:J with H:J blank, header
# included; a 7-column tab (never armed) stays v1.1.4 byte for byte.
# Functions added: _last_report_enabled, _last_report_window, _to_num,
# _days_since, _past_report, parse_prior_last, merge_last_report,
# last_report_cells, _stale_last_report_cols. Removed: 0.
# v1.3.0 (2026-11-03): DIVIDEND DETAIL (the corporate-action guard's data
# lane). WHY: this tab read 'INDV.US | Next Ex-Div Date 2026-11-03 | Days To
# ExDiv 1' on 2 Nov at 16:28 Riyadh; that day's cockpit issued an INDV.US
# ticket and the owner bought it. The dividend was an 8.13 USD special (about
# 22 % of the price; the shares opened ex-dividend at 28.15 and a 30 % US
# withholding applies to a Saudi-resident holder). The date was here; the
# size was not, so neither the cockpit nor the owner could tell a 22 %
# special from a 0.7 % quarterly payment. WHAT: TFB_CAL_DIVIDENDS = off
# (DEFAULT: nothing below runs; this tab, its reads, writes and every log
# line are v1.2.0 byte for byte) | on. When on, AFTER the Calendar_Events
# publication (which is unchanged) the same decision symbols go to
# calendar_provider v1.4.0 fetch_next_exdiv_detail_sync() and a SEPARATE tab
# (TFB_CAL_DIVIDENDS_SHEET, default Dividend_Events) is replaced in one
# atomic A1:J update: Symbol | Ex-Div Date | Days To ExDiv | Amount |
# Currency | Pay Date | Record Date | Period | Updated At (Riyadh) | Source.
# A separate tab keeps every Calendar_Events reader (the GAS cockpit reads
# A:G, the tracker and the brief read by header, the A4 lane owns H:J)
# untouched. FILL-ONLY CARRY: a harvested symbol whose fresh fetch has no
# dividend keeps a prior row while its ex-date is still in the future (a
# provider 429 must not erase a known special dividend); a carried row is
# marked '+carried' and keeps its own as-of; a symbol outside the harvest is
# never added. FAIL-SAFE: provider disabled or the tab unreadable -> the tab
# is left exactly as it is (nothing is blanked); a failed fetch publishes
# the prior rows only; a publication failure returns 3 (the calendar
# publication before it already succeeded). One summary line: dividends:
# with=n fresh=f carried=c -> <tab>. Rollback: unset TFB_CAL_DIVIDENDS (the
# tab stays as the last ON run left it; readers decide by its as-of).
# Functions added: _dividends_enabled, _dividends_sheet, parse_prior_div,
# merge_dividends, build_div_rows, dividend_lane. Removed: 0.
__version__ = "1.3.0"
_RIYADH = ZoneInfo("Asia/Riyadh")
_MAX_CALENDAR_BODY_ROWS = 5000

HEADERS = ["Symbol", "Next Earnings Date", "Days To Earnings",
           "Next Ex-Div Date", "Days To ExDiv", "Updated At (Riyadh)", "Source"]
# v1.2.0: appended after Source only when TFB_CAL_LAST_REPORT is on.
LAST_REPORT_HEADERS = ["Last Earnings Date", "Days Since Earnings",
                       "EPS Surprise %"]
# v1.3.0: the separate dividend-detail tab (TFB_CAL_DIVIDENDS=on only).
DIV_HEADERS = ["Symbol", "Ex-Div Date", "Days To ExDiv", "Amount", "Currency",
               "Pay Date", "Record Date", "Period", "Updated At (Riyadh)", "Source"]


def _out(msg: str) -> None:
    sys.stderr.write(f"[calendar_sync v{__version__}] {msg}\n")


def _env(name: str, default: str = "") -> str:
    return (os.getenv(name) or default).strip()


# A venue suffix remains mandatory: cockpit headings/counts are not symbols.
# Canonical class/unit roots include GRT-UN.TO and BRK.B.US, not just letters.
_TICKER_RE = re.compile(r"^[A-Z0-9]{1,8}(?:[-.][A-Z0-9]{1,3})?\.[A-Z]{1,4}$")
_UNKNOWN_EVENT_NOTE_RE = re.compile(
    r"\s*\[events unknown:(?:earnings|ex-div)(?:/(?:earnings|ex-div))?\]")


def _ticker_guard_enabled() -> bool:
    """v1.1.1 kill-switch — DEFAULT ON. TFB_CALENDAR_TICKER_GUARD=0
    restores the v1.1.0 unguarded harvest/prior byte-identically."""
    return (_env("TFB_CALENDAR_TICKER_GUARD", "1") or "1").strip().lower() \
        not in ("0", "false", "off", "no")


def _is_ticker_shaped(sym: str) -> bool:
    """True when `sym` matches the venue-suffixed decision-symbol shape."""
    return bool(_TICKER_RE.match((sym or "").strip().upper()))


def _last_report_enabled() -> bool:
    """v1.2.0 gate - DEFAULT OFF. TFB_CAL_LAST_REPORT=on (or 1/true/yes)
    adds the three last-report columns; anything else is v1.1.4."""
    return _env("TFB_CAL_LAST_REPORT", "off").lower() in ("on", "1", "true", "yes")


def _last_report_window() -> int:
    """v1.2.0: look-back days for the last report (TFB_CALENDAR_DAYS_BACK,
    default 21, clamped 1..120 - the provider's own bounds)."""
    try:
        v = int(float(_env("TFB_CALENDAR_DAYS_BACK", "21") or "21"))
    except ValueError:
        v = 21
    return max(1, min(120, v))


def _dividends_enabled() -> bool:
    """v1.3.0 gate - DEFAULT OFF. TFB_CAL_DIVIDENDS=on (or 1/true/yes)
    publishes the dividend-detail tab; anything else runs nothing new."""
    return _env("TFB_CAL_DIVIDENDS", "off").lower() in ("on", "1", "true", "yes")


def _dividends_sheet() -> str:
    """v1.3.0: the dividend-detail tab name (default Dividend_Events)."""
    return _env("TFB_CAL_DIVIDENDS_SHEET", "Dividend_Events") or "Dividend_Events"


def _to_num(value: Any) -> Optional[float]:
    """v1.2.0: finite float from a cell or payload value, else None."""
    if value is None or isinstance(value, bool):
        return None
    text = str(value).strip().replace("%", "").replace(",", "")
    if not text:
        return None
    try:
        v = float(text)
    except ValueError:
        return None
    return v if v == v and v not in (float("inf"), float("-inf")) else None


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


def _days_since(iso: Any) -> Any:
    """v1.2.0: whole days from a YYYY-MM-DD report to today (Riyadh); ''."""
    text = str(iso or "").strip()
    if not re.fullmatch(r"\d{4}-\d{2}-\d{2}", text):
        return ""
    try:
        return (_today_riyadh() - _dt.date.fromisoformat(text)).days
    except ValueError:
        return ""


def _past_report(value: Any, window: int) -> Optional[str]:
    """v1.2.0: the canonical report day when it lies 1..window days ago."""
    d = _days_since(value)
    if d == "" or not (1 <= d <= window):
        return None
    return str(value).strip()


def _future_date(value: Any) -> Optional[str]:
    """Canonical future calendar day, or None for a malformed/expired field."""
    text = str(value or "").strip()
    if not re.fullmatch(r"\d{4}-\d{2}-\d{2}", text):
        return None
    try:
        day = _dt.date.fromisoformat(text)
        return day.isoformat() if day >= _today_riyadh() else None
    except ValueError:
        return None


def _prior_asof(value: Any) -> str:
    """Retain a valid supplied timestamp; absent/invalid observation stays unknown."""
    text = str(value or "").strip()
    if "T" not in text and " " not in text:
        return ""
    try:
        _dt.datetime.fromisoformat(text.replace("Z", "+00:00"))
        return text
    except ValueError:
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
    parse_prior.junk_purged = 0
    if not values:
        return out
    hdr_i, cs, ce, cx, ca, cp = -1, -1, -1, -1, -1, -1
    for i, row in enumerate(values[:5]):
        low = [str(c or "").strip().lower() for c in row]
        if "symbol" in low:
            hdr_i, cs = i, low.index("symbol")
            for j, h in enumerate(low):
                if "next earnings" in h:
                    ce = j
                elif "ex-div" in h or "ex div" in h:
                    cx = j
                elif h == "updated at (riyadh)":
                    ca = j
                elif h == "source":
                    cp = j
            break
    if hdr_i < 0 or cs < 0:
        return out
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
            d = _future_date(row[cix] if 0 <= cix < len(row) else "")
            if d is not None:
                rec[key] = d
        if rec:
            rec["asof"] = _prior_asof(row[ca] if 0 <= ca < len(row) else "")
            rec["source"] = str(row[cp] if 0 <= cp < len(row) else "").strip()
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
    for c in ctx_out.values():
        for key in ("next_earnings_date", "next_ex_div_date"):
            c[key] = _future_date(c.get(key))
    carried: set = set()
    n_fill = 0
    for s in symbols:
        p = prior.get(s)
        if not p:
            continue
        c = ctx_out[s]
        fields = []
        for prior_key, key in (("e", "next_earnings_date"), ("x", "next_ex_div_date")):
            d = _future_date(p.get(prior_key))
            if not c.get(key) and d:
                c[key] = d
                fields.append(prior_key)
        if fields:
            c["_carried_fields"] = ",".join(fields)
            c["_carried_asof"] = _prior_asof(p.get("asof"))
            c["_carried_source"] = str(p.get("source") or "").strip()
            carried.add(s)
            n_fill += 1
    symbols_out = list(symbols)
    n_res = 0
    for s, p in prior.items():
        e, x = _future_date(p.get("e")), _future_date(p.get("x"))
        if s in ctx_out or not (e or x):
            continue
        symbols_out.append(s)
        ctx_out[s] = {"next_earnings_date": e, "next_ex_div_date": x,
                      "_carried_fields": ",".join(k for k, d in (("e", e), ("x", x)) if d),
                      "_carried_asof": _prior_asof(p.get("asof")),
                      "_carried_source": str(p.get("source") or "").strip()}
        carried.add(s)
        n_res += 1
    return symbols_out, ctx_out, carried, n_fill, n_res


def parse_prior_last(values: List[List[Any]], window: int) -> Dict[str, Dict[str, Any]]:
    """v1.2.0: {SYM: {"date": iso, "surprise": float|None}} from the prior
    tab's Last Earnings Date / EPS Surprise % columns - reports still inside
    the window only (the past expires); ticker-shape guarded like
    parse_prior. A tab without the column yields {}."""
    out: Dict[str, Dict[str, Any]] = {}
    if not values:
        return out
    hdr_i, cs, cd, cp = -1, -1, -1, -1
    for i, row in enumerate(values[:5]):
        low = [str(c or "").strip().lower() for c in row]
        if "symbol" in low:
            hdr_i, cs = i, low.index("symbol")
            for j, h in enumerate(low):
                if h == "last earnings date":
                    cd = j
                elif h == "eps surprise %":
                    cp = j
            break
    if hdr_i < 0 or cs < 0 or cd < 0:
        return out
    for row in values[hdr_i + 1:]:
        sym = str(row[cs] if cs < len(row) else "").strip().upper()
        if not sym or sym == "SYMBOL" or sym in out:
            continue
        if _ticker_guard_enabled() and not _is_ticker_shaped(sym):
            continue
        d = _past_report(row[cd] if cd < len(row) else "", window)
        if d is None:
            continue
        out[sym] = {"date": d,
                    "surprise": _to_num(row[cp] if 0 <= cp < len(row) else None)}
    return out


def merge_last_report(symbols: List[str],
                      fresh: Dict[str, Dict[str, Any]],
                      prior_last: Dict[str, Dict[str, Any]],
                      window: int) -> Tuple[Dict[str, Dict[str, Any]], int, int]:
    """v1.2.0: (merged, n_fresh, n_carried). Fresh provider data wins; a
    prior report fills a symbol the fresh fetch left empty while it is still
    inside the window; a symbol outside `symbols` is never added."""
    merged: Dict[str, Dict[str, Any]] = {}
    n_fresh = n_carried = 0
    for s in symbols:
        f = (fresh or {}).get(s) or {}
        d = _past_report(f.get("last_earnings_date"), window)
        if d:
            merged[s] = {"date": d, "surprise": _to_num(f.get("eps_surprise_pct")),
                         "carried": False}
            n_fresh += 1
            continue
        p = (prior_last or {}).get(s) or {}
        d = _past_report(p.get("date"), window)
        if d:
            merged[s] = {"date": d, "surprise": _to_num(p.get("surprise")),
                         "carried": True}
            n_carried += 1
    return merged, n_fresh, n_carried


def _stale_last_report_cols(ws: Any) -> bool:
    """v1.2.0 review fix: True when the tab still carries the last-report
    block of an earlier ON run - at least 10 columns AND H1:J1 equal to
    LAST_REPORT_HEADERS. A narrower tab -> False without any read; a failed
    header read -> WARN and False (H:J left as is)."""
    cols = getattr(ws, "col_count", None)
    if not isinstance(cols, int) or cols < len(HEADERS) + len(LAST_REPORT_HEADERS):
        return False
    try:
        got = ws.get("A1:J1")
    except Exception as e:
        _out(f"WARN: last-report header read failed ({e}) - H:J left as is")
        return False
    head = [str(c or "").strip() for c in (got[0] if got else [])][7:10]
    return head == LAST_REPORT_HEADERS


def last_report_cells(symbol: str, merged: Dict[str, Dict[str, Any]]) -> List[Any]:
    """v1.2.0: [Last Earnings Date, Days Since Earnings, EPS Surprise %]."""
    m = (merged or {}).get(symbol)
    if not m:
        return ["", "", ""]
    sp = m.get("surprise")
    return [m["date"], _days_since(m["date"]), "" if sp is None else round(float(sp), 1)]


def build_rows(symbols: List[str],
               ctx: Dict[str, Dict[str, Optional[str]]],
               source: str,
               carried: Optional[set] = None) -> List[List[Any]]:
    stamp = _dt.datetime.now(_RIYADH).strftime("%Y-%m-%d %H:%M")
    carried = carried or set()
    rows: List[List[Any]] = []
    for s in symbols:
        c = ctx.get(s) or {}
        e, x = _future_date(c.get("next_earnings_date")), _future_date(c.get("next_ex_div_date"))
        row_src, row_stamp = source, stamp if (e or x) else ""
        if s in carried:
            row_stamp = _prior_asof(c.get("_carried_asof"))
            prior_source = _UNKNOWN_EVENT_NOTE_RE.sub(
                "", str(c.get("_carried_source") or "")).strip().removesuffix(" +carried")
            prior_source = prior_source or "prior source unknown"
            fields = set(str(c.get("_carried_fields") or "").split(","))
            fresh = [label for key, d, label in (("e", e, "earnings"), ("x", x, "ex-div"))
                     if d and key not in fields]
            if fresh:
                old = "/".join(label for key, label in (("e", "earnings"), ("x", "ex-div")) if key in fields)
                row_src = f"fresh {'/'.join(fresh)}: {source}; carried {old}: {prior_source} +carried"
            else:
                row_src = prior_source + " +carried"
        # Missing facts are not a neutral event result or a successful lookup.
        # This is derived row status, not provider provenance; remove the prior
        # annotation on roundtrip so newly known fields do not retain old status.
        unknown = [label for d, label in ((e, "earnings"), (x, "ex-div")) if not d]
        if unknown:
            suffix = " +carried" if row_src.endswith(" +carried") else ""
            row_src = row_src.removesuffix(suffix) if suffix else row_src
            row_src += f" [events unknown:{'/'.join(unknown)}]" + suffix
        rows.append([s, e or "", _days_until(e), x or "", _days_until(x),
                     row_stamp, row_src])
    return rows


# ----------------------------------------------------------------------------- #
# v1.3.0 dividend-detail lane (TFB_CAL_DIVIDENDS=on only)
# ----------------------------------------------------------------------------- #
def parse_prior_div(values: List[List[Any]]) -> Dict[str, Dict[str, Any]]:
    """v1.3.0: {SYM: detail} from the prior Dividend_Events tab - rows whose
    ex-date is still in the future only (the past is never carried);
    ticker-shape guarded like parse_prior. A tab without the headers -> {}."""
    out: Dict[str, Dict[str, Any]] = {}
    if not values:
        return out
    head = [str(c or "").strip().lower() for c in values[0]]
    if "symbol" not in head or "ex-div date" not in head:
        return out
    col = {h: i for i, h in enumerate(head) if h}

    def cell(row: List[Any], name: str) -> Any:
        i = col.get(name.lower())
        return row[i] if i is not None and i < len(row) else ""
    for row in values[1:]:
        sym = str(cell(row, "Symbol") or "").strip().upper()
        if not sym or sym == "SYMBOL" or sym in out:
            continue
        if _ticker_guard_enabled() and not _is_ticker_shaped(sym):
            continue
        ex = _future_date(cell(row, "Ex-Div Date"))
        if ex is None:
            continue
        text = lambda name: str(cell(row, name) or "").strip() or None  # noqa: E731
        out[sym] = {"ex_date": ex, "amount": _to_num(cell(row, "Amount")),
                    "currency": text("Currency"), "pay_date": text("Pay Date"),
                    "record_date": text("Record Date"), "period": text("Period"),
                    "asof": _prior_asof(cell(row, "Updated At (Riyadh)")),
                    "source": str(cell(row, "Source") or "").strip()}
    return out


def merge_dividends(symbols: List[str],
                    fresh: Dict[str, Dict[str, Any]],
                    prior: Dict[str, Dict[str, Any]]
                    ) -> Tuple[Dict[str, Dict[str, Any]], int, int]:
    """v1.3.0: (merged, n_fresh, n_carried). Fresh provider data wins when it
    carries a future ex-date; a prior row fills a harvested symbol the fresh
    fetch left empty while its ex-date is still in the future; a symbol
    outside `symbols` is never added."""
    merged: Dict[str, Dict[str, Any]] = {}
    n_fresh = n_carried = 0
    for s in symbols:
        f = (fresh or {}).get(s) or {}
        ex = _future_date(f.get("ex_date"))
        if ex:
            merged[s] = dict(f, ex_date=ex, carried=False)
            n_fresh += 1
            continue
        p = (prior or {}).get(s) or {}
        ex = _future_date(p.get("ex_date"))
        if ex:
            merged[s] = dict(p, ex_date=ex, carried=True)
            n_carried += 1
    return merged, n_fresh, n_carried


def build_div_rows(symbols: List[str], merged: Dict[str, Dict[str, Any]],
                   source: str) -> List[List[Any]]:
    """v1.3.0: one Dividend_Events row per symbol with a known dividend, in
    harvest order. Amount is a number (or '' when unknown)."""
    stamp = _dt.datetime.now(_RIYADH).strftime("%Y-%m-%d %H:%M")
    rows: List[List[Any]] = []
    for s in symbols:
        m = merged.get(s)
        if not m:
            continue
        amount = m.get("amount")
        amount = "" if amount is None else round(float(amount), 6)
        if m.get("carried"):
            prior_source = str(m.get("source") or "").strip().removesuffix(" +carried")
            row_src = (prior_source or "prior source unknown") + " +carried"
            row_stamp = _prior_asof(m.get("asof"))
        else:
            row_src, row_stamp = source, stamp
        rows.append([s, m["ex_date"], _days_until(m["ex_date"]), amount,
                     m.get("currency") or "", m.get("pay_date") or "",
                     m.get("record_date") or "", m.get("period") or "",
                     row_stamp, row_src])
    return rows


def dividend_lane(book: Any, symbols: List[str], write: bool) -> int:
    """v1.3.0: fetch, merge and (write mode) publish the dividend-detail tab
    in one atomic A1:J update. Returns 0 (published, dry run or left as is)
    or 2/3 (prior unreadable / publication unconfirmed). Never raises."""
    tab = _dividends_sheet()
    width = len(DIV_HEADERS)
    last_col = chr(ord("A") + width - 1)
    try:
        from core.providers.calendar_provider import (  # noqa: PLC0415
            fetch_next_exdiv_detail_sync, is_enabled, __version__ as pv)
    except Exception as e:
        _out(f"dividends: provider unavailable ({e}) - {tab} left as is")
        return 0
    if not is_enabled():
        _out(f"dividends: provider disabled - {tab} left as is")
        return 0
    prior: Dict[str, Dict[str, Any]] = {}
    prior_extent = 0
    ws = None
    try:
        ws = book.worksheet(tab)
        rows_alloc = getattr(ws, "row_count", None)
        cols_alloc = getattr(ws, "col_count", None)
        if isinstance(rows_alloc, int) and rows_alloc > _MAX_CALENDAR_BODY_ROWS + 1:
            _out(f"dividends: ERROR prior {tab} exceeds the bounded table - preserving tab")
            return 2
        read_rows = rows_alloc if isinstance(rows_alloc, int) and rows_alloc > 0 \
            else _MAX_CALENDAR_BODY_ROWS + 2
        read_cols = min(width, cols_alloc) if isinstance(cols_alloc, int) and cols_alloc > 0 else width
        prior_values = ws.get(f"A1:{chr(ord('A') + read_cols - 1)}{read_rows}")
        prior_extent = max(min(read_rows, _MAX_CALENDAR_BODY_ROWS + 1), len(prior_values))
        prior = parse_prior_div(prior_values)
    except Exception as e:
        if type(e).__name__ != "WorksheetNotFound":
            _out(f"dividends: ERROR prior {tab} unreadable ({e}) - preserving tab")
            return 2
        ws = None
    source = f"eodhd /div via calendar_provider v{pv}"
    try:
        fresh = fetch_next_exdiv_detail_sync(symbols) or {}
    except Exception as e:
        _out(f"dividends: WARN fetch failed ({e}) - prior rows only")
        fresh = {}
    merged, n_fresh, n_carried = merge_dividends(symbols, fresh, prior)
    rows = build_div_rows(symbols, merged, source)
    _out(f"dividends: with={len(rows)} fresh={n_fresh} carried={n_carried} -> {tab}")
    if not write:
        for r in rows[:15]:
            _out("  div " + " | ".join(str(c) for c in r))
        return 0
    try:
        extent = max(1000, len(rows) + 1, prior_extent)
        if ws is None:
            ws = book.add_worksheet(title=tab, rows=extent, cols=width)
        else:
            rows_alloc = getattr(ws, "row_count", None)
            cols_alloc = getattr(ws, "col_count", None)
            if isinstance(rows_alloc, int) and rows_alloc < extent:
                ws.resize(rows=extent)
            if isinstance(cols_alloc, int) and cols_alloc < width:
                ws.resize(cols=width)
        values = [list(DIV_HEADERS)] + rows + [
            [""] * width for _ in range(extent - len(rows) - 1)]
        ws.update(values=values, range_name=f"A1:{last_col}{extent}",
                  value_input_option="RAW")
        try:
            ws.freeze(rows=1)
        except Exception:
            pass
        _out(f"dividends: wrote {len(rows)} rows -> {tab}")
        return 0
    except Exception as e:
        _out(f"dividends: ERROR publication unconfirmed: {e}; read back before retrying")
        return 3


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

    # v1.2.0: the last-report lane (DEFAULT OFF = v1.1.4 byte for byte).
    lr_on = _last_report_enabled()
    lr_window = _last_report_window()
    prior_last: Dict[str, Dict[str, Any]] = {}

    # Read one sentinel row beyond the supported full table. Never replace a
    # truncated prior table: that would erase unseen future sticky events.
    prior: Dict[str, Dict[str, str]] = {}
    prior_extent = 0
    prior_sheet = None
    try:
        prior_sheet = book.worksheet(tab)
        allocated_rows = getattr(prior_sheet, "row_count", None)
        if isinstance(allocated_rows, int) and allocated_rows > _MAX_CALENDAR_BODY_ROWS + 1:
            _out("ERROR: prior calendar grid exceeds bounded full-table capacity — preserving tab")
            return 2
        read_rows = allocated_rows if isinstance(allocated_rows, int) and allocated_rows > 0 \
            else _MAX_CALENDAR_BODY_ROWS + 2
        # v1.2.0: widen the read only when the tab already holds the
        # last-report columns (a read beyond the grid would fail).
        prior_cols = getattr(prior_sheet, "col_count", None)
        read_last = "J" if (lr_on and isinstance(prior_cols, int)
                            and prior_cols >= len(HEADERS) + len(LAST_REPORT_HEADERS)) else "G"
        prior_values = prior_sheet.get(f"A1:{read_last}{read_rows}")
        if len(prior_values) > _MAX_CALENDAR_BODY_ROWS + 1:
            _out("ERROR: prior calendar exceeds bounded full-table capacity — preserving tab")
            return 2
        prior_extent = max(min(read_rows, _MAX_CALENDAR_BODY_ROWS + 1), len(prior_values))
        prior = parse_prior(prior_values)
        if lr_on:
            prior_last = parse_prior_last(prior_values, lr_window)
    except Exception as e:
        _out(f"WARN: prior-tab read failed ({e}) — no carry this run")
        if write and type(e).__name__ != "WorksheetNotFound":
            _out("ERROR: cannot establish prior event provenance — preserving tab")
            return 2
    symbols, ctx, carried, n_fill, n_res = apply_sticky(symbols, ctx, prior)
    if len(symbols) > _MAX_CALENDAR_BODY_ROWS:
        _out("ERROR: merged calendar exceeds bounded full-table capacity — preserving tab")
        return 2
    rows = build_rows(symbols, ctx, source, carried)
    filled = sum(1 for r in rows if r[1] or r[3])
    _out(f"rows: {len(rows)} | with at least one date: {filled} | "
         f"carried={n_fill} resurrected={n_res} | "
         f"prior_junk_purged={getattr(parse_prior, 'junk_purged', 0)}")
    headers = list(HEADERS)
    if lr_on:
        fresh_last: Dict[str, Dict[str, Any]] = {}
        try:
            from core.providers.calendar_provider import (  # noqa: PLC0415
                fetch_last_earnings_sync, is_enabled as _lr_enabled)
            if _lr_enabled():
                fresh_last = fetch_last_earnings_sync(symbols, lr_window) or {}
        except Exception as e:
            _out(f"WARN: last-report fetch failed ({e}) - prior reports only")
        merged_last, n_lf, n_lc = merge_last_report(symbols, fresh_last, prior_last, lr_window)
        rows = [r + last_report_cells(r[0], merged_last) for r in rows]
        headers += LAST_REPORT_HEADERS
        _out(f"last_report: with={len(merged_last)} fresh={n_lf} carried={n_lc} "
             f"window={lr_window}d")
    last_col = "J" if lr_on else "G"

    if not write:
        for r in rows[:15]:
            _out("  " + " | ".join(str(c) for c in r))
        if _dividends_enabled():   # v1.3.0 (default off)
            dividend_lane(book, symbols, write=False)
        _out("dry-run — nothing written (pass --write to publish)")
        return 0

    # v1.2.0 review fix: lane off on a tab an earlier ON run widened -> blank
    # H:J (header included) in the same atomic update, so the old reports can
    # never be carried onto rows that moved under them.
    if not lr_on and prior_sheet is not None and _stale_last_report_cols(prior_sheet):
        headers += [""] * len(LAST_REPORT_HEADERS)
        rows = [r + [""] * len(LAST_REPORT_HEADERS) for r in rows]
        last_col = "J"
        _out("last_report: off - clearing H:J left by an earlier ON run")

    try:
        extent = max(1000, len(rows) + 1, prior_extent)
        ws = prior_sheet
        if ws is None:
            ws = book.add_worksheet(title=tab, rows=extent, cols=len(headers))
        else:
            allocated_rows = getattr(ws, "row_count", None)
            if isinstance(allocated_rows, int) and allocated_rows < extent:
                # Growing the grid preserves all existing cells. A cancelled
                # or rejected publication after resize still has its old facts.
                ws.resize(rows=extent)
            # v1.2.0: the last-report columns need a 10-column grid.
            allocated_cols = getattr(ws, "col_count", None)
            if lr_on and isinstance(allocated_cols, int) and allocated_cols < len(headers):
                ws.resize(cols=len(headers))
        values = [list(headers)] + rows + [
            [""] * len(headers) for _ in range(extent - len(rows) - 1)]
        # Sheets applies one Values.update atomically. Include explicit blanks
        # through the entire old extent: null/omitted cells would retain stale
        # values. Do not clear separately or retry after an ambiguous response;
        # acknowledgement loss may mean the complete replacement already won.
        ws.update(values=values, range_name=f"A1:{last_col}{extent}",
                  value_input_option="RAW")
        try:
            ws.freeze(rows=1)
        except Exception:
            pass
        _out(f"wrote {len(rows)} rows -> {tab}")
    except Exception as e:
        _out(f"ERROR: sheet publication unconfirmed: {e}; read back before retrying")
        return 3
    if _dividends_enabled():   # v1.3.0 (default off): after the calendar publication
        return dividend_lane(book, symbols, write=True)
    return 0




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
                   and by["NEW.US"][6] == "eodhd v1.2.0 [events unknown:ex-div]"))
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
    checks.append(("shape edge cases: suffix required, bounded canonical class roots",
                   _is_ticker_shaped("1050.SR") and _is_ticker_shaped("0405.HK")
                   and _is_ticker_shaped("MPHASIS.NS")
                   and _is_ticker_shaped("GRT-UN.TO")
                   and _is_ticker_shaped("BRK.B.US")
                   and not _is_ticker_shaped("FICO")
                   and not _is_ticker_shaped("GC=F")
                   and not _is_ticker_shaped("BRK-B")
                   and not _is_ticker_shaped("TOOLONGNAME.US")))
    # --- v1.2.0: last-report lane (pure helpers; gate parsing) ---
    d3 = (today - _dt.timedelta(days=3)).isoformat()
    d30 = (today - _dt.timedelta(days=30)).isoformat()
    lr_tab = [HEADERS + LAST_REPORT_HEADERS,
              ["FRST.US", "", "", "", "", "x", "eodhd", d3, 3, -22.2],
              ["OLD.US", "", "", "", "", "x", "eodhd", d30, 30, 5.0],
              ["FORECAST", "", "", "", "", "x", "eodhd", d3, 3, 1.0]]
    pl = parse_prior_last(lr_tab, 21)
    checks.append(("last report: prior inside the window kept, expired/junk dropped",
                   list(pl) == ["FRST.US"] and pl["FRST.US"] == {"date": d3, "surprise": -22.2}
                   and parse_prior_last(tab, 21) == {}))
    merged, nf2, nc2 = merge_last_report(
        ["FRST.US", "NEW.US", "GONE.US"],
        {"NEW.US": {"last_earnings_date": d3, "eps_surprise_pct": 4.04},
         "GONE.US": {"last_earnings_date": d30, "eps_surprise_pct": 1.0}},
        {"FRST.US": pl["FRST.US"], "LOST.US": {"date": d3, "surprise": 1.0}}, 21)
    checks.append(("last report: fresh wins, prior fills, out-of-window and unharvested ignored",
                   sorted(merged) == ["FRST.US", "NEW.US"] and nf2 == 1 and nc2 == 1
                   and merged["FRST.US"]["carried"] is True
                   and merged["NEW.US"]["carried"] is False))
    checks.append(("last report: cells (date, days since, surprise rounded 0.1; blanks)",
                   last_report_cells("NEW.US", merged) == [d3, 3, 4.0]
                   and last_report_cells("ABSENT.US", merged) == ["", "", ""]))
    saved_lr = os.environ.get("TFB_CAL_LAST_REPORT")
    states = []
    for v in (None, "", "off", "observe", "on", "1", "TRUE"):
        if v is None:
            os.environ.pop("TFB_CAL_LAST_REPORT", None)
        else:
            os.environ["TFB_CAL_LAST_REPORT"] = v
        states.append(_last_report_enabled())
    if saved_lr is None:
        os.environ.pop("TFB_CAL_LAST_REPORT", None)
    else:
        os.environ["TFB_CAL_LAST_REPORT"] = saved_lr
    checks.append(("last report: gate default off; on/1/true arm it",
                   states == [False, False, False, False, True, True, True]))
    passed = sum(1 for _, ok in checks if ok)
    for name, ok in checks:
        print(("PASS " if ok else "FAIL ") + name)
    print(f"[calendar_sync v{__version__}] SELFTEST {passed}/{len(checks)}")
    return 0 if passed == len(checks) else 1


if __name__ == "__main__":
    raise SystemExit(main())
