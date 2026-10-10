#!/usr/bin/env python3
# scripts/run_calendar_sync.py
"""
================================================================================
Calendar Sync — v1.2.0 (per-event evidence, off request path)
================================================================================
NEW script (owner greenlight 2026-07-05: "go with the recommended one").

WHAT THIS DOES
    Once a day (GitHub Actions: calendar_sync.yml), for the DECISION symbols:
      1. harvests symbols from the pages in TFB_CALENDAR_PAGES
         (default: Top_10_Investments + My_Portfolio),
      2. calls core.providers.calendar_provider.fetch_event_evidence_sync()
         (EODHD primary dates and independent Yahoo fallback fields),
      3. atomically REPLACES the Calendar_Events tab with one row per symbol:
         Symbol | Next Earnings Date | Days To Earnings | Next Ex-Div Date |
         Days To ExDiv | Updated At (Riyadh) | Source, followed by each event's
         Source | Observed At (UTC) | Evidence Status. The first seven columns
         remain compatible. Reported means provider-reported, not confirmed by
         an issuer; Yahoo earnings dates are estimated.
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
    Missing EODHD key permits Yahoo-only fallback. Disabled/unavailable sources
    leave unknown fields; prior future dates retain their original independent
    evidence. Unreadable or ambiguous prior tables prevent publication. One
    atomic values request replaces the full header/body/tail. A lost response
    is reported as unconfirmed and is never blindly retried.

ENV
    DEFAULT_SPREADSHEET_ID / SPREADSHEET_ID   production workbook id
    GOOGLE_SHEETS_CREDENTIALS                 service-account JSON (or base64)
    TFB_CALENDAR_ENABLED=1                  enable the calendar provider
    EODHD_API_KEY                           optional primary-provider key
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
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from zoneinfo import ZoneInfo

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from core.calendar_evidence import CALENDAR_HEADERS, normalize_event_evidence

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
# v1.2.0: publish independent source/UTC observation/evidence fields, retaining
# the original seven columns and every carried field's original evidence.
__version__ = "1.2.0"
_RIYADH = ZoneInfo("Asia/Riyadh")
_MAX_CALENDAR_BODY_ROWS = 5000

HEADERS = list(CALENDAR_HEADERS)


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


def _event_evidence(ctx: Dict[str, Any], prefix: str, event_date: Optional[str]) -> Dict[str, str]:
    """Closed typed evidence; legacy display notes never establish a source."""
    return normalize_event_evidence(ctx, prefix, event_date)


def _nonblank(value: Any) -> bool:
    """Zero and False are owned data, not empty extension cells."""
    return value is not None and bool(str(value).strip())


def _validate_prior_schema(values: List[List[Any]], header_index: int) -> None:
    header = [str(cell if cell is not None else "").strip().lower()
              for cell in values[header_index]]
    expected = [heading.lower() for heading in HEADERS]
    if header[:len(HEADERS)] == expected and not any(header[len(HEADERS):]):
        return
    if header[:7] != expected[:7] or any(header[7:]):
        raise ValueError("unrecognized prior calendar schema")
    # A seven-column header does not establish ownership of the extension.
    # Read the complete allocated table before claiming its new H:M columns.
    if any(_nonblank(cell) for row in values for cell in row[7:len(HEADERS)]):
        raise ValueError("legacy calendar evidence columns contain unowned data")


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
    evidence_columns: Dict[str, int] = {}
    evidence_headers = dict(zip(HEADERS[7:], (
        "earnings_source", "earnings_observed_at", "earnings_status",
        "exdiv_source", "exdiv_observed_at", "exdiv_status")))
    for i, row in enumerate(values[:5]):
        low = [str(c or "").strip().lower() for c in row]
        if "symbol" in low:
            nonempty = [h for h in low if h]
            if len(nonempty) != len(set(nonempty)):
                raise ValueError("duplicate prior calendar headers")
            hdr_i, cs = i, low.index("symbol")
            for j, h in enumerate(low):
                for label, key in evidence_headers.items():
                    if h == label.lower():
                        evidence_columns[key] = j
                if h == "next earnings date":
                    ce = j
                elif h in ("next ex-div date", "next ex div date"):
                    cx = j
                elif h == "updated at (riyadh)":
                    ca = j
                elif h == "source":
                    cp = j
            break
    if hdr_i < 0 or cs < 0:
        if any(_nonblank(cell) for row in values for cell in row):
            raise ValueError("unrecognized prior calendar schema")
        return out
    if ce < 0 or cx < 0:
        raise ValueError("missing prior calendar date headers")
    _validate_prior_schema(values, hdr_i)
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
            typed = {key: row[index] if index < len(row) else ""
                     for key, index in evidence_columns.items()}
            rec.update(_event_evidence(typed, "earnings", rec.get("e")))
            rec.update(_event_evidence(typed, "exdiv", rec.get("x")))
            if sym in out and out[sym] != rec:
                raise ValueError(f"conflicting prior calendar rows for {sym}")
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
        c.update(_event_evidence(c, "earnings", c.get("next_earnings_date")))
        c.update(_event_evidence(c, "exdiv", c.get("next_ex_div_date")))
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
                prefix = "earnings" if prior_key == "e" else "exdiv"
                c.update(_event_evidence(p, prefix, d))
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
        ctx_out[s].update(_event_evidence(p, "earnings", e))
        ctx_out[s].update(_event_evidence(p, "exdiv", x))
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
        e, x = _future_date(c.get("next_earnings_date")), _future_date(c.get("next_ex_div_date"))
        typed = {**_event_evidence(c, "earnings", e), **_event_evidence(c, "exdiv", x)}
        row_src, row_stamp = source, stamp if (e or x) else ""
        known_sources = [f"{label}: {typed[prefix + '_source']} ({typed[prefix + '_status']})"
                         for prefix, d, label in (("earnings", e, "earnings"), ("exdiv", x, "ex-div"))
                         if d and typed[prefix + "_source"] != "unknown"]
        if known_sources:
            row_src = "; ".join(known_sources)
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
            if known_sources:
                # Typed fields are authoritative for the readable summary.
                # Rebuilding it avoids recursively nesting mixed carry notes
                # and attributing one field's source to its sibling.
                parts = []
                for key, prefix, d, label in (("e", "earnings", e, "earnings"),
                                              ("x", "exdiv", x, "ex-div")):
                    if not d:
                        continue
                    kind = "carried" if key in fields else "fresh"
                    parts.append(f"{kind} {label}: {typed[prefix + '_source']} "
                                 f"({typed[prefix + '_status']})")
                row_src = "; ".join(parts) + " +carried"
        # Missing facts are not a neutral event result or a successful lookup.
        # This is derived row status, not provider provenance; remove the prior
        # annotation on roundtrip so newly known fields do not retain old status.
        unknown = [label for d, label in ((e, "earnings"), (x, "ex-div")) if not d]
        if unknown:
            suffix = " +carried" if row_src.endswith(" +carried") else ""
            row_src = row_src.removesuffix(suffix) if suffix else row_src
            row_src += f" [events unknown:{'/'.join(unknown)}]" + suffix
        rows.append([s, e or "", _days_until(e), x or "", _days_until(x),
                     row_stamp, row_src,
                     typed["earnings_source"], typed["earnings_observed_at"], typed["earnings_status"],
                     typed["exdiv_source"], typed["exdiv_observed_at"], typed["exdiv_status"]])
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

    source = "calendar provider — evidence unknown"
    try:
        from core.providers.calendar_provider import (  # noqa: PLC0415
            fetch_event_evidence_sync, __version__ as pv)
        # The merged provider gates its layer and sources independently. A
        # missing EODHD key still permits its documented Yahoo-only fallback.
        ctx = fetch_event_evidence_sync(symbols)
        source = f"calendar_provider v{pv} — evidence unknown"
    except Exception as e:
        _out(f"WARN: provider failed ({e}) — writing blank dates")
        ctx, source = {}, f"provider error — blank dates"

    # Read one sentinel row beyond the supported full table. Never replace a
    # truncated prior table: that would erase unseen future sticky events.
    prior: Dict[str, Dict[str, str]] = {}
    prior_extent = 0
    prior_sheet = None
    try:
        prior_sheet = book.worksheet(tab)
        allocated_rows = getattr(prior_sheet, "row_count", None)
        allocated_cols = getattr(prior_sheet, "col_count", None)
        if write and (type(allocated_rows) is not int or allocated_rows <= 0
                      or type(allocated_cols) is not int or allocated_cols <= 0):
            _out("ERROR: cannot establish complete prior grid extent — preserving tab")
            return 2
        if isinstance(allocated_rows, int) and allocated_rows > _MAX_CALENDAR_BODY_ROWS + 1:
            _out("ERROR: prior calendar grid exceeds bounded full-table capacity — preserving tab")
            return 2
        read_rows = allocated_rows if isinstance(allocated_rows, int) and allocated_rows > 0 \
            else _MAX_CALENDAR_BODY_ROWS + 2
        read_cols = min(allocated_cols, len(HEADERS)) if isinstance(allocated_cols, int) \
            and allocated_cols > 0 else len(HEADERS)
        read_end = chr(ord("A") + read_cols - 1)
        prior_values = prior_sheet.get(f"A1:{read_end}{read_rows}")
        if len(prior_values) > _MAX_CALENDAR_BODY_ROWS + 1:
            _out("ERROR: prior calendar exceeds bounded full-table capacity — preserving tab")
            return 2
        prior_extent = max(min(read_rows, _MAX_CALENDAR_BODY_ROWS + 1), len(prior_values))
        prior = parse_prior(prior_values)
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

    if not write:
        for r in rows[:15]:
            _out("  " + " | ".join(str(c) for c in r))
        _out("dry-run — nothing written (pass --write to publish)")
        return 0

    try:
        extent = max(1000, len(rows) + 1, prior_extent)
        ws = prior_sheet
        if ws is None:
            ws = book.add_worksheet(title=tab, rows=extent, cols=len(HEADERS))
        else:
            allocated_rows = getattr(ws, "row_count", None)
            allocated_cols = getattr(ws, "col_count", None)
            grow_rows = isinstance(allocated_rows, int) and allocated_rows < extent
            grow_cols = isinstance(allocated_cols, int) and allocated_cols < len(HEADERS)
            if grow_rows or grow_cols:
                # Growing the grid preserves all existing cells. A cancelled
                # or rejected publication after resize still has its old facts.
                dimensions = {}
                if grow_rows:
                    dimensions["rows"] = extent
                if grow_cols:
                    dimensions["cols"] = len(HEADERS)
                ws.resize(**dimensions)
        values = [list(HEADERS)] + rows + [
            [""] * len(HEADERS) for _ in range(extent - len(rows) - 1)]
        # Sheets applies one Values.update atomically. Include explicit blanks
        # through the entire old extent: null/omitted cells would retain stale
        # values. Do not clear separately or retry after an ambiguous response;
        # acknowledgement loss may mean the complete replacement already won.
        ws.update(values=values, range_name=f"A1:M{extent}",
                  value_input_option="RAW")
        try:
            ws.freeze(rows=1)
        except Exception:
            pass
        _out(f"wrote {len(rows)} rows -> {tab}")
        return 0
    except Exception as e:
        _out(f"ERROR: sheet publication unconfirmed: {e}; read back before retrying")
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
    passed = sum(1 for _, ok in checks if ok)
    for name, ok in checks:
        print(("PASS " if ok else "FAIL ") + name)
    print(f"[calendar_sync v{__version__}] SELFTEST {passed}/{len(checks)}")
    return 0 if passed == len(checks) else 1


if __name__ == "__main__":
    raise SystemExit(main())
