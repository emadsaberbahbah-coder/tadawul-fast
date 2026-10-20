#!/usr/bin/env python3
"""scripts/universe_maintenance.py -- Global_Markets universe maintenance v1.0.0 (2026-10-20)

WHY (owner request, 20 Oct 2026): the Global_Markets universe is maintained by hand (the 18 Oct
Top-200 additions paste, the 196-row retirement list of the same day). Dead tickers stay on the
page, fail every provider leg and pull the coverage gates down; important new companies arrive only
when someone remembers. The owner asked for automatic additions with the US market first, and for
dead rows to be flagged red (27_Decision_Pages_v2 v1.2.24) and removed once confirmed dead.

WHAT (one run = three parts; no other tab of the workbook changes):
  1. WATCH (every run). A Global_Markets row the sync could not read is "red" -- the same predicate
     as the v1.2.24 red-row rule: the Warnings cell carries acquisition_status:unavailable|preserved,
     identity_quarantined or quote_current_price_missing, or Last Updated (UTC) is blank or older
     than TFB_UNIVERSE_STALE_DAYS (2) days. `_Universe_Watch` keeps one streak per red symbol: +1 on
     each weekday (UTC) it is seen red; the first run that reads it well again drops it (the streak
     restarts from zero, exactly like the red colour clears).
  2. RETIRE (every run). A `.US` row red for >= TFB_UNIVERSE_RED_DAYS (10) trading days that is
     ABSENT from EODHD's active US exchange list (delisted, merged or renamed) is appended to
     `_Retired_Symbols` with Page = Global_Markets. The sync's own retirement seam
     (run_dashboard_sync v6.64.0) removes it on its next write when TFB_SYNC_RETIRE_TAB=enforce.
     Never: a symbol held on My_Portfolio, a symbol still on the active list (a data problem, not a
     delisting), a non-US row (no confirmable list in v1.0.0 -- reported for a manual check), more
     than TFB_UNIVERSE_RETIRE_MAX (25) per run, or below TFB_UNIVERSE_MIN_ROWS (6,512 = the
     publication floor of finalize_decision_publication.py).
  3. ADD (once a week, from TFB_UNIVERSE_ADD_WEEKDAY = SUN, Riyadh). The largest US common stocks by
     market cap (EODHD screener, top TFB_UNIVERSE_SCREEN_N = 1,000) that are missing from the page
     are appended to its column A, at most TFB_UNIVERSE_ADD_MAX (25) per week and never above
     TFB_UNIVERSE_MAX_ROWS (6,750 = the name-identity roster bound). Not added: a symbol already on
     the page (also as a bare ticker) or on `_Retired_Symbols`; the same company under another line,
     ADR or share class (normalised-name match against every row); anything that is not a Common
     Stock on NYSE / Nasdaq / NYSE American / Arca / Cboe BZX (no OTC); a foreign issuer
     (fundamentals country not US) unless TFB_UNIVERSE_ADD_FOREIGN=1; Israeli-domiciled or
     Israeli-founded companies (owner policy 28 Jul 2026: ISIN IL, fundamentals country or address
     Israel, or config/universe_exclusions.json).

MODES -- TFB_UNIVERSE_MAINT = off (DEFAULT: nothing is read or written) | observe | enforce.
  observe  computes everything and writes only its own tabs (`_Universe_Watch`, `_Universe_Report`)
           plus one `_Run_Log` row. Global_Markets and `_Retired_Symbols` are untouched; the report
           lists what enforce would add and retire.
  enforce  also appends the additions to Global_Markets column A and the retirements to
           `_Retired_Symbols`. Degrades to observe when the pure self-test fails.
The workflow skips a run while a Daily Sync is queued or in progress, so the sync never rewrites the
page under this job. Rollback: TFB_UNIVERSE_MAINT=off. Appended rows are ordinary rows (delete them,
or list them on `_Retired_Symbols`); retired rows come back by deleting their `_Retired_Symbols` line
and pasting the symbol into Global_Markets column A.
"""
from __future__ import annotations

import argparse
import base64
import datetime as dt
import json
import os
import re
import sys
import tempfile
import time
import urllib.parse
import urllib.request
from pathlib import Path
from typing import Any, Callable, Dict, Iterable, List, Optional, Sequence, Tuple

SCRIPT_VERSION = "1.0.0"
PAGE = "Global_Markets"
WATCH_TAB = "_Universe_Watch"
REPORT_TAB = "_Universe_Report"
RUNLOG_TAB = "_Run_Log"
HELD_TAB = "My_Portfolio"
WATCH_HEADER = ["Symbol", "First Red (UTC)", "Last Red (UTC)", "Red Trading Days", "Reason", "Name"]
RETIRE_HEADER = ["Symbol", "Retired On", "Reason", "Replacement", "Page"]
STATE_LABEL = "Last additions run (Riyadh date)"
EXCLUSIONS_PATH = Path(__file__).resolve().parents[1] / "config" / "universe_exclusions.json"
WEEKDAYS = ("MON", "TUE", "WED", "THU", "FRI", "SAT", "SUN")
# Same venue set as scripts/name_identity_evidence.py US_VENUES (+ the legacy AMEX spelling).
US_VENUES = {"NYSE", "NASDAQ", "NYSE ARCA", "BATS", "NYSE MKT", "NYSE AMERICAN", "AMEX"}
US_LIST_MIN = 5000          # a shorter active US list is treated as a partial download
RIYADH = dt.timedelta(hours=3)

RED_RE = re.compile(r"acquisition_status:(unavailable|preserved)|identity_quarantined|quote_current_price_missing")
_US_CODE = re.compile(r"^[A-Z][A-Z0-9]{0,5}(-[A-Z])?$")
_NAME_DROP = {
    "inc", "incorporated", "corp", "corporation", "co", "company", "plc", "ltd", "limited", "llc", "lp",
    "holdings", "holding", "group", "sa", "ag", "nv", "se", "spa", "asa", "ab", "oyj", "adr", "ads",
    "class", "cl", "a", "b", "c", "ordinary", "shares", "share", "common", "stock", "the", "new",
    "sponsored", "depositary", "receipt", "receipts", "unit", "units",
}


# --------------------------------------------------------------------------- #
# configuration                                                               #
# --------------------------------------------------------------------------- #
def mode() -> str:
    raw = (os.getenv("TFB_UNIVERSE_MAINT") or "off").strip().lower()
    return raw if raw in ("off", "observe", "enforce") else "off"


def env_int(name: str, default: int, lo: int, hi: int) -> int:
    try:
        v = int(float((os.getenv(name) or "").strip()))
    except Exception:
        return default
    return max(lo, min(hi, v))


def settings() -> Dict[str, Any]:
    wd = (os.getenv("TFB_UNIVERSE_ADD_WEEKDAY") or "SUN").strip().upper()[:3]
    return {
        "mode": mode(),
        "add_max": env_int("TFB_UNIVERSE_ADD_MAX", 25, 0, 200),
        "retire_max": env_int("TFB_UNIVERSE_RETIRE_MAX", 25, 0, 200),
        "max_rows": env_int("TFB_UNIVERSE_MAX_ROWS", 6750, 1000, 20000),
        "min_rows": env_int("TFB_UNIVERSE_MIN_ROWS", 6512, 1, 20000),
        "red_days": env_int("TFB_UNIVERSE_RED_DAYS", 10, 3, 60),
        "stale_days": env_int("TFB_UNIVERSE_STALE_DAYS", 2, 1, 14),
        "screen_n": env_int("TFB_UNIVERSE_SCREEN_N", 1000, 100, 1000),
        "fund_max": env_int("TFB_UNIVERSE_FUNDAMENTALS_MAX", 120, 10, 500),
        "add_weekday": wd if wd in WEEKDAYS else "SUN",
        "add_foreign": (os.getenv("TFB_UNIVERSE_ADD_FOREIGN") or "0").strip().lower() in ("1", "true", "on", "yes"),
        "retire_tab": (os.getenv("TFB_SYNC_RETIRE_TAB_NAME") or "").strip() or "_Retired_Symbols",
        "retire_mode": (os.getenv("TFB_SYNC_RETIRE_TAB") or "off").strip().lower(),
    }


# --------------------------------------------------------------------------- #
# pure rules                                                                  #
# --------------------------------------------------------------------------- #
def is_red(warnings: Any, last_utc: Any, today: dt.date, stale_days: int = 2) -> Tuple[bool, str]:
    """(red, reason). Same predicate as the v1.2.24 conditional-format rule."""
    m = RED_RE.search(str(warnings or ""))
    if m:
        return True, m.group(0)
    d = re.match(r"^(\d{4})-(\d{2})-(\d{2})", str(last_utc or "").strip())
    if not d:
        return True, "never read (no Last Updated)"
    try:
        age = (today - dt.date(int(d.group(1)), int(d.group(2)), int(d.group(3)))).days
    except ValueError:
        return True, "unreadable Last Updated"
    if age > stale_days:
        return True, f"no successful update for {age} days"
    return False, ""


def norm_name(name: Any) -> str:
    """Company identity for duplicate detection across lines, ADRs and share classes."""
    s = re.sub(r"[^a-z0-9 ]+", " ", str(name or "").lower().replace("&", " and "))
    return " ".join(t for t in s.split() if t not in _NAME_DROP)


def sym_keys(sym: Any) -> set:
    """A symbol and its US twin spelling (DDI <-> DDI.US): both count as the same line."""
    s = str(sym or "").strip().upper()
    if not s:
        return set()
    out = {s}
    if s.endswith(".US"):
        out.add(s[:-3])
    elif "." not in s:
        out.add(s + ".US")
    return out


def key_set(symbols: Iterable[Any]) -> set:
    out: set = set()
    for s in symbols:
        out |= sym_keys(s)
    return out


def update_watch(prev: Dict[str, Dict[str, Any]], rows: Sequence[Dict[str, Any]], today: dt.date,
                 stale_days: int) -> Tuple[Dict[str, Dict[str, Any]], int]:
    """New streak state: a red row keeps or extends its streak (+1 on each new weekday); a row that
    reads well again leaves the watch, so its count restarts. Returns (state, red_today)."""
    out: Dict[str, Dict[str, Any]] = {}
    iso, weekday = today.isoformat(), today.weekday() < 5
    for r in rows:
        red, why = is_red(r.get("warnings"), r.get("last_utc"), today, stale_days)
        if not red:
            continue
        sym = r["symbol"]
        p = prev.get(sym) or {}
        days = int(p.get("days") or 0)
        last = str(p.get("last") or "")
        if weekday and last != iso:
            days += 1
            last = iso
        out[sym] = {"first": p.get("first") or iso, "last": last or iso, "days": days,
                    "reason": why, "name": r.get("name") or p.get("name") or ""}
    return out, len(out)


def plan_retire(watch: Dict[str, Dict[str, Any]], rows_on_page: int, held: Iterable[str],
                retired: Iterable[str], us_active: Optional[set], red_days: int, min_rows: int,
                retire_max: int) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    """(retire, kept). Kept = red long enough but not retired, each with the reason."""
    held_k, retired_k = key_set(held), key_set(retired)
    room = max(0, min(retire_max, rows_on_page - min_rows))
    retire: List[Dict[str, Any]] = []
    kept: List[Dict[str, Any]] = []
    for sym, w in sorted(watch.items(), key=lambda kv: (-int(kv[1].get("days") or 0), kv[0])):
        days = int(w.get("days") or 0)
        if days < red_days:
            continue
        su = sym.strip().upper()
        item = {"symbol": sym, "name": w.get("name", ""), "days": days, "reason": w.get("reason", "")}
        if su in retired_k:
            continue
        if sym_keys(su) & held_k:
            kept.append(dict(item, why="held on My_Portfolio - never retired"))
        elif not su.endswith(".US"):
            kept.append(dict(item, why="non-US line - no confirmable exchange list in v1.0.0; check by hand"))
        elif us_active is None:
            kept.append(dict(item, why="US exchange list unavailable this run - not confirmed"))
        elif su[:-3] in us_active:
            kept.append(dict(item, why="still on the active US list - a data problem, not a delisting"))
        elif len(retire) >= room:
            kept.append(dict(item, why=f"cap reached ({retire_max} per run, floor {min_rows} rows) - next run"))
        else:
            retire.append(dict(item, why="delisted: absent from the active US exchange list"))
    return retire, kept


def is_israeli(code: str, isin: Any, fundamentals: Optional[Dict[str, Any]], curated: Iterable[str]) -> Optional[str]:
    """Reason when the owner's 28 Jul 2026 exclusion applies, else None."""
    if code.upper() in {str(c).upper() for c in curated}:
        return "owner exclusion list (Israeli-founded)"
    if str(isin or "").strip().upper().startswith("IL"):
        return "ISIN IL (Israeli-domiciled)"
    g = (fundamentals or {}).get("General") if isinstance(fundamentals, dict) else None
    if isinstance(g, dict):
        if str(g.get("ISIN") or "").strip().upper().startswith("IL"):
            return "ISIN IL (Israeli-domiciled)"
        if str(g.get("CountryISO") or "").strip().upper() == "IL" or str(g.get("CountryName") or "").strip().lower() == "israel":
            return "fundamentals: country Israel"
        addr = g.get("AddressData") if isinstance(g.get("AddressData"), dict) else {}
        if str(addr.get("Country") or "").strip().lower() == "israel" or re.search(r"\bisrael\b", str(g.get("Address") or ""), re.I):
            return "fundamentals: headquarters in Israel"
    return None


def foreign_country(fundamentals: Optional[Dict[str, Any]]) -> Optional[str]:
    """The issuer country when it is not the US (ADRs, foreign private issuers), else None."""
    g = (fundamentals or {}).get("General") if isinstance(fundamentals, dict) else None
    if not isinstance(g, dict):
        return None
    iso = str(g.get("CountryISO") or "").strip().upper()
    return iso if iso and iso != "US" else None


def plan_add(candidates: Sequence[Dict[str, Any]], page_symbols: Iterable[str], page_names: Iterable[str],
             retired: Iterable[str], us_list: Dict[str, Dict[str, Any]], curated: Iterable[str],
             fundamentals_of: Callable[[str], Dict[str, Any]], cap: int, fund_max: int = 120,
             add_foreign: bool = False) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]], List[str]]:
    """(add, skipped, notes). candidates = screener rows ranked by market cap (Code, Name, MarketCap)."""
    have, retired_k = key_set(page_symbols), key_set(retired)
    names = {n for n in (norm_name(x) for x in page_names) if n}
    curated = list(curated)
    add: List[Dict[str, Any]] = []
    skipped: List[Dict[str, Any]] = []
    notes: List[str] = []
    fund_calls = 0
    for rank, c in enumerate(candidates, 1):
        if len(add) >= cap:
            break
        code = str(c.get("Code") or "").strip().upper()
        sym = code + ".US"
        if not _US_CODE.match(code) or sym in have:
            continue
        nm = str(c.get("Name") or "").strip()
        base = {"symbol": sym, "name": nm, "rank": rank, "market_cap": c.get("MarketCap")}
        nn = norm_name(nm)
        meta = us_list.get(code)
        if sym in retired_k:
            why = "listed on _Retired_Symbols"
        elif nn and nn in names:
            why = "same company already on the page (another line, ADR or share class)"
        elif meta is None or str(meta.get("Type") or "") != "Common Stock":
            why = "not a Common Stock on the active US list"
        elif str(meta.get("Exchange") or "").strip().upper() not in US_VENUES:
            why = f"venue {meta.get('Exchange') or '?'} is not NYSE / Nasdaq (OTC lines are never added)"
        else:
            why = None
            pol = is_israeli(code, meta.get("Isin"), None, curated)
            if pol:
                why = "policy: " + pol
        if why is None:
            if fund_calls >= fund_max:
                notes.append(f"fundamentals budget ({fund_max} calls) used - remaining candidates wait for next week")
                break
            fund_calls += 1
            try:
                fund = fundamentals_of(code)
            except Exception as e:  # noqa: BLE001
                fund = None
                why = f"fundamentals unavailable ({type(e).__name__}) - not added this week"
            if why is None:
                pol = is_israeli(code, meta.get("Isin"), fund, curated)
                g = (fund or {}).get("General") if isinstance(fund, dict) else None
                fc = foreign_country(fund)
                if pol:
                    why = "policy: " + pol
                elif not isinstance(g, dict) or not g:
                    why = "fundamentals empty - not added this week"
                elif str(g.get("IsDelisted") or "").strip().lower() in ("true", "1"):
                    why = "fundamentals: delisted"
                elif fc and not add_foreign:
                    why = f"foreign issuer ({fc}) - add by hand if its home line is not on the page"
        if why:
            skipped.append(dict(base, why=why))
            continue
        add.append(dict(base, why=f"US #{rank} by market cap, missing from {PAGE}"))
        have |= sym_keys(sym)
        if nn:
            names.add(nn)
    return add, skipped, notes


def add_room(rows_on_page: int, retiring: int, max_rows: int, add_max: int) -> int:
    return max(0, min(add_max, max_rows - (rows_on_page - retiring)))


def week_start(today_riyadh: dt.date, weekday: str) -> dt.date:
    """The most recent add weekday on or before today (the add week starts on it)."""
    return today_riyadh - dt.timedelta(days=(today_riyadh.weekday() - WEEKDAYS.index(weekday)) % 7)


def additions_due(today_riyadh: dt.date, weekday: str, last_add: Optional[dt.date], last_mode: str,
                  run_mode: str) -> bool:
    """Once per add week. A switch from observe to enforce runs the additions on the next run."""
    if last_add is None or last_add < week_start(today_riyadh, weekday):
        return True
    return run_mode == "enforce" and last_mode != "enforce"


def load_curated(path: Path = EXCLUSIONS_PATH) -> List[str]:
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return []
    out = []
    for item in data.get("israeli_founded_us_listed", []) if isinstance(data, dict) else []:
        code = str(item.get("code") if isinstance(item, dict) else item).strip().upper()
        if code:
            out.append(code)
    return out


# --------------------------------------------------------------------------- #
# EODHD (read-only)                                                           #
# --------------------------------------------------------------------------- #
class Eodhd:
    def __init__(self, key: str, base: str = "https://eodhd.com/api"):
        self.key = key
        self.base = base.rstrip("/")

    def _json(self, path: str, params: Dict[str, Any], timeout: float = 90.0) -> Any:
        q = dict(params, api_token=self.key, fmt="json")
        url = f"{self.base}/{path}?" + urllib.parse.urlencode(q)
        req = urllib.request.Request(url, headers={"User-Agent": f"TFB-universe-maintenance/{SCRIPT_VERSION}"})
        last: Optional[Exception] = None
        for attempt in range(2):
            try:
                with urllib.request.urlopen(req, timeout=timeout) as resp:  # noqa: S310 (fixed https host)
                    return json.loads(resp.read().decode("utf-8"))
            except Exception as e:  # noqa: BLE001 -- the URL (with the key) is never printed
                last = e
                time.sleep(2.0 * (attempt + 1))
        raise RuntimeError(f"EODHD {path.split('/')[0]} failed: {type(last).__name__}")

    def us_list(self) -> Dict[str, Dict[str, Any]]:
        data = self._json("exchange-symbol-list/US", {})
        out: Dict[str, Dict[str, Any]] = {}
        for r in data if isinstance(data, list) else []:
            code = str(r.get("Code") or "").strip().upper()
            if code:
                out[code] = {"Type": r.get("Type"), "Isin": r.get("Isin"), "Name": r.get("Name"),
                             "Exchange": r.get("Exchange")}
        return out

    def screener_us(self, want: int) -> List[Dict[str, Any]]:
        out: List[Dict[str, Any]] = []
        seen: set = set()
        offset = 0
        while len(out) < want and offset <= 900:          # the screener caps the offset at 999
            try:
                data = self._json("screener", {"sort": "market_capitalization.desc",
                                               "filters": json.dumps([["exchange", "=", "US"]]),
                                               "limit": 100, "offset": offset})
            except Exception:
                if not out:
                    raise
                break                                       # keep the ranked head we already have
            rows = data.get("data") if isinstance(data, dict) else None
            if not rows:
                break
            for r in rows:
                code = str(r.get("code") or r.get("Code") or "").strip().upper()
                if code and code not in seen:
                    seen.add(code)
                    out.append({"Code": code, "Name": r.get("name") or r.get("Name") or "",
                                "MarketCap": r.get("market_capitalization")})
            if len(rows) < 100:
                break
            offset += 100
            time.sleep(0.25)
        return out[:want]

    def fundamentals(self, code: str) -> Dict[str, Any]:
        g = self._json(f"fundamentals/{code}.US", {"filter": "General"})
        return {"General": g if isinstance(g, dict) else {}}


# --------------------------------------------------------------------------- #
# Sheets                                                                      #
# --------------------------------------------------------------------------- #
def open_sheet():
    import gspread
    from google.oauth2.service_account import Credentials
    sid = (os.getenv("DEFAULT_SPREADSHEET_ID") or os.getenv("TARGET_SHEET_ID") or "").strip()
    if not sid:
        raise SystemExit("::error::Spreadsheet ID missing (DEFAULT_SPREADSHEET_ID).")
    path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
    raw, b64 = os.getenv("GOOGLE_SHEETS_CREDENTIALS"), os.getenv("GOOGLE_SHEETS_CREDENTIALS_B64")
    if not path and (raw or b64):
        data = raw if (raw or "").lstrip().startswith("{") else base64.b64decode(raw or b64).decode("utf-8")
        f = tempfile.NamedTemporaryFile("w", suffix=".json", delete=False)
        f.write(data)
        f.close()
        path = f.name
    if not path:
        raise SystemExit("::error::Google credentials missing.")
    creds = Credentials.from_service_account_file(path, scopes=["https://www.googleapis.com/auth/spreadsheets"])
    return gspread.authorize(creds).open_by_key(sid)


def _col_letter(n: int) -> str:
    s = ""
    while n > 0:
        n, r = divmod(n - 1, 26)
        s = chr(65 + r) + s
    return s


def _worksheet(sh, name: str):
    try:
        return sh.worksheet(name)
    except Exception:  # noqa: BLE001 (gspread.WorksheetNotFound and transport errors alike)
        return None


def read_page(ws) -> Tuple[List[Dict[str, Any]], int]:
    """Global_Markets rows (Symbol, Name, Warnings, Last Updated (UTC)) and the last used row."""
    header = [str(h).strip() for h in ws.row_values(1)]
    need = ["Symbol", "Name", "Warnings", "Last Updated (UTC)"]
    missing = [n for n in need if n not in header]
    if missing:
        raise RuntimeError(f"{PAGE} header is missing {missing}")
    cols = ws.batch_get([f"{_col_letter(header.index(n) + 1)}2:{_col_letter(header.index(n) + 1)}" for n in need])
    flat = [[(r[0] if r else "") for r in c] for c in cols]
    rows: List[Dict[str, Any]] = []
    last = 1
    for i in range(max((len(c) for c in flat), default=0)):
        cell = lambda k: flat[k][i] if i < len(flat[k]) else ""  # noqa: E731
        sym = str(cell(0)).strip()
        if not sym:
            continue
        last = i + 2
        rows.append({"symbol": sym, "name": cell(1), "warnings": cell(2), "last_utc": cell(3)})
    return rows, last


def _header_row(values: List[List[Any]], label: str = "symbol") -> int:
    """Index of the first of the top 10 rows holding a `label` cell (title rows above are fine)."""
    for r, row in enumerate(values[:10]):
        if any(str(c).strip().lower() == label for c in row):
            return r
    return -1


def read_symbols(sh, tab: str) -> List[str]:
    ws = _worksheet(sh, tab)
    if ws is None:
        return []
    values = ws.get_all_values()
    h = _header_row(values)
    if h < 0:
        return []
    col = [str(c).strip().lower() for c in values[h]].index("symbol")
    return [str(r[col]).strip() for r in values[h + 1:] if col < len(r) and str(r[col]).strip()]


def read_watch(sh) -> Tuple[Dict[str, Dict[str, Any]], Tuple[int, int]]:
    ws = _worksheet(sh, WATCH_TAB)
    if ws is None:
        return {}, (0, 0)
    values = ws.get_all_values()
    out: Dict[str, Dict[str, Any]] = {}
    for r in values[1:]:
        if not r or not str(r[0]).strip():
            continue
        r = list(r) + [""] * (len(WATCH_HEADER) - len(r))
        try:
            days = int(float(r[3] or 0))
        except Exception:
            days = 0
        out[str(r[0]).strip()] = {"first": r[1], "last": r[2], "days": days, "reason": r[4], "name": r[5]}
    return out, (len(values), max((len(r) for r in values), default=0))


def read_state(sh) -> Tuple[Optional[dt.date], str, Tuple[int, int]]:
    """(last additions date, mode of that run, report extent) from the report's state row."""
    ws = _worksheet(sh, REPORT_TAB)
    if ws is None:
        return None, "", (0, 0)
    values = ws.get_all_values()
    extent = (len(values), max((len(r) for r in values), default=0))
    for r in values[:10]:
        if r and str(r[0]).strip() == STATE_LABEL:
            m = re.match(r"^(\d{4})-(\d{2})-(\d{2})", str(r[1] if len(r) > 1 else "").strip())
            last = dt.date(int(m.group(1)), int(m.group(2)), int(m.group(3))) if m else None
            return last, (str(r[3]).strip().lower() if len(r) > 3 else ""), extent
    return None, "", extent


def _write_rect(sh, name: str, body: List[List[Any]], prev: Tuple[int, int]) -> None:
    """One overwrite from A1, padded to the previous extent (never clear-then-write: a failed second
    call would leave the tab empty and lose every streak)."""
    width = max([len(r) for r in body] + [prev[1], 1])
    height = max(len(body), prev[0], 1)
    rect = [list(r) + [""] * (width - len(r)) for r in body] + [[""] * width for _ in range(height - len(body))]
    ws = _worksheet(sh, name)
    if ws is None:
        ws = sh.add_worksheet(title=name, rows=max(height + 50, 100), cols=max(width, 8))
    if ws.row_count < height:
        ws.add_rows(height - ws.row_count)
    if ws.col_count < width:
        ws.add_cols(width - ws.col_count)
    ws.update(values=rect, range_name="A1", value_input_option="RAW")


def write_watch(sh, watch: Dict[str, Dict[str, Any]], prev: Tuple[int, int]) -> None:
    body = [WATCH_HEADER] + [[s, w["first"], w["last"], w["days"], w["reason"], w.get("name", "")]
                             for s, w in sorted(watch.items(), key=lambda kv: (-kv[1]["days"], kv[0]))]
    _write_rect(sh, WATCH_TAB, body, prev)


def append_retired(sh, tab: str, items: Sequence[Dict[str, Any]], today: str) -> None:
    ws = _worksheet(sh, tab)
    if ws is None:
        ws = sh.add_worksheet(title=tab, rows=500, cols=len(RETIRE_HEADER))
        ws.update(values=[RETIRE_HEADER], range_name="A1", value_input_option="RAW")
        values = [RETIRE_HEADER]
    else:
        values = ws.get_all_values()
    h = _header_row(values)
    if h < 0:
        raise RuntimeError(f"{tab} has no Symbol header row - nothing appended")
    header = [str(c).strip().lower() for c in values[h]]
    rows = []
    for it in items:
        row = [""] * len(header)
        for k, v in (("symbol", it["symbol"]), ("retired on", today), ("page", PAGE),
                     ("reason", f"universe_maintenance v{SCRIPT_VERSION}: {it['why']} (red {it['days']} trading days)")):
            if k in header:
                row[header.index(k)] = v
        rows.append(row)
    ws.append_rows(rows, value_input_option="RAW")


def append_page_symbols(ws, last_row: int, symbols: Sequence[str]) -> None:
    if not symbols:
        return
    start, end = last_row + 1, last_row + len(symbols)
    if ws.row_count < end:
        ws.add_rows(end - ws.row_count)
    ws.update(values=[[s] for s in symbols], range_name=f"A{start}:A{end}", value_input_option="RAW")


def runlog(sh, now_utc: dt.datetime, status: str, msg: str, details: Dict[str, Any]) -> None:
    try:
        ws = sh.worksheet(RUNLOG_TAB)
        ws.append_row([now_utc.strftime("%Y-%m-%d %H:%M:%S"), "INFO" if status in ("OK", "OBSERVE") else "WARNING",
                       "universe_maintenance", PAGE, status, msg, "", "", "",
                       json.dumps(details, sort_keys=True)[:4000]], value_input_option="RAW")
    except Exception as e:  # noqa: BLE001
        print(f"::warning::_Run_Log row skipped: {type(e).__name__}")


# --------------------------------------------------------------------------- #
# run                                                                         #
# --------------------------------------------------------------------------- #
def run(sh, eod, cfg: Dict[str, Any], now_utc: dt.datetime, force_add: bool = False) -> Dict[str, Any]:
    today, today_r = now_utc.date(), (now_utc + RIYADH).date()
    out: Dict[str, Any] = {"version": SCRIPT_VERSION, "mode": cfg["mode"], "today_utc": today.isoformat(),
                           "rows": 0, "red_today": 0, "added": [], "retired": [], "skipped_add": [],
                           "kept_red": [], "notes": [], "additions_ran": False, "wrote": False}
    ws = sh.worksheet(PAGE)
    rows, last_row = read_page(ws)
    out["rows"] = n_rows = len(rows)
    if n_rows < cfg["min_rows"]:
        out["notes"].append(f"{PAGE} read {n_rows} rows (< floor {cfg['min_rows']}): partial read suspected - nothing written")
        return out
    held = read_symbols(sh, HELD_TAB)
    retired = read_symbols(sh, cfg["retire_tab"])
    prev_watch, watch_extent = read_watch(sh)
    watch, out["red_today"] = update_watch(prev_watch, rows, today, cfg["stale_days"])
    us_list: Optional[Dict[str, Dict[str, Any]]] = None
    try:
        us_list = eod.us_list()
        if len(us_list) < US_LIST_MIN:
            out["notes"].append(f"active US list has only {len(us_list)} codes - treated as unavailable")
            us_list = None
    except Exception as e:  # noqa: BLE001
        out["notes"].append(f"active US list unavailable ({type(e).__name__}) - no retirement or addition this run")
    retire, kept = plan_retire(watch, n_rows, held, retired, set(us_list) if us_list is not None else None,
                               cfg["red_days"], cfg["min_rows"], cfg["retire_max"])
    out["retired"], out["kept_red"] = retire, kept

    last_add, last_mode, report_extent = read_state(sh)
    state = {"date": last_add.isoformat() if last_add else "", "n": "", "mode": last_mode}
    due = force_add or additions_due(today_r, cfg["add_weekday"], last_add, last_mode, cfg["mode"])
    retiring = len(retire) if cfg["retire_mode"] == "enforce" else 0
    room = add_room(n_rows, retiring, cfg["max_rows"], cfg["add_max"])
    if not due:
        out["notes"].append(f"additions already ran this week ({state['date']}, {last_mode}); next week starts "
                            f"{(week_start(today_r, cfg['add_weekday']) + dt.timedelta(days=7)).isoformat()} (Riyadh)")
    elif us_list is None:
        out["notes"].append("additions postponed: the active US list is needed to vet candidates")
    elif room <= 0:
        out["notes"].append(f"no room for additions: {n_rows} rows, cap {cfg['max_rows']}")
        state = {"date": today_r.isoformat(), "n": 0, "mode": cfg["mode"]}
        out["additions_ran"] = True
    else:
        try:
            cands = eod.screener_us(cfg["screen_n"])
        except Exception as e:  # noqa: BLE001
            cands = None
            out["notes"].append(f"US screener unavailable ({type(e).__name__}) - additions retried next run")
        if cands is not None:
            adds, skipped, notes = plan_add(cands, [r["symbol"] for r in rows], [r["name"] for r in rows],
                                            retired, us_list, load_curated(), eod.fundamentals, room,
                                            cfg["fund_max"], cfg["add_foreign"])
            out["added"], out["skipped_add"] = adds, skipped
            out["notes"] += notes
            out["screened"] = len(cands)
            state = {"date": today_r.isoformat(), "n": len(adds), "mode": cfg["mode"]}
            out["additions_ran"] = True

    if cfg["mode"] == "enforce":
        if retire:
            append_retired(sh, cfg["retire_tab"], retire, today.isoformat())
            for it in retire:
                watch.pop(it["symbol"], None)
            if cfg["retire_mode"] != "enforce":
                out["notes"].append(f"TFB_SYNC_RETIRE_TAB={cfg['retire_mode']}: the sync removes listed rows only under enforce")
        if out["added"]:
            append_page_symbols(ws, last_row, [a["symbol"] for a in out["added"]])
    write_watch(sh, watch, watch_extent)
    _write_rect(sh, REPORT_TAB, report_lines(out, cfg, state), report_extent)
    out["wrote"] = True
    enf = cfg["mode"] == "enforce"
    runlog(sh, now_utc, "OK" if enf else "OBSERVE",
           f"[UNIVERSE-MAINT v{SCRIPT_VERSION}] mode={cfg['mode']} rows={n_rows} red={out['red_today']} "
           f"{'added' if enf else 'would_add'}={len(out['added'])} {'retired' if enf else 'would_retire'}={len(retire)}",
           {"added": [a["symbol"] for a in out["added"]], "retired": [r["symbol"] for r in retire],
            "kept_red": len(kept), "skipped_add": len(out["skipped_add"]), "notes": out["notes"]})
    return out


def report_lines(out: Dict[str, Any], cfg: Dict[str, Any], state: Dict[str, Any]) -> List[List[Any]]:
    enf = out["mode"] == "enforce"
    L: List[List[Any]] = [
        [f"UNIVERSE MAINTENANCE v{SCRIPT_VERSION} - {PAGE}", f"mode {out['mode']}", f"run {out['today_utc']} (UTC)",
         "observe writes only this tab and _Universe_Watch" if not enf else "enforce: additions and retirements applied"],
        [STATE_LABEL, state.get("date", ""), state.get("n", ""), state.get("mode", "")],
        ["rows", out["rows"], "red today", out["red_today"], "floor / cap", f"{cfg['min_rows']} / {cfg['max_rows']}"],
        [],
        ["ADDED" if enf else "WOULD ADD (observe)", "Name", "US rank by market cap", "Market cap", "Why"],
    ]
    L += [[a["symbol"], a["name"], a["rank"], a.get("market_cap") or "", a["why"]] for a in out["added"]] or [["(none)"]]
    L += [[], ["RETIRED" if enf else "WOULD RETIRE (observe)", "Name", "Red trading days", "Last red reason", "Why"]]
    L += [[r["symbol"], r["name"], r["days"], r["reason"], r["why"]] for r in out["retired"]] or [["(none)"]]
    L += [[], [f"RED {cfg['red_days']}+ TRADING DAYS, KEPT", "Name", "Red trading days", "Last red reason", "Why"]]
    L += [[r["symbol"], r["name"], r["days"], r["reason"], r["why"]] for r in out["kept_red"][:150]] or [["(none)"]]
    L += [[], ["NOT ADDED", "Name", "US rank", "Market cap", "Why"]]
    L += [[s["symbol"], s["name"], s["rank"], s.get("market_cap") or "", s["why"]] for s in out["skipped_add"][:150]] or [["(none)"]]
    L += [[], ["NOTES"]] + ([[n] for n in out["notes"]] or [["(none)"]])
    return L


# --------------------------------------------------------------------------- #
# self-test (pure; enforce degrades to observe when it fails)                 #
# --------------------------------------------------------------------------- #
def selftest() -> List[str]:
    bad: List[str] = []
    d = dt.date(2026, 10, 20)                         # a Tuesday
    if is_red("x; identity_quarantined:kept_last_good", "2026-10-20T01:00:00+00:00", d)[0] is not True:
        bad.append("red regex")
    if is_red("", "2026-10-18T23:00:00+00:00", d)[0] is not False:
        bad.append("2-day stale boundary")
    if is_red("", "2026-10-17T23:00:00+00:00", d)[0] is not True or is_red("", "", d)[0] is not True:
        bad.append("stale / blank Last Updated")
    if norm_name("Alphabet Inc. Class A") != norm_name("ALPHABET INC CLASS C"):
        bad.append("share-class name match")
    w, _ = update_watch({"X.US": {"first": "2026-10-01", "last": "2026-10-19", "days": 9}},
                        [{"symbol": "X.US", "warnings": "acquisition_status:unavailable", "last_utc": ""},
                         {"symbol": "Y.US", "warnings": "", "last_utc": "2026-10-20"}], d, 2)
    if w.get("X.US", {}).get("days") != 10 or "Y.US" in w:
        bad.append("watch streak")
    r, k = plan_retire({"X.US": {"days": 10}, "H.US": {"days": 12}, "L.US": {"days": 11}, "Z.L": {"days": 30}},
                       6600, ["H.US"], [], {"L", "AAPL"}, 10, 6512, 25)
    if [i["symbol"] for i in r] != ["X.US"] or len(k) != 3:
        bad.append("retire rules")
    if plan_retire({"X.US": {"days": 10}}, 6512, [], [], set(), 10, 6512, 25)[0]:
        bad.append("floor")
    if is_israeli("ABC", "IL0011", None, []) is None or is_israeli("ABC", "US0001", {"General": {"CountryISO": "US"}}, []) is not None:
        bad.append("policy")
    if additions_due(d, "SUN", dt.date(2026, 10, 18), "observe", "observe") or not additions_due(d, "SUN", dt.date(2026, 10, 17), "observe", "observe"):
        bad.append("weekly cadence")
    return bad


def main(argv: Optional[Sequence[str]] = None) -> int:
    ap = argparse.ArgumentParser(description="Global_Markets universe maintenance (see the module docstring).")
    ap.add_argument("--force-add", action="store_true", help="run the weekly additions on this run")
    ap.add_argument("--json-out", default="", help="write the full run result here (workflow artifact)")
    ap.add_argument("--selftest", action="store_true", help="run the pure self-test only")
    args = ap.parse_args(argv)
    if args.selftest:
        bad = selftest()
        print("selftest: " + ("PASS" if not bad else "FAIL " + ", ".join(bad)))
        return 0 if not bad else 1
    cfg = settings()
    if cfg["mode"] == "off":
        print("TFB_UNIVERSE_MAINT=off - nothing read or written.")
        return 0
    bad = selftest()
    if bad and cfg["mode"] == "enforce":
        print(f"::warning::self-test failed ({', '.join(bad)}); enforce degraded to observe for this run.")
        cfg["mode"] = "observe"
    key = (os.getenv("EODHD_API_KEY") or os.getenv("EODHD_API_TOKEN") or "").strip()
    if not key:
        print("::error::EODHD_API_KEY missing.")
        return 1
    out = run(open_sheet(), Eodhd(key, os.getenv("EODHD_BASE_URL") or "https://eodhd.com/api"), cfg,
              dt.datetime.now(dt.timezone.utc).replace(tzinfo=None), force_add=args.force_add)
    if args.json_out:
        Path(args.json_out).write_text(json.dumps(out, indent=1, default=str), encoding="utf-8")
    enf = cfg["mode"] == "enforce"
    lines = [f"## Universe maintenance v{SCRIPT_VERSION} ({cfg['mode']})", "",
             f"- rows {out['rows']}, red today {out['red_today']}",
             f"- {'added' if enf else 'would add'}: {', '.join(a['symbol'] for a in out['added']) or 'none'}",
             f"- {'retired' if enf else 'would retire'}: {', '.join(r['symbol'] for r in out['retired']) or 'none'}",
             f"- red {cfg['red_days']}+ trading days but kept: {len(out['kept_red'])}"] + [f"- {n}" for n in out["notes"]]
    print("\n".join(lines))
    summary = os.getenv("GITHUB_STEP_SUMMARY")
    if summary:
        with open(summary, "a", encoding="utf-8") as fh:
            fh.write("\n".join(lines) + "\n")
    return 0


if __name__ == "__main__":
    sys.exit(main())
