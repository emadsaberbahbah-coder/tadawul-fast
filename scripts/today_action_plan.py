#!/usr/bin/env python3
"""
scripts/today_action_plan.py
================================================================================
TFB Today Action Plan - one page that says what to do today, and why
--------------------------------------------------------------------------------
v1.3.0 (2026-11-13)
  1.3.0: THE BROKER'S OWN CASH AND ORDERS, AND A CLEARER PAGE (owner, 13 Nov 11:51: "the action plan should be as per
         the cash available on IBKR ... full review for the action plan formatting").
         BROKER. The page reads the newest read-only IBKR capture (scripts/ibkr_capture.py; the file the scheduled
         tasks write to Drive every hour of the session) when it is at most TFB_TAP_IBKR_MAX_AGE_H hours old (30):
           - cash for the funding check = the capture's cash (the smaller of cash and settled cash), less working BUY
             orders on names that are not today's picks; the workbook's cash ledger is the fallback;
           - each holding's broker stop and the shares it covers come from the capture's working orders, so the page no
             longer says "broker orders not read": a holding with no stop, or one that covers only part of the
             position (DDI, 12 Nov: 120 of 240 shares), gets its own step; a raise the broker already carries is
             not asked for again;
           - a buy whose order is already working at IBKR reads PLACED, with the order's own shares and limit.
         PAGE. A row of five tiles under the title (cash at the broker, today's buys, cash after them, the news gate,
         page alignment); the tab is coloured; section B says where its broker stops come from.
         Nothing here prints a cash amount: the run log names the capture file, its age and two counts.
  1.2.0: ALIGNED WITH THE TOP 10 PAGES, THE NEWS GATE AND THE CASH LEDGER (owner, 13 Nov 11:05: "between top investment
         and V2 top investment and today action misalignment need to be matching and aligned to avoid mistake").
         On 13 Nov three pages gave three answers: this page said buy EFC, INSW and AX; Top_10_Investments_v2 read
         EXECUTABLE with a 29-share ticket on TEN (earnings in 6 days - the plan holds it back); the main page sized
         nothing. Two more facts never reached this page: the News & Macro Gate read NO_NEW_ENTRY (Hormuz), and the
         three buys cost about 21,700 SAR against 17,547 SAR of cash. Now:
           - PAGE ALIGNMENT (new section): every name that is a pick here or a sized ticket on either Top 10 page, with
             what each page says and one reading. This page is the order of record.
           - a ticket on a Top 10 page that is not an OPEN pick here becomes a DO NOT BUY step;
           - buys are funded in rank order from the latest _Cash_Snapshot balance; one that does not fit reads
             NOT FUNDED instead of BUY;
           - a News & Macro Gate verdict other than GO, at most TFB_TAP_NEWS_MAX_H hours old (24), is a READ FIRST step
             ahead of the buys (the gate is observe-only: the plan does not obey it yet).
         The four new reads are optional: a tab that cannot be read is named "not read" and changes no step.
  1.1.0: THE PAGE'S DESIGN (owner, 12 Nov 12:07: "this page need to improve formatting ... to get better design").
         Until now the page was a plain grid: one font, no colour, ten equal columns, text running under its
         neighbours. Now:
           - section A shows each step as a card of two rows: a coloured number with the kind of step under it (SELL,
             TAKE PROFIT, RAISE STOP, CONFIRM, REVIEW, BUY), the instruction in bold across the page, and under it
             the levels and the reason;
           - a title bar, a summary band (red when a source is old or was not read), section bands, table headers,
             right-aligned figures, coloured actions / statuses / P-L, sized columns, wrapped text, row lines,
             no gridlines, the title frozen, and the sheet cut to the page's own size;
           - section E in four small tables; a learning-gate row reads as a label and its figures.
         The values are the same copies as before; only their cells in sections A and E moved.
         ONE WRITE: values, looks, merges and sizes go out as a single Sheets batch, applied whole or not at all.
         If Sheets refuses the styled batch for any reason but its quota, the page is written as the plain grid
         (and the run says so) - a formatting fault never costs the day's instructions. TFB_TAP_STYLE=off writes the
         plain grid on purpose.
         PREVIEW: TFB_TAP_PREVIEW=true writes the tab Today_Action_Plan_Preview and leaves the live page alone; the
         next normal run hides that tab.
         EVIDENCE BOARD AGE: the board is rebuilt at most every 6 hours and not overnight, so the 3-hour limit made
         the live page read "OLD SOURCE: Evidence_Board" at 3.1 hours. It now has its own limit,
         TFB_TAP_STALE_EVIDENCE_H (18).
  1.0.2: WAIT AND TRY AGAIN WHEN GOOGLE SHEETS REPORTS ITS PER-MINUTE QUOTA. On 12 Nov 11:04 the Daily Plan and the
         Exit Plan were started together from the workbook menu; both finished in the same second and the two runs
         of this page that followed stopped with "APIError [429]: Quota exceeded for quota metric 'Read requests'"
         (runs 43179925808 and 43179929297; nothing written, the page stayed as it was). A quota answer is now
         followed by a wait of 20, 40, then 65 seconds and another try (TFB_TAP_RETRIES, default 3, 0 = none);
         every other error stops the run at once, as before.
  1.0.1: ONE LINE WHEN THE BROKER ORDERS WERE NOT READ. The first live run (12 Nov 10:42) gave three steps
         "Check the broker stop on X: none was read for it" - the Exit Plan's broker read was stale ("broker orders
         unknown"), while the stops were in place at the broker. When the Exit_Plan page says its broker orders are
         unknown, the page now gives ONE step ("confirm the stops in IBKR against section B") instead of one per
         holding, and section B says "broker orders not read" instead of "no broker stop read".

WHY (owner, 12 Nov 2026 08:59 Riyadh)
    The Daily Plan (new buys) and the Exit Plan (holdings) are two pages, and
    the context that qualifies them sits on two more (Evidence Board,
    Consistency Check). The owner asked for one sheet, "Today Action Plan",
    with clearer instructions that come from the other pages and stay aligned
    with them, and noticed "duplicate" Daily Plan / Exit Plan tabs - those are
    the two ledgers (_Daily_Plan, _Exit_Plan) the pages are built from.

WHAT
    Today_Action_Plan (one tab, rewritten on every run):
      A. DO TODAY   numbered steps, most urgent first: sells, profit-taking and
                    stop changes on holdings, a broker stop that is missing or
                    below the plan's level, then the new buys that are OPEN.
      B. HOLDINGS   every holding: action, the plan's stop, the broker's stop.
      C. NEW BUYS   every pick of today's plan with its status, levels and the
                    Evidence Board's figures.
      D. PIPELINE   the first names waiting, and what they wait for.
      E. CONTEXT    market state, the learning gate, consistency mismatches,
                    and the age of every source page.
    It COPIES: every level and action on it is read from Exit_Plan, _Daily_Plan,
    Evidence_Board and Consistency_Check, and each row names its source, so the
    page cannot disagree with them - a source older than TFB_TAP_STALE_H hours
    is flagged instead. It computes no level of its own.
    The ledger tabs listed in TFB_TAP_HIDE_TABS are hidden (not deleted: they
    hold the history the pages and the forward record are built from).

SAFETY
    Reads four tabs, writes one. No order is placed, changed or cancelled.
    TFB_TODAY_PLAN = on (default) | off. --dry-run prints and writes nothing.

ENV
    DEFAULT_SPREADSHEET_ID, GOOGLE_SHEETS_CREDENTIALS, TFB_TODAY_PLAN,
    TFB_TAP_STALE_H (3), TFB_TAP_STOP_TOL_PCT (1), TFB_TAP_PIPELINE (5),
    TFB_TAP_HIDE_TABS (_Daily_Plan,_Exit_Plan,_Evidence_Log; "none" = hide none),
    TFB_TAP_RETRIES (3; tries after a Sheets quota answer, 0 = none),
    TFB_TAP_STYLE (on | off = plain grid), TFB_TAP_PREVIEW (false | true = write the preview tab),
    TFB_TAP_STALE_EVIDENCE_H (18), TFB_TAP_ALIGN (on | off), TFB_TAP_NEWS_MAX_H (24),
    TFB_TAP_IBKR_MAX_AGE_H (30), TFB_IBKR_CAPTURE (on | off), TFB_IBKR_CAPTURE_FOLDER_ID.
================================================================================
"""
from __future__ import annotations

import argparse
import datetime as _dt
import math
import os
import re
import sys
import time
from typing import Any, Callable, Dict, List, Optional, Sequence, Tuple

__version__ = "1.3.0"

UTC = _dt.timezone.utc
RIYADH = _dt.timezone(_dt.timedelta(hours=3))
TAB = "Today_Action_Plan"
PREVIEW_TAB = "Today_Action_Plan_Preview"                    # written instead of TAB when TFB_TAP_PREVIEW is true
WIDTH = 10
DEFAULT_HIDE = "_Daily_Plan,_Exit_Plan,_Evidence_Log"
URGENCY = {"SELL": 0, "TAKE PROFIT": 1, "RAISE STOP": 2, "REVIEW": 3, "CHECK": 4}
_TAB_RE = re.compile(r"^[A-Za-z0-9_ ]{1,60}$")
_TS_RE = re.compile(r"(\d{4})-(\d{2})-(\d{2})[ T](\d{2}):(\d{2})")


def _out(msg: str) -> None:
    print("[today_plan %s] %s" % (__version__, msg), flush=True)


def _env(name: str, default: str = "") -> str:
    return (os.environ.get(name) or default).strip()


def _env_num(name: str, default: float, lo: float, hi: float) -> float:
    try:
        v = float(_env(name, str(default)))
    except ValueError:
        return default
    return v if lo <= v <= hi else default


QUOTA_WAITS = (20.0, 40.0, 65.0)                       # seconds; the Sheets quota is counted per minute


def quota_error(e: BaseException) -> bool:
    """True for the Sheets per-minute quota answer (HTTP 429); every other error is not retried."""
    code = getattr(getattr(e, "response", None), "status_code", None)
    s = "%s %s" % (type(e).__name__, e)
    return code == 429 or "[429]" in s or "Quota exceeded" in s or "RESOURCE_EXHAUSTED" in s


def with_retry(fn: Callable[[], Any], retries: int, sleep: Callable[[float], None], what: str) -> Any:
    """Run fn; after a quota answer wait and run it again, at most `retries` more times."""
    n = 0
    while True:
        try:
            return fn()
        except Exception as e:  # noqa: BLE001 - re-raised unless it is the quota answer and tries remain
            if n >= retries or not quota_error(e):
                raise
            wait = QUOTA_WAITS[min(n, len(QUOTA_WAITS) - 1)]
            n += 1
            _out("Sheets quota reached while %s: waiting %d s, then try %d of %d" % (what, wait, n, retries))
            sleep(wait)


def enabled() -> bool:
    return _env("TFB_TODAY_PLAN", "on").lower() not in ("off", "0", "false", "no")


def text(v: Any) -> str:
    return "" if v is None else str(v).strip()


def num(v: Any) -> Optional[float]:
    if v is None or isinstance(v, bool):
        return None
    if isinstance(v, (int, float)):
        return float(v) if math.isfinite(float(v)) else None
    try:
        x = float(text(v).replace(",", "").replace("%", "").split(" ")[0])
    except (ValueError, IndexError):
        return None
    return x if math.isfinite(x) else None


def fmt(v: Any, nd: int = 2) -> str:
    x = num(v)
    return "" if x is None else ("{:,.%df}" % nd).format(x)


def cell(row: Sequence[Any], i: Optional[int]) -> Any:
    return row[i] if i is not None and 0 <= i < len(row) else ""


def find_table(values: Sequence[Sequence[Any]], need: Sequence[str], start: int = 0) -> Tuple[Optional[int], Dict[str, int]]:
    """(header row index, {header: column}) of the first row at or after `start` that carries every `need`."""
    rows = list(values)
    for i in range(start, len(rows)):
        cells = [text(c) for c in rows[i]]
        if all(n in cells for n in need):
            return i, {name: cells.index(name) for name in cells if name}
    return None, {}


def page_time(values: Sequence[Sequence[Any]], now: _dt.datetime, rows: int = 3) -> Optional[_dt.datetime]:
    """The latest 'YYYY-MM-DD HH:MM' in the top rows of a page that is not in the future, read as Riyadh time (a page
    header carries its cut time and its last-checked time; the later one is the page's age)."""
    best: Optional[_dt.datetime] = None
    for r in list(values)[:rows]:
        for c in r:
            for m in _TS_RE.finditer(text(c)):
                try:
                    t = _dt.datetime(*[int(x) for x in m.groups()], tzinfo=RIYADH)
                except ValueError:
                    continue
                if t <= now + _dt.timedelta(minutes=5) and (best is None or t > best):
                    best = t
    return best


# ----------------------------------------------------------------------------- #
# Readers (pure)
# ----------------------------------------------------------------------------- #
def read_holdings(values: Sequence[Sequence[Any]]) -> List[Dict[str, Any]]:
    """The HOLDINGS table of the Exit_Plan page."""
    h, col = find_table(values, ("Symbol", "Qty", "Stop", "Action", "Why"))
    out: List[Dict[str, Any]] = []
    if h is None:
        return out
    for r in list(values)[h + 1:]:
        sym = text(cell(r, col.get("Symbol"))).upper()
        if not sym or " " in sym:
            break
        out.append({"symbol": sym, "name": text(cell(r, col.get("Name"))), "qty": num(cell(r, col.get("Qty"))),
                    "entry": num(cell(r, col.get("Entry"))),
                    "last": num(cell(r, col.get("Live"))) or num(cell(r, col.get("Last close"))),
                    "pl_pct": text(cell(r, col.get("P/L %"))), "stop": num(cell(r, col.get("Stop"))),
                    "was": num(cell(r, col.get("Was"))), "target": num(cell(r, col.get("Target"))),
                    "action": text(cell(r, col.get("Action"))).upper(), "why": text(cell(r, col.get("Why"))),
                    "broker_stop": num(cell(r, col.get("Broker stop")))})
    return out


def broker_unknown(values: Sequence[Sequence[Any]]) -> str:
    """The Exit_Plan page's own words when it could not read the broker orders ('broker orders unknown (...)'), else ''."""
    for r in list(values)[:6]:
        for c in r:
            m = re.search(r"broker orders unknown(?: \([^)]*\))?", text(c))
            if m:
                return m.group(0)
    return ""


def read_plan(values: Sequence[Sequence[Any]], today: _dt.date) -> Dict[str, Any]:
    """Today's rows of the Daily Plan ledger: {"date", "cut_at", "quality", "picks", "pipeline", "is_today"}."""
    rows = [list(r) for r in values if r]
    out: Dict[str, Any] = {"date": "", "cut_at": "", "quality": "", "picks": [], "pipeline": [], "is_today": False}
    if not rows:
        return out
    hdr = [text(c) for c in rows[0]]
    if not {"Plan Date", "Kind", "Symbol", "Status"} <= set(hdr):
        return out
    col = {n: hdr.index(n) for n in hdr if n}
    days = sorted({text(cell(r, col["Plan Date"])) for r in rows[1:] if text(cell(r, col["Plan Date"]))})
    if not days:
        return out
    last = days[-1]
    out["date"], out["is_today"] = last, last[:10] == today.isoformat()
    for r in rows[1:]:
        if text(cell(r, col["Plan Date"])) != last:
            continue
        d = {"symbol": text(cell(r, col["Symbol"])).upper(), "name": text(cell(r, col.get("Name"))),
             "rank": num(cell(r, col.get("Rank"))), "ccy": text(cell(r, col.get("Ccy"))),
             "entry": num(cell(r, col.get("Entry"))), "zone_low": num(cell(r, col.get("Zone Low"))),
             "zone_high": num(cell(r, col.get("Zone High"))), "qty": num(cell(r, col.get("Qty"))),
             "stop": num(cell(r, col.get("Stop"))), "tp1": num(cell(r, col.get("TP1"))),
             "score": num(cell(r, col.get("Score"))), "labels": text(cell(r, col.get("Labels"))),
             "waiting": text(cell(r, col.get("Waiting For"))), "status": text(cell(r, col.get("Status"))).upper(),
             "reason": text(cell(r, col.get("Status Reason"))), "price": num(cell(r, col.get("Last Price"))),
             "alloc": num(cell(r, col.get("Alloc SAR")))}
        out["cut_at"] = out["cut_at"] or text(cell(r, col.get("Cut At (Riyadh)")))
        out["quality"] = out["quality"] or text(cell(r, col.get("Cut Quality")))
        kind = text(cell(r, col["Kind"])).upper()
        if d["symbol"] and kind in ("PICK", "PIPELINE"):
            out["picks" if kind == "PICK" else "pipeline"].append(d)
    out["pipeline"].sort(key=lambda d: d["rank"] if d["rank"] is not None else 1e9)
    return out


def read_evidence(values: Sequence[Sequence[Any]]) -> Dict[str, Any]:
    """{"states": [...], "names": {SYMBOL: {...}}, "gate": [[...]]} from the Evidence_Board page."""
    rows = [list(r) for r in values]
    out: Dict[str, Any] = {"states": [], "names": {}, "gate": []}
    h, col = find_table(rows, ("Market", "Above 50-day avg", "State"))
    if h is not None:
        for r in rows[h + 1:]:
            if not text(cell(r, col["Market"])):
                break
            out["states"].append({"market": text(cell(r, col["Market"])), "bars_to": text(cell(r, col.get("Bars to"))),
                                  "above50": text(cell(r, col["Above 50-day avg"])), "state": text(cell(r, col["State"])),
                                  "all_up": text(cell(r, col.get("After this state: all names up"))),
                                  "weeks": text(cell(r, col.get("Weeks seen")))})
    h, col = find_table(rows, ("Symbol", "Shadow score", "Chance up, 21 bars"))
    if h is not None:
        for r in rows[h + 1:]:
            raw = text(cell(r, col["Symbol"]))
            if not raw:
                break
            out["names"][raw.split(" ")[0].upper()] = {"shadow": text(cell(r, col["Shadow score"])),
                                                       "chance": text(cell(r, col["Chance up, 21 bars"])),
                                                       "basis": text(cell(r, col.get("Based on")))}
    for i, r in enumerate(rows):
        if text(cell(r, 0)).startswith("6. LEARNING GATE"):
            for g in rows[i + 1:i + 9]:
                cells = [text(c) for c in g[:6]]
                if not any(cells):
                    break
                out["gate"].append(cells)
            break
    return out


def read_consistency(values: Sequence[Sequence[Any]]) -> List[Dict[str, str]]:
    """Rows of the Consistency_Check page whose status is MISMATCH or STALE."""
    h, col = find_table(values, ("Check", "Status", "Detail"))
    out: List[Dict[str, str]] = []
    if h is None:
        return out
    for r in list(values)[h + 1:]:
        st = text(cell(r, col["Status"])).upper()
        if st in ("MISMATCH", "STALE"):
            out.append({"check": text(cell(r, col["Check"])), "status": st, "detail": text(cell(r, col["Detail"]))})
    return out


# ----------------------------------------------------------------------------- #
# 1.2.0 [ALIGN]: the Top 10 pages, the news gate, the cash ledger (pure)
# ----------------------------------------------------------------------------- #
PEG = 3.75
NEWS_TAB, CASH_TAB = "_News_Gate", "_Cash_Snapshot"
T10_TAB, V2_TAB = "Top_10_Investments", "Top_10_Investments_v2"


def _stamp(s: str, after: str = "") -> str:
    """The first 'YYYY-MM-DD HH:MM' in s (after the word `after` when given), or ''."""
    if after:
        if after not in s:
            return ""
        s = s[s.index(after):]
    m = _TS_RE.search(s)
    return "%s-%s-%s %s:%s" % m.groups() if m else ""


def _squash(v: Any) -> str:
    """Cell text with every run of white space (a line break inside a header cell, a no-break space) as one space."""
    return " ".join(text(v).replace("\u00a0", " ").split())


def _header(rows: Sequence[Sequence[Any]], need: Sequence[str]) -> Tuple[Optional[int], Dict[str, int]]:
    """find_table on squashed header text: the live pages break header cells over two lines."""
    for i, r in enumerate(rows):
        cells = [_squash(c) for c in r]
        if all(n in cells for n in need):
            col: Dict[str, int] = {}
            for j, name in enumerate(cells):
                if name and name not in col:
                    col[name] = j
            return i, col
    return None, {}


def read_top10(values: Sequence[Sequence[Any]]) -> Dict[str, Any]:
    """The main Top 10 page: {"read", "banner", "built", "seats": {SYMBOL: {"ticket", "shares", "seat"}}}.
    A seat is a ticket when both its Ticket SAR and its Shares are above zero."""
    rows = [list(r) for r in values]
    out: Dict[str, Any] = {"read": False, "banner": "", "built": "", "seats": {}}
    h, col = _header(rows, ("Rank", "Symbol", "Ticket SAR", "Shares"))
    if h is None:
        return out
    out["read"] = True
    out["banner"] = " ".join(_squash(c) for c in rows[h - 1] if text(c)) if h else ""
    for r in rows[:4]:
        for c in r:
            out["built"] = out["built"] or _stamp(text(c), "Last run")
    for r in rows[h + 1:]:
        sym = text(cell(r, col["Symbol"])).upper()
        if not sym:
            break
        out["seats"][sym] = {"ticket": num(cell(r, col["Ticket SAR"])) or 0.0, "shares": num(cell(r, col["Shares"])) or 0.0,
                             "seat": text(cell(r, col.get("Funds From"))) or text(cell(r, col.get("Stability")))}
    return out


def read_top10_v2(values: Sequence[Sequence[Any]]) -> Dict[str, Any]:
    """The v2 Top 10 page, in the same shape as read_top10 (its headline is the banner). The page has two forms:
    with the engine's tickets its columns read "Alloc SAR" / "Buy qty"; without one they read "Preview SAR" /
    "Preview qty" - a research sizing, kept as seat["preview"] and never counted as a ticket."""
    rows = [list(r) for r in values]
    out: Dict[str, Any] = {"read": False, "banner": "", "built": "", "seats": {}}
    sar, qty = "Alloc SAR", "Buy qty"
    h, col = _header(rows, ("Symbol · name", "Seat status", sar, qty))
    if h is None:
        sar, qty = "Preview SAR", "Preview qty"
        h, col = _header(rows, ("Symbol · name", "Seat status", sar, qty))
    if h is None:
        return out
    preview = sar.startswith("Preview")
    out["read"] = True
    top = [text(c) for c in (rows[0] if rows else []) if text(c)]
    out["banner"] = top[1] if len(top) > 1 else ""
    out["built"] = _stamp(" ".join(top), "page built")
    for r in rows[h + 1:]:
        name = text(cell(r, col["Symbol · name"]))
        if not name:
            break
        a, q = num(cell(r, col[sar])) or 0.0, num(cell(r, col[qty])) or 0.0
        seat = {"ticket": 0.0 if preview else a, "shares": 0.0 if preview else q, "seat": text(cell(r, col["Seat status"]))}
        if preview and a > 0 and q > 0:
            seat["preview"] = (a, q)
        out["seats"][name.split("·")[0].strip().upper()] = seat
    return out


def read_news_gate(values: Sequence[Sequence[Any]], now: _dt.datetime) -> Dict[str, Any]:
    """The latest row of the _News_Gate tab: {"read", "verdict", "drivers", "at", "age_h"}."""
    rows = [list(r) for r in values if r]
    out: Dict[str, Any] = {"read": False, "verdict": "", "drivers": "", "at": "", "age_h": None}
    h, col = find_table(rows, ("Run At (Riyadh)", "Verdict"))
    if h is None:
        return out
    for r in rows[h + 1:]:
        v = text(cell(r, col["Verdict"])).upper()
        if v:
            out.update({"read": True, "verdict": v, "drivers": text(cell(r, col.get("Drivers"))),
                        "at": _stamp(text(cell(r, col["Run At (Riyadh)"])))})
    m = _TS_RE.search(out["at"])
    if m:
        t = _dt.datetime(*[int(x) for x in m.groups()], tzinfo=RIYADH)
        out["age_h"] = max(0.0, (now - t).total_seconds() / 3600.0)
    return out


def pick_cost(p: Dict[str, Any]) -> Optional[float]:
    """What a pick can cost in SAR: its shares at the highest price the step allows (the top of the entry zone, or
    the last price when that is higher), at the peg (USD) or at par (SAR) - and never less than the plan's own
    allocation, which was computed at the entry price (Codex P2, #971). Another currency: the allocation alone."""
    alloc = float(p["alloc"]) if p.get("alloc") else None
    fx = {"USD": PEG, "SAR": 1.0}.get(text(p.get("ccy")).upper())
    top = max(float(p.get("zone_high") or 0.0), float(p.get("price") or 0.0))
    if not (fx and p.get("qty") and top > 0):
        return alloc
    return max(alloc or 0.0, float(p["qty"]) * top * fx)


def fund(picks: Sequence[Dict[str, Any]], cash: Optional[float]) -> Optional[float]:
    """Fund the OPEN picks in rank order from `cash`; a pick that does not fit gets p["unfunded"] = (cost, left).
    Returns the cash left (None when the cash is not known - then nothing is marked)."""
    if cash is None:
        return None
    left = float(cash)
    for p in sorted([p for p in picks if p["status"] == "OPEN" and p.get("qty")], key=lambda p: p.get("rank") or 1e9):
        cost = pick_cost(p)
        if cost is None:
            continue
        if cost <= left + 0.5:
            left -= cost
        else:
            p["unfunded"] = (cost, left)
    return left


def _seat_text(page: Dict[str, Any], sym: str) -> Tuple[str, bool]:
    """(what the page says about sym, whether it sizes a ticket)."""
    if not page.get("read"):
        return "page not read", False
    s = page["seats"].get(sym)
    if s is None:
        return "not seated", False
    if s["ticket"] > 0 and s["shares"] > 0:
        return "ticket: %s shares, %s SAR" % (fmt(s["shares"], 0), fmt(s["ticket"], 0)), True
    if s.get("preview"):
        return "%s · preview only (%s shares), not a ticket" % (s["seat"] or "seated", fmt(s["preview"][1], 0)), False
    return s["seat"] or "seated, not sized", False


def align(plan: Dict[str, Any], t10: Dict[str, Any], v2: Dict[str, Any]) -> List[Dict[str, Any]]:
    """One row for every name that is a pick of today's plan or a sized ticket on either Top 10 page:
    {symbol, plan, main, v2, reading, level} with level ok | soft (this page buys, a Top 10 page does not size) |
    hard (a Top 10 page sizes a ticket this page does not hold as an OPEN buy)."""
    picks = {p["symbol"]: p for p in (plan["picks"] if plan.get("is_today") else [])}
    pipe = {p["symbol"]: p for p in (plan["pipeline"] if plan.get("is_today") else [])}
    names = list(picks)
    for page in (t10, v2):
        names += sorted(s for s, y in (page.get("seats") or {}).items() if y["ticket"] > 0 and y["shares"] > 0 and s not in names)
    out: List[Dict[str, Any]] = []
    for sym in names:
        p = picks.get(sym)
        buys = bool(p and p["status"] == "OPEN" and p.get("qty") and not p.get("unfunded"))
        if p and p.get("unfunded"):
            mine, held = "OPEN, not funded today", "it is not funded today"
        elif buys:
            mine, held = "OPEN - buy %s" % fmt(p["qty"], 0), ""
        elif p:
            mine = "%s%s" % (p["status"] or "PICK", " - " + p["reason"] if p.get("reason") else "")
            held = "it reads %s here" % (p["status"] or "not open")
        elif sym in pipe:
            mine = "pipeline: %s" % (pipe[sym].get("waiting") or "waiting")
            held = "the plan holds it back (%s)" % (pipe[sym].get("waiting") or "pipeline")
        elif plan.get("is_today"):
            mine, held = "not in today's plan", "it is not in today's plan"
        else:
            mine, held = "no plan cut today", "no plan has been cut today"
        (a, a_buy), (b, b_buy) = _seat_text(t10, sym), _seat_text(v2, sym)
        sized = [n for n, y in ((T10_TAB, a_buy), (V2_TAB, b_buy)) if y]
        if sized and not buys:
            level, reading = "hard", "DO NOT BUY from %s: %s." % (" or ".join(sized), held)
        elif buys and len(sized) == 2:
            level, reading = "ok", "ALIGNED: all three pages size it."
        elif buys:
            unread = [n for n, pg in ((T10_TAB, t10), (V2_TAB, v2)) if not pg.get("read")]
            level = "ok" if len(unread) == 2 else "soft"
            reading = ("Top 10 pages not read." if len(unread) == 2 else
                       "FOLLOW THIS PAGE: the plan is the order of record; %s no ticket." % (
                           "the Top 10 pages show" if not sized else "one Top 10 page shows"))
        else:
            level, reading = "ok", "ALIGNED: no page sizes it."
        out.append({"symbol": sym, "plan": mine, "main": a, "v2": b, "reading": reading, "level": level,
                    "sized": sized, "held": held})
    return out


# ----------------------------------------------------------------------------- #
# 1.3.0 [IBKR]: the broker's own cash and working orders, from the read-only capture (pure)
# ----------------------------------------------------------------------------- #
def apply_capture(holdings: List[Dict[str, Any]], plan: Dict[str, Any], cap: Optional[Dict[str, Any]],
                  max_age_h: float) -> Dict[str, Any]:
    """Put the capture's working stops on the holdings and return the broker view:
    {"used", "why", "at", "cash", "reserved", "open_orders": {SYM: [(limit, qty)]}}. Not used (holdings untouched)
    when there is no capture, it could not be read, or it is older than max_age_h."""
    out: Dict[str, Any] = {"used": False, "why": "", "at": "", "cash": None, "reserved": 0.0, "open_orders": {}}
    if not cap:
        out["why"] = "no capture given"
        return out
    if not cap.get("ok"):
        out["why"] = cap.get("reason") or "capture not read"
        return out
    if cap["age_h"] > max_age_h:
        out["why"] = "the newest capture is %.0f h old (limit %.0f h)" % (cap["age_h"], max_age_h)
        return out
    from scripts.ibkr_capture import reserved_usd, stop_cover  # noqa: PLC0415
    picks = set(p["symbol"] for p in (plan["picks"] if plan.get("is_today") else []) if p["status"] == "OPEN")
    usd = min(x for x in (cap.get("cash_usd"), cap.get("settled_usd")) if x is not None)
    held_back, complete = reserved_usd(cap, picks)
    out.update({"used": True, "at": cap["as_of"].astimezone(RIYADH).strftime("%Y-%m-%d %H:%M"),
                "cash": max(0.0, usd - held_back) * PEG, "reserved": held_back * PEG, "complete": complete,
                "open_orders": dict((sym, rows) for sym, rows in (cap.get("buys") or {}).items() if sym in picks)})
    for h in holdings:
        h["at_broker"] = h["symbol"] in cap["positions"]
        if h["at_broker"]:
            h["broker_stop"], h["broker_cover"] = stop_cover(cap, h["symbol"])
            h["broker_qty"] = cap["positions"][h["symbol"]]
    return out


def stop_state(h: Dict[str, Any], tol_pct: float) -> str:
    """How the broker's stop orders stand against the plan's stop, when the capture gave them:
    "" (not known) | none | partial | unpriced | low | ok."""
    cover, qty, stop, bs = h.get("broker_cover"), h.get("qty") or 0, h.get("stop"), h.get("broker_stop")
    if cover is None or not stop:
        return ""
    if cover <= 0:
        return "none"
    if qty and cover < qty - 1e-6:
        return "partial"
    if bs is None:
        return "unpriced"
    return "low" if bs < stop * (1 - tol_pct / 100.0) else "ok"


# ----------------------------------------------------------------------------- #
# The plan (pure)
# ----------------------------------------------------------------------------- #
def holding_steps(h: Dict[str, Any], tol_pct: float, broker_known: bool = True) -> List[Dict[str, Any]]:
    """The steps one holding asks for today, each {urgency, symbol, do, levels, why, source}."""
    sym, qty, stop, bs, act = h["symbol"], h.get("qty") or 0, h.get("stop"), h.get("broker_stop"), h["action"]
    steps: List[Dict[str, Any]] = []
    shares = "%s shares" % fmt(qty, 0) if qty else "the position"

    def step(urg: float, do: str, levels: str) -> None:
        steps.append({"urgency": urg, "symbol": sym, "do": do, "levels": levels, "why": h.get("why", ""),
                      "source": "Exit_Plan"})
    if act == "SELL":
        step(0, "Sell all %s of %s." % (shares, sym), "last %s · stop %s" % (fmt(h.get("last")), fmt(stop)))
    elif act == "TAKE PROFIT":
        half = int(qty // 2) if qty else 0
        rest = "the %d shares that remain" % (qty - half) if half else "the shares that remain"
        do = "Sell half of %s (%s) and keep a stop at %s on the rest." % (
            sym, "%d shares" % half if half else "half the position", fmt(stop))
        if stop and (bs is None or bs < stop * (1 - tol_pct / 100.0)):
            # one step, sized for what is left after the sale (Codex P1, #966): never "raise it for all N shares"
            do = "Sell half of %s (%s); then set the stop at %s for %s (broker stop now: %s)." % (
                sym, "%d shares" % half if half else "half the position", fmt(stop), rest, fmt(bs) or "none read")
        step(1, do, "last %s · next target %s" % (fmt(h.get("last")), fmt(h.get("target"))))
    elif act == "RAISE STOP":
        cover = h.get("broker_cover")
        if cover is not None and qty and cover >= qty - 1e-6 and bs is not None and stop and bs >= stop - 0.005:
            pass                                    # 1.3.0: the broker already carries this stop on every share
        else:
            at_ib = "" if cover is None else " on %s of %s shares" % (fmt(cover, 0), fmt(qty, 0))
            step(2, "Raise the stop on %s to %s for all %s." % (sym, fmt(stop), shares),
                 "was %s · broker stop %s%s" % (fmt(h.get("was")) or "-", fmt(bs) or (
                     ("none" if cover is not None else "not read") if broker_known else "orders not read"), at_ib))
    elif act in ("REVIEW", "CHECK"):
        step(3, "Review %s today: the exit rule could not decide on its own." % sym,
             "last %s · stop %s" % (fmt(h.get("last")), fmt(stop)))
    state = stop_state(h, tol_pct)                  # 1.3.0: "" unless the IBKR capture gave the orders
    if act in ("HOLD", "REVIEW", "CHECK") and stop and state:
        if state == "none":
            step(2.4, "No stop order is working at IBKR for %s: place one at %s for all %s." % (sym, fmt(stop), shares),
                 "plan stop %s · no stop at the broker" % fmt(stop))
        elif state == "partial":
            step(2.4, "The stop on %s covers %s of %s shares: add a stop at %s for the other %s." % (
                sym, fmt(h["broker_cover"], 0), fmt(qty, 0), fmt(stop), fmt(qty - h["broker_cover"], 0)),
                "plan stop %s · broker stop %s" % (fmt(stop), fmt(bs) or "price not read"))
        elif state == "unpriced":
            step(2.5, "Check the stop on %s at IBKR: its price could not be read. The plan's stop is %s." % (sym, fmt(stop)),
                 "plan stop %s" % fmt(stop))
        elif state == "low":
            step(2.5, "The broker stop on %s (%s) is below the plan's stop (%s): raise it for all %s." % (
                sym, fmt(bs), fmt(stop), shares), "plan stop %s · broker stop %s" % (fmt(stop), fmt(bs)))
    elif act in ("HOLD", "REVIEW", "CHECK") and stop and h.get("at_broker") is False:
        step(2.5, "%s is not in the IBKR account: check its stop (%s) where it is held." % (sym, fmt(stop)),
             "plan stop %s · not at IBKR" % fmt(stop))
    elif act in ("HOLD", "REVIEW", "CHECK") and stop:
        if bs is None and not broker_known:
            pass                                    # 1.0.1: one step for all holdings, added in build()
        elif bs is None:
            step(2.5, "Check the broker stop on %s: none was read for it. The plan's stop is %s." % (sym, fmt(stop)),
                 "plan stop %s · broker stop not read" % fmt(stop))
        elif bs < stop * (1 - tol_pct / 100.0):
            step(2.5, "The broker stop on %s (%s) is below the plan's stop (%s): raise it for all %s." % (
                sym, fmt(bs), fmt(stop), shares), "plan stop %s · broker stop %s" % (fmt(stop), fmt(bs)))
    return steps


def pick_step(p: Dict[str, Any], ev: Dict[str, Any], verified: bool) -> Optional[Dict[str, Any]]:
    """The buy step of an OPEN pick; None for every other status."""
    if p["status"] != "OPEN" or not p.get("qty"):
        return None
    if p.get("unfunded") and not p.get("placed"):                   # 1.2.0: the cash does not cover it
        cost, left = p["unfunded"]
        return {"urgency": 6 + (p.get("rank") or 0) / 1000.0, "symbol": p["symbol"], "kind": "nofund", "source": "Daily_Plan",
                "do": "%s is not funded today: its %s shares need %s SAR and %s SAR of cash is left after the buys above." % (
                    p["symbol"], fmt(p["qty"], 0), fmt(cost, 0), fmt(left, 0)),
                "levels": "plan: limit %s - %s · stop %s" % (fmt(p.get("zone_low")), fmt(p.get("zone_high")), fmt(p.get("stop"))),
                "why": "The plan was sized before this cash figure was read; the cash now available is less. "
                       "Buying it would use margin."}
    e = ev.get(p["symbol"]) or {}
    why = p.get("reason", "")
    if e.get("chance"):
        why += "; names like it finished up %s of the time over 21 bars (%s)" % (e["chance"], e.get("basis", ""))
    if p.get("labels"):
        why += "; " + p["labels"]
    do = "Buy %s %s with a limit between %s and %s; place the stop at %s." % (
        fmt(p["qty"], 0), p["symbol"], fmt(p.get("zone_low")), fmt(p.get("zone_high")), fmt(p.get("stop")))
    if p.get("placed"):                                             # 1.3.0: the order is already working at IBKR
        o_qty = sum(q for _, q in p["placed"])
        o_px = ", ".join(sorted(set(fmt(px) if px is not None else "no limit" for px, _ in p["placed"])))
        return {"urgency": 5 + (p.get("rank") or 0) / 1000.0, "symbol": p["symbol"], "kind": "placed", "source": "Daily_Plan",
                "do": "The buy order for %s is working at IBKR: %s shares at %s. After it fills, place the stop at %s.%s" % (
                    p["symbol"], fmt(o_qty, 0), o_px, fmt(p.get("stop")),
                    " Cash does not cover it: it needs %s SAR and %s SAR is left." % (
                        fmt(p["unfunded"][0], 0), fmt(p["unfunded"][1], 0)) if p.get("unfunded") else ""),
                "levels": "plan: %s shares · limit %s - %s · first target %s" % (
                    fmt(p["qty"], 0), fmt(p.get("zone_low")), fmt(p.get("zone_high")), fmt(p.get("tp1")) or "-"),
                "why": why.strip("; ")}
    if not verified:
        do = "Before buying, check the main page: its source pages were not verified at the cut. Then: " + do
    return {"urgency": 5 + (p.get("rank") or 0) / 1000.0, "symbol": p["symbol"], "do": do,
            "levels": "price %s · first target %s" % (fmt(p.get("price")), fmt(p.get("tp1")) or "-"), "why": why.strip("; "),
            "source": "Daily_Plan"}


def stale_limit(name: str, k: Dict[str, float]) -> float:
    """Hours after which a source page reads OLD. The Evidence Board is rebuilt at most every 6 hours, from daily bars,
    and not overnight - so it has its own, longer limit (1.1.0: it read OLD on the live page at 3.1 hours)."""
    return max(k["stale_h"], k.get("stale_ev", 0.0)) if name == "Evidence_Board" else k["stale_h"]


def build(holdings: List[Dict[str, Any]], plan: Dict[str, Any], evidence: Dict[str, Any], issues: List[Dict[str, str]],
          ages: Dict[str, Optional[float]], k: Dict[str, float], missing: Sequence[str] = (),
          broker_note: str = "", extra: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """`extra` (1.2.0): {"t10", "v2", "news", "cash", "cash_at"} - the Top 10 pages, the news gate and the cash ledger."""
    x = extra or {}
    steps: List[Dict[str, Any]] = []
    for h in holdings:
        steps += holding_steps(h, k["stop_tol"], not broker_note)
    # a SELL row is leaving in full: no stop to confirm on it (Codex P2, #967)
    unread = [h["symbol"] for h in holdings
              if h.get("stop") and h.get("broker_stop") is None and h["action"] not in ("NO RULE", "SELL")]
    if broker_note and unread:
        steps.append({"urgency": 2.6, "symbol": "ALL", "source": "Exit_Plan",
                      "do": "Confirm in IBKR that each holding's stop matches the plan's stop in section B (%s)."
                            % ", ".join(unread),
                      "levels": "broker orders were not read into the workbook",
                      "why": "The Exit Plan reports: %s. A stop may be in place at the broker; the workbook could not "
                             "compare it." % broker_note})
    picks = plan["picks"] if plan.get("is_today") else []
    broker = x.get("broker") or {}
    for p in picks:
        if p["status"] == "OPEN" and (broker.get("open_orders") or {}).get(p["symbol"]):
            p["placed"] = broker["open_orders"][p["symbol"]]
    cash_left = fund(picks, x.get("cash"))
    for p in picks:
        s = pick_step(p, evidence.get("names") or {}, plan.get("quality", "").upper() == "VERIFIED")
        if s:
            steps.append(s)
    rows = align(plan, x.get("t10") or {}, x.get("v2") or {}) if x else []
    for a in rows:
        if a["level"] == "hard":
            src = x["t10"] if T10_TAB in a["sized"] else x["v2"]
            seat = src["seats"][a["symbol"]]
            steps.append({"urgency": 4.6, "symbol": a["symbol"], "kind": "skip", "source": " / ".join(a["sized"]),
                          "do": "Do not buy %s from %s: %s." % (a["symbol"], " or ".join(a["sized"]), a["held"]),
                          "levels": "shown there: %s shares, %s SAR" % (fmt(seat["shares"], 0), fmt(seat["ticket"], 0)),
                          "why": "Orders come from this page only. The Top 10 pages size from their own rules and "
                                 "times; the Daily Plan is the one cut for the day."})
    news = x.get("news") or {}
    news_live = bool(news.get("read") and news.get("verdict") not in ("", "GO") and news.get("age_h") is not None
                     and news["age_h"] <= k.get("news_max_h", 24.0))
    if news_live and any(step_kind(s) in ("buy", "placed") for s in steps):
        steps.append({"urgency": 4.9, "symbol": "NEWS", "kind": "news", "source": NEWS_TAB,
                      "do": "Read before buying: the News & Macro Gate reads %s (%s Riyadh)." % (
                          news["verdict"].replace("_", " "), news["at"]),
                      "levels": "observe-only gate: the plan does not obey it yet",
                      "why": news.get("drivers") or "no driver recorded"})
    steps.sort(key=lambda s: (s["urgency"], s["symbol"]))
    stale = sorted(n for n, a in ages.items() if a is not None and a > stale_limit(n, k))
    return {"steps": steps, "holdings": holdings, "picks": picks, "pipeline": (plan["pipeline"] if plan.get("is_today") else []),
            "plan": plan, "evidence": evidence, "issues": issues, "ages": ages, "stale": stale,
            "missing": sorted(missing), "broker_note": broker_note, "align": rows, "news": news,
            "news_live": news_live, "cash": x.get("cash"), "cash_at": x.get("cash_at", ""), "cash_left": cash_left,
            "t10": x.get("t10") or {}, "v2": x.get("v2") or {}, "aligned": bool(x), "broker": broker,
            "cash_src": x.get("cash_src", "")}


# ----------------------------------------------------------------------------- #
# The page: values, styles, merges and row heights (pure)
# ----------------------------------------------------------------------------- #
COL_PX = (95, 160, 80, 130, 95, 115, 95, 125, 100, 290)        # A..J; sections B and C set the grid
PLAIN_COL_PX = 130                                             # every column, when the page is written plain
INK, MUTE, LINE, HAIR = "1F2933", "6B7785", "C5CDD8", "E8ECF1"
_GOOD, _WARN, _BAD, _INFO = ("E6F4EA", "2E7D32"), ("FFF3E0", "B45309"), ("FDECEA", "B3261E"), ("F1F3F4", "4A5568")
STYLES: Dict[str, Dict[str, Any]] = {
    "title": {"bg": "1B2A41", "fg": "FFFFFF", "bold": True, "size": 16},
    "sub": {"bg": "F7F9FC", "fg": MUTE, "italic": True, "size": 9},
    "summary": {"bg": "E8F0FE", "fg": "1B2A41", "bold": True, "size": 11},
    "alert": {"bg": _BAD[0], "fg": _BAD[1], "bold": True, "size": 11},
    "band_a": {"bg": "0F5C63", "fg": "FFFFFF", "bold": True, "size": 12},
    "band": {"bg": "3E4C59", "fg": "FFFFFF", "bold": True, "size": 11},
    "subhead": {"bg": "F7F9FC", "fg": "1B2A41", "bold": True, "top": True},
    "head": {"bg": "EEF1F5", "fg": "4A5568", "bold": True, "size": 9, "bottom": True},
    "head_num": {"bg": "EEF1F5", "fg": "4A5568", "bold": True, "size": 9, "bottom": True, "align": "RIGHT"},
    "card_sym": {"fg": INK, "bold": True, "size": 11, "top": True},
    "card_do": {"fg": INK, "bold": True, "size": 11, "top": True},
    "card_src": {"fg": "8A94A3", "italic": True, "size": 9, "valign": "TOP"},
    "card_note": {"fg": "4A5568", "valign": "TOP"},
    "msg": {"fg": "4A5568", "italic": True},
    "msg_alert": {"bg": _BAD[0], "fg": _BAD[1], "bold": True},
    "cell": {"fg": INK, "bottom": "thin"},
    "cell_b": {"fg": INK, "bold": True, "bottom": "thin"},
    "num": {"fg": INK, "align": "RIGHT", "bottom": "thin"},
    "pos": {"fg": _GOOD[1], "align": "RIGHT", "bottom": "thin"},
    "neg": {"fg": "C62828", "align": "RIGHT", "bottom": "thin"},
    "act_sell": {"bg": _BAD[0], "fg": "C62828", "bold": True, "align": "CENTER", "bottom": "thin"},
    "act_profit": {"bg": _GOOD[0], "fg": _GOOD[1], "bold": True, "align": "CENTER", "bottom": "thin"},
    "act_stop": {"bg": _WARN[0], "fg": _WARN[1], "bold": True, "align": "CENTER", "bottom": "thin"},
    "act_review": {"bg": "F3E8FD", "fg": "6A1B9A", "bold": True, "align": "CENTER", "bottom": "thin"},
    "act_hold": {"fg": "4A5568", "align": "CENTER", "bottom": "thin"},
    "act_none": {"fg": "9AA5B1", "align": "CENTER", "bottom": "thin"},
    "st_good": {"bg": _GOOD[0], "fg": _GOOD[1], "bold": True, "size": 9, "align": "CENTER", "bottom": "thin"},
    "st_warn": {"bg": _WARN[0], "fg": _WARN[1], "bold": True, "size": 9, "align": "CENTER", "bottom": "thin"},
    "st_bad": {"bg": _BAD[0], "fg": _BAD[1], "bold": True, "size": 9, "align": "CENTER", "bottom": "thin"},
    "st_info": {"bg": _INFO[0], "fg": _INFO[1], "size": 9, "align": "CENTER", "bottom": "thin"},
    "ok": {"fg": _GOOD[1], "bottom": "thin"},
    "warn": {"fg": _WARN[1], "bold": True, "bottom": "thin"},
    "bad": {"fg": _BAD[1], "bold": True, "bottom": "thin"},
    "foot": {"fg": MUTE, "italic": True, "size": 9, "valign": "TOP"},
    "tile_label": {"bg": "F7F9FC", "fg": MUTE, "bold": True, "size": 8, "align": "CENTER", "valign": "BOTTOM"},
    "tile_plain": {"bg": "F7F9FC", "fg": "1B2A41", "bold": True, "size": 14, "align": "CENTER"},
    "tile_good": {"bg": "F7F9FC", "fg": _GOOD[1], "bold": True, "size": 14, "align": "CENTER"},
    "tile_warn": {"bg": "F7F9FC", "fg": _WARN[1], "bold": True, "size": 14, "align": "CENTER"},
    "tile_bad": {"bg": "F7F9FC", "fg": _BAD[1], "bold": True, "size": 14, "align": "CENTER"},
    "tile_note": {"bg": "F7F9FC", "fg": MUTE, "size": 8, "align": "CENTER", "valign": "TOP", "bottom": True},
}
CHIP = {"sell": ("C62828", "SELL"), "profit": ("2E7D32", "TAKE PROFIT"), "stop": ("EF6C00", "RAISE STOP"),
        "broker": ("B26A00", "CONFIRM"), "review": ("6A1B9A", "REVIEW"), "buy": ("1565C0", "BUY"),
        "skip": ("7B1F1F", "DO NOT BUY"), "news": ("8E5A00", "READ FIRST"), "nofund": ("5F6B7A", "NOT FUNDED"),
        "placed": ("00695C", "PLACED")}
for _k, (_bg, _label) in CHIP.items():                         # the step number, and the kind's name under it
    STYLES["chip_" + _k] = {"bg": _bg, "fg": "FFFFFF", "bold": True, "size": 15, "align": "CENTER", "valign": "BOTTOM"}
    STYLES["chiplabel_" + _k] = {"bg": _bg, "fg": "FFFFFF", "bold": True, "size": 8, "align": "CENTER", "valign": "TOP"}
ACTION_STYLE = {"SELL": "act_sell", "TAKE PROFIT": "act_profit", "RAISE STOP": "act_stop", "REVIEW": "act_review",
                "CHECK": "act_review", "HOLD": "act_hold", "NO RULE": "act_none"}
STATUS_STYLE = {"OPEN": "st_good", "WAIT": "st_warn", "MATCH": "st_good", "MISMATCH": "st_warn", "STALE": "st_warn",
                "INFO": "st_info", "none": "st_good", "not read": "st_bad", "PASS": "st_good", "FAIL": "st_bad",
                "BROAD": "st_good", "MIXED": "st_info", "WASHED OUT": "st_warn", "WASHED_OUT": "st_warn"}


def step_kind(s: Dict[str, Any]) -> str:
    """The colour family of a step: its own "kind" when it names one, else from the urgency its source gave it."""
    if s.get("kind") in CHIP:
        return s["kind"]
    u = float(s.get("urgency") or 0)
    return ("sell" if u < 1 else "profit" if u < 2 else "stop" if u < 2.5 else "broker" if u < 3 else
            "review" if u < 5 else "buy")


def text_px(value: Any, width_px: float, pt: float = 10, bold: bool = False) -> int:
    """Row height (px) that shows `value` wrapped in a cell `width_px` wide. Merged cells get no automatic height in
    Sheets, so the height is estimated - on the generous side: a little white space, never clipped text."""
    per_line = max(8, int((width_px - 14) / (pt * (0.70 if bold else 0.64))))
    lines = 0
    for part in (text(value).split("\n") or [""]):
        used, n = 0, 1
        for word in part.split(" "):                                # greedy word wrap, as the sheet does it
            w = len(word)
            if used and used + 1 + w > per_line:
                n, used = n + 1, 0
            if w > per_line:                                        # a word longer than the line is broken
                n, w = n + (w - 1) // per_line, ((w - 1) % per_line) + 1
            used = used + (1 if used else 0) + w
        lines += n
    return int(max(21, round(lines * pt * 1.55 + 9)))


class _Page(object):
    def __init__(self) -> None:
        self.rows: List[List[Any]] = []
        self.styles: List[List[str]] = []
        self.merges: List[Tuple[int, int, int, int]] = []          # (row0, row1, col0, col1), ends exclusive
        self.heights: Dict[int, int] = {}

    def add(self, cells: Sequence[Any] = (), style: Any = "", spans: Sequence[Tuple[int, int]] = (),
            height: Optional[int] = None) -> int:
        """One row. `style`: one name for every cell, or a list (missing cells: none). `spans`: (col0, col1) merges."""
        r = len(self.rows)
        self.rows.append((list(cells) + [""] * WIDTH)[:WIDTH])
        st = [style] * WIDTH if isinstance(style, str) else (list(style) + [""] * WIDTH)[:WIDTH]
        for c0, c1 in spans:
            self.merges.append((r, r + 1, c0, c1))
            for c in range(c0 + 1, c1):
                st[c] = st[c0]                                      # the merged area takes the anchor's look
        self.styles.append(st)
        need = int(height or 0)
        if spans:
            # Sheets sizes a row from its unmerged cells only, so a row that has a merge gets its height here:
            # the tallest of its cells, each wrapped in its own (merged) width
            anchors = dict((c0, c1) for c0, c1 in spans)
            covered = set(c for c0, c1 in spans for c in range(c0 + 1, c1))
            for c, v in enumerate(self.rows[r]):
                if c in covered or not text(v):
                    continue
                sp = STYLES.get(st[c]) or {}
                need = max(need, text_px(v, sum(COL_PX[c:anchors.get(c, c + 1)]), sp.get("size", 10), bool(sp.get("bold"))))
        if need:
            self.heights[r] = need
        return r

    def full(self, value: Any, style: str, height: Optional[int] = None) -> int:
        """One value across the whole width (the height given is a floor)."""
        return self.add([value], style, [(0, WIDTH)], height)


def day_tiles(d: Dict[str, Any], n_buy: int, hard: Sequence[str], unfunded: Sequence[str]) -> List[Tuple[str, str, str, str]]:
    """Five (label, value, note, tone) tiles; tone is plain | good | warn | bad."""
    cash, src = d.get("cash"), d.get("cash_src") or "ledger"
    open_cost = sum(pick_cost(p) or 0.0 for p in d["picks"] if p["status"] == "OPEN" and p.get("qty") and not p.get("unfunded"))
    placed = sum(1 for p in d["picks"] if p.get("placed"))
    news = d.get("news") or {}
    where = {"ibkr": "CASH AT IBKR", "ledger": "CASH (LEDGER)"}.get(src, "CASH")
    t: List[Tuple[str, str, str, str]] = []
    t.append((where, "%s SAR" % fmt(cash, 0) if cash is not None else "not read",
              d.get("cash_at") or "the buys were not checked against cash", "plain" if cash is not None else "warn"))
    t.append(("BUYS TODAY", "%d of %d" % (n_buy + placed, sum(1 for p in d["picks"] if p["status"] == "OPEN" and p.get("qty"))),
              ("%s SAR" % fmt(open_cost, 0) if open_cost else "no open buy") +
              (" · %d placed" % placed if placed else "") + (" · %d not funded" % len(unfunded) if unfunded else ""),
              "bad" if unfunded else "plain"))
    t.append(("CASH AFTER THE BUYS", "%s SAR" % fmt(d.get("cash_left"), 0) if d.get("cash_left") is not None else "-",
              "after every funded buy fills at its top limit", "plain"))
    if not news.get("read"):
        t.append(("NEWS GATE", "not read", "_News_Gate", "warn"))
    else:
        t.append(("NEWS GATE", news["verdict"].replace("_", " "), "%s Riyadh · observe-only" % (news.get("at") or "-"),
                  "bad" if d.get("news_live") else ("good" if news["verdict"] == "GO" else "plain")))
    soft = [a["symbol"] for a in d.get("align") or [] if a["level"] == "soft"]
    t.append(("PAGES", "DISAGREE" if hard else ("THIS PAGE LEADS" if soft else "ALIGNED"),
              ("do not buy: " + ", ".join(hard)) if hard else
              ("Top 10 pages size no ticket for " + ", ".join(soft) if soft else "no page sizes anything else"),
              "bad" if hard else ("warn" if soft else "good")))
    return t


def page_layout(d: Dict[str, Any], stamp: str, day: str, k: Dict[str, float]) -> _Page:
    """The whole page: values (every one copied from a source page), a style name per cell, merges, row heights."""
    ev, plan = d["evidence"], d["plan"]
    miss = d.get("missing") or []
    n_hold = sum(1 for s in d["steps"] if s["source"] == "Exit_Plan")
    n_buy = sum(1 for s in d["steps"] if s["source"] == "Daily_Plan" and step_kind(s) == "buy")
    hard = [a["symbol"] for a in d.get("align") or [] if a["level"] == "hard"]
    unfunded = [p["symbol"] for p in d["picks"] if p.get("unfunded")]
    flags = ((" · PAGES DISAGREE: " + ", ".join(hard) if hard else "") +
             (" · NOT FUNDED: " + ", ".join(unfunded) if unfunded else "") +
             (" · NEWS GATE: " + d["news"]["verdict"].replace("_", " ") if d.get("news_live") else ""))
    waiting = sum(1 for p in d["picks"] if p["status"] == "WAIT")
    pg = _Page()
    pg.full("TODAY ACTION PLAN - %s" % day, "title", 44)
    pg.full("Built %s Riyadh · today_action_plan %s · copied from Exit_Plan, Daily_Plan, Evidence_Board and "
            "Consistency_Check · not an order: nothing here places, changes or cancels a trade" % (stamp, __version__),
            "sub")
    pg.full("%d step(s) on holdings · %d new buy(s) open · %d pick(s) waiting for price · %d page difference(s) to "
            "know about%s" % (n_hold, n_buy, waiting, len(d["issues"]),
                              flags + (" · OLD SOURCE: " + ", ".join(d["stale"]) if d["stale"] else "") +
                              (" · NOT READ: " + ", ".join(d["missing"]) if d.get("missing") else "")),
            "alert" if (d["stale"] or miss or flags) else "summary", 30)
    if d.get("aligned"):                                            # 1.3.0: five tiles - the day at a glance
        tiles = day_tiles(d, n_buy, hard, unfunded)
        pg.add(height=8)
        pg.add([c for t in tiles for c in (t[0], "")], "tile_label", [(i, i + 2) for i in range(0, WIDTH, 2)], 22)
        pg.add([c for t in tiles for c in (t[1], "")], [st for t in tiles for st in ("tile_" + t[3], "")],
               [(i, i + 2) for i in range(0, WIDTH, 2)], 34)
        pg.add([c for t in tiles for c in (t[2], "")], "tile_note", [(i, i + 2) for i in range(0, WIDTH, 2)])
    pg.add(height=10)

    # ---- A. the day's steps: two rows each - the instruction, then its levels and reason
    pg.full("A. DO TODAY - in this order", "band_a", 32)
    pg.add(["Step", "Symbol · from", "What to do, with its levels and reason underneath"], "head", [(2, WIDTH)])
    for i, s in enumerate(d["steps"], 1):
        kind = step_kind(s)
        pg.add([i, s["symbol"], s["do"]], ["chip_" + kind, "card_sym", "card_do"], [(2, WIDTH)], 34)
        pg.add([CHIP[kind][1], s["source"], s["levels"], "", "", "", s["why"]],
               ["chiplabel_" + kind, "card_src", "card_note", "", "", "", "card_note"], [(2, 6), (6, WIDTH)], 28)
    if not d["steps"]:
        # a source that could not be read is never an all-clear (Codex P2, #966)
        unread = "Exit_Plan" in miss or "_Daily_Plan" in miss
        msg = ("No step can be given: %s could not be read. Refresh the plan pages, then this page." % ", ".join(miss)
               if unread else "Nothing to do today: every holding reads HOLD with its broker stop in place, and no new "
                              "buy is open.")
        pg.add(["", "", msg], ["", "", "msg_alert" if unread else "msg"], [(2, WIDTH)])
    elif "Exit_Plan" in miss or "_Daily_Plan" in miss:
        pg.add(["", "", "INCOMPLETE: %s could not be read, so steps may be missing." % ", ".join(
            m for m in miss if m in ("Exit_Plan", "_Daily_Plan"))], ["", "", "msg_alert"], [(2, WIDTH)])

    # ---- 1.2.0 [ALIGN]: the same names on the Top 10 pages and here
    if d.get("aligned"):
        t10, v2 = d["t10"], d["v2"]
        pg.add(height=10)
        pg.full("PAGE ALIGNMENT - the same names on Top 10, Top 10 v2 and this plan", "band", 30)
        pg.full("Orders come from section A only. The Top 10 pages rank and research; a ticket there that is not an OPEN "
                "pick here is not an order.", "msg")
        for label, page in (("Top 10 (main)", t10), ("Top 10 v2", v2)):
            pg.add([label, "", ("%s · built %s" % (page.get("banner") or "no banner", page.get("built") or "time not read"))
                    if page.get("read") else "page not read"], ["cell_b", "", "cell"], [(0, 2), (2, WIDTH)])
        pg.add(["Symbol", "This plan (order of record)", "", "Top 10 (main)", "", "Top 10 v2", "", "", "Reading"], "head",
               [(1, 3), (3, 5), (5, 8), (8, WIDTH)])
        for a in d["align"]:
            pg.add([a["symbol"], a["plan"], "", a["main"], "", a["v2"], "", "", a["reading"]],
                   ["cell_b", "cell", "", "cell", "", "cell", "", "", {"hard": "bad", "soft": "warn"}.get(a["level"], "ok")],
                   [(1, 3), (3, 5), (5, 8), (8, WIDTH)])
        if not d["align"]:
            pg.add(["", "No pick today and no sized ticket on either Top 10 page."], ["", "msg"], [(1, WIDTH)])

    # ---- B. holdings
    pg.add(height=10)
    broker = d.get("broker") or {}
    pg.full("B. HOLDINGS (%d)%s" % (len(d["holdings"]), " - broker stops from the IBKR capture of %s Riyadh" % broker["at"]
                                    if broker.get("used") else ""), "band", 30)
    pg.add(["Symbol", "Name", "Shares", "Last", "P/L %", "Action", "Plan stop", "Broker stop", "Target", "Note"],
           ["head", "head", "head_num", "head_num", "head_num", "head", "head_num", "head_num", "head_num", "head"])
    for h in d["holdings"]:
        bs = h.get("broker_stop")
        state = stop_state(h, k["stop_tol"])
        note = "" if h["action"] == "NO RULE" else (
            {"none": "no stop at IBKR", "partial": "stop covers %s of %s shares" % (fmt(h.get("broker_cover"), 0), fmt(h.get("qty"), 0)),
             "unpriced": "stop price not read", "low": "broker stop below the plan", "ok": "stop in place"}[state] if state else (
                "not at IBKR" if h.get("at_broker") is False and h.get("stop") else (
                    ("broker orders not read" if d.get("broker_note") else "no broker stop read") if bs is None else (
                        "broker stop below the plan" if h.get("stop") and bs < h["stop"] * (1 - k["stop_tol"] / 100.0)
                        else "stop in place"))))
        pl = text(h.get("pl_pct", ""))
        pg.add([h["symbol"], h["name"], fmt(h.get("qty"), 0), fmt(h.get("last")), h.get("pl_pct", ""), h["action"],
                fmt(h.get("stop")), fmt(bs), fmt(h.get("target")), note],
               ["cell_b", "cell", "num", "num", "neg" if pl.startswith("-") else ("pos" if pl else "num"),
                ACTION_STYLE.get(h["action"], "act_hold"), "num", "num", "num",
                "ok" if note == "stop in place" else ("bad" if state in ("none", "partial") else ("warn" if note else "cell"))])
    if not d["holdings"]:
        pg.add(["", "The Exit_Plan page lists no holding (or could not be read)."], ["", "msg"], [(1, WIDTH)])

    # ---- C. new buys
    title = "C. NEW BUYS - plan of %s, cut %s (%s)" % (plan.get("date") or "-", plan.get("cut_at") or "-", plan.get("quality") or "-")
    pg.add(height=10)
    pg.full(title, "band", 30)
    pg.add(["Symbol", "Status", "Shares", "Entry zone", "Price now", "Stop", "First target", "Chance up, 21 bars",
            "Shadow score", "Reason"],
           ["head", "head", "head_num", "head_num", "head_num", "head_num", "head_num", "head_num", "head_num", "head"])
    for p in d["picks"]:
        e = (ev.get("names") or {}).get(p["symbol"]) or {}
        pg.add([p["symbol"], p["status"], fmt(p.get("qty"), 0),
                "%s - %s" % (fmt(p.get("zone_low")), fmt(p.get("zone_high"))), fmt(p.get("price")), fmt(p.get("stop")),
                fmt(p.get("tp1")), e.get("chance", ""), e.get("shadow", ""), p.get("reason", "")],
               ["cell_b", STATUS_STYLE.get(p["status"], "st_info"), "num", "num", "num", "num", "num", "num", "num", "cell"])
    if not d["picks"]:
        pg.add(["", "No plan has been cut today yet." if not plan.get("is_today") else
                "Today's plan has no pick: no name passed the execution gates."], ["", "msg"], [(1, WIDTH)])

    # ---- D. pipeline
    pg.add(height=10)
    pg.full("D. PIPELINE - next in line (%d of %d)" % (min(len(d["pipeline"]), int(k["pipeline"])), len(d["pipeline"])),
            "band", 30)
    pg.add(["Symbol", "Name", "Rank", "Score", "Waiting for"], ["head", "head", "head_num", "head_num", "head"], [(4, WIDTH)])
    for p in d["pipeline"][:int(k["pipeline"])]:
        pg.add([p["symbol"], p["name"], fmt(p.get("rank"), 0), fmt(p.get("score"), 1), p.get("waiting", "")],
               ["cell_b", "cell", "num", "num", "cell"], [(4, WIDTH)])

    # ---- E. context
    pg.add(height=10)
    pg.full("E. CONTEXT", "band", 30)
    pg.full("Market state (Evidence_Board)", "subhead")
    pg.add(["Market", "Bars to", "Above 50-day avg", "State", "After this state: names up", "Weeks seen"],
           ["head", "head", "head_num", "head", "head_num", "head_num", "head", "head", "head", "head"])
    for s in ev.get("states") or []:
        pg.add([s["market"], s["bars_to"], s["above50"], s["state"], s["all_up"], s["weeks"]],
               ["cell_b", "cell", "num", STATUS_STYLE.get(text(s["state"]).upper(), "st_info"), "num", "num", "cell",
                "cell", "cell", "cell"])
    if not ev.get("states"):
        pg.add(["", "The Evidence_Board page has no market state yet."], ["", "msg"], [(1, WIDTH)])
    pg.full("Learning gate (copied from S1_Gate)", "subhead")
    for g in (ev.get("gate") or [["", "not on the Evidence Board yet"]])[:4]:
        # a gate row is a label and its figures; its bracketed run-log cells ("[S1-...]") are left on the source page
        cells = [text(c) for c in g if text(c) and not text(c).startswith("[")]
        if not cells:
            continue
        if len(cells) == 1:
            pg.add([cells[0]], "cell_b" if text(g[0]) else "msg", [(0, WIDTH)])
            continue
        rest = " · ".join(cells[1:])
        verdict = "bad" if re.search(r"\bFAIL\b", rest) else ("ok" if re.search(r"\bPASS\b", rest) else "cell")
        pg.add([cells[0], "", rest], ["cell_b", "", verdict], [(0, 2), (2, WIDTH)])
    pg.full("Page differences (Consistency_Check)", "subhead")
    pg.add(["Check", "", "Status", "Detail"], "head", [(0, 2), (3, WIDTH)])
    issues = [[x["check"], "", x["status"], x["detail"]] for x in d["issues"]]
    if not issues:
        issues = [["", "", "not read", "The Consistency_Check page could not be read."] if "Consistency_Check" in miss else
                  ["", "", "none", "The pages agree on every checked figure."]]
    for x in issues:
        pg.add(x, ["cell_b", "", STATUS_STYLE.get(x[2], "st_info"), "cell"], [(0, 2), (3, WIDTH)])
    if d.get("aligned"):                                            # 1.2.0: the news gate and the cash ledger
        news = d.get("news") or {}
        pg.full("News & Macro Gate (_News_Gate; observe-only)", "subhead")
        if news.get("read"):
            old = news.get("age_h") is not None and not d.get("news_live") and news["verdict"] != "GO"
            pg.add([news["verdict"].replace("_", " "), "", news["at"] + (" (older than the limit)" if old else ""),
                    news.get("drivers") or "no driver recorded"],
                   ["bad" if d.get("news_live") else "cell_b", "", "cell", "cell"], [(0, 2), (3, WIDTH)])
        else:
            pg.add(["", "The _News_Gate tab was not read."], ["", "msg"], [(1, WIDTH)])
        pg.full("Cash (%s)" % {"ibkr": "IBKR read-only capture"}.get(d.get("cash_src"), "_Cash_Snapshot"), "subhead")
        if d.get("cash") is not None:
            open_cost = sum(pick_cost(p) or 0.0 for p in d["picks"] if p["status"] == "OPEN" and p.get("qty"))
            pg.add(["%s SAR" % fmt(d["cash"], 0), "", d.get("cash_at", ""),
                    "open buys cost %s SAR · left after the funded buys %s SAR" % (fmt(open_cost, 0), fmt(d.get("cash_left"), 0))],
                   ["cell_b", "", "cell", "bad" if any(p.get("unfunded") for p in d["picks"]) else "cell"],
                   [(0, 2), (3, WIDTH)])
        else:
            pg.add(["", "No cash figure was read: the buys were not checked against cash."], ["", "msg"], [(1, WIDTH)])
        if not broker.get("used"):
            pg.add(["", "IBKR capture not used: %s." % (broker.get("why") or "not read")], ["", "msg"], [(1, WIDTH)])
        elif broker.get("reserved"):
            pg.add(["", "%s SAR is held by working buy orders on other names and is not counted%s." % (
                fmt(broker["reserved"], 0), "" if broker.get("complete", True) else "; a buy order without a limit price is not counted either")],
                ["", "msg"], [(1, WIDTH)])
    pg.full("Source pages", "subhead")
    pg.add(["Source page", "", "Age (hours)", "Reading"], ["head", "", "head_num", "head"], [(0, 2), (3, WIDTH)])
    for name in sorted(d["ages"]):
        a = d["ages"][name]
        reading = "no time found" if a is None else (
            "OLD - refresh it before acting" if a > stale_limit(name, k) else "current")
        pg.add([name, "", "" if a is None else "%.1f" % a, reading],
               ["cell_b", "", "num", "ok" if reading == "current" else ("bad" if reading.startswith("OLD") else "warn")],
               [(0, 2), (3, WIDTH)])
    pg.add(height=10)
    pg.full("How to read: section A is the whole instruction; B to E show where each step comes from. A level on "
            "this page is always the level on its source page. Trading decisions and every order are the "
            "owner's; this page is information, not investment advice.", "foot")
    return pg


def page_rows(d: Dict[str, Any], stamp: str, day: str, k: Dict[str, float]) -> List[List[Any]]:
    return page_layout(d, stamp, day, k).rows


def _rgb(h: str) -> Dict[str, float]:
    return {"red": int(h[0:2], 16) / 255.0, "green": int(h[2:4], 16) / 255.0, "blue": int(h[4:6], 16) / 255.0}


def cell_format(name: str) -> Dict[str, Any]:
    """The Sheets userEnteredFormat of one style name ("" = a plain cell)."""
    sp = STYLES.get(name) or {}
    f: Dict[str, Any] = {"wrapStrategy": "WRAP", "verticalAlignment": sp.get("valign", "MIDDLE"),
                         "hyperlinkDisplayType": "PLAIN_TEXT",
                         "padding": {"top": 3, "bottom": 3, "left": 7, "right": 7},
                         "textFormat": {"fontSize": sp.get("size", 10), "bold": bool(sp.get("bold")),
                                        "italic": bool(sp.get("italic")), "underline": False,
                                        "foregroundColor": _rgb(sp.get("fg", INK))}}
    if sp.get("bg"):
        f["backgroundColor"] = _rgb(sp["bg"])
    if sp.get("align"):
        f["horizontalAlignment"] = sp["align"]
    borders = {}
    for side in ("top", "bottom"):
        if sp.get(side):                                            # "thin" = the hairline between table rows
            borders[side] = {"style": "SOLID", "color": _rgb(HAIR if sp[side] == "thin" else LINE)}
    if borders:
        f["borders"] = borders
    return f


def cell_data(value: Any, style: str) -> Dict[str, Any]:
    out: Dict[str, Any] = {"userEnteredFormat": cell_format(style)}
    if isinstance(value, bool) or value is None or value == "":
        return out
    if isinstance(value, (int, float)) and math.isfinite(float(value)):
        out["userEnteredValue"] = {"numberValue": value}
    else:
        out["userEnteredValue"] = {"stringValue": str(value)}       # as typed: never parsed into a date or a formula
    return out


def page_requests(sheet_id: int, pg: _Page) -> List[Dict[str, Any]]:
    """ONE batch for the whole page - values, looks, merges, sizes. Sheets applies a batch whole or not at all, so a
    reader never meets new values under an old layout."""
    n = max(len(pg.rows) + 1, 12)

    def rng(r0: int, r1: int, c0: int, c1: int) -> Dict[str, int]:
        return {"sheetId": sheet_id, "startRowIndex": r0, "endRowIndex": r1, "startColumnIndex": c0, "endColumnIndex": c1}

    def size(dim: str, i: int, px: int) -> Dict[str, Any]:
        return {"updateDimensionProperties": {"range": {"sheetId": sheet_id, "dimension": dim, "startIndex": i,
                                                        "endIndex": i + 1},
                                              "properties": {"pixelSize": int(px)}, "fields": "pixelSize"}}
    reqs: List[Dict[str, Any]] = [
        {"unmergeCells": {"range": {"sheetId": sheet_id}}},
        {"updateSheetProperties": {
            "properties": {"sheetId": sheet_id, "tabColorStyle": {"rgbColor": _rgb("0F5C63")},
                           "gridProperties": {"rowCount": n, "columnCount": WIDTH, "frozenRowCount": 1, "hideGridlines": True}},
            "fields": "tabColorStyle,gridProperties.rowCount,gridProperties.columnCount,gridProperties.frozenRowCount,"
                      "gridProperties.hideGridlines"}},
        {"updateCells": {"range": rng(0, n, 0, WIDTH), "fields": "userEnteredValue,userEnteredFormat",
                         "rows": [{"values": [cell_data(v, s) for v, s in zip(row, st)]}
                                  for row, st in zip(pg.rows, pg.styles)]}}]
    for r0, r1, c0, c1 in pg.merges:
        reqs.append({"mergeCells": {"range": rng(r0, r1, c0, c1), "mergeType": "MERGE_ALL"}})
    for i, px in enumerate(COL_PX):
        reqs.append(size("COLUMNS", i, px))
    reqs.append({"autoResizeDimensions": {"dimensions": {"sheetId": sheet_id, "dimension": "ROWS", "startIndex": 0,
                                                         "endIndex": len(pg.rows)}}})
    for r in sorted(pg.heights):
        reqs.append(size("ROWS", r, pg.heights[r]))
    return reqs


def plain_requests(sheet_id: int, n_rows: int) -> List[Dict[str, Any]]:
    """Take a styled page's looks off the sheet, so the plain grid that follows is readable: no merges, no formats,
    every row at the default height (a spacer row was 10 px), even columns (Codex P2, #969)."""
    whole = {"sheetId": sheet_id}
    return [{"unmergeCells": {"range": whole}},
            {"repeatCell": {"range": whole, "cell": {}, "fields": "userEnteredFormat"}},
            {"updateDimensionProperties": {"range": {"sheetId": sheet_id, "dimension": "ROWS", "startIndex": 0,
                                                     "endIndex": int(n_rows)},
                                           "properties": {"pixelSize": 21}, "fields": "pixelSize"}},
            {"updateDimensionProperties": {"range": {"sheetId": sheet_id, "dimension": "COLUMNS", "startIndex": 0,
                                                     "endIndex": WIDTH},
                                           "properties": {"pixelSize": PLAIN_COL_PX}, "fields": "pixelSize"}}]


def write_page(book: Any, tab: str, pg: _Page, styled: bool, retries: int, sleep: Callable[[float], None],
               show: bool = False) -> str:
    """Write the page; return "styled" or "plain". A styled write that Sheets refuses (anything but its quota) falls
    back to the plain grid, so a formatting fault never costs the day's instructions."""
    from scripts.daily_plan import write_grid  # noqa: PLC0415

    def target() -> Any:
        try:
            return book.worksheet(tab)
        except Exception as e:  # noqa: BLE001
            if type(e).__name__ != "WorksheetNotFound":
                raise
            return book.add_worksheet(title=tab, rows=max(80, len(pg.rows) + 5), cols=WIDTH)
    ws = with_retry(target, retries, sleep, "opening the page's tab")   # the quota can answer here too (Codex P2, #969)
    if show and (getattr(ws, "isSheetHidden", False) or getattr(ws, "is_hidden", False)):
        try:                                                        # a preview hidden by a normal run is shown again
            with_retry(ws.show, retries, sleep, "showing the tab")
        except Exception as e:  # noqa: BLE001 - the page is still written; only its tab stays hidden
            if quota_error(e):
                raise
            _out("could not show %s: %s" % (tab, type(e).__name__))
    if styled:
        try:
            with_retry(lambda: book.batch_update({"requests": page_requests(ws.id, pg)}), retries, sleep,
                       "writing the page")
            return "styled"
        except Exception as e:  # noqa: BLE001 - the plain page below still carries every instruction
            if quota_error(e):
                raise
            print("::warning::Today Action Plan: the styled write was refused (%s); the page was written plain"
                  % type(e).__name__, flush=True)
            _out("styled write refused: %s: %s" % (type(e).__name__, text(e)[:300]))
    try:                                                            # an earlier styled page's looks would hide values
        rows_now = max(1, min(int(getattr(ws, "row_count", 0) or 0) or len(pg.rows) + 5, 5000))
        with_retry(lambda: book.batch_update({"requests": plain_requests(ws.id, rows_now)}),
                   retries, sleep, "clearing the page's looks")
    except Exception as e:  # noqa: BLE001
        if quota_error(e):
            raise
        _out("could not clear the page's looks: %s" % type(e).__name__)
    with_retry(lambda: write_grid(book, tab, pg.rows, WIDTH, 80), retries, sleep, "writing the page")
    return "plain"


# ----------------------------------------------------------------------------- #
def hide_list(spec: str) -> List[str]:
    if spec.strip().lower() in ("none", "off", "-"):
        return []
    out: List[str] = []
    for t in spec.split(","):
        t = t.strip()
        if t and _TAB_RE.match(t) and t != TAB and t not in out:
            out.append(t)
    return out


def hide_tabs(book: Any, names: Sequence[str]) -> List[str]:
    """Hide each named tab that exists and is visible. Never deletes; a failure is reported, not raised."""
    done: List[str] = []
    for n in names:
        try:
            ws = book.worksheet(n)
            if getattr(ws, "isSheetHidden", False) or getattr(ws, "is_hidden", False):
                continue
            ws.hide()
            done.append(n)
        except Exception as e:  # noqa: BLE001 - housekeeping must not fail the page
            if type(e).__name__ != "WorksheetNotFound":
                _out("could not hide %s: %s" % (n, type(e).__name__))
    return done


def run_once(dry_run: bool, book=None, now: Optional[_dt.datetime] = None,
             sleep: Optional[Callable[[float], None]] = None, capture: Optional[Dict[str, Any]] = None) -> int:
    """`capture` (1.3.0): a parsed IBKR capture (scripts.ibkr_capture.parse). Left out, the live run reads the newest
    one from Drive; a run on an injected workbook reads none."""
    now = now or _dt.datetime.now(UTC)
    own_book = book is None
    if not enabled():
        _out("TFB_TODAY_PLAN is off: nothing read, nothing written")
        return 0
    k = {"stale_h": _env_num("TFB_TAP_STALE_H", 3, 0.25, 72), "stop_tol": _env_num("TFB_TAP_STOP_TOL_PCT", 1, 0, 20),
         "pipeline": _env_num("TFB_TAP_PIPELINE", 5, 0, 30),
         "stale_ev": _env_num("TFB_TAP_STALE_EVIDENCE_H", 18, 0.25, 96),
         "news_max_h": _env_num("TFB_TAP_NEWS_MAX_H", 24, 1, 168),
         "ibkr_max_h": _env_num("TFB_TAP_IBKR_MAX_AGE_H", 30, 0.5, 96)}
    aligned = _env("TFB_TAP_ALIGN", "on").lower() not in ("off", "0", "false", "no")
    retries, sleep = int(_env_num("TFB_TAP_RETRIES", 3, 0, 5)), sleep or time.sleep
    held = {"book": book}

    def load() -> Tuple[Dict[str, List[List[Any]]], List[List[Any]]]:
        if held["book"] is None:
            from scripts.run_calendar_sync import _open_book  # noqa: PLC0415
            held["book"] = _open_book()
        from scripts.daily_plan import _read  # noqa: PLC0415
        b = held["book"]
        got = {"Exit_Plan": _read(b, "Exit_Plan", "A1:P80"), "Daily_Plan": _read(b, "Daily_Plan", "A1:N6"),
               "Evidence_Board": _read(b, "Evidence_Board", "A1:L120"),
               "Consistency_Check": _read(b, "Consistency_Check", "A1:F60")}
        more: Dict[str, List[List[Any]]] = {}
        for tab, rng in (((T10_TAB, "A1:AE60"), (V2_TAB, "A1:P70"), (NEWS_TAB, "A1:L3000"), (CASH_TAB, "A1:G2000"))
                         if aligned else ()):
            try:                                                    # optional: a tab that cannot be read changes no step
                more[tab] = _read(b, tab, rng)
            except Exception as e:  # noqa: BLE001
                if quota_error(e):
                    raise
                _out("%s not read: %s" % (tab, type(e).__name__))
        held["more"] = more
        return got, _read(b, "_Daily_Plan", "A1:AB2100")
    try:
        pages, ledger = with_retry(load, retries, sleep, "reading the source pages")
    except Exception as e:  # noqa: BLE001 - a failed read leaves the previous page in place
        _out("ERROR: %s: %s; nothing written" % (type(e).__name__, text(e)[:300]))
        return 2
    book = held["book"]
    local = now.astimezone(RIYADH)
    ages: Dict[str, Optional[float]] = {}
    for name, vals in pages.items():
        t = page_time(vals, now)
        ages[name] = None if t is None else max(0.0, (now - t).total_seconds() / 3600.0)
    missing = [n for n, need in (("Exit_Plan", ("Symbol", "Qty", "Stop", "Action", "Why")),
                                 ("Consistency_Check", ("Check", "Status", "Detail")),
                                 ("Evidence_Board", ("Market", "Above 50-day avg", "State")))
               if find_table(pages[n], need)[0] is None]
    plan = read_plan(ledger, local.date())
    if not plan["date"]:
        missing.append("_Daily_Plan")
    extra: Optional[Dict[str, Any]] = None
    if aligned:
        from scripts.daily_plan import snapshot_cash  # noqa: PLC0415
        more = held.get("more") or {}
        cash, cash_at = snapshot_cash(more.get(CASH_TAB) or [])
        extra = {"t10": read_top10(more.get(T10_TAB) or []), "v2": read_top10_v2(more.get(V2_TAB) or []),
                 "news": read_news_gate(more.get(NEWS_TAB) or [], now), "cash": cash, "cash_at": cash_at,
                 "cash_src": "ledger" if cash is not None else ""}
    holdings, broker_note = read_holdings(pages["Exit_Plan"]), broker_unknown(pages["Exit_Plan"])
    if aligned:                                                     # 1.3.0: the broker's own cash and orders
        if capture is None and own_book:
            from scripts import ibkr_capture  # noqa: PLC0415
            capture = ibkr_capture.latest(now, out=_out)
        broker = apply_capture(holdings, plan, capture, k["ibkr_max_h"])
        extra["broker"] = broker
        if broker["used"]:
            broker_note = ""                                        # the orders are known: each holding is checked
            extra.update({"cash": broker["cash"], "cash_at": "IBKR capture of %s Riyadh" % broker["at"], "cash_src": "ibkr"})
        else:
            _out("IBKR capture not used: %s" % broker["why"])
    d = build(holdings, plan, read_evidence(pages["Evidence_Board"]),
              read_consistency(pages["Consistency_Check"]), ages, k, missing, broker_note, extra)
    for tab, page in (((T10_TAB, d["t10"]), (V2_TAB, d["v2"])) if aligned else ()):
        if not page.get("read"):                                    # say what was read, so a moved header is visible
            got = (held.get("more") or {}).get(tab) or []
            near = [" | ".join(_squash(c) for c in r if text(c))[:160] for r in got
                    if any(_squash(c) in ("Seat status", "Ticket SAR") for c in r)]
            _out("%s: no seats table found in %d row(s)%s" % (tab, len(got), "; nearest header: " + near[0] if near else ""))
    for a in d["align"]:
        _out("ALIGN %-9s %-5s plan: %s | main: %s | v2: %s" % (a["symbol"], a["level"], a["plan"], a["main"], a["v2"]))
    if aligned:
        _out("news gate %s (%s) · cash %s (%s)" % (                  # no amount in the run log
            d["news"].get("verdict") or "not read", d["news"].get("at") or "-",
            "read" if d["cash"] is not None else "not read", d["cash_at"] or "-"))
    for i, s in enumerate(d["steps"], 1):
        _out("STEP %d %-9s %s | %s" % (i, s["symbol"], s["do"], s["levels"]))
    if not d["steps"]:
        _out("no step today")
    _out("holdings %d · picks %d (%s) · pipeline %d · differences %d · old sources: %s · not read: %s" % (
        len(d["holdings"]), len(d["picks"]), ", ".join("%s %s" % (p["symbol"], p["status"]) for p in d["picks"]) or "-",
        len(d["pipeline"]), len(d["issues"]), ", ".join(d["stale"]) or "none", ", ".join(d["missing"]) or "none"))
    pg = page_layout(d, local.strftime("%Y-%m-%d %H:%M"), local.strftime("%a %d %b %Y"), k)
    rows = pg.rows
    if dry_run:
        _out("dry run: %d page row(s) computed, nothing written" % len(rows))
        return 0
    preview = _env("TFB_TAP_PREVIEW").lower() in ("1", "true", "yes", "on")
    tab = PREVIEW_TAB if preview else TAB                           # a preview never touches the live page
    how = write_page(book, tab, pg, _env("TFB_TAP_STYLE", "on").lower() not in ("off", "0", "false", "no", "plain"),
                     retries, sleep, show=preview)
    hidden = [] if preview else hide_tabs(book, hide_list(_env("TFB_TAP_HIDE_TABS", DEFAULT_HIDE)) + [PREVIEW_TAB])
    _out("wrote the %s page (%d rows, %d step(s), %s)%s" % (tab, len(rows), len(d["steps"]), how,
                                                            "; hid " + ", ".join(hidden) if hidden else ""))
    return 0


def main(argv: Optional[Sequence[str]] = None, **kw) -> int:
    ap = argparse.ArgumentParser(description="TFB Today Action Plan (one page; copies the plan pages)")
    ap.add_argument("--dry-run", action="store_true")
    a = ap.parse_args(list(argv) if argv is not None else None)
    return run_once(a.dry_run, **kw)


if __name__ == "__main__":
    sys.exit(main())
