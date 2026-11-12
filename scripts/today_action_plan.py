#!/usr/bin/env python3
"""
scripts/today_action_plan.py
================================================================================
TFB Today Action Plan - one page that says what to do today, and why
--------------------------------------------------------------------------------
v1.1.0 (2026-11-12)
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
    TFB_TAP_STALE_EVIDENCE_H (18).
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

__version__ = "1.1.0"

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
             "reason": text(cell(r, col.get("Status Reason"))), "price": num(cell(r, col.get("Last Price")))}
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
        step(2, "Raise the stop on %s to %s for all %s." % (sym, fmt(stop), shares),
             "was %s · broker stop %s" % (fmt(h.get("was")) or "-", fmt(bs) or ("not read" if broker_known else
                                                                                "orders not read")))
    elif act in ("REVIEW", "CHECK"):
        step(3, "Review %s today: the exit rule could not decide on its own." % sym,
             "last %s · stop %s" % (fmt(h.get("last")), fmt(stop)))
    if act in ("HOLD", "REVIEW", "CHECK") and stop:
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
    e = ev.get(p["symbol"]) or {}
    why = p.get("reason", "")
    if e.get("chance"):
        why += "; names like it finished up %s of the time over 21 bars (%s)" % (e["chance"], e.get("basis", ""))
    if p.get("labels"):
        why += "; " + p["labels"]
    do = "Buy %s %s with a limit between %s and %s; place the stop at %s." % (
        fmt(p["qty"], 0), p["symbol"], fmt(p.get("zone_low")), fmt(p.get("zone_high")), fmt(p.get("stop")))
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
          broker_note: str = "") -> Dict[str, Any]:
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
    for p in picks:
        s = pick_step(p, evidence.get("names") or {}, plan.get("quality", "").upper() == "VERIFIED")
        if s:
            steps.append(s)
    steps.sort(key=lambda s: (s["urgency"], s["symbol"]))
    stale = sorted(n for n, a in ages.items() if a is not None and a > stale_limit(n, k))
    return {"steps": steps, "holdings": holdings, "picks": picks, "pipeline": (plan["pipeline"] if plan.get("is_today") else []),
            "plan": plan, "evidence": evidence, "issues": issues, "ages": ages, "stale": stale,
            "missing": sorted(missing), "broker_note": broker_note}


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
}
CHIP = {"sell": ("C62828", "SELL"), "profit": ("2E7D32", "TAKE PROFIT"), "stop": ("EF6C00", "RAISE STOP"),
        "broker": ("B26A00", "CONFIRM"), "review": ("6A1B9A", "REVIEW"), "buy": ("1565C0", "BUY")}
for _k, (_bg, _label) in CHIP.items():                         # the step number, and the kind's name under it
    STYLES["chip_" + _k] = {"bg": _bg, "fg": "FFFFFF", "bold": True, "size": 15, "align": "CENTER", "valign": "BOTTOM"}
    STYLES["chiplabel_" + _k] = {"bg": _bg, "fg": "FFFFFF", "bold": True, "size": 8, "align": "CENTER", "valign": "TOP"}
ACTION_STYLE = {"SELL": "act_sell", "TAKE PROFIT": "act_profit", "RAISE STOP": "act_stop", "REVIEW": "act_review",
                "CHECK": "act_review", "HOLD": "act_hold", "NO RULE": "act_none"}
STATUS_STYLE = {"OPEN": "st_good", "WAIT": "st_warn", "MATCH": "st_good", "MISMATCH": "st_warn", "STALE": "st_warn",
                "INFO": "st_info", "none": "st_good", "not read": "st_bad", "PASS": "st_good", "FAIL": "st_bad",
                "BROAD": "st_good", "MIXED": "st_info", "WASHED OUT": "st_warn", "WASHED_OUT": "st_warn"}


def step_kind(s: Dict[str, Any]) -> str:
    """The colour family of a step, from the urgency its source gave it."""
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


def page_layout(d: Dict[str, Any], stamp: str, day: str, k: Dict[str, float]) -> _Page:
    """The whole page: values (every one copied from a source page), a style name per cell, merges, row heights."""
    ev, plan = d["evidence"], d["plan"]
    miss = d.get("missing") or []
    n_hold = sum(1 for s in d["steps"] if s["source"] == "Exit_Plan")
    n_buy = len(d["steps"]) - n_hold
    waiting = sum(1 for p in d["picks"] if p["status"] == "WAIT")
    pg = _Page()
    pg.full("TODAY ACTION PLAN - %s" % day, "title", 44)
    pg.full("Built %s Riyadh · today_action_plan %s · copied from Exit_Plan, Daily_Plan, Evidence_Board and "
            "Consistency_Check · not an order: nothing here places, changes or cancels a trade" % (stamp, __version__),
            "sub")
    pg.full("%d step(s) on holdings · %d new buy(s) open · %d pick(s) waiting for price · %d page difference(s) to "
            "know about%s" % (n_hold, n_buy, waiting, len(d["issues"]),
                              (" · OLD SOURCE: " + ", ".join(d["stale"]) if d["stale"] else "") +
                              (" · NOT READ: " + ", ".join(d["missing"]) if d.get("missing") else "")),
            "alert" if (d["stale"] or miss) else "summary", 30)
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

    # ---- B. holdings
    pg.add(height=10)
    pg.full("B. HOLDINGS (%d)" % len(d["holdings"]), "band", 30)
    pg.add(["Symbol", "Name", "Shares", "Last", "P/L %", "Action", "Plan stop", "Broker stop", "Target", "Note"],
           ["head", "head", "head_num", "head_num", "head_num", "head", "head_num", "head_num", "head_num", "head"])
    for h in d["holdings"]:
        bs = h.get("broker_stop")
        note = "" if h["action"] == "NO RULE" else (
            ("broker orders not read" if d.get("broker_note") else "no broker stop read") if bs is None else (
                "broker stop below the plan" if h.get("stop") and bs < h["stop"] * (1 - k["stop_tol"] / 100.0) else "stop in place"))
        pl = text(h.get("pl_pct", ""))
        pg.add([h["symbol"], h["name"], fmt(h.get("qty"), 0), fmt(h.get("last")), h.get("pl_pct", ""), h["action"],
                fmt(h.get("stop")), fmt(bs), fmt(h.get("target")), note],
               ["cell_b", "cell", "num", "num", "neg" if pl.startswith("-") else ("pos" if pl else "num"),
                ACTION_STYLE.get(h["action"], "act_hold"), "num", "num", "num",
                "ok" if note == "stop in place" else ("warn" if note else "cell")])
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
            "properties": {"sheetId": sheet_id, "gridProperties": {"rowCount": n, "columnCount": WIDTH,
                                                                   "frozenRowCount": 1, "hideGridlines": True}},
            "fields": "gridProperties.rowCount,gridProperties.columnCount,gridProperties.frozenRowCount,"
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
             sleep: Optional[Callable[[float], None]] = None) -> int:
    now = now or _dt.datetime.now(UTC)
    if not enabled():
        _out("TFB_TODAY_PLAN is off: nothing read, nothing written")
        return 0
    k = {"stale_h": _env_num("TFB_TAP_STALE_H", 3, 0.25, 72), "stop_tol": _env_num("TFB_TAP_STOP_TOL_PCT", 1, 0, 20),
         "pipeline": _env_num("TFB_TAP_PIPELINE", 5, 0, 30),
         "stale_ev": _env_num("TFB_TAP_STALE_EVIDENCE_H", 18, 0.25, 96)}
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
    d = build(read_holdings(pages["Exit_Plan"]), plan, read_evidence(pages["Evidence_Board"]),
              read_consistency(pages["Consistency_Check"]), ages, k, missing, broker_unknown(pages["Exit_Plan"]))
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
