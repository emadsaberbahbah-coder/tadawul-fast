#!/usr/bin/env python3
"""
scripts/today_action_plan.py
================================================================================
TFB Today Action Plan - one page that says what to do today, and why
--------------------------------------------------------------------------------
v1.0.0 (2026-11-12)

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
    TFB_TAP_HIDE_TABS (_Daily_Plan,_Exit_Plan,_Evidence_Log; "none" = hide none).
================================================================================
"""
from __future__ import annotations

import argparse
import datetime as _dt
import math
import os
import re
import sys
from typing import Any, Dict, List, Optional, Sequence, Tuple

__version__ = "1.0.0"

UTC = _dt.timezone.utc
RIYADH = _dt.timezone(_dt.timedelta(hours=3))
TAB = "Today_Action_Plan"
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
def holding_steps(h: Dict[str, Any], tol_pct: float) -> List[Dict[str, Any]]:
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
        step(1, "Sell half of %s (%s) and keep a stop at %s on the rest." % (
            sym, "%d shares" % half if half else "half the position", fmt(stop)),
            "last %s · next target %s" % (fmt(h.get("last")), fmt(h.get("target"))))
    elif act == "RAISE STOP":
        step(2, "Raise the stop on %s to %s for all %s." % (sym, fmt(stop), shares),
             "was %s · broker stop %s" % (fmt(h.get("was")) or "-", fmt(bs) or "not read"))
    elif act in ("REVIEW", "CHECK"):
        step(3, "Review %s today: the exit rule could not decide on its own." % sym,
             "last %s · stop %s" % (fmt(h.get("last")), fmt(stop)))
    if act in ("HOLD", "TAKE PROFIT", "REVIEW", "CHECK") and stop:
        if bs is None:
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


def build(holdings: List[Dict[str, Any]], plan: Dict[str, Any], evidence: Dict[str, Any], issues: List[Dict[str, str]],
          ages: Dict[str, Optional[float]], k: Dict[str, float]) -> Dict[str, Any]:
    steps: List[Dict[str, Any]] = []
    for h in holdings:
        steps += holding_steps(h, k["stop_tol"])
    picks = plan["picks"] if plan.get("is_today") else []
    for p in picks:
        s = pick_step(p, evidence.get("names") or {}, plan.get("quality", "").upper() == "VERIFIED")
        if s:
            steps.append(s)
    steps.sort(key=lambda s: (s["urgency"], s["symbol"]))
    stale = sorted(n for n, a in ages.items() if a is not None and a > k["stale_h"])
    return {"steps": steps, "holdings": holdings, "picks": picks, "pipeline": (plan["pipeline"] if plan.get("is_today") else []),
            "plan": plan, "evidence": evidence, "issues": issues, "ages": ages, "stale": stale}


def page_rows(d: Dict[str, Any], stamp: str, day: str, k: Dict[str, float]) -> List[List[Any]]:
    def pad(r: Sequence[Any]) -> List[Any]:
        return (list(r) + [""] * WIDTH)[:WIDTH]

    ev, plan = d["evidence"], d["plan"]
    n_hold = sum(1 for s in d["steps"] if s["source"] == "Exit_Plan")
    n_buy = len(d["steps"]) - n_hold
    waiting = sum(1 for p in d["picks"] if p["status"] == "WAIT")
    out = [pad(["TODAY ACTION PLAN - %s" % day]),
           pad(["Built %s Riyadh · today_action_plan %s · copied from Exit_Plan, Daily_Plan, Evidence_Board and "
                "Consistency_Check · not an order: nothing here places, changes or cancels a trade" % (stamp, __version__)]),
           pad(["%d step(s) on holdings · %d new buy(s) open · %d pick(s) waiting for price · %d page difference(s) to "
                "know about%s" % (n_hold, n_buy, waiting, len(d["issues"]),
                                  " · OLD SOURCE: " + ", ".join(d["stale"]) if d["stale"] else "")]),
           pad([]), pad(["A. DO TODAY - in this order"]),
           pad(["Step", "Symbol", "What to do", "Levels", "Why", "From"])]
    for i, s in enumerate(d["steps"], 1):
        out.append(pad([i, s["symbol"], s["do"], s["levels"], s["why"], s["source"]]))
    if not d["steps"]:
        out.append(pad(["", "", "Nothing to do today: every holding reads HOLD with its broker stop in place, and no "
                                "new buy is open."]))
    out += [pad([]), pad(["B. HOLDINGS (%d)" % len(d["holdings"])]),
            pad(["Symbol", "Name", "Shares", "Last", "P/L %", "Action", "Plan stop", "Broker stop", "Target", "Note"])]
    for h in d["holdings"]:
        bs = h.get("broker_stop")
        note = "" if h["action"] == "NO RULE" else (
            "no broker stop read" if bs is None else (
                "broker stop below the plan" if h.get("stop") and bs < h["stop"] * (1 - k["stop_tol"] / 100.0) else "stop in place"))
        out.append(pad([h["symbol"], h["name"], fmt(h.get("qty"), 0), fmt(h.get("last")), h.get("pl_pct", ""), h["action"],
                        fmt(h.get("stop")), fmt(bs), fmt(h.get("target")), note]))
    if not d["holdings"]:
        out.append(pad(["", "The Exit_Plan page lists no holding (or could not be read)."]))
    title = "C. NEW BUYS - plan of %s, cut %s (%s)" % (plan.get("date") or "-", plan.get("cut_at") or "-", plan.get("quality") or "-")
    out += [pad([]), pad([title]),
            pad(["Symbol", "Status", "Shares", "Entry zone", "Price now", "Stop", "First target", "Chance up, 21 bars",
                 "Shadow score", "Reason"])]
    for p in d["picks"]:
        e = (ev.get("names") or {}).get(p["symbol"]) or {}
        out.append(pad([p["symbol"], p["status"], fmt(p.get("qty"), 0),
                        "%s - %s" % (fmt(p.get("zone_low")), fmt(p.get("zone_high"))), fmt(p.get("price")), fmt(p.get("stop")),
                        fmt(p.get("tp1")), e.get("chance", ""), e.get("shadow", ""), p.get("reason", "")]))
    if not d["picks"]:
        out.append(pad(["", "No plan has been cut today yet." if not plan.get("is_today") else
                        "Today's plan has no pick: no name passed the execution gates."]))
    out += [pad([]), pad(["D. PIPELINE - next in line (%d of %d)" % (min(len(d["pipeline"]), int(k["pipeline"])), len(d["pipeline"]))]),
            pad(["Symbol", "Name", "Rank", "Score", "Waiting for"])]
    for p in d["pipeline"][:int(k["pipeline"])]:
        out.append(pad([p["symbol"], p["name"], fmt(p.get("rank"), 0), fmt(p.get("score"), 1), p.get("waiting", "")]))
    out += [pad([]), pad(["E. CONTEXT"]), pad(["Market state", "Bars to", "Above 50-day avg", "State", "After this state: names up", "Weeks seen"])]
    for s in ev.get("states") or []:
        out.append(pad([s["market"], s["bars_to"], s["above50"], s["state"], s["all_up"], s["weeks"]]))
    if not ev.get("states"):
        out.append(pad(["", "The Evidence_Board page has no market state yet."]))
    out.append(pad(["Learning gate (copied from S1_Gate)"]))
    for g in (ev.get("gate") or [["", "not on the Evidence Board yet"]])[:4]:
        out.append(pad(g))
    out.append(pad(["Page differences (Consistency_Check)", "Status", "Detail"]))
    for x in d["issues"]:
        out.append(pad([x["check"], x["status"], x["detail"]]))
    if not d["issues"]:
        out.append(pad(["", "none", "The pages agree on every checked figure."]))
    out.append(pad(["Source page", "Age (hours)", "Reading"]))
    for name in sorted(d["ages"]):
        a = d["ages"][name]
        out.append(pad([name, "" if a is None else "%.1f" % a,
                        "no time found" if a is None else ("OLD - refresh it before acting" if a > k["stale_h"] else "current")]))
    out += [pad([]), pad(["How to read: section A is the whole instruction; B to E show where each step comes from. A level on "
                          "this page is always the level on its source page. Trading decisions and every order are the "
                          "owner's; this page is information, not investment advice."])]
    return out


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


def run_once(dry_run: bool, book=None, now: Optional[_dt.datetime] = None) -> int:
    now = now or _dt.datetime.now(UTC)
    if not enabled():
        _out("TFB_TODAY_PLAN is off: nothing read, nothing written")
        return 0
    k = {"stale_h": _env_num("TFB_TAP_STALE_H", 3, 0.25, 72), "stop_tol": _env_num("TFB_TAP_STOP_TOL_PCT", 1, 0, 20),
         "pipeline": _env_num("TFB_TAP_PIPELINE", 5, 0, 30)}
    try:
        if book is None:
            from scripts.run_calendar_sync import _open_book  # noqa: PLC0415
            book = _open_book()
        from scripts.daily_plan import _read, write_grid  # noqa: PLC0415
        pages = {"Exit_Plan": _read(book, "Exit_Plan", "A1:P80"), "Daily_Plan": _read(book, "Daily_Plan", "A1:N6"),
                 "Evidence_Board": _read(book, "Evidence_Board", "A1:L120"),
                 "Consistency_Check": _read(book, "Consistency_Check", "A1:F60")}
        ledger = _read(book, "_Daily_Plan", "A1:AB2100")
    except Exception as e:  # noqa: BLE001 - a failed read leaves the previous page in place
        _out("ERROR: %s: %s; nothing written" % (type(e).__name__, text(e)[:300]))
        return 2
    local = now.astimezone(RIYADH)
    ages: Dict[str, Optional[float]] = {}
    for name, vals in pages.items():
        t = page_time(vals, now)
        ages[name] = None if t is None else max(0.0, (now - t).total_seconds() / 3600.0)
    d = build(read_holdings(pages["Exit_Plan"]), read_plan(ledger, local.date()), read_evidence(pages["Evidence_Board"]),
              read_consistency(pages["Consistency_Check"]), ages, k)
    for i, s in enumerate(d["steps"], 1):
        _out("STEP %d %-9s %s | %s" % (i, s["symbol"], s["do"], s["levels"]))
    if not d["steps"]:
        _out("no step today")
    _out("holdings %d · picks %d (%s) · pipeline %d · differences %d · old sources: %s" % (
        len(d["holdings"]), len(d["picks"]), ", ".join("%s %s" % (p["symbol"], p["status"]) for p in d["picks"]) or "-",
        len(d["pipeline"]), len(d["issues"]), ", ".join(d["stale"]) or "none"))
    rows = page_rows(d, local.strftime("%Y-%m-%d %H:%M"), local.strftime("%a %d %b %Y"), k)
    if dry_run:
        _out("dry run: %d page row(s) computed, nothing written" % len(rows))
        return 0
    write_grid(book, TAB, rows, WIDTH, 80)
    hidden = hide_tabs(book, hide_list(_env("TFB_TAP_HIDE_TABS", DEFAULT_HIDE)))
    _out("wrote the %s page (%d rows, %d step(s))%s" % (TAB, len(rows), len(d["steps"]),
                                                        "; hid " + ", ".join(hidden) if hidden else ""))
    return 0


def main(argv: Optional[Sequence[str]] = None, **kw) -> int:
    ap = argparse.ArgumentParser(description="TFB Today Action Plan (one page; copies the plan pages)")
    ap.add_argument("--dry-run", action="store_true")
    a = ap.parse_args(list(argv) if argv is not None else None)
    return run_once(a.dry_run, **kw)


if __name__ == "__main__":
    sys.exit(main())
