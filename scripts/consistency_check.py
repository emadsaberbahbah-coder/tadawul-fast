#!/usr/bin/env python3
"""
scripts/consistency_check.py - TFB cross-page consistency check
===============================================================
VERSION 1.0.0 (2026-11-11) - NEW SCRIPT

WHY (owner, Wed 11 Nov 2026 11:24 Riyadh): "improve and accurate output data
matching in all pages". Read on the live workbook at 11:30 the same day:
  - the main page showed one executable ticket, ZTO.US (earnings in 6 days),
    while the Daily Plan's picks were TEN.US, NMM.US and LADR.US and the plan
    refuses a name with earnings inside 7 days;
  - Top_10_Investments_v2 and Portfolio_Decision_v2 still showed the render
    of 10 Nov 16:03 (19 hours old) beside a main page rendered at 11:11;
  - DDI.US carried four different stops: broker 12.90, Portfolio_Decision
    14.44, the Exit Plan 14.24, the v2 page 15.01;
  - the ledger valued USD at 3.7570 and Portfolio_Decision at 3.7500;
  - Portfolio_Decision's total (82,353 SAR, the broker account) and the Daily
    Plan's NAV (92,114 SAR, with the sukuk held elsewhere) carried one name.
Nothing compared the pages, so each difference was found by the owner.

WHAT: one read of the decision pages and one page of results, Consistency_
Check: a row per check with MATCH / MISMATCH / STALE / INFO, the two figures
and the pages they come from. It changes no other page and decides nothing.
  C1 decision      main-page executable tickets vs the Daily Plan's open picks
  C2 earnings      a main-page ticket whose earnings fall inside the plan's window
  C3 v2 freshness  the v2 pages' render vs the main pages' render
  C4 prices        each holding's price on My_Portfolio, the ledger, Portfolio_Decision
  C5 quantities    Portfolio_Decision quantity vs the ledger's shares
  C6 FX            the ledger's USD rate vs the 3.75 peg
  C7 NAV           Portfolio_Decision total vs ledger total (named difference)
  C8 cash          Portfolio_Decision cash vs the main page's cash panel
  C9 stops         Portfolio_Decision stop vs the Exit Plan's stop per holding
  C10 actions      Portfolio_Decision action vs the Exit Plan's action

GATE: TFB_CONSISTENCY = off (default) | observe (print only) | on.
Rollback: repository variable TFB_CONSISTENCY=off.
"""
from __future__ import annotations

import argparse
import datetime as _dt
import os
import re
import sys
import time
from typing import Any, Dict, List, Optional, Sequence, Tuple

if __package__ in (None, ""):
    sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from scripts.daily_plan import _read, cell, fmt, num, read_calendar, riyadh_str, text, write_grid  # noqa: E402

__version__ = "1.0.0"

UTC = _dt.timezone.utc
RIYADH = _dt.timezone(_dt.timedelta(hours=3))
PAGE_TAB = "Consistency_Check"
PAGE_WIDTH = 6
PEG = 3.75
READ_ATTEMPTS = 3
READ_RETRY_S = 20
_SLEEP = time.sleep
_TS = re.compile(r"(\d{4}-\d{2}-\d{2})[ T](\d{2}:\d{2})")


def _out(msg: str) -> None:
    print("[consistency %s] %s" % (__version__, msg), flush=True)


def _env(name: str, default: str = "") -> str:
    return (os.environ.get(name) or default).strip()


def _env_num(name: str, default: float, lo: float, hi: float) -> float:
    try:
        v = float(_env(name, str(default)))
    except ValueError:
        return default
    return min(hi, max(lo, v)) if v == v else default


def mode() -> str:
    v = _env("TFB_CONSISTENCY", "off").lower()
    return v if v in ("off", "observe", "on") else "off"


def knobs() -> Dict[str, float]:
    return {"price_tol": _env_num("TFB_CONS_PRICE_TOL_PCT", 0.5, 0.0, 20.0),
            "stale_h": _env_num("TFB_CONS_STALE_H", 3.0, 0.1, 240.0),
            "fx_tol": _env_num("TFB_CONS_FX_TOL_PCT", 0.05, 0.0, 5.0),
            "stop_tol": _env_num("TFB_CONS_STOP_TOL_PCT", 1.0, 0.0, 50.0),
            "earn_days": _env_num("TFB_PLAN_EARN_DAYS", 7, 0, 30),
            "sar_tol": _env_num("TFB_CONS_SAR_TOL", 50.0, 0.0, 1e6)}


# ----------------------------------------------------------------------------- #
# Pure parsers
# ----------------------------------------------------------------------------- #
def find_table(values: Sequence[Sequence[Any]], need: Sequence[str]) -> Tuple[Optional[int], Dict[str, int]]:
    """(header row index, {header: column}) for the first row anywhere that carries every `need`."""
    for i, r in enumerate(values):
        cells = [text(c) for c in r]
        if all(n in cells for n in need):
            col: Dict[str, int] = {}
            for j, name in enumerate(cells):
                if name and name not in col:
                    col[name] = j
            return i, col
    return None, {}


def table_rows(values: Sequence[Sequence[Any]], need: Sequence[str], key: str) -> List[Dict[str, Any]]:
    """The rows under the header until the first row whose `key` cell is blank."""
    h, col = find_table(values, need)
    out: List[Dict[str, Any]] = []
    if h is None:
        return out
    for r in list(values)[h + 1:]:
        if not text(cell(r, col.get(key))):
            break
        out.append({name: cell(r, j) for name, j in col.items()})
    return out


def stamp_in(values: Sequence[Sequence[Any]], rows: int = 4, after: str = "") -> Optional[_dt.datetime]:
    """The first 'YYYY-MM-DD HH:MM' in the top rows (after the word `after` when given), as Riyadh time."""
    for r in list(values)[:rows]:
        for c in r:
            s = text(c)
            if after and after not in s:
                continue
            m = _TS.search(s[s.index(after):] if after else s)
            if m:
                return _dt.datetime.strptime(m.group(1) + " " + m.group(2), "%Y-%m-%d %H:%M").replace(tzinfo=RIYADH)
    return None


def panel(values: Sequence[Sequence[Any]], label: str) -> Any:
    for r in list(values)[:14]:
        cells = [text(c) for c in r]
        if label in cells and cells.index(label) + 1 < len(r):
            return r[cells.index(label) + 1]
    return None


def row(check: str, status: str, detail: str, a: str = "", b: str = "", pages: str = "") -> List[str]:
    return [check, status, detail, a, b, pages]


def _pct(a: float, b: float) -> float:
    return abs(a - b) / abs(b) * 100.0 if b else (0.0 if not a else 100.0)


# ----------------------------------------------------------------------------- #
# The checks (pure)
# ----------------------------------------------------------------------------- #
def run_checks(g: Dict[str, List[List[Any]]], today: _dt.date, k: Dict[str, float]) -> List[List[str]]:
    out: List[List[str]] = []
    t10, pd = g.get("Top_10_Investments") or [], g.get("Portfolio_Decision") or []
    seats = table_rows(t10, ("Rank", "Symbol", "Ticket SAR", "Shares"), "Symbol")
    tickets = {text(s["Symbol"]).upper(): s for s in seats if (num(s.get("Ticket SAR")) or 0) > 0
               and (num(s.get("Shares")) or 0) > 0}
    plan = [d for d in table_rows(g.get("_Daily_Plan") or [], ("Plan Date", "Kind", "Symbol", "Status"), "Plan Date")
            if text(d["Plan Date"]) == today.isoformat() and text(d["Kind"]) == "PICK"]
    open_picks = {text(d["Symbol"]).upper(): d for d in plan if text(d["Status"]).upper() not in (
        "INVALIDATED", "EXECUTED")}
    # C1
    if not plan:
        out.append(row("C1 decision", "INFO", "no Daily Plan for %s yet; main page tickets: %s" % (
            today.isoformat(), ", ".join(sorted(tickets)) or "none"), pages="Top_10_Investments / Daily_Plan"))
    else:
        only_main = sorted(set(tickets) - set(open_picks))
        only_plan = sorted(set(open_picks) - set(tickets))
        if not only_main and not only_plan:
            out.append(row("C1 decision", "MATCH", "the main page's tickets are the plan's open picks",
                           ", ".join(sorted(tickets)) or "none", ", ".join(sorted(open_picks)) or "none",
                           "Top_10_Investments / Daily_Plan"))
        else:
            seat_of = {text(s["Symbol"]).upper(): text(s.get("Funds From")) for s in seats}
            why = "; ".join(["%s: ticket on the main page, not an open pick" % s for s in only_main] +
                            ["%s: open pick, main page says '%s'" % (s, seat_of.get(s) or "not seated")
                             for s in only_plan])
            out.append(row("C1 decision", "MISMATCH", why, ", ".join(sorted(tickets)) or "none",
                           ", ".join(sorted(open_picks)) or "none", "Top_10_Investments / Daily_Plan"))
    # C2
    cal = read_calendar(g.get("Calendar_Events") or [], g.get("Dividend_Events") or [], today)
    soon = ["%s (earnings in %d day(s))" % (s, cal[s]["earn_days"]) for s in sorted(tickets)
            if s in cal and cal[s].get("earn_days") is not None and 0 <= cal[s]["earn_days"] <= k["earn_days"]]
    out.append(row("C2 earnings", "MISMATCH" if soon else "MATCH",
                   ("main-page ticket inside the plan's %d-day earnings window: %s" % (k["earn_days"], ", ".join(soon)))
                   if soon else "no main-page ticket inside the plan's %d-day earnings window" % k["earn_days"],
                   pages="Top_10_Investments / Calendar_Events"))
    # C3
    for main, v2, mark in (("Top_10_Investments", "Top_10_Investments_v2", "Last run"),
                           ("Portfolio_Decision", "Portfolio_Decision_v2", "")):
        a, b = stamp_in(g.get(main) or [], after="Last run"), stamp_in(g.get(v2) or [], after=mark)
        if a is None or b is None:
            out.append(row("C3 v2 freshness", "INFO", "%s: a render time is not readable" % v2, pages=main + " / " + v2))
            continue
        lag = (a - b).total_seconds() / 3600.0
        out.append(row("C3 v2 freshness", "STALE" if lag > k["stale_h"] else "MATCH",
                       "%s shows a render %.1f h older than %s" % (v2, lag, main) if lag > k["stale_h"]
                       else "%s is within %.1f h of %s" % (v2, k["stale_h"], main),
                       a.strftime("%Y-%m-%d %H:%M"), b.strftime("%Y-%m-%d %H:%M"), main + " / " + v2))
    # holdings
    lots: Dict[str, Dict[str, float]] = {}
    for d in table_rows(g.get("_Portfolio_CostBasis") or [], ("Symbol", "Status", "Shares"), "Symbol"):
        if text(d["Status"]).lower() != "active":
            continue
        o = lots.setdefault(text(d["Symbol"]).upper(), {"shares": 0.0, "mv": 0.0, "px": None, "fx": None, "ccy": ""})
        o["shares"] += num(d.get("Shares")) or 0.0
        o["mv"] += num(d.get("Market Value SAR")) or 0.0
        o["px"], o["fx"], o["ccy"] = num(d.get("Current Price")), num(d.get("FX→SAR")), text(d.get("Ccy")).upper()
    mp = {text(d["Symbol"]).upper(): num(d.get("Current Price")) for d in table_rows(
        g.get("My_Portfolio") or [], ("Symbol", "Current Price"), "Symbol")}
    pdr = {text(d["Symbol"]).upper(): d for d in table_rows(pd, ("Action", "Symbol", "Qty", "Price"), "Symbol")}
    # C4 / C5
    bad_px, bad_qty = [], []
    for s, lot in sorted(lots.items()):
        prices = {"ledger": lot["px"], "My_Portfolio": mp.get(s), "Portfolio_Decision": num((pdr.get(s) or {}).get("Price"))}
        have = {n: p for n, p in prices.items() if p}
        if len(have) >= 2 and _pct(max(have.values()), min(have.values())) > k["price_tol"]:
            bad_px.append("%s %s" % (s, ", ".join("%s %s" % (n, fmt(p)) for n, p in have.items())))
        q = num((pdr.get(s) or {}).get("Qty"))
        if q is not None and abs(q - lot["shares"]) > 1e-6:
            bad_qty.append("%s ledger %s vs Portfolio_Decision %s" % (s, fmt(lot["shares"], 0), fmt(q, 0)))
    pages3 = "My_Portfolio / _Portfolio_CostBasis / Portfolio_Decision"
    out.append(row("C4 prices", "MISMATCH" if bad_px else "MATCH", "; ".join(bad_px) or
                   "%d holding(s) priced within %.1f%% on every page" % (len(lots), k["price_tol"]), pages=pages3))
    out.append(row("C5 quantities", "MISMATCH" if bad_qty else "MATCH", "; ".join(bad_qty) or
                   "quantities agree for %d holding(s)" % len([s for s in lots if s in pdr]),
                   pages="_Portfolio_CostBasis / Portfolio_Decision"))
    # C6
    fx = sorted({round(lot["fx"], 4) for lot in lots.values() if lot["ccy"] == "USD" and lot["fx"]})
    off = [f for f in fx if _pct(f, PEG) > k["fx_tol"]]
    usd_mv = sum(lot["mv"] for lot in lots.values() if lot["ccy"] == "USD")
    out.append(row("C6 FX", "MISMATCH" if off else "MATCH",
                   "the ledger values USD at %s, not the %.2f peg: USD holdings read %s SAR higher" % (
                       ", ".join("%.4f" % f for f in off), PEG, fmt(usd_mv * (1 - PEG / off[0]), 0)) if off
                   else "the ledger values USD at the %.2f peg" % PEG,
                   ", ".join("%.4f" % f for f in fx), "%.4f" % PEG, "_Portfolio_CostBasis / Portfolio_Decision"))
    # C7 / C8
    kp = table_rows(pd, ("Portfolio (SAR)", "Holdings (SAR)", "Cash (SAR)"), "Holdings (SAR)")
    pd_total = num(kp[0].get("Portfolio (SAR)")) if kp else None
    pd_cash = num(kp[0].get("Cash (SAR)")) if kp else None
    pd_cash = pd_cash if pd_cash is not None else num(panel(pd, "PF: Cash Available SAR"))
    t10_cash = num(panel(t10, "Cash Available (SAR)"))
    if pd_total is not None and pd_cash is not None and lots:
        ledger_total = sum(lot["mv"] for lot in lots.values()) + pd_cash
        outside = [s for s in lots if s not in pdr]
        gap = ledger_total - pd_total
        named = sum(lots[s]["mv"] for s in outside)
        ok = abs(gap - named) <= max(k["sar_tol"], abs(ledger_total) * 0.005)
        out.append(row("C7 NAV", "INFO" if ok else "MISMATCH",
                       "two totals: Portfolio_Decision counts the broker account; the ledger total adds %s (%s SAR)" % (
                           ", ".join(sorted(outside)) or "nothing", fmt(named, 0)) if ok else
                       "Portfolio_Decision and the ledger differ by %s SAR; holdings outside Portfolio_Decision "
                       "explain %s SAR" % (fmt(gap, 0), fmt(named, 0)),
                       fmt(pd_total, 0), fmt(ledger_total, 0), "Portfolio_Decision / _Portfolio_CostBasis"))
    if pd_cash is not None and t10_cash is not None:
        same = abs(pd_cash - t10_cash) <= k["sar_tol"]
        out.append(row("C8 cash", "MATCH" if same else "MISMATCH", "cash agrees" if same else "cash differs by %s SAR" %
                       fmt(abs(pd_cash - t10_cash), 0), fmt(pd_cash, 0), fmt(t10_cash, 0),
                       "Portfolio_Decision / Top_10_Investments"))
    # C9 / C10
    ex = {text(d["Symbol"]).upper(): d for d in table_rows(g.get("_Exit_Plan") or [], ("Date", "Symbol", "Stop", "Action"),
                                                           "Date") if text(d["Date"]) == today.isoformat()}
    if not ex:
        out.append(row("C9 stops", "INFO", "no Exit Plan rows for %s yet" % today.isoformat(),
                       pages="Portfolio_Decision / Exit_Plan"))
    else:
        bad_stop, bad_act = [], []
        for s, d in sorted(ex.items()):
            p = pdr.get(s) or {}
            ps, px, pxs = num(p.get("Stop SAR")), num(p.get("Price")), num(p.get("Price SAR"))
            es = num(d.get("Stop"))
            if ps and px and pxs and es:
                pstop = ps * px / pxs                       # back to the listing currency at the page's own rate
                if _pct(pstop, es) > k["stop_tol"]:
                    bad_stop.append("%s Portfolio_Decision %s vs Exit Plan %s" % (s, fmt(pstop), fmt(es)))
            act, eact = text(p.get("Action")).upper(), text(d.get("Action")).upper()
            sells = ("TRIM", "EXIT", "SELL")
            if act and ((act in sells) != (eact in ("SELL", "TAKE PROFIT"))):
                bad_act.append("%s Portfolio_Decision %s vs Exit Plan %s" % (s, act, eact))
        out.append(row("C9 stops", "MISMATCH" if bad_stop else "MATCH", "; ".join(bad_stop) or
                       "stops agree within %.1f%%" % k["stop_tol"], pages="Portfolio_Decision / Exit_Plan"))
        out.append(row("C10 actions", "MISMATCH" if bad_act else "MATCH", "; ".join(bad_act) or
                       "sell-side actions agree", pages="Portfolio_Decision / Exit_Plan"))
    return out


def page_rows(results: List[List[str]], stamp: str) -> List[List[str]]:
    n = {s: sum(1 for r in results if r[1] == s) for s in ("MISMATCH", "STALE", "MATCH", "INFO")}
    rows = [["CONSISTENCY CHECK - the same figure on every page"],
            ["As of %s Riyadh · %d mismatch(es) · %d stale · %d match · %d note(s) · consistency_check %s" % (
                stamp, n["MISMATCH"], n["STALE"], n["MATCH"], n["INFO"], __version__)],
            [],
            ["Check", "Status", "Detail", "First page", "Second page", "Pages compared"]]
    order = {"MISMATCH": 0, "STALE": 1, "INFO": 2, "MATCH": 3}
    rows += sorted(results, key=lambda r: (order.get(r[1], 9), r[0]))
    return [(list(r) + [""] * PAGE_WIDTH)[:PAGE_WIDTH] for r in rows]


# ----------------------------------------------------------------------------- #
# IO
# ----------------------------------------------------------------------------- #
RANGES = {"Top_10_Investments": "A1:AE60", "Portfolio_Decision": "A1:X60", "Top_10_Investments_v2": "A1:H4",
          "Portfolio_Decision_v2": "A1:H4", "_Portfolio_CostBasis": "A1:Z600", "My_Portfolio": "A1:K60",
          "_Daily_Plan": "A1:AB2001", "_Exit_Plan": "A1:Q3001", "Calendar_Events": "A1:J4000",
          "Dividend_Events": "A1:J4000"}


def run(dry_run: bool, book=None, now: Optional[_dt.datetime] = None) -> int:
    write = mode() == "on" and not dry_run
    now = now or _dt.datetime.now(UTC)
    today = now.astimezone(RIYADH).date()
    if book is None:
        from scripts.run_calendar_sync import _open_book  # noqa: PLC0415
        try:
            book = _open_book()
        except Exception as e:  # noqa: BLE001
            _out("ERROR: cannot open workbook: %s" % type(e).__name__)
            return 2
    grids: Dict[str, List[List[Any]]] = {}
    for attempt in range(1, READ_ATTEMPTS + 1):
        try:
            grids = {tab: _read(book, tab, rng) for tab, rng in RANGES.items()}
            break
        except Exception as e:  # noqa: BLE001
            if attempt == READ_ATTEMPTS:
                _out("ERROR: cannot read the workbook: %s; nothing written" % type(e).__name__)
                return 2
            _out("read failed (%s) - attempt %d of %d" % (type(e).__name__, attempt, READ_ATTEMPTS))
            _SLEEP(READ_RETRY_S)
    results = run_checks(grids, today, knobs())
    for r in results:
        _out("%-16s %-8s %s" % (r[0], r[1], r[2]))
    if not write:
        _out("%s - nothing written" % ("dry run" if dry_run else "observe"))
        return 0
    try:
        write_grid(book, PAGE_TAB, page_rows(results, riyadh_str(now)), PAGE_WIDTH, 30)
    except Exception as e:  # noqa: BLE001
        _out("ERROR: publication unconfirmed: %s" % type(e).__name__)
        return 3
    _out("wrote the %s page (%d checks)" % (PAGE_TAB, len(results)))
    return 0


def main(argv: Optional[Sequence[str]] = None) -> int:
    ap = argparse.ArgumentParser(description="Compare the same figures across the decision pages.")
    ap.add_argument("--dry-run", action="store_true", help="compute and print, write nothing")
    args = ap.parse_args(list(argv) if argv is not None else None)
    if mode() == "off":
        _out("TFB_CONSISTENCY=off - nothing to do")
        return 0
    return run(args.dry_run)


if __name__ == "__main__":
    sys.exit(main())
