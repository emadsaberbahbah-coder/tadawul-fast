#!/usr/bin/env python3
"""
scripts/daily_plan.py - TFB Daily Plan
======================================
VERSION 1.0.0 (2026-11-10) - NEW SCRIPT

WHY (owner review, Tue 10 Nov 2026 13:05 Riyadh, verbatim: "my concern is
about recommdadtion stability and how is recommdadtions is change over hour
... at least should be between 2 or 3 recommdadtion to be invested as of each
day and other can be in the pipeline , also change of recommdadtion one hour
there is recommdadtions and after next hour it's removed ... also V2 of top
investment should be matching and alinged with the mian page").
Evidence (project doc TFB_Recommendation_Stability_Review_and_Daily_Plan_
Design_2026-11-10):
  - v_selection_log, 6-9 Nov: 32 board versions in 77 hours; 17 names over 10
    seats; 4 of 319 seat-slots carried a ticket; EXECUTABLE in 3 versions.
  - Main page, 10 Nov: 16 names qualified on merit, 0 seated as a plan; 9
    deferred for an analyst target held 1-5 h, 7 awaiting a 3-day confirmation.
  - The funded cockpit render (pdIbkrEvidenceFollowUp) timed out at the Apps
    Script 6-minute limit on all 7 logged attempts of 9-10 Nov.
  - v_board_episodes since 1 Oct: 61% of seats last one day or less.
A ticket existed only while six conditions held at the same moment, each one
re-tested on every render. Nothing recorded "this is today's plan".

WHAT: one decision per day, frozen.
  - CUT: once per Riyadh day, from the first FINISHED render of the main page
    (Top_10_Investments status 'ok', made today) whose source readiness reads
    VERIFIED; from TFB_PLAN_UNVERIFIED_AFTER_H o'clock on, a finished render
    that is not verified is accepted and every pick reads CHECK.
  - PICKS: the engine's own merit list ('ALL QUALIFIED - INVEST opportunity
    set', sorted by the engine's score) is walked in order; the first
    TFB_PLAN_PICKS names that pass the execution gates are the day's picks:
    not already held, not sold in the last TFB_PLAN_COOLDOWN_DAYS days, a
    fundable currency (TFB_PLAN_FUNDABLE_CCY), a price on its source page, no
    earnings inside TFB_PLAN_EARN_DAYS days, no ex-dividend of
    TFB_PLAN_EXDIV_PCT % or more inside TFB_PLAN_EXDIV_DAYS days, the sector
    cap, and no engine deferral other than the two soft ones (a held analyst
    target up to TFB_PLAN_HELD_TARGET_MAX_H hours; awaiting confirmation).
    Yesterday's unexecuted picks keep their place while they still qualify
    inside the first TFB_PLAN_KEEP_RANK ranks.
  - SIZING: deployable cash (panel cash less the cash floor) split equally
    over the picks, each capped at the smaller of the per-seat weight cap and
    the size at which the stop loses TFB_PLAN_RISK_PCT_NAV % of NAV; a pick
    below the minimum ticket goes to the pipeline.
  - FREEZE: a later run on the same day never changes a pick, its entry,
    quantity, stop or targets. It only refreshes the status: OPEN / WAIT /
    CHECK / EXECUTED / INVALIDATED (with the reason).
  - PIPELINE: every other qualified name with what it is waiting for.
Reads only: Top_10_Investments, Global_Markets, Market_Leaders,
Calendar_Events, Dividend_Events, _Portfolio_CostBasis. Writes only its own
two tabs: _Daily_Plan (the ledger) and Daily_Plan (the page). Never calls the
backend, never calls a broker, places no order.

1.0.1 (2026-11-10): the earnings and ex-dividend gates count days from the
DATE on the calendar row to the Riyadh plan date, not from the stored 'Days To'
column, which is as of the calendar sync (10 Nov: ZTO.US read 8 days from the
9 Nov sync while 17 Nov was 7 days away, so the 7-day gate passed it). A pick
without an engine 12-month ROI carries the label 'no 12-month target'.

1.1.0 (2026-11-10): THE EVIDENCE RULE (TFB_PLAN_EVIDENCE = off default |
observe | on). Measured on matured INVESTABLE records outside Tadawul, 1W-1M
horizons, one row per symbol and week (v_outcome_excess joined to
v_signal_history, entries 31 Aug - 10 Nov 2026): a name whose Risk Score was
under 30 AND whose analyst target stood 15% or more above its price finished
up in 72.2% of 72 symbol-weeks (34 names; beat its benchmark in 83.3%); the
other INVESTABLE names finished up in 43.6% of 110. By half: 77.2% and 66.4%
of records. The 11 records entered in November read 36.4%. This is an in-sample
reading over ten weeks, so the rule ships as a LABEL first:
  observe  every qualified name carries 'evidence rule PASS / FAIL / n/a' with
           its two numbers; picks are chosen exactly as before.
  on       a name that is not PASS is not picked (pipeline: 'evidence rule not
           met'); every other gate is unchanged.
Tadawul names read n/a: the rule was not measured as a pass there. Two more
columns are read from the source pages (Risk Score, Target Price); nothing new
is written outside the two tabs. Knobs: TFB_PLAN_EV_RISK_MAX (30),
TFB_PLAN_EV_UPSIDE_MIN (15).

1.2.0 (2026-11-11): HOURLY STATUS ON LIVE PRICES (owner, 12:52 Riyadh: the
outcome pages refresh at least every hour, automatically, with the new data).
The workflow runs every hour. The picks stay frozen; their status (OPEN /
WAIT / CHECK / INVALIDATED) is checked against the last traded price from the
Yahoo chart (TFB_PLAN_LIVE_PRICE = on) instead of the source pages' price,
which moves only when a sync lands (three times a day); the reason carries
'(live HH:MM Riyadh)'. A name with no live price keeps the page price. The cut
itself still uses the source pages, as before.

GATE: TFB_DAILY_PLAN = off (default) | observe (compute and print, write
nothing) | on. Rollback: set the repository variable TFB_DAILY_PLAN=off.
"""
from __future__ import annotations

import argparse
import datetime as _dt
import os
import re
import sys
import time
from typing import Any, Dict, List, Optional, Sequence, Tuple

__version__ = "1.2.0"

UTC = _dt.timezone.utc
RIYADH = _dt.timezone(_dt.timedelta(hours=3))
PAGE = "Top_10_Investments"
SOURCE_PAGES = ("Global_Markets", "Market_Leaders")
LEDGER_TAB = "_Daily_Plan"
PAGE_TAB = "Daily_Plan"
LEDGER_HEADERS = [
    "Plan Date", "Cut At (Riyadh)", "Source Render", "Cut Quality", "Kind", "Rank", "Symbol", "Name",
    "Market", "Sector", "Ccy", "Entry", "Zone Low", "Zone High", "Qty", "Alloc SAR", "Stop", "TP1", "TP2",
    "Risk SAR", "Score", "ROI 12M %", "Labels", "Waiting For", "Status", "Status Reason", "Last Price",
    "Status At (Riyadh)",
]
LEDGER_KEEP_DAYS = 30
MAX_LEDGER_ROWS = 2000
PAGE_WIDTH = 14
FX_TO_SAR = {"USD": 3.75, "SAR": 1.0}
QUAL_TITLE = "ALL QUALIFIED"
READ_ATTEMPTS = 3      # the workbook answers slowly under load; a transient read error is retried
READ_RETRY_S = 20
OK_OUTPUTS = ("EXECUTABLE", "QUALIFIED_UNFUNDED", "HELD", "WITHHELD", "NO_TRADE", "EMPTY")


_SLEEP = time.sleep


def _out(msg: str) -> None:
    print(msg, flush=True)


def _env(name: str, default: str = "") -> str:
    return str(os.environ.get(name, default) or default).strip()


def _env_num(name: str, default: float, lo: float, hi: float) -> float:
    try:
        v = float(_env(name, str(default)))
    except ValueError:
        return default
    if v != v or v < lo or v > hi:
        return default
    return v


def mode() -> str:
    v = _env("TFB_DAILY_PLAN", "off").lower()
    return v if v in ("off", "observe", "on") else "off"


def ev_mode() -> str:
    v = _env("TFB_PLAN_EVIDENCE", "off").lower()
    return v if v in ("off", "observe", "on") else "off"


def knobs() -> Dict[str, Any]:
    ccy = [c.strip().upper() for c in _env("TFB_PLAN_FUNDABLE_CCY", "USD,SAR").split(",") if c.strip()]
    return {
        "picks": int(_env_num("TFB_PLAN_PICKS", 3, 1, 5)),
        "ccy": [c for c in ccy if c in FX_TO_SAR] or ["USD", "SAR"],
        "earn_days": int(_env_num("TFB_PLAN_EARN_DAYS", 7, 0, 30)),
        "exdiv_days": int(_env_num("TFB_PLAN_EXDIV_DAYS", 3, 0, 30)),
        "exdiv_pct": _env_num("TFB_PLAN_EXDIV_PCT", 2.0, 0.0, 100.0),
        "held_h": _env_num("TFB_PLAN_HELD_TARGET_MAX_H", 48, 0, 720),
        "stop_pct": _env_num("TFB_PLAN_STOP_PCT", 8, 1, 30),
        "risk_pct": _env_num("TFB_PLAN_RISK_PCT_NAV", 1.0, 0.1, 5),
        "weight_pct": _env_num("TFB_PLAN_MAX_WEIGHT_PCT", 15, 1, 50),
        "floor_pct": _env_num("TFB_PLAN_CASH_FLOOR_PCT", 10, 0, 90),
        "min_ticket": _env_num("TFB_PLAN_MIN_TICKET_SAR", 2000, 0, 1e7),
        "keep_rank": int(_env_num("TFB_PLAN_KEEP_RANK", 8, 0, 50)),
        "cooldown": int(_env_num("TFB_PLAN_COOLDOWN_DAYS", 10, 0, 90)),
        "unverified_h": _env_num("TFB_PLAN_UNVERIFIED_AFTER_H", 15, 0, 24),
        "ev_mode": ev_mode(),
        "live": _env("TFB_PLAN_LIVE_PRICE", "off").lower() in ("on", "1", "true", "yes"),
        "ev_risk_max": _env_num("TFB_PLAN_EV_RISK_MAX", 30, 1, 100),
        "ev_upside_min": _env_num("TFB_PLAN_EV_UPSIDE_MIN", 15, 0, 200),
    }


# ----------------------------------------------------------------------------- #
# Pure parsers
# ----------------------------------------------------------------------------- #
def num(v: Any) -> Optional[float]:
    """A number from a sheet cell ('33,125 SAR', '31.2%', '▲ 5.62%', 12.5); None when it carries none."""
    if isinstance(v, bool) or v is None:
        return None
    if isinstance(v, (int, float)):
        return float(v) if v == v else None
    s = str(v).replace(",", "").replace("−", "-").strip()
    m = re.search(r"-?\d+(?:\.\d+)?", s)
    if not m:
        return None
    x = float(m.group(0))
    if s.startswith("(") and s.endswith(")"):
        x = -x
    return x


def text(v: Any) -> str:
    return str(v if v is not None else "").strip()


def cell(row: Sequence[Any], i: Optional[int]) -> Any:
    return row[i] if i is not None and 0 <= i < len(row) else ""


def riyadh_str(d: _dt.datetime) -> str:
    return d.astimezone(RIYADH).strftime("%Y-%m-%d %H:%M")


def parse_date(v: Any) -> Optional[_dt.date]:
    m = re.search(r"(\d{4})-(\d{2})-(\d{2})", text(v))
    if m:
        try:
            return _dt.date(int(m.group(1)), int(m.group(2)), int(m.group(3)))
        except ValueError:
            return None
    m = re.search(r"(\d{1,2})/(\d{1,2})/(\d{4})", text(v))
    if m:
        try:
            return _dt.date(int(m.group(3)), int(m.group(1)), int(m.group(2)))
        except ValueError:
            return None
    return None


def board_state(values: Sequence[Sequence[Any]], now: _dt.datetime) -> Dict[str, Any]:
    """The main page's status line: is there a FINISHED render made today, and is its source verified?"""
    line = ""
    for r in list(values)[:6]:
        for c in r:
            if text(c).startswith("Last run "):
                line = text(c)
                break
        if line:
            break
    out = {"line": line, "finished": False, "today": False, "verified": False, "output": "", "run_at": "",
           "reason": ""}
    m = re.match(r"Last run (\d{4}-\d{2}-\d{2}) (\d{2}:\d{2})(?::\d{2})?", line)
    if not m:
        out["reason"] = "no status line"
        return out
    out["run_at"] = m.group(1) + " " + m.group(2)
    st = re.search(r"\|\s*status:\s*([A-Za-z_]+)", line)
    op = re.search(r"\|\s*output:\s*([A-Z_]+)", line)
    out["output"] = op.group(1) if op else ""
    out["finished"] = bool(st and st.group(1).lower() == "ok" and out["output"] in OK_OUTPUTS)
    out["today"] = m.group(1) == now.astimezone(RIYADH).strftime("%Y-%m-%d")
    out["verified"] = "source readiness VERIFIED" in line and out["output"] != "WITHHELD"
    if not out["finished"]:
        out["reason"] = "the main page has no finished render (status %s)" % (st.group(1) if st else "?")
    elif not out["today"]:
        out["reason"] = "the main page's last finished render is from " + m.group(1)
    return out


def panel_value(values: Sequence[Sequence[Any]], label: str) -> Any:
    """The control-panel value in the cell right of `label` (first 16 rows)."""
    want = label.lower()
    for r in list(values)[:16]:
        for i, c in enumerate(r):
            if text(c).lower() == want and i + 1 < len(r):
                return r[i + 1]
    return ""


def read_qualified(values: Sequence[Sequence[Any]]) -> List[Dict[str, Any]]:
    """Rows of the main page's 'ALL QUALIFIED' table, in the engine's order."""
    rows = list(values)
    start = None
    for i, r in enumerate(rows):
        if any(text(c).startswith(QUAL_TITLE) for c in r):
            start = i
            break
    if start is None:
        return []
    hdr = None
    for i in range(start + 1, min(start + 4, len(rows))):
        cells = [text(c) for c in rows[i]]
        if "Symbol" in cells and "Rank" in cells:
            hdr = i
            break
    if hdr is None:
        return []
    cells = [text(c) for c in rows[hdr]]
    col = {name: cells.index(name) for name in cells if name}
    out: List[Dict[str, Any]] = []
    for r in rows[hdr + 1:]:
        sym = text(cell(r, col.get("Symbol"))).upper()
        rank = num(cell(r, col.get("Rank")))
        if not sym or rank is None or " " in sym:
            break
        out.append({
            "rank": int(rank), "symbol": sym, "name": text(cell(r, col.get("Name"))),
            "market": text(cell(r, col.get("Market"))), "sector": text(cell(r, col.get("Sector"))),
            "roi12": num(cell(r, col.get("Engine ROI % (12M)"))), "risk": text(cell(r, col.get("Risk"))),
            "score": num(cell(r, col.get("Score"))), "rel": num(cell(r, col.get("Rel"))),
            "selected": text(cell(r, col.get("Selected"))).lower() == "yes",
            "why": text(cell(r, col.get("Why Not Selected"))),
        })
    return out


def table_index(values: Sequence[Sequence[Any]], need: Sequence[str]) -> Tuple[Optional[int], Dict[str, int]]:
    """(header row index, {header: column}) for the first of the top 8 rows that carries every `need`."""
    for i, r in enumerate(list(values)[:8]):
        cells = [text(c) for c in r]
        if all(n in cells for n in need):
            return i, {name: cells.index(name) for name in cells if name}
    return None, {}


def read_sources(grids: Sequence[Sequence[Sequence[Any]]]) -> Dict[str, Dict[str, Any]]:
    """{SYMBOL: {price, ccy, sector, warnings, risk, target}} from the source pages; the first page that lists a
    symbol wins."""
    out: Dict[str, Dict[str, Any]] = {}
    for g in grids:
        h, col = table_index(g, ("Symbol", "Current Price", "Currency"))
        if h is None:
            continue
        for r in list(g)[h + 1:]:
            sym = text(cell(r, col.get("Symbol"))).upper()
            if not sym or sym in out:
                continue
            out[sym] = {"price": num(cell(r, col.get("Current Price"))),
                        "ccy": text(cell(r, col.get("Currency"))).upper(),
                        "sector": text(cell(r, col.get("Sector"))),
                        "warnings": text(cell(r, col.get("Warnings"))),
                        "risk": num(cell(r, col.get("Risk Score"))),
                        "target": num(cell(r, col.get("Target Price")))}
    return out


def evidence_rule(symbol: str, src: Dict[str, Any], risk_max: float, upside_min: float) -> Tuple[str, str]:
    """1.1.0 PURE: (PASS | FAIL | NA, label). PASS = Risk Score under risk_max AND the analyst target
    upside_min % or more above the price. Tadawul and a name without both numbers read NA."""
    if symbol.upper().endswith(".SR"):
        return "NA", "evidence rule n/a (not measured for Tadawul)"
    price, risk, target = src.get("price"), src.get("risk"), src.get("target")
    if risk is None or target is None or target <= 0 or not price or price <= 0:
        missing = "risk score" if risk is None else "analyst target"
        return "NA", "evidence rule n/a (no %s)" % missing
    upside = round((target / price - 1.0) * 100.0, 4)      # 4 places, as v_signal_history.target_upside_pp
    facts = "risk %.1f, analyst upside %+.0f%%" % (risk, upside)
    if risk < risk_max and upside >= upside_min:
        return "PASS", "evidence rule PASS (%s)" % facts
    need = []
    if risk >= risk_max:
        need.append("risk under %g" % risk_max)
    if upside < upside_min:
        need.append("upside %+g%% or more" % upside_min)
    return "FAIL", "evidence rule FAIL (%s; needs %s)" % (facts, " and ".join(need))


def _days_until(date_cell: Any, days_cell: Any, today: Optional[_dt.date]) -> Optional[float]:
    """1.0.1: days from `today` to the date in `date_cell` when it parses; else the stored day count."""
    d = parse_date(date_cell)
    if d is not None and today is not None:
        return float((d - today).days)
    return num(days_cell)


def read_calendar(cal: Sequence[Sequence[Any]], div: Sequence[Sequence[Any]],
                  today: Optional[_dt.date] = None) -> Dict[str, Dict[str, Any]]:
    """{SYMBOL: {earn_days, exdiv_days, div_amount}} from Calendar_Events and Dividend_Events.
    With `today`, days are counted from the event DATE (the stored count is as of the calendar sync)."""
    out: Dict[str, Dict[str, Any]] = {}
    h, col = table_index(cal, ("Symbol", "Days To Earnings"))
    if h is not None:
        for r in list(cal)[h + 1:]:
            sym = text(cell(r, col.get("Symbol"))).upper()
            if sym:
                out[sym] = {"earn_days": _days_until(cell(r, col.get("Next Earnings Date")),
                                                     cell(r, col.get("Days To Earnings")), today),
                            "exdiv_days": _days_until(cell(r, col.get("Next Ex-Div Date")),
                                                      cell(r, col.get("Days To ExDiv")), today),
                            "div_amount": None}
    h, col = table_index(div, ("Symbol", "Days To ExDiv", "Amount"))
    if h is not None:
        for r in list(div)[h + 1:]:
            sym = text(cell(r, col.get("Symbol"))).upper()
            if sym:
                e = out.setdefault(sym, {"earn_days": None, "exdiv_days": None, "div_amount": None})
                e["exdiv_days"] = _days_until(cell(r, col.get("Ex-Div Date")), cell(r, col.get("Days To ExDiv")), today)
                e["div_amount"] = num(cell(r, col.get("Amount")))
    return out


def read_ledger_lots(values: Sequence[Sequence[Any]], today: _dt.date, cooldown: int) -> Dict[str, Any]:
    """Active lots (symbol -> buy date, sector unknown), their SAR value, and names sold inside the cool-down."""
    h, col = table_index(values, ("Symbol", "Status", "Buy Date"))
    held: Dict[str, _dt.date] = {}
    recent: Dict[str, int] = {}
    mv = 0.0
    if h is None:
        return {"held": held, "recent": recent, "mv_sar": 0.0, "found": False}
    for r in list(values)[h + 1:]:
        sym = text(cell(r, col.get("Symbol"))).upper()
        if not sym:
            continue
        status = text(cell(r, col.get("Status"))).lower()
        if status == "active":
            bd = parse_date(cell(r, col.get("Buy Date"))) or _dt.date(1970, 1, 1)
            held[sym] = max(bd, held.get(sym, bd))
            mv += num(cell(r, col.get("Market Value SAR"))) or 0.0
        else:
            sd = parse_date(cell(r, col.get("Sell Date")))
            if sd is not None and 0 <= (today - sd).days <= cooldown:
                recent[sym] = min((today - sd).days, recent.get(sym, 10 ** 6))
    return {"held": held, "recent": recent, "mv_sar": mv, "found": True}


def held_target_hours(s: str) -> Optional[float]:
    m = re.search(r"analyst_lkg:(\d+(?:\.\d+)?)h", s or "")
    return float(m.group(1)) if m else None


def engine_deferral(why: str, held_max_h: float) -> Tuple[str, str]:
    """('', label) when the engine's reason does not block a pick, else (reason, '')."""
    w = text(why)
    lw = w.lower()
    # The cockpit's own non-reasons (16_Decision_Top10 dt10QualToRow_): a selected row reads '\u2014 selected';
    # a qualified row with no seat reads 'not selected \u2014 ...' or 'ranked below the Max Selected cut'.
    if (not w or lw.lstrip("\u2014- ").startswith("selected") or lw.startswith("not selected")
            or "max selected" in lw or lw.startswith("ranked") or "diversification cap" in lw):
        return "", ""
    if lw.startswith("held research seat"):
        m = re.search(r"\u2014\s*([A-Z][A-Z \-()]+?)\s*(?:;|$)", w)
        return "", "main page seat: %s" % (m.group(1).strip().lower() if m else "held")
    if lw.startswith("held target"):
        h = held_target_hours(w)
        if h is not None and h <= held_max_h:
            return "", "analyst target held %dh" % h
        return "analyst target held %s (limit %dh)" % ("%dh" % h if h is not None else "?", held_max_h), ""
    if lw.startswith("stability: awaiting confirmation"):
        m = re.search(r"\((\d+)/(\d+) days\)", w)
        return "", "confirmation %s/%s" % (m.group(1), m.group(2)) if m else "awaiting confirmation"
    return w[:120], ""


# ----------------------------------------------------------------------------- #
# The decision (pure)
# ----------------------------------------------------------------------------- #
def decide(qualified: List[Dict[str, Any]], sources: Dict[str, Dict[str, Any]], cal: Dict[str, Dict[str, Any]],
           lots: Dict[str, Any], cash_sar: float, max_per_sector: int, k: Dict[str, Any],
           prior_picks: Sequence[str] = ()) -> Dict[str, Any]:
    """Picks and pipeline from the engine's merit list. Never raises on a malformed row: it goes to the pipeline."""
    nav = max(0.0, lots.get("mv_sar", 0.0)) + max(0.0, cash_sar)
    cands: List[Dict[str, Any]] = []
    pipeline: List[Dict[str, Any]] = []
    for q in qualified:
        src = sources.get(q["symbol"]) or {}
        c = dict(q, price=src.get("price"), ccy=src.get("ccy") or "", labels=[], waiting="")
        if not c["sector"]:
            c["sector"] = src.get("sector") or ""
        ev = cal.get(q["symbol"]) or {}
        reason, label = ("", "") if q.get("selected") else engine_deferral(q.get("why", ""), k["held_h"])
        if label:
            c["labels"].append(label)
        ed, xd, amt = ev.get("earn_days"), ev.get("exdiv_days"), ev.get("div_amount")
        if q["symbol"] in lots.get("held", {}):
            reason = "already held"
        elif q["symbol"] in lots.get("recent", {}):
            reason = "sold %d day(s) ago (cool-down %d days)" % (lots["recent"][q["symbol"]], k["cooldown"])
        elif not c["price"] or c["price"] <= 0:
            reason = "no price on its source page"
        elif c["ccy"] not in k["ccy"]:
            reason = "currency %s is not fundable (%s)" % (c["ccy"] or "?", "/".join(k["ccy"]))
        elif ed is not None and 0 <= ed <= k["earn_days"]:
            reason = "earnings in %d day(s)" % ed
        elif (xd is not None and amt is not None and 0 <= xd <= k["exdiv_days"]
              and amt / c["price"] * 100.0 >= k["exdiv_pct"]):
            reason = "ex-dividend %.1f%% in %d day(s)" % (amt / c["price"] * 100.0, xd)
        if ed is None:
            c["labels"].append("earnings date not on the calendar")
        if k.get("ev_mode", "off") != "off":
            c["ev"], ev_label = evidence_rule(q["symbol"], src, k["ev_risk_max"], k["ev_upside_min"])
            c["labels"].append(ev_label)
            if k["ev_mode"] == "on" and c["ev"] != "PASS" and not reason:
                reason = "evidence rule not met"
        if reason:
            c["waiting"] = reason
            pipeline.append(c)
        else:
            cands.append(c)
    keep = [s for s in prior_picks if s]
    cands.sort(key=lambda c: (0 if (c["symbol"] in keep and c["rank"] <= k["keep_rank"]) else 1, c["rank"]))
    picks: List[Dict[str, Any]] = []
    per_sector: Dict[str, int] = {}
    for c in cands:
        sec = c["sector"] or "?"
        if len(picks) >= k["picks"]:
            c["waiting"] = "next in line (rank %d)" % c["rank"]
            pipeline.append(c)
        elif per_sector.get(sec, 0) >= max(1, max_per_sector):
            c["waiting"] = "sector cap (%s already has %d)" % (sec, per_sector[sec])
            pipeline.append(c)
        else:
            if c["symbol"] in keep and c["rank"] <= k["keep_rank"]:
                c["labels"].append("kept from the previous plan")
            per_sector[sec] = per_sector.get(sec, 0) + 1
            picks.append(c)
    deployable = max(0.0, cash_sar - k["floor_pct"] / 100.0 * nav)
    cap = min(k["weight_pct"] / 100.0 * nav, k["risk_pct"] / 100.0 * nav / (k["stop_pct"] / 100.0))
    def whole(p: Dict[str, Any], a: float) -> float:
        """What `a` SAR buys of p in whole shares, in SAR."""
        px = p["price"] * FX_TO_SAR[p["ccy"]]
        return (a // px) * px

    while picks:
        alloc = min(cap, deployable / len(picks))
        # the ticket that is published is whole shares, so the minimum is tested on that amount
        under = [p for p in picks if whole(p, alloc) <= 0 or whole(p, alloc) < k["min_ticket"]]
        if alloc < k["min_ticket"]:
            drop, what = picks[-1], "the minimum ticket"
        elif under:
            drop = under[-1]
            what = "one share" if whole(drop, alloc) <= 0 else "the minimum ticket in whole shares"
        else:
            break
        drop["waiting"] = "cash: %s SAR per pick is below %s" % ("{:,.0f}".format(alloc), what)
        picks.remove(drop)
        pipeline.append(drop)
    alloc = min(cap, deployable / len(picks)) if picks else 0.0
    for p in picks:
        fx = FX_TO_SAR[p["ccy"]]
        qty = int(alloc // (p["price"] * fx))
        entry = p["price"]
        stop = round(entry * (1 - k["stop_pct"] / 100.0), 4)
        roi = (p.get("roi12") or 0.0) / 100.0
        if roi <= 0:
            p["labels"].append("no 12-month target")
        p.update({"entry": entry, "zone_low": round(entry * 0.97, 4), "zone_high": round(entry * 1.01, 4),
                  "qty": qty, "alloc_sar": round(qty * entry * fx, 2), "stop": stop,
                  "tp1": round(entry * (1 + roi / 2.0), 4) if roi > 0 else None,
                  "tp2": round(entry * (1 + roi), 4) if roi > 0 else None,
                  "risk_sar": round(qty * (entry - stop) * fx, 2)})
    pipeline.sort(key=lambda c: c["rank"])
    return {"picks": picks, "pipeline": pipeline, "nav": nav, "cash": cash_sar, "deployable": deployable,
            "per_pick": alloc}


def pick_status(row: Dict[str, Any], price: Optional[float], held: Dict[str, _dt.date], plan_date: _dt.date,
                earn_days: Optional[float], earn_guard: int, quality: str) -> Tuple[str, str]:
    """Status of a frozen pick now. Order: EXECUTED, INVALIDATED, WAIT, CHECK, OPEN."""
    bd = held.get(row["symbol"])
    if bd is not None and bd >= plan_date:
        return "EXECUTED", "bought %s" % bd.isoformat()
    if earn_days is not None and 0 <= earn_days <= earn_guard:
        return "INVALIDATED", "earnings in %d day(s)" % earn_days
    if price is None or price <= 0:
        return "CHECK", "no current price"
    if price <= row["stop"]:
        return "INVALIDATED", "price %s is at or below the stop %s" % (fmt(price), fmt(row["stop"]))
    if price > row["zone_high"]:
        return "WAIT", "price %s is above the entry zone (up to %s)" % (fmt(price), fmt(row["zone_high"]))
    if quality != "VERIFIED":
        return "CHECK", "source pages were not verified at the cut"
    return "OPEN", "price %s is inside the entry zone" % fmt(price) if price >= row["zone_low"] else \
        "price %s is below the entry zone, above the stop" % fmt(price)


def fmt(v: Any, nd: int = 2) -> str:
    x = num(v)
    return "" if x is None else ("{:,.%df}" % nd).format(x)


# ----------------------------------------------------------------------------- #
# Ledger rows
# ----------------------------------------------------------------------------- #
def ledger_rows(plan: Dict[str, Any], plan_date: str, cut_at: str, render: str, quality: str) -> List[List[Any]]:
    rows: List[List[Any]] = []
    for kind, items in (("PICK", plan["picks"]), ("PIPELINE", plan["pipeline"])):
        for c in items:
            pick = kind == "PICK"
            rows.append([
                plan_date, cut_at, render, quality, kind, c["rank"], c["symbol"], c.get("name", ""),
                c.get("market", ""), c.get("sector", ""), c.get("ccy", ""),
                c["entry"] if pick else (c.get("price") or ""), c["zone_low"] if pick else "",
                c["zone_high"] if pick else "", c["qty"] if pick else "", c["alloc_sar"] if pick else "",
                c["stop"] if pick else "", (c["tp1"] or "") if pick else "", (c["tp2"] or "") if pick else "",
                c["risk_sar"] if pick else "", c.get("score") if c.get("score") is not None else "",
                c.get("roi12") if c.get("roi12") is not None else "", " · ".join(c.get("labels") or []),
                "" if pick else c.get("waiting", ""), "", "", "", "",
            ])
    return rows


def parse_ledger(values: Sequence[Sequence[Any]]) -> List[Dict[str, Any]]:
    rows = list(values)
    if not rows or [text(c) for c in rows[0]][:len(LEDGER_HEADERS)] != LEDGER_HEADERS:
        return []
    out = []
    for r in rows[1:]:
        if not text(cell(r, 0)):
            continue
        d = {LEDGER_HEADERS[i]: cell(r, i) for i in range(len(LEDGER_HEADERS))}
        out.append(d)
    return out


def live_overlay(day_rows: List[Dict[str, Any]], sources: Dict[str, Dict[str, Any]],
                 quote=None) -> Dict[str, str]:
    """1.2.0: the last traded price for each open pick, laid over its source-page price. Returns {SYMBOL: 'HH:MM'}
    (Riyadh) for the names that got one. `quote(symbol) -> (price, utc datetime) | None`."""
    if quote is None:
        from scripts.exit_plan import live_quote as quote  # noqa: PLC0415 - exit_plan imports this module
    out: Dict[str, str] = {}
    for d in day_rows:
        if text(d["Kind"]) != "PICK" or text(d["Status"]) in ("EXECUTED", "INVALIDATED"):
            continue
        sym = text(d["Symbol"]).upper()
        try:
            q = quote(sym)
        except Exception:  # noqa: BLE001 - a quote that fails keeps the page price
            q = None
        if not q or not q[0] or q[0] <= 0:
            continue
        sources[sym] = dict(sources.get(sym) or {}, price=float(q[0]))
        out[sym] = q[1].astimezone(RIYADH).strftime("%H:%M")
    return out


def refresh_status(day_rows: List[Dict[str, Any]], sources: Dict[str, Dict[str, Any]],
                   cal: Dict[str, Dict[str, Any]], held: Dict[str, _dt.date], plan_date: _dt.date,
                   earn_guard: int, stamp: str, live: Optional[Dict[str, str]] = None) -> None:
    for d in day_rows:
        if text(d["Kind"]) != "PICK":
            continue
        sym = text(d["Symbol"]).upper()
        if text(d["Status"]) in ("EXECUTED", "INVALIDATED"):
            continue   # terminal: a later run never reopens or re-labels it
        price = (sources.get(sym) or {}).get("price")
        row = {"symbol": sym, "stop": num(d["Stop"]) or 0.0, "zone_low": num(d["Zone Low"]) or 0.0,
               "zone_high": num(d["Zone High"]) or 0.0}
        st, why = pick_status(row, price, held, plan_date, (cal.get(sym) or {}).get("earn_days"), earn_guard,
                              text(d["Cut Quality"]))
        if live and sym in live and price is not None:
            why += " (live %s Riyadh)" % live[sym]
        d["Status"], d["Status Reason"], d["Status At (Riyadh)"] = st, why, stamp
        d["Last Price"] = price if price is not None else ""


def page_rows(today_rows: List[Dict[str, Any]], prev_picks: Sequence[str], info: Dict[str, Any]) -> List[List[Any]]:
    """The Daily_Plan page as plain rows of PAGE_WIDTH cells."""
    def pad(r: List[Any]) -> List[Any]:
        return (list(r) + [""] * PAGE_WIDTH)[:PAGE_WIDTH]

    picks = [d for d in today_rows if text(d["Kind"]) == "PICK"]
    pipe = [d for d in today_rows if text(d["Kind"]) == "PIPELINE"]
    out: List[List[Any]] = [pad(["DAILY PLAN — decided once a day, frozen until the next cut"])]
    out.append(pad([info.get("headline", "")]))
    out.append(pad([info.get("money", "")]))
    now_syms = [text(d["Symbol"]) for d in picks]
    kept = [s for s in now_syms if s in prev_picks]
    out.append(pad(["Stability: picks changed today after the cut: 0 (frozen) · versus the previous plan: "
                    "%d kept, %d new, %d dropped" % (len(kept), len(now_syms) - len(kept),
                                                    len([s for s in prev_picks if s not in now_syms]))]))
    out.append(pad([]))
    out.append(pad(["TODAY'S PICKS (%d)" % len(picks)]))
    out.append(pad(["#", "Symbol", "Name", "Sector", "Entry", "Entry zone", "Qty", "Amount SAR", "Stop", "TP1",
                    "TP2", "Risk SAR", "Status", "Why / notes"]))
    for i, d in enumerate(picks, 1):
        zone = "%s – %s %s" % (fmt(d["Zone Low"]), fmt(d["Zone High"]), text(d["Ccy"]))
        note = " · ".join([s for s in (text(d["Status Reason"]), text(d["Labels"])) if s])
        out.append(pad([i, text(d["Symbol"]), text(d["Name"]), text(d["Sector"]),
                        "%s %s" % (fmt(d["Entry"]), text(d["Ccy"])), zone, fmt(d["Qty"], 0), fmt(d["Alloc SAR"], 0),
                        fmt(d["Stop"]), fmt(d["TP1"]), fmt(d["TP2"]), fmt(d["Risk SAR"], 0), text(d["Status"]),
                        note]))
    if not picks:
        out.append(pad(["", info.get("no_picks", "No name passed the execution gates today; see the pipeline.")]))
    out.append(pad([]))
    out.append(pad(["PIPELINE (%d) — qualified on merit, not picked today" % len(pipe)]))
    out.append(pad(["Rank", "Symbol", "Name", "Sector", "Price", "ROI 12M %", "Score", "Waiting for", "Labels"]))
    for d in pipe:
        out.append(pad([d["Rank"], text(d["Symbol"]), text(d["Name"]), text(d["Sector"]),
                        "%s %s" % (fmt(d["Entry"]), text(d["Ccy"])), fmt(d["ROI 12M %"], 1), fmt(d["Score"], 1),
                        text(d["Waiting For"]), text(d["Labels"])]))
    out.append(pad([]))
    out.append(pad(["Rules: picks = the engine's merit list in its own order, after the execution gates "
                    "(not held, fundable currency, earnings and ex-dividend dates, sector cap). TP1 = half-way to "
                    "the 12-month target, TP2 = the 12-month target. Not an order: nothing here places a trade."]))
    return out


# ----------------------------------------------------------------------------- #
# Workbook
# ----------------------------------------------------------------------------- #
def _col(n: int) -> str:
    s = ""
    while n > 0:
        n, r = divmod(n - 1, 26)
        s = chr(65 + r) + s
    return s


def _read(book, tab: str, rng: str) -> List[List[Any]]:
    try:
        return book.worksheet(tab).get(rng)
    except Exception as e:  # noqa: BLE001
        if type(e).__name__ == "WorksheetNotFound":
            return []
        raise


def read_source_page(book, tab: str) -> List[List[Any]]:
    """Only the columns the plan needs from a source page (one header read, one batch read)."""
    ws = book.worksheet(tab)
    head = ws.get("A1:EZ8")
    h, col = table_index(head, ("Symbol", "Current Price", "Currency"))
    if h is None:
        return []
    names = [n for n in ("Symbol", "Current Price", "Currency", "Sector", "Warnings", "Risk Score", "Target Price")
             if n in col]
    ranges = ["%s%d:%s" % (_col(col[n] + 1), h + 1, _col(col[n] + 1)) for n in names]
    cols = ws.batch_get(ranges)
    height = max((len(c) for c in cols), default=0)
    grid: List[List[Any]] = []
    for i in range(height):
        grid.append([(c[i][0] if i < len(c) and c[i] else "") for c in cols])
    return grid


def write_grid(book, tab: str, rows: List[List[Any]], width: int, min_rows: int) -> None:
    try:
        ws = book.worksheet(tab)
    except Exception as e:  # noqa: BLE001
        if type(e).__name__ != "WorksheetNotFound":
            raise
        ws = book.add_worksheet(title=tab, rows=max(min_rows, len(rows) + 5), cols=width)
    extent = max(min_rows, len(rows) + 5)
    if getattr(ws, "row_count", extent) < extent:
        ws.resize(rows=extent)
    if getattr(ws, "col_count", width) < width:
        ws.resize(cols=width)
    keep = max(extent, min(getattr(ws, "row_count", extent), MAX_LEDGER_ROWS + 50))
    values = [(list(r) + [""] * width)[:width] for r in rows] + [[""] * width for _ in range(keep - len(rows))]
    ws.update(values=values, range_name="A1:%s%d" % (_col(width), keep), value_input_option="RAW")


def main(argv: Optional[Sequence[str]] = None, *, book=None, now: Optional[_dt.datetime] = None,
         sleep=None) -> int:
    ap = argparse.ArgumentParser(description="Cut or refresh the frozen Daily Plan.")
    ap.add_argument("--dry-run", action="store_true", help="compute and print, write nothing")
    ap.add_argument("--wait-min", type=int, default=0,
                    help="while today has no plan, look again every 5 minutes for up to this many minutes")
    args = ap.parse_args(list(argv) if argv is not None else None)
    if mode() == "off":
        _out("TFB_DAILY_PLAN=off - nothing to do")
        return 0
    if sleep is None:
        import time as _time
        sleep = _time.sleep
    waited = 0
    while True:
        t = (now + _dt.timedelta(minutes=waited)) if now is not None else None
        code, has_plan = run_once(args.dry_run, book, t)
        if code != 0 or has_plan or waited + 5 > max(0, min(args.wait_min, 180)):
            return code
        _out("no plan yet - looking again in 5 minutes (%d of %d waited)" % (waited, args.wait_min))
        sleep(300)
        waited += 5


def run_once(dry_run: bool, book=None, now: Optional[_dt.datetime] = None) -> Tuple[int, bool]:
    """One look at the workbook: cut today's plan if there is none, else refresh its status."""
    write = mode() == "on" and not dry_run
    now = now or _dt.datetime.now(UTC)
    k = knobs()
    today = now.astimezone(RIYADH).date()
    stamp = riyadh_str(now)
    if book is None:
        from scripts.run_calendar_sync import _open_book  # noqa: PLC0415
        try:
            book = _open_book()
        except Exception as e:  # noqa: BLE001
            _out("ERROR: cannot open workbook: %s" % type(e).__name__)
            return 2, False
    for attempt in range(1, READ_ATTEMPTS + 1):
        try:
            board = book.worksheet(PAGE).get("A1:AZ120")
            grids = [read_source_page(book, t) for t in SOURCE_PAGES]
            cal = read_calendar(_read(book, "Calendar_Events", "A1:J4000"),
                                _read(book, "Dividend_Events", "A1:J4000"), today)
            lots = read_ledger_lots(_read(book, "_Portfolio_CostBasis", "A1:Z400"), today, k["cooldown"])
            ledger = parse_ledger(_read(book, LEDGER_TAB,
                                        "A1:%s%d" % (_col(len(LEDGER_HEADERS)), MAX_LEDGER_ROWS + 1)))
            break
        except Exception as e:  # noqa: BLE001
            missing = type(e).__name__ == "WorksheetNotFound"
            if missing or attempt == READ_ATTEMPTS:
                _out("ERROR: cannot read the workbook: %s; nothing written" % type(e).__name__)
                return 2, False
            _out("read failed (%s) - attempt %d of %d, retrying in %d s" % (
                type(e).__name__, attempt, READ_ATTEMPTS, READ_RETRY_S))
            _SLEEP(READ_RETRY_S)
    sources = read_sources(grids)
    day = today.isoformat()
    today_rows = [d for d in ledger if text(d["Plan Date"]) == day]
    older = [d for d in ledger if text(d["Plan Date"]) != day and parse_date(d["Plan Date"]) is not None
             and (today - parse_date(d["Plan Date"])).days <= LEDGER_KEEP_DAYS]
    prev_dates = sorted({text(d["Plan Date"]) for d in older})
    prev_picks = [text(d["Symbol"]) for d in older if prev_dates and text(d["Plan Date"]) == prev_dates[-1]
                  and text(d["Kind"]) == "PICK" and text(d["Status"]) != "EXECUTED"]
    info: Dict[str, Any] = {}
    state = board_state(board, now)
    if today_rows:
        _out("plan of %s exists (%d rows) - frozen; refreshing the status only" % (day, len(today_rows)))
    else:
        hour = now.astimezone(RIYADH).hour + now.astimezone(RIYADH).minute / 60.0
        quality = "VERIFIED" if state["verified"] else "UNVERIFIED"
        if not (state["finished"] and state["today"]):
            info["no_picks"] = "No plan yet today: %s." % state["reason"]
        elif quality != "VERIFIED" and hour < k["unverified_h"]:
            info["no_picks"] = ("No plan yet today: the main page's source pages are not verified (%s); an "
                                "unverified cut is allowed from %02d:00." % (state["output"], int(k["unverified_h"])))
        else:
            qualified = read_qualified(board)
            cash = num(panel_value(board, "Cash Available (SAR)")) or 0.0
            mps = int(num(panel_value(board, "T10: Max Per Sector")) or 2)
            plan = decide(qualified, sources, cal, lots, cash, mps, k, prior_picks=prev_picks)
            if not qualified:
                info["no_picks"] = "No plan yet today: the main page lists no qualified name."
            else:
                rows = ledger_rows(plan, day, stamp, state["run_at"], quality)
                today_rows = [{LEDGER_HEADERS[i]: r[i] for i in range(len(LEDGER_HEADERS))} for r in rows]
                _out("CUT %s from the render of %s (%s): %d pick(s), %d in the pipeline | NAV %s cash %s "
                     "deployable %s per pick %s" % (day, state["run_at"], quality, len(plan["picks"]),
                                                    len(plan["pipeline"]), fmt(plan["nav"], 0), fmt(cash, 0),
                                                    fmt(plan["deployable"], 0), fmt(plan["per_pick"], 0)))
    if today_rows:
        live = live_overlay(today_rows, sources) if k["live"] else {}
        if live:
            _out("live prices for %s" % ", ".join("%s %s" % (s, fmt((sources.get(s) or {}).get("price")))
                                                  for s in sorted(live)))
        refresh_status(today_rows, sources, cal, lots["held"], today, k["earn_days"], stamp, live)
        first = today_rows[0]
        info["headline"] = "Plan of %s · cut %s Riyadh from the main page's render of %s (%s) · status " \
                           "checked %s" % (day, text(first["Cut At (Riyadh)"]), text(first["Source Render"]),
                                           text(first["Cut Quality"]), stamp)
        alloc = sum(num(d["Alloc SAR"]) or 0.0 for d in today_rows if text(d["Kind"]) == "PICK")
        risk = sum(num(d["Risk SAR"]) or 0.0 for d in today_rows if text(d["Kind"]) == "PICK")
        info["money"] = "Planned %s SAR over the picks · loss if every stop is hit %s SAR · stop %s%% " \
                        "under the entry" % (fmt(alloc, 0), fmt(risk, 0), fmt(k["stop_pct"], 0))
    else:
        info["headline"] = "Plan of %s · not cut yet · checked %s Riyadh" % (day, stamp)
        _out(info.get("no_picks", "no plan"))
    for d in today_rows:
        if text(d["Kind"]) == "PICK":
            _out("  PICK %s qty %s entry %s stop %s -> %s (%s)" % (
                text(d["Symbol"]), fmt(d["Qty"], 0), fmt(d["Entry"]), fmt(d["Stop"]), text(d["Status"]),
                text(d["Status Reason"])))
    page = page_rows(today_rows, prev_picks, info)
    if not write:
        _out("%s - nothing written" % ("dry run" if dry_run else "observe"))
        return 0, bool(today_rows)
    all_rows = older + today_rows
    grid = [list(LEDGER_HEADERS)] + [[d[h] for h in LEDGER_HEADERS] for d in all_rows][-MAX_LEDGER_ROWS:]
    try:
        write_grid(book, LEDGER_TAB, grid, len(LEDGER_HEADERS), 200)
        write_grid(book, PAGE_TAB, page, PAGE_WIDTH, 80)
    except Exception as e:  # noqa: BLE001
        _out("ERROR: publication unconfirmed: %s; read back before retrying" % type(e).__name__)
        return 3, bool(today_rows)
    _out("wrote %d ledger row(s) and the %s page" % (len(all_rows), PAGE_TAB))
    return 0, bool(today_rows)


if __name__ == "__main__":
    sys.exit(main())
