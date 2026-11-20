#!/usr/bin/env python3
"""
scripts/trial_labels.py
================================================================================
TFB Trial Labels - what the two open trials would do with each stock, beside the stock on the pages
--------------------------------------------------------------------------------
v1.0.0 (2026-11-19)

WHY
    Owner, 19 Nov 2026 18:46 Riyadh, verbatim: "This new updat for news is
    linked with the recommdadtions pages like protofilo or investment pages";
    18:50, in Arabic: "do the best and most suitable for the project".
    The two trials opened that day - C-2 (PR #1026: evidence-only buy calls)
    and the news overlay (PR #1030: the sector tilt of the world news) - were
    measured in BigQuery and shown on News_Board only. Neither stood beside
    the stock on Top_10_Investments or Portfolio_Decision, where the owner
    decides. Neither has evidence yet, so neither may change a decision: they
    are shown, observe-only, so the owner sees each day what each trial would
    do before any of them can be promoted.

WHAT IT WRITES (its tab Trial_Labels; nothing else)
    One row per stock: every stock of Final_Decisions (each holding, the
    day's picks, every name on a page), every holding of My_Portfolio, and
    every buy call of the latest call day.
      C-2 trial    KEEPS / DROPS / PARTLY / NO BUY CALL, and why: the
                   condition a call fails (HIGH confidence, reliability 80+,
                   Tadawul, no analyst target) or its analyst-target upside
                   and its rank among the day's eligible calls of its market
                   group (the top half is kept). From v_c2_calls, the trial's
                   own membership, so the label cannot disagree with the trial.
      News trial   FAVOURED / AVOIDED / MIXED / NEUTRAL / NO TILT / NO SECTOR (with the last day of
                   its window, 'News until', after which the pages call it EXPIRED):
                   the news tilt in force for the stock's sector in both arms
                   (political events; events + news themes), and whether the
                   news version of the investable list would take the stock
                   out or bring it in on the latest list day (the same reading
                   as News_Board section 3). Outside the US the tilt is
                   measured on US sector funds: the label says so.
    31_Final_Decision_Column.gs 1.4.0 shows the two labels in the row of each
    stock on Top_10_Investments and Portfolio_Decision, right of FINAL
    DECISION.

SAFETY
    Read-only on BigQuery (SELECT only, through the Evidence Board's reader)
    and on every tab but its own: Final_Decisions!A1:B400 and
    My_Portfolio!A1:B200. No symbol is printed in the run log, only counts.
    TFB_TRIAL_LABELS = on (default) | off. --dry-run prints counts and writes
    nothing. A failed read, query or write leaves the previous tab in place
    (exit 2). A Sheets quota answer (HTTP 429) is waited out and tried again.
    Analysis only: no decision, score, list or order changes; only the Today
    Action Plan gives orders.

ENV
    DEFAULT_SPREADSHEET_ID, GOOGLE_SHEETS_CREDENTIALS, TFB_BQ_PROJECT,
    TFB_BQ_DATASET (default tfb_archive), TFB_BQ_LOCATION (default US),
    TFB_TRIAL_LABELS.
Exit: 0 written (or computed in a dry run, or switched off), 2 a read, a query or the write failed.

Not a licensed financial adviser; the owner decides and executes every trade.
================================================================================
"""
from __future__ import annotations

import argparse
import datetime as _dt
import os
import sys
import time
from typing import Any, Dict, List, Optional, Sequence, Tuple

from scripts import news_board as nb

__version__ = "1.0.0"
_SLEEP = time.sleep                                     # the wait after a Sheets quota answer

UTC = _dt.timezone.utc
RIYADH = _dt.timezone(_dt.timedelta(hours=3))
TAB = "Trial_Labels"
FINAL_TAB = "Final_Decisions"
PORTFOLIO_TAB = "My_Portfolio"
HEAD = ["Symbol", "Final decision", "C-2 trial", "News trial", "Call day", "News week", "News until", "Sector"]
WIDTH = len(HEAD)
CALL_DAYS_BACK = 7                                      # the latest call day is looked for this far back
HORIZONS = ("1W", "2W", "1M", "3M")
ARM_SHORT = {"events": "political events", "all": "events + news themes"}
GROUP_WORD = {"favoured": "FAVOURED", "avoided": "AVOIDED", "neutral": "neutral"}
MOVE_WORD = {"out": "would leave", "in": "would enter"}


def _out(msg: str) -> None:
    print("[trial_labels %s] %s" % (__version__, msg), flush=True)


def enabled() -> bool:
    return (os.environ.get("TFB_TRIAL_LABELS") or "on").strip().lower() not in ("off", "0", "false", "no")


text, num, day, flag = nb.text, nb.num, nb.day, nb.flag


def short_day(d: Optional[_dt.date]) -> str:
    return "%d %s" % (d.day, nb._MON[d.month - 1]) if d else ""


# ----------------------------------------------------------------------------- #
# The stocks to label (pure)
# ----------------------------------------------------------------------------- #
def read_finals(values: Sequence[Sequence[Any]]) -> List[Tuple[str, str]]:
    """(symbol, decision) for each row of Final_Decisions under its 'Symbol' / 'Decision' header (order kept)."""
    rows = [list(r) for r in values or []]
    if not rows or [text(c) for c in rows[0][:2]] != ["Symbol", "Decision"]:
        return []
    out, seen = [], set()
    for r in rows[1:]:
        sym = text(r[0]).upper() if r else ""
        if nb._HOLD_RE.match(sym) and sym not in nb._NOT_A_HOLDING and sym not in seen:
            seen.add(sym)
            out.append((sym, text(r[1]).upper() if len(r) > 1 else ""))
    return out


def stock_list(finals: Sequence[Tuple[str, str]], holdings: Sequence[Dict[str, str]],
               calls: Dict[str, List[Dict[str, Any]]]) -> List[Tuple[str, str]]:
    """Final_Decisions first (its order), then holdings not on it, then the latest call day's buy calls not yet named."""
    out, seen = [], set()

    def add(sym: str, dec: str) -> None:
        keys = set(nb.aliases(sym))
        if sym and not keys & seen:
            seen.update(keys)
            out.append((sym, dec))
    for sym, dec in finals:
        add(sym, dec)
    for h in holdings:
        add(h["symbol"], "")
    for sym in sorted(calls):
        add(sym, "")
    return out


def _find(mapping: Dict[str, Any], symbol: str) -> Any:
    return next((mapping[a] for a in nb.aliases(symbol) if a in mapping), None)


# ----------------------------------------------------------------------------- #
# C-2 (pure)
# ----------------------------------------------------------------------------- #
def latest_calls(rows: Sequence[Dict[str, Any]]) -> Tuple[Optional[_dt.date], Dict[str, List[Dict[str, Any]]]]:
    """The latest call day of the rows and, per symbol, its calls of that day (one per horizon)."""
    days = sorted({day(r.get("call_day")) for r in rows if day(r.get("call_day"))})
    if not days:
        return None, {}
    last = days[-1]
    out: Dict[str, List[Dict[str, Any]]] = {}
    for r in rows:
        if day(r.get("call_day")) == last and text(r.get("symbol")) and text(r.get("horizon")) in HORIZONS:
            out.setdefault(text(r.get("symbol")).upper(), []).append(r)
    return last, out


def _ordinal(n: int) -> str:
    return "%d%s" % (n, "th" if 10 <= n % 100 <= 20 else {1: "st", 2: "nd", 3: "rd"}.get(n % 10, "th"))


def _horizons(hs: Sequence[str]) -> str:
    hs = [h for h in HORIZONS if h in hs]
    return "all horizons" if len(hs) == len(HORIZONS) else " ".join(hs)


def _reason(r: Dict[str, Any]) -> str:
    """Why C-2 keeps or drops one call."""
    if not flag(r.get("eligible")):
        return text(r.get("why_out")) or "not eligible"
    up, rank, n, keeps = num(r.get("target_upside_pp")), num(r.get("upside_rank")), num(r.get("eligible_calls")), num(r.get("keeps"))
    group = "US" if text(r.get("market_group")) == "US" else "other-market"
    if up is None or rank is None or not n:
        return "upside not read"
    return "analyst upside %s%%, %s of %d eligible %s calls, top %d kept" % (
        nb.fmt(up, 1, sign=True), _ordinal(int(rank)), int(n), group, int(keeps or 0))


def c2_label(calls: Optional[Sequence[Dict[str, Any]]], call_day: Optional[_dt.date]) -> str:
    """KEEPS / DROPS / PARTLY / NO BUY CALL with the reason; one reason per distinct outcome across horizons."""
    if call_day is None:
        return "NO CALL DAY - no buy call in the archive in the last %d days" % CALL_DAYS_BACK
    if not calls:
        return "NO BUY CALL - none on %s; C-2 judges buy calls only" % short_day(call_day)
    groups: Dict[Tuple[bool, str], List[str]] = {}
    for r in sorted(calls, key=lambda r: HORIZONS.index(text(r.get("horizon")))):
        groups.setdefault((flag(r.get("c2_keep")), _reason(r)), []).append(text(r.get("horizon")))
    kept = [k for k in groups if k[0]]
    dropped = [k for k in groups if not k[0]]
    if kept and dropped:
        word = "PARTLY"
    else:
        word = "KEEPS" if kept else "DROPS"
    if len(groups) == 1:
        (keep, why), hs = next(iter(groups.items()))
        return "%s (%s) - %s" % (word, _horizons(hs), why)
    parts = ["%s %s: %s" % ("keeps" if keep else "drops", _horizons(hs), why) for (keep, why), hs in groups.items()]
    return "%s - %s" % (word, "; ".join(parts))


# ----------------------------------------------------------------------------- #
# News (pure; the tilt and the list day as News_Board reads them)
# ----------------------------------------------------------------------------- #
def tilt_week(tilt: Dict[str, Dict[str, Any]]) -> str:
    """The news week of the tilt in force (the first arm that has one); '' when none is in force."""
    lead = next((a for a in nb.ARMS if (tilt.get(a) or {}).get("in_force")), None)
    return tilt[lead]["week"] if lead else ""


def tilt_until(tilt: Dict[str, Dict[str, Any]]) -> str:
    """The last day the tilt in force applies ('' when none is in force): the page's news labels expire after it, even
    when this job stops writing the tab (31_Final_Decision_Column 1.4.0 reads it; Codex P2 on #1036)."""
    lead = next((a for a in nb.ARMS if (tilt.get(a) or {}).get("in_force")), None)
    a_to = tilt[lead]["apply_to"] if lead else None
    return a_to.isoformat() if a_to else ""


def news_label(symbol: str, sector: str, tilt: Dict[str, Dict[str, Any]], names: Dict[str, Dict[str, Any]],
               shadow_day: str) -> str:
    if not sector:
        return "NO SECTOR - not known to the archive"
    groups = {a: ((tilt.get(a) or {}).get("sectors", {}).get(sector) or {}).get("group", "")
              for a in nb.ARMS if (tilt.get(a) or {}).get("in_force")}
    if not groups:
        return "NO TILT IN FORCE - no news week applies today (%s)" % sector
    fav = [a for a, g in groups.items() if g == "favoured"]
    avo = [a for a, g in groups.items() if g == "avoided"]
    word = "MIXED" if fav and avo else "FAVOURED" if fav else "AVOIDED" if avo else "NEUTRAL"
    arms = ", ".join("%s %s" % (ARM_SHORT[a], GROUP_WORD.get(g, "not rated")) for a, g in groups.items())
    out = "%s - %s: %s" % (word, sector, arms)
    aligned = bool(shadow_day) and not nb.shadow_note(shadow_day, tilt)
    seen = _find(names, symbol) if aligned else None
    if seen:
        moves = {a: MOVE_WORD[x] for a, x in (seen.get("actions") or {}).items() if x in MOVE_WORD}
        if moves:
            if len(set(moves.values())) == 1 and len(moves) == len(nb.ARMS):
                out += "; news list of %s: %s (both arms)" % (short_day(day(shadow_day)), next(iter(moves.values())))
            else:
                out += "; news list of %s: %s" % (short_day(day(shadow_day)),
                                                  ", ".join("%s (%s)" % (w, ARM_SHORT[a]) for a, w in moves.items()))
    if nb.market_of(symbol) != "US":
        out += "; measured on US sector funds - indicative only for this market"
    return out


# ----------------------------------------------------------------------------- #
# The tab (pure)
# ----------------------------------------------------------------------------- #
def label_rows(stocks: Sequence[Tuple[str, str]], call_day: Optional[_dt.date], calls: Dict[str, List[Dict[str, Any]]],
               sectors: Dict[str, str], tilt: Dict[str, Dict[str, Any]], names: Dict[str, Dict[str, Any]],
               shadow_day: str) -> List[Dict[str, str]]:
    week, until = tilt_week(tilt), tilt_until(tilt)
    out = []
    for sym, dec in stocks:
        sector = _find(sectors, sym) or ""
        out.append({"symbol": sym, "decision": dec, "c2": c2_label(_find(calls, sym), call_day),
                    "news": news_label(sym, sector, tilt, names, shadow_day),
                    "call_day": call_day.isoformat() if call_day else "", "week": week, "until": until,
                    "sector": sector})
    return out


def page_rows(rows: Sequence[Dict[str, str]], call_day: Optional[_dt.date], tilt: Dict[str, Dict[str, Any]],
              shadow_day: str, stamp: str) -> List[List[Any]]:
    lead = next((a for a in nb.ARMS if (tilt.get(a) or {}).get("in_force")), None)
    news = ("tilt of the news week of %s, in force %s to %s" % (tilt[lead]["week"], tilt[lead]["apply_from"],
                                                                 tilt[lead]["apply_to"]) if lead else "no news tilt in force")
    page: List[List[Any]] = [
        ["TRIAL LABELS - what the two open trials would do with each stock. OBSERVE ONLY: not a decision, not an order."],
        ["Built %s Riyadh | trial_labels %s | C-2: buy calls of %s | news: %s; list day %s | only the Today Action Plan "
         "gives orders" % (stamp, __version__, call_day.isoformat() if call_day else "none in the archive", news,
                            shadow_day or "none")],
        ["C-2 (fixed 19 Nov 2026): keeps a buy call only when its confidence is not HIGH and its forecast reliability is "
         "under 80, it is not a Tadawul call, and its analyst target upside is in the top half of the day's such calls in "
         "its market group and horizon. Evidence from the calls of 20 Nov 2026 on (v_trial_c2); a switch needs the "
         "promotion test and the owner's decision."],
        ["NEWS (fixed 19 Nov 2026): the sector tilt of the world news in force (two arms: political events; events + news "
         "themes), and the news version of the investable list - names out of avoided sectors, names in from favoured "
         "ones. Evidence from the list days of 20 Nov 2026 on (v_trial_news_overlay). Same reading as News_Board."],
        [""],
        list(HEAD),
    ]
    for r in rows:
        page.append([r["symbol"], r["decision"], r["c2"], r["news"], r["call_day"], r["week"], r["until"], r["sector"]])
    if not rows:
        page.append(["", "", "No stock could be read from Final_Decisions, My_Portfolio or the latest buy calls."])
    page.append([""])
    page.append(["How to read: the FINAL DECISION is the decision; these two labels say what each trial WOULD do and why, "
                 "so its record can be judged before it changes anything. Not investment advice."])
    return page


def counts(rows: Sequence[Dict[str, str]]) -> Dict[str, int]:
    out: Dict[str, int] = {}
    for r in rows:
        for key, cell in (("c2", r["c2"]), ("news", r["news"])):
            word = cell.split(" - ")[0].split(" (")[0]
            out["%s %s" % (key, word)] = out.get("%s %s" % (key, word), 0) + 1
    return out


# ----------------------------------------------------------------------------- #
# SQL (SELECT only)
# ----------------------------------------------------------------------------- #
def sql_c2(ds: str, today: _dt.date) -> str:
    return ("WITH c AS (SELECT symbol, market_group, horizon, CAST(recorded_date AS STRING) AS call_day, eligible, "
            "eligible_calls, upside_rank, keeps, c2_keep, why_out, target_upside_pp FROM `%s.v_c2_calls` "
            "WHERE recorded_date >= DATE_SUB(%s, INTERVAL %d DAY) AND recorded_date <= %s), "
            "m AS (SELECT MAX(call_day) AS d FROM c) "
            "SELECT c.* FROM c JOIN m ON c.call_day = m.d ORDER BY c.symbol, c.horizon"
            % (ds, nb._d(today), CALL_DAYS_BACK, nb._d(today)))


def collect(session: Any, project: str, dataset: str, location: str, finals: Sequence[Tuple[str, str]],
            holdings: Sequence[Dict[str, str]], today: _dt.date, query: Any) -> Dict[str, Any]:
    ds = "%s.%s" % (project, dataset)
    call_day, calls = latest_calls(query(session, project, location, sql_c2(ds, today)))
    tilt = nb.pick_tilt(query(session, project, location, nb.sql_tilt(ds, today)), today)
    shadow_day, names = nb.shadow_names(query(session, project, location, nb.sql_shadow(ds, today)))
    stocks = stock_list(finals, holdings, calls)
    syms = nb.clean_symbols(a for s, _ in stocks for a in nb.aliases(s))
    sectors = {text(r.get("symbol")).upper(): text(r.get("sector"))
               for r in (query(session, project, location, nb.sql_sectors(ds, syms)) if syms else [])}
    rows = label_rows(stocks, call_day, calls, sectors, tilt, names, shadow_day)
    return {"rows": rows, "call_day": call_day, "tilt": tilt, "shadow_day": shadow_day, "calls": len(calls)}


def run_once(dry_run: bool, book=None, session=None, now: Optional[_dt.datetime] = None, query=None) -> int:
    now = now or _dt.datetime.now(UTC)
    if not enabled():
        _out("TFB_TRIAL_LABELS is off: nothing read, nothing written")
        return 0
    project, dataset = nb._env("TFB_BQ_PROJECT"), nb._env("TFB_BQ_DATASET", "tfb_archive")
    if not nb._ID_RE.match(project) or not nb._ID_RE.match(dataset):
        _out("ERROR: TFB_BQ_PROJECT / TFB_BQ_DATASET missing or malformed; nothing written")
        return 2
    try:
        if query is None:
            from scripts.evidence_board import bq_query as query  # noqa: PLC0415 - one SELECT-only reader
        if book is None:
            from scripts.run_calendar_sync import _open_book  # noqa: PLC0415
            book = _open_book()
        if session is None:
            from scripts.archive_to_bigquery import make_session  # noqa: PLC0415
            session = make_session()
        from scripts.daily_plan import _read as _raw_read, write_grid  # noqa: PLC0415
        from scripts.sheets_quota import call as _quota_call  # noqa: PLC0415

        finals = read_finals(_quota_call(lambda: _raw_read(book, FINAL_TAB, "A1:B400"), _SLEEP, _out,
                                         "reading %s" % FINAL_TAB))
        holdings = nb.read_holdings(_quota_call(lambda: _raw_read(book, PORTFOLIO_TAB, "A1:B200"), _SLEEP, _out,
                                                "reading %s" % PORTFOLIO_TAB))
        today = now.astimezone(RIYADH).date()
        data = collect(session, project, dataset, nb._env("TFB_BQ_LOCATION", "US"), finals, holdings, today, query)
    except Exception as e:  # noqa: BLE001 - one tab; a failed read leaves the previous tab in place
        _out("ERROR: %s: %s; nothing written" % (type(e).__name__, text(e)[:300]))
        return 2
    rows = data["rows"]
    _out("C-2: buy calls of %s (%d name(s) called) | news: week %s | list day %s" % (
        data["call_day"].isoformat() if data["call_day"] else "none", data["calls"], tilt_week(data["tilt"]) or "none in force",
        data["shadow_day"] or "none"))
    _out("LABELS %d stock(s) (%d from Final_Decisions, %d holding(s) read): %s" % (
        len(rows), len(finals), len(holdings), ", ".join("%s %d" % (k, v) for k, v in sorted(counts(rows).items())) or "none"))
    page = page_rows(rows, data["call_day"], data["tilt"], data["shadow_day"], now.astimezone(RIYADH).strftime("%Y-%m-%d %H:%M"))
    if dry_run:
        _out("dry run: %d row(s) computed, nothing written" % len(page))
        return 0
    try:                                                # one full overwrite: safe to send again after a quota answer
        _quota_call(lambda: write_grid(book, TAB, page, WIDTH, 60), _SLEEP, _out, "writing %s" % TAB)
    except Exception as e:  # noqa: BLE001 - reported, with the exit code that marks the run failed
        _out("ERROR: %s: %s; the tab was not written" % (type(e).__name__, text(e)[:300]))
        return 2
    _out("wrote the %s tab (%d stock(s))" % (TAB, len(rows)))
    return 0


def main(argv: Optional[Sequence[str]] = None, **kw) -> int:
    ap = argparse.ArgumentParser(description="TFB Trial Labels (observe-only labels of the open trials)")
    ap.add_argument("--dry-run", action="store_true")
    a = ap.parse_args(list(argv) if argv is not None else None)
    return run_once(a.dry_run, **kw)


if __name__ == "__main__":
    sys.exit(main())
