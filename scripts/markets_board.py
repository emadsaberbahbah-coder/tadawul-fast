#!/usr/bin/env python3
"""
scripts/markets_board.py
================================================================================
TFB Markets Board - the condition of every market, their order, and the score
--------------------------------------------------------------------------------
v1.3.0 (2026-11-28) - THE LIVE BUY RULE, SAID AS CONFIGURED. Owner, 28 Nov 2026
    14:24 Riyadh, in Arabic: the important thing is that the pages agree and
    do not contradict each other. That day the owner narrowed the buy hold to
    drawdown and crash_day (TFB_PLAN_REGIME_BLOCK, and TFB_MARKET_GATE_HOLD on
    Render), while this page still said "the Daily Plan holds a new US buy
    unless the US is calm" and "cash stays cash" on a day the plan could buy.
    Now:
    US        names the Daily Plan's rule as configured (its hold labels and
              whether today's US label is held);
    LIVE RULE (new, under it) the hold labels and, for the five stock markets
              the main page's market gate covers, which are held today and
              which are open;
    VERDICT   and the last days' verdicts are named as this page's market test
              (calm only: the market trial fixed on 21 Nov), never as the
              live rule; the How to read line says so.
    The views, the trial and every section below are unchanged.
v1.2.0 (2026-11-21) - THE EVENTS AHEAD AND THEIR SCORECARD. Owner, 21 Nov 2026
    11:03 Riyadh, item 5 of the build list: "News: a news reading per market;
    forward event scenarios; momentum/surprise; influential figures of 19 Nov;
    score every news signal." Two sections and one line, read from
    v_event_scenarios, v_event_trial and v_event_trial_summary (views 1.21.0):
    NEXT EVENT under the verdict - the first event ahead and what its earlier
       ones did to the US and to oil;
    7. EVENTS AHEAD - each Fed, ECB, Bank of Japan and OPEC+ meeting and each
       tracked release of the next 14 days: the forecast, the chances of the
       figure (above / in line / below, from the family's earlier figures), and
       for the US, Saudi Arabia, oil, gold and bonds the median event-day move
       of the earlier events with its middle 80 % band, the US move after a
       figure above / below its forecast, the US chance of a big move, and the
       market that moved most on such days; then the events of the last 7 days
       against their band;
    8. THE EVENTS SCORECARD - per family and for all, the backtest and the
       forward test (from 23 Nov 2026): how often the band held (80 % for a true
       band), whether the family's big-move chance beat any day's, how often the
       direction after a figure held, whether the figure chances beat equal
       thirds, with the reading of each period in one sentence.
    Until views 1.21.0 are in BigQuery both sections say so and the rest of the
    page is built all the same; any other refusal still fails the run.
v1.1.0 (2026-11-21) - THE READY LIST AND THE SWITCH CHECK. Owner, 21 Nov 2026
    11:03 Riyadh, items 3 and 4 of the build list: "A ready list: each day the
    2-3 best qualified names from the top-ranked markets, each labelled with
    what it is waiting for (cash / calmer market / switch). Every page shows the
    same final decision; only the Today Action Plan gives orders." and "A switch
    check: propose a swap only when a qualified name's expected return beats a
    holding's over the same period, after costs on both sides." Three sections
    are added, read from v_ready_list, v_hold_vs_switch and v_ready_trial
    (views 1.19.0):
    4. THE READY LIST - every qualified name of the day in the ready order
       (holdings are out: section 5 compares each), the market it tracks (not
       its currency) and that market's state, whether the market rule and the
       live US rule would hold it, what it is waiting for, and the Today Action
       Plan's own decision for it, copied from Final_Decisions (so this page
       never states another decision).
    5. THE SWITCH CHECK - each holding against the best qualified name of its
       market (v_hold_vs_switch: the forecast gap converted into what such a
       gap has been worth, both trades charged, the chance it ends ahead).
    6. THE READY LIST SCORECARD - what the qualified names did next, by the
       state of their market, on and off the ready list, held or not by each
       rule; its reading puts the ready list against the other qualified names
       first, then each rule's held names against the ones it let through.
    It now reads one tab, Final_Decisions!A1:C400, and nothing else.
v1.0.0 (2026-11-21)

WHY
    Owner, 21 Nov 2026 11:03 Riyadh, the build list after the voice call:
    "A condition reading for every market, with the same tests as
    v_market_regime ... A daily ranking of the markets by trend and risk ...
    When every market is weak, cash stays cash ... Accuracy: log and score
    everything. Observe for two weeks (10 sessions), then review with the
    owner, and switch on only what proved itself." In the call itself:
    Global_Markets had 23 names left to invest, Top 10 had funded nothing for
    weeks and every market kept falling - were Japan, China or Europe, or gold,
    oil, bonds, sukuk or REITs, doing any better? The readings are BigQuery
    views (v_market_condition, v_market_rank, v_market_trial_summary - views
    1.18.0); this page puts them in the workbook, beside the decision pages.

WHAT IT WRITES (its tab Markets_Board; nothing else)
    1. The verdict of the day: how many of the markets with a fund are open,
       'ALL ON HOLD - cash stays cash' when none is, and for how many days in a
       row every market has been on hold.
    2. One row per market, in the day's order (open first, then the held ones,
       each group by trend score): its state, its condition, everything it is
       waiting for (each failed test with its limit), the figures behind it and
       the fund a buy would use.
    3. The last 14 days: the verdict of each.
    4. The scorecard: what each market's fund did after the reading - open
       against held markets, the day's first open market against SPY, SPY after
       the days with every market on hold, the rank correlation of the trend
       score - for the backtest (the same rules read back on the archive) and
       the forward test (from Monday 23 Nov 2026, reviewed after 10 sessions),
       with the reading of each in one sentence.
    5. How to read the page.

SAFETY
    Read-only on BigQuery (SELECT only, through the Evidence Board's reader);
    it reads Final_Decisions!A1:C400 (1.1.0) and writes only Markets_Board.
    No symbol is printed in the run log, only counts. Observe only: no decision,
    gate, list or order reads this page; the Daily Plan keeps its US rule;
    only the Today Action Plan gives orders. Nothing of the owner's account or
    holdings is read, written or printed. TFB_MARKETS_BOARD = on (default) |
    off. --dry-run prints the counts and writes nothing. A failed query or
    write leaves the previous tab in place (exit 2). A Sheets quota answer
    (HTTP 429) is waited out and tried again.

ENV
    DEFAULT_SPREADSHEET_ID, GOOGLE_SHEETS_CREDENTIALS, TFB_BQ_PROJECT,
    TFB_BQ_DATASET (default tfb_archive), TFB_BQ_LOCATION (default US),
    TFB_MARKETS_BOARD; TFB_PLAN_REGIME, TFB_PLAN_REGIME_BLOCK and
    TFB_PLAN_REGIME_CCY (1.1.0: the live US rule's own settings, passed by the
    workflow with the Daily Plan's defaults, to say what that rule does with
    each name).
Exit: 0 written (or computed in a dry run, or switched off), 2 a query or the write failed.

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

__version__ = "1.3.0"
_SLEEP = time.sleep                                     # the wait after a Sheets quota answer

UTC = _dt.timezone.utc
RIYADH = _dt.timezone(_dt.timedelta(hours=3))
TAB = "Markets_Board"
HEAD = ["Rank", "Market", "State", "Condition", "Waiting for", "Trend score", "1 close %", "5 closes %",
        "20 closes %", "vs 50-close average %", "20-close volatility %", "From 250-close high %", "Normal volatility %",
        "Fund", "Fund 5 closes %", "Fund 20 closes %", "Read on", "Last close"]
WIDTH = len(HEAD)
# the markets of v_market_condition, in its market_order (test M7 checks the view lists the same ids)
MARKETS = ("us", "sa", "eu", "jp", "cn", "gold", "oil", "bonds", "sukuk", "reits_us", "reits_sa")
FORWARD_FROM = _dt.date(2026, 11, 23)                   # v_market_trial: period 'forward' from this US session
REVIEW_SESSIONS = 10
HISTORY_DAYS = 60                                       # read back to count the days in a row on hold
SHOW_DAYS = 14
HORIZONS = (5, 10, 20)
PERIODS = ("backtest", "forward")
FUND_WORD = {"SA_REITS": "the REITs on Tadawul"}
FINAL_TAB = "Final_Decisions"
READY_HEAD = ["Ready", "Symbol", "Name", "Tracks", "Market state", "Market rule", "Live US rule", "Waiting for",
              "Final decision (Today Action Plan)", "1-month expected %", "Overall score", "Currency", "Page"]
SWITCH_HEAD = ["Holding", "Name", "Verdict", "Horizon", "Holding forecast %", "Best qualified name", "Its forecast %",
               "Net after both trades, pp", "Chance it ends ahead %", "Why"]
TRIAL_HEAD = ["Period", "Horizon", "Group", "Name-days", "Days", "Mean change %", "Against the day's median, pp",
              "Rose %", "Worst 5 % %"]
READY_N = 3
PLAN_LABELS = ("crash_day", "drawdown", "stress", "weak", "calm")
# 1.2.0: the events (views 1.21.0)
EVENT_DAYS = 14                                         # the events of the next 14 days
EVENT_BACK_DAYS = 7                                     # and the events of the last 7, against their band
EVENT_MARKETS = ("us", "sa", "oil", "gold", "bonds")    # the markets shown beside each event
EVENT_FORWARD_FROM = _dt.date(2026, 11, 23)             # v_event_trial: period 'forward' from 23 Nov 2026 (Riyadh)
EVENT_HEAD = ["When (Riyadh)", "Event", "Forecast", "Previous", "Figure: above / in line / below %", "Earlier events",
              "United States", "US if above / if below", "Saudi Arabia", "Oil", "Gold", "Bonds",
              "US big-move chance %", "Moved most"]
EVENT_LAST_HEAD = ["When (Riyadh)", "Event", "Figure", "Forecast", "Actual", "", "United States", "", "Saudi Arabia",
                   "Oil", "Gold", "Bonds"]
EVENT_TRIAL_HEAD = ["Period", "Family", "Events", "Band held %", "Band rows", "Big moves %", "Any day %",
                    "Big-move skill", "Direction held %", "Direction events", "Direction z", "Drift held %",
                    "Drift events", "Figure skill", "Figures", "Verdict"]
EVENT_VIEWS = "v_event_scenarios, v_event_trial and v_event_trial_summary (apply_bq_views 1.21.0)"

text, num, day, flag = nb.text, nb.num, nb.day, nb.flag


def _out(msg: str) -> None:
    print("[markets_board %s] %s" % (__version__, msg), flush=True)


def enabled() -> bool:
    return (os.environ.get("TFB_MARKETS_BOARD") or "on").strip().lower() not in ("off", "0", "false", "no")


def r2(v: Any) -> Any:
    """A figure for a cell: rounded to 2 places, or '' when it is not known."""
    x = num(v)
    return "" if x is None else round(x, 2)


def short_day(d: Optional[_dt.date], year: bool = True) -> str:
    return nb.day_text(d, year) if isinstance(d, _dt.date) else "none"


# ----------------------------------------------------------------------------- #
# SQL (SELECT only)
# ----------------------------------------------------------------------------- #
def sql_rank(ds: str, today: _dt.date) -> str:
    return ("SELECT CAST(as_of AS STRING) AS as_of, market_id, market, market_order, reading_symbol, reading_name, "
            "fund_symbol, buyable, CAST(date AS STRING) AS session, days_old, label, state, rank, buyable_rank, "
            "trend_score, r1_pct, r5_pct, r20_pct, vs_50d_pct, vol20_pct, dd_from_high_pct, normal_vol_pct, k, "
            "vix_close, waiting_for, fund_r5_pct, fund_r20_pct, open_markets, buyable_markets, verdict "
            "FROM `%s.v_market_rank` WHERE as_of > DATE_SUB(%s, INTERVAL %d DAY) AND as_of <= %s "
            "ORDER BY as_of DESC, rank" % (ds, nb._d(today), HISTORY_DAYS, nb._d(today)))


def sql_summary(ds: str) -> str:
    return "SELECT * FROM `%s.v_market_trial_summary` ORDER BY period, horizon" % ds


def sql_ready(ds: str, today: _dt.date) -> str:
    """1.1.0: the latest day of the ready list on or before `today` (6 days at most), in the ready order."""
    return ("SELECT CAST(snapshot_date AS STRING) AS snapshot_date, symbol, name, source_page, currency, "
            "market_country, market_id, market, market_state, market_label, market_waiting_for, market_rank, "
            "ready_rank, in_ready, qualified, expected_roi_1m_pct, overall_score, waiting_for, us_label "
            "FROM `%s.v_ready_list` WHERE snapshot_date > DATE_SUB(%s, INTERVAL 7 DAY) AND snapshot_date <= %s "
            "QUALIFY snapshot_date = MAX(snapshot_date) OVER () ORDER BY ready_rank"
            % (ds, nb._d(today), nb._d(today)))


def sql_switch(ds: str) -> str:
    """1.1.0: the switch check of every holding and horizon."""
    return ("SELECT symbol, name, horizon, horizon_order, holding_forecast_pp, best_symbol, best_forecast_pp, net_pp, "
            "chance_pct, verdict, reason FROM `%s.v_hold_vs_switch` ORDER BY symbol, horizon_order" % ds)


def sql_ready_trial(ds: str) -> str:
    return "SELECT * FROM `%s.v_ready_trial` ORDER BY period, horizon, grp" % ds


def _ts(t: _dt.datetime) -> str:
    """1.2.0: a moment for the SQL - a timezone-aware time, never 'now' inside the query."""
    if not isinstance(t, _dt.datetime) or t.tzinfo is None:
        raise ValueError("an aware time is needed")
    return "TIMESTAMP '%s'" % t.astimezone(UTC).strftime("%Y-%m-%d %H:%M:%S+00")


def sql_events_ahead(ds: str, now: _dt.datetime) -> str:
    """1.2.0: the scenario of every event of the next EVENT_DAYS days, market by market."""
    return ("SELECT event_id, family, family_name, kind, country, CAST(event_at AS STRING) AS event_at, period, "
            "estimate, previous, market_order, market_id, market, n_prior, r1_p10, r1_p50, r1_p90, p_big, mean_abs_z, "
            "n_above, n_below, r1_p50_above, r1_p50_below, n_fig, p_above, p_in_line, p_below "
            "FROM `%s.v_event_scenarios` WHERE NOT is_out AND event_at >= %s "
            "AND event_at < TIMESTAMP_ADD(%s, INTERVAL %d DAY) ORDER BY event_at, event_id, market_order"
            % (ds, _ts(now), _ts(now), EVENT_DAYS))


def sql_events_last(ds: str, now: _dt.datetime) -> str:
    """1.2.0: the events of the last EVENT_BACK_DAYS days that are out, against their band, for the shown markets."""
    return ("SELECT event_id, family_name, CAST(event_at AS STRING) AS event_at, figure, estimate, actual, market_id, "
            "market_order, r1_pct, r1_p10, r1_p90, band_hit, move "
            "FROM `%s.v_event_trial` WHERE event_at >= TIMESTAMP_SUB(%s, INTERVAL %d DAY) AND event_at < %s "
            "AND market_id IN (%s) ORDER BY event_at DESC, event_id, market_order"
            % (ds, _ts(now), EVENT_BACK_DAYS, _ts(now), ", ".join("'%s'" % m for m in EVENT_MARKETS)))


def sql_events_summary(ds: str) -> str:
    """1.2.0: the events scorecard, the all-markets lines of each family and of all families."""
    return ("SELECT period, family, family_name, events, not_logged, band_n, band_pct, big_n, big_pct, any_pct, "
            "big_skill, dir_n, dir_events, dir_pct, dir_z, drift_n, drift_events, drift_pct, drift_z, fig_n, "
            "fig_skill, verdict "
            "FROM `%s.v_event_trial_summary` WHERE market_id = 'all' ORDER BY period, family" % ds)


# ----------------------------------------------------------------------------- #
# the day, the history, the verdict
# ----------------------------------------------------------------------------- #
def by_day(rows: Sequence[Dict[str, Any]]) -> List[Tuple[_dt.date, List[Dict[str, Any]]]]:
    """[(as_of, its rows in rank order)], the latest day first."""
    days: Dict[_dt.date, List[Dict[str, Any]]] = {}
    for r in rows:
        d = day(r.get("as_of"))
        if d is not None and text(r.get("market_id")):
            days.setdefault(d, []).append(r)
    return [(d, sorted(days[d], key=lambda r: (num(r.get("rank")) or 99))) for d in sorted(days, reverse=True)]


TEST_WORDS = "market test: calm only"                   # 1.3.0: this page's test, never the live rule
LIVE_MARKETS = (("us", "United States"), ("sa", "Saudi Arabia"), ("eu", "Europe"), ("jp", "Japan"),
                ("cn", "China / Hong Kong"))           # the stock markets the main page's market gate covers


def test_verdict(v: Any) -> str:
    """1.3.0: the view's verdict of a day named as this page's market test (calm only), not as the live buy rule."""
    t = text(v)
    if not t:
        return t
    return ("ALL ON HOLD (%s)" % TEST_WORDS) if t.startswith("ALL ON HOLD") else "%s (%s)" % (t, TEST_WORDS)


def us_words(us: str, rule: Dict[str, Any]) -> str:
    """1.3.0 PURE: the Daily Plan's US rule as configured, and what it does with today's US label."""
    if rule["mode"] == "off":
        return "the Daily Plan's US rule is off: a new US buy is not held for the market"
    labels = ", ".join(rule["block"]) or "no label"
    held = us in rule["block"]
    today = ("held" if rule["mode"] == "enforce" else "would be held (observe)") if held else "not held"
    return "the Daily Plan holds a new US buy on %s (TFB_PLAN_REGIME_BLOCK): today the US reads %s - %s" % (
        labels, us or "not read", today)


def live_rule_line(rows: Sequence[Dict[str, Any]], rule: Dict[str, Any]) -> Tuple[str, str]:
    """1.3.0 PURE: the live buy rule today, market by market - a market is held when its label is one of the Daily
    Plan's hold labels (the main page's market gate holds the same list on Render; Consistency_Check C16 compares
    the two)."""
    if rule["mode"] == "off":
        return "off", "the Daily Plan's market rule is off: no market is held for its condition"
    labels = {text(r.get("market_id")): text(r.get("label")) for r in rows}
    held, allowed = [], []
    for mid, name in LIVE_MARKETS:
        lab = labels.get(mid)
        if lab:
            (held if lab in rule["block"] else allowed).append("%s (%s)" % (name, lab))
    head = "hold on %s%s" % (", ".join(rule["block"]) or "no label", " (observe)" if rule["mode"] == "observe" else "")
    return head, "new buys held today: %s; allowed: %s - the main page and the Daily Plan" % (
        ", ".join(held) or "none", ", ".join(allowed) or "none")


def day_facts(d: _dt.date, rows: Sequence[Dict[str, Any]]) -> Dict[str, Any]:
    first = rows[0] if rows else {}
    us = [r for r in rows if text(r.get("market_id")) == "us"]
    opened = [r for r in rows if text(r.get("state")) == "open" and flag(r.get("buyable"))]
    return {"day": d, "open": int(num(first.get("open_markets")) or 0),
            "buyable": int(num(first.get("buyable_markets")) or 0), "verdict": test_verdict(first.get("verdict")),
            "open_names": [text(r.get("market")) for r in opened], "first": text(first.get("market")),
            "first_state": text(first.get("state")), "us": text(us[0].get("label")) if us else "not read",
            "read": len(rows)}


def hold_streak(days: Sequence[Dict[str, Any]]) -> Tuple[int, Optional[_dt.date], bool]:
    """(calendar days in a row, from the latest day back, with no market open; the first of them; whether the run
    reaches the start of the history read - then it is at least that long)."""
    n, start = 0, None
    for i, f in enumerate(days):
        if f["open"] != 0:
            return n, start, False
        if i and (days[i - 1]["day"] - f["day"]).days != 1:          # a gap in the calendar ends the run
            return n, start, False
        n, start = n + 1, f["day"]
    return n, start, bool(days)


def verdict_lines(f: Dict[str, Any], rows: Sequence[Dict[str, Any]], streak: Tuple[int, Optional[_dt.date], bool]
                  ) -> Tuple[str, str]:
    n, start, capped = streak
    if f["open"] == 0:
        how_long = ("%s%d day(s) in a row, since %s" % ("at least " if capped else "", n, short_day(start))) if n else ""
        return ("ALL ON HOLD (%s)" % TEST_WORDS,
                "none of the %d markets with a fund is calm (oil is a reading only)%s - the live buy rule is the "
                "LIVE RULE line" % (f["buyable"], "; every market on hold for " + how_long if how_long else ""))
    first = [r for r in rows if text(r.get("state")) == "open" and flag(r.get("buyable"))][0]
    return ("%d of %d OPEN (%s)" % (f["open"], f["buyable"], TEST_WORDS),
            "open: %s; first: %s - a buy would use %s" % (
                ", ".join(f["open_names"]), text(first.get("market")), fund_word(first.get("fund_symbol"))))


def fund_word(sym: Any) -> str:
    s = text(sym)
    return FUND_WORD.get(s, s) if s else "none (reading only)"


def last_close(r: Dict[str, Any]) -> str:
    d, old = day(r.get("session")), int(num(r.get("days_old")) or 0)
    return short_day(d) + (" - %d days old" % old if old >= 3 else "")


def market_row(r: Dict[str, Any]) -> List[Any]:
    state = text(r.get("state")).upper()
    waiting = text(r.get("waiting_for")) or ("nothing - calm" if state == "OPEN" else "")
    return [int(num(r.get("rank")) or 0), text(r.get("market")), state, text(r.get("label")), waiting,
            r2(r.get("trend_score")), r2(r.get("r1_pct")), r2(r.get("r5_pct")), r2(r.get("r20_pct")),
            r2(r.get("vs_50d_pct")), r2(r.get("vol20_pct")), r2(r.get("dd_from_high_pct")), r2(r.get("normal_vol_pct")),
            fund_word(r.get("fund_symbol")), r2(r.get("fund_r5_pct")), r2(r.get("fund_r20_pct")),
            text(r.get("reading_name")), last_close(r)]


def missing_markets(rows: Sequence[Dict[str, Any]], history: Sequence[Dict[str, Any]]) -> List[List[Any]]:
    """A row for each market of v_market_condition with no close in the last 6 days on the board's day."""
    seen = {text(r.get("market_id")) for r in rows}
    names = {text(r.get("market_id")): text(r.get("market")) for r in history}
    out = []
    for m in MARKETS:
        if m not in seen:
            out.append(["", names.get(m) or m, "NO READING", "",
                        "no close in the last 6 days (a holiday, or the series stopped): left out of the order"]
                       + [""] * (WIDTH - 5))
    return out


# ----------------------------------------------------------------------------- #
# the scorecard
# ----------------------------------------------------------------------------- #
SCORE_ROWS = (
    ("days", "US sessions read"),
    ("open_n", "Open markets: outcomes"),
    ("open_mean_pct", "Open markets: mean change of the fund %"),
    ("open_up_pct", "Open markets: share that rose %"),
    ("open_p5_pct", "Open markets: worst 5 % (5th percentile) %"),
    ("hold_n", "Held markets: outcomes"),
    ("hold_mean_pct", "Held markets: mean change of the fund %"),
    ("hold_up_pct", "Held markets: share that rose %"),
    ("hold_p5_pct", "Held markets: worst 5 % (5th percentile) %"),
    ("open_minus_hold_pp", "Open minus held, pp (above 0 = the hold earned its place)"),
    ("top_n", "First open market of the day: days"),
    ("top_us_n", "... of which the US itself (no switch)"),
    ("top_vs_spy_pp", "... the others against SPY, pp (what a switch changed)"),
    ("top_beat_spy_pct", "... the others beat SPY, % of days"),
    ("cash_days", "Days with every market on hold (cash)"),
    ("cash_spy_mean_pct", "SPY after those days % (below 0 = cash avoided a loss)"),
    ("ic_days", "Days with 5 markets or more scored"),
    ("mean_ic", "Rank correlation, trend score against the outcome (mean of days)"),
)


def score_index(summary: Sequence[Dict[str, Any]]) -> Dict[Tuple[str, int], Dict[str, Any]]:
    out = {}
    for r in summary:
        p, h = text(r.get("period")), num(r.get("horizon"))
        if p in PERIODS and h is not None and int(h) in HORIZONS:
            out[(p, int(h))] = r
    return out


def score_table(summary: Sequence[Dict[str, Any]]) -> List[List[Any]]:
    idx = score_index(summary)
    cols = [(p, h) for p in PERIODS for h in HORIZONS]
    head = ["Measure"] + ["%s, %d closes" % (p.capitalize(), h) for p, h in cols]
    rows = [head]
    for key, label in SCORE_ROWS:
        cells = []
        for c in cols:
            v = (idx.get(c) or {}).get(key)
            x = num(v)
            cells.append("" if x is None else (int(x) if key.endswith("_n") or key in ("days", "cash_days", "ic_days")
                                               else round(x, 3 if key == "mean_ic" else 2)))
        rows.append([label] + cells)
    return rows


def _sign(x: float) -> str:
    return "%+.2f %%" % x


def reading(period: str, summary: Sequence[Dict[str, Any]]) -> str:
    """One sentence on what the scorecard of `period` says, from its 5-close row (and the 20-close tail)."""
    idx = score_index(summary)
    r5, r20 = idx.get((period, 5)) or {}, idx.get((period, 20)) or {}
    o, h = num(r5.get("open_mean_pct")), num(r5.get("hold_mean_pct"))
    if period == "forward" and not (num(r5.get("open_n")) or num(r5.get("hold_n"))):
        return ("FORWARD (from %s): nothing has matured yet - a reading is scored 5 closes after the first close that "
                "follows it; the owner reviews it after %d sessions." % (short_day(FORWARD_FROM), REVIEW_SESSIONS))
    if o is None or h is None:
        return "%s: not enough outcomes to read yet." % period.upper()
    head = ("BACKTEST (the same rules read back on the archive, before %s)" % short_day(FORWARD_FROM)
            if period == "backtest" else "FORWARD (from %s, %d session(s) read)" % (
                short_day(FORWARD_FROM), int(num(r5.get("days")) or 0)))
    parts = ["over the next 5 closes the open markets' funds averaged %s and the held ones %s - %s" % (
        _sign(o), _sign(h), "the hold earned its place" if o > h else "the hold has not earned its place")]
    op5, hp5 = num(r5.get("open_p5_pct")), num(r5.get("hold_p5_pct"))
    if op5 is not None and hp5 is not None:
        parts.append("the worst 5 %% of the held ones was %s against %s for the open ones (%s)" % (
            _sign(hp5), _sign(op5), "the label does flag the riskier markets" if hp5 < op5 else "no deeper"))
    ic = num(r5.get("mean_ic"))
    if ic is not None:
        parts.append("the trend order: rank correlation %+.3f (%s)" % (
            ic, "no edge" if abs(ic) < 0.05 else ("it helps" if ic > 0 else "it works against")))
    vs = num(r5.get("top_vs_spy_pp"))
    if vs is not None:
        parts.append("switching to the first open market other than the US changed %+.2f pp against SPY" % vs)
    cd, cs = int(num(r5.get("cash_days")) or 0), num(r5.get("cash_spy_mean_pct"))
    if cd and cs is not None:
        cs20 = num(r20.get("cash_spy_mean_pct"))
        parts.append("on the %d day(s) with every market on hold SPY %s %.2f %% over the next 5 closes%s (%s)" % (
            cd, "rose" if cs > 0 else "fell", abs(cs), " and %s over 20" % _sign(cs20) if cs20 is not None else "",
            "cash missed the rise" if cs > 0 else "cash avoided the fall"))
    return "%s: %s." % (head, "; ".join(parts))


# ----------------------------------------------------------------------------- #
# 1.1.0: the ready list and the switch check
# ----------------------------------------------------------------------------- #
def read_final(values: Sequence[Sequence[Any]]) -> Dict[str, Tuple[str, str]]:
    """Final_Decisions!A1:C400 -> {SYMBOL: (decision, do today)}; {} when the header is not the expected one."""
    if not values:
        return {}
    head = [text(c) for c in values[0]]
    if head[:3] != ["Symbol", "Decision", "Do today"]:
        return {}
    out: Dict[str, Tuple[str, str]] = {}
    for row in values[1:]:
        cells = (list(row) + ["", "", ""])[:3]
        sym = text(cells[0]).upper()
        if sym and sym not in out:
            out[sym] = (text(cells[1]).upper(), text(cells[2]))
    return out


def plan_rule() -> Dict[str, Any]:
    """The live US rule's settings as the Daily Plan reads them (the workflow passes its Variables with the Daily
    Plan's defaults): mode, the labels that hold a buy, the currencies held."""
    mode = (os.environ.get("TFB_PLAN_REGIME") or "enforce").strip().lower()
    block = [x.strip().lower() for x in (os.environ.get("TFB_PLAN_REGIME_BLOCK") or "weak,stress,drawdown,crash_day"
                                         ).split(",") if x.strip().lower() in PLAN_LABELS and x.strip().lower() != "calm"]
    ccy = [c.strip().upper() for c in (os.environ.get("TFB_PLAN_REGIME_CCY") or "USD").split(",") if c.strip()]
    return {"mode": mode if mode in ("off", "observe", "enforce") else "off", "block": block, "ccy": ccy}


def live_rule_word(r: Dict[str, Any], rule: Dict[str, Any]) -> str:
    us = text(r.get("us_label"))
    if rule["mode"] == "off":
        return "off"
    if not us:
        return "US not read: does not hold it"
    if text(r.get("currency")).upper() in rule["ccy"] and us in rule["block"]:
        return "holds it (US %s)" % us if rule["mode"] == "enforce" else "observe: would hold it (US %s)" % us
    return "does not hold it"


def switch_for(symbol: str, switch: Sequence[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    """The first SWITCH CANDIDATE row that would bring `symbol` in, else None."""
    for r in switch:
        if text(r.get("verdict")) == "SWITCH CANDIDATE" and text(r.get("best_symbol")).upper() == symbol.upper():
            return r
    return None


def waiting_text(r: Dict[str, Any], final: Dict[str, Tuple[str, str]], switch: Sequence[Dict[str, Any]]) -> str:
    """What the name is waiting for. The Today Action Plan's decision comes first and is never contradicted (Codex P2,
    #1046 and #1048): a funded BUY waits for nothing, a HOLD or SELL is held, 'cash' is said only when that decision
    itself reports a funding shortfall, and any other WATCH or DO NOT INVEST keeps the plan's own reason. The market
    rule and the switch check, both observe only, follow it in brackets. Only a name the plan has not picked waits for
    them: a calmer market, a reading of its market, or a switch."""
    sym = text(r.get("symbol"))
    state = text(r.get("market_state"))
    dec, do = final.get(sym.upper(), ("", ""))
    unfunded = "not funded" in do.lower()
    calmer = "a calmer market: %s is %s (%s)" % (text(r.get("market")), text(r.get("market_label")),
                                                  text(r.get("market_waiting_for")) or "see section 1")
    unread = "a reading of its market: %s is not read yet" % (text(r.get("market_country")) or "its market")
    sw = switch_for(sym, switch)
    figures = (text(sw.get("symbol")), text(sw.get("horizon")), num(sw.get("net_pp")) or 0.0,
               int(num(sw.get("chance_pct")) or 0)) if sw else ()
    swap = "it would replace %s over %s, net %+.2f pp after both trades, chance %d %%, the owner's review" % figures \
        if sw else ""
    if not dec:                                       # not picked by the plan: the observe-only rules are its wait
        if state == "hold":
            first = calmer
        elif state != "open":
            first = unread
        elif sw:
            return ("a switch: it would replace %s over %s (net %+.2f pp after both trades, chance %d %%) - the "
                    "owner's review" % figures)
        else:
            return ("a pick of today's plan: it is not on Final_Decisions (the plan has not picked it, so no cash is "
                    "set for it)")
        return first + (" (the switch check, observe only: %s)" % swap if swap else "")
    notes = []
    if state == "hold":
        notes.append("the market rule, observe only, would wait for " + calmer)
    elif state != "open":
        notes.append("the market rule, observe only, waits for " + unread)
    if swap:
        notes.append("the switch check, observe only: " + swap)
    tail = " (%s)" % "; ".join(notes) if notes else ""
    if dec == "BUY" and not unfunded:
        return "nothing%s: the Today Action Plan says BUY - %s%s" % (
            " today" if state == "hold" else "", do or "see Final_Decisions", tail)
    if dec in ("HOLD", "SELL"):
        return "nothing new: it is held - the Today Action Plan says %s - %s%s" % (dec, do or "see Final_Decisions", tail)
    if unfunded:
        return "cash: %s%s" % (do, tail)
    return "the Today Action Plan's own reason: %s - %s%s" % (dec, do or "see Final_Decisions", tail)


def ready_row(r: Dict[str, Any], final: Dict[str, Tuple[str, str]], switch: Sequence[Dict[str, Any]],
              rule: Dict[str, Any]) -> List[Any]:
    rank = int(num(r.get("ready_rank")) or 0)
    state = text(r.get("market_state"))
    dec, do = final.get(text(r.get("symbol")).upper(), ("", ""))
    return ["READY %d" % rank if flag(r.get("in_ready")) else rank, text(r.get("symbol")), text(r.get("name")),
            text(r.get("market")) or "not read (%s)" % (text(r.get("market_country")) or "unknown"),
            {"open": "OPEN", "hold": "HOLD (%s)" % text(r.get("market_label"))}.get(state, "NOT READ"),
            {"open": "does not hold it", "hold": "holds it"}.get(state, "cannot read it"),
            live_rule_word(r, rule), waiting_text(r, final, switch),
            ("%s - %s" % (dec, do) if do else dec) if dec else "not on Final_Decisions",
            r2(r.get("expected_roi_1m_pct")), r2(r.get("overall_score")), text(r.get("currency")),
            text(r.get("source_page"))]


def switch_rows(switch: Sequence[Dict[str, Any]]) -> List[List[Any]]:
    """One row per holding: its SWITCH CANDIDATE row when there is one, else its 1-month row."""
    by: Dict[str, List[Dict[str, Any]]] = {}
    for r in switch:
        by.setdefault(text(r.get("symbol")), []).append(r)
    out = []
    for sym in sorted(by):
        rows = sorted(by[sym], key=lambda x: num(x.get("horizon_order")) or 9)
        pick = ([r for r in rows if text(r.get("verdict")) == "SWITCH CANDIDATE"]
                or [r for r in rows if text(r.get("horizon")) == "1M"] or rows)[0]
        out.append([sym, text(pick.get("name")), text(pick.get("verdict")), text(pick.get("horizon")),
                    r2(pick.get("holding_forecast_pp")), text(pick.get("best_symbol")) or "none",
                    r2(pick.get("best_forecast_pp")), r2(pick.get("net_pp")), r2(pick.get("chance_pct")),
                    text(pick.get("reason"))])
    return out


def trial_rows(trial: Sequence[Dict[str, Any]]) -> List[List[Any]]:
    return [[text(r.get("period")), text(r.get("horizon")), text(r.get("grp")), int(num(r.get("n")) or 0),
             int(num(r.get("days")) or 0), r2(r.get("mean_ret_pp")), r2(r.get("mean_excess_pp")), r2(r.get("up_pct")),
             r2(r.get("p5_ret_pp"))] for r in trial]


def _versus(x: float, y: float, worse: str, better: str) -> str:
    return worse if x < y else better if x > y else "no difference"


def trial_reading(trial: Sequence[Dict[str, Any]], period: str) -> str:
    """One sentence for `period`, over one week: the ready list's names against the other qualified names, then the
    names each rule held against the names it let through."""
    by = {(text(r.get("period")), text(r.get("horizon")), text(r.get("grp"))): r for r in trial}

    def pair(on: str, off: str) -> Optional[Tuple[float, float, int, int]]:
        a, b = by.get((period, "1W", on)), by.get((period, "1W", off))
        if not (a and b) or num(a.get("mean_ret_pp")) is None or num(b.get("mean_ret_pp")) is None:
            return None
        return (num(a.get("mean_ret_pp")), num(b.get("mean_ret_pp")), int(num(a.get("n")) or 0),
                int(num(b.get("n")) or 0))

    parts = []
    got = pair("ready list (top 3)", "other qualified")
    if got:
        x, y, n1, n2 = got
        parts.append("the ready list's names moved %+.2f %% in the next week against %+.2f %% for the other qualified "
                     "names (%d and %d name-days) - %s" % (x, y, n1, n2, _versus(x, y, "it picked the weaker ones",
                                                                                 "it picked the stronger ones")))
    for rule, on, off in (("the market rule", "market rule holds it", "market rule does not"),
                          ("the live US rule", "live US rule holds it", "live US rule does not")):
        got = pair(on, off)
        if got:
            x, y, n1, n2 = got
            parts.append("the names %s held moved %+.2f %% in the next week against %+.2f %% for the ones it let "
                         "through (%d and %d name-days) - %s" % (rule, x, y, n1, n2,
                                                                _versus(x, y, "it held the weaker ones",
                                                                        "it held the stronger ones")))
    if not parts:
        return ("READY LIST, %s: nothing has matured yet - a name is scored a week after the day it was listed."
                % period.upper())
    return "READY LIST, %s: %s." % (period.upper(), "; ".join(parts))


# ----------------------------------------------------------------------------- #
# 1.2.0: the events ahead, the last events and their scorecard (views 1.21.0)
# ----------------------------------------------------------------------------- #
def _at(v: Any) -> Optional[_dt.datetime]:
    """A BigQuery TIMESTAMP as CAST(... AS STRING) gives it ('2026-11-29 12:00:00+00'), read to the minute, UTC."""
    t = text(v)
    try:
        return _dt.datetime.strptime(t[:16], "%Y-%m-%d %H:%M").replace(tzinfo=UTC) if len(t) >= 16 else None
    except ValueError:
        return None


def _when(v: Any) -> str:
    t = _at(v)
    if t is None:
        return ""
    r = t.astimezone(RIYADH)
    return "%s %s" % (short_day(r.date(), year=False), r.strftime("%H:%M"))


def _pp(v: Any) -> str:
    x = num(v)
    return "" if x is None else "%+.2f" % x


def band_cell(r: Optional[Dict[str, Any]]) -> str:
    """The median event-day move of the earlier events, with the middle 80 % band: '+0.12 (-0.81 .. +0.95)'."""
    if not r or num(r.get("r1_p50")) is None:
        return ""
    return "%s (%s .. %s)" % (_pp(r.get("r1_p50")), _pp(r.get("r1_p10")), _pp(r.get("r1_p90")))


def by_event(rows: Sequence[Dict[str, Any]]) -> List[Tuple[Dict[str, Any], Dict[str, Dict[str, Any]]]]:
    """[(the event's first row, {market_id: row})], in the order the rows came."""
    out: List[Tuple[Dict[str, Any], Dict[str, Dict[str, Any]]]] = []
    seen: Dict[str, int] = {}
    for r in rows:
        eid = text(r.get("event_id"))
        if not eid:
            continue
        if eid not in seen:
            seen[eid] = len(out)
            out.append((r, {}))
        out[seen[eid]][1][text(r.get("market_id"))] = r
    return out


def event_name(r: Dict[str, Any]) -> str:
    period = text(r.get("period"))
    return text(r.get("family_name")) + (" - %s" % period if period else "")


def figure_chances(r: Dict[str, Any]) -> str:
    """The chances of the figure from the family's earlier figures: 'above 38 / in line 31 / below 31 (from 24)'."""
    n = int(num(r.get("n_fig")) or 0)
    if not n or num(r.get("p_above")) is None:
        return "no forecast to read" if text(r.get("kind")) == "meeting" and num(r.get("estimate")) is None else ""
    return "%d / %d / %d (from %d)" % (round(100 * num(r.get("p_above"))), round(100 * num(r.get("p_in_line"))),
                                       round(100 * num(r.get("p_below"))), n)


def after_figure(r: Optional[Dict[str, Any]]) -> str:
    """The US median event-day move after a figure above / below its forecast, with the count: '-0.41 (9) / +0.38 (7)';
    '' for a family with no figure on record (a meeting with no forecast)."""
    if not r or not (int(num(r.get("n_above")) or 0) or int(num(r.get("n_below")) or 0)):
        return ""
    parts = []
    for side in ("above", "below"):
        n = int(num(r.get("n_" + side)) or 0)
        parts.append("%s (%d)" % (_pp(r.get("r1_p50_" + side)), n) if n and num(r.get("r1_p50_" + side)) is not None
                     else "none")
    return " / ".join(parts)


def moved_most(markets: Dict[str, Dict[str, Any]]) -> str:
    best = max((r for r in markets.values() if num(r.get("mean_abs_z")) is not None),
               key=lambda r: (num(r.get("mean_abs_z")), -(num(r.get("market_order")) or 99)), default=None)
    return "" if best is None else "%s (%.1f normal days)" % (text(best.get("market")), num(best.get("mean_abs_z")))


def event_rows(ahead: Sequence[Dict[str, Any]]) -> List[List[Any]]:
    out = []
    for first, mk in by_event(ahead):
        us = mk.get("us")
        n_prior = max((int(num(r.get("n_prior")) or 0) for r in mk.values()), default=0)
        out.append([_when(first.get("event_at")), event_name(first), r2(first.get("estimate")),
                    r2(first.get("previous")), figure_chances(first), n_prior,
                    band_cell(us), after_figure(us), band_cell(mk.get("sa")), band_cell(mk.get("oil")),
                    band_cell(mk.get("gold")), band_cell(mk.get("bonds")),
                    "" if not us or num(us.get("p_big")) is None else round(100 * num(us.get("p_big"))),
                    moved_most(mk)])
    return out


def last_cell(r: Optional[Dict[str, Any]]) -> str:
    if not r or num(r.get("r1_pct")) is None:
        return ""
    hit = text(r.get("band_hit")).lower()
    return "%s (%s)" % (_pp(r.get("r1_pct")), "inside the band" if hit == "true" else
                        "OUTSIDE the band" if hit == "false" else "no band yet")


def last_rows(last: Sequence[Dict[str, Any]]) -> List[List[Any]]:
    out = []
    for first, mk in by_event(last):
        out.append([_when(first.get("event_at")), text(first.get("family_name")),
                    text(first.get("figure")).replace("_", " ") or "no forecast", r2(first.get("estimate")),
                    r2(first.get("actual")), "", last_cell(mk.get("us")), "", last_cell(mk.get("sa")),
                    last_cell(mk.get("oil")), last_cell(mk.get("gold")), last_cell(mk.get("bonds"))])
    return out


def event_trial_rows(summary: Sequence[Dict[str, Any]]) -> List[List[Any]]:
    out = []
    for r in summary:
        out.append([text(r.get("period")), text(r.get("family_name")), int(num(r.get("events")) or 0),
                    r2(r.get("band_pct")), int(num(r.get("band_n")) or 0), r2(r.get("big_pct")), r2(r.get("any_pct")),
                    r2(r.get("big_skill")), r2(r.get("dir_pct")), int(num(r.get("dir_events")) or 0),
                    r2(r.get("dir_z")), r2(r.get("drift_pct")), int(num(r.get("drift_events")) or 0),
                    r2(r.get("fig_skill")),
                    int(num(r.get("fig_n")) or 0), text(r.get("verdict"))])
    return out


def event_reading(summary: Sequence[Dict[str, Any]], period: str) -> str:
    """One sentence for `period` from its all-families line."""
    line = next((r for r in summary if text(r.get("period")) == period and text(r.get("family")) == "all"), None)
    n = int(num((line or {}).get("events")) or 0)
    unlogged = int(num((line or {}).get("not_logged")) or 0)
    late = ("; %d more came with no scenario logged before them and are not scored" % unlogged) if unlogged else ""
    if period == "forward" and n < REVIEW_SESSIONS:
        return ("EVENTS, FORWARD (from %s): %d event(s) scored so far%s - collecting; the owner reviews them with "
                "the rest after %d sessions." % (short_day(EVENT_FORWARD_FROM), n, late, REVIEW_SESSIONS))
    if not line:
        return "EVENTS, %s: no event scored yet." % period.upper()
    parts = []
    band, band_n = num(line.get("band_pct")), int(num(line.get("band_n")) or 0)
    if band is not None and band_n:
        parts.append("the band of the earlier moves held %.0f %% of %d event-market rows (a true band holds 80 %%: %s)"
                     % (band, band_n, "close" if 70 <= band <= 90 else "too narrow" if band < 70 else "too wide"))
    big, anyp, skill = num(line.get("big_pct")), num(line.get("any_pct")), num(line.get("big_skill"))
    if big is not None and anyp is not None and skill is not None:
        parts.append("a big move came on %.0f %% of the event days against %.0f %% of any days, and the family's "
                     "chance of one scored %+.2f against any day's (%s)" % (
                         big, anyp, skill, "it told more" if skill > 0 else "it told nothing more"))
    dn, de = int(num(line.get("dir_n")) or 0), int(num(line.get("dir_events")) or 0)
    if dn and num(line.get("dir_pct")) is not None:
        parts.append("the direction after a figure held %.0f %% of %d rows over %d events (z %+.2f, each event once)"
                     % (num(line.get("dir_pct")), dn, de, num(line.get("dir_z")) or 0.0))
    else:
        parts.append("no direction after a figure is scored yet")
    fn, fs = int(num(line.get("fig_n")) or 0), num(line.get("fig_skill"))
    if fn and fs is not None:
        parts.append("the figure chances scored %+.2f against equal thirds over %d figures" % (fs, fn))
    head = ("EVENTS, BACKTEST (the same rules read back on the archive, %d events)" % n if period == "backtest" else
            "EVENTS, FORWARD (from %s, %d events, each scored on the scenario logged before it%s)"
            % (short_day(EVENT_FORWARD_FROM), n, late))
    return "%s: %s." % (head, "; ".join(parts))


def next_event_line(ahead: Sequence[Dict[str, Any]]) -> Optional[Tuple[str, str]]:
    """(what and when, what its earlier events did to the US and oil) for the first event ahead, or None."""
    evs = by_event(ahead)
    if not evs:
        return None
    first, mk = evs[0]
    what = "%s, %s Riyadh" % (event_name(first), _when(first.get("event_at")))
    bits = []
    for m, word in (("us", "the US"), ("oil", "oil")):
        r = mk.get(m)
        if r and num(r.get("r1_p50")) is not None:
            bits.append("%s moved a median %s %% (middle 80 %%: %s .. %s)" % (
                word, _pp(r.get("r1_p50")), _pp(r.get("r1_p10")), _pp(r.get("r1_p90"))))
    n = max((int(num(r.get("n_prior")) or 0) for r in mk.values()), default=0)
    return what, ("on its %d earlier event(s) %s" % (n, " and ".join(bits)) if bits else
                  "no earlier event of its kind to read yet")


def _optional(session: Any, project: str, location: str, sql: str, query: Any) -> Optional[List[Dict[str, Any]]]:
    """The rows of a view of apply_bq_views 1.21.0, or None while it is not in BigQuery yet (the sections say so and
    the page is built all the same); any other refusal still fails the run."""
    try:
        return query(session, project, location, sql)
    except RuntimeError as e:
        if "Not found" not in text(e):
            raise
        _out("a view of apply_bq_views 1.21.0 is not in BigQuery yet: %s"
             % sql.split("FROM `", 1)[1].split("`", 1)[0].rsplit(".", 1)[-1])
        return None


def event_counts(ahead: Optional[Sequence[Dict[str, Any]]], last: Optional[Sequence[Dict[str, Any]]],
                 ev_summary: Optional[Sequence[Dict[str, Any]]]) -> str:
    if ahead is None or last is None or ev_summary is None:
        return "EVENTS: views 1.21.0 not in BigQuery yet"
    return ("EVENTS ahead (%d days): %d event(s) with a scenario | last %d days: %d | event scorecard lines: backtest "
            "%d, forward %d" % (EVENT_DAYS, len(by_event(ahead)), EVENT_BACK_DAYS, len(by_event(last)),
                                sum(1 for r in ev_summary if text(r.get("period")) == "backtest"),
                                sum(1 for r in ev_summary if text(r.get("period")) == "forward")))


# ----------------------------------------------------------------------------- #
# the page
# ----------------------------------------------------------------------------- #
HOW_TO = (
    "How to read: the condition is the US market regime's five labels, first match wins, read for every market: "
    "crash_day = the last close fell 2 k % or more; drawdown = it fell 3 k % or more over 5 closes, or stands 8 k % "
    "or more under its 250-close high; stress = its 20-close volatility is 25 k % or more (the US also: the VIX at "
    "25 or more); weak = it closed under its 50-close average; calm = none of these.",
    "k = the market's normal yearly volatility / 16 (16 % is the S&P 500's normal level, on which the US limits were "
    "set): a quiet bond fund and a wild oil price are held to the same standard against their own movement. The US "
    "keeps k = 1, exactly the Daily Plan's rule.",
    "State: OPEN = calm; HOLD = any other label - this page's market test, the one the market trial was fixed on "
    "(21 Nov); the VERDICT says it. The live buy rule is set apart from it and can differ: the LIVE RULE line names "
    "its hold labels and the markets it holds today. Trend score = the mean of the 20- and 60-close change, each in "
    "units of the market's normal move over that span; it orders the open markets first, then the held ones.",
    "Read on = the index or price the market is read on; Fund = what a buy would use (Saudi Arabia KSA, Europe VGK, "
    "Japan EWJ, China / Hong Kong MCHI, gold GLD, bonds IEF, sukuk SPSK, US REITs VNQ; the Saudi REITs are bought on "
    "Tadawul). A fund can move apart from its market (currency, make-up). Oil is a reading only.",
    "The forward test starts on %s. After %d sessions the owner reviews the scorecard; only what proved itself is "
    "switched on, and only with his approval. Until then this page controls no buy. Not investment advice."
    % (short_day(FORWARD_FROM), REVIEW_SESSIONS),
)


def page_rows(days: Sequence[Tuple[_dt.date, List[Dict[str, Any]]]], summary: Sequence[Dict[str, Any]],
              built: str, ready: Sequence[Dict[str, Any]] = (), final: Optional[Dict[str, Tuple[str, str]]] = None,
              switch: Sequence[Dict[str, Any]] = (), trial: Sequence[Dict[str, Any]] = (),
              rule: Optional[Dict[str, Any]] = None, events: Optional[Sequence[Dict[str, Any]]] = None,
              last: Optional[Sequence[Dict[str, Any]]] = None,
              ev_summary: Optional[Sequence[Dict[str, Any]]] = None) -> List[List[Any]]:
    """1.2.0: events / last / ev_summary are None while views 1.21.0 are not in BigQuery (the sections say so)."""
    def line(*cells: Any) -> List[Any]:
        return (list(cells) + [""] * WIDTH)[:WIDTH]

    rows: List[List[Any]] = [
        line("MARKETS BOARD - the condition of every market and their order by trend and risk - OBSERVE ONLY: it "
             "controls no buy"),
    ]
    if not days:
        rows += [line("Built %s | markets_board %s | no market could be read from v_market_rank: the views may not be "
                      "installed yet, or the archive is empty | only the Today Action Plan gives orders" % (
                          built, __version__)), line()]
    else:
        latest, today_rows = days[0]
        facts = [day_facts(d, r) for d, r in days]
        head, detail = verdict_lines(facts[0], today_rows, hold_streak(facts))
        us = facts[0]["us"]
        rows += [
            line("Built %s | markets_board %s | as of %s (each market's latest close, 6 days at most) | BigQuery "
                 "v_market_rank | only the Today Action Plan gives orders" % (built, __version__, short_day(latest))),
            line("VERDICT", head, detail),
            line("US", us, us_words(us, rule or plan_rule())),                               # 1.3.0
            line("LIVE RULE", *live_rule_line(today_rows, rule or plan_rule())),            # 1.3.0
        ]
        nxt = next_event_line(events or ())                                                     # 1.2.0
        if nxt:
            rows.append(line("NEXT EVENT", nxt[0], nxt[1] + " - section 7 has the next %d days" % EVENT_DAYS))
        rows += [
            line(),
            line("1. THE MARKETS - open first, then the held ones, each group by trend score"),
            list(HEAD),
        ]
        rows += [market_row(r) for r in today_rows]
        rows += missing_markets(today_rows, [r for _, rs in days for r in rs])
        rows += [line(), line("2. THE LAST %d DAYS" % SHOW_DAYS),
                 line("Day", "Open", "Verdict", "Open markets", "First in the order", "US condition", "Markets read")]
        for f in facts[:SHOW_DAYS]:
            rows.append(line(short_day(f["day"]), "%d of %d" % (f["open"], f["buyable"]), f["verdict"],
                             ", ".join(f["open_names"]) or "none", "%s (%s)" % (f["first"], f["first_state"]),
                             f["us"], f["read"]))
        rows.append(line())
    rows.append(line("3. THE SCORECARD - what each market's fund did after the reading (v_market_trial_summary; "
                     "the windows of consecutive days overlap: read the means, not the counts as independent trials)"))
    rows += [line(*r) for r in score_table(summary)]
    rows += [line(reading(p, summary)) for p in PERIODS]
    rows.append(line())
    # 1.1.0: the ready list, the switch check and the ready list's own scorecard
    final, rule = final or {}, rule or plan_rule()
    day_txt = short_day(day(ready[0].get("snapshot_date"))) if ready else "none"
    in_ready = sum(1 for r in ready if flag(r.get("in_ready")))
    rows.append(line("4. THE READY LIST - the %d best qualified names of %s from the top-ranked markets (READY 1-%d), "
                     "then every other qualified name in the same order (holdings are out: section 5 compares each); "
                     "the decision is the Today Action Plan's own"
                     % (in_ready, day_txt, in_ready)))
    rows.append(line("Market rule = the hold read market by market (this page, observe only). Live US rule = the "
                     "Daily Plan's rule as configured (%s; %s; currencies %s). Only the Today Action Plan gives orders."
                     % (rule["mode"], ", ".join(rule["block"]) or "no label", ", ".join(rule["ccy"]) or "none")))
    rows.append(list(READY_HEAD) + [""] * (WIDTH - len(READY_HEAD)))
    rows += [line(*ready_row(r, final, switch, rule)) for r in ready]
    if not ready:
        rows.append(line("", "no qualified name in the last 7 days (v_ready_list)"))
    rows += [line(), line("5. THE SWITCH CHECK - each holding against the best qualified name of its market "
                          "(v_hold_vs_switch: the forecast gap as worth what such a gap has been worth, both trades "
                          "charged; SWITCH CANDIDATE only when proven, positive and 60 % likely - never an order)"),
             list(SWITCH_HEAD) + [""] * (WIDTH - len(SWITCH_HEAD))]
    rows += [line(*r) for r in switch_rows(switch)] or [line("", "no holding read (v_hold_vs_switch)")]
    rows += [line(), line("6. THE READY LIST SCORECARD - what the qualified names did next (v_ready_trial: a week and "
                          "two weeks, local currency, against the day's median name)"),
             list(TRIAL_HEAD) + [""] * (WIDTH - len(TRIAL_HEAD))]
    rows += [line(*r) for r in trial_rows(trial)] or [line("", "nothing has matured yet")]
    rows += [line(trial_reading(trial, p)) for p in PERIODS]
    rows.append(line())
    # 1.2.0: the events ahead, the last events and their scorecard
    rows.append(line("7. EVENTS AHEAD - the Fed, ECB, Bank of Japan and OPEC+ meetings and the tracked releases of the "
                     "next %d days, each with what the earlier events of its kind did (v_event_scenarios: the median "
                     "move on the event day, the middle 80 %% of those moves in brackets, in %%; for information - no "
                     "order comes from this section)" % EVENT_DAYS))
    if events is None:
        rows.append(line("", "the event views are not in BigQuery yet: %s" % EVENT_VIEWS))
    else:
        rows.append(list(EVENT_HEAD) + [""] * (WIDTH - len(EVENT_HEAD)))
        rows += [line(*r) for r in event_rows(events)] or [
            line("", "no event with earlier ones of its kind in the next %d days" % EVENT_DAYS)]
        rows.append(line("Figure = the release against its forecast, read against the family's own earlier misses "
                         "(half of their usual size or more: above / below). US if above / if below = the median "
                         "event-day move of the US after such a figure (the count in brackets). Big-move chance = how "
                         "often the US moved a normal day's move or more on such days."))
    rows.append(line("THE LAST %d DAYS - each event's move against its band (the middle 80 %% of the earlier moves)"
                     % EVENT_BACK_DAYS))
    if last is None:
        rows.append(line("", "the event views are not in BigQuery yet: %s" % EVENT_VIEWS))
    else:
        rows.append(list(EVENT_LAST_HEAD) + [""] * (WIDTH - len(EVENT_LAST_HEAD)))
        rows += [line(*r) for r in last_rows(last)] or [line("", "no event in the last %d days" % EVENT_BACK_DAYS)]
    rows += [line(), line("8. THE EVENTS SCORECARD - each scenario against what came (v_event_trial_summary; a true "
                          "band holds 80 %%; skill above 0 = better than any day's chance, or than equal thirds for "
                          "the figure; z against a coin, each event once; forward from %s)"
                          % short_day(EVENT_FORWARD_FROM))]
    if ev_summary is None:
        rows.append(line("", "the event views are not in BigQuery yet: %s" % EVENT_VIEWS))
    else:
        rows.append(list(EVENT_TRIAL_HEAD) + [""] * (WIDTH - len(EVENT_TRIAL_HEAD)))
        rows += [line(*r) for r in event_trial_rows(ev_summary)] or [line("", "no event scored yet")]
        rows += [line(event_reading(ev_summary, p)) for p in PERIODS]
    rows.append(line())
    rows += [line(t) for t in HOW_TO]
    rows.append(line())
    return rows


def counts(days: Sequence[Tuple[_dt.date, List[Dict[str, Any]]]], summary: Sequence[Dict[str, Any]]) -> str:
    if not days:
        return "no market read"
    d, rows = days[0]
    f = day_facts(d, rows)
    by_label: Dict[str, int] = {}
    for r in rows:
        by_label[text(r.get("label"))] = by_label.get(text(r.get("label")), 0) + 1
    idx = score_index(summary)
    return ("as of %s: %d of %d market(s) read, %d of %d open by the market test (%s); labels %s; scorecard rows: "
            "backtest %d, forward %d"
            % (d.isoformat(), len(rows), len(MARKETS), f["open"], f["buyable"],
               f["verdict"].replace(" (%s)" % TEST_WORDS, "") or "none",
               ", ".join("%s %d" % kv for kv in sorted(by_label.items())),
               sum(1 for k in idx if k[0] == "backtest"), sum(1 for k in idx if k[0] == "forward")))


def ready_counts(ready: Sequence[Dict[str, Any]], switch: Sequence[Dict[str, Any]], trial: Sequence[Dict[str, Any]]) -> str:
    states: Dict[str, int] = {}
    for r in ready:
        states[text(r.get("market_state"))] = states.get(text(r.get("market_state")), 0) + 1
    holdings = len({text(r.get("symbol")) for r in switch})
    cands = len({text(r.get("symbol")) for r in switch if text(r.get("verdict")) == "SWITCH CANDIDATE"})
    return ("READY %d of %d qualified (%s) | switch check: %d holding(s), %d with a SWITCH CANDIDATE | ready scorecard "
            "rows: %d" % (sum(1 for r in ready if flag(r.get("in_ready"))), len(ready),
                          ", ".join("%s %d" % kv for kv in sorted(states.items())) or "none", holdings, cands, len(trial)))


def collect(session: Any, project: str, dataset: str, location: str, today: _dt.date, query: Any,
            now: Optional[_dt.datetime] = None) -> Dict[str, Any]:
    ds = "%s.%s" % (project, dataset)
    days = by_day(query(session, project, location, sql_rank(ds, today)))
    summary = query(session, project, location, sql_summary(ds))
    ready = query(session, project, location, sql_ready(ds, today))                      # 1.1.0
    switch = query(session, project, location, sql_switch(ds))
    trial = query(session, project, location, sql_ready_trial(ds))
    events = last = ev_summary = None                                                    # 1.2.0
    if now is not None:
        events = _optional(session, project, location, sql_events_ahead(ds, now), query)
        if events is not None:
            last = _optional(session, project, location, sql_events_last(ds, now), query)
            ev_summary = _optional(session, project, location, sql_events_summary(ds), query)
    return {"days": days, "summary": summary, "ready": ready, "switch": switch, "trial": trial, "events": events,
            "last": last, "ev_summary": ev_summary}


def run_once(dry_run: bool, book=None, session=None, now: Optional[_dt.datetime] = None, query=None) -> int:
    now = now or _dt.datetime.now(UTC)
    if not enabled():
        _out("TFB_MARKETS_BOARD is off: nothing read, nothing written")
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
        from scripts.daily_plan import _read as _raw_read  # noqa: PLC0415
        from scripts.sheets_quota import call as _quota_call  # noqa: PLC0415
        # 1.1.0: the Today Action Plan's own decision per stock, so the ready list never states another one
        final = read_final(_quota_call(lambda: _raw_read(book, FINAL_TAB, "A1:C400"), _SLEEP, _out,
                                       "reading %s" % FINAL_TAB))
        data = collect(session, project, dataset, nb._env("TFB_BQ_LOCATION", "US"), now.astimezone(RIYADH).date(), query,
                       now=now)
    except Exception as e:  # noqa: BLE001 - one tab; a failed read leaves the previous tab in place
        _out("ERROR: %s: %s; nothing written" % (type(e).__name__, text(e)[:300]))
        return 2
    _out("MARKETS %s" % counts(data["days"], data["summary"]))
    _out("%s | Final_Decisions: %d stock(s) read" % (ready_counts(data["ready"], data["switch"], data["trial"]), len(final)))
    _out(event_counts(data["events"], data["last"], data["ev_summary"]))                  # 1.2.0
    page = page_rows(data["days"], data["summary"], now.astimezone(RIYADH).strftime("%Y-%m-%d %H:%M Riyadh"),
                     data["ready"], final, data["switch"], data["trial"], plan_rule(), data["events"], data["last"],
                     data["ev_summary"])
    if dry_run:
        _out("dry run: %d row(s) computed, nothing written" % len(page))
        return 0
    try:
        from scripts.daily_plan import write_grid  # noqa: PLC0415
        # one full overwrite: safe to send again after a quota answer
        _quota_call(lambda: write_grid(book, TAB, page, WIDTH, 160), _SLEEP, _out, "writing %s" % TAB)
    except Exception as e:  # noqa: BLE001 - reported, with the exit code that marks the run failed
        _out("ERROR: %s: %s; the tab was not written" % (type(e).__name__, text(e)[:300]))
        return 2
    _out("wrote the %s tab (%d row(s))" % (TAB, len(page)))
    return 0


def main(argv: Optional[Sequence[str]] = None, **kw) -> int:
    ap = argparse.ArgumentParser(description="TFB Markets Board (the condition and the order of every market, observe only)")
    ap.add_argument("--dry-run", action="store_true")
    a = ap.parse_args(list(argv) if argv is not None else None)
    return run_once(a.dry_run, **kw)


if __name__ == "__main__":
    sys.exit(main())
