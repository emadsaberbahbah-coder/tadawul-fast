#!/usr/bin/env python3
"""
scripts/markets_board.py
================================================================================
TFB Markets Board - the condition of every market, their order, and the score
--------------------------------------------------------------------------------
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
    it reads no tab and writes only Markets_Board. Observe only: no decision,
    gate, list or order reads this page; the Daily Plan keeps its US rule;
    only the Today Action Plan gives orders. Nothing of the owner's account or
    holdings is read, written or printed. TFB_MARKETS_BOARD = on (default) |
    off. --dry-run prints the counts and writes nothing. A failed query or
    write leaves the previous tab in place (exit 2). A Sheets quota answer
    (HTTP 429) is waited out and tried again.

ENV
    DEFAULT_SPREADSHEET_ID, GOOGLE_SHEETS_CREDENTIALS, TFB_BQ_PROJECT,
    TFB_BQ_DATASET (default tfb_archive), TFB_BQ_LOCATION (default US),
    TFB_MARKETS_BOARD.
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

__version__ = "1.0.0"
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


def day_facts(d: _dt.date, rows: Sequence[Dict[str, Any]]) -> Dict[str, Any]:
    first = rows[0] if rows else {}
    us = [r for r in rows if text(r.get("market_id")) == "us"]
    opened = [r for r in rows if text(r.get("state")) == "open" and flag(r.get("buyable"))]
    return {"day": d, "open": int(num(first.get("open_markets")) or 0),
            "buyable": int(num(first.get("buyable_markets")) or 0), "verdict": text(first.get("verdict")),
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
        return ("ALL ON HOLD - cash stays cash",
                "none of the %d markets with a fund is calm (oil is a reading only)%s" % (
                    f["buyable"], "; every market on hold for " + how_long if how_long else ""))
    first = [r for r in rows if text(r.get("state")) == "open" and flag(r.get("buyable"))][0]
    return ("%d of %d OPEN" % (f["open"], f["buyable"]),
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
    "State: OPEN = calm; HOLD = any other label - the Daily Plan holds a new US buy unless the US is calm, and this "
    "page reads the same test market by market. Trend score = the mean of the 20- and 60-close change, each in units "
    "of the market's normal move over that span; it orders the open markets first, then the held ones.",
    "Read on = the index or price the market is read on; Fund = what a buy would use (Saudi Arabia KSA, Europe VGK, "
    "Japan EWJ, China / Hong Kong MCHI, gold GLD, bonds IEF, sukuk SPSK, US REITs VNQ; the Saudi REITs are bought on "
    "Tadawul). A fund can move apart from its market (currency, make-up). Oil is a reading only.",
    "The forward test starts on %s. After %d sessions the owner reviews the scorecard; only what proved itself is "
    "switched on, and only with his approval. Until then this page controls no buy. Not investment advice."
    % (short_day(FORWARD_FROM), REVIEW_SESSIONS),
)


def page_rows(days: Sequence[Tuple[_dt.date, List[Dict[str, Any]]]], summary: Sequence[Dict[str, Any]],
              built: str) -> List[List[Any]]:
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
            line("US", us, "the Daily Plan holds a new US buy unless the US is calm (its own rule, unchanged)"),
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
    return ("as of %s: %d of %d market(s) read, %d of %d open (%s); labels %s; scorecard rows: backtest %d, forward %d"
            % (d.isoformat(), len(rows), len(MARKETS), f["open"], f["buyable"], f["verdict"] or "none",
               ", ".join("%s %d" % kv for kv in sorted(by_label.items())),
               sum(1 for k in idx if k[0] == "backtest"), sum(1 for k in idx if k[0] == "forward")))


def collect(session: Any, project: str, dataset: str, location: str, today: _dt.date, query: Any) -> Dict[str, Any]:
    ds = "%s.%s" % (project, dataset)
    days = by_day(query(session, project, location, sql_rank(ds, today)))
    summary = query(session, project, location, sql_summary(ds))
    return {"days": days, "summary": summary}


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
        if session is None:
            from scripts.archive_to_bigquery import make_session  # noqa: PLC0415
            session = make_session()
        data = collect(session, project, dataset, nb._env("TFB_BQ_LOCATION", "US"), now.astimezone(RIYADH).date(), query)
    except Exception as e:  # noqa: BLE001 - one tab; a failed read leaves the previous tab in place
        _out("ERROR: %s: %s; nothing written" % (type(e).__name__, text(e)[:300]))
        return 2
    _out("MARKETS %s" % counts(data["days"], data["summary"]))
    page = page_rows(data["days"], data["summary"], now.astimezone(RIYADH).strftime("%Y-%m-%d %H:%M Riyadh"))
    if dry_run:
        _out("dry run: %d row(s) computed, nothing written" % len(page))
        return 0
    try:
        if book is None:
            from scripts.run_calendar_sync import _open_book  # noqa: PLC0415
            book = _open_book()
        from scripts.daily_plan import write_grid  # noqa: PLC0415
        from scripts.sheets_quota import call as _quota_call  # noqa: PLC0415
        # one full overwrite: safe to send again after a quota answer
        _quota_call(lambda: write_grid(book, TAB, page, WIDTH, 90), _SLEEP, _out, "writing %s" % TAB)
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
