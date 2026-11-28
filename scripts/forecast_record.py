#!/usr/bin/env python3
"""
scripts/forecast_record.py
================================================================================
TFB Forecast Record - what TFB's buy calls delivered against what they promised, copied into the workbook
--------------------------------------------------------------------------------
v1.0.0 (2026-11-28)

WHY
    Owner, 28 Nov 2026 01:02 Riyadh, in Arabic: a return of at least 5 % a
    month on the capital, in all cases, or stop before the losses grow.
    01:30: "Try to make full review and engening restructured looking for
    better results". The pages show each name's promise - its forecast price
    over its price - and the record of what such promises delivered stood in
    BigQuery only (v_forecast_record, views 1.39.0): the US one-month buy
    calls promised +3.66 % and delivered +1.13 %, the Saudi ones promised
    +4.64 % and delivered -4.37 %, and what was delivered rose with the size
    of the promise in the US (the lowest third -1.28 %, the highest +3.26 %).
    The live system never reads BigQuery, so this copies the record into the
    workbook, where the Daily Plan (1.12.0, TFB_PLAN_HONEST observe) reads it
    once per cut and labels every name with its honest forecast.

WHAT IT WRITES (its tab Forecast_Record; nothing else)
    Four lines on how to read it, then one table: every row of
    v_forecast_record - per market, horizon (1M, 3M, 1W, 2W), band (all, and
    the thirds low / mid / high of the market's promises, cut once on the
    calls scored by 27 Nov 2026) and period (all calls; forward = calls
    recorded from 29 Nov 2026) - the calls, their mean promise and result,
    the share of the promise, how often they rose and how often they beat
    their market's own index. The markets with the most calls come first.
    The table names markets only: no symbol, amount or holding.

SAFETY
    Read-only on BigQuery (one SELECT, through the Evidence Board's reader)
    and on every tab but its own. The run log gives counts only.
    TFB_FORECAST_RECORD = on (default) | off. --dry-run prints counts and
    writes nothing. A failed query, an empty record or a failed write leaves
    the previous tab in place (exit 2). A Sheets quota answer (HTTP 429) is
    waited out and tried again. A record of past calls: no decision, score,
    list or order changes; only the Today Action Plan gives orders.

ENV
    DEFAULT_SPREADSHEET_ID, GOOGLE_SHEETS_CREDENTIALS, TFB_BQ_PROJECT,
    TFB_BQ_DATASET (default tfb_archive), TFB_BQ_LOCATION (default US),
    TFB_FORECAST_RECORD.
Exit: 0 written (or computed in a dry run, or switched off), 2 the query, the record or the write failed.

Not a licensed financial adviser; the owner decides and executes every trade.
================================================================================
"""
from __future__ import annotations

import argparse
import datetime as _dt
import os
import sys
import time
from typing import Any, Dict, List, Optional, Sequence

from scripts import news_board as nb

__version__ = "1.0.0"
_SLEEP = time.sleep                                     # the wait after a Sheets quota answer

UTC = _dt.timezone.utc
RIYADH = _dt.timezone(_dt.timedelta(hours=3))
TAB = "Forecast_Record"
VIEW = "v_forecast_record"
MAX_ROWS = 390                                          # the Daily Plan reads A1:Z400 of the tab
# (header, view column, kind): kind n = number, i = whole number, t = text
COLUMNS = [
    ("Market", "market", "t"), ("Horizon", "horizon", "t"), ("Band", "band", "t"), ("Period", "period", "t"),
    ("Low To %", "low_to_pp", "n"), ("Mid To %", "mid_to_pp", "n"),
    ("Promise From %", "promise_from_pp", "n"), ("Promise To %", "promise_to_pp", "n"),
    ("Calls", "calls", "i"), ("Symbols", "symbols", "i"), ("Days", "days", "i"),
    ("First Call", "first_call", "t"), ("Last Call", "last_call", "t"), ("Scored Through", "scored_through", "t"),
    ("Promised %", "promised_pp", "n"), ("Delivered %", "delivered_pp", "n"), ("Share Of Promise", "share_of_promise", "n"),
    ("Rose %", "rose_pct", "n"), ("Index", "index_symbol", "t"), ("Index %", "index_pp", "n"),
    ("Excess %", "excess_pp", "n"), ("Beat %", "beat_pct", "n"), ("Reading", "reading", "t"),
    ("Suffixes", "suffixes", "t"),
]
HEAD = [h for h, _, _ in COLUMNS]
WIDTH = len(HEAD)
HORIZON_ORDER = {"1M": 0, "3M": 1, "1W": 2, "2W": 3}
BAND_ORDER = {"all": 0, "low": 1, "mid": 2, "high": 3}
PERIOD_ORDER = {"all": 0, "forward": 1}


def _out(msg: str) -> None:
    print("[forecast_record %s] %s" % (__version__, msg), flush=True)


def enabled() -> bool:
    return (os.environ.get("TFB_FORECAST_RECORD") or "on").strip().lower() not in ("off", "0", "false", "no")


text, num = nb.text, nb.num


# ----------------------------------------------------------------------------- #
# The record (pure)
# ----------------------------------------------------------------------------- #
def cell_value(v: Any, kind: str) -> Any:
    """A view value as the tab keeps it: a number, a whole number or text; '' when there is none."""
    if kind == "t":
        return text(v)
    x = num(v)
    if x is None:
        return ""
    return int(round(x)) if kind == "i" else x


def order_rows(rows: Sequence[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """The markets with the most calls first (their 'all' rows of every horizon), then horizon 1M, 3M, 1W, 2W, the
    band all, low, mid, high and the period all, forward."""
    size: Dict[str, float] = {}
    for r in rows:
        if text(r.get("band")) == "all" and text(r.get("period")) == "all":
            size[text(r.get("market"))] = size.get(text(r.get("market")), 0.0) + (num(r.get("calls")) or 0.0)
    return sorted(rows, key=lambda r: (-size.get(text(r.get("market")), 0.0), text(r.get("market")),
                                       HORIZON_ORDER.get(text(r.get("horizon")), 9),
                                       BAND_ORDER.get(text(r.get("band")), 9), PERIOD_ORDER.get(text(r.get("period")), 9)))


def summary(rows: Sequence[Dict[str, Any]]) -> Dict[str, Any]:
    """Counts for the run log and the tab's second line: rows, markets, the last scored outcome day."""
    return {"rows": len(rows), "markets": len({text(r.get("market")) for r in rows}),
            "forward": sum(1 for r in rows if text(r.get("period")) == "forward"),
            "through": max((text(r.get("scored_through")) for r in rows), default="")}


def page_rows(rows: Sequence[Dict[str, Any]], stamp: str) -> List[List[Any]]:
    s = summary(rows)
    page: List[List[Any]] = [
        ["FORECAST RECORD - what TFB's buy calls delivered against what they promised. A RECORD OF PAST CALLS: not a "
         "decision, not an order, not a promise."],
        ["Built %s Riyadh | forecast_record %s | %s | %d market(s), calls scored through %s | the Daily Plan reads this "
         "table for each name's honest forecast (TFB_PLAN_HONEST)" % (stamp, __version__, VIEW, s["markets"],
                                                                      s["through"] or "-")],
        ["How to read: Promised % is the page's forecast at the call; Delivered % what the call made to its horizon "
         "(total return); Rose % how often it rose; Beat % how often it beat its market's own index over the same "
         "dates (Index %). Band: the market's calls cut once into thirds by their promise, on the calls scored by "
         "27 Nov 2026 (low up to Low To %, mid up to Mid To %, high above), or all. Period forward: the calls recorded "
         "from 29 Nov 2026, which did not shape the thirds."],
        ["Reading (fixed 28 Nov 2026): 'too few calls' under 30 calls or 10 call days; 'lost money' when Delivered % is "
         "0 or below; 'beat its index' when the mean excess is above 0 and half or more beat it; else 'made money, "
         "behind its index'. Calls of neighbouring days overlap, so a row holds fewer independent results than calls."],
        [""],
        list(HEAD),
    ]
    for r in order_rows(rows)[:MAX_ROWS]:
        page.append([cell_value(r.get(col), kind) for _, col, kind in COLUMNS])
    page.append([""])
    page.append(["Source: BigQuery %s (views 1.39.0) over v_outcome_engine - the INVESTABLE buy calls scored from the "
                 "archived daily bars. Not investment advice." % VIEW])
    return page


def sql_record(ds: str) -> str:
    return "SELECT %s FROM `%s.%s`" % (", ".join(col for _, col, _ in COLUMNS), ds, VIEW)


# ----------------------------------------------------------------------------- #
# The run
# ----------------------------------------------------------------------------- #
def run_once(dry_run: bool, book=None, session=None, now: Optional[_dt.datetime] = None, query=None) -> int:
    now = now or _dt.datetime.now(UTC)
    if not enabled():
        _out("TFB_FORECAST_RECORD is off: nothing read, nothing written")
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
        rows = query(session, project, nb._env("TFB_BQ_LOCATION", "US"), sql_record("%s.%s" % (project, dataset)))
    except Exception as e:  # noqa: BLE001 - one tab; a failed query leaves the previous tab in place
        _out("ERROR: %s: %s; nothing written" % (type(e).__name__, text(e)[:300]))
        return 2
    s = summary(rows)
    if not rows:
        _out("ERROR: %s returned no row; the previous tab is left in place" % VIEW)
        return 2
    _out("RECORD %d row(s), %d market(s), %d forward row(s), calls scored through %s" % (
        s["rows"], s["markets"], s["forward"], s["through"] or "-"))
    page = page_rows(rows, now.astimezone(RIYADH).strftime("%Y-%m-%d %H:%M"))
    if dry_run:
        _out("dry run: %d line(s) computed, nothing written" % len(page))
        return 0
    try:
        if book is None:
            from scripts.run_calendar_sync import _open_book  # noqa: PLC0415
            book = _open_book()
        from scripts.daily_plan import write_grid  # noqa: PLC0415
        from scripts.sheets_quota import call as _quota_call  # noqa: PLC0415
        # one full overwrite: safe to send again after a quota answer
        _quota_call(lambda: write_grid(book, TAB, page, WIDTH, 60), _SLEEP, _out, "writing %s" % TAB)
    except Exception as e:  # noqa: BLE001 - reported, with the exit code that marks the run failed
        _out("ERROR: %s: %s; the tab was not written" % (type(e).__name__, text(e)[:300]))
        return 2
    _out("wrote the %s tab (%d row(s))" % (TAB, min(len(rows), MAX_ROWS)))
    return 0


def main(argv: Optional[Sequence[str]] = None, **kw) -> int:
    ap = argparse.ArgumentParser(description="TFB Forecast Record (what the buy calls delivered, copied into the workbook)")
    ap.add_argument("--dry-run", action="store_true")
    a = ap.parse_args(list(argv) if argv is not None else None)
    return run_once(a.dry_run, **kw)


if __name__ == "__main__":
    sys.exit(main())
