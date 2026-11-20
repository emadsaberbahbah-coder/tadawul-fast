#!/usr/bin/env python3
"""
scripts/ibkr_history.py - the broker's own return and trade record, for Portfolio_Decision_v2
================================================================================
VERSION 1.0.0 (2026-11-21)

Review (Codex, PR 1041): (P2) a trip in another currency with no USD
conversion in the fills is left out of the record and named in the stamp,
never written in its own currency into realized_usd, which the page sums as
USD. (P2) the record and its as_of come only from files that carry a trades
read: a file with only the performance series updates the return, and the
record keeps what it holds. Rows of a hand-filled record (no as_of) that close
before the first fill are carried forward, marked in column G with the closing
day they were given, so a first run without the older fills cannot drop them
and a later year cannot misread them. Only consecutive fills of one order are
joined: a fill on the other side in between starts a new execution.

WHY (owner, 20 Nov 2026 22:40 Riyadh, in Arabic: a full review of the file
against the market - the main differences between the pages, and between the
file and the market; 23:34: "Keep review the file for coming 3 hours and give
me next update and improvements")
    Portfolio_Decision_v2 reads the account's time-weighted return from
    _IBKR_NAV_History and its record of closed trades from _IBKR_Trades. Both
    tabs were filled once, by hand, from a connector read of 17 Oct, and never
    again (consistency_check C15: STALE, 34 days). On 20 Nov the page showed a
    return of -0.1% since 11 Aug and 3 wins in 15 closed trades; IBKR's own
    figures were -4.7% and 5 wins in 22.

WHAT
    A daily read-only scheduled task (configured outside the repository, like
    the hourly capture) writes 'TFB_IBKR_History_<date>-<time>.json' (schema
    tfb.ibkr-history/v1: IBKR's performance series and its trades, copied
    exactly as returned) to the capture folder. This script reads the newest
    MAX_FILES of them and writes three tabs:
      _IBKR_NAV_History  date | twr_cum_since_inception: IBKR's cumulative TWR
                         from TFB_IBKR_NAV_FROM (2026-08-11, the day the tab
                         has always started on). The tab's rows up to the
                         first day IBKR's series shares with it are kept; every
                         later day is IBKR's, rebased onto that day, so the line
                         carries on across IBKR's period resets (YTD restarts
                         on 1 Jan) and IBKR's revisions of recent days flow in.
      _IBKR_Fills        every stock fill IBKR reported (trade id, time,
                         symbol, side, size, price, order type, commission,
                         realized P&L), merged by trade id: the tab keeps the
                         fills older than the task's 90-day read.
      _IBKR_Trades       symbol | name | ccy | exit_type | span | realized_usd,
                         one row per closed round trip (flat, holding, flat
                         again), rebuilt from _IBKR_Fills: IBKR's realized P&L
                         summed over the trip (IBKR's figure, after its
                         commissions); a trip in another currency converted at
                         the USD.<ccy> conversion IBKR reported nearest to its
                         close (kept in its own currency when there is none);
                         the exit type from the closing order (stop, limit TP,
                         limit exit, market exit). A trip is matched on its
                         symbol: one whose ticker changed between the buy and
                         the sale shows an unknown start ("?"). H1:I4 carry
                         as_of, source, fills and written, which
                         consistency_check C15 reads.
    The fills of one order (same symbol, side and order id) count as one
    execution, so an order filled in pieces is one sale.
    Checked on 21 Nov against IBKR's own trades since 28 Jul: the 15 trips the
    hand-filled tab held come out with the same realized figure, span and exit
    type (the one SGD trip within 0.8 USD: the tab had used another rate), and
    the 7 trips closed since 8 Oct are added. With the 90-day read alone, the
    record still holds the 22 trips: the 5 hand-filled rows that close before
    the first fill are carried, and 4 trips show an unknown start.
    The page reads the three tabs as before. _IBKR_Account and _IBKR_Orders
    are left alone: the hourly capture supersedes them on the page.

SAFETY
    Read-only on Drive (drive.readonly, the service account the workflows
    already use). This script never calls the broker. It writes only its
    three tabs. A file that is unreadable, of another schema, dated in the
    future or whose captured_at differs from Drive's creation time by more
    than 10 minutes is not used; with no usable file nothing is written (exit
    0 with the reason; the tabs keep what they hold). A Drive read that fails
    (an error answer, a raised call, or no file downloadable) is exit 2, so a
    lost permission shows as a failed run. The run log carries
    counts and dates only: no amount, quantity, symbol or account number.
    Reporting only: no decision, score, list or order changes.

ENV
    TFB_IBKR_HISTORY (on | off), TFB_IBKR_CAPTURE_FOLDER_ID, TFB_IBKR_NAV_FROM
    (default 2026-08-11), DEFAULT_SPREADSHEET_ID, GOOGLE_SHEETS_CREDENTIALS.
Exit: 0 written (or nothing usable to write, switched off, or a dry run), 2 a read or a write failed.

Not a licensed financial adviser; the owner decides and executes every trade.
================================================================================
"""
from __future__ import annotations

import argparse
import datetime as _dt
import json
import re
import sys
import time
from typing import Any, Dict, List, Optional, Sequence, Tuple

from scripts import ibkr_capture as ic

__version__ = "1.0.0"
_SLEEP = time.sleep                                     # the wait after a Sheets quota answer

UTC = _dt.timezone.utc
RIYADH = _dt.timezone(_dt.timedelta(hours=3))
SCHEMA = "tfb.ibkr-history/v1"
PREFIX = "TFB_IBKR_History_"
MAX_BYTES = 2000000
MAX_FILES = 10                                          # the newest history files whose fills are merged
MAX_CLOCK_GAP_S = 600                                   # captured_at against Drive's creation time
FUTURE_SLACK_S = 300
EPS = 1e-6
NAV_TAB, FILLS_TAB, TRADES_TAB = "_IBKR_NAV_History", "_IBKR_Fills", "_IBKR_Trades"
NAV_HEAD = ["date", "twr_cum_since_inception"]
FILL_HEAD = ["trade_id", "time_utc", "symbol", "name", "ccy", "sec_type", "side", "size", "price", "order_type",
             "commission", "realized_pnl", "order_id"]
TRADE_HEAD = ["symbol", "name", "ccy", "exit_type", "span", "realized_usd"]
TRADE_WIDTH = 9                                         # A:F the record, H:I the stamp C15 reads
DEFAULT_NAV_FROM = "2026-08-11"
_DAY8 = re.compile(r"^(\d{4})(\d{2})(\d{2})$")
_DAY10 = re.compile(r"^(\d{4})-(\d{2})-(\d{2})")
_FX_SYM = re.compile(r"^USD\.([A-Z]{3})$")
_LEGAL = re.compile(r"(\s*[-/](ADR|A|B|IN|VA)|\s+(INC|CORP|CORPORATION|CO|CRP|LP|LTD|PLC|NV|SA|AG|SE))+\.?$", re.I)


def _out(msg: str) -> None:
    print("[ibkr_history %s] %s" % (__version__, msg), flush=True)


def enabled() -> bool:
    return ic._env("TFB_IBKR_HISTORY", "on").lower() not in ("off", "0", "false", "no")


def nav_from() -> _dt.date:
    d = day_of(ic._env("TFB_IBKR_NAV_FROM", DEFAULT_NAV_FROM))
    return d or _dt.date.fromisoformat(DEFAULT_NAV_FROM)


def day_of(v: Any) -> Optional[_dt.date]:
    """'20261120', '2026-11-20', '2026-11-20 00:00:00' or a Sheets day serial as a date; None otherwise."""
    if isinstance(v, _dt.datetime):
        return v.date()
    if isinstance(v, _dt.date):
        return v
    if isinstance(v, (int, float)) and not isinstance(v, bool) and 20000 <= v <= 80000:
        return _dt.date(1899, 12, 30) + _dt.timedelta(days=int(v))
    s = str(v or "").strip()
    m = _DAY8.match(s) or _DAY10.match(s)
    if not m:
        return None
    try:
        return _dt.date(int(m.group(1)), int(m.group(2)), int(m.group(3)))
    except ValueError:
        return None


# --------------------------------------------------------------------------- the file
def nav_series(perf: Any) -> List[Tuple[_dt.date, float]]:
    """IBKR's longest daily series of one account: [(day, cumulative return from the series' own start)]. PURE."""
    best: List[Tuple[_dt.date, float]] = []
    accounts = (perf or {}).get("accounts") if isinstance(perf, dict) else None
    for acct in (accounts.values() if isinstance(accounts, dict) else []):
        periods = (acct or {}).get("periods") if isinstance(acct, dict) else None
        for p in (periods.values() if isinstance(periods, dict) else []):
            if not isinstance(p, dict):
                continue
            dates, cps = p.get("dates") or [], p.get("cps") or []
            if not isinstance(dates, list) or not isinstance(cps, list) or len(dates) != len(cps):
                continue
            rows: Dict[_dt.date, float] = {}
            for d, c in zip(dates, cps):
                day, x = day_of(d), ic._num(c)
                if day is not None and x is not None and x > -1.0:
                    rows[day] = x
            if len(rows) > len(best):
                best = sorted(rows.items())
    return best


def fill(t: Any) -> Optional[Dict[str, Any]]:
    """One IBKR trade as a fill; None when it lacks an id, a time, a side, a size or a price. PURE."""
    if not isinstance(t, dict):
        return None
    tid = str(t.get("trade_id") or "").strip()
    at = ic._time(t.get("trade_time"))
    side = str(t.get("side") or "").strip().upper()
    size, price = ic._num(t.get("size")), ic._num(t.get("price"))
    if not tid or at is None or side not in ("BUY", "SELL") or size is None or size <= 0 or price is None:
        return None
    return {"trade_id": tid, "time": at.astimezone(UTC), "symbol": str(t.get("symbol") or "").strip().upper(),
            "name": str(t.get("company_name") or "").strip(), "ccy": str(t.get("currency") or "").strip().upper(),
            "sec_type": str(t.get("sec_type") or "").strip().upper(), "side": side, "size": size, "price": price,
            "order_type": str(t.get("order_type") or "").strip().upper(),
            "commission": ic._num(t.get("commission")) or 0.0, "realized": ic._num(t.get("realized_pnl")) or 0.0,
            "order_id": str(t.get("order_id") or "").strip()}


def fills_of(body: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Every fill of every 'trades*' read in the file (one per trade id). PURE."""
    out: Dict[str, Dict[str, Any]] = {}
    for k, v in body.items():
        if not str(k).startswith("trades") or not isinstance(v, dict):
            continue
        for t in v.get("trades") or []:
            f = fill(t)
            if f:
                out[f["trade_id"]] = f
    return list(out.values())


def parse(body: Any, created: Optional[_dt.datetime], now: _dt.datetime, name: str = "") -> Dict[str, Any]:
    """One history file as this script uses it. PURE.
    {"ok", "reason", "name", "as_of", "age_h", "series": [(day, cps)], "fills": [fill], "has_trades": a trades
    read is in the file (an empty list of trades is still a read)}"""
    out: Dict[str, Any] = {"ok": False, "reason": "", "name": name, "as_of": None, "age_h": None, "series": [],
                           "fills": [], "has_trades": False}
    if not isinstance(body, dict) or str(body.get("schema")) != SCHEMA:
        out["reason"] = "history schema not recognised"
        return out
    at = ic._time(body.get("captured_at"))
    if at is None:
        out["reason"] = "captured_at unreadable"
        return out
    if (at - now).total_seconds() > FUTURE_SLACK_S:
        out["reason"] = "captured_at is in the future"
        return out
    if created is not None and abs((at - created).total_seconds()) > MAX_CLOCK_GAP_S:
        out["reason"] = "captured_at differs from the file's creation time"
        return out
    out["as_of"], out["age_h"] = at, max(0.0, (now - at).total_seconds() / 3600.0)
    out["series"], out["fills"] = nav_series(body.get("performance")), fills_of(body)
    out["has_trades"] = any(str(k).startswith("trades") and isinstance(v, dict) and isinstance(v.get("trades"), list)
                            for k, v in body.items())
    if not out["series"] and not out["has_trades"]:
        out["reason"] = "no performance series and no trades in the file"
        return out
    out["ok"] = True
    return out


def latest_files(now: _dt.datetime, session: Any = None, folder: Optional[str] = None) -> Dict[str, Any]:
    """The newest usable history files of the folder, newest first. Never raises. "failed" marks a read that could
    not be made (Drive answered an error, the call raised, the folder id is malformed, or every file to read failed
    to download): the run then exits 2. No file, or only unusable files, is not a failure (Codex P2)."""
    res: Dict[str, Any] = {"ok": False, "failed": False, "reason": "", "files": [], "skipped": 0}
    folder = ic._env("TFB_IBKR_CAPTURE_FOLDER_ID") if folder is None else folder
    if not folder:
        res["reason"] = "capture folder not set (TFB_IBKR_CAPTURE_FOLDER_ID)"
        return res
    if not ic._FOLDER_RE.match(folder):
        res["reason"], res["failed"] = "capture folder id invalid", True
        return res
    errors = 0
    try:
        sess = session or ic._session()
        r = sess.get("https://www.googleapis.com/drive/v3/files", timeout=30, params={
            "q": "'%s' in parents and trashed = false and name contains '%s'" % (folder, PREFIX),
            "orderBy": "createdTime desc", "pageSize": MAX_FILES * 2, "fields": "files(id,name,createdTime,size)",
            "supportsAllDrives": "true", "includeItemsFromAllDrives": "true"})
        if r.status_code != 200:
            res["reason"], res["failed"] = "Drive list answered HTTP %d" % r.status_code, True
            return res
        listed = [f for f in (r.json().get("files") or [])
                  if str(f.get("name", "")).startswith(PREFIX) and str(f.get("name", "")).lower().endswith(".json")]
        for f in listed[:MAX_FILES]:
            if int(f.get("size") or 0) > MAX_BYTES:
                res["skipped"] += 1
                continue
            g = sess.get("https://www.googleapis.com/drive/v3/files/%s" % f["id"], timeout=30,
                         params={"alt": "media", "supportsAllDrives": "true"})
            if g.status_code != 200:
                res["skipped"] += 1
                errors += 1
                continue
            try:
                body = json.loads(g.content.decode("utf-8"))
            except (ValueError, UnicodeDecodeError):
                res["skipped"] += 1
                continue
            p = parse(body, ic._time(f.get("createdTime")), now, str(f.get("name")))
            if p["ok"]:
                res["files"].append(p)
            else:
                res["skipped"] += 1
    except Exception as e:  # noqa: BLE001 - nothing is written without a file; the run reports the failure
        res["reason"], res["failed"] = "history read failed (%s)" % type(e).__name__, True
        return res
    if not res["files"]:
        if errors:
            res["reason"], res["failed"] = "no history file could be downloaded (%d Drive error(s))" % errors, True
        else:
            res["reason"] = ("no history file in the folder (the daily IBKR history task writes %s*.json)" % PREFIX
                             if not res["skipped"] else "no usable history file (%d unusable)" % res["skipped"])
        return res
    res["ok"] = True
    return res


# --------------------------------------------------------------------------- the three tabs
def read_nav(values: Sequence[Sequence[Any]]) -> List[Tuple[_dt.date, float]]:
    """The tab's rows (header skipped), one per day. PURE."""
    rows: Dict[_dt.date, float] = {}
    for r in list(values)[1:]:
        if len(r) < 2:
            continue
        d, x = day_of(r[0]), ic._num(r[1])
        if d is not None and x is not None and x > -1.0:
            rows[d] = x
    return sorted(rows.items())


def chain_nav(existing: Sequence[Tuple[_dt.date, float]], series: Sequence[Tuple[_dt.date, float]],
              start: _dt.date) -> Tuple[List[Tuple[_dt.date, float]], str]:
    """The tab's rows up to the first day IBKR's series shares with it, then IBKR's days rebased onto that day. PURE."""
    old, ser = dict(existing), dict(series)
    if not ser:
        return sorted(old.items()), "kept (no IBKR series)"
    common = sorted(d for d in old if d in ser and d >= start)
    if common:
        a = common[0]
        k = (1.0 + old[a]) / (1.0 + ser[a])
        rows = [(d, v) for d, v in sorted(old.items()) if d <= a]
        rows += [(d, k * (1.0 + c) - 1.0) for d, c in sorted(ser.items()) if d > a]
        return rows, "chained at %s" % a.isoformat()
    if not [d for d in old if d >= start]:              # nothing from the start day yet: IBKR's own figures
        rows = [(d, v) for d, v in sorted(old.items()) if d < start]
        rows += [(d, c) for d, c in sorted(ser.items()) if d >= start]
        return rows, "rebuilt from IBKR"
    return sorted(old.items()), "kept (IBKR's series shares no day with the tab)"


def fill_row(f: Dict[str, Any]) -> List[Any]:
    return [f["trade_id"], f["time"].strftime("%Y-%m-%dT%H:%M:%SZ"), f["symbol"], f["name"], f["ccy"], f["sec_type"],
            f["side"], f["size"], f["price"], f["order_type"], round(f["commission"], 6), round(f["realized"], 6),
            f["order_id"]]


def read_fills(values: Sequence[Sequence[Any]]) -> List[Dict[str, Any]]:
    """The fills already kept in _IBKR_Fills (header skipped). PURE."""
    out = []
    for r in list(values)[1:]:
        c = (list(r) + [""] * len(FILL_HEAD))[:len(FILL_HEAD)]
        f = fill({"trade_id": c[0], "trade_time": c[1], "symbol": c[2], "company_name": c[3], "currency": c[4],
                  "sec_type": c[5], "side": c[6], "size": c[7], "price": c[8], "order_type": c[9], "commission": c[10],
                  "realized_pnl": c[11], "order_id": c[12]})
        if f:
            out.append(f)
    return out


def merge_fills(old: Sequence[Dict[str, Any]], new: Sequence[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """One fill per trade id, the later read winning; oldest first. PURE."""
    by: Dict[str, Dict[str, Any]] = {}
    for f in list(old) + list(new):
        by[f["trade_id"]] = f
    return sorted(by.values(), key=lambda f: (f["time"], f["trade_id"]))


def executions(fills: Sequence[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Consecutive stock fills of one order (same symbol, currency, side and order id; without an order id, the same
    time) as one execution at its last fill: an order filled in pieces is one sale, not several. A fill of the symbol
    on the other side in between starts a new execution, so the position's history keeps its order (Codex P2). PURE."""
    out: List[Dict[str, Any]] = []
    last: Dict[Tuple[str, str], Dict[str, Any]] = {}
    for f in sorted((f for f in fills if f["sec_type"] in ("STK", "")), key=lambda f: (f["time"], f["trade_id"])):
        sym = (f["symbol"], f["ccy"])
        key = (f["side"], f["order_id"] or f["time"].isoformat())
        e = last.get(sym)
        if e is not None and e["_key"] == key:
            e["size"] += f["size"]
            e["realized"] += f["realized"]
            e["commission"] += f["commission"]
            e["time"], e["trade_id"], e["order_type"] = f["time"], f["trade_id"], f["order_type"] or e["order_type"]
            continue
        e = dict(f, _key=key)
        out.append(e)
        last[sym] = e
    return sorted(out, key=lambda f: (f["time"], f["trade_id"]))


def round_trips(fills: Sequence[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Closed round trips per symbol and currency, from flat to flat again, over executions (an order's fills
    together). A sale of more shares than the fills show held closes a trip whose start is unknown. Open positions
    are not returned. PURE."""
    stocks = executions(fills)
    pos: Dict[Tuple[str, str], float] = {}
    open_: Dict[Tuple[str, str], Dict[str, Any]] = {}
    trips: List[Dict[str, Any]] = []
    for f in stocks:
        key = (f["symbol"], f["ccy"])
        q = pos.get(key, 0.0)
        t = open_.get(key)
        if t is None:
            t = {"symbol": f["symbol"], "name": f["name"], "ccy": f["ccy"], "start": f["time"] if abs(q) < EPS else None,
                 "realized": 0.0, "fills": 0}
            open_[key] = t
        t["realized"] += f["realized"]
        t["fills"] += 1
        t["end"], t["last"] = f["time"], f
        t["name"] = f["name"] or t["name"]
        q += f["size"] if f["side"] == "BUY" else -f["size"]
        if q < -EPS:                                    # sold more than the fills show held: the start is unknown
            t["start"], q = None, 0.0
        if abs(q) < EPS:
            trips.append(t)
            del open_[key]
            q = 0.0
        pos[key] = q
    return trips


def fx_rates(fills: Sequence[Dict[str, Any]]) -> Dict[str, List[Tuple[_dt.datetime, float]]]:
    """IBKR's conversions USD.<ccy> (price = <ccy> per USD) by currency. PURE."""
    out: Dict[str, List[Tuple[_dt.datetime, float]]] = {}
    for f in fills:
        m = _FX_SYM.match(f["symbol"]) if f["sec_type"] == "CASH" else None
        if m and f["price"] > 0:
            out.setdefault(m.group(1), []).append((f["time"], f["price"]))
    return out


def exit_type(t: Dict[str, Any]) -> str:
    ot = str(t["last"]["order_type"]).upper().replace(" ", "_")
    if ot in ("STOP", "STP", "STOP_LIMIT", "STP_LMT", "TRAIL", "TRAILING_STOP"):
        return "stop"
    if ot in ("LIMIT", "LMT"):
        return "limit TP" if t["realized"] > 0 else "limit exit"
    if ot in ("MARKET", "MKT", "MOC", "MARKET_ON_CLOSE"):
        return "market exit"
    return "other exit"


def span(a: Optional[_dt.datetime], b: _dt.datetime) -> str:
    if a is None:
        return "?→%d %s" % (b.day, b.strftime("%b"))
    if a.date() == b.date():
        return "%d %s" % (b.day, b.strftime("%b"))
    if (a.year, a.month) == (b.year, b.month):
        return "%d→%d %s" % (a.day, b.day, b.strftime("%b"))
    return "%d %s→%d %s" % (a.day, a.strftime("%b"), b.day, b.strftime("%b"))


def tidy_name(s: str) -> str:
    s = re.sub(r"\s+", " ", str(s or "")).strip()
    t = _LEGAL.sub("", s).strip(" -/,.") or s
    return " ".join(w if (len(w) <= 3 and w.isalpha()) else w.capitalize() for w in t.split())


def trade_rows(trips: Sequence[Dict[str, Any]], fx: Dict[str, List[Tuple[_dt.datetime, float]]]
               ) -> Tuple[List[List[Any]], List[str]]:
    """symbol | name | ccy | exit_type | span | realized_usd | (G: blank) per closed trip, best first, and the trips
    left out because no USD conversion was found for their currency ("SYM (CCY)"). PURE."""
    rows: List[List[Any]] = []
    out: List[str] = []
    for t in trips:
        val, ccy = t["realized"], t["ccy"] or "USD"
        if ccy != "USD":
            near = sorted(fx.get(ccy) or [], key=lambda x: abs((x[0] - t["end"]).total_seconds()))
            if not near:                                # the page sums this column as USD: never a native amount
                out.append("%s (%s)" % (t["symbol"], ccy))
                continue
            val = val / near[0][1]
        name = tidy_name(t["name"]) + (" (%s)" % ccy if ccy != "USD" else "")
        rows.append([t["symbol"], name, ccy, exit_type(t), span(t["start"], t["end"]), round(val, 2), ""])
    return sorted(rows, key=lambda r: -r[5]), out


_SPAN_END = re.compile(r"(\d{1,2}) ([A-Z][a-z]{2})\s*$")
_MONTHS = {m: i for i, m in enumerate(("Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov",
                                       "Dec"), 1)}
CARRY_PREFIX = "carried: closed "
_CARRIED = re.compile(r"^carried: closed (\d{4}-\d{2}-\d{2})")


def carry_mark(end: _dt.date) -> str:
    """Column G of a carried row: the closing day it was given when first carried, so the year is never guessed again
    (Codex P2: a yearless span re-read a year later would land in the new year)."""
    return "%s%s (hand-filled, before the first fill)" % (CARRY_PREFIX, end.isoformat())


def span_end(text: Any, today: _dt.date) -> Optional[_dt.date]:
    """The last day of a span ('24 Aug\u21923 Sep' -> 3 Sep) in the latest year that does not put it after today. PURE."""
    m = _SPAN_END.search(str(text or "").strip())
    if not m or m.group(2) not in _MONTHS:
        return None
    for year in (today.year, today.year - 1):
        try:
            d = _dt.date(year, _MONTHS[m.group(2)], int(m.group(1)))
        except ValueError:
            return None
        if d <= today:
            return d
    return None


def carried_rows(values: Sequence[Sequence[Any]], first_fill: Optional[_dt.date], today: _dt.date) -> List[List[Any]]:
    """The rows of a hand-filled record (no as_of in H:I), or rows already carried (column G), that close before the
    first fill: the fills cannot rebuild them, so they stay, marked in column G. PURE."""
    rows = [(list(r) + [""] * TRADE_WIDTH)[:TRADE_WIDTH] for r in list(values)]
    stamped = any(str(r[7]).strip().lower() == "as_of" for r in rows)
    out: List[List[Any]] = []
    for r in rows[1:]:
        m = _CARRIED.match(str(r[6]).strip())
        if not str(r[0]).strip() or (stamped and not m):
            continue
        end = _dt.date.fromisoformat(m.group(1)) if m else span_end(r[4], today)
        val = ic._num(r[5])
        if end is None or val is None or (first_fill is not None and end >= first_fill):
            continue
        out.append([str(r[0]).strip(), r[1], r[2], r[3], r[4], val, carry_mark(end)])
    return out


def stamp_rows(rows: List[List[Any]], as_of: _dt.datetime, source: str, n_fills: int, now: _dt.datetime,
               carried: int = 0, unconverted: Sequence[str] = ()) -> List[List[Any]]:
    """The record (A:F, G the carry mark) with the stamp in H1:I6; C15 reads as_of."""
    grid = [list(TRADE_HEAD) + ["", "", ""]] + [(list(r) + [""] * 7)[:7] + ["", ""] for r in rows]
    while len(grid) < 6:
        grid.append([""] * TRADE_WIDTH)
    stamp = [("as_of", as_of.astimezone(RIYADH).strftime("%Y-%m-%d %H:%M") + " Riyadh"), ("source", source),
             ("fills", n_fills), ("written", now.astimezone(RIYADH).strftime("%Y-%m-%d %H:%M") + " Riyadh"),
             ("carried", carried), ("not converted", ", ".join(unconverted) or "none")]
    for i, (k, v) in enumerate(stamp):
        grid[i][7], grid[i][8] = k, v
    return grid


# --------------------------------------------------------------------------- the run
def read_tab(book, tab: str, rng: str) -> List[List[Any]]:
    """Numbers as numbers (a cell shown as 1.09% reads 0.0109), dates as their text; a missing tab is empty."""
    try:
        ws = book.worksheet(tab)
    except Exception as e:  # noqa: BLE001
        if type(e).__name__ != "WorksheetNotFound":
            raise
        return []
    return ws.get(rng, value_render_option="UNFORMATTED_VALUE", date_time_render_option="FORMATTED_STRING") or []


def compute(files: Sequence[Dict[str, Any]], nav_values: Sequence[Sequence[Any]],
            fill_values: Sequence[Sequence[Any]], trade_values: Sequence[Sequence[Any]],
            start: _dt.date) -> Dict[str, Any]:
    """Everything the run writes, from the files (newest first) and the three tabs as read. PURE.
    The return comes from the newest file with a series; the fills and the record only from files with a trades read
    (none: "trade_file" is None and the two tabs are left as they are)."""
    nav_file = next((f for f in files if f["series"]), None)
    trade_files = [f for f in files if f.get("has_trades")]
    nav, how = chain_nav(read_nav(nav_values), nav_file["series"] if nav_file else [], start)
    x: Dict[str, Any] = {"nav_file": nav_file, "nav": nav, "how": how, "trade_file": None, "fills": [], "added": 0,
                         "trades": [], "won": 0, "carried": 0, "unconverted": []}
    if not trade_files:
        return x
    old_fills = read_fills(fill_values)
    new_fills: List[Dict[str, Any]] = []
    for f in reversed(trade_files):                    # oldest file first: a later read of a trade id wins
        new_fills += f["fills"]
    fills = merge_fills(old_fills, new_fills)
    rows, unconverted = trade_rows(round_trips(fills), fx_rates(fills))
    times = [f["time"] for f in fills if f["sec_type"] in ("STK", "")]
    today = trade_files[0]["as_of"].astimezone(RIYADH).date()
    carried = carried_rows(trade_values, min(times).date() if times else None, today)
    rows = sorted(rows + carried, key=lambda r: -r[5])
    known = {f["trade_id"] for f in old_fills}
    x.update({"trade_file": trade_files[0], "fills": fills, "added": sum(1 for f in fills if f["trade_id"] not in known),
              "trades": rows, "won": sum(1 for r in rows if r[5] > 0), "carried": len(carried),
              "unconverted": unconverted})
    return x


def run_once(dry_run: bool, book=None, session=None, now: Optional[_dt.datetime] = None,
             found: Optional[Dict[str, Any]] = None) -> int:
    now = now or _dt.datetime.now(UTC)
    if not enabled():
        _out("TFB_IBKR_HISTORY is off: nothing read, nothing written")
        return 0
    found = found if found is not None else latest_files(now, session)
    if not found.get("ok"):
        if found.get("failed"):                         # a read that could not be made is a failed run (exit 2)
            _out("ERROR: %s; nothing written" % (found.get("reason") or "history read failed"))
            return 2
        _out("%s: nothing written; the tabs keep what they hold" % (found.get("reason") or "no history file"))
        return 0
    try:
        if book is None:
            from scripts.run_calendar_sync import _open_book  # noqa: PLC0415
            book = _open_book()
        from scripts.sheets_quota import call as _quota_call  # noqa: PLC0415
        nav_values = _quota_call(lambda: read_tab(book, NAV_TAB, "A1:B3000"), _SLEEP, _out, "reading %s" % NAV_TAB)
        fill_values = _quota_call(lambda: read_tab(book, FILLS_TAB, "A1:M20000"), _SLEEP, _out,
                                  "reading %s" % FILLS_TAB)
        trade_values = _quota_call(lambda: read_tab(book, TRADES_TAB, "A1:I400"), _SLEEP, _out,
                                   "reading %s" % TRADES_TAB)
    except Exception as e:  # noqa: BLE001 - a failed read leaves the tabs in place
        _out("ERROR: %s: %s; nothing written" % (type(e).__name__, str(e)[:300]))
        return 2
    x = compute(found["files"], nav_values or [], fill_values or [], trade_values or [], nav_from())
    newest, tf = found["files"][0], x["trade_file"]
    last = x["nav"][-1][0].isoformat() if x["nav"] else "none"
    _out("history %s read: %.1f h old, %d file(s) merged%s" % (
        newest["name"], newest["age_h"], len(found["files"]),
        (", %d unusable" % found["skipped"]) if found.get("skipped") else ""))
    _out("NAV %d day(s) to %s (%s) | %s" % (len(x["nav"]), last, x["how"], (
        "fills %d (%d new) | closed trades %d (%d won; %d carried from the hand-filled record; %d not converted)" % (
            len(x["fills"]), x["added"], len(x["trades"]), x["won"], x["carried"], len(x["unconverted"]))
        if tf else "no trades read in the files: %s and %s keep what they hold" % (FILLS_TAB, TRADES_TAB))))
    if dry_run:
        _out("dry run: nothing written")
        return 0
    from scripts.daily_plan import write_grid  # noqa: PLC0415
    from scripts.sheets_quota import call as _quota_call  # noqa: PLC0415
    grids = []
    if tf:                                              # the fills first: they are the store the record is built from
        grids.append((FILLS_TAB, [list(FILL_HEAD)] + [fill_row(f) for f in x["fills"]], len(FILL_HEAD), 200,
                      len(x["fills"])))
    if len(x["nav"]) and not x["how"].startswith("kept"):
        grids.append((NAV_TAB, [list(NAV_HEAD)] + [[d.isoformat(), round(v, 8)] for d, v in x["nav"]], len(NAV_HEAD),
                      200, len(x["nav"])))
    if tf:
        grids.append((TRADES_TAB, stamp_rows(x["trades"], tf["as_of"], tf["name"], len(x["fills"]), now, x["carried"],
                                             x["unconverted"]), TRADE_WIDTH, 60, len(x["trades"])))
    wrote = []
    for tab, grid, width, min_rows, n in grids:
        try:                                            # one full overwrite: safe to send again after a quota answer
            _quota_call(lambda: write_grid(book, tab, grid, width, min_rows), _SLEEP, _out, "writing %s" % tab)
        except Exception as e:  # noqa: BLE001 - reported, with the exit code that marks the run failed
            _out("ERROR: %s: %s; %s was not written" % (type(e).__name__, str(e)[:300], tab))
            return 2
        wrote.append("%s (%d row(s))" % (tab, n))
    _out("wrote %s" % (", ".join(wrote) or "nothing (nothing new to write)"))
    return 0


def main(argv: Optional[Sequence[str]] = None, **kw) -> int:
    ap = argparse.ArgumentParser(description="TFB IBKR history: the broker's return and trade record tabs")
    ap.add_argument("--dry-run", action="store_true")
    a = ap.parse_args(list(argv) if argv is not None else None)
    return run_once(a.dry_run, **kw)


if __name__ == "__main__":
    sys.exit(main())
