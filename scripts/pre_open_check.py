#!/usr/bin/env python3
# scripts/pre_open_check.py
"""
================================================================================
TFB Pre-Open Check - v1.0.0 (the Pre_Open_Check tab; workflow-scoped)
================================================================================
v1.0.0 (2026-11-08) -- THE 17:00 RIYADH RE-CHECK OF THE EXECUTABLE TICKETS
WHY (owner, 8 Nov 2026, after the week of 2-6 Nov: "how we can avoid that in
  the future" / "let's do the requirement and implement it"; post-mortem
  plan item 2): the tickets are computed the night before and cannot see
  the open. On 5 Nov SNDA.US and IBOC.US were bought at the 17:30 Riyadh
  open of a day S&P futures had already marked down (SPY closed -1.1 %, VIX
  +15 %); SNDA hit its stop 2 h 15 min later. Archive evidence is thin but
  one-sided: USD seats taken the day after a >= 1.5 % SPY drop lost 3.5 %
  at +5 sessions, 1 win in 7.
WHAT: about 30 minutes before the NYSE open (the workflow fires at 13:00
  and 14:00 UTC; the script acts only inside the window
  [open - 75 min, open + 15 min], so winter and summer time both land once)
  the job reads the EXECUTABLE tickets of Top_10_Investments (Ticket SAR >
  0 and Shares > 0, US listings), fetches S&P 500 futures (ES=F) and each
  ticket's pre-market quote through the same Yahoo chart path the market
  archive uses, and writes the Pre_Open_Check tab:
    Checked At (Riyadh) | US Open (Riyadh) | Symbol | Verdict | Reason |
    Planned Price | Pre-Market Last | Gap % | ES=F % | Session | Source
  Row 1 after the header is the market itself (Symbol ES=F): WAIT when the
  futures are down TFB_PREOPEN_ES_WAIT_PCT (default 1.0) percent or more
  from the previous settlement, GO otherwise, CHECK when the quote is
  missing. Each ticket: WAIT when the market reads WAIT or the pre-market
  last sits TFB_PREOPEN_GAP_PCT (default 3.0) percent or more away from the
  planned price in either direction (a gap down breaks the plan, a gap up
  buys above it); GO inside the band; CHECK when no pre-market trade
  printed (a thin name) - never silently GO. Non-US tickets are listed as
  SKIP (another session). The tab is replaced whole each run (bounded);
  outside the window or on a US holiday nothing is written and the run
  says so. NOTHING here decides, sizes or places anything: the tab is the
  owner's checklist at 17:00, and the cockpit may read it later (a WAIT
  hold in the ticket guard is a separate, default-off change).
GATES: the job runs only from .github/workflows/pre_open_check.yml
  (schedule + manual dispatch; a manual dispatch writes only with
  write=true, and may force the window with ignore_window=true for a
  rehearsal). TFB_PREOPEN_SHEET (default Pre_Open_Check), TFB_PREOPEN_PAGE
  (default Top_10_Investments), TFB_PREOPEN_ES_WAIT_PCT (0.25..5),
  TFB_PREOPEN_GAP_PCT (0.5..10), TFB_PREOPEN_TIMEOUT_S (5..60).
ROLLBACK: disable the workflow. Nothing else reads the tab yet.
--------------------------------------------------------------------------------
"""
from __future__ import annotations

import argparse
import datetime as _dt
import json
import os
import re
import sys
import time
import urllib.request
from typing import Any, Callable, Dict, List, Optional, Sequence, Tuple

__version__ = "1.0.0"

UTC = _dt.timezone.utc
RIYADH = _dt.timezone(_dt.timedelta(hours=3))
DEFAULT_SHEET = "Pre_Open_Check"
DEFAULT_PAGE = "Top_10_Investments"
HEADERS = ["Checked At (Riyadh)", "US Open (Riyadh)", "Symbol", "Verdict", "Reason",
           "Planned Price", "Pre-Market Last", "Gap %", "ES=F %", "Session", "Source"]
MAX_ROWS = 200
WINDOW_BEFORE_MIN = 75
WINDOW_AFTER_MIN = 15
ES_SYMBOL = "ES=F"
YAHOO_HOSTS = ("query1.finance.yahoo.com", "query2.finance.yahoo.com")
YAHOO_PATH = "/v8/finance/chart/{sym}?range=1d&interval={iv}&includePrePost=true"
YAHOO_UA = "Mozilla/5.0 (compatible; TFB-pre-open-check/1.0)"
_US_SUFFIX = re.compile(r"^([A-Z0-9][A-Z0-9.\-]{0,11})\.US$")
_NUM = re.compile(r"-?\d+(?:\.\d+)?")


def _out(msg: str) -> None:
    print(msg, flush=True)


def _env(name: str, default: str = "") -> str:
    return str(os.environ.get(name, default) or default).strip()


def _env_float(name: str, default: float, lo: float, hi: float) -> float:
    try:
        v = float(_env(name, str(default)) or default)
    except (TypeError, ValueError):
        return default
    if v != v or v in (float("inf"), float("-inf")):
        return default
    return min(hi, max(lo, v))


def es_wait_pct() -> float:
    return _env_float("TFB_PREOPEN_ES_WAIT_PCT", 1.0, 0.25, 5.0)


def gap_pct() -> float:
    return _env_float("TFB_PREOPEN_GAP_PCT", 3.0, 0.5, 10.0)


def timeout_s() -> int:
    return int(_env_float("TFB_PREOPEN_TIMEOUT_S", 20, 5, 60))


def riyadh(dt: _dt.datetime) -> str:
    return dt.astimezone(RIYADH).strftime("%Y-%m-%d %H:%M")


# ----------------------------------------------------------------------------- #
# Pure helpers
# ----------------------------------------------------------------------------- #
def us_yahoo_symbol(symbol: str) -> str:
    """'SNDA.US' -> 'SNDA'; 'BRK.B.US' -> 'BRK-B'; anything else -> ''."""
    m = _US_SUFFIX.match(str(symbol or "").strip().upper())
    return m.group(1).replace(".", "-") if m else ""


def _num(v: Any) -> Optional[float]:
    if v is None or isinstance(v, bool):
        return None
    if isinstance(v, (int, float)):
        f = float(v)
        return f if f == f else None
    m = _NUM.search(str(v).replace(",", ""))
    if not m:
        return None
    try:
        return float(m.group(0))
    except ValueError:
        return None


def read_tickets(values: List[List[Any]]) -> List[Dict[str, Any]]:
    """The board's tickets from the raw Top_10 grid: the header row is the
    first row holding both 'Symbol' and 'Ticket SAR' (the title / banner
    rows above it are skipped). A ticket is executable when Ticket SAR > 0
    and Shares > 0; its planned price is the Price column (local currency).
    Returns every ticket row with executable flag, in board order."""
    hdr: Optional[Dict[str, int]] = None
    out: List[Dict[str, Any]] = []
    for row in values or []:
        cells = [str(c if c is not None else "").strip() for c in row]
        if hdr is None:
            low = [c.lower() for c in cells]
            if "symbol" in low and "ticket sar" in low:
                hdr = {c: i for i, c in enumerate(low)}
            continue

        def cell(name: str) -> str:
            i = hdr.get(name)
            return cells[i] if i is not None and i < len(cells) else ""
        sym = cell("symbol").upper()
        if not sym or sym == "SYMBOL" or " " in sym:
            continue
        ticket = _num(cell("ticket sar")) or 0.0
        shares = _num(cell("shares")) or 0.0
        out.append({"symbol": sym, "market": cell("market"), "ccy": cell("ccy"),
                    "price": _num(cell("price")),
                    "executable": ticket > 0 and shares > 0})
    return out


def parse_prepost(payload: Any, now_ts: Optional[int] = None) -> Dict[str, Any]:
    """From a Yahoo v8 chart (range=1d, includePrePost): the last printed
    price, which session printed it (pre / regular / post / none) and the
    previous close. A print after the regular open is a regular-session
    print, so a check that runs late still reads the real open."""
    res: Dict[str, Any] = {"last": None, "last_ts": None, "prev_close": None,
                           "session": "none", "currency": ""}
    try:
        r = payload["chart"]["result"][0]
        meta = r.get("meta") or {}
    except (KeyError, IndexError, TypeError, AttributeError):
        return res
    if not isinstance(r, dict) or not isinstance(meta, dict):
        return res
    res["currency"] = str(meta.get("currency") or "")
    res["prev_close"] = _num(meta.get("chartPreviousClose")) or _num(meta.get("previousClose"))
    ts = r.get("timestamp") or []
    closes = (((r.get("indicators") or {}).get("quote") or [{}])[0] or {}).get("close") or []
    periods = meta.get("currentTradingPeriod") or {}
    reg = periods.get("regular") or {}
    reg_start, reg_end = _num(reg.get("start")), _num(reg.get("end"))
    last_i = None
    for i in range(min(len(ts), len(closes)) - 1, -1, -1):
        c = _num(closes[i])
        if c is not None and c > 0 and (now_ts is None or _num(ts[i]) is None or ts[i] <= now_ts):
            last_i = i
            break
    if last_i is None:
        return res
    t = _num(ts[last_i])
    res["last"], res["last_ts"] = _num(closes[last_i]), int(t) if t is not None else None
    if reg_start is not None and t is not None and t < reg_start:
        res["session"] = "pre"
    elif reg_end is not None and t is not None and t >= reg_end:
        res["session"] = "post"
    else:
        res["session"] = "regular"
    return res


def pct(a: Optional[float], b: Optional[float]) -> Optional[float]:
    if a is None or b is None or b <= 0:
        return None
    return round((a / b - 1.0) * 100.0, 2)


def market_verdict(es: Dict[str, Any], wait_pct: float) -> Tuple[str, str, Optional[float]]:
    """(verdict, reason, es_pct). WAIT when the futures are down wait_pct or
    more from the previous settlement; CHECK when no quote."""
    chg = pct(es.get("last"), es.get("prev_close"))
    if chg is None:
        return "CHECK", "no ES=F quote - read the futures before the open", None
    if chg <= -abs(wait_pct):
        return "WAIT", "S&P futures {:+.2f}% vs settlement (wait at <= -{:.2f}%)".format(chg, abs(wait_pct)), chg
    return "GO", "S&P futures {:+.2f}% vs settlement".format(chg), chg


def ticket_verdict(t: Dict[str, Any], q: Optional[Dict[str, Any]], market: str,
                   band_pct: float) -> Dict[str, Any]:
    """One ticket's row fields: verdict, reason, last, gap, session."""
    row = {"verdict": "CHECK", "reason": "", "last": None, "gap": None, "session": ""}
    if not us_yahoo_symbol(t["symbol"]):
        row.update(verdict="SKIP", reason="not a US listing - another session")
        return row
    if not t.get("executable"):
        row.update(verdict="SKIP", reason="not an executable ticket")
        return row
    q = q or {}
    row["last"], row["session"] = q.get("last"), q.get("session") or "none"
    row["gap"] = pct(q.get("last"), t.get("price"))
    if row["gap"] is None:
        row.update(reason=("quote unread (Yahoo) - read the quote before the open" if q.get("unread")
                           else "no pre-market print - read the quote before the open"))
        if market == "WAIT":
            row.update(verdict="WAIT", reason="market WAIT; " + row["reason"])
        return row
    if market == "WAIT":
        row.update(verdict="WAIT", reason="market WAIT (S&P futures); gap {:+.2f}% vs planned price".format(row["gap"]))
    elif abs(row["gap"]) >= abs(band_pct):
        row.update(verdict="WAIT", reason="{} {:+.2f}% from the planned price (band {:.1f}%)".format(
            "gap up" if row["gap"] > 0 else "gap down", row["gap"], abs(band_pct)))
    else:
        row.update(verdict="GO", reason="within {:.1f}% of the planned price ({:+.2f}%)".format(abs(band_pct), row["gap"]))
    if market == "CHECK" and row["verdict"] == "GO":
        row.update(verdict="CHECK", reason="market unread (ES=F); " + row["reason"])
    return row


def build_rows(now: _dt.datetime, us_open: Optional[_dt.datetime], tickets: List[Dict[str, Any]],
               quotes: Dict[str, Dict[str, Any]], es: Dict[str, Any], wait_pct: float,
               band_pct: float) -> List[List[str]]:
    mv, mreason, es_chg = market_verdict(es, wait_pct)
    stamp, open_s = riyadh(now), riyadh(us_open) if us_open else ""
    es_s = "" if es_chg is None else "{:+.2f}".format(es_chg)

    def f(v: Optional[float], nd: int = 2) -> str:
        return "" if v is None else ("{:." + str(nd) + "f}").format(v)
    rows = [[stamp, open_s, ES_SYMBOL, mv, mreason, f(es.get("prev_close")), f(es.get("last")),
             es_s, es_s, es.get("session") or "none", "Yahoo chart"]]
    for t in tickets:
        r = ticket_verdict(t, quotes.get(t["symbol"]), mv, band_pct)
        rows.append([stamp, open_s, t["symbol"], r["verdict"], r["reason"], f(t.get("price")),
                     f(r["last"]), "" if r["gap"] is None else "{:+.2f}".format(r["gap"]), es_s,
                     r["session"], "Yahoo chart" if r["last"] is not None else ""])
    return rows[:MAX_ROWS]


def preopen_window(now: _dt.datetime, cal: Any) -> Tuple[Optional[_dt.datetime], bool, str]:
    """(today's NYSE open in UTC or None, inside the window?, why). The
    window is [open - WINDOW_BEFORE_MIN, open + WINDOW_AFTER_MIN]."""
    import pandas as pd  # local import: pinned with exchange_calendars
    day = pd.Timestamp(now.astimezone(UTC).date())
    if not cal.is_session(day):
        return None, False, "no US session today"
    op = cal.session_open(day).to_pydatetime().astimezone(UTC)
    lo, hi = op - _dt.timedelta(minutes=WINDOW_BEFORE_MIN), op + _dt.timedelta(minutes=WINDOW_AFTER_MIN)
    if lo <= now <= hi:
        return op, True, "inside the pre-open window"
    return op, False, "outside the pre-open window ({} .. {} Riyadh)".format(riyadh(lo), riyadh(hi))


# ----------------------------------------------------------------------------- #
# Yahoo
# ----------------------------------------------------------------------------- #
def _default_opener(req: urllib.request.Request, timeout: float):  # pragma: no cover - network
    return urllib.request.urlopen(req, timeout=timeout)


class Quotes:
    """query1 then query2; a 404 is final; two consecutive failures stop the
    remaining reads (reported, never hidden)."""

    def __init__(self, timeout: int, opener: Optional[Callable[..., Any]] = None,
                 sleep: Optional[Callable[[float], None]] = None):
        self.timeout = timeout
        self.opener = opener or _default_opener
        self.sleep = sleep or time.sleep
        self.failures: List[str] = []
        self.skipped: List[str] = []

    def fetch(self, ysym: str, interval: str, now_ts: Optional[int]) -> Dict[str, Any]:
        for host in YAHOO_HOSTS:
            req = urllib.request.Request("https://" + host + YAHOO_PATH.format(sym=ysym, iv=interval),
                                         headers={"User-Agent": YAHOO_UA, "Accept": "application/json"})
            try:
                with self.opener(req, self.timeout) as resp:
                    q = parse_prepost(json.loads(resp.read().decode("utf-8", "replace")), now_ts)
                if q.get("last") is not None or q.get("prev_close") is not None:
                    return q
            except Exception as exc:  # noqa: BLE001
                if getattr(exc, "code", None) == 404:
                    break
            self.sleep(1.0)
        self.failures.append(ysym)
        return {}

    def collect(self, tickets: Sequence[Dict[str, Any]], now_ts: Optional[int]) -> Dict[str, Dict[str, Any]]:
        out: Dict[str, Dict[str, Any]] = {}
        streak = 0
        for t in tickets:
            ysym = us_yahoo_symbol(t["symbol"])
            if not ysym or not t.get("executable"):
                continue
            if streak >= 2:
                self.skipped.append(t["symbol"])
                out[t["symbol"]] = {"unread": True}
                continue
            before = len(self.failures)
            q = self.fetch(ysym, "1m", now_ts)
            if len(self.failures) > before:
                streak += 1
                out[t["symbol"]] = {"unread": True}
                continue
            streak = 0
            out[t["symbol"]] = q
            self.sleep(0.2)
        return out


# ----------------------------------------------------------------------------- #
# Workbook
# ----------------------------------------------------------------------------- #
def write_tab(book, tab: str, rows: List[List[str]]) -> int:
    width = len(HEADERS)
    last_col = chr(ord("A") + width - 1)
    ws = None
    prior_extent = 0
    try:
        ws = book.worksheet(tab)
        rows_alloc = getattr(ws, "row_count", None)
        if isinstance(rows_alloc, int) and rows_alloc > MAX_ROWS + 1:
            _out(f"ERROR prior {tab} exceeds the bounded table ({rows_alloc} rows) - preserving tab")
            return 2
        prior_extent = rows_alloc if isinstance(rows_alloc, int) and rows_alloc > 0 else 0
    except Exception as e:  # noqa: BLE001
        if type(e).__name__ != "WorksheetNotFound":
            _out(f"ERROR prior {tab} unreadable ({type(e).__name__}) - preserving tab")
            return 2
        ws = None
    try:
        extent = max(60, len(rows) + 1, prior_extent)
        if ws is None:
            ws = book.add_worksheet(title=tab, rows=extent, cols=width)
        else:
            rows_alloc = getattr(ws, "row_count", None)
            cols_alloc = getattr(ws, "col_count", None)
            if isinstance(rows_alloc, int) and rows_alloc < extent:
                ws.resize(rows=extent)
            if isinstance(cols_alloc, int) and cols_alloc < width:
                ws.resize(cols=width)
        values = [list(HEADERS)] + rows + [[""] * width for _ in range(extent - len(rows) - 1)]
        ws.update(values=values, range_name=f"A1:{last_col}{extent}", value_input_option="RAW")
        try:
            ws.freeze(rows=1)
        except Exception:  # noqa: BLE001
            pass
        _out(f"wrote {len(rows)} rows -> {tab}")
        return 0
    except Exception as e:  # noqa: BLE001
        _out(f"ERROR publication unconfirmed: {type(e).__name__}; read back before retrying")
        return 3


def main(argv: Optional[Sequence[str]] = None, *, book=None, quotes: Optional[Quotes] = None,
         now: Optional[_dt.datetime] = None, cal: Any = None) -> int:
    ap = argparse.ArgumentParser(description="Write the Pre_Open_Check tab before the US open.")
    g = ap.add_mutually_exclusive_group()
    g.add_argument("--write", action="store_true", help="write the tab (CI mode)")
    g.add_argument("--dry-run", action="store_true", help="print the rows only")
    ap.add_argument("--ignore-window", action="store_true",
                    help="run outside the pre-open window (rehearsal)")
    args = ap.parse_args(list(argv) if argv is not None else None)
    write = bool(args.write) and not args.dry_run
    now = now or _dt.datetime.now(UTC)
    tab = _env("TFB_PREOPEN_SHEET", DEFAULT_SHEET) or DEFAULT_SHEET
    page = _env("TFB_PREOPEN_PAGE", DEFAULT_PAGE) or DEFAULT_PAGE

    if cal is None:
        import exchange_calendars as xcals  # local import: pinned in requirements
        cal = xcals.get_calendar("XNYS")
    us_open, inside, why = preopen_window(now, cal)
    _out(f"now {riyadh(now)} Riyadh | US open {riyadh(us_open) if us_open else '-'} | {why}")
    if not inside and not args.ignore_window:
        _out("nothing to do")
        return 0

    from scripts.run_calendar_sync import _open_book  # noqa: PLC0415
    if book is None:
        try:
            book = _open_book()
        except Exception as e:  # noqa: BLE001
            _out(f"ERROR: cannot open workbook: {type(e).__name__}")
            return 2
    try:
        vals = book.worksheet(page).get("A1:AZ60")
    except Exception as e:  # noqa: BLE001
        _out(f"ERROR: cannot read page {page}: {type(e).__name__}; nothing written")
        return 2
    tickets = read_tickets(vals)
    execs = [t for t in tickets if t["executable"]]
    _out(f"page {page}: {len(tickets)} ticket row(s), {len(execs)} executable, "
         f"{sum(1 for t in execs if us_yahoo_symbol(t['symbol']))} US")

    quotes = quotes or Quotes(timeout_s())
    now_ts = int(now.timestamp())
    es = quotes.fetch(ES_SYMBOL, "5m", now_ts)
    qmap = quotes.collect(tickets, now_ts)
    rows = build_rows(now, us_open, tickets, qmap, es, es_wait_pct(), gap_pct())
    for r in rows:
        _out("  " + " | ".join(r[2:9]))
    if quotes.failures or quotes.skipped:
        _out(f"WARN: quotes unread: {', '.join(quotes.failures)}"
             + (f" | skipped after the breaker: {', '.join(quotes.skipped)}" if quotes.skipped else ""))
    if not write:
        _out("dry run - nothing written")
        return 0
    return write_tab(book, tab, rows)


if __name__ == "__main__":
    sys.exit(main())
