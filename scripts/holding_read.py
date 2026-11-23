#!/usr/bin/env python3
"""
scripts/holding_read.py - TFB Holding Read
==========================================
VERSION 1.0.2 (2026-11-23) - first written 2026-11-18 (1.0.0)
1.0.2 (23 Nov 2026): OPENING THE WORKBOOK WAITS OUT THE QUOTA TOO. The Daily Plan run
    45123566985 (23 Nov 06:3x UTC) ended "cannot open workbook: APIError" at its first call: opening the book reads its
    metadata, which counts against the same per-minute Sheets quota, and it was the one read never retried. It now goes
    through scripts/sheets_quota.call (a quota answer waits 65 s, twice at most), and the error line names the HTTP status
    when there is one (no value of the workbook).
1.0.1 (2026-11-23): THE RUN LOG NAMES NO HOLDING, AND A QUOTA REFUSAL OF THE WRITE IS WAITED OUT (owner, 23 Nov
    08:42 Riyadh: "review the full project and check specially the workflows ... if there is any failing or other
    issue please update and try to improve"). Every run since 1.0.0 printed one line per holding - its symbol and
    its last session against the market and its group - into the GitHub Actions log of a public repository (the
    owner's standing rule: no holdings' symbols in run logs). On GitHub Actions each holding now prints as
    "holding 3 of 8: GROUP | <read>"; gate TFB_HREAD_LOG_DETAIL = auto (default: no detail on GitHub Actions) | on |
    off. And 22 Nov 20:27 UTC the run ended "publication unconfirmed: APIError" on the Sheets per-minute quota:
    the two page writes overwrite their whole range, so a quota answer is now waited out and the writes tried again
    (scripts/sheets_quota.call, 65 / 65 s). The page and the ledger are unchanged.

WHY (owner, Wed 18 Nov 2026 09:36 Riyadh: "check what happened to my investments as there is now 2 days loss ...
add two new [fields] to the analysis of my portfolio: the main news that affected the last day's performance of
each stock, in the row of analysis of each stock, and the expectation for the next day's performance - will it go
further down, or rise, or reverse"). Read that morning on the broker account and the archive:
  - the account fell 1.85% over the sessions of 16 and 17 Nov while the S&P 500 fell 2.9% (-1.9%, -1.0%) and
    financials fell 1.9% on the second day. Every holding's fall but one was the size its group's fall explains;
  - the workbook could not say so. The news tab held no item for most holdings on those days, and no page
    compared a holding's day with its market and its group, so "what moved it" had no answer on any page;
  - the engine's 1-month and 3-month figures are fixed shares of its 12-month valuation target (0.18 and 0.42 of
    it for six of the eight holdings). They carry no information about the next session.

WHAT. One row per holding, on the page Holding_Read, after every close:
  LAST SESSION   the holding's move, its market's and its group's (a sector fund), and one DRIVER:
                   COMPANY NEWS   a company item of the session, and a move the market does not explain
                   SECTOR         no such item; the group moved the same way and explains the move better
                   MARKET         no such item; the market moved the same way
                   NORMAL RANGE   under half its usual daily move and under 1%: no cause is needed
                   NO NEWS FOUND  a move of its own with no item read - a gap in the news read, said as one
  MAIN NEWS      the session's company headline (source, time), or the group's and market's move with the news
                 gate's macro themes when there is no company item. Law-firm notices and unconfirmed clusters
                 are never used (exit_plan.news_is_noise).
  NEXT SESSION   a READ, not a forecast of size: among this stock's own past days of the same kind (the same
                 size of move against its own volatility, on the same kind of market day), how often the next
                 session closed up, on how many days, against how often it does on any day:
                   REBOUND MORE LIKELY / FURTHER FALL MORE LIKELY      after a fall
                   RISE MORE LIKELY TO CONTINUE / PULLBACK MORE LIKELY after a rise
                   UP MORE LIKELY / DOWN MORE LIKELY                   after a flat day
                   NO EDGE                the share is within TFB_HREAD_EDGE_PTS of any day's, or the
                                          difference is not statistically clear (|z| < TFB_HREAD_Z)
                   NOT ENOUGH HISTORY     fewer than TFB_HREAD_MIN_N such days
                   PINNED BY AN OFFER     a tender / merger item and a price that has stopped moving
                   EARNINGS DUE: NO READ  results within a day: the read does not cover a results reaction
                 Most days read NO EDGE. That is the honest answer, and it is printed as one.
  SCORE          every read is kept in _Holding_Read and marked HIT or MISS when the next session closes; the
                 page shows the count. A read that does not beat a coin over time will show it here first.
No order follows from this page: the Today Action Plan gives orders. The page is analysis.

SOURCES. _Portfolio_CostBasis (active lots), My_Portfolio (name, class, industry), _News_Items and _News_Gate
(news_macro_gate), Calendar_Events / Dividend_Events, and daily bars from the Yahoo chart service: the holding,
its market (SPY for .US, ^TASI.SR for .SR) and its group's fund (TFB_HREAD_SECTOR_MAP overrides, 'SYM=FUND,...').

THE RUN LOG IS PUBLIC (the repository is): it prints moves in percent, labels, shares and counts - never a
quantity, a value or a cash figure.

GATE: TFB_HOLDING_READ = off (default) | observe (print only) | on.
Rollback: repository variable TFB_HOLDING_READ=off; the two tabs can be deleted.
"""
from __future__ import annotations

import argparse
import datetime as _dt
import json
import math
import os
import re
import sys
import time
import urllib.request
from typing import Any, Callable, Dict, List, Optional, Sequence, Tuple

if __package__ in (None, ""):
    sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from scripts.daily_plan import (  # noqa: E402
    _col, _read, cell, num, read_calendar, riyadh_str, table_index, text, write_grid,
)
from scripts.exit_plan import (  # noqa: E402
    NO_STOP_CLASSES, YAHOO_HOSTS, YAHOO_UA, _parse_ts, news_is_noise, parse_chart, read_columns, read_lots,
    split_open_session, yahoo_symbol,
)

__version__ = "1.0.2"
UTC = _dt.timezone.utc
RIYADH = _dt.timezone(_dt.timedelta(hours=3))
PAGE_TAB = "Holding_Read"
LEDGER_TAB = "_Holding_Read"
PAGE_WIDTH = 12
MAX_LEDGER_ROWS = 2000
MAX_PENDING = 20                     # symbols no longer held whose open reads are still marked, per run
LEDGER_HEADERS = ["Session", "Symbol", "Stock %", "Market %", "Sector %", "Driver", "Main news", "Read",
                  "Up share %", "Sample", "Base %", "Next session", "Next %", "Result", "Written (Riyadh)"]
PAGE_HEAD = ["Symbol", "Name", "Last session", "Stock %", "Market %", "Sector %", "Driver",
             "Main news (last session)", "Next session read", "Up after similar days", "Why", "After the close"]
INFO_COLS = ("Symbol", "Name", "Asset Class", "Sector", "Industry")
HISTORY_PATH = "/v8/finance/chart/{sym}?range={rng}&interval=1d"
READ_ATTEMPTS = 3
READ_RETRY_S = 20
_SLEEP = time.sleep


def log_detail() -> bool:
    """1.0.1: may the run log name the holdings? TFB_HREAD_LOG_DETAIL = on | off | auto (default: not on GitHub
    Actions, whose logs are public for this repository)."""
    v = (os.environ.get("TFB_HREAD_LOG_DETAIL") or "auto").strip().lower()
    if v in ("on", "1", "true", "yes"):
        return True
    if v in ("off", "0", "false", "no"):
        return False
    return (os.environ.get("GITHUB_ACTIONS") or "").strip().lower() != "true"


UP_READS = ("REBOUND MORE LIKELY", "RISE MORE LIKELY TO CONTINUE", "UP MORE LIKELY")
DOWN_READS = ("FURTHER FALL MORE LIKELY", "PULLBACK MORE LIKELY", "DOWN MORE LIKELY")
NO_EDGE, NO_HISTORY, PINNED, EARNINGS = ("NO EDGE", "NOT ENOUGH HISTORY", "PINNED BY AN OFFER",
                                         "EARNINGS DUE: NO READ")
SEV_RANK = {"H": 0, "M": 1, "L": 2}
# The group's fund by the words of the industry / sector cell (US listings); the first match wins.
SECTOR_FUNDS: Tuple[Tuple[str, str], ...] = (
    (r"\bbank", "KRE"), (r"\breit\b|real estate", "VNQ"), (r"shipping|marine|tanker", "BOAT"),
    (r"drug|pharma|biotech", "XPH"), (r"insurance", "KIE"), (r"semiconductor", "SMH"),
    (r"oil|gas|energy", "XLE"), (r"utilit", "XLU"), (r"gold|mining|metal", "XME"),
    (r"software|information technology", "XLK"),
)
OFFER_RE = re.compile(r"tender offer|to be acquired|merger agreement|take[- ]private|buyout|acquisition offer", re.I)


def _status_of(e: BaseException) -> str:
    """1.0.2: the HTTP status of a Sheets error for the run log, e.g. " [429]" (never a value of the workbook)."""
    code = getattr(getattr(e, "response", None), "status_code", None) or getattr(getattr(e, "resp", None), "status", None)
    if code is None:
        m = re.search(r"\[(\d{3})\]", str(e))
        code = m.group(1) if m else None
    return " [%s]" % code if code else ""


def _out(msg: str) -> None:
    print("[holding_read %s] %s" % (__version__, msg), flush=True)


def _env(name: str, default: str = "") -> str:
    return (os.getenv(name) or default).strip()


def _env_num(name: str, default: float, lo: float, hi: float) -> float:
    try:
        v = float(_env(name) or default)
    except ValueError:
        return default
    return v if math.isfinite(v) and lo <= v <= hi else default


def mode() -> str:
    v = _env("TFB_HOLDING_READ", "off").lower()
    return v if v in ("off", "observe", "on") else "off"


def knobs() -> Dict[str, Any]:
    return {
        "range": _env("TFB_HREAD_RANGE", "5y") if _env("TFB_HREAD_RANGE", "5y") in ("2y", "5y", "10y") else "5y",
        "vol_win": int(_env_num("TFB_HREAD_VOL_WIN", 60, 20, 250)),
        "min_n": int(_env_num("TFB_HREAD_MIN_N", 30, 10, 500)),
        "edge_pts": _env_num("TFB_HREAD_EDGE_PTS", 5.0, 1.0, 30.0),
        "z": _env_num("TFB_HREAD_Z", 1.64, 0.5, 4.0),
        "pin_pct": _env_num("TFB_HREAD_PIN_PCT", 0.6, 0.05, 3.0),
        "sector_map": parse_map(_env("TFB_HREAD_SECTOR_MAP")),
    }


def parse_map(spec: str) -> Dict[str, str]:
    """'STNG.US=BOAT, AX.US=KRE' -> {'STNG.US': 'BOAT', 'AX.US': 'KRE'}; a malformed pair is dropped."""
    out: Dict[str, str] = {}
    for part in (spec or "").split(","):
        m = re.fullmatch(r"\s*([A-Za-z0-9.\-]{1,15})\s*=\s*([A-Za-z0-9.\-^=]{0,15})\s*", part)
        if m:
            out[m.group(1).upper()] = m.group(2).upper()
    return out


# ----------------------------------------------------------------------------- #
# Pure: returns, volatility, the day's attribution
# ----------------------------------------------------------------------------- #
def returns(bars: Sequence[Sequence[Any]]) -> List[Tuple[_dt.date, float]]:
    """[(date, close-to-close return in percent)] from bars (date, o, h, l, close, factor); the first bar has none."""
    out = []
    for i in range(1, len(bars)):
        p, c = bars[i - 1][4], bars[i][4]
        if p and p > 0:
            out.append((bars[i][0], (c / p - 1.0) * 100.0))
    return out


def pstdev(xs: Sequence[float]) -> Optional[float]:
    if len(xs) < 2:
        return None
    mu = sum(xs) / len(xs)
    return math.sqrt(sum((x - mu) ** 2 for x in xs) / len(xs))


def beta_of(rets: Sequence[Tuple[_dt.date, float]], mkt: Dict[_dt.date, float], n: int = 120) -> Optional[float]:
    """Slope of the stock's return on its market's over the last n common sessions (60 at least), kept in 0..3."""
    pairs = [(r, mkt[d]) for d, r in rets if d in mkt][-n:]
    if len(pairs) < 60:
        return None
    mx = sum(m for _, m in pairs) / len(pairs)
    my = sum(r for r, _ in pairs) / len(pairs)
    var = sum((m - mx) ** 2 for _, m in pairs)
    if var <= 0:
        return None
    return max(0.0, min(3.0, sum((m - mx) * (r - my) for r, m in pairs) / var))


def _sign(x: Optional[float]) -> int:
    return 0 if not x else (1 if x > 0 else -1)


def attribute(r: float, m: Optional[float], s: Optional[float], beta: Optional[float], sigma: Optional[float],
              item: Optional[Dict[str, Any]]) -> str:
    """The driver of a session's move r (percent) given its market's m, its group's s, the stock's beta and usual
    daily move sigma, and the session's material company item (or None)."""
    exp = (beta if beta is not None else 1.0) * m if m is not None else None
    resid = r - exp if exp is not None else r
    unit = sigma if sigma and sigma > 0 else 1.0
    if item and (item.get("dir") in (0, None) or _sign(item["dir"]) == _sign(r)) and abs(resid) >= 0.5 * unit:
        return "COMPANY NEWS"
    if abs(r) < 0.5 * unit and abs(r) < 1.0:
        return "NORMAL RANGE"
    if s is not None and _sign(s) == _sign(r) and abs(s) >= 0.5 and (exp is None or abs(r - s) <= abs(resid)):
        return "SECTOR"
    if m is not None and _sign(m) == _sign(r) and abs(m) >= 0.5:
        return "MARKET"
    return "NO NEWS FOUND"


def macro_themes(drivers: str) -> str:
    """'market: VIX 25 | macro: war_escalation/hormuz grade 1, 38 sources: ...' -> 'war escalation/hormuz'."""
    seen: List[str] = []
    for part in text(drivers).split("|"):
        m = re.match(r"\s*macro:\s*([A-Za-z0-9_/\- ]+?)\s+grade\b", part)
        if m:
            t = m.group(1).replace("_", " ").strip()
            if t and t not in seen:
                seen.append(t)
    return "; ".join(seen[:3])


def pct(v: Optional[float], nd: int = 1) -> str:
    return "" if v is None else ("%+." + str(nd) + "f%%") % v


def item_text(item: Dict[str, Any]) -> str:
    when = item["pub"].astimezone(RIYADH).strftime("%m-%d %H:%M") if item.get("pub") else ""
    src = ", ".join(x for x in (text(item.get("domain")), when and when + " Riyadh") if x)
    return "%s%s" % (text(item.get("headline"))[:170], " (%s)" % src if src else "")


def main_news(driver: str, r: float, m: Optional[float], s: Optional[float], sigma: Optional[float],
              item: Optional[Dict[str, Any]], mkt_name: str, fund: str, themes: str) -> str:
    macro = " Macro themes in the news gate: %s." % themes if themes else ""
    if driver == "COMPANY NEWS" and item:
        return item_text(item)
    if driver == "SECTOR":
        return "No company item. Its group moved: %s %s, %s %s.%s" % (fund, pct(s), mkt_name, pct(m), macro)
    if driver == "MARKET":
        return "No company item. The market moved: %s %s.%s" % (mkt_name, pct(m), macro)
    if driver == "NORMAL RANGE":
        tail = " On the day: %s" % item_text(item) if item else ""
        return "Inside its usual daily move (about %s%%): no cause is needed.%s" % (
            ("%.1f" % sigma) if sigma else "n/a", tail)
    other = " The day's item pointed the other way: %s" % item_text(item) if item else ""
    return ("No cause in the news read: it moved %s against %s %s. Check the company's own announcements.%s"
            % (pct(r), mkt_name or "its market", pct(m) or "n/a", other))


# ----------------------------------------------------------------------------- #
# Pure: the next-session read
# ----------------------------------------------------------------------------- #
def z_bucket(z: float) -> str:
    if z <= -1.0:
        return "BIG DOWN"
    if z <= -0.35:
        return "DOWN"
    if z < 0.35:
        return "FLAT"
    if z < 1.0:
        return "UP"
    return "BIG UP"


def m_bucket(m: Optional[float]) -> str:
    if m is None:
        return "NA"
    return "DOWN" if m <= -0.5 else ("UP" if m >= 0.5 else "FLAT")


def states(rets: Sequence[Tuple[_dt.date, float]], mkt: Dict[_dt.date, float], win: int
           ) -> List[Tuple[str, str, Optional[float]]]:
    """One (stock bucket, market bucket, next session's return or None for the last day) per session that has `win`
    sessions before it. The stock bucket is the day's return over the volatility of the `win` sessions BEFORE it."""
    out: List[Tuple[str, str, Optional[float]]] = []
    vals = [r for _, r in rets]
    for t in range(win, len(rets)):
        sd = pstdev(vals[t - win:t])
        if not sd or sd <= 0:
            continue
        nxt = vals[t + 1] if t + 1 < len(vals) else None
        out.append((z_bucket(vals[t] / sd), m_bucket(mkt.get(rets[t][0])), nxt))
    return out


_Z_WORDS = {"BIG DOWN": "a hard fall", "DOWN": "a fall", "FLAT": "a flat day", "UP": "a rise", "BIG UP": "a strong rise"}


def next_read(st: Sequence[Tuple[str, str, Optional[float]]], k: Dict[str, Any]) -> Dict[str, Any]:
    """The read for the session after the last state in `st` (whose next return is None)."""
    out: Dict[str, Any] = {"label": NO_HISTORY, "p": None, "n": 0, "base": None, "mean": None, "why": ""}
    if not st or st[-1][2] is not None:
        out["why"] = "no completed session to read from"
        return out
    zb, mb, _ = st[-1]
    past = [x for x in st[:-1] if x[2] is not None]
    if len(past) < k["min_n"]:
        out["why"] = "only %d past sessions with a next day" % len(past)
        return out
    base = sum(1 for x in past if x[2] > 0) / len(past)
    same = [x[2] for x in past if x[0] == zb and x[1] == mb] if mb != "NA" else []
    scope = "the market %s" % mb.lower()
    if len(same) < k["min_n"]:
        same, scope = [x[2] for x in past if x[0] == zb], "any market"
    out.update({"n": len(same), "base": base * 100.0})
    if len(same) < k["min_n"]:
        out["why"] = "only %d past days like it (%s); %d are needed" % (len(same), _Z_WORDS[zb], k["min_n"])
        return out
    p = sum(1 for x in same if x > 0) / len(same)
    mean = sum(same) / len(same)
    se = math.sqrt(base * (1.0 - base) / len(same)) if 0.0 < base < 1.0 else 0.0
    z = (p - base) / se if se > 0 else 0.0
    edge = abs(p - base) * 100.0 >= k["edge_pts"] and abs(z) >= k["z"]
    last = "down" if zb in ("BIG DOWN", "DOWN") else ("up" if zb in ("UP", "BIG UP") else "flat")
    # A direction is named only when the days like it agree with it themselves: more than half closed up for an
    # up read, fewer than half for a down read. A share that differs from any day's but stays on the other side of
    # one half (60% up against 72%) names nothing.
    tail = ""
    if not edge:
        label = NO_EDGE
    elif p > base and p > 0.5:
        label = {"down": UP_READS[0], "up": UP_READS[1], "flat": UP_READS[2]}[last]
    elif p < base and p < 0.5:
        label = {"down": DOWN_READS[0], "up": DOWN_READS[1], "flat": DOWN_READS[2]}[last]
    else:
        label = NO_EDGE
        tail = " The share differs from any day's but does not cross one half: no direction is named."
    out.update({"label": label, "p": p * 100.0, "mean": mean,
                "why": "After %s with %s, the next session closed up %.0f%% of %d times (average %+.2f%%); "
                       "on any day it is %.0f%%.%s" % (_Z_WORDS[zb], scope, p * 100.0, len(same), mean,
                                                       base * 100.0, tail)})
    return out


def pinned(rets: Sequence[Tuple[_dt.date, float]], items: Sequence[Dict[str, Any]], pin_pct: float,
           session: _dt.date) -> bool:
    """A price that has stopped moving (the last 5 sessions' returns vary by under pin_pct) with a tender / merger
    item in the 30 days before the session."""
    last5 = [r for _, r in rets[-5:]]
    sd = pstdev(last5)
    if len(last5) < 5 or sd is None or sd >= pin_pct:
        return False
    since = _dt.datetime(session.year, session.month, session.day, tzinfo=UTC) - _dt.timedelta(days=30)
    return any(i.get("pub") and i["pub"] >= since and not i.get("noise")
               and (text(i.get("event")).lower() == "mna" or OFFER_RE.search(text(i.get("headline")))) for i in items)


# ----------------------------------------------------------------------------- #
# Workbook parsers (pure)
# ----------------------------------------------------------------------------- #
def read_info(values: Sequence[Sequence[Any]]) -> Dict[str, Dict[str, str]]:
    h, col = table_index(values, ("Symbol",))
    out: Dict[str, Dict[str, str]] = {}
    if h is None:
        return out
    for r in list(values)[h + 1:]:
        sym = text(cell(r, col.get("Symbol"))).upper()
        if sym and sym not in out:
            out[sym] = {"name": text(cell(r, col.get("Name"))), "asset": text(cell(r, col.get("Asset Class"))),
                        "group": " ".join(x for x in (text(cell(r, col.get("Industry"))),
                                                       text(cell(r, col.get("Sector")))) if x)}
    return out


def read_items(values: Sequence[Sequence[Any]]) -> Dict[str, List[Dict[str, Any]]]:
    """{SYMBOL: [item]} from _News_Items; an item = pub (UTC), dir (-1/0/1), sev, event, headline, domain, noise."""
    h, col = table_index(values, ("Scope", "Direction", "Severity", "Headline"))
    out: Dict[str, List[Dict[str, Any]]] = {}
    if h is None:
        return out
    for r in list(values)[h + 1:]:
        scope = text(cell(r, col.get("Scope"))).upper()
        pub = _parse_ts(cell(r, col.get("Published (UTC)")))
        head = text(cell(r, col.get("Headline")))
        if not scope or scope == "MACRO" or pub is None or not head:
            continue
        d = num(cell(r, col.get("Direction")))
        ev = text(cell(r, col.get("Event Type")))
        out.setdefault(scope, []).append({
            "pub": pub, "dir": int(d) if d in (-1, 0, 1) else 0, "sev": text(cell(r, col.get("Severity"))).upper(),
            "event": ev, "headline": head, "domain": text(cell(r, col.get("Domain"))),
            "noise": news_is_noise(ev, head, cell(r, col.get("Cluster Grade")))})
    return out


def read_gate(values: Sequence[Sequence[Any]]) -> Dict[str, str]:
    """The newest row of _News_Gate: verdict, drivers, market."""
    h, col = table_index(values, ("Run At (Riyadh)", "Verdict"))
    out = {"verdict": "", "drivers": "", "market": ""}
    if h is None:
        return out
    for r in list(values)[h + 1:]:
        if text(cell(r, col["Verdict"])):
            out = {"verdict": text(cell(r, col["Verdict"])).upper(), "drivers": text(cell(r, col.get("Drivers"))),
                   "market": text(cell(r, col.get("Market")))}
    return out


VENUE_CLOSE = {".SR": ("Asia/Riyadh", 15, 0, 3), ".L": ("Europe/London", 16, 30, 0)}
US_CLOSE = ("America/New_York", 16, 0, -5)


def close_utc(symbol: str, day: _dt.date) -> _dt.datetime:
    """The end of the regular session of `day`, in UTC: 16:00 New York, 15:00 Riyadh, 16:30 London, each in the
    venue's own time so daylight saving is followed. Without a time-zone database the standard-time offset is used."""
    s = symbol.upper()
    zone, hh, mm, std = next((v for suf, v in VENUE_CLOSE.items() if s.endswith(suf)), US_CLOSE)
    try:
        from zoneinfo import ZoneInfo  # noqa: PLC0415
        tz: _dt.tzinfo = ZoneInfo(zone)
    except Exception:  # noqa: BLE001
        tz = _dt.timezone(_dt.timedelta(hours=std))
    return _dt.datetime(day.year, day.month, day.day, hh, mm, tzinfo=tz).astimezone(UTC)


def material(i: Dict[str, Any]) -> bool:
    return not i.get("noise") and (i.get("sev") in ("H", "M") or i.get("dir") in (-1, 1))


def pick_item(items: Sequence[Dict[str, Any]], start: _dt.datetime, end: Optional[_dt.datetime]
              ) -> Optional[Dict[str, Any]]:
    """The one item of (start, end] to show: the most severe, then one with a direction, then the newest."""
    pool = [i for i in items if material(i) and i["pub"] > start and (end is None or i["pub"] <= end)]
    if not pool:
        return None
    return sorted(pool, key=lambda i: (SEV_RANK.get(i.get("sev"), 3), 0 if i.get("dir") else 1,
                                       -i["pub"].timestamp()))[0]


def market_symbol(symbol: str) -> Tuple[str, str]:
    s = symbol.upper()
    if s.endswith(".US"):
        return "SPY", "S&P 500"
    if s.endswith(".SR"):
        return "^TASI.SR", "TASI"
    return "", ""


def sector_fund(symbol: str, group: str, overrides: Dict[str, str]) -> str:
    s = symbol.upper()
    if s in overrides:
        return overrides[s]
    if not s.endswith(".US"):
        return ""
    for pat, fund in SECTOR_FUNDS:
        if re.search(pat, group or "", re.I):
            return fund
    return ""


# ----------------------------------------------------------------------------- #
# One holding (pure given its inputs)
# ----------------------------------------------------------------------------- #
def read_holding(symbol: str, name: str, bars: Sequence[Sequence[Any]], mkt_bars: Sequence[Sequence[Any]],
                 sec_bars: Sequence[Sequence[Any]], items: Sequence[Dict[str, Any]], cal: Dict[str, Any],
                 gate: Dict[str, str], group: str, fund: str, mkt_name: str, k: Dict[str, Any]) -> Dict[str, Any]:
    res: Dict[str, Any] = {"symbol": symbol, "name": name, "ok": False, "driver": "NOT READ", "news": "",
                           "label": NO_HISTORY, "why": "", "after": "", "session": None, "r": None, "m": None,
                           "s": None, "p": None, "n": 0, "base": None}
    rets = returns(bars)
    if len(rets) < k["vol_win"] + 2:
        res["news"] = "no price history (%d sessions read)" % len(rets)
        return res
    mkt = dict(returns(mkt_bars))
    sec = dict(returns(sec_bars))
    session, r = rets[-1]
    m, s = mkt.get(session), sec.get(session)
    sigma = pstdev([x for _, x in rets[-k["vol_win"] - 1:-1]])
    prev_close = close_utc(symbol, rets[-2][0])
    this_close = close_utc(symbol, session)
    item = pick_item(items, prev_close, this_close)
    later = pick_item(items, this_close, None)
    driver = attribute(r, m, s, beta_of(rets, mkt), sigma, item)
    res.update({"ok": True, "session": session, "r": r, "m": m, "s": s, "driver": driver,
                "news": main_news(driver, r, m, s, sigma, item, mkt_name, fund, macro_themes(gate.get("drivers", ""))),
                "after": ("%s: %s" % ({1: "positive", -1: "negative"}.get(later.get("dir"), "neutral"),
                                      item_text(later)) if later else "")})
    nr = next_read(states(rets, mkt, k["vol_win"]), k)
    res.update({"label": nr["label"], "p": nr["p"], "n": nr["n"], "base": nr["base"], "why": nr["why"]})
    earn = cal.get("earn_days")
    if earn is not None and 0 <= earn <= 1:
        res["label"] = EARNINGS
        res["why"] = "Results are due within a day: the read does not cover a results reaction. " + res["why"]
    elif pinned(rets, items, k["pin_pct"], session):
        res["label"] = PINNED
        res["why"] = ("The price has stopped moving with a tender / merger item in the news: little movement is "
                      "expected while the offer stands; the risk is the offer failing. " + res["why"])
    if later and later.get("dir") == -1 and later.get("sev") in ("H", "M"):
        res["why"] += " A negative company item came after the close: weigh it before the read."
    return res


# ----------------------------------------------------------------------------- #
# The ledger: keep each read, mark it when the next session closes
# ----------------------------------------------------------------------------- #
def parse_ledger(values: Sequence[Sequence[Any]]) -> List[Dict[str, Any]]:
    rows = list(values)
    if not rows or [text(c) for c in rows[0]][:len(LEDGER_HEADERS)] != LEDGER_HEADERS:
        return []
    return [{LEDGER_HEADERS[i]: cell(r, i) for i in range(len(LEDGER_HEADERS))} for r in rows[1:] if text(cell(r, 0))]


def result_of(label: str, nxt: Optional[float]) -> str:
    if nxt is None:
        return ""
    if label in UP_READS:
        return "HIT" if nxt > 0 else "MISS"
    if label in DOWN_READS:
        return "HIT" if nxt < 0 else "MISS"
    return "-"


def settle(ledger: List[Dict[str, Any]], rets_by_symbol: Dict[str, Sequence[Tuple[_dt.date, float]]]) -> int:
    """Fill Next session / Next % / Result on rows whose next session has closed. Returns how many were marked."""
    done = 0
    for d in ledger:
        if text(d["Result"]):
            continue
        rets = rets_by_symbol.get(text(d["Symbol"]).upper()) or []
        nxt = next(((day, r) for day, r in rets if day.isoformat() > text(d["Session"])), None)
        if nxt is None:
            continue
        d["Next session"], d["Next %"] = nxt[0].isoformat(), round(nxt[1], 2)
        d["Result"] = result_of(text(d["Read"]), nxt[1])
        done += 1
    return done


def score(ledger: Sequence[Dict[str, Any]]) -> Dict[str, int]:
    res = [text(d["Result"]) for d in ledger]
    return {"hit": res.count("HIT"), "miss": res.count("MISS"), "none": res.count("-"),
            "open": sum(1 for x in res if not x)}


def score_line(sc: Dict[str, int]) -> str:
    n = sc["hit"] + sc["miss"]
    if not n:
        return ("Score: no directional read has been marked yet (%d read(s) gave no direction, %d wait for the next "
                "close)." % (sc["none"], sc["open"]))
    return ("Score: %d of %d directional reads were right (%.0f%%); %d read(s) gave no direction; %d wait for the "
            "next close. Under about 55%% over 40 reads or more, the read is no better than a coin."
            % (sc["hit"], n, 100.0 * sc["hit"] / n, sc["none"], sc["open"]))


# ----------------------------------------------------------------------------- #
# Page
# ----------------------------------------------------------------------------- #
def page_rows(results: Sequence[Dict[str, Any]], info: Dict[str, str]) -> List[List[Any]]:
    rows: List[List[Any]] = [
        ["HOLDING READ - what moved each holding last session, and the read for the next one"],
        [info["status"]],
        ["How to read: Driver says what the move followed. The next-session read is a count from this stock's own "
         "past days of the same kind - how often the next session closed up - not a forecast of size. NO EDGE means "
         "history gives no reason to expect either direction. Orders come from Today_Action_Plan only."],
        [info["score"]],
        [],
        list(PAGE_HEAD),
    ]
    for r in results:
        rows.append([
            r["symbol"], r.get("name", ""), r["session"].isoformat() if r.get("session") else "",
            pct(r.get("r"), 2), pct(r.get("m"), 2), pct(r.get("s"), 2), r["driver"], r["news"], r["label"],
            ("%.0f%% of %d (any day %.0f%%)" % (r["p"], r["n"], r["base"])) if r.get("p") is not None else "",
            r.get("why", ""), r.get("after", "")])
    return rows


def ledger_row(r: Dict[str, Any], stamp: str) -> List[Any]:
    rnd = lambda v: "" if v is None else round(v, 2)  # noqa: E731
    return [r["session"].isoformat(), r["symbol"], rnd(r.get("r")), rnd(r.get("m")), rnd(r.get("s")), r["driver"],
            text(r["news"])[:300], r["label"], rnd(r.get("p")), r.get("n") or "", rnd(r.get("base")), "", "", "",
            stamp]


# ----------------------------------------------------------------------------- #
# IO
# ----------------------------------------------------------------------------- #
def _default_opener(req: urllib.request.Request, timeout: float):  # pragma: no cover - network
    return urllib.request.urlopen(req, timeout=timeout)


def fetch_history(ysym: str, opener: Callable[..., Any], rng: str, now_ts: float, timeout: float = 20.0
                  ) -> List[Tuple[Any, ...]]:
    """Completed daily bars (an open session's bar is set aside); [] when no host answers or the symbol is unknown."""
    for host in YAHOO_HOSTS:
        req = urllib.request.Request("https://" + host + HISTORY_PATH.format(sym=ysym, rng=rng),
                                     headers={"User-Agent": YAHOO_UA, "Accept": "application/json"})
        try:
            with opener(req, timeout) as resp:
                payload = json.loads(resp.read().decode("utf-8", "replace"))
            bars = parse_chart(payload)
            if bars:
                return list(split_open_session(payload, bars, now_ts)[0])
        except Exception as exc:  # noqa: BLE001
            if getattr(exc, "code", None) == 404:
                return []
        _SLEEP(1.0)
    return []


def run(dry_run: bool, book=None, now: Optional[_dt.datetime] = None,
        opener: Optional[Callable[..., Any]] = None) -> int:
    md = mode()
    if md == "off":
        _out("TFB_HOLDING_READ is off - nothing read, nothing written")
        return 0
    k = knobs()
    write = md == "on" and not dry_run
    now = now or _dt.datetime.now(UTC)
    today = now.astimezone(RIYADH).date()
    stamp = riyadh_str(now)
    opener = opener or _default_opener
    if book is None:
        from scripts.run_calendar_sync import _open_book  # noqa: PLC0415
        try:
            from scripts import sheets_quota  # noqa: PLC0415
            book = sheets_quota.call(_open_book, _SLEEP, _out, "opening the workbook")   # 1.0.2
        except Exception as e:  # noqa: BLE001
            _out("ERROR: cannot open workbook: %s%s" % (type(e).__name__, _status_of(e)))
            return 2
    for attempt in range(1, READ_ATTEMPTS + 1):
        try:
            lots = read_lots(_read(book, "_Portfolio_CostBasis", "A1:Z600"))
            try:
                info_map = read_info(read_columns(book, "My_Portfolio", INFO_COLS))
            except Exception as e:  # noqa: BLE001
                if type(e).__name__ != "WorksheetNotFound":
                    raise
                info_map = {}
            items = read_items(_read(book, "_News_Items", "A1:P12000"))
            gate = read_gate(_read(book, "_News_Gate", "A1:L4000"))
            cal = read_calendar(_read(book, "Calendar_Events", "A1:J4000"), _read(book, "Dividend_Events", "A1:J4000"),
                                today)
            ledger = parse_ledger(_read(book, LEDGER_TAB, "A1:%s%d" % (_col(len(LEDGER_HEADERS)), MAX_LEDGER_ROWS + 1)))
            break
        except Exception as e:  # noqa: BLE001
            if attempt == READ_ATTEMPTS:
                _out("ERROR: cannot read the workbook: %s; nothing written" % type(e).__name__)
                return 2
            from scripts.sheets_quota import wait_for  # noqa: PLC0415
            wait = wait_for(e, attempt) or READ_RETRY_S
            _out("read failed (%s) - attempt %d of %d, retrying in %d s" % (type(e).__name__, attempt,
                                                                          READ_ATTEMPTS, wait))
            _SLEEP(wait)
    cache: Dict[str, List[Tuple[Any, ...]]] = {}
    now_ts = now.timestamp()

    def bars_of(ysym: str) -> List[Tuple[Any, ...]]:
        if not ysym:
            return []
        if ysym not in cache:
            cache[ysym] = fetch_history(ysym, opener, k["range"], now_ts)
        return cache[ysym]

    results: List[Dict[str, Any]] = []
    rets_by_symbol: Dict[str, List[Tuple[_dt.date, float]]] = {}
    for sym, lot in sorted(lots.items()):
        inf = info_map.get(sym) or {}
        name = inf.get("name") or lot.get("name", "")
        if any(c in text(inf.get("asset")).lower() for c in NO_STOP_CLASSES):
            results.append({"symbol": sym, "name": name, "ok": False, "driver": "NOT READ", "session": None,
                            "news": "%s: a daily read is not made for fixed income" % inf.get("asset"),
                            "label": "", "why": "", "after": ""})
            continue
        mkt_sym, mkt_name = market_symbol(sym)
        fund = sector_fund(sym, inf.get("group", ""), k["sector_map"])
        bars = bars_of(yahoo_symbol(sym))
        rets_by_symbol[sym] = returns(bars)
        results.append(read_holding(sym, name, bars, bars_of(mkt_sym), bars_of(fund), items.get(sym) or [],
                                    cal.get(sym) or {}, gate, inf.get("group", ""), fund, mkt_name, k))
    # A read whose holding has since been sold is still marked: its bars are read too (at most MAX_PENDING symbols).
    pending = sorted({text(d["Symbol"]).upper() for d in ledger if not text(d["Result"])} - set(rets_by_symbol))
    for sym in pending[:MAX_PENDING]:
        rets_by_symbol[sym] = returns(bars_of(yahoo_symbol(sym)))
    marked = settle(ledger, rets_by_symbol)
    ok = [r for r in results if r.get("ok")]
    keys = {(r["session"].isoformat(), r["symbol"]) for r in ok}
    keep = [d for d in ledger if (text(d["Session"]), text(d["Symbol"]).upper()) not in keys]
    grid = [list(LEDGER_HEADERS)] + ([[d[h] for h in LEDGER_HEADERS] for d in keep]
                                     + [ledger_row(r, stamp) for r in ok])[-MAX_LEDGER_ROWS:]
    sessions = sorted({r["session"] for r in ok})
    info = {"status": "As of %s Riyadh · last session %s · news gate %s%s · holding_read %s" % (
                stamp, sessions[-1].isoformat() if sessions else "n/a", gate.get("verdict") or "not read",
                " (%s)" % gate["market"] if gate.get("market") else "", __version__),
            "score": score_line(dict(score(keep), open=score(keep)["open"] + len(ok)))}
    detail = log_detail()                                                            # 1.0.1 [LOG-PRIVACY]
    for i, r in enumerate(results, 1):
        if detail:
            _out("%-9s %-13s stock %s market %s group %s | %s%s" % (
                r["symbol"], r["driver"], pct(r.get("r"), 2) or "n/a", pct(r.get("m"), 2) or "n/a",
                pct(r.get("s"), 2) or "n/a", r.get("label") or "no read",
                (" (up %.0f%% of %d)" % (r["p"], r["n"])) if r.get("p") is not None else ""))
        else:
            _out("holding %d of %d: %s | %s" % (i, len(results), r["driver"], r.get("label") or "no read"))
    _out("%d earlier read(s) marked; %s" % (marked, info["score"]))
    if not write:
        _out("%s - nothing written" % ("dry run" if dry_run else "observe"))
        return 0
    from scripts import sheets_quota  # noqa: PLC0415

    def publish() -> None:                     # both writes overwrite their whole range: safe to repeat (1.0.1)
        write_grid(book, LEDGER_TAB, grid, len(LEDGER_HEADERS), 200)
        write_grid(book, PAGE_TAB, page_rows(results, info), PAGE_WIDTH, 40)
    try:
        sheets_quota.call(publish, _SLEEP, _out, "writing the Holding Read")
    except Exception as e:  # noqa: BLE001
        _out("ERROR: publication unconfirmed: %s; read back before retrying" % type(e).__name__)
        return 3
    _out("wrote %d read(s) and the %s page" % (len(ok), PAGE_TAB))
    return 0


def main(argv: Optional[Sequence[str]] = None) -> int:
    ap = argparse.ArgumentParser(description="TFB Holding Read: what moved each holding, and the next-session read")
    ap.add_argument("--dry-run", action="store_true", help="compute and print only")
    a = ap.parse_args(argv)
    return run(a.dry_run)


if __name__ == "__main__":
    sys.exit(main())
