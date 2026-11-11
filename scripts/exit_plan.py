#!/usr/bin/env python3
"""
scripts/exit_plan.py - TFB Exit Plan
====================================
VERSION 1.0.0 (2026-11-11) - NEW SCRIPT

WHY (owner, Wed 11 Nov 2026 09:30 Riyadh): fixed stop-loss and take-profit
levels do not fit every stock. Each holding needs its own levels from its own
volatility, the engine's forecast and confidence, and the news; and every day
after the close the stop and the target should be reviewed and raised, so a
stock that has climbed well above its entry does not fall back to the stop set
at the purchase. The recommendation is shown in the workbook and sent by
e-mail every morning.

Evidence for the numbers (project doc TFB_Stop_Loss_and_Take_Profit_Review_
2026-11-11; INVESTABLE records outside Tadawul, next-open entry, daily bars):
  - stop -5% or 2xATR: 43-46% of entries stopped, win rate about 45%;
    stop -8%: 22% stopped, -10%: 15%, -12%: 12% (20 sessions, n=190);
  - a trail of 3xATR after +1R had the best 45-session result (+2.50%, win
    59.3%, n=108); a 2xATR trail ended 66-94% of entries;
  - the engine's 12-month target (about +30%) was never reached in 20
    sessions; +8% was reached by 38% of entries;
  - the real book's broker stops sat 4.7-8.4% under cost; AER, SBAC and DDI
    rose 14-25% after their stops filled.

THE RULE (per holding, recomputed after every close):
  - R (the initial risk) = TFB_EXIT_ATR_INIT x ATR14 at the purchase, kept
    between TFB_EXIT_MIN_PCT and TFB_EXIT_MAX_PCT % of the entry (Tadawul
    names at most TFB_EXIT_SR_MAX_PCT %). Initial stop = entry - R.
  - Once the highest close since the purchase is 1 R above the entry, the
    stop is at least the entry plus TFB_EXIT_BE_PCT % and trails
    TFB_EXIT_TRAIL_ATR x ATR14 (today's) under that highest close.
  - The trail tightens to TFB_EXIT_TRAIL_TIGHT_ATR x ATR14 when any of these
    holds: negative news on the symbol in the last TFB_EXIT_NEWS_DAYS days
    (direction -1, severity H or M); the engine's 1-month forecast is below
    the last close with forecast reliability TFB_EXIT_FC_REL_MIN or more; the
    gain at the high reached 2 R and the last close is under the 20-day
    average. The tightening rules are judgment, not yet measured; they only
    ever raise a stop.
  - The stop never moves down: it is at least yesterday's recommended stop
    for the same lot.
  - Target: the next whole R step above the highest close (the first is
    entry + 1 R, where half the position is sold); each step crossed raises
    the target one step.
  - Actions: SELL (the last close is at or below the stop), TAKE PROFIT (the
    first target was reached for the first time: sell half), RAISE STOP (the
    stop moved up), REVIEW (TFB_EXIT_TIME_SESSIONS sessions without +1 R),
    HOLD. Earnings inside TFB_EXIT_EARN_WARN_DAYS days add a warning: an
    earnings gap can jump any stop.
  - Fixed income (sukuk, bonds, funds) has no stop rule.
  - Prices are dividend-adjusted (Yahoo's adjusted close), and so is the
    entry (the buy price times the adjustment factor on the buy date), so a
    dividend - 10 Nov: SUPN's 8.13 special dividend - neither reads as a loss
    nor leaves a pre-dividend high that would put the stop above the price.
TODAY'S PLAN PICKS: the same initial stop and first target for the Daily
Plan's open picks, beside the plan's fixed stop.

Reads only: _Portfolio_CostBasis, My_Portfolio, Global_Markets,
Market_Leaders, Calendar_Events, Dividend_Events, _News_Items, _IBKR_Account,
_IBKR_Orders, _Daily_Plan and its own ledger. Daily bars from the Yahoo chart
API. Writes only its own two tabs: _Exit_Plan (the ledger) and Exit_Plan (the
page). Sends one e-mail when TFB_EXIT_MAIL=1. Never calls a broker, places,
changes or cancels no order.

GATE: TFB_EXIT_PLAN = off (default) | observe (compute and print, write and
send nothing) | on. Rollback: repository variable TFB_EXIT_PLAN=off.
"""
from __future__ import annotations

import argparse
import datetime as _dt
import html
import json
import math
import os
import sys
import time
import urllib.request
from typing import Any, Callable, Dict, List, Optional, Sequence, Tuple

if __package__ in (None, ""):
    sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from scripts.daily_plan import (  # noqa: E402
    _col, _read, cell, fmt, num, parse_date, read_calendar, riyadh_str, table_index, text, write_grid,
)

__version__ = "1.0.0"

UTC = _dt.timezone.utc
RIYADH = _dt.timezone(_dt.timedelta(hours=3))
LEDGER_TAB = "_Exit_Plan"
PAGE_TAB = "Exit_Plan"
PAGE_WIDTH = 15
MAX_LEDGER_ROWS = 3000
LEDGER_HEADERS = [
    "Date", "Run At (Riyadh)", "Symbol", "Lot", "Entry", "Shares", "Last", "Highest Close", "ATR", "R",
    "Stop", "Target", "TP1 Reached", "Action", "Tighten", "Bars To",
]
INFO_COLS = ("Symbol", "Name", "Asset Class", "Current Price", "Forecast Price (1M)", "Forecast Reliability Score",
             "Confidence Score", "Target Price")
YAHOO_HOSTS = ("query1.finance.yahoo.com", "query2.finance.yahoo.com")
YAHOO_PATH = "/v8/finance/chart/{sym}?range=1y&interval=1d"
YAHOO_UA = "Mozilla/5.0 (tfb-exit-plan)"
READ_ATTEMPTS = 3
READ_RETRY_S = 20
_SLEEP = time.sleep
NO_STOP_CLASSES = ("fixed income", "sukuk", "bond", "fund", "money market")


def _out(msg: str) -> None:
    print("[exit_plan %s] %s" % (__version__, msg), flush=True)


def _env(name: str, default: str = "") -> str:
    return (os.environ.get(name) or default).strip()


def _env_num(name: str, default: float, lo: float, hi: float) -> float:
    try:
        v = float(_env(name, str(default)))
    except ValueError:
        return default
    return min(hi, max(lo, v)) if math.isfinite(v) else default


def mode() -> str:
    v = _env("TFB_EXIT_PLAN", "off").lower()
    return v if v in ("off", "observe", "on") else "off"


def knobs() -> Dict[str, Any]:
    k = {
        "atr_init": _env_num("TFB_EXIT_ATR_INIT", 3.0, 1.0, 6.0),
        "min_pct": _env_num("TFB_EXIT_MIN_PCT", 8.0, 2.0, 30.0),
        "max_pct": _env_num("TFB_EXIT_MAX_PCT", 12.0, 2.0, 30.0),
        "sr_max_pct": _env_num("TFB_EXIT_SR_MAX_PCT", 8.0, 2.0, 30.0),
        "trail": _env_num("TFB_EXIT_TRAIL_ATR", 3.0, 1.0, 6.0),
        "trail_tight": _env_num("TFB_EXIT_TRAIL_TIGHT_ATR", 2.0, 1.0, 6.0),
        "be_pct": _env_num("TFB_EXIT_BE_PCT", 0.3, 0.0, 5.0),
        "time_sessions": int(_env_num("TFB_EXIT_TIME_SESSIONS", 45, 5, 250)),
        "news_days": int(_env_num("TFB_EXIT_NEWS_DAYS", 3, 0, 30)),
        "fc_rel_min": _env_num("TFB_EXIT_FC_REL_MIN", 70, 0, 100),
        "earn_warn_days": int(_env_num("TFB_EXIT_EARN_WARN_DAYS", 5, 0, 30)),
        "mail": _env("TFB_EXIT_MAIL", "0") in ("1", "true", "on", "yes"),
    }
    k["min_pct"] = min(k["min_pct"], k["max_pct"])
    k["trail_tight"] = min(k["trail_tight"], k["trail"])
    return k


# ----------------------------------------------------------------------------- #
# Bars and the rule (pure)
# ----------------------------------------------------------------------------- #
Bar = Tuple[_dt.date, float, float, float, float, float]   # date, open, high, low, close, adjustment factor


def parse_chart(payload: Dict[str, Any]) -> List[Bar]:
    """Daily bars from a Yahoo v8 chart payload, dividend-adjusted when the payload carries the adjusted
    close (factor = adjusted close / close, applied to all four prices); a bar without all four prices is
    dropped."""
    try:
        r = payload["chart"]["result"][0]
        ts = r.get("timestamp") or []
        q = r["indicators"]["quote"][0]
        off = int((r.get("meta") or {}).get("gmtoffset") or 0)
    except (KeyError, IndexError, TypeError, ValueError):
        return []
    try:
        adj = r["indicators"]["adjclose"][0]["adjclose"]
    except (KeyError, IndexError, TypeError):
        adj = None
    out: Dict[_dt.date, Bar] = {}
    for i, t in enumerate(ts):
        try:
            o, h, lo, c = (float(q[key][i]) for key in ("open", "high", "low", "close"))
        except (TypeError, ValueError, KeyError, IndexError):
            continue
        if not all(math.isfinite(x) and x > 0 for x in (o, h, lo, c)) or h < lo:
            continue
        f = 1.0
        try:
            a = float(adj[i]) if adj is not None else None
            if a is not None and math.isfinite(a) and a > 0:
                f = a / c
        except (TypeError, ValueError, IndexError):
            f = 1.0
        if not 0.2 <= f <= 1.5:
            f = 1.0                                      # an implausible factor is not applied
        d = _dt.datetime.fromtimestamp(int(t) + off, UTC).date()
        out[d] = (d, o * f, h * f, lo * f, c * f, f)
    return [out[d] for d in sorted(out)]


def atr(bars: Sequence[Bar], end: int, n: int = 14) -> Optional[float]:
    """Average true range of the n bars ending at index `end` (inclusive); None with fewer than n/2."""
    trs = []
    for i in range(max(1, end - n + 1), end + 1):
        h, lo = bars[i][2], bars[i][3]
        pc = bars[i - 1][4]
        trs.append(max(h - lo, abs(h - pc), abs(lo - pc)))
    return sum(trs) / len(trs) if len(trs) >= max(2, n // 2) else None


def yahoo_symbol(symbol: str) -> str:
    s = text(symbol).upper()
    if s.endswith(".US"):
        return s[:-3].replace(".", "-")
    return s


def _round(x: Optional[float]) -> Optional[float]:
    return None if x is None else round(x, 4)


def exit_levels(symbol: str, entry: float, entry_date: _dt.date, bars: Sequence[Bar], info: Dict[str, Any],
                prev: Dict[str, Any], k: Dict[str, Any], today: _dt.date) -> Dict[str, Any]:
    """The stop, target and action for one lot. `prev` = the last ledger row for the same lot (or {})."""
    res: Dict[str, Any] = {"symbol": symbol, "entry": entry, "ok": False, "why": [], "tighten": []}
    if not bars or entry <= 0:
        res["action"], res["why"] = "CHECK", ["no daily price history"]
        return res
    idx_e = max((i for i, b in enumerate(bars) if b[0] <= entry_date), default=None)
    f_e = bars[idx_e][5] if idx_e is not None and len(bars[idx_e]) > 5 else 1.0
    buy = entry
    entry = buy * f_e
    res["entry"] = entry
    a_e = atr(bars, idx_e) if idx_e is not None else None
    if a_e is None:
        a_e = atr(bars, min(len(bars) - 1, 14))      # bought before the history starts: the earliest ATR
    a_now = atr(bars, len(bars) - 1)
    if a_e is None or a_now is None:
        res["action"], res["why"] = "CHECK", ["not enough daily bars for an ATR"]
        return res
    hi_pct = k["sr_max_pct"] if symbol.upper().endswith(".SR") else k["max_pct"]
    lo_pct = min(k["min_pct"], hi_pct)
    dist_pct = min(hi_pct, max(lo_pct, k["atr_init"] * a_e / entry * 100.0))
    r = entry * dist_pct / 100.0
    init_stop = entry - r
    since = [b for b in bars if b[0] >= entry_date] or [bars[-1]]
    hc = max(b[4] for b in since)
    after = [b for b in bars if b[0] > entry_date]      # the buy day's high may come before the fill
    hh = max((b[2] for b in after), default=0.0)
    last = bars[-1]
    closes = [b[4] for b in bars[-20:]]
    sma20 = sum(closes) / len(closes)
    r_high = (hc - entry) / r
    news_neg = int(info.get("news_neg") or 0)
    if news_neg:
        res["tighten"].append("negative news (%d item%s in %d days)" % (
            news_neg, "" if news_neg == 1 else "s", k["news_days"]))
    fc, rel = info.get("forecast_1m"), info.get("reliability")
    if fc is not None and rel is not None and rel >= k["fc_rel_min"] and fc < last[4]:
        res["tighten"].append("1-month forecast %s is below the last close (reliability %.0f)" % (fmt(fc), rel))
    if r_high >= 2 and last[4] < sma20:
        res["tighten"].append("close under its 20-day average after +%.1fR" % r_high)
    trail_k = k["trail_tight"] if res["tighten"] else k["trail"]
    stop = init_stop
    if r_high >= 1:
        stop = max(stop, entry * (1 + k["be_pct"] / 100.0), hc - trail_k * a_now)
    prev_stop = num(prev.get("Stop")) if prev else None
    if prev_stop is not None and prev_stop > stop:
        stop = prev_stop                                   # the stop never moves down
    step = max(1, int(math.floor(r_high)) + 1)
    target = entry + step * r
    tp1 = hh >= entry + r
    touch = next((b for b in after if b[2] >= entry + r), None)
    tp1_now = last[4] >= entry + r or (after and last[0] == after[-1][0] and last[2] >= entry + r)
    sessions = sum(1 for b in bars if b[0] > entry_date)
    res.update({"ok": True, "atr": a_now, "atr_entry": a_e, "atr_pct": a_now / last[4] * 100.0, "r": r,
                "dist_pct": dist_pct, "init_stop": init_stop, "hc": hc, "last": last[4], "bars_to": last[0],
                "r_high": r_high, "stop": stop, "prev_stop": prev_stop, "target": target, "tp1": tp1,
                "trail_k": trail_k, "sessions": sessions, "pl_pct": (last[4] / entry - 1) * 100.0,
                "stop_gap_pct": (last[4] / stop - 1) * 100.0})
    tp1_before = text(prev.get("TP1 Reached")).lower() == "yes" if prev else False
    if last[4] <= stop:
        res["action"] = "SELL"
        res["why"].append("last close %s is at or below the stop %s" % (fmt(last[4]), fmt(stop)))
    elif tp1 and not tp1_before and tp1_now:
        res["action"] = "TAKE PROFIT"
        res["why"].append("first target %s reached (+1R): sell half; the stop rises to at least the entry"
                          % fmt(entry + r))
    elif prev_stop is not None and stop > prev_stop * 1.0025:
        res["action"] = "RAISE STOP"
        res["why"].append("raise the stop %s -> %s" % (fmt(prev_stop), fmt(stop)))
    elif prev_stop is None and r_high >= 1:
        res["action"] = "RAISE STOP"
        res["why"].append("the stop belongs at %s (+%.1fR at the high)" % (fmt(stop), r_high))
    elif sessions >= k["time_sessions"] and r_high < 1:
        res["action"] = "REVIEW"
        res["why"].append("%d sessions without +1R" % sessions)
    else:
        res["action"] = "HOLD"
        res["why"].append("+%.1fR at the high; stop %.1f%% under the last close" % (
            max(0.0, r_high), res["stop_gap_pct"]))
    if tp1 and not tp1_now and not tp1_before and touch is not None:
        res["why"].append("the first target %s was touched on %s (high %s) and the price is back under it" % (
            fmt(entry + r), touch[0].isoformat(), fmt(touch[2])))
    if f_e < 0.995:
        res["why"].append("entry %s is the buy price %s adjusted for the dividends paid since" % (fmt(entry), fmt(buy)))
    if res["tighten"]:
        res["why"].append("trail tightened to %gxATR: %s" % (trail_k, "; ".join(res["tighten"])))
    ed = info.get("earn_days")
    if ed is not None and 0 <= ed <= k["earn_warn_days"]:
        res["why"].append("earnings %s: a gap can jump the stop - decide to hold through or sell half" % (
            "today" if ed == 0 else "in %d day(s)" % ed))
    bs = info.get("broker_stop")
    if bs is not None and bs < stop * 0.9975:
        res["why"].append("broker stop %s is under the plan's %s" % (fmt(bs), fmt(stop)))
    return res


def pick_levels(symbol: str, entry: float, bars: Sequence[Bar], k: Dict[str, Any]) -> Dict[str, Any]:
    """Initial stop and first target for a new pick, from its own ATR."""
    if not bars or entry <= 0:
        return {"ok": False}
    a = atr(bars, len(bars) - 1)
    if a is None:
        return {"ok": False}
    hi_pct = k["sr_max_pct"] if symbol.upper().endswith(".SR") else k["max_pct"]
    dist = min(hi_pct, max(min(k["min_pct"], hi_pct), k["atr_init"] * a / entry * 100.0))
    r = entry * dist / 100.0
    return {"ok": True, "stop": entry - r, "dist_pct": dist, "target": entry + r, "atr_pct": a / entry * 100.0}


# ----------------------------------------------------------------------------- #
# Workbook parsers (pure)
# ----------------------------------------------------------------------------- #
def read_lots(values: Sequence[Sequence[Any]]) -> Dict[str, Dict[str, Any]]:
    """Active lots per symbol: shares, the weighted buy price, the latest buy date."""
    h, col = table_index(values, ("Symbol", "Status", "Buy Date", "Buy Price", "Shares"))
    out: Dict[str, Dict[str, Any]] = {}
    if h is None:
        return out
    for r in list(values)[h + 1:]:
        sym = text(cell(r, col.get("Symbol"))).upper()
        if not sym or text(cell(r, col.get("Status"))).lower() != "active":
            continue
        px, sh = num(cell(r, col.get("Buy Price"))), num(cell(r, col.get("Shares")))
        bd = parse_date(cell(r, col.get("Buy Date")))
        if not px or not sh or px <= 0 or sh <= 0 or bd is None:
            continue
        o = out.setdefault(sym, {"symbol": sym, "name": text(cell(r, col.get("Name"))), "shares": 0.0,
                                 "cost": 0.0, "date": bd, "ccy": text(cell(r, col.get("Ccy")))})
        o["shares"] += sh
        o["cost"] += px * sh
        o["date"] = max(o["date"], bd)
    for o in out.values():
        o["buy"] = o["entry"] = o["cost"] / o["shares"]
    return out


def read_info(grids: Sequence[Sequence[Sequence[Any]]]) -> Dict[str, Dict[str, Any]]:
    """{SYMBOL: name, asset class, forecast 1M, reliability, confidence, target}; the first page listing it wins."""
    out: Dict[str, Dict[str, Any]] = {}
    for g in grids:
        h, col = table_index(g, ("Symbol",))
        if h is None:
            continue
        rel_col = next((c for name, c in col.items() if name.startswith("Forecast Reliability")), None)
        for r in list(g)[h + 1:]:
            sym = text(cell(r, col.get("Symbol"))).upper()
            if not sym or sym in out:
                continue
            out[sym] = {"name": text(cell(r, col.get("Name"))), "asset": text(cell(r, col.get("Asset Class"))),
                        "price": num(cell(r, col.get("Current Price"))),
                        "forecast_1m": num(cell(r, col.get("Forecast Price (1M)"))),
                        "reliability": num(cell(r, rel_col)), "confidence": num(cell(r, col.get("Confidence Score"))),
                        "target": num(cell(r, col.get("Target Price")))}
    return out


def read_news(values: Sequence[Sequence[Any]], now: _dt.datetime, days: int) -> Dict[str, Dict[str, Any]]:
    """Negative symbol news (direction -1, severity H or M) published in the last `days` days."""
    h, col = table_index(values, ("Scope", "Direction", "Severity"))
    out: Dict[str, Dict[str, Any]] = {}
    if h is None or days <= 0:
        return out
    since = now - _dt.timedelta(days=days)
    for r in list(values)[h + 1:]:
        scope = text(cell(r, col.get("Scope"))).upper()
        if not scope or scope == "MACRO":
            continue
        if num(cell(r, col.get("Direction"))) != -1 or text(cell(r, col.get("Severity"))).upper() not in ("H", "M"):
            continue
        pub = _parse_ts(cell(r, col.get("Published (UTC)")))
        if pub is None or pub < since:
            continue
        o = out.setdefault(scope, {"n": 0, "headline": "", "at": None})
        o["n"] += 1
        if o["at"] is None or pub > o["at"]:
            o["at"], o["headline"] = pub, text(cell(r, col.get("Headline")))[:90]
    return out


def _parse_ts(v: Any) -> Optional[_dt.datetime]:
    """'2026-11-10 16:30' / '2026-11-10T16:30...' / '2026-11-10' as UTC; None otherwise."""
    s = text(v)
    for f, n in (("%Y-%m-%d %H:%M", 16), ("%Y-%m-%dT%H:%M", 16), ("%Y-%m-%d", 10)):
        try:
            return _dt.datetime.strptime(s[:n], f).replace(tzinfo=UTC)
        except ValueError:
            continue
    return None


def read_broker_stops(account: Sequence[Sequence[Any]], orders: Sequence[Sequence[Any]],
                      now: _dt.datetime, max_age_h: float = 30.0) -> Tuple[Dict[str, float], str]:
    """Working SELL STOP prices per symbol when the _IBKR_Account read is fresh, else ({}, why)."""
    as_of = None
    for r in account:
        if text(cell(r, 0)).lower() == "as_of":
            s = text(cell(r, 1))
            try:
                as_of = _dt.datetime.strptime(s[:16], "%Y-%m-%d %H:%M").replace(tzinfo=RIYADH)
            except ValueError:
                as_of = None
    if as_of is None:
        return {}, "broker orders unknown (no _IBKR_Account read)"
    age = (now - as_of).total_seconds() / 3600.0
    if age > max_age_h:
        return {}, "broker orders unknown (last read %s, %.0f h old)" % (as_of.strftime("%Y-%m-%d %H:%M"), age)
    h, col = table_index(orders, ("symbol", "side", "type", "price", "status"))
    out: Dict[str, float] = {}
    if h is None:
        return out, "broker orders read %s" % as_of.strftime("%Y-%m-%d %H:%M")
    for r in list(orders)[h + 1:]:
        if text(cell(r, col.get("side"))).upper() != "SELL" or "STOP" not in text(cell(r, col.get("type"))).upper():
            continue
        if text(cell(r, col.get("status"))).upper() in ("CANCELLED", "FILLED", "INACTIVE"):
            continue
        sym = text(cell(r, col.get("symbol"))).upper()
        sym = sym if "." in sym else sym + ".US"
        px = num(cell(r, col.get("price")))
        if px:
            out[sym] = max(px, out.get(sym, 0.0))
    return out, "broker orders read %s" % as_of.strftime("%Y-%m-%d %H:%M")


def read_plan_picks(values: Sequence[Sequence[Any]]) -> List[Dict[str, Any]]:
    """The latest plan's PICK rows that are still open (not EXECUTED / INVALIDATED)."""
    h, col = table_index(values, ("Plan Date", "Kind", "Symbol", "Entry"))
    if h is None:
        return []
    rows = [r for r in list(values)[h + 1:] if text(cell(r, col.get("Kind"))) == "PICK"]
    dates = sorted({text(cell(r, col.get("Plan Date"))) for r in rows if parse_date(cell(r, col.get("Plan Date")))})
    if not dates:
        return []
    out = []
    for r in rows:
        if text(cell(r, col.get("Plan Date"))) != dates[-1]:
            continue
        if text(cell(r, col.get("Status"))).upper() in ("EXECUTED", "INVALIDATED"):
            continue
        out.append({"date": dates[-1], "symbol": text(cell(r, col.get("Symbol"))).upper(),
                    "entry": num(cell(r, col.get("Entry"))), "qty": num(cell(r, col.get("Qty"))),
                    "stop": num(cell(r, col.get("Stop"))), "ccy": text(cell(r, col.get("Ccy"))).upper()})
    return out


def tp1_flag(r: Dict[str, Any]) -> str:
    """'yes' once TAKE PROFIT was issued for the lot (on this run or an earlier one): it is issued once. A touch
    that the price has already left does not count, so the action still comes if the price climbs back."""
    return "yes" if r.get("action") == "TAKE PROFIT" or text((r.get("prev") or {}).get("TP1 Reached")).lower() == \
        "yes" else "no"


def parse_ledger(values: Sequence[Sequence[Any]]) -> List[Dict[str, Any]]:
    rows = list(values)
    if not rows or [text(c) for c in rows[0]][:len(LEDGER_HEADERS)] != LEDGER_HEADERS:
        return []
    return [{h: (r[i] if i < len(r) else "") for i, h in enumerate(LEDGER_HEADERS)} for r in rows[1:] if any(
        text(c) for c in r)]


def lot_key(symbol: str, lot: Dict[str, Any]) -> str:
    return "%s|%s|%.4f" % (symbol, lot["date"].isoformat(), lot["buy"])


def prior_row(ledger: Sequence[Dict[str, Any]], key: str, today: _dt.date) -> Dict[str, Any]:
    """The lot's last ledger row before today, carrying the highest stop ever recommended for it."""
    rows = [d for d in ledger if text(d["Lot"]) == key and (parse_date(d["Date"]) or today) < today]
    if not rows:
        return {}
    last = dict(rows[-1])
    stops = [num(d["Stop"]) for d in rows if num(d["Stop"]) is not None]
    if stops:
        last["Stop"] = max(stops)
    if any(text(d["TP1 Reached"]).lower() == "yes" for d in rows):
        last["TP1 Reached"] = "yes"
    return last


# ----------------------------------------------------------------------------- #
# Output
# ----------------------------------------------------------------------------- #
ORDER = {"SELL": 0, "TAKE PROFIT": 1, "RAISE STOP": 2, "REVIEW": 3, "CHECK": 4, "HOLD": 5, "NO RULE": 6}


def page_rows(results: List[Dict[str, Any]], picks: List[Dict[str, Any]], info: Dict[str, Any],
              k: Dict[str, Any]) -> List[List[Any]]:
    w = PAGE_WIDTH
    rows: List[List[Any]] = [
        ["EXIT PLAN - stop and target per holding, reviewed after every close"],
        [info["headline"]],
        ["Rule: initial stop %gxATR at the purchase, kept between %g%% and %g%% of the entry (Tadawul at most %g%%); "
         "after +1R the stop is at least the entry and trails %gxATR under the highest close (%gxATR on negative "
         "news, a falling forecast or a lost 20-day average after +2R); the stop never moves down; target = the "
         "next +1R step (sell half at the first)." % (k["atr_init"], k["min_pct"], k["max_pct"], k["sr_max_pct"],
                                                      k["trail"], k["trail_tight"])],
        [info["summary"]],
        [],
        ["HOLDINGS (%d)" % len(results)],
        ["Symbol", "Name", "Qty", "Entry", "Last", "P/L %", "Highest close", "ATR %", "Gain at high (R)", "Stop",
         "Was", "Target", "Action", "Why", "Broker stop"],
    ]
    for r in sorted(results, key=lambda x: (ORDER.get(x["action"], 9), x["symbol"])):
        if not r.get("ok"):
            rows.append([r["symbol"], r.get("name", ""), fmt(r.get("shares"), 0), fmt(r.get("entry")), "", "", "", "",
                         "", "", "", "", r["action"], "; ".join(r["why"]), ""])
            continue
        rows.append([r["symbol"], r.get("name", ""), fmt(r.get("shares"), 0), fmt(r["entry"]), fmt(r["last"]),
                     "%+.1f" % r["pl_pct"], fmt(r["hc"]), "%.1f" % r["atr_pct"], "%+.1f" % r["r_high"],
                     fmt(r["stop"]), fmt(r["prev_stop"]) if r["prev_stop"] is not None else "—",
                     fmt(r["target"]), r["action"], "; ".join(r["why"]),
                     fmt(r.get("broker_stop")) if r.get("broker_stop") is not None else "unknown"])
    rows += [[], ["TODAY'S PLAN PICKS - per-stock levels (%d)" % len(picks)],
             ["Symbol", "Entry", "Plan stop", "Per-stock stop", "Stop %", "First target", "Target %", "ATR %",
              "Plan qty", "Risk at plan qty", "Note"]]
    for p in picks:
        lv = p.get("levels") or {}
        if not lv.get("ok"):
            rows.append([p["symbol"], fmt(p.get("entry")), fmt(p.get("stop")), "", "", "", "", "", fmt(p.get("qty"), 0),
                         "", "no daily price history"])
            continue
        risk = (p["entry"] - lv["stop"]) * (p.get("qty") or 0.0)
        rows.append([p["symbol"], fmt(p["entry"]), fmt(p.get("stop")), fmt(lv["stop"]), "-%.1f" % lv["dist_pct"],
                     fmt(lv["target"]), "+%.1f" % lv["dist_pct"], "%.1f" % lv["atr_pct"], fmt(p.get("qty"), 0),
                     "%s %s" % (fmt(risk, 0), p.get("ccy", "")), "sell half at the first target"])
    rows += [[], ["Advisory only: nothing here places, changes or cancels an order. Evidence: project doc "
                  "TFB_Stop_Loss_and_Take_Profit_Review_2026-11-11. exit_plan %s" % __version__]]
    return [(list(r) + [""] * w)[:w] for r in rows]


def mail_bodies(results: List[Dict[str, Any]], picks: List[Dict[str, Any]], info: Dict[str, Any]
                ) -> Tuple[str, str, str]:
    acts = [r for r in results if r["action"] in ("SELL", "TAKE PROFIT", "RAISE STOP", "REVIEW", "CHECK")]
    subject = "TFB Exit Plan %s - %s" % (info["date"], ("%d action%s: " % (len(acts), "" if len(acts) == 1 else "s")
                                                       + ", ".join("%s %s" % (r["symbol"], r["action"].lower())
                                                                   for r in sorted(acts, key=lambda x: ORDER.get(
                                                                       x["action"], 9)))) if acts else "hold all")
    lines = [info["headline"], info["summary"], ""]
    trs = []
    for r in sorted(results, key=lambda x: (ORDER.get(x["action"], 9), x["symbol"])):
        stop = fmt(r.get("stop")) if r.get("ok") else ""
        tgt = fmt(r.get("target")) if r.get("ok") else ""
        last = fmt(r.get("last")) if r.get("ok") else ""
        lines.append("%-8s %-11s last %-9s stop %-9s target %-9s %s" % (r["symbol"], r["action"], last, stop, tgt,
                                                                         "; ".join(r["why"])))
        color = {"SELL": "#b42318", "TAKE PROFIT": "#027a48", "RAISE STOP": "#b54708"}.get(r["action"], "#344054")
        trs.append("<tr><td><b>%s</b></td><td style='color:%s'><b>%s</b></td><td>%s</td><td>%s</td><td>%s</td>"
                   "<td>%s</td></tr>" % (html.escape(r["symbol"]), color, html.escape(r["action"]), last, stop, tgt,
                                         html.escape("; ".join(r["why"]))))
    ptrs = []
    for p in picks:
        lv = p.get("levels") or {}
        if lv.get("ok"):
            lines.append("PICK %-8s entry %s stop %s (-%.1f%%) first target %s" % (
                p["symbol"], fmt(p["entry"]), fmt(lv["stop"]), lv["dist_pct"], fmt(lv["target"])))
            ptrs.append("<tr><td><b>%s</b></td><td>%s</td><td>%s (-%.1f%%)</td><td>%s</td><td>%s</td></tr>" % (
                html.escape(p["symbol"]), fmt(p["entry"]), fmt(lv["stop"]), lv["dist_pct"], fmt(lv["target"]),
                fmt(p.get("stop"))))
    lines += ["", "Advisory only: no order is placed or changed."]
    th = "style='text-align:left;padding:4px 8px;border-bottom:1px solid #d0d5dd'"
    body = ("<div style='font-family:Arial,sans-serif;font-size:14px;color:#101828'><h2 style='margin:0 0 6px'>"
            "Exit Plan - %s</h2><p style='margin:0 0 4px'>%s</p><p style='margin:0 0 12px'>%s</p>"
            "<table style='border-collapse:collapse'><tr><th %s>Symbol</th><th %s>Action</th><th %s>Last</th>"
            "<th %s>Stop</th><th %s>Target</th><th %s>Why</th></tr>%s</table>" % (
                html.escape(info["date"]), html.escape(info["headline"]), html.escape(info["summary"]),
                th, th, th, th, th, th, "".join(trs)))
    if ptrs:
        body += ("<h3 style='margin:16px 0 6px'>Today's plan picks - per-stock levels</h3><table style='border-"
                 "collapse:collapse'><tr><th %s>Symbol</th><th %s>Entry</th><th %s>Stop</th><th %s>First target"
                 "</th><th %s>Plan stop</th></tr>%s</table>" % (th, th, th, th, th, "".join(ptrs)))
    body += "<p style='color:#667085;font-size:12px'>Advisory only: no order is placed or changed. exit_plan %s</p>" \
            "</div>" % __version__
    return subject, "\n".join(lines), body


# ----------------------------------------------------------------------------- #
# IO
# ----------------------------------------------------------------------------- #
def _default_opener(req: urllib.request.Request, timeout: float):  # pragma: no cover - network
    return urllib.request.urlopen(req, timeout=timeout)


def fetch_bars(ysym: str, opener: Callable[..., Any], timeout: float = 20.0) -> List[Bar]:
    for host in YAHOO_HOSTS:
        req = urllib.request.Request("https://" + host + YAHOO_PATH.format(sym=ysym),
                                     headers={"User-Agent": YAHOO_UA, "Accept": "application/json"})
        try:
            with opener(req, timeout) as resp:
                bars = parse_chart(json.loads(resp.read().decode("utf-8", "replace")))
            if bars:
                return bars
        except Exception as exc:  # noqa: BLE001
            if getattr(exc, "code", None) == 404:
                return []
        _SLEEP(1.0)
    return []


def read_columns(book, tab: str, names: Sequence[str]) -> List[List[Any]]:
    """Only the named columns of a page (one header read, one batch read)."""
    ws = book.worksheet(tab)
    head = ws.get("A1:EZ8")
    h, col = table_index(head, ("Symbol",))
    if h is None:
        return []
    have = [n for n in names if n in col]
    rel = next((n for n in col if n.startswith("Forecast Reliability")), None)
    if rel and rel not in have:
        have.append(rel)
    ranges = ["%s%d:%s" % (_col(col[n] + 1), h + 1, _col(col[n] + 1)) for n in have]
    cols = ws.batch_get(ranges)
    height = max((len(c) for c in cols), default=0)
    return [[(c[i][0] if i < len(c) and c[i] else "") for c in cols] for i in range(height)]


def _mail(subject: str, text_body: str, html_body: str) -> None:  # pragma: no cover - network
    from scripts.run_daily_brief import _mail_config, send_email  # noqa: PLC0415
    send_email(html_body, text_body, subject, _mail_config())


def run(dry_run: bool, send: bool, book=None, now: Optional[_dt.datetime] = None,
        opener: Optional[Callable[..., Any]] = None, mailer: Optional[Callable[[str, str, str], None]] = None) -> int:
    k = knobs()
    write = mode() == "on" and not dry_run
    now = now or _dt.datetime.now(UTC)
    today = now.astimezone(RIYADH).date()
    stamp = riyadh_str(now)
    opener = opener or _default_opener
    if book is None:
        from scripts.run_calendar_sync import _open_book  # noqa: PLC0415
        try:
            book = _open_book()
        except Exception as e:  # noqa: BLE001
            _out("ERROR: cannot open workbook: %s" % type(e).__name__)
            return 2
    for attempt in range(1, READ_ATTEMPTS + 1):
        try:
            lots = read_lots(_read(book, "_Portfolio_CostBasis", "A1:Z600"))
            grids = []
            for tab in ("My_Portfolio", "Global_Markets", "Market_Leaders"):
                try:
                    grids.append(read_columns(book, tab, INFO_COLS))
                except Exception as e:  # noqa: BLE001
                    if type(e).__name__ != "WorksheetNotFound":
                        raise
            cal = read_calendar(_read(book, "Calendar_Events", "A1:J4000"), _read(book, "Dividend_Events", "A1:J4000"),
                                today)
            news = read_news(_read(book, "_News_Items", "A1:M12000"), now, k["news_days"])
            stops, broker_note = read_broker_stops(_read(book, "_IBKR_Account", "A1:B40"),
                                                   _read(book, "_IBKR_Orders", "A1:H200"), now)
            picks = read_plan_picks(_read(book, "_Daily_Plan", "A1:AB2001"))
            ledger = parse_ledger(_read(book, LEDGER_TAB, "A1:%s%d" % (_col(len(LEDGER_HEADERS)), MAX_LEDGER_ROWS + 1)))
            break
        except Exception as e:  # noqa: BLE001
            if type(e).__name__ == "WorksheetNotFound" or attempt == READ_ATTEMPTS:
                _out("ERROR: cannot read the workbook: %s; nothing written" % type(e).__name__)
                return 2
            _out("read failed (%s) - attempt %d of %d, retrying in %d s" % (type(e).__name__, attempt,
                                                                          READ_ATTEMPTS, READ_RETRY_S))
            _SLEEP(READ_RETRY_S)
    info_map = read_info(grids)
    bars_cache: Dict[str, List[Bar]] = {}

    def bars_for(sym: str) -> List[Bar]:
        if sym not in bars_cache:
            bars_cache[sym] = fetch_bars(yahoo_symbol(sym), opener)
        return bars_cache[sym]

    results: List[Dict[str, Any]] = []
    for sym, lot in sorted(lots.items()):
        inf = dict(info_map.get(sym) or {})
        if any(c in inf.get("asset", "").lower() for c in NO_STOP_CLASSES):
            results.append({"symbol": sym, "name": inf.get("name") or lot["name"], "shares": lot["shares"],
                            "entry": lot["entry"], "ok": False, "action": "NO RULE",
                            "why": ["%s: no stop rule for fixed income" % inf["asset"]]})
            continue
        inf["news_neg"] = (news.get(sym) or {}).get("n", 0)
        inf["earn_days"] = (cal.get(sym) or {}).get("earn_days")
        inf["broker_stop"] = stops.get(sym)
        prev = prior_row(ledger, lot_key(sym, lot), today)
        res = exit_levels(sym, lot["entry"], lot["date"], bars_for(sym), inf, prev, k, today)
        res.update({"name": inf.get("name") or lot["name"], "shares": lot["shares"], "lot": lot_key(sym, lot),
                    "broker_stop": stops.get(sym), "prev": prev})
        if (news.get(sym) or {}).get("headline"):
            res["why"].append("latest negative item: %s" % news[sym]["headline"])
        results.append(res)
    for p in picks:
        p["levels"] = pick_levels(p["symbol"], p["entry"] or 0.0, bars_for(p["symbol"]), k) if p.get("entry") else {}
    counts: Dict[str, int] = {}
    for r in results:
        counts[r["action"]] = counts.get(r["action"], 0) + 1
    bars_to = max((r["bars_to"] for r in results if r.get("bars_to")), default=None)
    info = {"date": today.isoformat(),
            "headline": "As of %s Riyadh · daily bars to %s · %s" % (
                stamp, bars_to.isoformat() if bars_to else "n/a", broker_note),
            "summary": "Today: " + (" · ".join("%s %d" % (a, counts[a]) for a in sorted(counts, key=lambda x: ORDER.get(
                x, 9))) or "no holdings")}
    for r in results:
        _out("%-9s %-11s last %s stop %s (was %s) target %s | %s" % (
            r["symbol"], r["action"], fmt(r.get("last")), fmt(r.get("stop")), fmt(r.get("prev_stop")),
            fmt(r.get("target")), "; ".join(r["why"])))
    for p in picks:
        lv = p.get("levels") or {}
        _out("PICK %-8s entry %s plan stop %s per-stock stop %s target %s" % (
            p["symbol"], fmt(p.get("entry")), fmt(p.get("stop")), fmt(lv.get("stop")), fmt(lv.get("target"))))
    page = page_rows(results, picks, info, k)
    subject, text_body, html_body = mail_bodies(results, picks, info)
    if not write:
        _out("%s - nothing written, nothing sent | %s" % ("dry run" if dry_run else "observe", subject))
        return 0
    day = today.isoformat()
    keep = [d for d in ledger if text(d["Date"]) != day]
    new = [[day, stamp, r["symbol"], r.get("lot", ""), _round(r.get("entry")), r.get("shares"), _round(r.get("last")),
            _round(r.get("hc")), _round(r.get("atr")), _round(r.get("r")), _round(r.get("stop")),
            _round(r.get("target")), tp1_flag(r), r["action"], "; ".join(r.get("tighten") or []),
            r["bars_to"].isoformat() if r.get("bars_to") else ""] for r in results if r.get("ok")]
    grid = [list(LEDGER_HEADERS)] + ([[d[h] for h in LEDGER_HEADERS] for d in keep] + new)[-MAX_LEDGER_ROWS:]
    try:
        write_grid(book, LEDGER_TAB, grid, len(LEDGER_HEADERS), 200)
        write_grid(book, PAGE_TAB, page, PAGE_WIDTH, 60)
    except Exception as e:  # noqa: BLE001
        _out("ERROR: publication unconfirmed: %s; read back before retrying" % type(e).__name__)
        return 3
    _out("wrote %d ledger row(s) for %s and the %s page" % (len(new), day, PAGE_TAB))
    if send and k["mail"]:
        try:
            (mailer or _mail)(subject, text_body, html_body)
            _out("e-mail sent: %s" % subject)
        except Exception as e:  # noqa: BLE001
            _out("ERROR: e-mail not sent: %s" % type(e).__name__)
            return 4
    elif send:
        _out("e-mail skipped: TFB_EXIT_MAIL is not 1")
    return 0


def main(argv: Optional[Sequence[str]] = None) -> int:
    ap = argparse.ArgumentParser(description="Review the stop and the target of every holding.")
    ap.add_argument("--dry-run", action="store_true", help="compute and print, write and send nothing")
    ap.add_argument("--send", action="store_true", help="send the e-mail (needs TFB_EXIT_MAIL=1)")
    args = ap.parse_args(list(argv) if argv is not None else None)
    if mode() == "off":
        _out("TFB_EXIT_PLAN=off - nothing to do")
        return 0
    return run(args.dry_run, args.send)


if __name__ == "__main__":
    sys.exit(main())
