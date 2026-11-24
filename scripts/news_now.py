#!/usr/bin/env python3
"""
scripts/news_now.py
================================================================================
TFB News Now - each holding and Top 10 name, read again every hour of the US session
--------------------------------------------------------------------------------
v1.0.0 (2026-11-24)
  WHY: owner, 24 Nov 2026 10:10 Riyadh, in Arabic: the goal is for the news analysis to have an immediate effect on
       the reading of a stock's price now, through gathering, comparing and forecasting - not results on 8 December;
       10:20: "if something is missing that we can switch on now or start building, let us start". News Board 1.5.0
       (section 11) shows the news-and-trend reading twice a day from the nightly feed, which sees a headline the
       morning after. This job reads it within the hour.
  WHAT, each run (chained to the hourly Exit Plan in the UTC hours of TFB_NEWS_NOW_CHAIN_HOURS, weekdays):
    1. the names: the holdings (My_Portfolio, a plain ticker read as a US listing) and the Top 10 names of the last
       3 board days (v_top10_board), at most MAX_NAMES;
    2. their company news of the last NEWS_DAYS days from EODHD (the nightly job's reader, parser and daily-budget
       guard: 5 calls a name, refused above TFB_DEEP_EODHD_MAX_SHARE of the daily limit) into symbol_news_intraday:
       today's partition is rewritten as this run's items MERGED with the items already in it (Codex P2 on #1072),
       so a headline pushed off the provider's first page, or a name whose call failed, keeps what the day held;
       a name whose call failed (or every name, when the news was not refreshed) is read 'not refreshed' and gets
       no state - an empty answer is never taken for 'no news';
    3. the price so far for each US name and SPY (Yahoo chart, no key): the regular session open now, closed today,
       or not open yet; the previous close on the adjusted basis;
    4. each headline re-read with file 97's rules (v_symbol_news_class_now, apply_bq_views 1.27.0), and each US name
       read with EXACTLY the state rules of v_news_trend_daily (122) on the session so far: the company news since
       the last session, the price's answer against SPY in daily standard deviations (the 20 sessions before; the
       two-session reaction counts the first session only, as the nightly view does for the newest session), the
       trend before (the last close against its 50-session average and the close 21 sessions back), the state, the
       move research expects, and TFB's view beside it (the recommendation of v_universe_latest; the agreement of
       v_news_understanding);
    5. one row per name and run appended to news_now_log - the hourly record the forward reading is scored from
       later - and one page, News_Now, rewritten as a whole.
  OBSERVE ONLY: nothing here changes a recommendation, a gate or an order; the page says so on its second line. Each
  state's forward record is on News_Board section 11 (from 24 Nov 2026; first reading 8 Dec) - until a state reads
  'works as expected' it is context, not a signal.
  LOGS: the repository's run logs are public, so the job prints counts only - never a holding's symbol, a quantity or
  an amount. The EODHD token never reaches a log line.
  ROLLBACK: repository Variable TFB_NEWS_NOW=off (nothing read, nothing written), or TFB_NEWS_NOW_CHAIN_HOURS=off.
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
import urllib.parse
import urllib.request
from typing import Any, Callable, Dict, List, Optional, Sequence, Tuple

_HERE = os.path.dirname(os.path.abspath(__file__))
if _HERE not in sys.path:
    sys.path.insert(0, _HERE)

import archive_market_series as ams  # noqa: E402 - same folder: the Yahoo reader
import archive_symbol_deep as asd  # noqa: E402 - same folder: EODHD reader, budget guard, BigQuery loader
import news_board as nb  # noqa: E402 - same folder: holdings, board names, text helpers

__version__ = "1.0.0"

UTC = _dt.timezone.utc
RIYADH = _dt.timezone(_dt.timedelta(hours=3))
TAB = "News_Now"
PORTFOLIO_TAB = "My_Portfolio"
WIDTH = 12
MAX_NAMES = 40
NEWS_DAYS = 2                      # each run asks for the last two days, so the latest run of a day holds them all
NEWS_LIMIT = 20
HIST_DAYS = 120                    # calendar days of daily bars: 50 sessions for the trend and 20 for the volatility
SD_SESSIONS = 20                   # v_news_trend_daily: STDDEV over the 20 sessions before
SPY = "SPY"
TABLE_NEWS = "symbol_news_intraday"
TABLE_LOG = "news_now_log"
SCHEMA_NEWS = asd.SCHEMA_NEWS      # the nightly feed's columns, so file 97's rules read it unchanged
_F = "FLOAT"
SCHEMA_LOG: List[Dict[str, str]] = [
    {"name": "snapshot_date", "type": "DATE", "mode": "REQUIRED", "description": "UTC date of the run (one partition per day)"},
    {"name": "captured_at", "type": "TIMESTAMP", "mode": "REQUIRED", "description": "when the job ran"},
    {"name": "symbol", "type": "STRING", "mode": "REQUIRED", "description": "TFB symbol"},
    {"name": "listed_as", "type": "STRING", "mode": "REQUIRED", "description": "holding | Top 10"},
    {"name": "session_state", "type": "STRING", "mode": "NULLABLE", "description": "open | closed today | not open yet"},
    {"name": "session_date", "type": "DATE", "mode": "NULLABLE", "description": "venue date of the session read (NULL before the open)"},
    {"name": "prev_date", "type": "DATE", "mode": "NULLABLE", "description": "venue date of the last complete session before it"},
    {"name": "price", "type": _F, "mode": "NULLABLE", "description": "latest price (local currency)"},
    {"name": "prev_close", "type": _F, "mode": "NULLABLE", "description": "previous close, adjusted basis"},
    {"name": "change_pct", "type": _F, "mode": "NULLABLE", "description": "price against the previous close, percent"},
    {"name": "spy_change_pct", "type": _F, "mode": "NULLABLE", "description": "SPY against its previous close, percent"},
    {"name": "reaction_pp", "type": _F, "mode": "NULLABLE", "description": "the change minus SPY's, points"},
    {"name": "reaction_z", "type": _F, "mode": "NULLABLE", "description": "reaction_pp / (daily volatility x sqrt 2), as v_news_trend_daily"},
    {"name": "day_z", "type": _F, "mode": "NULLABLE", "description": "the day's excess log return / daily volatility"},
    {"name": "daily_vol_pp", "type": _F, "mode": "NULLABLE", "description": "standard deviation of the 20 sessions' excess returns, points"},
    {"name": "trend", "type": "STRING", "mode": "NULLABLE", "description": "up | down | mixed | unknown (before the session)"},
    {"name": "news_items", "type": "INTEGER", "mode": "NULLABLE", "description": "company headlines since the last session"},
    {"name": "news_score", "type": _F, "mode": "NULLABLE", "description": "their summed score (file 97's rules)"},
    {"name": "top_headline", "type": "STRING", "mode": "NULLABLE", "description": "the strongest headline"},
    {"name": "state", "type": "STRING", "mode": "NULLABLE", "description": "the state of v_news_trend_daily, on the session so far"},
    {"name": "expected", "type": "INTEGER", "mode": "NULLABLE", "description": "+1 up / -1 down against SPY / 0 no lean"},
    {"name": "trend_fit", "type": "STRING", "mode": "NULLABLE", "description": "with the trend | against the trend | no clear trend"},
    {"name": "recommendation", "type": "STRING", "mode": "NULLABLE", "description": "TFB's recommendation (v_universe_latest)"},
    {"name": "agreement", "type": "STRING", "mode": "NULLABLE", "description": "the expected move against TFB's view"},
    {"name": "news_coverage", "type": "STRING", "mode": "REQUIRED", "description": "fresh (this run asked EODHD) | not refreshed (no state)"},
    {"name": "run_id", "type": "STRING", "mode": "NULLABLE", "description": "GitHub Actions run id"},
]
SPEC_NEWS = (SCHEMA_NEWS, "snapshot_date", ["symbol"],
             "TFB hourly per-symbol headlines of the last two days for the holdings and the Top 10 names (EODHD news); "
             "today's partition replaced by each run of scripts/news_now.py.")
SPEC_LOG = (SCHEMA_LOG, "snapshot_date", ["symbol"],
            "TFB hourly news-and-price reading per holding and Top 10 name (one row per name and run), appended by "
            "scripts/news_now.py. Research only.")
EXPECTED = {"good news, price agreed": 1, "bad news, price agreed": -1, "good news, price fell": -1,
            "bad news, price rose": 1, "big rise, no news": -1, "big fall, no news": 1}
EXPECTED_TEXT = {1: "up against SPY", -1: "down against SPY", 0: "no lean"}
SESSION_TEXT = {"open": "open", "closed": "closed today", "pre": "not open yet"}
_BUY = frozenset({"BUY", "STRONG_BUY", "ACCUMULATE", "ADD"})
_SELL = frozenset({"SELL", "STRONG_SELL", "REDUCE", "AVOID", "EXIT"})
_ID_RE = re.compile(r"^[A-Za-z0-9_\-]+$")
_PLAIN_RE = re.compile(r"^[A-Z][A-Z0-9\-]{0,9}$")
_SLEEP = time.sleep


def _out(msg: str) -> None:
    print("[news_now %s] %s" % (__version__, msg), flush=True)


def _env(name: str, default: str = "") -> str:
    return (os.getenv(name) or default).strip()


def enabled() -> bool:
    return _env("TFB_NEWS_NOW", "on").lower() not in ("off", "0", "false", "no")


num, text = nb.num, nb.text


# ----------------------------------------------------------------------------- #
# The names
# ----------------------------------------------------------------------------- #
def tfb_symbol(symbol: Any) -> str:
    """A holding's ticker as the archive names it: a plain ticker is a US listing (AX -> AX.US); a listed one stays."""
    s = text(symbol).upper()
    if _PLAIN_RE.match(s):
        return s + ".US"
    return s if nb._SYM_RE.match(s) and "." in s else ""


def pick_names(holdings: Sequence[Dict[str, str]], board: Sequence[Any], cap: int = MAX_NAMES) -> List[Tuple[str, str]]:
    """(symbol, 'holding' | 'Top 10'): the holdings first, then the board names not held; no duplicate, at most cap."""
    out: List[Tuple[str, str]] = []
    seen: set = set()
    for sym, why in [(tfb_symbol(h.get("symbol")), "holding") for h in holdings] + \
                    [(text(b).upper(), "Top 10") for b in board]:
        if sym and nb._SYM_RE.match(sym) and sym not in seen and len(out) < cap:
            seen.add(sym)
            out.append((sym, why))
    return out


# ----------------------------------------------------------------------------- #
# The price so far
# ----------------------------------------------------------------------------- #
def chart_reading(payload: Any, now_ts: int) -> Optional[Dict[str, Any]]:
    """Yahoo daily chart -> the session read now: 'open' (the regular session is open: the latest price against the
    last complete close), 'closed' (today's session is over: its close against the one before) or 'pre' (no session
    today yet: no move). The bars before the session read, on the adjusted basis, feed the volatility and the trend.
    None when there is no complete bar."""
    try:
        meta = ((payload["chart"]["result"] or [None])[0] or {}).get("meta") or {}
    except (KeyError, TypeError, IndexError, AttributeError):
        return None
    done = ams.parse_chart(payload, now_ts)              # the bars of a session still open are left out
    if not done:
        return None
    try:
        off = int(meta.get("gmtoffset") or 0)
    except (TypeError, ValueError):
        off = 0
    venue_today = _dt.datetime.fromtimestamp(now_ts + off, tz=UTC).strftime("%Y-%m-%d")
    ref = [{"date": b["date"], "close": b["adj_close"] or b["close"], "raw": b["close"]} for b in done]
    if ams._live_session_start(meta, now_ts) is not None:
        allbars = ams.parse_chart(payload, None)
        price = num(meta.get("regularMarketPrice")) or (allbars[-1]["close"] if allbars else None)
        state, session, base = "open", venue_today, ref
        if price is None or not price > 0:
            return None
    elif ref[-1]["date"] == venue_today and len(ref) >= 2:
        state, session, base, price = "closed", venue_today, ref[:-1], ref[-1]["close"]
    else:
        state, session, base, price = "pre", None, ref, ref[-1]["close"]
    return {"state": state, "session_date": session, "prev_date": base[-1]["date"], "price": price,
            "prev_close": base[-1]["close"], "bars": base}


def _stdev(xs: Sequence[float]) -> Optional[float]:
    if len(xs) < 2:
        return None
    m = sum(xs) / len(xs)
    return math.sqrt(sum((x - m) ** 2 for x in xs) / (len(xs) - 1))


def trend_of(closes: Sequence[float]) -> str:
    """v_news_trend_daily: the last close against the average of the 50 closes before the session and the close 21
    sessions back; fewer than 40 of the 50 or no close 21 back is unknown."""
    window = list(closes[-50:])
    if len(window) < 40 or len(closes) < 21:
        return "unknown"
    c_prev, sma, c21 = closes[-1], sum(window) / len(window), closes[-21]
    if c_prev > sma and c_prev > c21:
        return "up"
    if c_prev < sma and c_prev < c21:
        return "down"
    return "mixed"


def price_answer(stock: Optional[Dict[str, Any]], spy: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    """The stock against SPY on the session read, on the dates both traded: the daily volatility of the 20 sessions
    before (excess log returns, sample deviation, in points), the trend before, and - when the session is open or
    closed today - the reaction (simple returns, points; z over sd x sqrt 2) and the day's z (log, over sd)."""
    out: Dict[str, Any] = {"daily_vol_pp": None, "trend": None, "reaction_pp": None, "reaction_z": None,
                           "day_z": None, "change_pct": None, "spy_change_pct": None}
    if not stock or not spy:
        return out
    sp = {b["date"]: b["close"] for b in spy["bars"]}
    joined = [(b["close"], sp[b["date"]]) for b in stock["bars"] if b["date"] in sp and b["close"] > 0 and sp[b["date"]] > 0]
    xs = [math.log(c / pc) - math.log(s / ps) for (pc, ps), (c, s) in zip(joined, joined[1:])]
    sd = _stdev(xs[-SD_SESSIONS:])
    out["daily_vol_pp"] = round(sd * 100, 4) if sd else None
    out["trend"] = trend_of([c for c, _ in joined])
    if stock["state"] == "pre" or spy["state"] == "pre" or stock["prev_date"] != spy["prev_date"]:
        return out
    c_prev, s_prev = stock["prev_close"], spy["prev_close"]
    p, s = stock["price"], spy["price"]
    if not (c_prev and s_prev and p and s):
        return out
    out["change_pct"] = round(100 * (p / c_prev - 1), 4)
    out["spy_change_pct"] = round(100 * (s / s_prev - 1), 4)
    out["reaction_pp"] = round(100 * (p / c_prev - s / s_prev), 4)
    if sd:
        out["reaction_z"] = round(out["reaction_pp"] / (sd * 100 * math.sqrt(2)), 4)
        out["day_z"] = round((math.log(p / c_prev) - math.log(s / s_prev)) / sd, 4)
    return out


# ----------------------------------------------------------------------------- #
# The state - the rules of v_news_trend_daily, unchanged
# ----------------------------------------------------------------------------- #
def news_state(sd: Optional[float], items: int, score: float, reaction_z: Optional[float], day_z: Optional[float],
               items_before: int) -> str:
    """The CASE of v_news_trend_daily (122) on the session so far. Before the open the price has not answered: its
    reaction and z count as 0 (as the nightly view reads the newest session on its first day only). The session
    after is not known yet, so it holds no news - as the nightly view's IFNULL(items_after, 0)."""
    rz = reaction_z if reaction_z is not None else 0.0
    dz = day_z if day_z is not None else 0.0
    if sd is None or sd <= 0:
        return "quiet"
    if items > 0 and score > 0 and rz >= 1:
        return "good news, price agreed"
    if items > 0 and score < 0 and rz <= -1:
        return "bad news, price agreed"
    if items > 0 and score > 0 and rz <= -1:
        return "good news, price fell"
    if items > 0 and score < 0 and rz >= 1:
        return "bad news, price rose"
    if items > 0 and score != 0:
        return "news, price quiet"
    if items > 0 and abs(rz) >= 2:
        return "unsigned news, big move"
    if items > 0:
        return "routine news"
    if items_before == 0 and dz >= 2.5:
        return "big rise, no news"
    if items_before == 0 and dz <= -2.5:
        return "big fall, no news"
    return "quiet"


def expected_of(state: str, score: float, reaction_z: Optional[float]) -> int:
    if state in EXPECTED:
        return EXPECTED[state]
    if state == "news, price quiet":
        return (score > 0) - (score < 0)
    if state == "unsigned news, big move":
        rz = reaction_z or 0.0
        return (rz > 0) - (rz < 0)
    return 0


def trend_fit(expected: int, trend: str) -> str:
    if expected == 0 or trend in ("mixed", "unknown"):
        return "no clear trend"
    return "with the trend" if (expected == 1 and trend == "up") or (expected == -1 and trend == "down") \
        else "against the trend"


def tfb_stance(recommendation: Any) -> Optional[int]:
    """v_news_understanding: +1 BUY-type, -1 SELL-type (spaces and hyphens read as underscores), 0 otherwise, None
    without a recommendation."""
    if recommendation is None or text(recommendation) == "":
        return None
    r = re.sub(r"[ -]+", "_", text(recommendation).upper())
    return 1 if r in _BUY else -1 if r in _SELL else 0


def agreement(expected: int, stance: Optional[int]) -> str:
    if expected == 0:
        return "no lean from the news"
    if stance is None:
        return "TFB has no view today"
    if stance == 0:
        return "TFB holds, the news leans up" if expected > 0 else "TFB holds, the news leans down"
    return "agrees with TFB" if stance == expected else "leans against TFB"


def window_news(rows: Sequence[Dict[str, Any]], symbol: str, after: Optional[str], upto: Optional[str] = None
                ) -> List[Dict[str, Any]]:
    """The company headlines of `symbol` whose Riyadh news day falls in (after, upto] - the window v_news_trend_daily
    gives a session (news days after the previous session, up to the session itself)."""
    out = []
    for r in rows:
        d = text(r.get("news_date"))[:10]
        if text(r.get("symbol")).upper() == symbol and d and (after is None or d > after) and (upto is None or d <= upto):
            out.append(r)
    return out


def read_name(symbol: str, listed_as: str, stock: Optional[Dict[str, Any]], spy: Optional[Dict[str, Any]],
              classified: Optional[Sequence[Dict[str, Any]]], raw_items: int, universe: Dict[str, Dict[str, Any]],
              today_riyadh: str, fresh: bool = True) -> Dict[str, Any]:
    """One name's reading. A name outside the US gets its news only (the state rules read a US name against SPY).
    A name whose news was not refreshed this run gets no state: what is missing is not 'no news' (Codex P2, #1072)."""
    u = universe.get(symbol) or {}
    row: Dict[str, Any] = {"symbol": symbol, "listed_as": listed_as, "name": text(u.get("name")),
                           "recommendation": text(u.get("recommendation")) or None,
                           "session_state": SESSION_TEXT.get(stock["state"]) if stock else None,
                           "session_date": stock.get("session_date") if stock else None,
                           "prev_date": stock.get("prev_date") if stock else None,
                           "price": round(stock["price"], 4) if stock else None,
                           "prev_close": round(stock["prev_close"], 4) if stock else None,
                           "state": None, "expected": None, "trend_fit": None, "agreement": None,
                           "news_items": None, "news_score": None, "top_headline": None,
                           "raw_items": raw_items, "note": "",
                           "news_coverage": "fresh" if fresh else "not refreshed"}
    row.update(price_answer(stock, spy) if symbol.endswith(".US") else
               {"daily_vol_pp": None, "trend": None, "reaction_pp": None, "reaction_z": None, "day_z": None,
                "change_pct": None, "spy_change_pct": None})
    if classified is None:
        row["note"] = "headlines not classified yet (apply_bq_views 1.27.0)"
        return row
    if stock:
        prev, upto = stock.get("prev_date"), stock.get("session_date") or today_riyadh
    else:                                       # no price: the last two news days
        prev = (_dt.date.fromisoformat(today_riyadh) - _dt.timedelta(days=2)).isoformat()
        upto = today_riyadh
    news = window_news(classified, symbol, prev, upto)
    row["news_items"] = len(news)
    row["news_score"] = round(sum(num(r.get("score")) or 0.0 for r in news), 2)
    if news:                                    # the strongest headline, the newest of equals (as the nightly view)
        top = max(news, key=lambda r: (abs(num(r.get("score")) or 0.0), text(r.get("published_at"))))
        row["top_headline"] = text(top.get("title"))[:200]
    if not symbol.endswith(".US"):
        row["note"] = "news only: the state is read for US names against SPY"
        return row
    if not fresh:
        row["note"] = "company news not refreshed this run: no state"
        return row
    if not stock or not spy:
        row["note"] = "no price from Yahoo this run"
        return row
    bars = stock["bars"]
    before = window_news(classified, symbol, bars[-2]["date"] if len(bars) >= 2 else None, prev)
    sd = row["daily_vol_pp"]
    state = news_state(sd, row["news_items"], row["news_score"], row["reaction_z"], row["day_z"], len(before))
    exp = expected_of(state, row["news_score"], row["reaction_z"])
    row.update({"state": state, "expected": exp, "trend_fit": trend_fit(exp, row["trend"] or "unknown"),
                "agreement": agreement(exp, tfb_stance(row["recommendation"]))})
    return row


# ----------------------------------------------------------------------------- #
# The page
# ----------------------------------------------------------------------------- #
def _sign(v: Optional[float], nd: int = 2, unit: str = "") -> str:
    return "" if v is None else ("%+." + str(nd) + "f%s") % (v, unit)


def verdict_line(rows: Sequence[Dict[str, Any]]) -> str:
    """Counts only - this line also goes to the public run log."""
    us = [r for r in rows if r.get("state") is not None]
    news = sum(1 for r in rows if (r.get("news_items") or 0) > 0)
    lean = sum(1 for r in us if r.get("expected"))
    against = sum(1 for r in us if r.get("agreement") == "leans against TFB")
    moved = sum(1 for r in us if abs(r.get("day_z") or 0) >= 2.5)
    return ("NEWS NOW: %d name(s) read (%d US with a state); %d with company news since the last session, %d with a "
            "lean for the next sessions, %d leaning against TFB's view, %d moving 2.5 sd or more against SPY. Not "
            "evidence yet: each state's forward record is on News_Board section 11 (from 24 Nov 2026; first reading "
            "8 Dec)." % (len(rows), len(us), news, lean, against, moved))


def page_rows(rows: Sequence[Dict[str, Any]], stamp: str, ny_stamp: str, session: str, notes: Sequence[str]) -> List[List[Any]]:
    pad = [""] * WIDTH

    def line(*cells: Any) -> List[Any]:
        r = list(cells)[:WIDTH]
        return r + pad[len(r):]

    out = [line("NEWS NOW - each holding and Top 10 name, read again every hour of the US session: its company news "
                "since the last session, how the price is answering against SPY so far, the trend before, and the "
                "move research expects next, beside TFB's own view"),
           line("Built %s Riyadh | news_now %s | SHADOW: analysis only. Nothing on this page changes a recommendation "
                "or places an order; only the Today Action Plan gives orders." % (stamp, __version__)),
           line("US session: %s | prices at %s New York" % (session, ny_stamp)),
           line(verdict_line(rows))]
    out += [line("Note: " + n) for n in notes]
    out.append(line())
    out.append(line("Name", "Listed as", "Price vs last close", "Against SPY", "z (day)", "News since last session",
                    "State now", "Research expects", "Trend before", "TFB's view", "Agreement", "Main headline"))
    for r in rows:
        name = r["symbol"] + (" - " + r["name"] if r.get("name") else "")
        if r.get("news_items") is None:
            news = "%d headline(s), not classified yet" % r["raw_items"] if r.get("raw_items") else ""
        else:
            news = "%d, score %s" % (r["news_items"], _sign(r["news_score"], 1)) if r["news_items"] else "none"
            if r.get("news_coverage") != "fresh":
                news += " (not refreshed)"
        exp = r.get("expected")
        out.append(line(name, r["listed_as"], _sign(r.get("change_pct"), 2, " %"),
                        _sign(r.get("reaction_pp"), 2, " pp"), _sign(r.get("day_z"), 1), news,
                        r.get("state") or r.get("note") or "", EXPECTED_TEXT.get(exp, "") if exp is not None else "",
                        r.get("trend") or "", r.get("recommendation") or "",
                        nb.AGREEMENT_TEXT.get(r.get("agreement") or "", r.get("agreement") or ""),
                        r.get("top_headline") or ""))
    out.append(line())
    for h in ("How to read: the state joins the company news since the last session (each headline re-read with the "
              "rules of v_symbol_news_class) to the price's answer against SPY so far, in daily standard deviations "
              "of the 20 sessions before. Research expects a move with news to continue, a big move without news "
              "(2.5 sd) to reverse, and news the price has not answered to drift its way.",
              "The rules are the nightly ones (v_news_trend_daily), read on the session so far: a state can change "
              "within the day as the price moves, and tonight's nightly reading fixes it. Before the open the price "
              "has not answered, so news reads as 'news, price quiet'.",
              "Each state's forward record (from 24 Nov 2026) is on News_Board section 11; none counts as evidence "
              "before it reads 'works as expected'. This page is context beside the decision, never an order."):
        out.append(line(h))
    return out


# ----------------------------------------------------------------------------- #
# Sources
# ----------------------------------------------------------------------------- #
def _default_opener(req: urllib.request.Request, timeout: float):  # pragma: no cover - network
    return urllib.request.urlopen(req, timeout=timeout)


def fetch_chart(ysym: str, now: _dt.datetime, opener: Callable[..., Any], sleep: Callable[[float], None]) -> Any:
    """The raw Yahoo daily chart (query1, then query2); None when neither answered."""
    p1 = int((now - _dt.timedelta(days=HIST_DAYS)).timestamp())
    p2 = int(now.timestamp()) + 86400
    path = ams.YAHOO_PATH.format(sym=urllib.parse.quote(ysym, safe=""), p1=p1, p2=p2)
    for host in ams.YAHOO_HOSTS:
        req = urllib.request.Request("https://" + host + path, headers={"User-Agent": ams.YAHOO_UA,
                                                                       "Accept": "application/json"})
        try:
            with opener(req, 30) as resp:
                return json.loads(resp.read().decode("utf-8", "replace"))
        except Exception as exc:  # noqa: BLE001
            if getattr(exc, "code", None) == 404:
                return None
        sleep(1.0)
    return None


def fetch_news(names: Sequence[str], token: str, day: str, captured_at: str, run_id: str,
               opener: Callable[..., Any], sleep: Callable[[float], None], pause: float = 0.3
               ) -> Tuple[List[Dict[str, Any]], List[str]]:
    """(rows, the names whose call failed). The nightly reader and parser; the token never leaves _scrub."""
    since = (_dt.date.fromisoformat(day) - _dt.timedelta(days=NEWS_DAYS)).isoformat()
    base = {"snapshot_date": day, "captured_at": captured_at, "run_id": run_id or None}
    rows: List[Dict[str, Any]] = []
    failed: List[str] = []
    for sym in names:
        try:
            items = asd.parse_news(asd.eodhd_get("news", {"s": asd.eodhd_symbol(sym), "from": since,
                                                          "limit": NEWS_LIMIT, "offset": 0}, token, opener),
                                   NEWS_LIMIT)
            rows.extend(dict(base, symbol=sym, **it) for it in items)
        except Exception:  # noqa: BLE001 - counted; the message may name a holding, so it is not printed
            failed.append(sym)
        sleep(pause)
    return rows, failed


def sql_today_news(ds: str, day: str) -> str:
    """The rows today's partition already holds, in the load's own formats (BigQuery returns a TIMESTAMP as epoch
    seconds and every number as text)."""
    return ("SELECT CAST(snapshot_date AS STRING) AS snapshot_date, symbol, "
            "FORMAT_TIMESTAMP('%%Y-%%m-%%dT%%H:%%M:%%SZ', published_at) AS published_at, title, link, domain, polarity, "
            "neg, neu, pos, tags, FORMAT_TIMESTAMP('%%Y-%%m-%%dT%%H:%%M:%%SZ', captured_at) AS captured_at, run_id "
            "FROM `%s.%s` WHERE snapshot_date = DATE '%s'" % (ds, TABLE_NEWS, day))


def merge_news(new: Sequence[Dict[str, Any]], old: Sequence[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """This run's items, then every earlier item of the day it does not repeat (one per symbol and link, or title)."""
    out: List[Dict[str, Any]] = []
    seen: set = set()
    for r in list(new) + [dict(o) for o in old]:
        key = (text(r.get("symbol")).upper(), text(r.get("link")) or text(r.get("title")))
        if not key[0] or not text(r.get("title")) or key in seen:
            continue
        seen.add(key)
        row = {f["name"]: r.get(f["name"]) for f in SCHEMA_NEWS}
        for k in ("polarity", "neg", "neu", "pos"):
            row[k] = num(row[k])
        out.append(row)
    return out


def sql_classified(ds: str, symbols: Sequence[str], since: str) -> str:
    return ("SELECT symbol, CAST(news_date AS STRING) AS news_date, CAST(published_at AS STRING) AS published_at, "
            "title, event_type, score FROM `%s.v_symbol_news_class_now` WHERE relevance = 'company' "
            "AND symbol IN (%s) AND news_date >= DATE '%s'" % (ds, ", ".join("'%s'" % s for s in symbols), since))


def sql_universe(ds: str, symbols: Sequence[str]) -> str:
    return ("SELECT symbol, name, recommendation FROM `%s.v_universe_latest` WHERE symbol IN (%s) "
            "QUALIFY ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY captured_at DESC) = 1"
            % (ds, ", ".join("'%s'" % s for s in symbols)))


# ----------------------------------------------------------------------------- #
# The run
# ----------------------------------------------------------------------------- #
def run_once(dry_run: bool, book=None, session=None, now: Optional[_dt.datetime] = None, query=None,
             opener: Optional[Callable[..., Any]] = None, sleep: Callable[[float], None] = time.sleep,
             ensure: Optional[Callable[..., Any]] = None, load: Optional[Callable[..., Any]] = None) -> int:
    now = now or _dt.datetime.now(UTC)
    if not enabled():
        _out("TFB_NEWS_NOW is off: nothing read, nothing written")
        return 0
    project, dataset = _env("TFB_BQ_PROJECT"), _env("TFB_BQ_DATASET", "tfb_archive")
    location = _env("TFB_BQ_LOCATION", "US")
    if not _ID_RE.match(project) or not _ID_RE.match(dataset):
        _out("ERROR: TFB_BQ_PROJECT / TFB_BQ_DATASET missing or malformed; nothing written")
        return 2
    ds = "%s.%s" % (project, dataset)
    opener = opener or _default_opener
    ensure = ensure or asd.ensure_table
    load = load or asd.load
    run_id = _env("GITHUB_RUN_ID", "local")
    day = now.astimezone(UTC).strftime("%Y-%m-%d")
    today_riyadh = now.astimezone(RIYADH).date()
    captured_at = now.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")
    notes: List[str] = []
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
        holdings = nb.read_holdings(_quota_call(lambda: _raw_read(book, PORTFOLIO_TAB, "A1:B200"), _SLEEP, _out,
                                                "reading %s" % PORTFOLIO_TAB))
        board_rows = nb.read_optional(session, project, location, nb.sql_board_names(ds, today_riyadh), query) or []
        names = pick_names(holdings, [r.get("symbol") for r in board_rows])
    except Exception as e:  # noqa: BLE001 - a failed read leaves the previous page in place
        _out("ERROR: %s while reading the names; nothing written" % type(e).__name__)
        return 2
    if not names:
        _out("no holding and no Top 10 name to read; nothing written")
        return 0
    syms = [s for s, _ in names]
    held = sum(1 for _, w in names if w == "holding")
    _out("names: %d (%d holding(s), %d Top 10 name(s) not held)" % (len(names), held, len(names) - held))

    # 2. company news (EODHD, behind the nightly job's budget guard)
    raw: List[Dict[str, Any]] = []
    fresh: set = set()                          # the names EODHD answered this run
    token = _env("EODHD_API_KEY")
    if _env("TFB_NEWS_NOW_EODHD", "on").lower() in ("off", "0", "false", "no"):
        notes.append("company news not refreshed this run (TFB_NEWS_NOW_EODHD is off)")
    elif not token:
        notes.append("company news not refreshed this run (EODHD_API_KEY missing)")
    else:
        ok, why = asd.budget_check(token, asd.COST_NEWS * len(syms), asd.max_share(), opener, today=day)
        _out("EODHD budget: %s" % why)
        if not ok:
            notes.append("company news not refreshed this run (the EODHD budget guard: %s)" % why)
        else:
            raw, failed = fetch_news(syms, token, day, captured_at, run_id, opener, sleep)
            fresh = set(syms) - set(failed)
            _out("news: %d headline(s) for %d name(s), %d call(s) failed" % (len(raw), len(syms), len(failed)))
            if failed:
                notes.append("%d news call(s) failed: those names are read 'not refreshed' (no state)" % len(failed))
            if dry_run:
                _out("dry run: %d headline row(s) not loaded" % len(raw))
            else:
                try:
                    ensure(session, project, dataset, TABLE_NEWS, spec=SPEC_NEWS)
                    try:
                        old = query(session, project, location, sql_today_news(ds, day))
                    except RuntimeError as e:
                        if "Not found" not in text(e):
                            raise
                        old = []
                    merged = merge_news(raw, old)
                    n = load(session, project, dataset, location, TABLE_NEWS, day, merged, run_id, schema=SCHEMA_NEWS)
                    _out("loaded %d headline row(s) into %s$%s (%d new this run, %d kept from earlier runs)" % (
                        n, TABLE_NEWS, day.replace("-", ""), len(raw), len(merged) - len(merge_news(raw, []))))
                except Exception as e:  # noqa: BLE001
                    _out("ERROR: the headlines were not loaded (%s: %s)" % (type(e).__name__, text(e)[:200]))
                    notes.append("this run's headlines were not loaded into BigQuery: every name is read 'not "
                                 "refreshed' (no state)")
                    fresh = set()

    # 3. the price so far
    now_ts = int(now.timestamp())
    charts: Dict[str, Optional[Dict[str, Any]]] = {}
    for sym in [SPY] + [s for s in syms if s.endswith(".US")]:
        charts[sym] = chart_reading(fetch_chart(asd.yahoo_symbol(sym), now, opener, sleep), now_ts)
        sleep(0.2)
    spy = charts.get(SPY)
    missing = sum(1 for s in syms if s.endswith(".US") and not charts.get(s))
    _out("prices: SPY %s; %d of %d US name(s) without a price" % (
        SESSION_TEXT.get(spy["state"]) if spy else "NOT READ", missing, sum(1 for s in syms if s.endswith(".US"))))
    if not spy:
        notes.append("SPY was not read: no state this run")

    # 4. the readings
    since = (today_riyadh - _dt.timedelta(days=10)).isoformat()
    try:
        classified = nb.read_optional(session, project, location, sql_classified(ds, syms, since), query)
        universe = {text(r.get("symbol")).upper(): r for r in query(session, project, location, sql_universe(ds, syms))}
    except Exception as e:  # noqa: BLE001
        _out("ERROR: %s while reading BigQuery; nothing written" % type(e).__name__)
        return 2
    raw_count = {s: sum(1 for r in raw if r["symbol"] == s) for s in syms}
    rows = [read_name(s, w, charts.get(s), spy, classified, raw_count[s], universe, today_riyadh.isoformat(),
                      fresh=s in fresh) for s, w in names]
    _out(verdict_line(rows))

    log = [{"snapshot_date": day, "captured_at": captured_at, "symbol": r["symbol"], "listed_as": r["listed_as"],
            "session_state": r["session_state"], "session_date": r["session_date"], "prev_date": r["prev_date"],
            "price": r["price"], "prev_close": r["prev_close"], "change_pct": r["change_pct"],
            "spy_change_pct": r["spy_change_pct"], "reaction_pp": r["reaction_pp"], "reaction_z": r["reaction_z"],
            "day_z": r["day_z"], "daily_vol_pp": r["daily_vol_pp"], "trend": r["trend"],
            "news_items": r["news_items"], "news_score": r["news_score"], "top_headline": r["top_headline"],
            "state": r["state"], "expected": r["expected"], "trend_fit": r["trend_fit"],
            "recommendation": r["recommendation"], "agreement": r["agreement"],
            "news_coverage": r["news_coverage"], "run_id": run_id or None}
           for r in rows]
    ny = now.astimezone(_ny_zone())
    page = page_rows(rows, now.astimezone(RIYADH).strftime("%Y-%m-%d %H:%M"), ny.strftime("%Y-%m-%d %H:%M"),
                     SESSION_TEXT.get(spy["state"], "") if spy else "not read", notes)
    if dry_run:
        _out("dry run: %d log row(s) and %d page row(s) computed, nothing written" % (len(log), len(page)))
        return 0
    rc = 0
    try:
        ensure(session, project, dataset, TABLE_LOG, spec=SPEC_LOG)
        n = load(session, project, dataset, location, TABLE_LOG, day, log, run_id, schema=SCHEMA_LOG,
                 disposition="WRITE_APPEND")
        _out("appended %d row(s) to %s" % (n, TABLE_LOG))
    except Exception as e:  # noqa: BLE001 - the page is still written; the run is marked failed
        _out("ERROR: the log was not appended (%s: %s)" % (type(e).__name__, text(e)[:200]))
        rc = 1
    try:
        from scripts.daily_plan import write_grid  # noqa: PLC0415
        from scripts.sheets_quota import call as _quota_call  # noqa: PLC0415
        _quota_call(lambda: write_grid(book, TAB, page, WIDTH, 60), _SLEEP, _out, "writing %s" % TAB)
    except Exception as e:  # noqa: BLE001
        _out("ERROR: %s: the page was not written" % type(e).__name__)
        return 2
    _out("wrote the %s page (%d rows)" % (TAB, len(page)))
    return rc


def _ny_zone() -> _dt.tzinfo:
    try:
        from zoneinfo import ZoneInfo  # noqa: PLC0415
        return ZoneInfo("America/New_York")
    except Exception:  # noqa: BLE001 - no tz database: Eastern Standard Time
        return _dt.timezone(_dt.timedelta(hours=-5))


def main(argv: Optional[Sequence[str]] = None, **kw) -> int:
    ap = argparse.ArgumentParser(description="TFB News Now (hourly news and price reading, observe only)")
    ap.add_argument("--dry-run", action="store_true")
    a = ap.parse_args(list(argv) if argv is not None else None)
    return run_once(a.dry_run, **kw)


if __name__ == "__main__":
    sys.exit(main())
