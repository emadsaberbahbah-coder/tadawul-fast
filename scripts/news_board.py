#!/usr/bin/env python3
"""
scripts/news_board.py
================================================================================
TFB News Board - the world news, the sector tilt it gives, and whether following it would have helped
--------------------------------------------------------------------------------
v1.7.0 (2026-11-24)
  1.7.0: [SNAPSHOTS] sections 11 and 12 read v_news_understanding and v_news_trend_learning from their snapshots
         (scripts/bq_snapshots.py 1.0.0: computed once after each collector, about 0.37 GB a read each before) when
         the snapshot is there and younger than 30 hours; otherwise from the views, as before. Owner, 24 Nov 2026
         13:07 Riyadh: BigQuery faster; 13:53 "Let's go with next". The same rows, read in milliseconds instead of
         recomputed; one small freshness read per snapshot and run.
  1.6.0: [INTERIM] section 12, THE NEWS RECORD SO FAR, observe only. Owner, 24 Nov 2026 12:17 Riyadh: "ok let's start
         the next stage" - the interim daily reading of the news record, so its progress is in front of the owner
         every day, not only on 8 Dec. Two records, each per state and horizon, with its fixed reading and how far it
         is from being read: the nightly news-and-trend states (v_news_trend_learning, forward from 24 Nov 2026,
         1 and 5 sessions; read in the same query as section 11's records, which now asks for both horizons) and
         the hourly News Now reading (v_news_now_learning, apply_bq_views 1.28.0: the rest of the session, 1 and 5
         sessions). A record is 'collecting' under 20 events or under max(10, 2 x horizon) dates; the section says
         how many of each it holds. One line under the verdict, in counts. Until the hourly view is in BigQuery the
         section says so and the page is built all the same.
  1.5.0: [NEWS NOW] section 11, NEWS AND TREND NOW, observe only. Owner, 24 Nov 2026 10:10 Riyadh, in Arabic: the
         goal is for the news analysis to have an immediate effect on the reading of a stock's price now, through
         gathering, comparing and forecasting - not results on 8 December; 10:20: "if something is missing that we
         can switch on now or start building, let us start". The news-and-trend reading of apply_bq_views 1.25.0
         (v_news_understanding) was only on the strategist's site. Each holding and each Top 10 name of the last 3
         days that TFB follows now shows, on this page after every sync: its latest news event, how the price
         answered against SPY (pp and z), the trend, the move research expects over the next sessions, TFB's own
         view and whether the two agree, the main headline, and that state's forward record over 5 sessions
         (v_news_trend_learning) - so the reading is in front of the owner the same day while its record fills.
         Then the 8 latest events across every followed name. A state counts as evidence only once its forward
         record reads 'works as expected'; until then the section says so on every line. Until the views are in
         BigQuery it says so and the page is built all the same. The run log prints counts only.
  1.4.0: [MARKETS / PEOPLE] three sections from the views of apply_bq_views 1.20.0, observe only. Owner, 21 Nov 2026
         11:03 Riyadh, item 5 of the build list: "News: a news reading per market; forward event scenarios;
         momentum/surprise; influential figures of 19 Nov; score every news signal." Section 8, NEWS BY MARKET: the
         latest complete news day, each market's place (the US, Saudi Arabia, the war zone around the Gulf for gold
         and oil, Europe, Japan, China / Hong Kong) against its own previous 250 news days - favourable / neutral /
         unfavourable, its momentum and a surprise (v_market_news). Section 9, PEOPLE IN THE NEWS: 17 people who move
         markets, their attention and tone against their own normal, the spikes of the last 7 days and the markets
         each is expected to move (v_people_news). Section 10, NEWS SIGNALS ON TRIAL: every reading against what the
         markets did next, forward from 23 Nov 2026, the backtest apart (v_news_trial_summary). One line of each
         under the verdict. Until the views are in BigQuery each section says so and the page is built all the same.
  1.3.0: [PEERS] the record of the new classes in section 7 is read against the same-day average of the US
         names (v_company_news_eval, apply_bq_views 1.17.0) - the S&P 500 beside it - because against the
         index every class looked ahead, neutral as much as positive: the names beat the index, not their
         news. Owner, 21 Nov 2026 02:20 Riyadh, in Arabic: "let us also review the news analysis". Until the
         view is updated in BigQuery the line keeps the 1.2.0 reading against the S&P 500 and says so, so the
         page never fails on the order in which the two are deployed.
  1.2.0: [COMPANY] section 7, COMPANY NEWS RE-READ - each holding's headlines of the last 7 days as
         v_symbol_news_class reads them from their own text: how many are about the company and how many,
         filed under it, are about someone else; the week's class (POSITIVE / NEGATIVE / neutral / no company
         news) from the summed score; the main headline and its event; the provider's old reading beside it;
         and the record of the new classes from v_company_news_eval (5 sessions after the news day, against
         the S&P 500). Owner, 20 Nov 2026 12:54 Riyadh, in Arabic: "let us do the expected improvement to the
         news classification, improve it and re-classify it". Analysis only, like the rest of the page.
  1.1.0: [EVENTS] section 6, EVENTS AHEAD - the dated events of the next 60 days (summits, central-bank
         decisions, OPEC+ meetings, deadlines) from the hand-kept data/news_event_calendar.json, each with the
         page its date was read from. Owner, 19 Nov 2026 16:09: "Next" - part 3 of the stage, the calendar of
         summits and deadlines. Information only. The file states the day it was last checked; the page says
         so when that is more than 21 days ago, when the file runs out, or when it cannot be read - and is
         built all the same.
  1.0.0: the page (news learning loop, phase B: the news in the file).

WHY
    Owner, 19 Nov 2026: world news - politics, wars, the economy, the fear of
    AI, elections, the summit in China, the Iran war - is part of valuing any
    stock; weigh each kind of news, let the weights correct themselves, and
    show whether the recommendations change and by how much ("build the new
    news and its effect in the file"; 14:30: "move to the next stage").
    Phase A (PR #1030) put the themes, the self-correcting sector weights, the
    shadow list and the trial into BigQuery. Nothing of it was in the workbook.
    This script reads those five views on every run and publishes one page.

WHAT IT WRITES (its page News_Board; nothing else)
    VERDICT: where the forward trial stands, in one or two lines, and any
       warning (a stale news archive, no tilt in force).
    1. WORLD NEWS: each theme's last full week against its own previous four
       weeks, how unusual that is against a year of its own weekly changes,
       and the running week so far.
    2. SECTOR TILT: the eleven US sectors, favoured / neutral / avoided, for
       both arms (political events only; events and news themes), the themes
       with a working weight, last week's tilt and what the sector then did;
       and the record of the tilt, past weeks apart from forward weeks.
    3. HOLDINGS: each holding of My_Portfolio beside its sector's tilt.
    4. SHADOW LIST: the names the tilt would move out of and into the
       investable list on the latest list day. It is not an order. When
       that day lies outside the window of the tilt shown in section 2 (a
       new tilt began today, the list is yesterday's), the page says so
       and claims no shadow action for a holding.
    5. TRIAL: v_trial_news_overlay as it stands, and the promotion rule.
    6. EVENTS AHEAD (v1.1.0): the dated events of the next 60 days from
       data/news_event_calendar.json, soonest first, each with its source
       page; and the next one in a line under the verdict. No query: the
       file is read from the repository. For information only.
    7. COMPANY NEWS, RE-READ (v1.2.0): each holding's headlines of the last
       7 days, re-classified from their own text (v_symbol_news_class), with
       the provider's old reading beside them and the record of the new
       classes (v_company_news_eval; v1.3.0: against the same-day US names,
       the S&P 500 beside it). For information only.
    8. NEWS BY MARKET (v1.4.0): v_market_news on the latest complete news
       day, one row per market.
    9. PEOPLE IN THE NEWS (v1.4.0): v_people_news, the latest day and the
       spikes of the last 7 days, one row per person.
    10. NEWS SIGNALS ON TRIAL (v1.4.0): v_news_trial_summary over 10 closes,
       forward lines first, and the signals' rank correlation.
    11. NEWS AND TREND NOW (v1.5.0): each holding and Top 10 name TFB follows
       read with the trend (v_news_understanding) and its state's forward
       record (v_news_trend_learning), then the latest events of every name.

SAFETY
    Read-only on BigQuery (SELECT only) and on every tab but its own; the
    holdings' symbols are read from My_Portfolio and are not printed in the
    run log. TFB_NEWS_BOARD = on (default) | off. --dry-run prints and writes
    nothing. A Sheets quota answer (HTTP 429) is waited out and tried again,
    for the read and for the one write (scripts/sheets_quota.py). Analysis only: no order is placed, no recommendation is changed;
    only the Today Action Plan gives orders.

ENV
    DEFAULT_SPREADSHEET_ID, GOOGLE_SHEETS_CREDENTIALS, TFB_BQ_PROJECT,
    TFB_BQ_DATASET (default tfb_archive), TFB_BQ_LOCATION (default US),
    TFB_NEWS_BOARD.
Exit: 0 written (or computed, in a dry run; or switched off), 2 a read, a
    query or the write failed - the previous page is left in place.

Not a licensed financial adviser; the owner decides and executes every trade.
================================================================================
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
from typing import Any, Dict, List, Optional, Sequence, Tuple

__version__ = "1.7.0"
_SLEEP = time.sleep                                     # the wait after a Sheets quota answer

UTC = _dt.timezone.utc
RIYADH = _dt.timezone(_dt.timedelta(hours=3))
TAB = "News_Board"
PORTFOLIO_TAB = "My_Portfolio"
WIDTH = 12
ARMS = ("events", "all")
ARM_TEXT = {"events": "Political events", "all": "Events + news themes"}
FORWARD = "forward from 2026-11-20"                     # the trial view's own label
FORWARD_WEEK = _dt.date(2026, 11, 16)                   # first news week that closed after the rule was fixed
STALE_DAYS = 3
MIN_HISTORY = 12                                        # usable weeks needed before a theme is called unusual
GROUP_TEXT = {"favoured": "FAVOURED", "avoided": "AVOIDED", "neutral": "neutral"}
ACTION_TEXT = {"out": "would leave", "in": "would enter", "keep": "stays"}
LINE_TEXT = {"CHAMPION": "List as it is", "NEWS_OVERLAY": "List with the news overlay",
             "MOVED_OUT": "Names the overlay removes", "MOVED_IN": "Names the overlay adds",
             "OVERLAY_MINUS_CHAMPION": "Overlay minus list (per day)"}
LINE_ORDER = tuple(LINE_TEXT)
# v1.1.0 [EVENTS]: the hand-kept calendar of dated events
CALENDAR_FILE = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data", "news_event_calendar.json")
CALENDAR_DAYS = 60                                      # how far ahead section 6 looks
CALENDAR_SOON = 14                                      # the line under the verdict names the next event this near
CALENDAR_STALE_DAYS = 21                                # a file checked longer ago than this is said on the page
KIND_TEXT = {"summit": "Summit", "central_bank": "Central bank", "energy": "Oil", "deadline": "Deadline",
             "budget": "Budget"}
STATUS_TEXT = {"confirmed": "confirmed", "tentative": "tentative", "date_not_set": "date not announced"}
# v1.2.0 [COMPANY]: the holdings' own news, re-read (v_symbol_news_class, v_company_news_eval)
COMPANY_DAYS = 7                                        # the window of section 7, today included
NEWS_CLASS_TEXT = {"positive": "POSITIVE", "negative": "NEGATIVE", "neutral": "neutral", "none": "no company news"}
EVENT_TEXT = {
    "PREVIEW": "results ahead", "TAKEOVER_TARGET": "takeover of the company", "OFFERING": "share offering",
    "LEGAL": "legal matter", "DIVIDEND_CUT": "dividend cut", "DIVIDEND_UP": "dividend raised",
    "GUIDANCE_DOWN": "guidance lowered", "GUIDANCE_UP": "guidance raised", "DOWNGRADE": "rating downgrade",
    "UPGRADE": "rating upgrade", "TARGET_DOWN": "price target lowered", "TARGET_UP": "price target raised",
    "EARNINGS_INLINE": "results in line", "EARNINGS_MISS": "results below estimates",
    "EARNINGS_BEAT": "results above estimates", "MGMT_EXIT": "management exit", "INDEX_ADD": "added to an index",
    "APPROVAL_DEAL": "approval or deal", "ESTIMATES_UP": "estimates rising", "ESTIMATES_DOWN": "estimates falling",
    "BUYBACK": "buyback", "PRICE_DOWN": "price fell (already happened)", "PRICE_UP": "price rose (already happened)",
    "OPINION_NEG": "opinion: negative", "OPINION_POS": "opinion: positive", "RESULTS": "results",
    "DIVIDEND_REGULAR": "regular dividend", "ACQUIRER": "acquisition by the company", "OTHER": "other news",
}
# v1.4.0 [MARKETS / PEOPLE]: the news by market and by person, and every news signal on trial (apply_bq_views 1.20.0)
MARKET_NEWS_DAYS = 10                                   # section 8 reads the latest news day of the last 10
PEOPLE_DAYS = 7                                         # section 9: the latest day and the spikes of the last 7
NEWS_TRIAL_HORIZON = 10                                 # section 10 shows the 10-close lines (5 and 20 in the IC line)
NEWS_FORWARD_FROM = _dt.date(2026, 11, 23)              # v_market_trial's forward start, which v_news_trial keeps
NEWS_REVIEW_SESSIONS = 10                               # a reading is read after 10 sessions of its own
NOT_SET_UP = "not set up yet: its BigQuery view (apply_bq_views 1.20.0) is applied after the owner merges the change"
READING_TEXT = {"favourable": "FAVOURABLE", "unfavourable": "UNFAVOURABLE", "neutral": "neutral"}
MOMENTUM_TEXT = {"improving": "improving", "worsening": "worsening", "steady": "steady"}
SURPRISE_TEXT = {"surge_favourable": "unusual attention, favourable tone",
                 "surge_unfavourable": "unusual attention, unfavourable tone", "none": ""}
SPIKE_TEXT = {"spike_positive": "SPIKE, positive tone", "spike_negative": "SPIKE, negative tone", "quiet": "quiet"}
SIGNAL_TEXT = {"market_news": "The market's news", "momentum": "Its momentum", "surprise": "Unusual attention"}
SIGNAL_READING_TEXT = {"favourable": "favourable", "neutral": "neutral", "unfavourable": "unfavourable",
                       "improving": "improving", "steady": "steady", "worsening": "worsening",
                       "surge_favourable": "a surge, favourable tone", "surge_unfavourable": "a surge, unfavourable tone",
                       "none": "no surge", "spike_positive": "a spike, positive tone",
                       "spike_negative": "a spike, negative tone", "quiet": "quiet"}
# v1.5.0 [NEWS NOW]: the news read with the trend, per holding and Top 10 name (apply_bq_views 1.25.0)
NEWS_NOW_BOARD_DAYS = 3                                 # Top 10 names seated in the last 3 days
NEWS_NOW_EVENTS = 8                                     # the latest events across every followed name
NEWS_NOW_HORIZON = 5                                    # the state's record shown over 5 sessions
INTERIM_NIGHT_HORIZONS = (1, 5)                         # v1.6.0: the nightly record's horizons in section 12
INTERIM_MIN_EVENTS = 20                                 # the views' own 'collecting' rule: 20 events and
HORIZON_TEXT = {0: "rest of the session", 1: "1 session", 5: "5 sessions"}   # max(10, 2 x h) dates
NEWS_NOW_FORWARD = "forward from 2026-11-24"            # v_news_trend_learning's evidence period
EXPECTED_TEXT = {1: "up against SPY", -1: "down against SPY", 0: "no lean"}
AGREEMENT_TEXT = {"agrees with TFB": "agrees with TFB", "leans against TFB": "LEANS AGAINST TFB"}
_DOW = ("Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun")
_MON = ("Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec")
# feature -> (label, kind of news, short name)
THEMES: Dict[str, Tuple[str, str, str]] = {
    "iran_war": ("Iran: fighting", "War", "Iran war"),
    "russia_war": ("Russia: fighting", "War", "Russia war"),
    "armed_conflict": ("Armed conflict, world", "War", "armed conflict"),
    "us_china_coercion": ("US-China: threats and pressure", "Politics", "US-China pressure"),
    "us_china_tone": ("US-China: tone of relations", "Politics", "US-China tone"),
    "election": ("Elections", "Politics", "elections"),
    "trade_policy": ("Trade policy and tariffs", "Economy", "trade policy"),
    "fed_rates": ("Central banks and interest rates", "Economy", "rates"),
    "inflation": ("Inflation", "Economy", "inflation"),
    "oil_price": ("Oil price", "Economy", "oil price"),
    "uncertainty": ("Economic uncertainty", "Economy", "uncertainty"),
    "ai_tech": ("AI and technology", "Technology", "AI"),
    "ai_tone": ("AI: tone of the news", "Technology", "AI tone"),
    "market_tone": ("Stock market: tone of the news", "Markets", "market tone"),
}
_ID_RE = re.compile(r"^[A-Za-z0-9_\-]+$")
_SYM_RE = re.compile(r"^[A-Z0-9][A-Z0-9.\-]{0,14}$")            # what may be sent to BigQuery
_HOLD_RE = re.compile(r"^[A-Z0-9^][A-Z0-9.\-=^]{0,14}$")         # what may be shown as a holding (AAPL, GC=F, ^GSPC)
_NOT_A_HOLDING = frozenset({"TOTAL", "TOTALS", "SUBTOTAL"})
_DAY_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")
_URL_RE = re.compile(r"^https://[A-Za-z0-9.\-]+\.[A-Za-z]{2,}(?:/[^\s\"<>]*)?$")   # a calendar source: https, a host, no spaces


def _out(msg: str) -> None:
    print("[news_board %s] %s" % (__version__, msg), flush=True)


def _env(name: str, default: str = "") -> str:
    return (os.environ.get(name) or default).strip()


def enabled() -> bool:
    return _env("TFB_NEWS_BOARD", "on").lower() not in ("off", "0", "false", "no")


def num(v: Any) -> Optional[float]:
    if v is None or isinstance(v, bool):
        return None
    if isinstance(v, (int, float)):
        return float(v) if math.isfinite(float(v)) else None
    try:
        x = float(str(v).replace(",", "").replace("%", "").strip())
    except ValueError:
        return None
    return x if math.isfinite(x) else None


def text(v: Any) -> str:
    return "" if v is None else str(v).strip()


def flag(v: Any) -> bool:
    """A BigQuery BOOLEAN as the REST API returns it ("true" / "false")."""
    return v is True or text(v).lower() == "true"


def day(v: Any) -> Optional[_dt.date]:
    try:
        return _dt.date.fromisoformat(text(v)[:10])
    except ValueError:
        return None


def fmt(v: Any, nd: int = 2, sign: bool = False) -> str:
    x = num(v)
    if x is None:
        return ""
    return ("{:%s,.%df}" % ("+" if sign else "", nd)).format(x)


def market_of(symbol: str) -> str:
    """The exchange suffix; a plain ticker with none (AAPL) is read as US, as the archive files it."""
    s = text(symbol).upper()
    if "." in s:
        return s.rsplit(".", 1)[1]
    return "US" if s.isalnum() else ""


def aliases(symbol: str) -> List[str]:
    """The names a holding may carry in the archive: itself, and TICKER.US for a plain ticker."""
    s = text(symbol).upper()
    return [s, s + ".US"] if s and "." not in s and s.isalnum() else [s]


# ----------------------------------------------------------------------------- #
# 1. World news
# ----------------------------------------------------------------------------- #
def theme_reading(kind: str, surprise: Optional[float], sd: Optional[float], n: Optional[float]) -> str:
    """How unusual a week is against the theme's own weekly changes of the last year."""
    if surprise is None:
        return "no reading yet"
    if sd is None or sd <= 0 or (n or 0) < MIN_HISTORY:
        return "too little history to compare"
    z = surprise / sd
    if abs(z) < 0.5:
        return "about normal"
    far = "far " if abs(z) >= 1.5 else ""
    if kind == "mean":
        return "tone %s%s than normal" % (far, "more positive" if z > 0 else "more negative")
    return "%s%s news than normal" % (far, "more" if z > 0 else "less")


def change_text(kind: str, value: Optional[float], prev: Optional[float]) -> str:
    if value is None or prev is None:
        return ""
    if kind == "mean":
        return "%+.2f" % (value - prev)
    if prev <= 0:
        return ""
    return "%+.0f%%" % (100.0 * (value / prev - 1.0))


def theme_rows(rows: Sequence[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """One line per theme: its last closed week and the running week (when one has begun)."""
    by: Dict[str, List[Dict[str, Any]]] = {}
    for r in rows:
        if text(r.get("feature")) and day(r.get("week_start")):
            by.setdefault(text(r.get("feature")), []).append(r)
    order = [f for f in THEMES if f in by] + sorted(f for f in by if f not in THEMES)
    out: List[Dict[str, Any]] = []
    for feat in order:
        wk = sorted(by[feat], key=lambda r: text(r.get("week_start")))
        closed = [r for r in wk if flag(r.get("is_closed"))]
        last = closed[-1] if closed else None
        later = [r for r in wk if not flag(r.get("is_closed"))
                 and (last is None or text(r.get("week_start")) > text(last.get("week_start")))]
        cur = later[-1] if later else None
        label, kind_of_news, _ = THEMES.get(feat, (feat, "Other", feat))
        kind = text((last or cur or {}).get("kind"))
        d: Dict[str, Any] = {"feature": feat, "label": label, "news": kind_of_news, "kind": kind,
                             "week_start": text(last.get("week_start")) if last else "",
                             "week_end": text(last.get("week_end")) if last else "",
                             "value": None, "prev": None, "change": "", "reading": "no full week yet",
                             "now_value": None, "now_days": None, "now_reading": ""}
        if last:
            v, p = num(last.get("value")), num(last.get("prev_4w_avg"))
            d.update(value=v, prev=p, change=change_text(kind, v, p))
            d["reading"] = theme_reading(kind, num(last.get("surprise")), num(last.get("sd")), num(last.get("n"))) \
                if flag(last.get("usable")) else "no reading: the week or the four before it are incomplete"
        if cur:
            d.update(now_value=num(cur.get("value")), now_days=num(cur.get("days")))
            if num(cur.get("prev_4w_avg")) is not None:
                d["now_reading"] = theme_reading(kind, num(cur.get("surprise")), num(cur.get("sd")), num(cur.get("n")))
        out.append(d)
    return out


# ----------------------------------------------------------------------------- #
# 2. Sector tilt
# ----------------------------------------------------------------------------- #
def pick_tilt(rows: Sequence[Dict[str, Any]], today: _dt.date) -> Dict[str, Dict[str, Any]]:
    """Per arm: the news week whose tilt has begun to apply (the latest one), whether it is still in force
    today, and the week before it."""
    out: Dict[str, Dict[str, Any]] = {}
    for arm in ARMS:
        weeks: Dict[str, List[Dict[str, Any]]] = {}
        for r in rows:
            if text(r.get("arm")) == arm and day(r.get("week_start")) and day(r.get("apply_from")) and text(r.get("sector")):
                weeks.setdefault(text(r.get("week_start"))[:10], []).append(r)
        begun = sorted(w for w, rs in weeks.items() if day(rs[0].get("apply_from")) <= today)
        if not begun:
            continue
        cur = begun[-1]
        older = [w for w in sorted(weeks) if w < cur]

        def pack(week: str) -> Dict[str, Dict[str, Any]]:
            return {text(r.get("sector")): {"etf": text(r.get("etf")), "group": text(r.get("tilt_group")) or "neutral",
                                            "score": num(r.get("score")), "pos": num(r.get("pos")),
                                            "active": text(r.get("active_list")),
                                            "realized": num(r.get("realized_excess_pp"))}
                    for r in weeks[week]}
        first = weeks[cur][0]
        a_from, a_to = day(first.get("apply_from")), day(first.get("apply_to"))
        out[arm] = {"week": cur, "apply_from": a_from, "apply_to": a_to,
                    "in_force": bool(a_to and a_from <= today <= a_to), "sectors": pack(cur),
                    "prev_week": older[-1] if older else "", "prev": pack(older[-1]) if older else {}}
    return out


def themes_text(active: str) -> str:
    return ", ".join(THEMES.get(f.strip(), ("", "", f.strip()))[2] for f in text(active).split(",") if f.strip())


def sector_rows(tilt: Dict[str, Dict[str, Any]]) -> List[Dict[str, Any]]:
    """The sectors in the order of the first arm that has a tilt; both arms side by side."""
    lead = next((a for a in ARMS if a in tilt), None)
    if lead is None:
        return []
    names = sorted(tilt[lead]["sectors"], key=lambda s: (tilt[lead]["sectors"][s]["pos"] or 99, s))
    names += sorted(s for a in ARMS if a in tilt for s in tilt[a]["sectors"] if s not in names)
    seen, out = set(), []
    for s in names:
        if s in seen:
            continue
        seen.add(s)
        row: Dict[str, Any] = {"sector": s, "etf": ""}
        for a in ARMS:
            cell = (tilt.get(a) or {}).get("sectors", {}).get(s) or {}
            row["etf"] = row["etf"] or cell.get("etf", "")
            row[a] = cell.get("group", "")
            row[a + "_score"] = cell.get("score")
        wide = (tilt.get("all") or tilt[lead])["sectors"].get(s) or {}
        row["themes"] = themes_text(wide.get("active", ""))
        prev = tilt[lead]["prev"].get(s) or {}
        row["prev"], row["prev_realized"] = prev.get("group", ""), prev.get("realized")
        out.append(row)
    return out


def record_reading(period: str, weeks: Optional[float], spread: Optional[float], t: Optional[float]) -> str:
    ahead = spread is not None and spread > 0 and t is not None and t >= 2.0
    if period != "forward":
        return "past weeks, not a clean test: " + ("ahead" if ahead else "no better than chance")
    if (weeks or 0) < 8:
        return "collecting: %d of 8 weeks" % int(weeks or 0)
    return "ahead so far" if ahead else "no better than chance so far"


def record_rows(rows: Sequence[Dict[str, Any]]) -> List[Dict[str, Any]]:
    out = []
    for arm in ARMS:
        for period in ("forward", "before"):
            for r in rows:
                if text(r.get("arm")) == arm and text(r.get("period")) == period:
                    w, sp, t = num(r.get("weeks")), num(r.get("spread")), num(r.get("t"))
                    out.append({"arm": arm, "period": period, "weeks": w, "fav": num(r.get("fav")), "avo": num(r.get("avo")),
                                "spread": sp, "t": t, "right": num(r.get("right_pct")),
                                "reading": record_reading(period, w, sp, t)})
    return out


# ----------------------------------------------------------------------------- #
# 3 and 4. Holdings and the shadow list
# ----------------------------------------------------------------------------- #
def clean_symbols(symbols: Sequence[Any]) -> List[str]:
    out: List[str] = []
    for s in symbols:
        u = text(s).upper()
        if _SYM_RE.match(u) and u not in out:
            out.append(u)
    return out


def read_holdings(values: Sequence[Sequence[Any]]) -> List[Dict[str, str]]:
    """Symbol and name of each row under the 'Symbol' header of My_Portfolio (order kept). A symbol needs no
    exchange suffix (AAPL, QQQ and GC=F are holdings too - Codex P2, #1032); only a totals label is passed over."""
    rows = [list(r) for r in values or []]
    for hi, r in enumerate(rows[:12]):
        cells = [text(c) for c in r]
        if "Symbol" in cells:
            si = cells.index("Symbol")
            out, seen = [], set()
            for row in rows[hi + 1:]:
                sym = text(row[si]).upper() if len(row) > si else ""
                if _HOLD_RE.match(sym) and sym not in _NOT_A_HOLDING and sym not in seen:
                    seen.add(sym)
                    out.append({"symbol": sym, "name": text(row[si + 1]) if len(row) > si + 1 else ""})
            return out
    return []


def shadow_names(rows: Sequence[Dict[str, Any]]) -> Tuple[str, Dict[str, Dict[str, Any]]]:
    """The latest list day of the rows and, per symbol, what each arm would do with it."""
    days = sorted({text(r.get("base_date"))[:10] for r in rows if day(r.get("base_date"))})
    if not days:
        return "", {}
    last = days[-1]
    names: Dict[str, Dict[str, Any]] = {}
    for r in rows:
        if text(r.get("base_date"))[:10] != last or not text(r.get("symbol")):
            continue
        sym = text(r.get("symbol")).upper()
        d = names.setdefault(sym, {"symbol": sym, "name": text(r.get("name")), "sector": text(r.get("sector")),
                                   "score": num(r.get("overall_score")), "vol": text(r.get("vol_band")),
                                   "listed": False, "actions": {}})
        d["listed"] = d["listed"] or flag(r.get("in_champion"))
        if text(r.get("arm")) in ARMS and text(r.get("news_action")) in ACTION_TEXT:
            d["actions"][text(r.get("arm"))] = text(r.get("news_action"))
    return last, names


def shadow_counts(names: Dict[str, Dict[str, Any]]) -> Dict[str, Dict[str, int]]:
    out = {}
    for a in ARMS:
        acts = [d["actions"].get(a) for d in names.values()]
        out[a] = {"list": sum(1 for x in acts if x in ("keep", "out")), "out": acts.count("out"), "in": acts.count("in")}
    return out


def shadow_rows(names: Dict[str, Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Names an arm would move: those leaving first (by sector), then those entering (best engine score first)."""
    moved = [d for d in names.values() if any(d["actions"].get(a) in ("out", "in") for a in ARMS)]
    leaving = [d for d in moved if "out" in d["actions"].values()]
    entering = [d for d in moved if "out" not in d["actions"].values()]
    return sorted(leaving, key=lambda d: (d["sector"], d["symbol"])) \
        + sorted(entering, key=lambda d: (-(d["score"] if d["score"] is not None else -1.0), d["symbol"]))


def shadow_note(shadow_day: str, tilt: Dict[str, Dict[str, Any]]) -> str:
    """Empty when the list day lies inside the window of the tilt the page shows; else what the reader must know."""
    lead = next((a for a in ARMS if a in tilt), None)
    d = day(shadow_day)
    if lead is None or d is None:
        return ""
    a_from, a_to = tilt[lead]["apply_from"], tilt[lead]["apply_to"]
    if a_from <= d and (a_to is None or d <= a_to):
        return ""
    return ("NOTE: this list day is outside the window of the tilt in section 2 (%s to %s); its moves come from the tilt "
            "that applied on that day." % (a_from.isoformat(), a_to.isoformat() if a_to else "?"))


def holding_rows(holdings: Sequence[Dict[str, str]], sectors: Dict[str, str], tilt: Dict[str, Dict[str, Any]],
                 names: Dict[str, Dict[str, Any]], shadow_day: str = "") -> List[Dict[str, Any]]:
    """The shadow column is filled only when the list day lies under the tilt shown beside it; with no list day
    nothing is said about the list."""
    aligned = bool(shadow_day) and not shadow_note(shadow_day, tilt)
    out = []
    for h in holdings:
        sym = h["symbol"]
        sector = next((sectors[a] for a in aliases(sym) if sectors.get(a)), "")
        seen = next((names[a] for a in aliases(sym) if a in names), {})
        row: Dict[str, Any] = {"symbol": sym, "name": h.get("name") or seen.get("name", ""), "sector": sector,
                               "listed": ("yes" if seen.get("listed") else "no") if shadow_day else "",
                               "shadow": ACTION_TEXT.get((seen.get("actions") or {}).get("events", ""), "") if aligned else ""}
        for a in ARMS:
            t = tilt.get(a) or {}
            row[a] = (t.get("sectors", {}).get(sector) or {}).get("group", "") if t.get("in_force") else ""
        if not sector:
            row["note"] = "sector not known to the archive"
        elif market_of(sym) != "US":
            row["note"] = "the tilt is measured on US sector funds: indicative only for this market"
        else:
            row["note"] = ""
        out.append(row)
    return out


# ----------------------------------------------------------------------------- #
# v1.2.0 [COMPANY]: section 7, each holding's own news re-read
# ----------------------------------------------------------------------------- #
def news_class(score: Optional[float], company_items: int) -> str:
    """The week's class of a name from the summed score of its company headlines (the view's own thresholds)."""
    if not company_items:
        return "none"
    if score is not None and score >= 1:
        return "positive"
    if score is not None and score <= -1:
        return "negative"
    return "neutral"


def provider_reading(mean: Optional[float]) -> str:
    """The provider's old reading of the same headlines (the thresholds v_news_signal_eval uses)."""
    if mean is None:
        return ""
    return "positive" if mean >= 0.15 else ("negative" if mean <= -0.15 else "neutral")


def company_rows(holdings: Sequence[Dict[str, str]], found: Sequence[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """One row per holding, in the order of My_Portfolio; a holding with no headline at all says so."""
    by = {text(r.get("symbol")).upper(): r for r in found or []}
    out = []
    for h in holdings:
        r = next((by[a] for a in aliases(h["symbol"]) if a in by), {})
        comp, other = int(num(r.get("company_items")) or 0), int(num(r.get("other_items")) or 0)
        score = num(r.get("score")) if comp else None
        cls = news_class(score, comp)
        title = text(r.get("top_title")) if comp else ""
        out.append({"symbol": h["symbol"], "name": h.get("name", ""), "class": cls, "score": score,
                    "company": comp, "other": other, "title": title[:160] + ("..." if len(title) > 160 else ""),
                    "event": EVENT_TEXT.get(text(r.get("top_event")), text(r.get("top_event"))) if comp else "",
                    "day": text(r.get("top_day"))[:10] if comp else "",
                    "provider": provider_reading(num(r.get("provider_mean"))) if r else "",
                    "note": ("no headline in the archive for these %d days" % COMPANY_DAYS) if not r else ""})
    return out


def _class_figure(c: str, mean: Any, days: Any, beat: Any = None) -> str:
    if num(mean) is None:
        return "%s: no matured day yet" % c
    return "%s %s pts on %s day(s)%s" % (c, fmt(mean, 2, True), fmt(days, 0) or "0",
                                         (", ahead on %s %%" % fmt(beat, 1)) if num(beat) is not None else "")


def company_record_line(rows: Sequence[Dict[str, Any]]) -> str:
    """The record of the new classes, in one line; the provider's own reading after it. v1.3.0: against the same-day
    average of the US names (the view's reading, apply_bq_views 1.17.0), the S&P 500 beside it; rows from the view
    before 1.17.0 (no peer column) keep the 1.2.0 line against the S&P 500 and say the peers are still to come."""
    mine = {text(r.get("class")): r for r in rows if text(r.get("signal")) == "company_news"}
    if not mine:
        return "Record of the new classes: no scored day yet."
    order = [c for c in ("positive", "neutral", "negative") if c in mine]
    reading = text(next(iter(mine.values())).get("reading")) or "not read"
    prov = next((text(r.get("reading")) for r in rows if text(r.get("signal")) == "provider_sentiment"), "")
    tail = (" The provider's own reading, scored the same way: %s." % prov) if prov else ""
    if not any("mean_5d_peer_pp" in mine[c] for c in order):
        parts = [_class_figure(c, mine[c].get("mean_5d_excess_pp"), mine[c].get("matured_5d")) for c in order]
        return ("Record of the new classes (US names, the 5 sessions after the news day, against the S&P 500; the "
                "comparison with the same-day US names comes with the next BigQuery views update): %s - reading: %s.%s"
                % ("; ".join(parts), reading, tail))
    peers = next((fmt(mine[c].get("peers"), 0) for c in order if num(mine[c].get("peers"))), "")
    parts = [_class_figure(c, mine[c].get("mean_5d_peer_pp"), mine[c].get("matured_5d_peer", mine[c].get("matured_5d")),
                           mine[c].get("beat_5d_peer_pct")) for c in order]
    spy = "; ".join("%s %s" % (c, fmt(mine[c].get("mean_5d_excess_pp"), 2, True) or "n/a") for c in order)
    return ("Record of the new classes (US names, the 5 sessions after the news day, against the same-day average of "
            "the US names%s): %s - reading: %s. Against the S&P 500 instead: %s pts.%s"
            % ((", about %s of them" % peers) if peers else "", "; ".join(parts), reading, spy, tail))


# ----------------------------------------------------------------------------- #
# v1.4.0 [MARKETS / PEOPLE]: sections 8 to 10 (views of apply_bq_views 1.20.0)
# ----------------------------------------------------------------------------- #
def market_news_rows(rows: Optional[Sequence[Dict[str, Any]]]) -> Tuple[str, List[Dict[str, Any]]]:
    """The latest news day of v_market_news and its markets, in v_market_condition's order."""
    days = sorted({text(r.get("date")) for r in rows or [] if _DAY_RE.match(text(r.get("date")))})
    if not days:
        return "", []
    last = days[-1]
    out = []
    for r in rows or []:
        if text(r.get("date")) != last:
            continue
        out.append({"order": num(r.get("market_order")) or 99, "market": text(r.get("market")),
                    "place": text(r.get("place_name")), "reading": text(r.get("reading")),
                    "score": num(r.get("market_score")), "momentum": text(r.get("momentum_label")),
                    "surprise": text(r.get("surprise")), "docs": num(r.get("docs")), "att_z": num(r.get("att_z")),
                    "tone_z": num(r.get("tone_z")), "fight_z": num(r.get("fight_z"))})
    return last, sorted(out, key=lambda m: (m["order"], m["market"]))


def people_rows(rows: Optional[Sequence[Dict[str, Any]]]) -> Tuple[str, List[Dict[str, Any]]]:
    """Each person's latest news day of v_people_news, with the spikes of the days read (the last PEOPLE_DAYS)."""
    by: Dict[str, Dict[str, Any]] = {}
    last = ""
    for r in rows or []:
        pid, d = text(r.get("person_id")), text(r.get("date"))
        if not pid or not _DAY_RE.match(d):
            continue
        p = by.setdefault(pid, {"order": num(r.get("person_order")) or 99, "person": text(r.get("person")),
                                "role": text(r.get("role")), "markets": text(r.get("markets")), "date": "",
                                "spikes": 0, "spike_days": []})
        if text(r.get("reading")).startswith("spike_"):
            p["spikes"] += 1
            p["spike_days"].append(d)
        if d > p["date"]:
            p.update(date=d, docs=num(r.get("docs")), share=num(r.get("share_pct")),
                     normal=num(r.get("normal_share_pct")), att_z=num(r.get("att_z")), tone_z=num(r.get("tone_z")),
                     reading=text(r.get("reading")))
        last = max(last, d)
    return last, sorted(by.values(), key=lambda p: (p["order"], p["person"]))


def news_trial_rows(rows: Optional[Sequence[Dict[str, Any]]], horizon: int = NEWS_TRIAL_HORIZON) -> List[Dict[str, Any]]:
    """The lines of section 10: one horizon, forward first, the market's own signals before the people; a person's
    quiet days are the base the spikes are read against and are left out of the table."""
    def key(r: Dict[str, Any]) -> Tuple[int, int, str, float, str]:
        sig = text(r.get("signal"))
        return (0 if text(r.get("period")) == "forward" else 1,
                list(SIGNAL_TEXT).index(sig) if sig in SIGNAL_TEXT else 9, sig, -(num(r.get("expected")) or 0),
                text(r.get("reading")))
    keep = [dict(r) for r in rows or [] if num(r.get("horizon")) == horizon
            and not (text(r.get("signal")).startswith("person_") and text(r.get("reading")) == "quiet")]
    return sorted(keep, key=key)


def signal_name(signal: str, people: Sequence[Dict[str, Any]]) -> str:
    if signal in SIGNAL_TEXT:
        return SIGNAL_TEXT[signal]
    pid = signal[len("person_"):] if signal.startswith("person_") else signal
    names = {text(p.get("person_id")): text(p.get("person")) for p in people}
    return "News of %s" % (names.get(pid) or pid)


def ic_line(rows: Optional[Sequence[Dict[str, Any]]], period: str = "backtest") -> str:
    """The rank correlation of the value-carrying market signals, 5 / 10 / 20 closes, in one line."""
    parts = []
    for sig in ("market_news", "momentum"):
        got = {int(num(r.get("horizon")) or 0): r for r in rows or []
               if text(r.get("signal")) == sig and text(r.get("period")) == period and num(r.get("ic")) is not None}
        if not got:
            continue
        hs = sorted(got)
        parts.append("%s %s (t %s; %s sessions)" % (
            SIGNAL_TEXT[sig].lower(), " / ".join(fmt(got[h].get("ic"), 3, True) for h in hs),
            " / ".join(fmt(got[h].get("ic_t_adj"), 2) or "n/a" for h in hs), fmt(got[hs[0]].get("ic_days"), 0)))
    if not parts:
        return ""
    return ("Rank correlation across the markets between the reading and the next 5 / 10 / 20 closes (%s): %s. Above 0 "
            "means the reading ranked the markets in the right order." % (period, "; ".join(parts)))


def news_verdict_lines(market_day: str, markets: Optional[List[Dict[str, Any]]], people_day: str,
                       people: Optional[List[Dict[str, Any]]], trial: Optional[List[Dict[str, Any]]]) -> List[str]:
    out: List[str] = []
    if markets is None:
        out.append("News by market: %s." % NOT_SET_UP)
    elif not markets:
        out.append("News by market: no news day read in the last %d days." % MARKET_NEWS_DAYS)
    else:
        fav = [m["market"] for m in markets if m["reading"] == "favourable"]
        unf = [m["market"] for m in markets if m["reading"] == "unfavourable"]
        surge = [m["market"] for m in markets if m["surprise"].startswith("surge_")]
        out.append("News by market, news day %s: favourable %s; unfavourable %s%s." % (
            market_day, ", ".join(fav) or "none", ", ".join(unf) or "none",
            ("; unusual attention: %s" % ", ".join(surge)) if surge else ""))
    if people is None:
        out.append("People in the news: %s." % NOT_SET_UP)
    else:
        today = [p for p in people if p.get("date") == people_day and text(p.get("reading")).startswith("spike_")]
        out.append("People in the news, %s: %s." % (people_day or "no day read", "; ".join(
            "%s (%s tone)" % (p["person"], "positive" if p["reading"] == "spike_positive" else "negative")
            for p in today) or "no spike"))
    if trial is None:
        out.append("News signals on trial: %s." % NOT_SET_UP)
    else:
        # Codex P2 (#1047): the headline counts the horizon section 10 shows, so a reading that holds at 5 closes only
        # is not "held over 10 closes", one that holds at all three counts once, and the forward count waits for the
        # 10-close outcomes to mature like the section's own lines.
        trial = [r for r in trial if num(r.get("horizon")) == NEWS_TRIAL_HORIZON]
        fwd = [r for r in trial if text(r.get("period")) == "forward"]
        held = [r for r in trial if text(r.get("period")) == "backtest" and text(r.get("verdict")).startswith("holds")]
        days = max((int(num(r.get("days")) or 0) for r in fwd), default=0)
        out.append("News signals on trial: forward from %s, %s; backtest (not evidence): %s." % (
            NEWS_FORWARD_FROM.isoformat(),
            "no session counted yet" if not fwd else "up to %d of %d sessions" % (days, NEWS_REVIEW_SESSIONS),
            "%d reading(s) hold over %d closes" % (len(held), NEWS_TRIAL_HORIZON) if held else
            "no reading holds over %d closes" % NEWS_TRIAL_HORIZON))
    return out


def read_optional(session: Any, project: str, location: str, sql: str, query: Any) -> Optional[List[Dict[str, Any]]]:
    """v1.4.0: the rows of a view of apply_bq_views 1.20.0, or None while it is not in BigQuery yet - the section says
    so and the page is built all the same; any other refusal still fails the run."""
    try:
        return query(session, project, location, sql)
    except RuntimeError as e:
        if "Not found" not in text(e):
            raise
        _out("a view of apply_bq_views 1.20.0 is not in BigQuery yet: %s" % sql.split("FROM `", 1)[1].split("`", 1)[0]
             .rsplit(".", 1)[-1])
        return None


# ----------------------------------------------------------------------------- #
# v1.5.0 [NEWS NOW]: section 11
# ----------------------------------------------------------------------------- #
def _int(v: Any) -> Optional[int]:
    x = num(v)
    return int(x) if x is not None else None


def state_records(rows: Optional[Sequence[Dict[str, Any]]]) -> Dict[str, Dict[str, Any]]:
    """{state: its forward record over NEWS_NOW_HORIZON sessions} from v_news_trend_learning (trend fit 'all')."""
    out: Dict[str, Dict[str, Any]] = {}
    for r in rows or []:
        if text(r.get("period")) == NEWS_NOW_FORWARD and _int(r.get("horizon_sessions")) == NEWS_NOW_HORIZON \
                and text(r.get("trend_fit")) == "all":
            out[text(r.get("state"))] = r
    return out


def record_text(rec: Optional[Dict[str, Any]]) -> str:
    """One cell: the state's forward record, or that none has matured yet."""
    if not rec:
        return "no forward event matured yet"
    ev = _int(rec.get("events")) or 0
    right = num(rec.get("right_pct"))
    return "%s | %d event(s)%s%s" % (
        text(rec.get("reading")) or "not read", ev,
        ", %.0f%% went the expected way" % right if right is not None else "",
        ", edge %s pp" % fmt(rec.get("edge_pp"), 2, True) if num(rec.get("edge_pp")) is not None else "")


def answer_text(r: Dict[str, Any]) -> str:
    """How the price answered: the 2-session reaction against SPY when the event has one, else the day's move.
    An event on the latest session has only its first day: the cell says the next session is still to come."""
    if num(r.get("reaction_pp")) is not None:
        done = text(r.get("reaction_complete")).lower() not in ("false", "0")
        return "%s pp (z %s) %s" % (fmt(r.get("reaction_pp"), 2, True), fmt(r.get("reaction_z"), 1, True),
                                    "over 2 sessions" if done else "first session only, the next is still to come")
    if num(r.get("day_pp")) is not None:
        return "%s pp (z %s) on the day" % (fmt(r.get("day_pp"), 2, True), fmt(r.get("day_z"), 1, True))
    return ""


def trend_text(r: Dict[str, Any]) -> str:
    """The trend before the shown event (the one its state was read against); the trend now beside it when it changed.
    A name with no event shows the trend now."""
    before, now = text(r.get("trend_before")), text(r.get("trend_now"))
    if not text(r.get("event_day")) or not before:
        return now
    return before if not now or now == before else "%s before the event; %s now" % (before, now)


def news_now_row(r: Dict[str, Any], why: str, records: Dict[str, Dict[str, Any]]) -> Dict[str, Any]:
    exp = _int(r.get("expected"))
    state = text(r.get("state")) or "quiet"
    return {"symbol": text(r.get("symbol")), "name": text(r.get("name")), "why": why,
            "event_day": text(r.get("event_day")), "state": state, "answer": answer_text(r),
            "trend": trend_text(r), "expected": EXPECTED_TEXT.get(exp, "") if exp is not None else "",
            "tfb": text(r.get("recommendation")), "agreement": AGREEMENT_TEXT.get(text(r.get("agreement")),
                                                                               text(r.get("agreement"))),
            "record": record_text(records.get(state)) if exp not in (None, 0) else "",
            "headline": text(r.get("top_headline"))[:160]}


def news_now_rows(holdings: Sequence[Dict[str, str]], board: Sequence[str], found: Optional[Sequence[Dict[str, Any]]],
                  records: Dict[str, Dict[str, Any]]) -> Optional[List[Dict[str, Any]]]:
    """One row per holding, then per Top 10 name not held, in that order; a name TFB does not follow says so."""
    if found is None:
        return None
    by = {text(r.get("symbol")).upper(): r for r in found}
    out: List[Dict[str, Any]] = []
    seen: set = set()
    listed = [(h["symbol"], "holding") for h in holdings] + [(b, "Top 10") for b in board]
    for sym, why in listed:
        hit = next((by[a] for a in aliases(sym) if a in by), None)
        key = text(hit.get("symbol")).upper() if hit else sym.upper()
        if key in seen:
            continue
        seen.add(key)
        if hit:
            out.append(news_now_row(hit, why, records))
        elif market_of(sym) == "US":
            out.append({"symbol": sym, "name": "", "why": why, "state": "", "event_day": "", "answer": "",
                        "trend": "", "expected": "", "tfb": "", "agreement": "", "record": "",
                        "headline": "not in the followed set yet (company news is collected for the holdings, the "
                                    "board, the ROI leaders, the 50 largest US names and the day's big movers)"})
    return out


def news_now_lines(rows: Optional[Sequence[Dict[str, Any]]], events: Optional[Sequence[Dict[str, Any]]],
                   records: Dict[str, Dict[str, Any]]) -> List[str]:
    """The lines under the verdict: what section 11 holds today, in counts."""
    if rows is None:
        return ["NEWS AND TREND NOW: %s." % NOT_SET_UP.replace("1.20.0", "1.25.0")]
    leaning = [r for r in rows if r.get("expected") and r["expected"] != "no lean"]
    against = [r for r in rows if r.get("agreement") == AGREEMENT_TEXT["leans against TFB"]]
    proven = sorted(s for s, rec in records.items() if text(rec.get("reading")) == "works as expected")
    return ["NEWS AND TREND NOW: %d of your holdings and Top 10 names read; %d with a lean for the next sessions, "
            "%d leaning against TFB's view; %d event(s) across all followed names. %s" % (
                len(rows), len(leaning), len(against), len(events or []),
                ("States proven forward: %s." % ", ".join(proven)) if proven else
                "No state is proven forward yet (evidence from 24 Nov 2026; first reading 8 Dec): read it as "
                "context, not as a signal.")]


def _interim_row(source: str, r: Dict[str, Any]) -> Dict[str, Any]:
    h = _int(r.get("horizon_sessions")) or 0
    ev, days = _int(r.get("events")) or 0, _int(r.get("days")) or 0
    need = max(10, 2 * h)
    reading = text(r.get("reading")) or "not read"
    right, edge, t = num(r.get("right_pct")), num(r.get("edge_pp")), num(r.get("t_adj"))
    return {"source": source, "state": text(r.get("state")), "h": h,
            "horizon": HORIZON_TEXT.get(h, "%d sessions" % h), "events": ev, "days": days,
            "right": "%.0f%%" % right if right is not None else "",
            "edge": "%s pp" % fmt(edge, 2, True) if edge is not None else "",
            "t": fmt(t, 1, True) if t is not None else "", "reading": reading,
            "progress": ("%d of %d events, %d of %d dates" % (min(ev, INTERIM_MIN_EVENTS), INTERIM_MIN_EVENTS,
                                                             min(days, need), need)) if reading == "collecting" else ""}


def interim_rows(night: Optional[Sequence[Dict[str, Any]]], hourly: Optional[Sequence[Dict[str, Any]]]
                 ) -> Dict[str, Optional[List[Dict[str, Any]]]]:
    """v1.6.0: section 12 - the forward rows of the nightly record (trend fit 'all', the states with an expected move,
    1 and 5 sessions) and every row of the hourly record; None for a record whose view is not in BigQuery yet."""
    def key(r: Dict[str, Any]) -> Tuple[int, int, str]:
        return (0 if r["state"] == "all with a lean" else 1, r["h"], r["state"])
    n = None if night is None else sorted(
        (_interim_row("nightly", r) for r in night
         if text(r.get("period")) == NEWS_NOW_FORWARD and text(r.get("trend_fit")) == "all"
         and _int(r.get("horizon_sessions")) in INTERIM_NIGHT_HORIZONS
         and text(r.get("state")) not in ("quiet", "routine news", "quiet lately")), key=key)
    h = None if hourly is None else sorted((_interim_row("hourly", r) for r in hourly), key=key)
    return {"nightly": n, "hourly": h}


def interim_line(rec: Dict[str, Optional[List[Dict[str, Any]]]]) -> str:
    """The line under the verdict, counts only."""
    parts = []
    for src, label in (("nightly", "nightly news-and-trend record"), ("hourly", "hourly News Now record")):
        rows = rec.get(src)
        if rows is None:
            parts.append("%s: not set up yet" % label)
            continue
        proven = sorted({r["state"] for r in rows if r["reading"] == "works as expected"})
        against = sorted({r["state"] for r in rows if r["reading"] == "opposite of expected"})
        parts.append("%s: %d event(s) scored in %d state-horizon(s), %s%s" % (
            label, sum(r["events"] for r in rows), len(rows),
            ("proven: " + ", ".join(proven)) if proven else "none proven yet",
            ("; opposite of expected: " + ", ".join(against)) if against else ""))
    return "THE NEWS RECORD SO FAR (forward from 24 Nov 2026; first full reading 8 Dec): " + "; ".join(parts) + "."


def interim_section_rows(data: Dict[str, Any], pad: Any) -> List[List[Any]]:
    """v1.6.0: section 12."""
    head = ["Record", "State", "Horizon", "Events", "Dates", "Went the expected way", "Edge vs SPY", "t",
            "Reading", "Still to collect"]
    out = [pad([]),
           pad(["12. THE NEWS RECORD SO FAR - did each news state move the stock against SPY the way research "
                "expected? Forward from 24 Nov 2026 only; a state is read once it holds 20 events over at least "
                "max(10, 2 x horizon) dates, and switches nothing on before the owner's review."]),
           pad(head)]
    rec = data.get("interim") or {}
    for src, label in (("nightly", "Nightly news and trend (v_news_trend_learning)"),
                       ("hourly", "Hourly News Now (v_news_now_learning)")):
        rows = rec.get(src)
        if rows is None:
            out.append(pad([label, "not set up yet: its BigQuery view is applied after the owner merges the change"]))
        elif not rows:
            out.append(pad([label, "no forward event has its outcome yet: the 1-session outcomes arrive the morning "
                                   "after the next session closes, the 5-session ones a week later"]))
        for r in rows or []:
            out.append(pad([label, r["state"], r["horizon"], r["events"], r["days"], r["right"], r["edge"], r["t"],
                            r["reading"].upper() if r["reading"] in ("works as expected", "opposite of expected")
                            else r["reading"], r["progress"]]))
    out.append(pad(["How to read: the edge is the mean return against SPY in the direction the state expected (above "
                    "0 = it went that way); t is over distinct dates, each date's events averaged first. 'Works as "
                    "expected' needs t of 2 or more. The nightly record starts from the close after the news; the "
                    "hourly one from the first hourly reading of the session, for the rest of the session and then "
                    "from its close. Observe only: no order comes from this section."]))
    return out


def news_now_section_rows(data: Dict[str, Any], pad: Any) -> List[List[Any]]:
    """v1.5.0: section 11."""
    head = ["Name", "Listed as", "Event day", "State", "Price answer vs SPY", "Trend", "Research expects",
            "TFB's view", "Agreement", "State's record (5 sessions, forward)", "Main headline"]

    def line(r: Dict[str, Any]) -> List[Any]:
        name = r["symbol"] + (" - " + r["name"] if r.get("name") else "")
        return pad([name, r["why"], r["event_day"], r["state"], r["answer"], r["trend"], r["expected"], r["tfb"],
                    r["agreement"], r["record"], r["headline"]])

    out = [pad([]),
           pad(["11. NEWS AND TREND NOW - each holding and Top 10 name TFB follows: its latest company news, how the "
                "price answered against SPY, the trend before it and the move research expects over the next "
                "sessions, beside TFB's own view. Observe only: no order comes from this section."]),
           pad(head)]
    rows = data.get("news_now")
    if rows is None:
        out.append(pad(["", "Section 11 is %s." % NOT_SET_UP.replace("1.20.0", "1.25.0")]))
    elif not rows:
        out.append(pad(["", "No holding or Top 10 name to read."]))
    for r in rows or []:
        out.append(line(r))
    out += [pad([]), pad(["Latest events across every name TFB follows (the %d most recent, largest first)"
                          % NEWS_NOW_EVENTS]), pad(head)]
    events = data.get("news_events")
    if events is None:
        out.append(pad(["", "Not read: %s." % NOT_SET_UP.replace("1.20.0", "1.25.0")]))
    elif not events:
        out.append(pad(["", "No news event in the last 5 sessions."]))
    for r in events or []:
        out.append(line(r))
    out.append(pad(["How to read: the state joins the company news (both feeds) to the price's answer against SPY over "
                    "two sessions and the trend before it. Research expects a move with news to continue, a big move "
                    "without news to reverse, and news the price has not answered to drift its way (Chan 2003; "
                    "Tetlock 2011). Each state's record counts only sessions from 24 Nov 2026; a state is evidence "
                    "once it reads 'works as expected' (first reading 8 Dec 2026). Until then the lean is context: "
                    "the decision stays with TFB's view and the Today Action Plan."]))
    return out


# ----------------------------------------------------------------------------- #
# 5. Trial and the verdict
# ----------------------------------------------------------------------------- #
# ----------------------------------------------------------------------------- #
# v1.1.0 [EVENTS]: the calendar of dated events (a repository file; no query)
# ----------------------------------------------------------------------------- #
def _strict_day(v: Any) -> Optional[_dt.date]:
    if not isinstance(v, str) or not _DAY_RE.match(v):
        return None
    try:
        return _dt.date.fromisoformat(v)
    except ValueError:
        return None


def clean_event(e: Any) -> Optional[Dict[str, Any]]:
    """One calendar entry, or None when it cannot be trusted: a date that is not a date, an end before its
    start, no title, an unknown kind or status, or a source that is not an https page."""
    if not isinstance(e, dict):
        return None
    start = _strict_day(e.get("start"))
    end = _strict_day(e.get("end")) if e.get("end") not in (None, "") else start
    title, kind, status, source = text(e.get("title")), text(e.get("kind")), text(e.get("status")), text(e.get("source"))
    if start is None or end is None or end < start or not title or kind not in KIND_TEXT or status not in STATUS_TEXT \
            or not _URL_RE.match(source):
        return None
    return {"id": text(e.get("id")), "start": start, "end": end, "title": title, "kind": kind, "status": status,
            "time": text(e.get("time")), "why": text(e.get("why")), "markets": text(e.get("markets")), "source": source}


def load_calendar(path: Optional[str] = None) -> Dict[str, Any]:
    """The hand-kept calendar. Never raises: a file that is missing or broken is said on the page, and the
    page is built without it."""
    blank = {"checked": None, "events": [], "dropped": 0, "error": ""}
    try:
        with open(path or CALENDAR_FILE, encoding="utf-8") as f:
            raw = json.load(f)
    except (OSError, ValueError) as e:
        return dict(blank, error="file missing" if isinstance(e, OSError) else "not valid JSON")
    if not isinstance(raw, dict) or not isinstance(raw.get("events"), list):
        return dict(blank, error="no list of events in it")
    events = [clean_event(e) for e in raw["events"]]
    kept = sorted((e for e in events if e is not None), key=lambda e: (e["start"], e["end"], e["id"], e["title"]))
    return {"checked": _strict_day(raw.get("checked")), "events": kept, "dropped": len(events) - len(kept), "error": ""}


def day_text(d: _dt.date, year: bool = True) -> str:
    return "%s %d %s%s" % (_DOW[d.weekday()], d.day, _MON[d.month - 1], " %d" % d.year if year else "")


def span_text(start: _dt.date, end: _dt.date) -> str:
    if start == end:
        return day_text(start)
    return "%s - %s" % (day_text(start, year=start.year != end.year), day_text(end))


def when_text(ev: Dict[str, Any], today: _dt.date) -> str:
    if ev["status"] == "date_not_set":
        return "date not announced"
    if ev["start"] > today:
        n = (ev["start"] - today).days
        return "tomorrow" if n == 1 else "in %d days" % n
    if ev["start"] == ev["end"]:
        return "today"
    if ev["end"] == today:
        return "ends today"
    return "starts today" if ev["start"] == today else "under way"


def calendar_rows(cal: Dict[str, Any], today: _dt.date, days: int = CALENDAR_DAYS) -> List[Dict[str, Any]]:
    """The events that have not ended and begin within the window: the dated ones soonest first, then those
    whose date is not announced."""
    last = today + _dt.timedelta(days=days)
    out = []
    for ev in cal.get("events") or []:
        if ev["end"] < today or ev["start"] > last:
            continue
        window = ev["status"] == "date_not_set"
        out.append({"date": ("by %s" % day_text(ev["end"])) if window else span_text(ev["start"], ev["end"]),
                    "when": when_text(ev, today), "title": ev["title"], "kind": KIND_TEXT[ev["kind"]], "time": ev["time"],
                    "why": ev["why"], "markets": ev["markets"], "status": STATUS_TEXT[ev["status"]],
                    "source": ev["source"], "start": ev["start"], "dated": not window})
    return sorted(out, key=lambda r: not r["dated"])                # stable: each group keeps its date order


def calendar_notes(cal: Dict[str, Any], today: _dt.date) -> List[str]:
    """What the reader must know about the calendar itself: unreadable, entries left out, old, or running out."""
    if cal.get("error"):
        return ["The events calendar could not be read (%s). No event is shown; the rest of the page is unaffected."
                % cal["error"]]
    notes = []
    if cal.get("dropped"):
        notes.append("%d calendar entr%s could not be read and %s left out." % (
            cal["dropped"], "y" if cal["dropped"] == 1 else "ies", "is" if cal["dropped"] == 1 else "are"))
    checked = cal.get("checked")
    if checked is None:
        notes.append("The calendar does not say when it was last checked; dates may have moved.")
    elif (today - checked).days > CALENDAR_STALE_DAYS:
        notes.append("The calendar was last checked on %s (%d days ago); dates may have moved since."
                     % (day_text(checked), (today - checked).days))
    events = cal.get("events") or []
    last = max((e["start"] for e in events if e["status"] != "date_not_set"), default=None)
    if last is None or last < today + _dt.timedelta(days=CALENDAR_SOON):
        notes.append("The calendar holds no dated event after %s; it needs new dates."
                     % (day_text(last) if last else "today"))
    return notes


def next_event_line(rows: Sequence[Dict[str, Any]], today: _dt.date) -> str:
    """The line under the verdict: the next dated event when it starts within two weeks."""
    for r in rows:
        if r["dated"] and (r["start"] - today).days <= CALENDAR_SOON:
            return "NEXT EVENT (%s): %s, %s. Section 6 lists the next %d days." % (
                r["when"], r["title"], r["date"], CALENDAR_DAYS)
    return ""


def trial_rows(rows: Sequence[Dict[str, Any]]) -> List[Dict[str, Any]]:
    def key(r: Dict[str, Any]) -> Tuple[int, int, str, int]:
        return (0 if text(r.get("period")) == FORWARD else 1,
                ARMS.index(text(r.get("arm"))) if text(r.get("arm")) in ARMS else 9, text(r.get("horizon")),
                LINE_ORDER.index(text(r.get("line"))) if text(r.get("line")) in LINE_ORDER else 9)
    return sorted((dict(r) for r in rows if text(r.get("line"))), key=key)


def _difference(trial: Sequence[Dict[str, Any]], forward: bool, arm: str = "events", horizon: str = "1W") -> Optional[Dict[str, Any]]:
    for r in trial:
        if text(r.get("line")) == "OVERLAY_MINUS_CHAMPION" and text(r.get("arm")) == arm \
                and text(r.get("horizon")) == horizon and (text(r.get("period")) == FORWARD) == forward:
            return r
    return None


def verdict_lines(trial: Sequence[Dict[str, Any]], tilt: Dict[str, Dict[str, Any]], last_news: Optional[_dt.date],
                  today: _dt.date) -> List[str]:
    out: List[str] = []
    fwd, past = _difference(trial, True), _difference(trial, False)
    if fwd and (num(fwd.get("days")) or 0) > 0:
        out.append("Forward trial (political events, 1 week): %d day(s) counted, overlay minus list %s pp, t %s - %s."
                   % (int(num(fwd.get("days")) or 0), fmt(fwd.get("edge_pp"), 2, True) or "n/a",
                      fmt(fwd.get("t_adj"), 2) or "n/a", text(fwd.get("reading")) or "no reading"))
    else:
        out.append("The forward trial has no counted day yet: it starts with the list of 20 Nov 2026, and a day counts "
                   "once its week has passed and every moved name has its outcome.")
    if past and (num(past.get("days")) or 0) > 0:
        t = num(past.get("t_adj"))
        out.append("Past days (not evidence): overlay minus list %s pp over %d day(s), t %s - %s."
                   % (fmt(past.get("edge_pp"), 2, True) or "n/a", int(num(past.get("days")) or 0), fmt(t, 2) or "n/a",
                      "no better than chance" if t is None or abs(t) < 2.0 else "a difference the forward trial has to confirm"))
    out.append("Nothing on this page changes a recommendation. The list stays as it is until the trial says otherwise "
               "and the owner decides.")
    if last_news is None:
        out.append("WARNING: the news archive could not be dated; treat the tilt below as unconfirmed.")
    elif (today - last_news).days > STALE_DAYS:
        out.append("WARNING: the news archive stops at %s (%d days ago); the tilt below may be out of date."
                   % (last_news.isoformat(), (today - last_news).days))
    if not any((tilt.get(a) or {}).get("in_force") for a in ARMS):
        out.append("WARNING: no sector tilt is in force for today; sections 2 to 4 show the last one, for reference.")
    return out


# ----------------------------------------------------------------------------- #
# The page
# ----------------------------------------------------------------------------- #
def page_rows(data: Dict[str, Any], stamp: str) -> List[List[Any]]:
    def pad(r: Sequence[Any]) -> List[Any]:
        return (list(r) + [""] * WIDTH)[:WIDTH]

    tilt = data["tilt"]
    lead = next((a for a in ARMS if a in tilt), None)
    out = [pad(["NEWS BOARD - what the world news says, which sectors it favours, and whether following it would have helped"]),
           pad(["Built %s Riyadh | news_board %s | SHADOW: analysis only. Nothing on this page changes a recommendation "
                "or places an order; only the Today Action Plan gives orders." % (stamp, __version__)]),
           pad([]), pad(["VERDICT"])]
    out += [pad([line]) for line in data["verdict"]]
    out += [pad([line]) for line in data.get("news_verdict") or []]
    out += [pad([line]) for line in data.get("news_now_lines") or []]
    if data.get("next_event"):
        out.append(pad([data["next_event"]]))

    weeks = sorted({t["week_start"] for t in data["themes"] if t["week_start"]})
    ends = sorted({t["week_end"] for t in data["themes"] if t["week_end"]})
    out += [pad([]),
            pad(["1. WORLD NEWS - last full week%s against its own previous four weeks; news archive to %s"
                 % (" (%s to %s)" % (weeks[-1], ends[-1]) if weeks and ends else "", data.get("last_news") or "unknown")]),
            pad(["Kind of news", "Theme", "Last full week", "Normal (4 weeks before)", "Change", "Reading",
                 "This week so far", "Days so far", "Reading so far"])]
    for t in data["themes"]:
        out.append(pad([t["news"], t["label"], fmt(t["value"]), fmt(t["prev"]), t["change"], t["reading"],
                        fmt(t["now_value"]), fmt(t["now_days"], 0), t["now_reading"]]))
    if not data["themes"]:
        out.append(pad(["", "No theme week could be read."]))

    head = "2. SECTOR TILT - no tilt has begun to apply yet"
    if lead:
        t0 = tilt[lead]
        head = "2. SECTOR TILT - %s %s to %s, from the news of the week of %s" % (
            "in force" if t0["in_force"] else "NOT in force today; it applied",
            t0["apply_from"].isoformat(), t0["apply_to"].isoformat() if t0["apply_to"] else "?", t0["week"])
    out += [pad([]), pad([head]),
            pad(["Sector", "US sector fund", ARM_TEXT["events"] + ": tilt", "Score", ARM_TEXT["all"] + ": tilt", "Score",
                 "Themes with a working weight", "Week before: tilt", "Sector vs S&P 500 that week (pp)"])]
    for s in data["sectors"]:
        out.append(pad([s["sector"], s["etf"], GROUP_TEXT.get(s.get("events", ""), ""), fmt(s.get("events_score"), 3, True),
                        GROUP_TEXT.get(s.get("all", ""), ""), fmt(s.get("all_score"), 3, True), s["themes"],
                        GROUP_TEXT.get(s["prev"], ""), fmt(s["prev_realized"], 2, True)]))
    out += [pad([]),
            pad(["Record of the tilt - favoured sectors minus avoided sectors, per week, percentage points against the S&P 500"]),
            pad(["Arm", "Period", "Weeks", "Favoured", "Avoided", "Difference", "t", "Favoured ahead in", "Reading"])]
    for r in data["record"]:
        out.append(pad([ARM_TEXT[r["arm"]], "forward (news weeks from %s)" % FORWARD_WEEK.isoformat() if r["period"] == "forward"
                        else "before the rule was fixed", fmt(r["weeks"], 0), fmt(r["fav"], 2, True), fmt(r["avo"], 2, True),
                        fmt(r["spread"], 2, True), fmt(r["t"], 2), (fmt(r["right"], 1) + "%") if r["right"] is not None else "",
                        r["reading"]]))
    if not data["record"]:
        out.append(pad(["", "No scored week yet."]))

    out += [pad([]), pad(["3. YOUR HOLDINGS AND THE TILT (%d holding(s))" % len(data["holdings"])]),
            pad(["Symbol", "Name", "Sector", ARM_TEXT["events"] + ": tilt", ARM_TEXT["all"] + ": tilt",
                 "On today's investable list (market pages)", "Shadow list (political events)", "Note"])]
    for h in data["holdings"]:
        out.append(pad([h["symbol"], h["name"], h["sector"], GROUP_TEXT.get(h.get("events", ""), ""),
                        GROUP_TEXT.get(h.get("all", ""), ""), h["listed"], h["shadow"], h["note"]]))
    if not data["holdings"]:
        out.append(pad(["", "No holding could be read from %s." % PORTFOLIO_TAB]))

    c = data["counts"]
    out += [pad([]),
            pad(["4. SHADOW LIST for %s - names the news tilt would move. It is NOT an order."
                 % (data.get("shadow_day") or "no list day")]),
            pad(["; ".join("%s: list of %d, %d would leave, %d would enter" % (ARM_TEXT[a], c[a]["list"], c[a]["out"], c[a]["in"])
                           for a in ARMS)]),
            pad(["Symbol", "Name", "Sector", "Engine score", "Volatility", ARM_TEXT["events"], ARM_TEXT["all"]])]
    if data.get("shadow_note"):
        out.insert(len(out) - 1, pad([data["shadow_note"]]))
    for d in data["shadow"]:
        out.append(pad([d["symbol"], d["name"], d["sector"], fmt(d["score"], 1), d["vol"]]
                       + [ACTION_TEXT.get(d["actions"].get(a, ""), "not on the list") for a in ARMS]))
    if not data["shadow"]:
        out.append(pad(["", "No name would move on this list day."]))

    out += [pad([]),
            pad(["5. TRIAL - the list with the news overlay against the list as it is (return against the market median, "
                 "percentage points)"]),
            pad(["Period", "Arm", "Horizon", "Line", "Names", "Days counted", "Days left out", "News weeks",
                 "Against the market (pp)", "Beat the market", "t", "Reading"])]
    for r in data["trial"]:
        out.append(pad([text(r.get("period")), ARM_TEXT.get(text(r.get("arm")), text(r.get("arm"))), text(r.get("horizon")),
                        LINE_TEXT.get(text(r.get("line")), text(r.get("line"))), fmt(r.get("names"), 0), fmt(r.get("days"), 0),
                        fmt(r.get("incomplete_days"), 0), fmt(r.get("news_weeks"), 0), fmt(r.get("edge_pp"), 2, True),
                        (fmt(r.get("right_pct"), 1) + "%") if num(r.get("right_pct")) is not None else "",
                        fmt(r.get("t_adj"), 2), text(r.get("reading"))]))
    if not data["trial"]:
        out.append(pad(["", "The trial has no row yet."]))
    out += [pad(["Promotion rule, fixed on 19 Nov 2026: the overlay is offered to the owner only after 20 changed days "
                 "(1 week) or 28 (2 weeks), 8 news weeks and a t of 2.5 or more. The owner decides."]),
            pad([]),
            pad(["6. EVENTS AHEAD - dated events of the next %d days%s. For information: no order comes from this section."
                 % (CALENDAR_DAYS, (", calendar checked %s" % data["events_checked"]) if data.get("events_checked") else "")]),
            pad(["Date", "When", "Event", "Kind", "Time (Riyadh)", "Why it matters", "Markets", "Status", "Source"])]
    for e in data.get("events") or []:
        out.append(pad([e["date"], e["when"], e["title"], e["kind"], e["time"], e["why"], e["markets"], e["status"],
                        e["source"]]))
    if not data.get("events"):
        out.append(pad(["", "No listed event in the next %d days." % CALENDAR_DAYS]))
    out += [pad([note]) for note in data.get("events_notes") or []]
    out += [pad(["The news gate already raises CAUTION by itself within 30 hours of a Fed decision or a high-impact US "
                 "data release; this list is the longer view. Dates are read from the pages in the Source column and "
                 "can move."]),
            pad([]),
            pad(["7. COMPANY NEWS, RE-READ - your holdings' headlines of the last %d days, classified from their own text. "
                 "For information: no order comes from this section." % COMPANY_DAYS]),
            pad(["Symbol", "Name", "News (%d days)" % COMPANY_DAYS, "Score", "Headlines about the company",
                 "Filed under it, about others", "Main headline", "Its event", "Day", "Provider's reading (old)", "Note"])]
    for c in data.get("company") or []:
        out.append(pad([c["symbol"], c["name"], NEWS_CLASS_TEXT[c["class"]], fmt(c["score"], 1, True), str(c["company"]),
                        str(c["other"]), c["title"], c["event"], c["day"], c["provider"], c["note"]]))
    if not data.get("company"):
        out.append(pad(["", "No holding could be read from %s." % PORTFOLIO_TAB]))
    out += [pad([data.get("company_record") or "Record of the new classes: no scored day yet."]),
            pad(["A headline counts for a holding only when it names the company (its name, an alias or its ticker in the "
                 "market's own form); the score adds 2 for a hard event (results against estimates, guidance, a rating "
                 "change, a dividend cut or raise, an offering, a legal matter), 1 for a softer one, 0.5 for an opinion "
                 "and 0 for a price move that has already happened. POSITIVE at +1 or more, NEGATIVE at -1 or less."]),
            pad([])]
    out += news_section_rows(data, pad)
    out += news_now_section_rows(data, pad)         # v1.5.0 [NEWS NOW]
    out += interim_section_rows(data, pad)          # v1.6.0 [INTERIM]
    out += [pad([]),
            pad(["How to read: a theme is the share of the week's news on that subject, per 1,000 (the Iran, Russia and "
                 "US-China lines count events; the rest count news documents); a tone is an index where higher is more "
                 "positive. 'Normal' is the theme's own previous four weeks. A sector's weight for a theme is re-fitted "
                 "every week on past weeks only and is switched off when its own past forecasts were wrong. A sector's "
                 "score is its expected move against the S&P 500 in the following week from the working weights, each "
                 "scaled by its own past accuracy; the three highest positive scores are favoured, the three lowest "
                 "negative ones avoided. The tilt is measured on US sector funds; a name of another market is moved by "
                 "its sector alone. Sources: BigQuery tfb_archive (GDELT public tables, the price and signal archive). "
                 "Not investment advice."])]
    return out


def news_section_rows(data: Dict[str, Any], pad: Any) -> List[List[Any]]:
    """v1.4.0: sections 8 to 10."""
    def z(v: Optional[float]) -> str:
        return fmt(v, 2, True) if v is not None else ""

    out = [pad(["8. NEWS BY MARKET - news day %s (UTC), each market's own place against its previous 250 news days. "
                "For information: no order comes from this section." % (data.get("market_day") or "none")]),
           pad(["Market", "Its news", "Reading", "5-day score", "Momentum", "Surprise", "Documents", "Attention (z)",
                "Tone (z)", "Fighting (z)"])]
    markets = data.get("markets")
    if markets is None:
        out.append(pad(["", "Section 8 is %s." % NOT_SET_UP]))
    elif not markets:
        out.append(pad(["", "No news day in the last %d days." % MARKET_NEWS_DAYS]))
    for m in markets or []:
        out.append(pad([m["market"], m["place"], READING_TEXT.get(m["reading"], m["reading"] or "not read yet"),
                        z(m["score"]), MOMENTUM_TEXT.get(m["momentum"], m["momentum"]),
                        SURPRISE_TEXT.get(m["surprise"], m["surprise"]), fmt(m["docs"], 0), z(m["att_z"]),
                        z(m["tone_z"]), z(m["fight_z"])]))
    out += [pad(["A place's day is read against its own previous 250 news days, in standard deviations (z): the "
                 "share of the day's news naming it (attention), the tone of that news, and the share of the day's "
                 "events that are fighting located there. The score is (tone - fighting) / 2, averaged over 5 days and "
                 "signed for the market: gold, oil and bonds read the reverse (a worse war zone or a worse US day has "
                 "lifted them). FAVOURABLE at +0.5 or more, UNFAVOURABLE at -0.5 or less; momentum is the score "
                 "against its level 5 news days before; unusual attention is a z of 2 or more."]),
            pad([]),
            pad(["9. PEOPLE IN THE NEWS - %s (UTC), against each person's previous 250 news days; spikes of the last %d "
                 "days. For information: no order comes from this section." % (data.get("people_day") or "no day read",
                                                                             PEOPLE_DAYS)]),
            pad(["Person", "Role", "Documents", "Share of the news (%)", "Normal share (%)", "Attention (z)",
                 "Tone (z)", "Reading", "Spike days (%d)" % PEOPLE_DAYS, "Markets it may move"])]
    people = data.get("people")
    if people is None:
        out.append(pad(["", "Section 9 is %s." % NOT_SET_UP]))
    elif not people:
        out.append(pad(["", "No person's day read in the last %d days." % PEOPLE_DAYS]))
    for p in people or []:
        out.append(pad([p["person"], p["role"], fmt(p.get("docs"), 0), fmt(p.get("share"), 3),
                        fmt(p.get("normal"), 3), z(p.get("att_z")), z(p.get("tone_z")),
                        SPIKE_TEXT.get(p.get("reading", ""), p.get("reading") or ""), str(p["spikes"]),
                        p["markets"].replace(",", ", ")]))
    out += [pad(["A spike is an attention z of 2 or more on 20 or more documents; its tone is the tone of that news "
                 "against the person's normal. GDELT's list of people almost never names Emmanuel Macron, so he is "
                 "not read here."]),
            pad([]),
            pad(["10. NEWS SIGNALS ON TRIAL - what the markets' funds did over the %d closes after the news day, "
                 "against the same signal's own mean (forward from %s; the backtest is not evidence). No order comes "
                 "from this section." % (NEWS_TRIAL_HORIZON, NEWS_FORWARD_FROM.isoformat())]),
            pad(["Period", "Signal", "Reading", "Market-sessions", "Sessions", "Mean change (%)", "Signal's mean (%)",
                 "Edge (pp)", "t", "Rose", "Verdict"])]
    trial = data.get("news_trial")
    if trial is None:
        out.append(pad(["", "Section 10 is %s." % NOT_SET_UP]))
    elif not trial:
        out.append(pad(["", "No scored line yet."]))
    names = data.get("people_names") or []
    for r in trial or []:
        out.append(pad([text(r.get("period")), signal_name(text(r.get("signal")), names),
                        SIGNAL_READING_TEXT.get(text(r.get("reading")), text(r.get("reading"))), fmt(r.get("n"), 0),
                        fmt(r.get("days"), 0), fmt(r.get("mean_pct"), 2, True), fmt(r.get("base_pct"), 2, True),
                        fmt(r.get("edge_pp"), 2, True), fmt(r.get("t_adj"), 2),
                        (fmt(r.get("up_pct"), 1) + "%") if num(r.get("up_pct")) is not None else "",
                        text(r.get("verdict"))]))
    if data.get("news_ic"):
        out.append(pad([data["news_ic"]]))
    out.append(pad(["Rules fixed on 21 Nov 2026, before any forward session. A reading is read once it has %d sessions "
                    "of its own; the owner reviews the forward sessions and switches on only what proved itself. The "
                    "windows of consecutive days overlap: t counts one window per market per %d closes."
                    % (NEWS_REVIEW_SESSIONS, NEWS_TRIAL_HORIZON)]))
    return out


# ----------------------------------------------------------------------------- #
# SQL (SELECT only; every date is a date object, every symbol passes _SYM_RE)
# ----------------------------------------------------------------------------- #
def _d(d: _dt.date) -> str:
    if not isinstance(d, _dt.date) or isinstance(d, _dt.datetime):
        raise ValueError("a date is needed")
    return "DATE '%s'" % d.isoformat()


def sql_last_news(ds: str, today: _dt.date) -> str:
    return ("SELECT CAST(MAX(date) AS STRING) AS last_day, COUNT(DISTINCT feature) AS features "
            "FROM `%s.news_theme_daily` WHERE date >= DATE_SUB(%s, INTERVAL 40 DAY)" % (ds, _d(today)))


def sql_themes(ds: str, today: _dt.date) -> str:
    return ("WITH w AS (SELECT week_start, week_end, feature, kind, days, is_closed, usable, value, prev_4w_avg, surprise "
            "FROM `%s.v_news_theme_weekly` WHERE week_start >= DATE_SUB(%s, INTERVAL 420 DAY)), "
            "s AS (SELECT feature, STDDEV_SAMP(IF(usable, surprise, NULL)) AS sd, COUNTIF(usable) AS n FROM w GROUP BY feature) "
            "SELECT CAST(w.week_start AS STRING) AS week_start, CAST(w.week_end AS STRING) AS week_end, w.feature, w.kind, "
            "w.days, w.is_closed, w.usable, w.value, w.prev_4w_avg, w.surprise, s.sd, s.n "
            "FROM w JOIN s USING (feature) WHERE w.week_start >= DATE_SUB(%s, INTERVAL 21 DAY) "
            "ORDER BY w.feature, w.week_start" % (ds, _d(today), _d(today)))


def sql_tilt(ds: str, today: _dt.date) -> str:
    return ("SELECT arm, CAST(week_start AS STRING) AS week_start, CAST(apply_from AS STRING) AS apply_from, "
            "CAST(apply_to AS STRING) AS apply_to, etf, sector, score, pos, active_list, tilt_group, realized_excess_pp "
            "FROM `%s.v_news_sector_tilt` WHERE week_start >= DATE_SUB(%s, INTERVAL 42 DAY) "
            "ORDER BY arm, week_start, pos" % (ds, _d(today)))


def sql_record(ds: str) -> str:
    return ("WITH wk AS (SELECT arm, IF(week_start >= %s, 'forward', 'before') AS period, week_start, "
            "AVG(IF(tilt_group = 'favoured', realized_excess_pp, NULL)) AS f, "
            "AVG(IF(tilt_group = 'avoided', realized_excess_pp, NULL)) AS a "
            "FROM `%s.v_news_sector_tilt` WHERE realized_excess_pp IS NOT NULL GROUP BY arm, period, week_start), "
            "b AS (SELECT * FROM wk WHERE f IS NOT NULL AND a IS NOT NULL) "
            "SELECT arm, period, COUNT(*) AS weeks, ROUND(AVG(f), 2) AS fav, ROUND(AVG(a), 2) AS avo, "
            "ROUND(AVG(f - a), 2) AS spread, ROUND(SAFE_DIVIDE(AVG(f - a), STDDEV_SAMP(f - a)) * SQRT(COUNT(*)), 2) AS t, "
            "ROUND(100 * COUNTIF(f > a) / COUNT(*), 1) AS right_pct FROM b GROUP BY arm, period" % (_d(FORWARD_WEEK), ds))


def sql_sectors(ds: str, symbols: Sequence[Any]) -> str:
    syms = clean_symbols(symbols)
    if not syms:
        raise ValueError("no symbol to look up")
    return ("SELECT symbol, sector FROM `%s.v_symbol_sector` WHERE symbol IN (%s)"
            % (ds, ", ".join("'%s'" % s for s in syms)))


def sql_shadow(ds: str, today: _dt.date) -> str:
    return ("WITH o AS (SELECT arm, base_date, symbol, name, sector, overall_score, vol_band, in_champion, news_action "
            "FROM `%s.v_news_overlay_list` WHERE base_date >= DATE_SUB(%s, INTERVAL 6 DAY) AND base_date <= %s), "
            "m AS (SELECT MAX(base_date) AS d FROM o) "
            "SELECT o.arm, CAST(o.base_date AS STRING) AS base_date, o.symbol, o.name, o.sector, o.overall_score, "
            "o.vol_band, o.in_champion, o.news_action FROM o JOIN m ON o.base_date = m.d "
            "ORDER BY o.arm, o.symbol" % (ds, _d(today), _d(today)))


def sql_trial(ds: str) -> str:
    return ("SELECT period, arm, horizon, line, names, days, incomplete_days, news_weeks, edge_pp, right_pct, t_adj, "
            "reading FROM `%s.v_trial_news_overlay`" % ds)


def sql_company_news(ds: str, symbols: Sequence[Any], today: _dt.date) -> str:
    """v1.2.0: per holding, the last COMPANY_DAYS days of re-read headlines; the main headline is the one with the
    largest absolute score, the newest first, then by title (one order for the four picks, so they agree)."""
    syms = clean_symbols(symbols)
    if not syms:
        raise ValueError("no symbol to look up")
    pick = "ORDER BY ABS(score) DESC, published_at DESC, title LIMIT 1)[SAFE_OFFSET(0)]"
    return ("SELECT symbol, COUNTIF(relevance = 'company') AS company_items, COUNTIF(relevance != 'company') AS other_items, "
            "ROUND(SUM(score), 1) AS score, ROUND(AVG(provider_polarity), 3) AS provider_mean, "
            "ARRAY_AGG(IF(relevance = 'company', title, NULL) IGNORE NULLS %s AS top_title, "
            "ARRAY_AGG(IF(relevance = 'company', event_type, NULL) IGNORE NULLS %s AS top_event, "
            "ARRAY_AGG(IF(relevance = 'company', CAST(news_date AS STRING), NULL) IGNORE NULLS %s AS top_day "
            "FROM `%s.v_symbol_news_class` WHERE symbol IN (%s) AND news_date >= DATE_SUB(%s, INTERVAL %d DAY) "
            "AND news_date <= %s GROUP BY symbol"
            % (pick, pick, pick, ds, ", ".join("'%s'" % s for s in syms), _d(today), COMPANY_DAYS - 1, _d(today)))


def sql_company_eval(ds: str, peers: bool = True) -> str:
    """v1.3.0: with the peer columns of apply_bq_views 1.17.0; peers=False is the 1.2.0 read, for a view not yet
    updated in BigQuery."""
    extra = ", mean_5d_peer_pp, matured_5d_peer, beat_5d_peer_pct, corr_5d_peer, peers" if peers else ""
    return ("SELECT signal, class, matured_5d, mean_5d_excess_pp, beat_5d_pct, corr_5d, reading%s "
            "FROM `%s.v_company_news_eval` ORDER BY signal, class" % (extra, ds))


def sql_market_news(ds: str, today: _dt.date) -> str:
    """v1.4.0: the news days before today, the last MARKET_NEWS_DAYS of them."""
    return ("SELECT CAST(date AS STRING) AS date, market_id, market, market_order, place_name, docs, att_z, tone_z, "
            "fight_z, market_score, reading, momentum_label, surprise FROM `%s.v_market_news` "
            "WHERE date >= DATE_SUB(%s, INTERVAL %d DAY) AND date < %s ORDER BY date DESC, market_order"
            % (ds, _d(today), MARKET_NEWS_DAYS, _d(today)))


def sql_people_news(ds: str, today: _dt.date) -> str:
    return ("SELECT CAST(date AS STRING) AS date, person_id, person, role, person_order, markets, docs, share_pct, "
            "normal_share_pct, att_z, tone_z, reading FROM `%s.v_people_news` "
            "WHERE date >= DATE_SUB(%s, INTERVAL %d DAY) AND date < %s ORDER BY person_order, date DESC"
            % (ds, _d(today), PEOPLE_DAYS, _d(today)))


def sql_news_trial(ds: str) -> str:
    return ("SELECT period, horizon, signal, reading, expected, n, days, markets, mean_pct, base_pct, edge_pp, t_adj, "
            "up_pct, ic, ic_days, ic_t_adj, verdict FROM `%s.v_news_trial_summary` "
            "ORDER BY period DESC, signal, horizon, expected DESC, reading" % ds)


_NOW_COLS = ("symbol, name, CAST(latest_event_date AS STRING) AS event_day, state, expected, reaction_pp, reaction_z, "
             "day_pp, day_z, trend_before, trend_now, reaction_complete, top_headline, recommendation, agreement")


def sql_board_names(ds: str, today: _dt.date) -> str:
    """v1.5.0: the Top 10 names of the last NEWS_NOW_BOARD_DAYS days, best seat first."""
    return ("SELECT symbol, MIN(seat_rank) AS best FROM `%s.v_top10_board` WHERE snapshot_date >= %s "
            "AND symbol IS NOT NULL GROUP BY symbol ORDER BY best, symbol LIMIT 20"
            % (ds, _d(today - _dt.timedelta(days=NEWS_NOW_BOARD_DAYS))))


def sql_news_now(ds: str, symbols: Sequence[Any], src: str = "v_news_understanding") -> str:
    syms = clean_symbols(symbols)
    if not syms:
        raise ValueError("no symbol")
    return "SELECT %s FROM `%s.%s` WHERE symbol IN (%s)" % (
        _NOW_COLS, ds, src, ", ".join("'%s'" % s for s in syms))


def sql_news_events(ds: str, src: str = "v_news_understanding") -> str:
    return ("SELECT %s FROM `%s.%s` WHERE latest_event_date IS NOT NULL "
            "AND state NOT IN ('routine news', 'quiet') "
            "ORDER BY latest_event_date DESC, ABS(IFNULL(reaction_z, day_z)) DESC, symbol LIMIT %d"
            % (_NOW_COLS, ds, src, NEWS_NOW_EVENTS))


def sql_news_state_record(ds: str, src: str = "v_news_trend_learning") -> str:
    return ("SELECT period, state, trend_fit, horizon_sessions, events, days, edge_pp, right_pct, t_adj, reading "
            "FROM `%s.%s` WHERE trend_fit = 'all' AND horizon_sessions IN (%s)"
            % (ds, src, ", ".join(str(h) for h in sorted(set(INTERIM_NIGHT_HORIZONS) | {NEWS_NOW_HORIZON}))))


def sql_news_now_learning(ds: str) -> str:
    """v1.6.0: the hourly reading's record (apply_bq_views 1.28.0)."""
    return ("SELECT state, horizon_sessions, events, symbols, days, edge_pp, right_pct, t_adj, reading "
            "FROM `%s.v_news_now_learning`" % ds)


def read_company_eval(session: Any, project: str, location: str, ds: str, query: Any) -> List[Dict[str, Any]]:
    """v1.3.0: the record with the peers; if BigQuery does not know the peer columns yet (the view before
    apply_bq_views 1.17.0), the 1.2.0 read instead - any other refusal still fails the run."""
    try:
        return query(session, project, location, sql_company_eval(ds))
    except RuntimeError as e:
        if "Unrecognized name" not in text(e):
            raise
        _out("COMPANY NEWS record: the view has no peer columns yet (apply_bq_views 1.17.0 not applied); "
             "read against the S&P 500 only")
        return query(session, project, location, sql_company_eval(ds, peers=False))


# ----------------------------------------------------------------------------- #
def collect(session: Any, project: str, dataset: str, location: str, holdings: Sequence[Dict[str, str]],
            today: _dt.date, query: Any, calendar: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    ds = "%s.%s" % (project, dataset)
    cal = calendar if calendar is not None else load_calendar()
    events = calendar_rows(cal, today)
    last = query(session, project, location, sql_last_news(ds, today))
    last_news = day(last[0].get("last_day")) if last else None
    themes = theme_rows(query(session, project, location, sql_themes(ds, today)))
    tilt = pick_tilt(query(session, project, location, sql_tilt(ds, today)), today)
    record = record_rows(query(session, project, location, sql_record(ds)))
    syms = clean_symbols(a for h in holdings for a in aliases(h["symbol"]))
    sectors = {text(r.get("symbol")).upper(): text(r.get("sector"))
               for r in (query(session, project, location, sql_sectors(ds, syms)) if syms else [])}
    shadow_day, names = shadow_names(query(session, project, location, sql_shadow(ds, today)))
    trial = trial_rows(query(session, project, location, sql_trial(ds)))
    found = query(session, project, location, sql_company_news(ds, syms, today)) if syms else []
    company_eval = read_company_eval(session, project, location, ds, query)
    mnews = read_optional(session, project, location, sql_market_news(ds, today), query)
    pnews = read_optional(session, project, location, sql_people_news(ds, today), query)
    ntrial = read_optional(session, project, location, sql_news_trial(ds), query)
    # v1.5.0 [NEWS NOW]: the holdings and the Top 10 names, read with the trend
    board_rows = read_optional(session, project, location, sql_board_names(ds, today), query) or []
    board = clean_symbols(text(r.get("symbol")).upper() for r in board_rows)
    now_syms = clean_symbols([a for h in holdings for a in aliases(h["symbol"])] + [a for b in board for a in aliases(b)])
    # v1.7.0 [SNAPSHOTS]: the snapshot when it is fresh, else the view
    from scripts.bq_snapshots import source_for  # noqa: PLC0415
    src_u = source_for("v_news_understanding", session, project, location, ds, query, note=_out)
    src_l = source_for("v_news_trend_learning", session, project, location, ds, query, note=_out)
    found_now = read_optional(session, project, location, sql_news_now(ds, now_syms, src_u), query) if now_syms else []
    ev_rows = read_optional(session, project, location, sql_news_events(ds, src_u), query)
    rec_rows = read_optional(session, project, location, sql_news_state_record(ds, src_l), query)
    records = state_records(rec_rows)
    interim = interim_rows(rec_rows, read_optional(session, project, location, sql_news_now_learning(ds), query))
    news_now = news_now_rows(holdings, board, found_now, records)
    news_events = None if ev_rows is None else [news_now_row(r, "event", records) for r in ev_rows]
    market_day, markets = market_news_rows(mnews)
    people_day, people = people_rows(pnews)
    return {"news_now": news_now, "news_events": news_events,
            "news_now_lines": news_now_lines(news_now, news_events, records) + [interim_line(interim)],
            "interim": interim,
            "market_day": market_day, "markets": None if mnews is None else markets,
            "people_day": people_day, "people": None if pnews is None else people,
            "news_trial": None if ntrial is None else news_trial_rows(ntrial),
            "news_ic": ic_line(ntrial) if ntrial is not None else "",
            "news_verdict": news_verdict_lines(market_day, None if mnews is None else markets, people_day,
                                               None if pnews is None else people, ntrial),
            "people_names": [{"person_id": text(r.get("person_id")), "person": text(r.get("person"))}
                             for r in pnews or []],
            "last_news": last_news.isoformat() if last_news else "", "themes": themes, "tilt": tilt,
            "sectors": sector_rows(tilt), "record": record,
            "holdings": holding_rows(holdings, sectors, tilt, names, shadow_day), "shadow_day": shadow_day,
            "shadow_note": shadow_note(shadow_day, tilt),
            "counts": shadow_counts(names), "shadow": shadow_rows(names), "trial": trial,
            "verdict": verdict_lines(trial, tilt, last_news, today),
            "events": events, "events_notes": calendar_notes(cal, today), "next_event": next_event_line(events, today),
            "events_checked": day_text(cal["checked"]) if cal.get("checked") else "",
            "company": company_rows(holdings, found), "company_record": company_record_line(company_eval)}


def run_once(dry_run: bool, book=None, session=None, now: Optional[_dt.datetime] = None, query=None,
             calendar: Optional[Dict[str, Any]] = None) -> int:
    now = now or _dt.datetime.now(UTC)
    if not enabled():
        _out("TFB_NEWS_BOARD is off: nothing read, nothing written")
        return 0
    project, dataset = _env("TFB_BQ_PROJECT"), _env("TFB_BQ_DATASET", "tfb_archive")
    if not _ID_RE.match(project) or not _ID_RE.match(dataset):
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

        holdings = read_holdings(_quota_call(lambda: _raw_read(book, PORTFOLIO_TAB, "A1:B200"), _SLEEP, _out,
                                             "reading %s" % PORTFOLIO_TAB))
        today = now.astimezone(RIYADH).date()
        data = collect(session, project, dataset, _env("TFB_BQ_LOCATION", "US"), holdings, today, query, calendar)
    except Exception as e:  # noqa: BLE001 - one page; a failed read leaves the previous page in place
        _out("ERROR: %s: %s; nothing written" % (type(e).__name__, text(e)[:300]))
        return 2
    for line in data["verdict"]:
        _out("VERDICT %s" % line)
    _out("EVENTS %d in the next %d days" % (len(data["events"]), CALENDAR_DAYS))
    if data["next_event"]:
        _out(data["next_event"])
    for note in data["events_notes"]:
        _out("EVENTS note: %s" % note)
    for line in data.get("news_verdict") or []:
        _out("NEWS %s" % line)
    for line in data.get("news_now_lines") or []:        # v1.5.0: counts only, never a holding's name
        _out(line)
    tally = {k: sum(1 for c in data["company"] if c["class"] == k) for k in NEWS_CLASS_TEXT}
    _out("COMPANY NEWS %d holding(s): %d positive, %d negative, %d neutral, %d without company news; %s" % (
        len(data["company"]), tally["positive"], tally["negative"], tally["neutral"], tally["none"], data["company_record"]))
    for a in ARMS:
        t = data["tilt"].get(a)
        if t:
            groups = {g: sorted(s for s, c in t["sectors"].items() if c["group"] == g) for g in ("favoured", "avoided")}
            _out("TILT %-6s week %s, %s %s to %s | favoured: %s | avoided: %s" % (
                a, t["week"], "in force" if t["in_force"] else "NOT in force", t["apply_from"], t["apply_to"],
                ", ".join(groups["favoured"]) or "none", ", ".join(groups["avoided"]) or "none"))
        c = data["counts"][a]
        _out("SHADOW %-6s %s: list %d, %d would leave, %d would enter" % (a, data["shadow_day"] or "no list day",
                                                                        c["list"], c["out"], c["in"]))
    rows = page_rows(data, now.astimezone(RIYADH).strftime("%Y-%m-%d %H:%M"))
    if dry_run:
        _out("dry run: %d page row(s) computed (%d theme(s), %d sector(s), %d holding(s), %d moved name(s), %d trial "
             "row(s), %d event(s) ahead), nothing written" % (
                 len(rows), len(data["themes"]), len(data["sectors"]), len(data["holdings"]), len(data["shadow"]),
                 len(data["trial"]), len(data["events"])))
        return 0
    try:                                                # one full overwrite: safe to send again after a quota answer
        _quota_call(lambda: write_grid(book, TAB, rows, WIDTH, 120), _SLEEP, _out, "writing %s" % TAB)
    except Exception as e:  # noqa: BLE001 - reported, with the exit code that marks the run failed
        _out("ERROR: %s: %s; the page was not written" % (type(e).__name__, text(e)[:300]))
        return 2
    _out("wrote the %s page (%d rows: %d theme(s), %d sector(s), %d holding(s), %d moved name(s), %d trial row(s), "
         "%d event(s) ahead)" % (TAB, len(rows), len(data["themes"]), len(data["sectors"]), len(data["holdings"]),
                                 len(data["shadow"]), len(data["trial"]), len(data["events"])))
    return 0


def main(argv: Optional[Sequence[str]] = None, **kw) -> int:
    ap = argparse.ArgumentParser(description="TFB News Board (read-only analysis page)")
    ap.add_argument("--dry-run", action="store_true")
    a = ap.parse_args(list(argv) if argv is not None else None)
    return run_once(a.dry_run, **kw)


if __name__ == "__main__":
    sys.exit(main())
