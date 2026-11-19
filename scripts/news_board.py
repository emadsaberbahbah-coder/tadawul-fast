#!/usr/bin/env python3
"""
scripts/news_board.py
================================================================================
TFB News Board - the world news, the sector tilt it gives, and whether following it would have helped
--------------------------------------------------------------------------------
v1.0.0 (2026-11-19)
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
       investable list today. It is not an order.
    5. TRIAL: v_trial_news_overlay as it stands, and the promotion rule.

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
import math
import os
import re
import sys
import time
from typing import Any, Dict, List, Optional, Sequence, Tuple

__version__ = "1.0.0"
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
_SYM_RE = re.compile(r"^[A-Z0-9][A-Z0-9.\-]{0,14}$")


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
    s = text(symbol).upper()
    return s.rsplit(".", 1)[1] if "." in s else ""


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
    """Symbol and name of each row under the 'Symbol' header of My_Portfolio (order kept)."""
    rows = [list(r) for r in values or []]
    for hi, r in enumerate(rows[:12]):
        cells = [text(c) for c in r]
        if "Symbol" in cells:
            si = cells.index("Symbol")
            out, seen = [], set()
            for row in rows[hi + 1:]:
                sym = text(row[si]).upper() if len(row) > si else ""
                if _SYM_RE.match(sym) and "." in sym and sym not in seen:
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


def holding_rows(holdings: Sequence[Dict[str, str]], sectors: Dict[str, str], tilt: Dict[str, Dict[str, Any]],
                 names: Dict[str, Dict[str, Any]]) -> List[Dict[str, Any]]:
    out = []
    for h in holdings:
        sym = h["symbol"]
        sector = sectors.get(sym, "")
        seen = names.get(sym) or {}
        row: Dict[str, Any] = {"symbol": sym, "name": h.get("name") or seen.get("name", ""), "sector": sector,
                               "listed": "yes" if seen.get("listed") else "no",
                               "shadow": ACTION_TEXT.get((seen.get("actions") or {}).get("events", ""), "")}
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
# 5. Trial and the verdict
# ----------------------------------------------------------------------------- #
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


# ----------------------------------------------------------------------------- #
def collect(session: Any, project: str, dataset: str, location: str, holdings: Sequence[Dict[str, str]],
            today: _dt.date, query: Any) -> Dict[str, Any]:
    ds = "%s.%s" % (project, dataset)
    last = query(session, project, location, sql_last_news(ds, today))
    last_news = day(last[0].get("last_day")) if last else None
    themes = theme_rows(query(session, project, location, sql_themes(ds, today)))
    tilt = pick_tilt(query(session, project, location, sql_tilt(ds, today)), today)
    record = record_rows(query(session, project, location, sql_record(ds)))
    syms = clean_symbols(h["symbol"] for h in holdings)
    sectors = {text(r.get("symbol")).upper(): text(r.get("sector"))
               for r in (query(session, project, location, sql_sectors(ds, syms)) if syms else [])}
    shadow_day, names = shadow_names(query(session, project, location, sql_shadow(ds, today)))
    trial = trial_rows(query(session, project, location, sql_trial(ds)))
    return {"last_news": last_news.isoformat() if last_news else "", "themes": themes, "tilt": tilt,
            "sectors": sector_rows(tilt), "record": record,
            "holdings": holding_rows(holdings, sectors, tilt, names), "shadow_day": shadow_day,
            "counts": shadow_counts(names), "shadow": shadow_rows(names), "trial": trial,
            "verdict": verdict_lines(trial, tilt, last_news, today)}


def run_once(dry_run: bool, book=None, session=None, now: Optional[_dt.datetime] = None, query=None) -> int:
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
        data = collect(session, project, dataset, _env("TFB_BQ_LOCATION", "US"), holdings, today, query)
    except Exception as e:  # noqa: BLE001 - one page; a failed read leaves the previous page in place
        _out("ERROR: %s: %s; nothing written" % (type(e).__name__, text(e)[:300]))
        return 2
    for line in data["verdict"]:
        _out("VERDICT %s" % line)
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
             "row(s)), nothing written" % (len(rows), len(data["themes"]), len(data["sectors"]), len(data["holdings"]),
                                           len(data["shadow"]), len(data["trial"])))
        return 0
    try:                                                # one full overwrite: safe to send again after a quota answer
        _quota_call(lambda: write_grid(book, TAB, rows, WIDTH, 120), _SLEEP, _out, "writing %s" % TAB)
    except Exception as e:  # noqa: BLE001 - reported, with the exit code that marks the run failed
        _out("ERROR: %s: %s; the page was not written" % (type(e).__name__, text(e)[:300]))
        return 2
    _out("wrote the %s page (%d rows: %d theme(s), %d sector(s), %d holding(s), %d moved name(s), %d trial row(s))"
         % (TAB, len(rows), len(data["themes"]), len(data["sectors"]), len(data["holdings"]), len(data["shadow"]),
            len(data["trial"])))
    return 0


def main(argv: Optional[Sequence[str]] = None, **kw) -> int:
    ap = argparse.ArgumentParser(description="TFB News Board (read-only analysis page)")
    ap.add_argument("--dry-run", action="store_true")
    a = ap.parse_args(list(argv) if argv is not None else None)
    return run_once(a.dry_run, **kw)


if __name__ == "__main__":
    sys.exit(main())
