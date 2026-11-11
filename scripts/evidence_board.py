#!/usr/bin/env python3
"""
scripts/evidence_board.py
================================================================================
TFB Evidence Board - what the archive says works, measured again every day
--------------------------------------------------------------------------------
v1.0.0 (2026-11-12)

WHY
    Reviews of 12 Nov 2026 (Claude and ChatGPT, consolidated plan in the project
    docs): the overall score shows no reliable link to later returns, 66-86% of
    forecasts are synthetic, and the 1M / 3M "forecast" is a fixed fraction of
    the 12M one. The owner asked for the improved analysis to be in the scripts
    now, not in a document. This script re-runs the measurements of that review
    on the BigQuery archive on every run and publishes them on one page.

WHAT IT WRITES (one tab, Evidence_Board; nothing else)
    1. MARKET STATE per market (US, Tadawul): the share of names above their
       50-day average, the state (WASHED OUT < 40% / MIXED / BROAD >= 60%) and
       what followed that state in the last year of weekly dates.
    2. NAMES: every INVESTABLE name of the latest signal day and every Daily
       Plan pick, with
         - a SHADOW SCORE (equal-weight challenger: analyst-target upside rank,
           price-trend rank, low risk; Tadawul: price trend only; minus 5 points
           per missing core field). It is a registered challenger, not an
           optimum, and it changes no recommendation;
         - the HISTORICAL chance of finishing up over the next 21 bars for names
           in the same market, market state and trend bucket (a frequency with
           its sample size, not a calibrated probability);
         - a 10%-90% price range for 21 bars from the stock's own 60-day
           volatility (no drift assumed).
    3. FACTOR EDGE: the rank correlation of each price factor with the return of
       the next 21 bars, per market, with a t value scaled for overlap.
    4. LABEL STABILITY: next-day retention of the INVESTABLE label, today's rule
       against the two-level rule (enter 72, leave below 67).

SAFETY
    Read-only on BigQuery (SELECT only) and on every tab but its own. TFB_EVIDENCE
    = on (default) | off. --dry-run prints and writes nothing. Advisory only: no
    order is placed, no recommendation is changed.

ENV
    DEFAULT_SPREADSHEET_ID, GOOGLE_SHEETS_CREDENTIALS, TFB_BQ_PROJECT,
    TFB_BQ_DATASET (default tfb_archive), TFB_BQ_LOCATION (default US),
    TFB_EVIDENCE, TFB_EVIDENCE_ENTER (72), TFB_EVIDENCE_EXIT (67),
    TFB_EVIDENCE_MIN_WEEKS (4).
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

UTC = _dt.timezone.utc
RIYADH = _dt.timezone(_dt.timedelta(hours=3))
TAB = "Evidence_Board"
PLAN_LEDGER_TAB = "_Daily_Plan"
WIDTH = 12
BQ_API = "https://bigquery.googleapis.com/bigquery/v2"
Z80 = 1.2816            # 10% / 90% points of a normal distribution
HORIZON = 21            # bars
CORE_FIELDS = ("P/E", "revenue growth", "free cash flow", "analyst target")
STATE_TEXT = {"WASHED_OUT": "WASHED OUT", "MIXED": "MIXED", "BROAD": "BROAD"}
MARKET_TEXT = {"US": "US", "SR": "Tadawul", "OTHER": "Other"}
FACTOR_TEXT = {"mom12_1": "12-month momentum", "hi52": "Close to 52-week high", "vs200": "Price above 200-day average",
               "lowvol": "Low volatility", "ret1m": "Last-month return", "volsurge": "Volume surge"}
_ID_RE = re.compile(r"^[A-Za-z0-9_\-]+$")
_SYM_RE = re.compile(r"^[A-Z0-9][A-Z0-9.\-]{0,14}$")


def _out(msg: str) -> None:
    print("[evidence %s] %s" % (__version__, msg), flush=True)


def _env(name: str, default: str = "") -> str:
    return (os.environ.get(name) or default).strip()


def _env_num(name: str, default: float, lo: float, hi: float) -> float:
    try:
        v = float(_env(name, str(default)))
    except ValueError:
        return default
    return v if lo <= v <= hi else default


def enabled() -> bool:
    return _env("TFB_EVIDENCE", "on").lower() not in ("off", "0", "false", "no")


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


# ----------------------------------------------------------------------------- #
# Pure analysis
# ----------------------------------------------------------------------------- #
def market_of(symbol: str) -> str:
    s = text(symbol).upper()
    return "SR" if s.endswith(".SR") else ("US" if s.endswith(".US") else "OTHER")


def state_of(above50: Optional[float]) -> str:
    """The market state from the share (0-1) of names above their 50-day average."""
    if above50 is None:
        return ""
    return "BROAD" if above50 >= 0.6 else ("MIXED" if above50 >= 0.4 else "WASHED_OUT")


def bucket_of(pmom: Optional[float], phi: Optional[float]) -> str:
    """STRONG = top fifth by 12-month momentum and upper 40% by closeness to the 52-week high; WEAK = bottom fifth."""
    if pmom is None or phi is None:
        return ""
    if pmom >= 0.8 and phi >= 0.6:
        return "STRONG"
    return "WEAK" if pmom <= 0.2 else "MIDDLE"


def history_cells(rows: Sequence[Dict[str, Any]]) -> Dict[Tuple[str, str, str], Dict[str, float]]:
    """{(market, state, bucket | 'ALL'): {up, ret, n, weeks}} from the per-bucket history rows."""
    out: Dict[Tuple[str, str, str], Dict[str, float]] = {}
    for r in rows:
        m, st, b = text(r.get("m")), text(r.get("state")), text(r.get("bucket"))
        n, up, ret, weeks = num(r.get("n")), num(r.get("up")), num(r.get("ret")), num(r.get("weeks"))
        if not m or not st or not b or not n or up is None:
            continue
        out[(m, st, b)] = {"up": up, "ret": ret or 0.0, "n": n, "weeks": weeks or 0.0}
        a = out.setdefault((m, st, "ALL"), {"up": 0.0, "ret": 0.0, "n": 0.0, "weeks": 0.0})
        tot = a["n"] + n
        a["up"], a["ret"] = (a["up"] * a["n"] + up * n) / tot, (a["ret"] * a["n"] + (ret or 0.0) * n) / tot
        a["n"], a["weeks"] = tot, max(a["weeks"], weeks or 0.0)
    return out


def chance_up(cells: Dict[Tuple[str, str, str], Dict[str, float]], market: str, state: str, bucket: str,
              min_weeks: float) -> Tuple[Optional[float], str]:
    """(share 0-1 | None, the basis in words). Falls back from the bucket to all names of that state."""
    for b, label in ((bucket, bucket.lower() + " trend"), ("ALL", "all names")):
        c = cells.get((market, state, b)) if b else None
        if c and c["weeks"] >= min_weeks and c["n"] >= 30:
            return c["up"], "%s, %s market, %s: %d name-weeks over %d weeks" % (
                MARKET_TEXT.get(market, market), STATE_TEXT.get(state, state).lower(), label, c["n"], c["weeks"])
    return None, "not enough history for this market state"


def price_range(close: Optional[float], vol_daily: Optional[float], bars: int = HORIZON) -> Tuple[Optional[float], Optional[float]]:
    """The 10% and 90% points of the price after `bars` bars from the daily volatility; no drift assumed."""
    if not close or close <= 0 or vol_daily is None or vol_daily <= 0 or vol_daily > 0.25:
        return None, None
    w = Z80 * vol_daily * math.sqrt(bars)
    return close * math.exp(-w), close * math.exp(w)


def missing_fields(r: Dict[str, Any]) -> List[str]:
    keys = ("has_pe", "has_g", "has_fcf", "has_tp")
    return [CORE_FIELDS[i] for i, k in enumerate(keys) if not num(r.get(k))]


def trend_rank(r: Dict[str, Any]) -> Optional[float]:
    parts = [x for x in (num(r.get("pmom")), num(r.get("phi")), num(r.get("p200"))) if x is not None]
    return sum(parts) / len(parts) if len(parts) == 3 else None


def shadow_score(r: Dict[str, Any]) -> Tuple[Optional[float], str]:
    """Equal-weight challenger, 0-100, minus 5 per missing core field. Returns (score | None, the parts in words)."""
    market, miss = market_of(r.get("symbol")), missing_fields(r)
    trend = trend_rank(r)
    parts: List[Tuple[str, float]] = []
    if trend is not None:
        parts.append(("trend %d" % round(trend * 100), trend))
    if market != "SR":
        pup, rk = num(r.get("pup")), num(r.get("rk"))
        if pup is not None and num(r.get("has_tp")):
            parts.append(("analyst upside %d" % round(pup * 100), pup))
        if rk is not None:
            lr = 1.0 if rk < 30 else (0.5 if rk < 45 else 0.0)
            parts.append(("low risk %d" % round(lr * 100), lr))
    if market == "SR" and trend is None:
        return None, "no price history: a Tadawul name is scored on trend only"
    if not parts:
        return None, "no input available"
    score = max(0.0, 100.0 * sum(p[1] for p in parts) / len(parts) - 5.0 * len(miss))
    why = ", ".join(p[0] for p in parts) + ("; -%d for missing %s" % (5 * len(miss), ", ".join(miss)) if miss else "")
    return round(score, 1), why


def edge_reading(t: Optional[float], ic: Optional[float]) -> str:
    if t is None or ic is None:
        return ""
    if abs(t) >= 2:
        return "edge" if ic > 0 else "works in reverse"
    return "weak" if abs(t) >= 1 else "no edge"


def name_rows(names: Sequence[Dict[str, Any]], now_state: Dict[str, str],
              cells: Dict[Tuple[str, str, str], Dict[str, float]], picks: Sequence[str], min_weeks: float) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    pset = {text(p).upper() for p in picks}
    for r in names:
        sym = text(r.get("symbol")).upper()
        if not sym:
            continue
        m = market_of(sym)
        score, why = shadow_score(r)
        bucket = bucket_of(num(r.get("pmom")), num(r.get("phi")))
        chance, basis = chance_up(cells, m, now_state.get(m, ""), bucket, min_weeks) if now_state.get(m) else (
            None, "no market state for this market")
        lo, hi = price_range(num(r.get("close")), num(r.get("vol60")))
        px, tp = num(r.get("px")), num(r.get("tp"))
        out.append({"symbol": sym, "name": text(r.get("name")), "pick": sym in pset,
                    "label": "INVESTABLE" if num(r.get("inv")) else "not investable", "score": num(r.get("sc")),
                    "shadow": score, "shadow_why": why, "chance": chance, "basis": basis, "lo": lo, "hi": hi,
                    "close": num(r.get("close")), "upside": (tp / px - 1.0) if (px and tp and px > 0) else None,
                    "trend": trend_rank(r), "missing": missing_fields(r)})
    out.sort(key=lambda d: (0 if d["pick"] else 1, -(d["shadow"] if d["shadow"] is not None else -1.0), d["symbol"]))
    return out


def fmt(v: Any, nd: int = 2) -> str:
    x = num(v)
    return "" if x is None else ("{:,.%df}" % nd).format(x)


def pct(v: Any, nd: int = 1) -> str:
    x = num(v)
    return "" if x is None else ("{:.%df}%%" % nd).format(x * 100.0)


def page_rows(data: Dict[str, Any], stamp: str) -> List[List[Any]]:
    def pad(r: Sequence[Any]) -> List[Any]:
        return (list(r) + [""] * WIDTH)[:WIDTH]

    out = [pad(["EVIDENCE BOARD - what the archive says works, measured again on every run"]),
           pad(["Built %s Riyadh · evidence_board %s · SHADOW: nothing on this page changes a recommendation or places "
                "an order · figures are historical frequencies, not calibrated probabilities" % (stamp, __version__)]),
           pad([]), pad(["1. MARKET STATE"]),
           pad(["Market", "Bars to", "Above 50-day avg", "Above 200-day avg", "State", "After this state: all names up",
                "Avg return", "Strong-trend names up", "Weeks seen", "Reading"])]
    for s in data["states"]:
        out.append(pad([MARKET_TEXT.get(s["m"], s["m"]), s["date"], pct(s["above50"]), pct(s["above200"]),
                        STATE_TEXT.get(s["state"], ""), pct(s.get("all_up")), pct(s.get("all_ret"), 2),
                        pct(s.get("strong_up")), fmt(s.get("weeks"), 0), s.get("reading", "")]))
    out += [pad([]), pad(["2. NAMES - Daily Plan picks first, then INVESTABLE names by shadow score (%d)" % len(data["names"])]),
            pad(["Symbol", "Name", "Engine label", "Engine score", "Shadow score", "Chance up, 21 bars", "Based on",
                 "Range low (10%)", "Range high (90%)", "Analyst upside", "Trend rank", "Shadow parts / missing data"])]
    for d in data["names"]:
        out.append(pad([d["symbol"] + (" (pick)" if d["pick"] else ""), d["name"], d["label"], fmt(d["score"], 1),
                        fmt(d["shadow"], 1), pct(d["chance"]), d["basis"], fmt(d["lo"]), fmt(d["hi"]),
                        pct(d["upside"]), pct(d["trend"], 0), d["shadow_why"]]))
    if not data["names"]:
        out.append(pad(["", "No INVESTABLE name and no pick on the latest signal day."]))
    out += [pad([]), pad(["3. FACTOR EDGE - rank correlation of each factor with the return of the next 21 bars"]),
            pad(["Market", "Factor", "Rank correlation", "t (overlap-scaled)", "Top fifth: up", "Top fifth: beat market",
                 "All names: up", "Weeks", "Reading"])]
    for f in data["factors"]:
        out.append(pad([MARKET_TEXT.get(f["m"], f["m"]), FACTOR_TEXT.get(f["feat"], f["feat"]), fmt(f["ic"], 3),
                        fmt(f["t"], 2), pct(f["top_up"]), pct(f["top_beat"]), pct(f["all_up"]), fmt(f["weeks"], 0),
                        edge_reading(f["t"], f["ic"])]))
    out += [pad([]), pad(["4. LABEL STABILITY - names outside Tadawul, last 60 days"]),
            pad(["Rule", "Name-days", "Kept the label next day", "Drops", "Names a day"])]
    for s in data["stability"]:
        out.append(pad([s["rule"], fmt(s["name_days"], 0), pct(s["kept"]), fmt(s["drops"], 0), fmt(s["per_day"], 1)]))
    out += [pad([]), pad(["How to read: 'Chance up' is how often names in the same market, market state and trend bucket "
                          "finished higher 21 bars later in the past year. The range assumes no drift. The shadow score "
                          "is an equal-weight challenger (analyst-target upside, price trend, low risk; Tadawul: trend "
                          "only), minus 5 per missing core field. Sources: BigQuery tfb_archive. Not investment advice."])]
    return out


# ----------------------------------------------------------------------------- #
# SQL (SELECT only)
# ----------------------------------------------------------------------------- #
def _base(ds: str, extra: str = "") -> str:
    return (
        "b AS (SELECT symbol, CASE WHEN symbol LIKE '%.SR' THEN 'SR' WHEN symbol LIKE '%.US' THEN 'US' ELSE 'OTHER' END m, "
        "date, close, volume, COUNT(*) OVER (PARTITION BY symbol) nb, ROW_NUMBER() OVER w rn, LAG(close,1) OVER w c1, "
        "LAG(close,21) OVER w c21, LAG(close,252) OVER w c252, LEAD(close,21) OVER w f21, "
        "MAX(high) OVER (w ROWS BETWEEN 251 PRECEDING AND CURRENT ROW) hi252, "
        "AVG(close) OVER (w ROWS BETWEEN 49 PRECEDING AND CURRENT ROW) sma50, "
        "AVG(close) OVER (w ROWS BETWEEN 199 PRECEDING AND CURRENT ROW) sma200, "
        "AVG(volume) OVER (w ROWS BETWEEN 4 PRECEDING AND CURRENT ROW) v5, "
        "AVG(volume) OVER (w ROWS BETWEEN 59 PRECEDING AND CURRENT ROW) v60 "
        "FROM `{DS}.v_symbol_candles` WHERE close>0 {EXTRA} QUALIFY nb>=250 WINDOW w AS (PARTITION BY symbol ORDER BY date))"
    ).replace("{DS}", ds).replace("{EXTRA}", extra)


def sql_state(ds: str) -> str:
    return (
        "WITH " + _base(ds, "AND (symbol LIKE '%.SR' OR symbol LIKE '%.US')") + ", "
        "br AS (SELECT m, date, AVG(IF(close>sma50,1,0)) above50, AVG(IF(close>sma200,1,0)) above200, COUNT(*) n "
        "FROM b WHERE rn>=200 GROUP BY 1,2 HAVING n>=150), "
        "k AS (SELECT b.m, b.date, f21/close-1 fwd, PERCENT_RANK() OVER (PARTITION BY b.m,b.date ORDER BY c21/c252) pmom, "
        "PERCENT_RANK() OVER (PARTITION BY b.m,b.date ORDER BY close/hi252) phi, br.above50 FROM b JOIN br USING (m,date) "
        "WHERE rn>=253 AND f21 IS NOT NULL AND EXTRACT(DAYOFWEEK FROM b.date)=4 AND ABS(f21/close-1)<0.8), "
        "g AS (SELECT m, date, fwd, CASE WHEN above50>=0.6 THEN 'BROAD' WHEN above50>=0.4 THEN 'MIXED' ELSE 'WASHED_OUT' END state, "
        "CASE WHEN pmom>=0.8 AND phi>=0.6 THEN 'STRONG' WHEN pmom<=0.2 THEN 'WEAK' ELSE 'MIDDLE' END bucket FROM k) "
        "SELECT 'HIST' kind, m, state, bucket, COUNT(DISTINCT date) weeks, COUNT(*) n, AVG(IF(fwd>0,1,0)) up, AVG(fwd) ret, "
        "CAST(NULL AS STRING) date FROM g GROUP BY 2,3,4 "
        "UNION ALL SELECT 'NOW', m, CAST(NULL AS STRING), CAST(NULL AS STRING), NULL, n, above50, above200, CAST(date AS STRING) "
        "FROM br QUALIFY ROW_NUMBER() OVER (PARTITION BY m ORDER BY date DESC)=1")


def sql_factors(ds: str) -> str:
    return (
        "WITH " + _base(ds) + ", "
        "s AS (SELECT *, STDDEV(SAFE_DIVIDE(close,c1)-1) OVER (PARTITION BY symbol ORDER BY date ROWS BETWEEN 59 PRECEDING AND CURRENT ROW) vol60 FROM b), "
        "k AS (SELECT m, date, f21/close-1 fwd, c21/c252-1 mom12_1, close/c21-1 ret1m, close/hi252 hi52, close/sma200-1 vs200, "
        "-vol60 lowvol, v5/NULLIF(v60,0) volsurge FROM s WHERE rn>=253 AND f21 IS NOT NULL AND EXTRACT(DAYOFWEEK FROM date)=4 "
        "AND ABS(f21/close-1)<0.8 AND v60>0), "
        "u AS (SELECT m, date, fwd, f.name feat, f.val FROM k, UNNEST([STRUCT('mom12_1' AS name, mom12_1 AS val),('hi52',hi52),"
        "('vs200',vs200),('lowvol',lowvol),('ret1m',ret1m),('volsurge',volsurge)]) f WHERE f.val IS NOT NULL), "
        "p AS (SELECT m, date, feat, fwd, PERCENT_RANK() OVER (PARTITION BY m,date,feat ORDER BY val) pv, "
        "PERCENT_RANK() OVER (PARTITION BY m,date,feat ORDER BY fwd) pf, AVG(fwd) OVER (PARTITION BY m,date,feat) mk FROM u), "
        "d AS (SELECT m, date, feat, COUNT(*) n, CORR(pv,pf) ic, AVG(IF(pv>=0.8,IF(fwd>0,1,0),NULL)) top_up, "
        "AVG(IF(pv>=0.8,IF(fwd>mk,1,0),NULL)) top_beat, AVG(IF(fwd>0,1,0)) all_up FROM p GROUP BY 1,2,3 HAVING n>=100) "
        "SELECT m, feat, COUNT(*) weeks, AVG(ic) ic, AVG(ic)/NULLIF(STDDEV(ic),0)*SQRT(COUNT(*)/4) t, AVG(top_up) top_up, "
        "AVG(top_beat) top_beat, AVG(all_up) all_up FROM d GROUP BY 1,2 ORDER BY 1, ic DESC")


def clean_symbols(symbols: Sequence[Any]) -> List[str]:
    out: List[str] = []
    for s in symbols:
        t = text(s).upper()
        if _SYM_RE.match(t) and t not in out:
            out.append(t)
    return out[:60]


def sql_names(ds: str, symbols: Sequence[Any]) -> str:
    lst = ",".join("'%s'" % s for s in clean_symbols(symbols))
    return (
        "WITH " + _base(ds) + ", "
        "r AS (SELECT *, STDDEV(SAFE_DIVIDE(close,c1)-1) OVER (PARTITION BY symbol ORDER BY date ROWS BETWEEN 59 PRECEDING AND CURRENT ROW) vol60 FROM b), "
        "f AS (SELECT symbol, close, vol60, PERCENT_RANK() OVER (PARTITION BY m ORDER BY c21/c252) pmom, "
        "PERCENT_RANK() OVER (PARTITION BY m ORDER BY close/hi252) phi, PERCENT_RANK() OVER (PARTITION BY m ORDER BY close/sma200) p200 "
        "FROM r WHERE rn=nb AND rn>=253 AND date>=DATE_SUB(CURRENT_DATE(), INTERVAL 10 DAY)), "
        "sg0 AS (SELECT symbol, signal_date d, MAX(IF(investability='INVESTABLE',1,0)) inv, AVG(overall_score) sc, "
        "AVG(risk_score) rk, AVG(price) px, AVG(target_price) tp FROM `{DS}.v_signal_history` "
        "WHERE signal_date>=DATE_SUB(CURRENT_DATE(), INTERVAL 14 DAY) GROUP BY 1,2), "
        "sg AS (SELECT symbol, IF(d=MAX(d) OVER (), inv, 0) inv, sc, rk, px, tp FROM sg0 "
        "QUALIFY ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY d DESC)=1), "
        "u AS (SELECT symbol, ANY_VALUE(name) name, MAX(IF(pe_ttm IS NULL,0,1)) has_pe, MAX(IF(revenue_growth_yoy_pct IS NULL,0,1)) has_g, "
        "MAX(IF(free_cash_flow_ttm IS NULL,0,1)) has_fcf, MAX(IF(target_price IS NULL,0,1)) has_tp, MAX(target_upside_pct) up_pct "
        "FROM `{DS}.v_universe_latest` GROUP BY 1), "
        "uu AS (SELECT symbol, PERCENT_RANK() OVER (PARTITION BY symbol LIKE '%.SR' ORDER BY up_pct) pup FROM u "
        "WHERE has_tp=1 AND up_pct BETWEEN -80 AND 300), "
        "x AS (SELECT symbol FROM sg WHERE inv=1 UNION DISTINCT SELECT s FROM UNNEST(ARRAY<STRING>[{LIST}]) s) "
        "SELECT x.symbol, u.name, sg.inv, sg.sc, sg.rk, sg.px, sg.tp, f.close, f.vol60, f.pmom, f.phi, f.p200, uu.pup, "
        "u.has_pe, u.has_g, u.has_fcf, u.has_tp FROM x LEFT JOIN sg USING (symbol) LEFT JOIN f USING (symbol) "
        "LEFT JOIN u USING (symbol) LEFT JOIN uu USING (symbol)"
    ).replace("{DS}", ds).replace("{LIST}", lst)


def sql_stability(ds: str, enter: float, leave: float) -> str:
    return (
        "WITH s AS (SELECT symbol, signal_date d, MAX(IF(investability='INVESTABLE',1,0)) inv, "
        "MAX(IF(investability='BLOCKED',1,0)) blk, AVG(overall_score) sc FROM `%s.v_signal_history` "
        "WHERE signal_date>=DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY) AND NOT symbol LIKE '%%.SR' GROUP BY 1,2), "
        "e AS (SELECT *, CASE WHEN blk=1 OR sc<%s THEN 0 WHEN inv=1 AND sc>=%s THEN 1 END ev FROM s), "
        "h AS (SELECT *, IFNULL(LAST_VALUE(ev IGNORE NULLS) OVER (PARTITION BY symbol ORDER BY d ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW),0) hb FROM e), "
        "j AS (SELECT *, LEAD(inv) OVER w n_inv, LEAD(hb) OVER w n_hb FROM h WINDOW w AS (PARTITION BY symbol ORDER BY d)), "
        "u AS (SELECT 'Current label' rule, inv x, n_inv nx, d FROM j WHERE d>=DATE_SUB(CURRENT_DATE(), INTERVAL 60 DAY) "
        "UNION ALL SELECT 'Two-level: enter %s, leave below %s', hb, n_hb, d FROM j WHERE d>=DATE_SUB(CURRENT_DATE(), INTERVAL 60 DAY)) "
        "SELECT rule, COUNTIF(x=1) name_days, SAFE_DIVIDE(COUNTIF(x=1 AND nx=1), COUNTIF(x=1 AND nx IS NOT NULL)) kept, "
        "COUNTIF(x=1 AND nx=0) drops, SAFE_DIVIDE(COUNTIF(x=1), COUNT(DISTINCT d)) per_day FROM u GROUP BY 1 ORDER BY 1"
        % (ds, _n(leave), _n(enter), _n(enter), _n(leave)))


def _n(v: float) -> str:
    return ("%g" % float(v))


# ----------------------------------------------------------------------------- #
# BigQuery (REST, read-only)
# ----------------------------------------------------------------------------- #
def parse_result(payload: Dict[str, Any]) -> List[Dict[str, Any]]:
    fields = [text(f.get("name")) for f in ((payload.get("schema") or {}).get("fields") or [])]
    out: List[Dict[str, Any]] = []
    for row in payload.get("rows") or []:
        vals = [c.get("v") if isinstance(c, dict) else None for c in (row.get("f") or [])]
        out.append({fields[i]: (vals[i] if i < len(vals) else None) for i in range(len(fields))})
    return out


def bq_query(session: Any, project: str, location: str, sql: str, wait_s: float = 300.0, sleep=time.sleep) -> List[Dict[str, Any]]:
    """Run one SELECT and return its rows as dicts. Raises RuntimeError on any refusal or timeout."""
    if not re.match(r"^\s*(WITH|SELECT)\b", sql, re.I) or re.search(r"\b(INSERT|UPDATE|DELETE|MERGE|CREATE|DROP|ALTER|TRUNCATE)\b", sql, re.I):
        raise RuntimeError("only a SELECT may run here")
    resp = session.post("%s/projects/%s/queries" % (BQ_API, project), timeout=120,
                        json={"query": sql, "useLegacySql": False, "location": location, "timeoutMs": 50000,
                              "maxResults": 5000})
    if getattr(resp, "status_code", 500) != 200:
        raise RuntimeError("BigQuery refused the query (HTTP %s): %s" % (getattr(resp, "status_code", "?"), text(getattr(resp, "text", ""))[:300]))
    payload = resp.json()
    end = time.monotonic() + wait_s
    while not payload.get("jobComplete"):
        if time.monotonic() > end:
            raise RuntimeError("BigQuery did not finish the query in %d s" % wait_s)
        sleep(2)
        ref = payload.get("jobReference") or {}
        resp = session.get("%s/projects/%s/queries/%s?location=%s&timeoutMs=50000&maxResults=5000"
                           % (BQ_API, project, ref.get("jobId"), ref.get("location") or location), timeout=120)
        if getattr(resp, "status_code", 500) != 200:
            raise RuntimeError("BigQuery refused the result read (HTTP %s)" % getattr(resp, "status_code", "?"))
        payload = resp.json()
    return parse_result(payload)


def collect(session: Any, project: str, dataset: str, location: str, picks: Sequence[str], k: Dict[str, float],
            query=bq_query) -> Dict[str, Any]:
    ds = "%s.%s" % (project, dataset)
    st = query(session, project, location, sql_state(ds))
    cells = history_cells([r for r in st if text(r.get("kind")) == "HIST"])
    states, now_state = [], {}
    for r in sorted((r for r in st if text(r.get("kind")) == "NOW"), key=lambda r: text(r.get("m")), reverse=True):
        m, a50 = text(r.get("m")), num(r.get("up"))
        state = state_of(a50)
        now_state[m] = state
        allc, strong = cells.get((m, state, "ALL")), cells.get((m, state, "STRONG"))
        thin = (not allc) or allc["weeks"] < k["min_weeks"]
        states.append({"m": m, "date": text(r.get("date")), "above50": a50, "above200": num(r.get("ret")), "state": state,
                       "all_up": allc["up"] if allc else None, "all_ret": allc["ret"] if allc else None,
                       "strong_up": strong["up"] if strong else None, "weeks": allc["weeks"] if allc else 0,
                       "reading": "seen in too few weeks to read" if thin else ""})
    names = name_rows(query(session, project, location, sql_names(ds, picks)), now_state, cells, picks, k["min_weeks"])
    factors = [{"m": text(r.get("m")), "feat": text(r.get("feat")), "weeks": num(r.get("weeks")), "ic": num(r.get("ic")),
                "t": num(r.get("t")), "top_up": num(r.get("top_up")), "top_beat": num(r.get("top_beat")),
                "all_up": num(r.get("all_up"))} for r in query(session, project, location, sql_factors(ds))]
    stability = [{"rule": text(r.get("rule")), "name_days": num(r.get("name_days")), "kept": num(r.get("kept")),
                  "drops": num(r.get("drops")), "per_day": num(r.get("per_day"))}
                 for r in query(session, project, location, sql_stability(ds, k["enter"], k["leave"]))]
    return {"states": states, "names": names, "factors": factors, "stability": stability}


def plan_picks(values: Sequence[Sequence[Any]], today: _dt.date) -> List[str]:
    """Symbols of the latest plan day's PICK rows in the Daily Plan ledger (at most 7 days old)."""
    rows = [list(r) for r in values if r]
    if not rows:
        return []
    hdr = [text(c) for c in rows[0]]
    if not {"Plan Date", "Kind", "Symbol"} <= set(hdr):
        return []
    di, ki, si = hdr.index("Plan Date"), hdr.index("Kind"), hdr.index("Symbol")
    days = sorted({text(r[di]) for r in rows[1:] if len(r) > max(di, ki, si) and text(r[di])})
    if not days:
        return []
    last = days[-1]
    try:
        if (today - _dt.date.fromisoformat(last[:10])).days > 7:
            return []
    except ValueError:
        return []
    return clean_symbols(r[si] for r in rows[1:] if len(r) > max(di, ki, si) and text(r[di]) == last and text(r[ki]) == "PICK")


# ----------------------------------------------------------------------------- #
def run_once(dry_run: bool, book=None, session=None, now: Optional[_dt.datetime] = None, query=bq_query) -> int:
    now = now or _dt.datetime.now(UTC)
    if not enabled():
        _out("TFB_EVIDENCE is off: nothing read, nothing written")
        return 0
    project, dataset = _env("TFB_BQ_PROJECT"), _env("TFB_BQ_DATASET", "tfb_archive")
    if not _ID_RE.match(project) or not _ID_RE.match(dataset):
        _out("ERROR: TFB_BQ_PROJECT / TFB_BQ_DATASET missing or malformed; nothing written")
        return 2
    k = {"enter": _env_num("TFB_EVIDENCE_ENTER", 72, 0, 100), "leave": _env_num("TFB_EVIDENCE_EXIT", 67, 0, 100),
         "min_weeks": _env_num("TFB_EVIDENCE_MIN_WEEKS", 4, 1, 52)}
    try:
        if book is None:
            from scripts.run_calendar_sync import _open_book  # noqa: PLC0415
            book = _open_book()
        if session is None:
            from scripts.archive_to_bigquery import make_session  # noqa: PLC0415
            session = make_session()
        from scripts.daily_plan import _read, write_grid  # noqa: PLC0415
        picks = plan_picks(_read(book, PLAN_LEDGER_TAB, "A1:Z2100"), now.astimezone(RIYADH).date())
        data = collect(session, project, dataset, _env("TFB_BQ_LOCATION", "US"), picks, k, query=query)
    except Exception as e:  # noqa: BLE001 - one page; a failed read leaves the previous page in place
        _out("ERROR: %s: %s; nothing written" % (type(e).__name__, text(e)[:300]))
        return 2
    for s in data["states"]:
        _out("STATE %-7s %s above-50d %s -> %s | after this state: all up %s, strong-trend up %s (%s weeks)" % (
            MARKET_TEXT.get(s["m"], s["m"]), s["date"], pct(s["above50"]), STATE_TEXT.get(s["state"], "?"),
            pct(s["all_up"]), pct(s["strong_up"]), fmt(s["weeks"], 0)))
    for d in data["names"][:25]:
        _out("NAME %-9s %s engine %s shadow %s chance-up %s range %s-%s | %s" % (
            d["symbol"], "PICK" if d["pick"] else "    ", fmt(d["score"], 1) or "-", fmt(d["shadow"], 1) or "-",
            pct(d["chance"]) or "-", fmt(d["lo"]) or "-", fmt(d["hi"]) or "-", d["shadow_why"]))
    for f in data["factors"]:
        if edge_reading(f["t"], f["ic"]) in ("edge", "works in reverse"):
            _out("EDGE %-7s %-28s corr %s t %s" % (MARKET_TEXT.get(f["m"], f["m"]), FACTOR_TEXT.get(f["feat"], f["feat"]),
                                                   fmt(f["ic"], 3), fmt(f["t"], 2)))
    for s in data["stability"]:
        _out("STABILITY %-40s kept %s drops %s names/day %s" % (s["rule"], pct(s["kept"]), fmt(s["drops"], 0), fmt(s["per_day"], 1)))
    rows = page_rows(data, now.astimezone(RIYADH).strftime("%Y-%m-%d %H:%M"))
    if dry_run:
        _out("dry run: %d page row(s) computed, nothing written" % len(rows))
        return 0
    write_grid(book, TAB, rows, WIDTH, 120)
    _out("wrote the %s page (%d rows: %d market(s), %d name(s), %d factor row(s))" % (
        TAB, len(rows), len(data["states"]), len(data["names"]), len(data["factors"])))
    return 0


def main(argv: Optional[Sequence[str]] = None, **kw) -> int:
    ap = argparse.ArgumentParser(description="TFB Evidence Board (read-only analysis page)")
    ap.add_argument("--dry-run", action="store_true")
    a = ap.parse_args(list(argv) if argv is not None else None)
    return run_once(a.dry_run, **kw)


if __name__ == "__main__":
    sys.exit(main())
