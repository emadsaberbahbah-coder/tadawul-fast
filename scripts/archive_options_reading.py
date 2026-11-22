#!/usr/bin/env python3
# scripts/archive_options_reading.py
"""
================================================================================
TFB Options Reading - v1.0.1 (22 Nov 2026) - what the options market says about a stock
================================================================================
WHY (owner, 22 Nov 2026 19:53 Riyadh): "I need to add how future options and
other options affect the current price and recommendations, as well we can
add this effect to our score and analysis". TFB scored every stock from its
own price, fundamentals, analysts and news, but never read the options
market, where traders pay for protection before bad news and for upside
before good news. Studies of US stocks find some of that information leaks
into the share price days later: a steep put skew (puts dear against calls)
has been followed by weaker returns, calls dear against puts at the same
strike by stronger ones, and the implied move tells how far the market
expects the price to travel to an expiry (earnings in particular). Whether
those findings hold for TFB's names is for TFB's own record to show:
v_options_reading and v_driver_learning (views 1.23.0) score every reading
against the next week and month, observe-only.

v1.0.1 (22 Nov 2026, the first written run): every one of 935 chains failed
from the GitHub runner and the log could not say why. The request now looks
like the browser call Cboe's own quote pages make (a browser User-Agent,
Accept JSON, Origin and Referer www.cboe.com), and a failed run counts its
misses by reason (http 403, http 404, other http, network, unreadable) -
counts only, never a symbol.

WHAT: once a day after the US close, for the US stocks TFB follows (the
largest by market value, every name the engine rates INVESTABLE or a buy, and
the owner's holdings) plus a fixed set of ETFs, the job reads the delayed
option chain that Cboe publishes for free (cdn.cboe.com, 15-minute delay, no
key) and keeps one row per underlying and trading day:
  iv30              Cboe's 30-day implied volatility of the stock (%)
  atm_iv_front/30/90  at-the-money implied volatility of the front expiry and
                    of the expiries nearest 30 and 90 days (%)
  implied_move_pct  the at-the-money straddle of the front expiry over the
                    price: how far the market prices the stock to travel by
                    that expiry, either way (%)
  skew_25d_pp       implied volatility of the 25-delta put minus the 25-delta
                    call, expiry nearest 30 days (points)
  cp_iv_spread_pp   call minus put implied volatility at the at-the-money
                    strike, same expiry (points)
  put/call volume and open interest, their ratios, and the day's option
  volume over open interest (fresh positioning)
and, from FINRA's public Reg SHO file of the same day (no key), the share of
the day's volume sold short. Option contracts on an adjusted deliverable
(roots such as SUPN1 after a corporate action, or strikes that are not a
multiple of 0.50, such as the 26.87 strikes SUPN's own root carries after its
merger) are left out, as are strikes beyond half or one and a half times the
price.
NOTHING is written to the workbook; the live system never reads BigQuery; no
decision, gate or count depends on this table. Logs print counts only: the
symbols read are never named (the owner's holdings are among them).

TABLE: <dataset>.options_daily, partitioned by snapshot_date (the trading day
the prices belong to, from Cboe's last trade time). Each run replaces that
day's partition (WRITE_TRUNCATE on options_daily$YYYYMMDD); a stock whose
data belongs to another day is left out.

GATE: TFB_BQ_OPTIONS = on|1|true|yes arms the job (default off here - the
workflow arms it; kill with the repository Variable TFB_BQ_OPTIONS=off).
Without --write it is a dry run (reads Cboe and FINRA, loads nothing).
ENV: TFB_BQ_PROJECT, TFB_BQ_DATASET (default tfb_archive), TFB_BQ_LOCATION
(default US), GOOGLE_SHEETS_CREDENTIALS, GITHUB_RUN_ID, TFB_OPTIONS_MAX
(largest names read beside the rated ones and the holdings; default 900).
CLI: --write | --dry-run (default) ; --max N ; --symbols A,B (read only these,
by plain ticker).
Exit: 0 loaded (or dry run), 1 fewer than half the names could be read or
the load failed, 2 configuration missing or the table differs from SCHEMA.

Not a licensed financial adviser; the owner decides and executes every trade.
================================================================================
"""
from __future__ import annotations

import argparse
import concurrent.futures as _cf
import datetime as _dt
import gzip
import importlib.util
import json
import math
import os
import re
import sys
import time
import urllib.request
import uuid
from pathlib import Path
from typing import Any, Callable, Dict, Iterable, List, Optional, Sequence, Tuple

__version__ = "1.0.1"

TABLE = "options_daily"
CBOE_URL = "https://cdn.cboe.com/api/global/delayed_quotes/options/{sym}.json"
FINRA_URL = "https://cdn.finra.org/equity/regsho/daily/CNMSshvol{ymd}.txt"
UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) "
      "Chrome/131.0.0.0 Safari/537.36")
CBOE_HEADERS = {"Accept": "application/json, text/plain, */*", "Origin": "https://www.cboe.com",
                "Referer": "https://www.cboe.com/", "Accept-Language": "en-US,en;q=0.9"}
DEFAULT_MAX = 900
WORKERS = 6
# ETFs read every day: the market, its sectors and the assets the readings use
ETFS = ("SPY", "QQQ", "IWM", "DIA", "XLK", "XLF", "XLE", "XLV", "XLI", "XLY", "XLP", "XLU", "XLB", "XLRE", "XLC",
        "GLD", "TLT", "HYG", "EEM", "KSA")
OCC = re.compile(r"^([A-Z][A-Z0-9.]*?)(\d{6})([CP])(\d{8})$")
SOURCE = "cboe_delayed+finra_regsho"

SCHEMA: List[Dict[str, str]] = [
    {"name": "snapshot_date", "type": "DATE", "mode": "REQUIRED", "description": "trading day the prices belong to (US)"},
    {"name": "symbol", "type": "STRING", "mode": "REQUIRED", "description": "TFB symbol (SUPN.US) or ETF ticker"},
    {"name": "underlying", "type": "STRING", "mode": "REQUIRED", "description": "ticker as Cboe lists it"},
    {"name": "is_etf", "type": "BOOLEAN", "mode": "REQUIRED", "description": "one of the fixed ETFs"},
    {"name": "price", "type": "FLOAT", "mode": "NULLABLE", "description": "underlying last price at Cboe"},
    {"name": "iv30", "type": "FLOAT", "mode": "NULLABLE", "description": "Cboe 30-day implied volatility, %"},
    {"name": "atm_iv_front", "type": "FLOAT", "mode": "NULLABLE", "description": "ATM implied vol, front expiry, %"},
    {"name": "atm_iv_30d", "type": "FLOAT", "mode": "NULLABLE", "description": "ATM implied vol, expiry nearest 30 days, %"},
    {"name": "atm_iv_90d", "type": "FLOAT", "mode": "NULLABLE", "description": "ATM implied vol, expiry nearest 90 days, %"},
    {"name": "front_expiry", "type": "DATE", "mode": "NULLABLE", "description": "front expiry used (2 days or more away)"},
    {"name": "front_days", "type": "INTEGER", "mode": "NULLABLE", "description": "calendar days to the front expiry"},
    {"name": "implied_move_pct", "type": "FLOAT", "mode": "NULLABLE", "description": "ATM straddle / price to the front expiry, %"},
    {"name": "skew_25d_pp", "type": "FLOAT", "mode": "NULLABLE", "description": "25-delta put IV minus 25-delta call IV, points"},
    {"name": "cp_iv_spread_pp", "type": "FLOAT", "mode": "NULLABLE", "description": "ATM call IV minus put IV, points"},
    {"name": "call_volume", "type": "FLOAT", "mode": "NULLABLE", "description": "contracts traded, calls"},
    {"name": "put_volume", "type": "FLOAT", "mode": "NULLABLE", "description": "contracts traded, puts"},
    {"name": "call_oi", "type": "FLOAT", "mode": "NULLABLE", "description": "open interest, calls"},
    {"name": "put_oi", "type": "FLOAT", "mode": "NULLABLE", "description": "open interest, puts"},
    {"name": "pc_volume_ratio", "type": "FLOAT", "mode": "NULLABLE", "description": "put volume / call volume"},
    {"name": "pc_oi_ratio", "type": "FLOAT", "mode": "NULLABLE", "description": "put OI / call OI"},
    {"name": "volume_oi_ratio", "type": "FLOAT", "mode": "NULLABLE", "description": "option volume / open interest"},
    {"name": "contracts_used", "type": "INTEGER", "mode": "NULLABLE", "description": "standard contracts in the band"},
    {"name": "adjusted_left_out", "type": "INTEGER", "mode": "NULLABLE", "description": "contracts on an adjusted root"},
    {"name": "short_volume", "type": "FLOAT", "mode": "NULLABLE", "description": "FINRA short sale volume"},
    {"name": "short_total_volume", "type": "FLOAT", "mode": "NULLABLE", "description": "FINRA total volume"},
    {"name": "short_ratio_pct", "type": "FLOAT", "mode": "NULLABLE", "description": "short volume / total volume, %"},
    {"name": "short_date", "type": "DATE", "mode": "NULLABLE", "description": "day of the FINRA file used"},
    {"name": "source", "type": "STRING", "mode": "REQUIRED", "description": "cboe_delayed+finra_regsho"},
    {"name": "captured_at", "type": "TIMESTAMP", "mode": "REQUIRED", "description": "when the job read it"},
    {"name": "run_id", "type": "STRING", "mode": "NULLABLE", "description": "GitHub run id"},
]


def _out(msg: str) -> None:
    sys.stderr.write(f"[options_reading v{__version__}] {msg}\n")


def _env(name: str, default: str = "") -> str:
    return (os.getenv(name) or default).strip()


def enabled() -> bool:
    return _env("TFB_BQ_OPTIONS").lower() in ("1", "on", "true", "yes")


def _num(v: Any) -> Optional[float]:
    try:
        x = float(v)
    except (TypeError, ValueError):
        return None
    return x if math.isfinite(x) else None


def _r(x: Optional[float], d: int = 2) -> Optional[float]:
    return None if x is None else round(x, d)


# --------------------------------------------------------------------- the chain
def parse_occ(code: str) -> Optional[Tuple[str, _dt.date, str, float]]:
    """('SUPN', date(2026,12,18), 'C', 27.5) from 'SUPN261218C00027500'."""
    m = OCC.match(str(code or ""))
    if not m:
        return None
    root, ymd, cp, k = m.groups()
    try:
        exp = _dt.date(2000 + int(ymd[:2]), int(ymd[2:4]), int(ymd[4:]))
    except ValueError:
        return None
    return root, exp, cp, int(k) / 1000.0


MIN_OI = 500          # a chain with less open interest than this gives no derived reading (its quotes are not prices)
MAX_SPREAD = 0.6      # a quote whose bid-ask spread is wider than this share of its mid is not used


def _mid(o: Dict[str, Any]) -> Optional[float]:
    """The mid of a two-sided quote tight enough to be a price; None otherwise."""
    b, a = _num(o.get("bid")), _num(o.get("ask"))
    if b is None or a is None or b <= 0 or a < b:
        return None
    m = (a + b) / 2.0
    return m if (a - b) / m <= MAX_SPREAD else None


def _iv(o: Dict[str, Any]) -> Optional[float]:
    """Implied volatility in %, only for a contract with a usable quote."""
    v = _num(o.get("iv"))
    return v * 100.0 if v is not None and 0.01 < v < 5.0 and _mid(o) is not None else None


def trade_day(data: Dict[str, Any], stamp: Any) -> Optional[_dt.date]:
    """The trading day of the underlying's prices: its last trade time, else Cboe's timestamp."""
    for v in (data.get("last_trade_time"), stamp):
        s = str(v or "")[:10]
        if re.fullmatch(r"\d{4}-\d{2}-\d{2}", s):
            return _dt.date.fromisoformat(s)
    return None


def chain_features(payload: Dict[str, Any], underlying: str) -> Optional[Dict[str, Any]]:
    """One row of readings from a Cboe delayed-quotes answer; None when it holds no usable chain."""
    data = (payload or {}).get("data") or {}
    day = trade_day(data, (payload or {}).get("timestamp"))
    price = _num(data.get("current_price")) or _num(data.get("close"))
    if not day or not price or price <= 0:
        return None
    want = underlying.replace(".", "").upper()
    by_exp: Dict[_dt.date, Dict[float, Dict[str, Dict[str, Any]]]] = {}
    used = adjusted = 0
    cv = pv = coi = poi = 0.0
    for o in data.get("options") or []:
        p = parse_occ(o.get("option"))
        if not p:
            continue
        root, exp, cp, k = p
        if root.replace(".", "") != want or round(k * 1000) % 500:
            adjusted += 1          # an adjusted deliverable: another root (SUPN1), or an odd strike (26.87) after a merger
            continue
        if (exp - day).days < 0 or not (0.5 * price <= k <= 1.5 * price):
            continue
        used += 1
        vol, oi = _num(o.get("volume")) or 0.0, _num(o.get("open_interest")) or 0.0
        if cp == "C":
            cv, coi = cv + vol, coi + oi
        else:
            pv, poi = pv + vol, poi + oi
        by_exp.setdefault(exp, {}).setdefault(k, {})[cp] = o
    if not used:
        return None

    liquid = coi + poi >= MIN_OI

    def atm(exp: _dt.date) -> Optional[Tuple[float, Dict[str, Any], Dict[str, Any]]]:
        pairs = [(abs(k - price), k, s) for k, s in by_exp.get(exp, {}).items()
                 if "C" in s and "P" in s and _mid(s["C"]) is not None and _mid(s["P"]) is not None]
        if not pairs:
            return None
        _, k, s = min(pairs, key=lambda x: (x[0], x[1]))
        return k, s["C"], s["P"]

    def atm_iv(exp: Optional[_dt.date]) -> Optional[float]:
        a = atm(exp) if exp and liquid else None
        if not a:
            return None
        ivs = [v for v in (_iv(a[1]), _iv(a[2])) if v is not None]
        return sum(ivs) / len(ivs) if ivs else None

    exps = sorted(e for e in by_exp if (e - day).days >= 2)

    def nearest(days: int, lo: int) -> Optional[_dt.date]:
        c = [e for e in exps if (e - day).days >= lo]
        return min(c, key=lambda e: (abs((e - day).days - days), e)) if c else None

    front = exps[0] if exps else None
    e30, e90 = nearest(30, 7), nearest(90, 45)
    move = None
    if front and liquid and atm(front):
        k, c, p = atm(front)
        mc, mp = _mid(c), _mid(p)
        if mc is not None and mp is not None:
            move = (mc + mp) / price * 100.0
    skew = cp_spread = None
    if e30 and liquid:
        calls = [(abs(_num(s["C"].get("delta")) - 0.25), _iv(s["C"])) for s in by_exp[e30].values()
                 if "C" in s and _num(s["C"].get("delta")) is not None and 0.10 <= _num(s["C"].get("delta")) <= 0.40]
        puts = [(abs(_num(s["P"].get("delta")) + 0.25), _iv(s["P"])) for s in by_exp[e30].values()
                if "P" in s and _num(s["P"].get("delta")) is not None and -0.40 <= _num(s["P"].get("delta")) <= -0.10]
        calls = [c for c in calls if c[1] is not None]
        puts = [p for p in puts if p[1] is not None]
        if calls and puts:
            skew = min(puts)[1] - min(calls)[1]
        a = atm(e30)
        if a and _iv(a[1]) is not None and _iv(a[2]) is not None:
            cp_spread = _iv(a[1]) - _iv(a[2])
    iv30 = _num(data.get("iv30"))
    return {
        "snapshot_date": day.isoformat(), "price": _r(price, 4),
        "iv30": _r(iv30) if iv30 and iv30 > 0 else _r(atm_iv(e30)),
        "atm_iv_front": _r(atm_iv(front)), "atm_iv_30d": _r(atm_iv(e30)), "atm_iv_90d": _r(atm_iv(e90)),
        "front_expiry": front.isoformat() if front else None, "front_days": (front - day).days if front else None,
        "implied_move_pct": _r(move), "skew_25d_pp": _r(skew), "cp_iv_spread_pp": _r(cp_spread),
        "call_volume": cv, "put_volume": pv, "call_oi": coi, "put_oi": poi,
        "pc_volume_ratio": _r(pv / cv, 3) if cv > 0 else None, "pc_oi_ratio": _r(poi / coi, 3) if coi > 0 else None,
        "volume_oi_ratio": _r((cv + pv) / (coi + poi), 4) if coi + poi > 0 else None,
        "contracts_used": used, "adjusted_left_out": adjusted,
    }


# The derived readings (ATM volatility, implied move, skew, call-put spread) need a liquid chain: at least
# MIN_OI contracts open and two-sided quotes within MAX_SPREAD of their mid. A thin chain still gives Cboe's own
# iv30 and the volumes.


# --------------------------------------------------------------------- FINRA
def parse_finra(text: str) -> Dict[str, Tuple[float, float]]:
    """{'SUPN': (short_volume, total_volume)} from a Reg SHO daily file (Date|Symbol|ShortVolume|ShortExemptVolume|TotalVolume|Market)."""
    out: Dict[str, Tuple[float, float]] = {}
    lines = [l for l in str(text or "").splitlines() if l.strip()]
    if not lines or not lines[0].lower().startswith("date|symbol"):
        return out
    for line in lines[1:]:
        parts = line.split("|")
        if len(parts) < 5:
            continue
        sv, tv = _num(parts[2]), _num(parts[4])
        if sv is None or not tv or tv <= 0:
            continue
        out[parts[1].strip().upper()] = (sv, tv)
    return out


# --------------------------------------------------------------------- network
def _default_get(url: str, timeout: float = 45.0) -> bytes:  # pragma: no cover - network
    headers = {"User-Agent": UA, "Accept-Encoding": "gzip"}
    if "cboe.com" in url:
        headers.update(CBOE_HEADERS)
    req = urllib.request.Request(url, headers=headers)
    with urllib.request.urlopen(req, timeout=timeout) as r:
        raw = r.read()
        return gzip.decompress(raw) if r.headers.get("Content-Encoding") == "gzip" else raw


def cboe_symbol(ticker: str) -> str:
    """Cboe writes class shares without the dot (BRK.B -> BRKB)."""
    return ticker.replace(".", "").replace("-", "").upper()


def miss_reason(exc: BaseException) -> str:
    """Why one chain could not be read, in a word the log can count (never the symbol)."""
    code = getattr(exc, "code", None)
    text = str(exc)
    if isinstance(code, int) or re.search(r"HTTP Error \d{3}", text):
        n = code if isinstance(code, int) else int(re.search(r"HTTP Error (\d{3})", text).group(1))
        return f"http {n}" if n in (403, 404, 429) else "other http"
    if isinstance(exc, (ValueError, KeyError, TypeError)):
        return "unreadable"
    return "network"


def read_chain(ticker: str, get: Callable[[str], bytes], sleep: Callable[[float], None],
               misses: Optional[List[str]] = None) -> Optional[Dict[str, Any]]:
    why = "network"
    for attempt in range(2):
        try:
            return chain_features(json.loads(get(CBOE_URL.format(sym=cboe_symbol(ticker)))), cboe_symbol(ticker))
        except Exception as exc:  # noqa: BLE001 - one name never stops the others
            why = miss_reason(exc)
            if why in ("http 403", "http 404"):
                break
            sleep(2.0 * (attempt + 1))
    if misses is not None:
        misses.append(why)
    return None


def read_finra(day: _dt.date, get: Callable[[str], bytes]) -> Tuple[Optional[_dt.date], Dict[str, Tuple[float, float]]]:
    """The newest FINRA file on or before `day` (it may not be posted yet the same evening)."""
    for back in range(0, 6):
        d = day - _dt.timedelta(days=back)
        if d.weekday() >= 5:
            continue
        try:
            got = parse_finra(get(FINRA_URL.format(ymd=d.strftime("%Y%m%d"))).decode("utf-8", "replace"))
        except Exception:  # noqa: BLE001
            got = {}
        if got:
            return d, got
    return None, {}


# --------------------------------------------------------------------- BigQuery
def _ms_module():  # pragma: no cover - thin loader
    path = Path(__file__).resolve().parent / "archive_market_series.py"
    spec = importlib.util.spec_from_file_location("_tfb_market_series", path)
    mod = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(mod)
    return mod


def make_session():  # pragma: no cover - needs google-auth and a real key
    return _ms_module().make_session()


UNIVERSE_SQL = """
WITH u AS (
  SELECT symbol, ANY_VALUE(market_cap) AS cap,
    LOGICAL_OR(investability_status = 'INVESTABLE' OR recommendation IN ('STRONG_BUY', 'BUY', 'ACCUMULATE')) AS rated
  FROM `{p}.{d}.v_universe_latest`
  WHERE market_code = 'US' AND source_page IN ('Global_Markets', 'Market_Leaders') AND current_price >= 2
  GROUP BY symbol),
h AS (
  SELECT DISTINCT symbol FROM `{p}.{d}.v_my_portfolio`
  WHERE snapshot_date = (SELECT MAX(snapshot_date) FROM `{p}.{d}.v_my_portfolio`) AND ENDS_WITH(symbol, '.US'))
SELECT symbol, why FROM (
  SELECT symbol, 'held' AS why, 0 AS o, 0 AS cap FROM h
  UNION ALL SELECT symbol, 'rated', 1, IFNULL(cap, 0) FROM u WHERE rated
  UNION ALL SELECT symbol, 'large', 2, cap FROM (SELECT symbol, cap FROM u WHERE cap >= 2e9 ORDER BY cap DESC LIMIT {n}))
QUALIFY ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY o) = 1
ORDER BY o, cap DESC
"""


def bq_rows(session: Any, project: str, location: str, sql: str) -> List[Dict[str, Any]]:
    ms = _ms_module()
    resp = session.post(f"{ms.BQ_API}/projects/{project}/queries", timeout=120,
                        json={"query": sql, "useLegacySql": False, "location": location, "timeoutMs": 110000})
    if resp.status_code != 200:
        raise RuntimeError(f"query: {ms._err(resp)}")
    body = resp.json() or {}
    if not body.get("jobComplete", True):
        raise RuntimeError("query did not finish in time")
    names = [f["name"] for f in (body.get("schema") or {}).get("fields") or []]
    return [{n: (c or {}).get("v") for n, c in zip(names, r.get("f") or [])} for r in body.get("rows") or []]


def universe(session: Any, project: str, dataset: str, location: str, n: int) -> List[Tuple[str, bool]]:
    """[(TFB symbol or ETF ticker, is_etf)] - holdings first, then rated names, then the largest."""
    rows = bq_rows(session, project, location, UNIVERSE_SQL.format(p=project, d=dataset, n=int(n)))
    seen, out = set(), []
    for r in rows:
        s = str(r.get("symbol") or "")
        if s and s not in seen:
            seen.add(s)
            out.append((s, False))
    return out + [(e, True) for e in ETFS if e not in seen]


def ticker_of(symbol: str) -> str:
    return symbol[:-3] if symbol.endswith(".US") else symbol


def schema_problems(table: Dict[str, Any]) -> List[str]:
    want = [(f["name"], f["type"], f["mode"]) for f in SCHEMA]
    alias = {"FLOAT64": "FLOAT", "INT64": "INTEGER", "BOOL": "BOOLEAN"}
    got = [(f.get("name"), alias.get(str(f.get("type")).upper(), str(f.get("type")).upper()), str(f.get("mode") or "NULLABLE").upper())
           for f in ((table.get("schema") or {}).get("fields") or [])]
    problems = []
    if got != want:
        problems.append("columns differ: " + "; ".join(f"{w} != {g}" for w, g in zip(want, got) if w != g)[:400]
                        if len(got) == len(want) else f"{len(got)} columns, want {len(want)}")
    tp = table.get("timePartitioning") or {}
    if tp.get("field") != "snapshot_date" or tp.get("type") != "DAY":
        problems.append("not day-partitioned on snapshot_date")
    return problems


def ensure_table(session: Any, project: str, dataset: str) -> bool:
    ms = _ms_module()
    base = f"{ms.BQ_API}/projects/{project}/datasets/{dataset}/tables"
    resp = session.get(f"{base}/{TABLE}", timeout=60)
    if resp.status_code == 200:
        problems = schema_problems(resp.json() or {})
        if problems:
            raise RuntimeError(f"table {TABLE} differs from SCHEMA: {' | '.join(problems)}")
        return False
    if resp.status_code != 404:
        raise RuntimeError(f"table {TABLE}: {ms._err(resp)}")
    body = {"tableReference": {"projectId": project, "datasetId": dataset, "tableId": TABLE},
            "schema": {"fields": SCHEMA},
            "timePartitioning": {"type": "DAY", "field": "snapshot_date"},
            "description": ("TFB daily options reading of US stocks and ETFs (Cboe delayed chains) and the FINRA "
                            "short sale volume; one partition per trading day, replaced on each run by "
                            "scripts/archive_options_reading.py. Research only.")}
    resp = session.post(base, json=body, timeout=60)
    if resp.status_code not in (200, 201):
        raise RuntimeError(f"create {TABLE}: {ms._err(resp)}")
    return True


def load_day(session: Any, project: str, dataset: str, location: str, day: str, data: bytes, run_id: str,
             sleep: Callable[[float], None] = time.sleep) -> Dict[str, Any]:
    """Replace one day's partition (options_daily$YYYYMMDD) with `data`."""
    ms = _ms_module()
    job_id = "tfb_options_%s_%s_%s" % (day.replace("-", ""), re.sub(r"[^0-9A-Za-z_-]", "", run_id or "local") or "local",
                                       uuid.uuid4().hex[:8])
    config = {"jobReference": {"projectId": project, "jobId": job_id, "location": location},
              "configuration": {"load": {
                  "destinationTable": {"projectId": project, "datasetId": dataset,
                                       "tableId": f"{TABLE}${day.replace('-', '')}"},
                  "sourceFormat": "NEWLINE_DELIMITED_JSON", "writeDisposition": "WRITE_TRUNCATE",
                  "createDisposition": "CREATE_NEVER", "schema": {"fields": SCHEMA}, "maxBadRecords": 0},
                  "labels": {"tfb": "options-reading"}}}
    start = session.post(f"{ms.BQ_UPLOAD}/projects/{project}/jobs?uploadType=resumable", json=config, timeout=60,
                         headers={"X-Upload-Content-Type": "application/octet-stream",
                                  "X-Upload-Content-Length": str(len(data))})
    if start.status_code not in (200, 201):
        raise RuntimeError(f"load: start {ms._err(start)}")
    headers = getattr(start, "headers", {}) or {}
    url = headers.get("Location") or headers.get("location")
    if not url:
        raise RuntimeError("load: no upload session")
    put = session.put(url, data=data, timeout=300, headers={"Content-Type": "application/octet-stream"})
    if put.status_code not in (200, 201):
        raise RuntimeError(f"load: upload {ms._err(put)}")
    job = put.json() or {}
    for _ in range(150):
        status = job.get("status") or {}
        if status.get("state") == "DONE":
            if status.get("errorResult"):
                raise RuntimeError(f"load: {status['errorResult'].get('message', 'job failed')[:300]}")
            return job
        sleep(2.0)
        resp = session.get(f"{ms.BQ_API}/projects/{project}/jobs/{job_id}?location={location}", timeout=60)
        if resp.status_code != 200:
            raise RuntimeError(f"load: poll {ms._err(resp)}")
        job = resp.json() or {}
    raise RuntimeError(f"load: job {job_id} not done after 300 s")


# --------------------------------------------------------------------- the run
def build_rows(names: Sequence[Tuple[str, bool]], chains: Dict[str, Optional[Dict[str, Any]]],
               finra_day: Optional[_dt.date], finra: Dict[str, Tuple[float, float]],
               captured_at: str, run_id: str) -> Tuple[Optional[str], List[Dict[str, Any]], int]:
    """(the trading day, the rows of that day, rows left out for belonging to another day)."""
    days: Dict[str, int] = {}
    for f in chains.values():
        if f:
            days[f["snapshot_date"]] = days.get(f["snapshot_date"], 0) + 1
    if not days:
        return None, [], 0
    day = max(days, key=lambda d: (days[d], d))
    rows, other = [], 0
    for sym, is_etf in names:
        f = chains.get(sym)
        if not f:
            continue
        if f["snapshot_date"] != day:
            other += 1
            continue
        t = ticker_of(sym)
        row = {"symbol": sym, "underlying": cboe_symbol(t), "is_etf": bool(is_etf)}
        row.update(f)
        sv = finra.get(t.upper()) or finra.get(cboe_symbol(t))
        row.update({"short_volume": sv[0] if sv else None, "short_total_volume": sv[1] if sv else None,
                    "short_ratio_pct": _r(sv[0] / sv[1] * 100.0) if sv else None,
                    "short_date": finra_day.isoformat() if sv and finra_day else None,
                    "source": SOURCE, "captured_at": captured_at, "run_id": run_id or None})
        rows.append(row)
    return day, rows, other


def run(session: Any, *, project: str, dataset: str, location: str, names: Sequence[Tuple[str, bool]],
        write: bool, now: _dt.datetime, run_id: str, get: Callable[[str], bytes] = _default_get,
        sleep: Callable[[float], None] = time.sleep, workers: int = WORKERS) -> int:
    ms = _ms_module()
    try:
        if not ms.dataset_exists(session, project, dataset):
            _out(f"ERROR: dataset {project}.{dataset} not found")
            return 2
        created = ensure_table(session, project, dataset) if write else False
    except Exception as exc:  # noqa: BLE001
        _out(f"ERROR: {str(exc)[:400]}")
        return 2
    chains: Dict[str, Optional[Dict[str, Any]]] = {}
    misses: List[str] = []
    with _cf.ThreadPoolExecutor(max_workers=max(1, workers)) as pool:
        futs = {pool.submit(read_chain, ticker_of(s), get, sleep, misses): s for s, _ in names}
        for fut in _cf.as_completed(futs):
            try:
                chains[futs[fut]] = fut.result()
            except Exception:  # noqa: BLE001
                chains[futs[fut]] = None
    got = sum(1 for v in chains.values() if v)
    day_guess = max((v["snapshot_date"] for v in chains.values() if v), default=None)
    finra_day, finra = read_finra(_dt.date.fromisoformat(day_guess) if day_guess else now.date(), get)
    captured_at = now.astimezone(_dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")
    day, rows, other = build_rows(names, chains, finra_day, finra, captured_at, run_id)
    with_short = sum(1 for r in rows if r.get("short_volume") is not None)
    summary = (f"{got}/{len(names)} chains read; {len(rows)} rows for {day or '-'}"
               + (f"; {other} left out (another day)" if other else "")
               + f"; FINRA short volume for {with_short} ({finra_day or 'no file found'})")
    if misses:
        counts: Dict[str, int] = {}
        for m in misses:
            counts[m] = counts.get(m, 0) + 1
        summary += "; misses: " + ", ".join(f"{k}={v}" for k, v in sorted(counts.items()))
    if got < len(names) / 2 or not rows:
        print(f"[options_reading v{__version__}] FAILED: {summary}")
        return 1
    if not write:
        print(f"[options_reading v{__version__}] {summary} | dry run, nothing loaded")
        return 0
    try:
        data = b"".join(json.dumps(r, separators=(",", ":")).encode() + b"\n" for r in rows)
        job = load_day(session, project, dataset, location, day, data, run_id, sleep=sleep)
    except Exception as exc:  # noqa: BLE001
        print(f"[options_reading v{__version__}] FAILED to load: {str(exc)[:300]} | {summary}")
        return 1
    loaded = ((job.get("statistics") or {}).get("load") or {}).get("outputRows", str(len(rows)))
    print(f"[options_reading v{__version__}] {summary} | loaded {loaded}" + (" | table created" if created else ""))
    return 0


def main(argv: Optional[Sequence[str]] = None, session: Any = None, now: Optional[_dt.datetime] = None,
         get: Optional[Callable[[str], bytes]] = None, sleep: Callable[[float], None] = time.sleep) -> int:
    ap = argparse.ArgumentParser(description="TFB options reading -> BigQuery")
    mode = ap.add_mutually_exclusive_group()
    mode.add_argument("--write", action="store_true")
    mode.add_argument("--dry-run", action="store_true")
    ap.add_argument("--max", type=int, default=None, help="largest names read beside the rated and held ones")
    ap.add_argument("--symbols", default="", help="read only these tickers (comma-separated)")
    args = ap.parse_args(list(argv) if argv is not None else None)
    if not enabled():
        _out("TFB_BQ_OPTIONS is off - nothing read, nothing written")
        return 0
    project, dataset, location = _env("TFB_BQ_PROJECT"), _env("TFB_BQ_DATASET", "tfb_archive"), _env("TFB_BQ_LOCATION", "US")
    if not project:
        _out("ERROR: TFB_BQ_PROJECT is required")
        return 2
    n = args.max if args.max is not None else int(_env("TFB_OPTIONS_MAX", str(DEFAULT_MAX)) or DEFAULT_MAX)
    if not (1 <= n <= 3000):
        _out(f"ERROR: --max must be 1..3000, got {n}")
        return 2
    session = session if session is not None else make_session()
    if args.symbols.strip():
        picks = [s.strip().upper() for s in args.symbols.split(",") if s.strip()]
        names = [(s, True) if s in ETFS else (s if s.endswith(".US") else s + ".US", False) for s in picks]
    else:
        try:
            names = universe(session, project, dataset, location, n)
        except Exception as exc:  # noqa: BLE001
            _out(f"ERROR: the list of names could not be read: {str(exc)[:300]}")
            return 2
    _out(f"reading {len(names)} names ({sum(1 for _, e in names if e)} ETFs)")
    return run(session, project=project, dataset=dataset, location=location, names=names,
               write=bool(args.write), now=now or _dt.datetime.now(_dt.timezone.utc),
               run_id=_env("GITHUB_RUN_ID"), get=get or _default_get, sleep=sleep)


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
