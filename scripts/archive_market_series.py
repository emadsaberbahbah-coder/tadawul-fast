#!/usr/bin/env python3
# scripts/archive_market_series.py
"""
================================================================================
TFB Market Series - v1.1.1 (program register C8, Phase 2b: global markets page)
================================================================================
v1.1.1 (29 Oct 2026, review of #880): the 30-bar minimum is a history test
for the nightly 1,100-day window, but a --days 30 run (the smallest window
the job accepts) holds only 20-22 trading days, so every complete answer was
refused and the run failed. The minimum is now window-aware, min_bars(p1,
p2): MIN_BARS, or half the trading days a window of that length holds (5 of
every 7 calendar days, holidays aside) when that is smaller, never below 2.
fetch_series (when to rebuild from hourly bars) and _one (when to refuse a
series) use the same number; the nightly window still needs MIN_BARS = 30.
v1.1.0 (29 Oct 2026, first live load): Yahoo serves ONE daily bar for
^TASI.SR and 000300.SS (every range, both hosts - their daily history is
missing upstream) while their hourly history is complete, so the first fill
loaded 47/48 series and CSI 300 with a single day. Now a series whose daily
answer has fewer than MIN_BARS bars is rebuilt from Yahoo's hourly bars
(range=730d, ~2.9 years): one row per venue-local trading day - first open,
highest high, lowest low, LAST hourly close, summed volume - tagged
source=yahoo_chart_v8_1h. On TASI the last hourly close is the price before
the closing auction (typically within a few tenths of a percent of the
official close). And a series that still has fewer than MIN_BARS bars is a
failure that keeps its rows, never a WRITE_TRUNCATE down to one day.
WHY (owner request 29 Oct 2026): the first Looker Studio page must show how
the world's markets move - the US, Europe, China, Japan and the Gulf - day by
day: the trend over the year, the average return of each market, one market
alone or several side by side, sectors, and any period (a day, a week, a
month). The workbook holds instrument snapshots, not index histories, so
this job keeps a small daily history of reference series in BigQuery.

WHAT: for every series in SERIES the job reads the daily bars from the Yahoo
chart API (no key, no EODHD quota - the same source score_selection_log uses
for the board scorecard) for the last --days days, and loads them into
<dataset>.market_series_daily, one integer-range partition per series_id,
WRITE_TRUNCATE. A series that cannot be read today keeps the rows it already
has (its partition is not touched); every other series is refreshed; a
failed series is tried once more at the end. A bar of a session still open
when the job runs is left out (the next run loads it once final). Rows carry
the series' name, region, kind, currency and sector, so the views need no
join. NOTHING is written to the workbook; the live system never reads
BigQuery; no decision, gate or count depends on this table.

SERIES kinds: "Index" (local currency, price index), "Market ETF (USD)",
"US sector ETF (USD)" and "World sector ETF (USD)" (one currency for a fair
comparison; dividend-adjusted closes, so total return - SAR is pegged to the
dollar), "Commodity (USD)" (front-month futures: Brent, WTI, gold).
series_id values are permanent: a retired series keeps its id, a new one
takes the next free id (1..1000).

GATE: TFB_BQ_MARKETS = on|1|true|yes arms the job (default off here - the
workflow arms it; kill with the repository Variable TFB_BQ_MARKETS=off).
Without --write it is a dry run: it reads Yahoo and prints what it would
load, and touches BigQuery only to check the dataset exists.
ENV: TFB_BQ_PROJECT, TFB_BQ_DATASET (default tfb_archive), TFB_BQ_LOCATION
(default US), GOOGLE_SHEETS_CREDENTIALS (the archive's service account:
BigQuery Data Editor + Job User), GITHUB_RUN_ID.
CLI: --write | --dry-run (default) ; --days N (default 1100, 30..4000) ;
--symbols A,B (a subset of SERIES by symbol).
Exit: 0 every series loaded, 1 one or more series failed (the others are
loaded), 2 configuration missing or the table differs from SCHEMA.

Not a licensed financial adviser; the owner decides and executes every trade.
================================================================================
"""
from __future__ import annotations

import argparse
import datetime as _dt
import importlib.util
import json
import os
import re
import sys
import time
import urllib.parse
import urllib.request
import uuid
from pathlib import Path
from typing import Any, Callable, Dict, Iterable, List, Optional, Sequence, Tuple

__version__ = "1.1.1"

TABLE = "market_series_daily"
DEFAULT_DAYS = 1100
MIN_DAYS, MAX_DAYS = 30, 4000
MAX_SERIES_ID = 1000
YAHOO_HOSTS = ("query1.finance.yahoo.com", "query2.finance.yahoo.com")
YAHOO_PATH = ("/v8/finance/chart/{sym}?period1={p1}&period2={p2}"
              "&interval=1d&events=div%2Csplits&includeAdjustedClose=true")
YAHOO_HOURLY_PATH = "/v8/finance/chart/{sym}?range=730d&interval=1h"
YAHOO_UA = "Mozilla/5.0 (compatible; TFB-market-series/1.1)"
# fewer bars than this is not a history: rebuild from hourly bars, else fail
# (the bar for a long window; a short one needs fewer - see min_bars)
MIN_BARS = 30
BQ_API = "https://bigquery.googleapis.com/bigquery/v2"
BQ_UPLOAD = "https://bigquery.googleapis.com/upload/bigquery/v2"

KINDS = ("Index", "Market ETF (USD)", "US sector ETF (USD)", "World sector ETF (USD)",
         "Commodity (USD)")
REGIONS = ("US", "Europe", "China", "Japan", "Gulf", "World", "Commodities")

# (series_id, yahoo symbol, display name, region, country, kind, currency, sector)
SERIES: Tuple[Tuple[int, str, str, str, str, str, str, str], ...] = (
    (1, "^GSPC", "S&P 500", "US", "United States", "Index", "USD", ""),
    (2, "^IXIC", "Nasdaq Composite", "US", "United States", "Index", "USD", ""),
    (3, "^DJI", "Dow Jones Industrial Average", "US", "United States", "Index", "USD", ""),
    (4, "^STOXX50E", "Euro Stoxx 50", "Europe", "Eurozone", "Index", "EUR", ""),
    (5, "^FTSE", "FTSE 100", "Europe", "United Kingdom", "Index", "GBP", ""),
    (6, "^GDAXI", "DAX 40", "Europe", "Germany", "Index", "EUR", ""),
    (7, "^FCHI", "CAC 40", "Europe", "France", "Index", "EUR", ""),
    (8, "000001.SS", "Shanghai Composite", "China", "China", "Index", "CNY", ""),
    (9, "399001.SZ", "Shenzhen Component", "China", "China", "Index", "CNY", ""),
    (10, "000300.SS", "CSI 300", "China", "China", "Index", "CNY", ""),
    (11, "^HSI", "Hang Seng", "China", "Hong Kong", "Index", "HKD", ""),
    (12, "^N225", "Nikkei 225", "Japan", "Japan", "Index", "JPY", ""),
    (13, "^TASI.SR", "Tadawul All Share (TASI)", "Gulf", "Saudi Arabia", "Index", "SAR", ""),
    (20, "SPY", "US market ETF (SPY)", "US", "United States", "Market ETF (USD)", "USD", ""),
    (21, "VGK", "Europe ETF (VGK)", "Europe", "Europe", "Market ETF (USD)", "USD", ""),
    (22, "MCHI", "China ETF (MCHI)", "China", "China", "Market ETF (USD)", "USD", ""),
    (23, "EWJ", "Japan ETF (EWJ)", "Japan", "Japan", "Market ETF (USD)", "USD", ""),
    (24, "KSA", "Saudi Arabia ETF (KSA)", "Gulf", "Saudi Arabia", "Market ETF (USD)", "USD", ""),
    (25, "UAE", "UAE ETF (UAE)", "Gulf", "United Arab Emirates", "Market ETF (USD)", "USD", ""),
    (26, "QAT", "Qatar ETF (QAT)", "Gulf", "Qatar", "Market ETF (USD)", "USD", ""),
    (27, "KWT", "Kuwait ETF (KWT)", "Gulf", "Kuwait", "Market ETF (USD)", "USD", ""),
    (28, "ACWI", "World ETF (ACWI)", "World", "World", "Market ETF (USD)", "USD", ""),
    (29, "EEM", "Emerging markets ETF (EEM)", "World", "Emerging markets", "Market ETF (USD)", "USD", ""),
    (40, "XLK", "US Technology (XLK)", "US", "United States", "US sector ETF (USD)", "USD", "Technology"),
    (41, "XLF", "US Financials (XLF)", "US", "United States", "US sector ETF (USD)", "USD", "Financials"),
    (42, "XLE", "US Energy (XLE)", "US", "United States", "US sector ETF (USD)", "USD", "Energy"),
    (43, "XLV", "US Health Care (XLV)", "US", "United States", "US sector ETF (USD)", "USD", "Health Care"),
    (44, "XLI", "US Industrials (XLI)", "US", "United States", "US sector ETF (USD)", "USD", "Industrials"),
    (45, "XLY", "US Consumer Discretionary (XLY)", "US", "United States", "US sector ETF (USD)", "USD",
     "Consumer Discretionary"),
    (46, "XLP", "US Consumer Staples (XLP)", "US", "United States", "US sector ETF (USD)", "USD",
     "Consumer Staples"),
    (47, "XLU", "US Utilities (XLU)", "US", "United States", "US sector ETF (USD)", "USD", "Utilities"),
    (48, "XLB", "US Materials (XLB)", "US", "United States", "US sector ETF (USD)", "USD", "Materials"),
    (49, "XLRE", "US Real Estate (XLRE)", "US", "United States", "US sector ETF (USD)", "USD", "Real Estate"),
    (50, "XLC", "US Communication Services (XLC)", "US", "United States", "US sector ETF (USD)", "USD",
     "Communication Services"),
    (60, "BZ=F", "Brent crude (front month)", "Commodities", "", "Commodity (USD)", "USD", "Energy"),
    (61, "CL=F", "WTI crude (front month)", "Commodities", "", "Commodity (USD)", "USD", "Energy"),
    (62, "GC=F", "Gold (front month)", "Commodities", "", "Commodity (USD)", "USD", "Precious metals"),
    (70, "IXN", "World Technology (IXN)", "World", "World", "World sector ETF (USD)", "USD", "Technology"),
    (71, "IXG", "World Financials (IXG)", "World", "World", "World sector ETF (USD)", "USD", "Financials"),
    (72, "IXC", "World Energy (IXC)", "World", "World", "World sector ETF (USD)", "USD", "Energy"),
    (73, "IXJ", "World Health Care (IXJ)", "World", "World", "World sector ETF (USD)", "USD", "Health Care"),
    (74, "EXI", "World Industrials (EXI)", "World", "World", "World sector ETF (USD)", "USD", "Industrials"),
    (75, "RXI", "World Consumer Discretionary (RXI)", "World", "World", "World sector ETF (USD)", "USD",
     "Consumer Discretionary"),
    (76, "KXI", "World Consumer Staples (KXI)", "World", "World", "World sector ETF (USD)", "USD",
     "Consumer Staples"),
    (77, "JXI", "World Utilities (JXI)", "World", "World", "World sector ETF (USD)", "USD", "Utilities"),
    (78, "MXI", "World Materials (MXI)", "World", "World", "World sector ETF (USD)", "USD", "Materials"),
    (79, "REET", "World Real Estate (REET)", "World", "World", "World sector ETF (USD)", "USD", "Real Estate"),
    (80, "IXP", "World Communication Services (IXP)", "World", "World", "World sector ETF (USD)", "USD",
     "Communication Services"),
)

SCHEMA: List[Dict[str, str]] = [
    {"name": "series_id", "type": "INTEGER", "mode": "REQUIRED", "description": "permanent id (one partition per series)"},
    {"name": "symbol", "type": "STRING", "mode": "REQUIRED", "description": "Yahoo symbol"},
    {"name": "name", "type": "STRING", "mode": "REQUIRED", "description": "display name"},
    {"name": "region", "type": "STRING", "mode": "REQUIRED", "description": "US | Europe | China | Japan | Gulf | World | Commodities"},
    {"name": "country", "type": "STRING", "mode": "NULLABLE", "description": "country or area"},
    {"name": "kind", "type": "STRING", "mode": "REQUIRED", "description": "Index | Market ETF (USD) | US sector ETF (USD) | World sector ETF (USD) | Commodity (USD)"},
    {"name": "currency", "type": "STRING", "mode": "REQUIRED", "description": "currency of the prices"},
    {"name": "sector", "type": "STRING", "mode": "NULLABLE", "description": "sector of a sector ETF or commodity"},
    {"name": "date", "type": "DATE", "mode": "REQUIRED", "description": "trading date on the venue's own calendar"},
    {"name": "open", "type": "FLOAT", "mode": "NULLABLE", "description": "open"},
    {"name": "high", "type": "FLOAT", "mode": "NULLABLE", "description": "high"},
    {"name": "low", "type": "FLOAT", "mode": "NULLABLE", "description": "low"},
    {"name": "close", "type": "FLOAT", "mode": "REQUIRED", "description": "close (price, not dividend-adjusted)"},
    {"name": "adj_close", "type": "FLOAT", "mode": "NULLABLE", "description": "close adjusted for dividends and splits"},
    {"name": "volume", "type": "INTEGER", "mode": "NULLABLE", "description": "volume (0 or null for most indices)"},
    {"name": "source", "type": "STRING", "mode": "REQUIRED", "description": "yahoo_chart_v8"},
    {"name": "captured_at", "type": "TIMESTAMP", "mode": "REQUIRED", "description": "when this job read the bars"},
    {"name": "run_id", "type": "STRING", "mode": "NULLABLE", "description": "GitHub Actions run id"},
]
_TYPE_ALIASES = {"INT64": "INTEGER", "FLOAT64": "FLOAT", "BOOL": "BOOLEAN"}


def _out(msg: str) -> None:
    sys.stderr.write(f"[market_series v{__version__}] {msg}\n")


def _env(name: str, default: str = "") -> str:
    return (os.getenv(name) or default).strip()


def markets_enabled() -> bool:
    return _env("TFB_BQ_MARKETS").lower() in ("on", "1", "true", "yes")


def series_records(symbols: str = "") -> List[Dict[str, Any]]:
    keys = ("series_id", "symbol", "name", "region", "country", "kind", "currency", "sector")
    recs = [dict(zip(keys, s)) for s in SERIES]
    want = [x.strip() for x in (symbols or "").split(",") if x.strip()]
    if not want:
        return recs
    known = {r["symbol"]: r for r in recs}
    missing = [w for w in want if w not in known]
    if missing:
        raise ValueError(f"unknown symbol(s): {', '.join(missing)}")
    return [known[w] for w in want]


def _num(v: Any) -> Optional[float]:
    try:
        f = float(v)
    except (TypeError, ValueError):
        return None
    return f if f == f and f not in (float("inf"), float("-inf")) else None


def _live_session_start(meta: Dict[str, Any], now_ts: Optional[int]) -> Optional[int]:
    """Start of the regular session that is open at now_ts, else None."""
    if now_ts is None:
        return None
    try:
        reg = (meta.get("currentTradingPeriod") or {}).get("regular") or {}
        start, end = int(reg["start"]), int(reg["end"])
    except (KeyError, TypeError, ValueError, AttributeError):
        return None
    return start if start <= int(now_ts) < end else None


def parse_chart(payload: Any, now_ts: Optional[int] = None) -> List[Dict[str, Any]]:
    """Yahoo v8 chart JSON -> [{date, open, high, low, close, adj_close,
    volume}] sorted by date. The date is the venue-local trading day
    (timestamp + meta.gmtoffset); a bar without a positive close is skipped;
    a repeated date keeps the LAST bar; when now_ts falls inside the regular
    session of meta.currentTradingPeriod, the bars of that open session are
    left out (they are not final yet)."""
    try:
        r0 = (payload["chart"]["result"] or [None])[0]
    except (KeyError, TypeError, IndexError):
        return []
    if not r0:
        return []
    meta = r0.get("meta") or {}
    try:
        off = int(meta.get("gmtoffset") or 0)
    except (TypeError, ValueError):
        off = 0
    live_from = _live_session_start(meta, now_ts)
    ind = r0.get("indicators") or {}
    try:
        q = (ind.get("quote") or [{}])[0] or {}
    except (TypeError, IndexError, AttributeError):
        q = {}
    try:
        adj = ((ind.get("adjclose") or [{}])[0] or {}).get("adjclose") or []
    except (TypeError, IndexError, AttributeError):
        adj = []
    ts = r0.get("timestamp") or []
    out: Dict[str, Dict[str, Any]] = {}
    for i, t in enumerate(ts):
        def at(seq: Sequence[Any]) -> Any:
            return seq[i] if i < len(seq) else None
        close = _num(at(q.get("close") or []))
        if close is None or not close > 0:
            continue
        try:
            t = int(t)
            if live_from is not None and t >= live_from:
                continue
            day = _dt.datetime.fromtimestamp(t + off, tz=_dt.timezone.utc).strftime("%Y-%m-%d")
        except (TypeError, ValueError, OverflowError, OSError):
            continue
        vol = _num(at(q.get("volume") or []))
        a = _num(at(adj))
        out[day] = {
            "date": day,
            "open": _num(at(q.get("open") or [])),
            "high": _num(at(q.get("high") or [])),
            "low": _num(at(q.get("low") or [])),
            "close": close,
            "adj_close": a if (a is not None and a > 0) else None,
            "volume": int(vol) if vol is not None and vol >= 0 else None,
        }
    return [out[d] for d in sorted(out)]


def parse_hourly(payload: Any, now_ts: Optional[int] = None) -> List[Dict[str, Any]]:
    """Yahoo v8 HOURLY chart JSON -> one daily bar per venue-local trading
    day: first open, highest high, lowest low, last close, summed volume
    (bars without a positive close skipped; the bars of a session still open
    at now_ts left out, as in parse_chart). adj_close is None (indices)."""
    try:
        r0 = (payload["chart"]["result"] or [None])[0]
    except (KeyError, TypeError, IndexError):
        return []
    if not r0:
        return []
    meta = r0.get("meta") or {}
    try:
        off = int(meta.get("gmtoffset") or 0)
    except (TypeError, ValueError):
        off = 0
    live_from = _live_session_start(meta, now_ts)
    try:
        q = ((r0.get("indicators") or {}).get("quote") or [{}])[0] or {}
    except (TypeError, IndexError, AttributeError):
        q = {}
    days: Dict[str, Dict[str, Any]] = {}
    for i, t in sorted(enumerate(r0.get("timestamp") or []), key=lambda it: (_num(it[1]) or 0)):
        def at(key: str) -> Optional[float]:
            seq = q.get(key) or []
            return _num(seq[i]) if i < len(seq) else None
        close = at("close")
        if close is None or not close > 0:
            continue
        try:
            t = int(t)
            if live_from is not None and t >= live_from:
                continue
            day = _dt.datetime.fromtimestamp(t + off, tz=_dt.timezone.utc).strftime("%Y-%m-%d")
        except (TypeError, ValueError, OverflowError, OSError):
            continue
        o, h, lo, v = at("open"), at("high"), at("low"), at("volume")
        d = days.get(day)
        if d is None:
            days[day] = {"date": day, "open": o, "high": h, "low": lo, "close": close, "adj_close": None,
                         "volume": int(v) if v is not None and v >= 0 else None,
                         "source": "yahoo_chart_v8_1h"}
            continue
        if d["open"] is None:
            d["open"] = o
        if h is not None:
            d["high"] = h if d["high"] is None else max(d["high"], h)
        if lo is not None:
            d["low"] = lo if d["low"] is None else min(d["low"], lo)
        d["close"] = close
        if v is not None and v >= 0:
            d["volume"] = int(v) + (d["volume"] or 0)
    return [days[d] for d in sorted(days)]


def _default_opener(req: urllib.request.Request, timeout: float):  # pragma: no cover - network
    return urllib.request.urlopen(req, timeout=timeout)


def _fetch(path: str, parse: Callable[..., List[Dict[str, Any]]], now_ts: Optional[int],
           opener: Callable[..., Any], sleep: Callable[[float], None]) -> List[Dict[str, Any]]:
    """query1 then query2; a 404 (unknown symbol) is not retried."""
    for host in YAHOO_HOSTS:
        req = urllib.request.Request("https://" + host + path,
                                     headers={"User-Agent": YAHOO_UA, "Accept": "application/json"})
        try:
            with opener(req, 30) as resp:
                bars = parse(json.loads(resp.read().decode("utf-8", "replace")), now_ts)
            if bars:
                return bars
        except Exception as exc:  # noqa: BLE001
            if getattr(exc, "code", None) == 404:
                return []
        sleep(1.5)
    return []


def min_bars(p1: int, p2: int) -> int:
    """The fewest bars that count as a history of the window p1..p2 (epoch
    seconds): MIN_BARS, or half the trading days a window of that length
    holds (5 of every 7 calendar days, holidays aside) when that is smaller.
    --days 30 (p2 one day past the run) gives 11 of its 20-22 trading days;
    the nightly 1,100 days give 30. Never below 2: the one bar Yahoo serves
    for ^TASI.SR and 000300.SS is never a history."""
    days = max(0, (int(p2) - int(p1)) // 86400)
    return max(2, min(MIN_BARS, days * 5 // 7 // 2))


def fetch_series(symbol: str, p1: int, p2: int, now_ts: Optional[int] = None,
                 opener: Callable[..., Any] = _default_opener,
                 sleep: Callable[[float], None] = time.sleep) -> List[Dict[str, Any]]:
    """Daily bars for one Yahoo symbol. When the daily answer has fewer than
    min_bars(p1, p2) bars, the series is rebuilt from hourly bars (from p1
    on) if that gives more. [] when nothing usable came back."""
    ysym = urllib.parse.quote(symbol, safe="")
    daily = _fetch(YAHOO_PATH.format(sym=ysym, p1=p1, p2=p2), parse_chart, now_ts, opener, sleep)
    if len(daily) >= min_bars(p1, p2):
        return daily
    first_day = _dt.datetime.fromtimestamp(int(p1), tz=_dt.timezone.utc).strftime("%Y-%m-%d")
    hourly = [b for b in _fetch(YAHOO_HOURLY_PATH.format(sym=ysym), parse_hourly, now_ts, opener, sleep)
              if b["date"] >= first_day]
    return hourly if len(hourly) > len(daily) else daily


def build_rows(rec: Dict[str, Any], bars: Iterable[Dict[str, Any]],
               captured_at: str, run_id: str) -> List[Dict[str, Any]]:
    rows = []
    for b in bars:
        row = {k: rec[k] for k in ("series_id", "symbol", "name", "region", "kind", "currency")}
        row["country"] = rec.get("country") or None
        row["sector"] = rec.get("sector") or None
        row.update({k: b.get(k) for k in ("date", "open", "high", "low", "close", "adj_close", "volume")})
        row.update({"source": b.get("source") or "yahoo_chart_v8", "captured_at": captured_at,
                    "run_id": run_id or None})
        rows.append(row)
    return rows


def ndjson(rows: Iterable[Dict[str, Any]]) -> bytes:
    return b"".join(json.dumps(r, ensure_ascii=False, separators=(",", ":")).encode("utf-8") + b"\n"
                    for r in rows)


# --------------------------------------------------------------------- BigQuery
def _archive_module():  # pragma: no cover - thin loader
    path = Path(__file__).resolve().parent / "archive_to_bigquery.py"
    spec = importlib.util.spec_from_file_location("_tfb_archive_to_bigquery", path)
    mod = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(mod)
    return mod


def make_session():  # pragma: no cover - needs google-auth and a real key
    return _archive_module().make_session()


def _err(resp: Any) -> str:
    try:
        msg = (((resp.json() or {}).get("error") or {}).get("message") or "")
    except Exception:
        msg = ""
    return f"HTTP {getattr(resp, 'status_code', '?')}" + (f": {msg[:300]}" if msg else "")


def dataset_exists(session: Any, project: str, dataset: str) -> bool:
    resp = session.get(f"{BQ_API}/projects/{project}/datasets/{dataset}", timeout=60)
    if resp.status_code == 200:
        return True
    if resp.status_code == 404:
        return False
    raise RuntimeError(f"dataset {project}.{dataset}: {_err(resp)}")


def schema_problems(table: Dict[str, Any]) -> List[str]:
    """[] when an existing table has SCHEMA's columns (name, type, mode, in
    order) and the series_id range partitioning the loads rely on."""
    want = [(f["name"], f["type"], f["mode"]) for f in SCHEMA]
    got = []
    for f in ((table.get("schema") or {}).get("fields") or []):
        t = str(f.get("type") or "").upper()
        got.append((f.get("name"), _TYPE_ALIASES.get(t, t), str(f.get("mode") or "NULLABLE").upper()))
    problems = []
    if got != want:
        diff = [f"{w} != {g}" for w, g in zip(want, got) if w != g]
        problems.append("columns differ: " + ("; ".join(diff[:5]) or f"{len(got)} columns, want {len(want)}"))
    rp = table.get("rangePartitioning") or {}
    rng = rp.get("range") or {}
    if (rp.get("field") != "series_id" or str(rng.get("start")) != "1"
            or str(rng.get("interval")) != "1" or int(rng.get("end") or 0) <= MAX_SERIES_ID):
        problems.append("not range-partitioned on series_id (start 1, interval 1, end > %d)" % MAX_SERIES_ID)
    return problems


def ensure_table(session: Any, project: str, dataset: str) -> bool:
    """True when the table was created now (integer-range partitioned on
    series_id, one partition per series), False when it existed with the
    right schema; RuntimeError when it exists with another layout."""
    base = f"{BQ_API}/projects/{project}/datasets/{dataset}/tables"
    resp = session.get(f"{base}/{TABLE}", timeout=60)
    if resp.status_code == 200:
        problems = schema_problems(resp.json() or {})
        if problems:
            raise RuntimeError(f"table {TABLE} differs from SCHEMA: {' | '.join(problems)}")
        return False
    if resp.status_code != 404:
        raise RuntimeError(f"table {TABLE}: {_err(resp)}")
    body = {
        "tableReference": {"projectId": project, "datasetId": dataset, "tableId": TABLE},
        "schema": {"fields": SCHEMA},
        "rangePartitioning": {"field": "series_id",
                              "range": {"start": "1", "end": str(MAX_SERIES_ID + 1), "interval": "1"}},
        "description": ("TFB daily bars of reference market series (indices, market and sector ETFs, "
                        "commodities) from the Yahoo chart API; one partition per series_id, replaced "
                        "on each run by scripts/archive_market_series.py."),
    }
    resp = session.post(base, json=body, timeout=60)
    if resp.status_code not in (200, 201):
        raise RuntimeError(f"create {TABLE}: {_err(resp)}")
    return True


def load_series(session: Any, project: str, dataset: str, location: str, series_id: int,
                data: bytes, run_id: str, poll_s: float = 2.0, timeout_s: float = 300.0,
                sleep: Callable[[float], None] = time.sleep,
                clock: Callable[[], float] = time.monotonic) -> Dict[str, Any]:
    """Replace one series' partition (table$<series_id>) with `data`."""
    job_id = "tfb_market_%d_%s_%s" % (series_id, re.sub(r"[^0-9A-Za-z_-]", "", run_id or "local") or "local",
                                      uuid.uuid4().hex[:8])
    config = {
        "jobReference": {"projectId": project, "jobId": job_id, "location": location},
        "configuration": {"load": {
            "destinationTable": {"projectId": project, "datasetId": dataset,
                                 "tableId": f"{TABLE}${series_id}"},
            "sourceFormat": "NEWLINE_DELIMITED_JSON",
            "writeDisposition": "WRITE_TRUNCATE",
            "createDisposition": "CREATE_NEVER",
            "schema": {"fields": SCHEMA},
            "maxBadRecords": 0,
        }, "labels": {"tfb": "market-series"}},
    }
    start = session.post(f"{BQ_UPLOAD}/projects/{project}/jobs?uploadType=resumable", json=config, timeout=60,
                         headers={"X-Upload-Content-Type": "application/octet-stream",
                                  "X-Upload-Content-Length": str(len(data))})
    if start.status_code not in (200, 201):
        raise RuntimeError(f"load {series_id}: start {_err(start)}")
    headers = getattr(start, "headers", {}) or {}
    upload_url = headers.get("Location") or headers.get("location")
    if not upload_url:
        raise RuntimeError(f"load {series_id}: no upload session")
    put = session.put(upload_url, data=data, timeout=300, headers={"Content-Type": "application/octet-stream"})
    if put.status_code not in (200, 201):
        raise RuntimeError(f"load {series_id}: upload {_err(put)}")
    deadline = clock() + timeout_s
    job = put.json() or {}
    while True:
        status = job.get("status") or {}
        if status.get("state") == "DONE":
            if status.get("errorResult"):
                raise RuntimeError(f"load {series_id}: {status['errorResult'].get('message', 'job failed')[:300]}")
            return job
        if clock() > deadline:
            raise RuntimeError(f"load {series_id}: job {job_id} not done after {int(timeout_s)}s")
        sleep(poll_s)
        resp = session.get(f"{BQ_API}/projects/{project}/jobs/{job_id}?location={location}", timeout=60)
        if resp.status_code != 200:
            raise RuntimeError(f"load {series_id}: poll {_err(resp)}")
        job = resp.json() or {}


def _one(session: Any, rec: Dict[str, Any], *, project: str, dataset: str, location: str,
         p1: int, p2: int, now_ts: int, captured_at: str, run_id: str, write: bool,
         fetch: Callable[..., List[Dict[str, Any]]], sleep: Callable[[float], None]) -> int:
    """Read and (with write) load one series; the number of bars. Raises on
    any failure so the caller can retry it and keep going."""
    bars = fetch(rec["symbol"], p1, p2, now_ts)
    if not bars:
        raise RuntimeError("no bars from Yahoo (series left as it was)")
    need = min_bars(p1, p2)
    if len(bars) < need:
        raise RuntimeError(f"only {len(bars)} bar(s) from Yahoo, daily and hourly, {need} needed for "
                           f"this window (series left as it was)")
    rows = build_rows(rec, bars, captured_at, run_id)
    span = f"{rows[0]['date']}..{rows[-1]['date']}" + (" (rebuilt from hourly bars)"
                                                         if rows[-1]["source"] == "yahoo_chart_v8_1h" else "")
    if not write:
        _out(f"{rec['symbol']:10s} {rec['name']}: {len(rows)} bars {span} | dry run")
        return len(rows)
    job = load_series(session, project, dataset, location, int(rec["series_id"]), ndjson(rows), run_id,
                      sleep=sleep)
    loaded = ((job.get("statistics") or {}).get("load") or {}).get("outputRows", str(len(rows)))
    _out(f"{rec['symbol']:10s} {rec['name']}: {len(rows)} bars {span} | loaded {loaded}")
    return len(rows)


def run(session: Any, *, project: str, dataset: str, location: str, records: Sequence[Dict[str, Any]],
        days: int, now: _dt.datetime, run_id: str, write: bool,
        fetch: Callable[..., List[Dict[str, Any]]] = fetch_series,
        pause: float = 0.3, retry_wait: float = 20.0,
        sleep: Callable[[float], None] = time.sleep) -> int:
    try:
        if not dataset_exists(session, project, dataset):
            _out(f"ERROR: dataset {project}.{dataset} not found")
            return 2
        created = ensure_table(session, project, dataset) if write else False
    except Exception as exc:  # noqa: BLE001
        _out(f"ERROR: {str(exc)[:400]}")
        return 2
    now_ts = int(now.timestamp())
    p2 = now_ts + 86400
    p1 = int((now - _dt.timedelta(days=days)).timestamp())
    captured_at = now.astimezone(_dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")
    kw = dict(project=project, dataset=dataset, location=location, p1=p1, p2=p2, now_ts=now_ts,
              captured_at=captured_at, run_id=run_id, write=write, fetch=fetch, sleep=sleep)
    total = 0
    failed: List[Dict[str, Any]] = []
    for rec in records:
        try:
            total += _one(session, rec, **kw)
        except Exception as exc:  # noqa: BLE001 - one series never stops the others
            failed.append(rec)
            _out(f"retry later {rec['symbol']}: {str(exc)[:300]}")
        sleep(pause)
    still: List[str] = []
    if failed:
        sleep(retry_wait)
        for rec in failed:
            try:
                total += _one(session, rec, **kw)
            except Exception as exc:  # noqa: BLE001
                still.append(rec["symbol"])
                _out(f"ERROR {rec['symbol']}: {str(exc)[:300]}")
            sleep(pause)
    print(f"[market_series v{__version__}] {len(records) - len(still)}/{len(records)} series, {total} bars, "
          f"last {days} days{'' if write else ' (dry run, nothing loaded)'}"
          + (" | table created" if created else "")
          + (f"; FAILED (left as they were): {', '.join(still)}" if still else ""))
    return 1 if still else 0


def main(argv: Optional[Sequence[str]] = None, session: Any = None,
         now: Optional[_dt.datetime] = None,
         fetch: Optional[Callable[..., List[Dict[str, Any]]]] = None,
         sleep: Callable[[float], None] = time.sleep) -> int:
    ap = argparse.ArgumentParser(description="TFB market series -> BigQuery")
    mode = ap.add_mutually_exclusive_group()
    mode.add_argument("--write", action="store_true", help="load into BigQuery")
    mode.add_argument("--dry-run", action="store_true", help="read Yahoo and report only (default)")
    ap.add_argument("--days", type=int, default=DEFAULT_DAYS, help=f"history window ({MIN_DAYS}..{MAX_DAYS})")
    ap.add_argument("--symbols", default="", help="comma-separated subset of the series symbols")
    args = ap.parse_args(list(argv) if argv is not None else None)
    if not markets_enabled():
        _out("TFB_BQ_MARKETS is off - nothing read, nothing written")
        return 0
    project = _env("TFB_BQ_PROJECT")
    dataset = _env("TFB_BQ_DATASET", "tfb_archive")
    location = _env("TFB_BQ_LOCATION", "US")
    if not project:
        _out("ERROR: TFB_BQ_PROJECT is required")
        return 2
    if not (MIN_DAYS <= int(args.days) <= MAX_DAYS):
        _out(f"ERROR: --days must be {MIN_DAYS}..{MAX_DAYS}, got {args.days}")
        return 2
    try:
        records = series_records(args.symbols)
    except ValueError as exc:
        _out(f"ERROR: {exc}")
        return 2
    now = now or _dt.datetime.now(_dt.timezone.utc)
    if session is None:
        try:
            session = make_session()
        except Exception as exc:  # noqa: BLE001
            _out(f"ERROR: {str(exc)[:300]}")
            return 2
    return run(session, project=project, dataset=dataset, location=location, records=records,
               days=int(args.days), now=now, run_id=_env("GITHUB_RUN_ID"), write=bool(args.write),
               fetch=fetch or fetch_series, sleep=sleep)


if __name__ == "__main__":
    sys.exit(main())
