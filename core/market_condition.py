#!/usr/bin/env python3
# core/market_condition.py
"""
================================================================================
TFB Market Condition - v1.0.0 (the condition of each stock market, live)
================================================================================
v1.0.0 (21 Nov 2026) - NEW. WHY (owner, Sat 21 Nov 2026 19:10 Riyadh: "MODEL
EVIDENCE 47.60% INVESTABLE hit rate, matured records ... since 2026-11-10:
11.1% (n 45) ... need to improve"). Measured that evening on the matured
Performance_Log records that tile reads (2,384 INVESTABLE WIN / LOSS records,
June to 21 Nov 2026), joined to the label the stock's market carried at its
last close before the record:
  - entered while the market was calm, 52.5% finished higher (1,150 records);
    on the records that matured 11-21 Nov alone, 58.9% (146) against 42.6%
    (383) for all INVESTABLE records;
  - Tadawul shares entered while TASI was in a drawdown: 31.5% (533 records,
    over a fifth of all INVESTABLE records); 40 of the 45 records since
    10 Nov were US shares entered while the US was weak, and 4 of them won;
  - the labels lived only in BigQuery (99_v_market_condition.sql, a research
    view the live system never reads), so no live verdict could use them.

WHAT. The same five labels, from the same rules, for the five stock markets
whose shares the engine labels (market id: the reading):
    us  SPY's price close and the VIX       sa  Tadawul All Share ^TASI.SR
    eu  STOXX Europe 600 ^STOXX             jp  Nikkei 225 ^N225
    cn  Hang Seng ^HSI
  k = 1 for the US; elsewhere the market's normal yearly volatility (the
  sample deviation of its last 250 daily changes, annualised, at least 120 of
  them) / 16. First match wins; a figure that is not known never matches:
    crash_day   the last close moved -2 k % or more
    drawdown    -3 k % or more over 5 closes, or 8 k % or more under the
                250-close high
    stress      20-close volatility (annualised) 25 k % or more; the US also
                the VIX at 25 or more
    weak        under the 50-close average
    calm        everything else
  Every figure and limit is rounded to 4 decimals before it is compared, as
  the view does. metrics() and classify() are pure and mirror the view line
  for line (tests pin both to the same cases).

READ. read() fetches each reading's daily bars from the Yahoo chart service
(two years), sets aside the bar of a session still open, and labels the last
COMPLETED session; a reading with fewer than MIN_DAILY daily bars is rebuilt
from hourly bars, one day per venue-local date with its last close (Yahoo
serves a single daily bar for ^TASI.SR; archive_market_series does the same).
A market whose last completed session is more than MAX_AGE_DAYS old is not
read. labels() keeps the read for TFB_MARKET_CONDITION_TTL_SEC (default 1800)
in this process and refreshes it in a background thread, so a verdict never
waits on the network: before the first read completes, and for a market that
could not be read for STALE_MAX_SEC, there is no label - and a gate that reads
an unknown market never holds it.

This module decides nothing by itself and writes nothing.

v1.0.1 (22 Nov 2026) - THE READ IS WARMED AND REPORTED. On the first deploy
(22 Nov 06:12 UTC, both gates on enforce) /health read market_labels {} for 16
minutes: a worker reads the labels only when an INVESTABLE row reaches the
market tier, and none had (Sunday, no page read since the boot), so the
read-back could not tell a quiet worker from a failing read. Now state() - the
engine's /health and startup line - also starts a refresh when one is due
(never waiting on it), so each worker reads at its first health line, and it
reports the last read: when, which markets were read, and for each one not
read why ("query1...: HTTP 429", "no bars", "last session 2026-11-10 is more
than 7 days old", "not enough closes for a label"). The Yahoo request carries
the browser User-Agent of yahoo_chart_provider's raw chart transport, the
one the engine already uses from Render. Labels and rules unchanged.
================================================================================
"""
from __future__ import annotations

import datetime as _dt
import json
import math
import os
import re
import threading
import time
import urllib.parse
import urllib.request
from typing import Any, Callable, Dict, List, Optional, Sequence, Tuple

__version__ = "1.0.1"

UTC = _dt.timezone.utc
LABELS = ("crash_day", "drawdown", "stress", "weak", "calm")
# market id -> (Yahoo symbol of the reading, what it is)
MARKETS: Dict[str, Tuple[str, str]] = {
    "us": ("SPY", "S&P 500 fund (SPY)"),
    "sa": ("^TASI.SR", "Tadawul All Share (TASI)"),
    "eu": ("^STOXX", "STOXX Europe 600"),
    "jp": ("^N225", "Nikkei 225"),
    "cn": ("^HSI", "Hang Seng"),
}
MARKET_NAMES = {"us": "the US market", "sa": "the Saudi market", "eu": "the European market",
                "jp": "the Japanese market", "cn": "the Hong Kong market"}
VIX = "^VIX"
YAHOO_HOSTS = ("query1.finance.yahoo.com", "query2.finance.yahoo.com")
# 1.0.1: yahoo_chart_provider's _RAW_CHART_UA (its raw chart transport, the one that answers from Render)
YAHOO_UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/124.0 Safari/537.36")
DAILY_PATH = "/v8/finance/chart/{sym}?range=2y&interval=1d"
HOURLY_PATH = "/v8/finance/chart/{sym}?range=730d&interval=1h"
MIN_DAILY = 30
MAX_AGE_DAYS = 7
TIMEOUT_SEC = 10.0
DEFAULT_TTL_SEC = 1800
STALE_MAX_SEC = 12 * 3600

# the exchange suffix of a page symbol -> the market id
_SUFFIX: Dict[str, str] = {"US": "us", "SR": "sa"}
_SUFFIX.update({s: "eu" for s in ("L", "LSE", "DE", "XETRA", "F", "PA", "AS", "MI", "MC", "SW", "BR", "ST", "HE",
                                   "CO", "OL", "LS", "IR", "VI")})
_SUFFIX.update({s: "jp" for s in ("T", "TSE", "JP")})
_SUFFIX.update({s: "cn" for s in ("HK", "SS", "SZ", "SHG", "SHE")})
_TADAWUL_BARE = re.compile(r"^[0-9]{4}$")
_US_BARE = re.compile(r"^[A-Z][A-Z0-9\-]{0,9}$")
# core.symbols.normalize: a US share class is LETTERS.LETTER (BRK.B, HEI.A; normalize_symbol turns BRK-B into
# BRK.B) unless the letter is one of its single-letter exchange suffixes (.F .L .N .T .V)
_US_SHARE_CLASS = re.compile(r"^[A-Z]{1,5}\.[A-Z]$")
_EXCHANGE_LETTERS = frozenset("FLNTV")


def market_of(symbol: Any) -> str:
    """The market id of a page symbol: '2222.SR' and a bare four-digit code -> 'sa'; 'EFC.US', a bare ticker
    ('GOOGL', 'BRK-B'; no suffix is the US, as v_outcome_engine reads it) and a US share class ('BRK.B') -> 'us';
    'BBOX.L' -> 'eu', '7203.T' -> 'jp', '0700.HK' -> 'cn'; any other suffix or shape ('PEO.WA', 'ABC.V', 'GC=F',
    '^N225') -> '' (no market)."""
    s = str(symbol or "").strip().upper()
    if not s:
        return ""
    if _TADAWUL_BARE.match(s):
        return "sa"
    if "." not in s:
        return "us" if _US_BARE.match(s) else ""
    suffix = s.rsplit(".", 1)[1]
    if suffix in _SUFFIX:
        return _SUFFIX[suffix]
    if _US_SHARE_CLASS.match(s) and suffix not in _EXCHANGE_LETTERS:
        return "us"
    return ""


# ------------------------------------------------------------------ the rules
def _num(v: Any) -> Optional[float]:
    try:
        f = float(v)
    except (TypeError, ValueError):
        return None
    return f if math.isfinite(f) else None


def _r4(x: Optional[float]) -> Optional[float]:
    return None if x is None else round(x, 4)


def _pct(a: float, b: Optional[float]) -> Optional[float]:
    return None if not b else (a / b - 1.0) * 100.0


def _sd(xs: Sequence[float]) -> Optional[float]:
    """The sample standard deviation (STDDEV_SAMP); None under two values."""
    n = len(xs)
    if n < 2:
        return None
    mu = sum(xs) / n
    return math.sqrt(sum((x - mu) ** 2 for x in xs) / (n - 1))


def metrics(closes: Sequence[float], vix: Optional[float], market_id: str) -> Dict[str, Optional[float]]:
    """The figures of 99_v_market_condition.sql for the last close in `closes` (oldest first)."""
    n = len(closes)
    if n == 0:
        return {"r1": None, "r5": None, "vs50": None, "vol20": None, "dd": None, "normal_vol": None, "vix": vix,
                "k": None}
    c = closes[-1]
    ch20 = [closes[i] / closes[i - 1] - 1.0 for i in range(max(1, n - 20), n)]
    ch250 = [closes[i] / closes[i - 1] - 1.0 for i in range(max(1, n - 250), n)]
    sd20 = _sd(ch20)
    sd250 = _sd(ch250)
    normal = _r4(sd250 * math.sqrt(252.0) * 100.0) if (sd250 is not None and len(ch250) >= 120) else None
    if market_id == "us":
        k: Optional[float] = 1.0
    else:
        k = round(normal / 16.0, 4) if normal is not None else None
    return {
        "r1": _r4(_pct(c, closes[-2])) if n >= 2 else None,
        "r5": _r4(_pct(c, closes[-6])) if n >= 6 else None,
        "vs50": _r4(_pct(c, sum(closes[-50:]) / 50.0)) if n >= 50 else None,
        "vol20": _r4(sd20 * math.sqrt(252.0) * 100.0) if (n >= 20 and sd20 is not None) else None,
        "dd": _r4(_pct(c, max(closes[-250:]))),
        "normal_vol": normal,
        "vix": vix if market_id == "us" else None,
        "k": k,
    }


def limits(k: float) -> Dict[str, float]:
    return {"r1": round(-2.0 * k, 4), "r5": round(-3.0 * k, 4), "dd": round(-8.0 * k, 4), "vol20": round(25.0 * k, 4)}


def classify(m: Dict[str, Optional[float]]) -> Optional[str]:
    """One label, first match wins; None when k is not known (as the view's NULL)."""
    k = m.get("k")
    if k is None:
        return None
    lim = limits(k)

    def le(key: str, x: float) -> bool:
        return m.get(key) is not None and m[key] <= x

    def ge(key: str, x: float) -> bool:
        return m.get(key) is not None and m[key] >= x

    if le("r1", lim["r1"]):
        return "crash_day"
    if le("r5", lim["r5"]) or le("dd", lim["dd"]):
        return "drawdown"
    if ge("vix", 25.0) or ge("vol20", lim["vol20"]):
        return "stress"
    if m.get("vs50") is not None and m["vs50"] < 0:
        return "weak"
    return "calm"


def describe(market_id: str, label: str, m: Dict[str, Optional[float]], day: Optional[_dt.date]) -> str:
    """'the Saudi market in a drawdown as of 2026-11-20 (TASI 9.3% under its 250-close high)'."""
    name = MARKET_NAMES.get(market_id, market_id)
    reading = MARKETS.get(market_id, ("", market_id))[1].split(" (")[0]
    k = m.get("k")
    lim = limits(k) if k is not None else {}
    why: List[str] = []
    if label == "crash_day" and m.get("r1") is not None:
        why.append("%s %+.1f%% on the day" % (reading, m["r1"]))
    if label == "drawdown":
        if m.get("r5") is not None and m["r5"] <= lim.get("r5", -1e9):
            why.append("%s %+.1f%% over 5 closes" % (reading, m["r5"]))
        if m.get("dd") is not None and m["dd"] <= lim.get("dd", -1e9):
            why.append("%s %.1f%% under its 250-close high" % (reading, -m["dd"]))
    if label == "stress":
        if m.get("vix") is not None and m["vix"] >= 25.0:
            why.append("VIX %.1f" % m["vix"])
        if m.get("vol20") is not None and m["vol20"] >= lim.get("vol20", 1e9):
            why.append("%s 20-close volatility %.1f%%" % (reading, m["vol20"]))
    if label == "weak" and m.get("vs50") is not None:
        why.append("%s %.1f%% under its 50-close average" % (reading, -m["vs50"]))
    words = {"crash_day": "on a crash day", "drawdown": "in a drawdown", "stress": "under stress",
             "weak": "weak", "calm": "calm"}[label]
    return "%s %s as of %s%s" % (name, words, day.isoformat() if day else "n/a",
                                 (" (" + ", ".join(why) + ")") if why else "")


# ------------------------------------------------------------------ Yahoo
def _live_from(meta: Dict[str, Any], now_ts: Optional[float]) -> Optional[int]:
    """The start of the regular session when now_ts falls inside it (its bars are not final yet)."""
    if now_ts is None:
        return None
    try:
        reg = (meta.get("currentTradingPeriod") or {}).get("regular") or {}
        start, end = int(reg["start"]), int(reg["end"])
    except (KeyError, TypeError, ValueError, AttributeError):
        return None
    return start if start <= now_ts < end else None


def _bars(payload: Any, now_ts: Optional[float]) -> List[Tuple[_dt.date, float]]:
    """(venue-local day, close) for every bar with a positive close, a session still open left out, the LAST bar
    of a day kept (hourly bars collapse into their day's last close)."""
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
    live = _live_from(meta, now_ts)
    try:
        closes = ((r0.get("indicators") or {}).get("quote") or [{}])[0].get("close") or []
    except (TypeError, IndexError, AttributeError):
        closes = []
    out: Dict[_dt.date, float] = {}
    for i, t in enumerate(r0.get("timestamp") or []):
        c = _num(closes[i]) if i < len(closes) else None
        if c is None or c <= 0:
            continue
        try:
            t = int(t)
        except (TypeError, ValueError):
            continue
        if live is not None and t >= live:
            continue
        out[_dt.datetime.fromtimestamp(t + off, UTC).date()] = c
    return [(d, out[d]) for d in sorted(out)]


def _default_opener(req: urllib.request.Request, timeout: float):  # pragma: no cover - network
    return urllib.request.urlopen(req, timeout=timeout)


def _why(exc: Exception) -> str:
    code = getattr(exc, "code", None)
    return ("HTTP %s" % code) if code else type(exc).__name__


def _fetch(path: str, now_ts: float, opener: Callable[..., Any], sleep: Callable[[float], None],
           notes: Optional[List[str]] = None) -> List[Tuple[_dt.date, float]]:
    """1.0.1: `notes` collects, per host tried, why it gave no bars."""
    for host in YAHOO_HOSTS:
        req = urllib.request.Request("https://" + host + path,
                                     headers={"User-Agent": YAHOO_UA, "Accept": "application/json"})
        try:
            with opener(req, TIMEOUT_SEC) as resp:
                bars = _bars(json.loads(resp.read().decode("utf-8", "replace")), now_ts)
            if bars:
                return bars
            if notes is not None:
                notes.append("%s: no bars" % host.split(".")[0])
        except Exception as exc:  # noqa: BLE001
            if notes is not None:
                notes.append("%s: %s" % (host.split(".")[0], _why(exc)))
            if getattr(exc, "code", None) == 404:
                return []
        sleep(1.0)
    return []


def series(ysym: str, now_ts: float, opener: Callable[..., Any],
           sleep: Callable[[float], None] = time.sleep,
           notes: Optional[List[str]] = None) -> List[Tuple[_dt.date, float]]:
    """The completed daily closes of one reading; rebuilt from hourly bars when the daily answer is short."""
    sym = urllib.parse.quote(ysym, safe="")
    daily = _fetch(DAILY_PATH.format(sym=sym), now_ts, opener, sleep, notes)
    if len(daily) >= MIN_DAILY:
        return daily
    hourly = _fetch(HOURLY_PATH.format(sym=sym), now_ts, opener, sleep, notes)
    return hourly if len(hourly) > len(daily) else daily


def read(now: Optional[_dt.datetime] = None, opener: Optional[Callable[..., Any]] = None,
         sleep: Callable[[float], None] = time.sleep,
         markets: Sequence[str] = tuple(MARKETS),
         missing: Optional[Dict[str, str]] = None) -> Dict[str, Dict[str, Any]]:
    """{market id: {"label", "day", "text", "metrics"}} for each market read; a market not read is left out, and
    (1.0.1) `missing` gets the reason for it."""
    now = now or _dt.datetime.now(UTC)
    opener = opener or _default_opener
    now_ts = now.timestamp()
    out: Dict[str, Dict[str, Any]] = {}
    vix_bars: Optional[List[Tuple[_dt.date, float]]] = None
    why: Dict[str, str] = {}
    for mid in markets:
        if mid not in MARKETS:
            continue
        notes: List[str] = []
        try:
            bars = series(MARKETS[mid][0], now_ts, opener, sleep, notes)
            if len(bars) < 2:
                why[mid] = "; ".join(dict.fromkeys(notes)) or "no bars"
                continue
            day = bars[-1][0]
            if (now.astimezone(UTC).date() - day).days > MAX_AGE_DAYS:
                why[mid] = "last session %s is more than %d days old" % (day.isoformat(), MAX_AGE_DAYS)
                continue
            vix = None
            if mid == "us":
                if vix_bars is None:
                    vix_bars = series(VIX, now_ts, opener, sleep)
                vix = next((c for d, c in vix_bars if d == day), None)      # the SAME session, never carried
            m = metrics([c for _, c in bars], vix, mid)
            label = classify(m)
            if label is None:
                why[mid] = "not enough closes for a label (%d)" % len(bars)
                continue
            out[mid] = {"label": label, "day": day, "text": describe(mid, label, m, day), "metrics": m}
        except Exception as exc:  # noqa: BLE001 - one market never stops the others
            why[mid] = _why(exc)
            continue
    if missing is not None:
        missing.update(why)
    return out


# ------------------------------------------------------------------ the cache
_LOCK = threading.Lock()
_STATE: Dict[str, Any] = {"labels": {}, "read_at": {}, "attempt_at": None, "running": False, "last": None}


def ttl_sec() -> int:
    try:
        v = int(float((os.getenv("TFB_MARKET_CONDITION_TTL_SEC") or "").strip()))
    except ValueError:
        v = DEFAULT_TTL_SEC
    return v if 60 <= v <= 86400 else DEFAULT_TTL_SEC


def _refresh(opener: Optional[Callable[..., Any]], sleep: Callable[[float], None]) -> None:
    missing: Dict[str, str] = {}
    try:
        got = read(opener=opener, sleep=sleep, missing=missing)
        now_ts = time.time()
        with _LOCK:
            for mid, info in got.items():
                _STATE["labels"][mid] = info
                _STATE["read_at"][mid] = now_ts
            _STATE["last"] = {"at": now_ts, "read": sorted(got), "missing": dict(sorted(missing.items()))}
    except Exception as exc:  # noqa: BLE001
        with _LOCK:
            _STATE["last"] = {"at": time.time(), "read": [], "missing": {"all": _why(exc)}}
        raise
    finally:
        with _LOCK:
            _STATE["running"] = False


def _spawn(target: Callable[..., None], args: Tuple[Any, ...]) -> None:
    threading.Thread(target=target, args=args, daemon=True, name="tfb-market-condition").start()


def labels(blocking: bool = False, opener: Optional[Callable[..., Any]] = None,
           sleep: Callable[[float], None] = time.sleep) -> Dict[str, Dict[str, Any]]:
    """The labels known now: {market id: {"label", "day", "text", ...}}. A refresh is started (in a background
    thread unless `blocking`) when the last attempt is older than the TTL; a market not read for STALE_MAX_SEC is
    left out. Never raises."""
    try:
        now_ts = time.time()
        start = False
        with _LOCK:
            last = _STATE["attempt_at"]
            if not _STATE["running"] and (last is None or now_ts - last >= ttl_sec()):
                _STATE["running"] = True
                _STATE["attempt_at"] = now_ts
                start = True
        if start:
            if blocking:
                _refresh(opener, sleep)
            else:
                try:
                    _spawn(_refresh, (opener, sleep))
                except Exception:  # noqa: BLE001 - no thread: try again after the TTL
                    with _LOCK:
                        _STATE["running"] = False
        with _LOCK:
            return {mid: dict(info) for mid, info in _STATE["labels"].items()
                    if now_ts - _STATE["read_at"].get(mid, 0) <= STALE_MAX_SEC}
    except Exception:  # noqa: BLE001
        return {}


def set_labels(values: Dict[str, Dict[str, Any]], at: Optional[float] = None) -> None:
    """Install known labels (tests, and a warm start): {market id: {"label", "day", "text"}}; the next refresh is not
    due before the TTL."""
    t = time.time() if at is None else at
    with _LOCK:
        _STATE["labels"] = {mid: dict(v) for mid, v in values.items()}
        _STATE["read_at"] = {mid: t for mid in values}
        _STATE["attempt_at"] = t
        _STATE["running"] = False


def reset() -> None:
    with _LOCK:
        _STATE["labels"], _STATE["read_at"] = {}, {}
        _STATE["attempt_at"], _STATE["running"], _STATE["last"] = None, False, None


def state() -> Dict[str, Any]:
    """For the engine's gate report: what is known, from when - {market id: {"label", "day", "age_min"}} and, once
    a read has been tried, "_read": {"ago_min", "read", "missing", "running"}. 1.0.1: it also starts a refresh when
    one is due (in the background: it never waits), so a worker reads at its first health line."""
    try:
        labels()
        now_ts = time.time()
        with _LOCK:
            out: Dict[str, Any] = {mid: {"label": info.get("label"), "day": str(info.get("day")),
                                         "age_min": int((now_ts - _STATE["read_at"].get(mid, now_ts)) / 60)}
                                   for mid, info in _STATE["labels"].items()}
            last = _STATE["last"]
            if last is not None or _STATE["running"]:
                out["_read"] = {"ago_min": int((now_ts - last["at"]) / 60) if last else None,
                                "read": list(last["read"]) if last else [],
                                "missing": dict(last["missing"]) if last else {},
                                "running": bool(_STATE["running"])}
            return out
    except Exception:  # noqa: BLE001
        return {}
