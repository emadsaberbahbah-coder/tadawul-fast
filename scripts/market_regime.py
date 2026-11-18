#!/usr/bin/env python3
"""
scripts/market_regime.py - the US market regime, for the live plan
==================================================================
VERSION 1.0.0 (2026-11-18)

WHY (owner, Wed 18 Nov 2026 14:51 and 14:56 Riyadh, after two weeks of market shocks: "do we need to change the
limits ... or the scoring? ... on a scientific basis and continuous learning" - "yes, let's start building and
activating"). Measured that day on the archive (v_regime_outcomes, seats of 20 Jun - 17 Nov 2026):
  - a USD pick made while the US market was 'weak' closed higher five sessions later 37.8% of the time, average
    -1.03% (143 picks); in a 'calm' market 54.1%, average +0.29% (229 picks);
  - the market had been 'weak' or worse on every session since 30 Oct, and the four buys of 12 and 16 Nov were all
    made in it;
  - the regime label existed only in BigQuery (77_v_market_regime.sql, a research view the live system never
    reads), so no page and no rule could use it.

WHAT. The same label, computed for the live system from the same inputs and the same rules, first match wins:
    crash_day   the S&P 500 fund (SPY) fell 2% or more on the last session
    drawdown    it fell 3% or more over 5 sessions, or stands 8% or more under its 250-session high
    stress      the VIX closed at 25 or more, or SPY's 20-session volatility (annualised) is 25% or more
    weak        SPY closed under its 50-session average
    calm        everything else
classify() is pure and mirrors the SQL line for line (tests pin both to the same cases). read() fetches the daily
bars of SPY and ^VIX from the Yahoo chart service and returns the label of the last COMPLETED session, with the
figures behind it. The Daily Plan (1.10.0) uses it to hold new US buys in a weak market.

This file decides nothing by itself and writes nothing.
"""
from __future__ import annotations

import datetime as _dt
import json
import math
import os
import sys
import time
import urllib.request
from typing import Any, Callable, Dict, List, Optional, Sequence

if __package__ in (None, ""):
    sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from scripts.exit_plan import YAHOO_HOSTS, YAHOO_UA, parse_chart, split_open_session  # noqa: E402

__version__ = "1.0.0"
UTC = _dt.timezone.utc
LABELS = ("crash_day", "drawdown", "stress", "weak", "calm")
PATH = "/v8/finance/chart/{sym}?range=2y&interval=1d"
_SLEEP = time.sleep


def _pct(a: float, b: Optional[float]) -> Optional[float]:
    return None if not b else (a / b - 1.0) * 100.0


def metrics(closes: Sequence[float], vix: Optional[float]) -> Dict[str, Optional[float]]:
    """The figures of 77_v_market_regime.sql for the last close in `closes` (oldest first)."""
    n = len(closes)
    c = closes[-1]
    rets = [closes[i] / closes[i - 1] - 1.0 for i in range(max(1, n - 20), n)]
    vol = None
    if len(rets) == 20:
        mu = sum(rets) / 20.0
        vol = math.sqrt(sum((r - mu) ** 2 for r in rets) / 19.0) * math.sqrt(252.0) * 100.0   # STDDEV_SAMP
    return {"r1": _pct(c, closes[-2]) if n >= 2 else None,
            "r5": _pct(c, closes[-6]) if n >= 6 else None,
            "r20": _pct(c, closes[-21]) if n >= 21 else None,
            "vs50": _pct(c, sum(closes[-50:]) / 50.0) if n >= 50 else None,
            "vol20": vol,
            "dd": _pct(c, max(closes[-250:])),
            "vix": vix}


def classify(m: Dict[str, Optional[float]]) -> str:
    """One label, first match wins; a figure that is not known never matches (as NULL does not in the view)."""
    def le(key: str, x: float) -> bool:
        return m.get(key) is not None and m[key] <= x

    def ge(key: str, x: float) -> bool:
        return m.get(key) is not None and m[key] >= x

    if le("r1", -2.0):
        return "crash_day"
    if le("r5", -3.0) or le("dd", -8.0):
        return "drawdown"
    if ge("vix", 25.0) or ge("vol20", 25.0):
        return "stress"
    if m.get("vs50") is not None and m["vs50"] < 0:
        return "weak"
    return "calm"


def describe(label: str, m: Dict[str, Optional[float]], day: Optional[_dt.date]) -> str:
    """'STRESS as of 2026-11-17 (S&P 500 fund -1.0% on the day, 4.0% under its 50-day average, VIX 25.5)'."""
    parts: List[str] = []
    if m.get("r1") is not None:
        parts.append("S&P 500 fund %+.1f%% on the day" % m["r1"])
    if m.get("vs50") is not None:
        parts.append("%.1f%% %s its 50-day average" % (abs(m["vs50"]), "under" if m["vs50"] < 0 else "over"))
    parts.append("VIX %.1f" % m["vix"] if m.get("vix") is not None else "VIX not read")
    return "%s as of %s (%s)" % (label.upper().replace("_", " "), day.isoformat() if day else "n/a", ", ".join(parts))


def _default_opener(req: urllib.request.Request, timeout: float):  # pragma: no cover - network
    return urllib.request.urlopen(req, timeout=timeout)


def _bars(ysym: str, opener: Callable[..., Any], now_ts: float, timeout: float = 20.0) -> List[Any]:
    for host in YAHOO_HOSTS:
        req = urllib.request.Request("https://" + host + PATH.format(sym=ysym),
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


def read(now: Optional[_dt.datetime] = None, opener: Optional[Callable[..., Any]] = None,
         max_age_days: int = 5) -> Dict[str, Any]:
    """{"ok", "label", "date", "metrics", "text"} for the last completed US session, or {"ok": False, "why"}.
    Unadjusted closes are not available from parse_chart; its dividend-adjusted closes move SPY's figures by a few
    hundredths of a percent, well inside every threshold's margin except on the line itself."""
    now = now or _dt.datetime.now(UTC)
    opener = opener or _default_opener
    spy = _bars("SPY", opener, now.timestamp())
    if len(spy) < 51:
        return {"ok": False, "why": "S&P 500 fund history not read (%d sessions)" % len(spy)}
    day = spy[-1][0]
    if (now.date() - day).days > max_age_days:
        return {"ok": False, "why": "the last S&P 500 fund session read is %s" % day.isoformat()}
    vix_bars = _bars("%5EVIX", opener, now.timestamp())
    # the VIX close of the SAME session, as the view joins it; an earlier close is never carried forward
    vix = next((b[4] / (b[5] or 1.0) for b in vix_bars if b[0] == day), None)
    m = metrics([b[4] for b in spy], vix)
    label = classify(m)
    return {"ok": True, "label": label, "date": day, "metrics": m, "text": describe(label, m, day)}
