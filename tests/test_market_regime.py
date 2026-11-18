#!/usr/bin/env python3
"""tests/test_market_regime.py - scripts/market_regime.py 1.0.0: the US market regime for the live plan.

  R1 classify(): the five labels, first match wins, on the archive's own figures for 29 Oct - 17 Nov 2026
  R2 the thresholds at the edge; a figure that is not known never matches
  R3 metrics(): the view's arithmetic (changes, 50-session average, 250-session high, sample volatility)
  R4 the rules here are the rules of sql/bigquery/77_v_market_regime.sql
  R5 read(): the last completed session, the VIX of that day, a stale or short history is not a read
  R6 the manifest pin and the CI lane
"""
from __future__ import annotations

import datetime as dt
import json
import math
import sys
from pathlib import Path

import pytest
import yaml

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts import market_regime as mr  # noqa: E402
from tests.test_exit_plan import Resp, chart  # noqa: E402

UTC = dt.timezone.utc
NOW = dt.datetime(2026, 11, 18, 6, 0, tzinfo=UTC)


def M(**over):
    m = {"r1": 0.1, "r5": 0.5, "r20": 1.0, "vs50": 1.0, "vol20": 12.0, "dd": -1.0, "vix": 16.0}
    m.update(over)
    return m


@pytest.fixture(autouse=True)
def _fast(monkeypatch):
    monkeypatch.setattr(mr, "_SLEEP", lambda s: None)


# rows of v_market_regime as archived (date, r1, r5, vs50, dd, vix, label)
ARCHIVE = [("2026-10-29", -0.38, 0.71, 0.23, -1.25, 17.2, "calm"), ("2026-10-30", -1.77, -0.56, -1.53, -3.0, 19.4, "weak"),
           ("2026-11-04", 1.65, -0.71, -0.03, -1.58, 16.6, "weak"), ("2026-11-05", -4.41, -4.73, -4.36, -5.92, 28.2, "crash_day"),
           ("2026-11-06", 0.31, -2.7, -3.97, -5.62, 25.6, "stress"), ("2026-11-09", 2.37, 0.08, -1.65, -3.39, 19.9, "weak"),
           ("2026-11-16", -1.91, -1.69, -3.13, -5.02, 22.9, "weak"), ("2026-11-17", -1.01, -2.33, -4.02, -5.99, 25.5, "stress")]


@pytest.mark.parametrize("day,r1,r5,vs50,dd,vix,label", ARCHIVE)
def test_r1_the_archives_own_days(day, r1, r5, vs50, dd, vix, label):
    assert mr.classify(M(r1=r1, r5=r5, vs50=vs50, dd=dd, vix=vix)) == label, day


def test_r2_edges_and_unknown_figures():
    assert mr.classify(M(r1=-2.0)) == "crash_day" and mr.classify(M(r1=-1.99)) == "calm"
    assert mr.classify(M(r5=-3.0)) == "drawdown" and mr.classify(M(dd=-8.0)) == "drawdown"
    assert mr.classify(M(r5=-2.99, dd=-7.99)) == "calm"
    assert mr.classify(M(vix=25.0)) == "stress" and mr.classify(M(vol20=25.0)) == "stress"
    assert mr.classify(M(vix=24.99, vol20=24.99)) == "calm"
    assert mr.classify(M(vs50=-0.01)) == "weak" and mr.classify(M(vs50=0.0)) == "calm"
    assert mr.classify(M(r1=-3.0, r5=-5.0, vix=40.0, vs50=-9.0)) == "crash_day"       # first match wins
    assert mr.classify(M(r5=-5.0, vix=40.0, vs50=-9.0)) == "drawdown"
    none = {k: None for k in M()}
    assert mr.classify(none) == "calm"                                             # nothing known, nothing matches
    assert mr.classify(M(vix=None, vs50=-1.0)) == "weak"                           # an unread VIX is not stress
    assert mr.LABELS == ("crash_day", "drawdown", "stress", "weak", "calm")


def test_r3_metrics_are_the_views_arithmetic():
    closes = [100.0] * 255 + [110.0, 108.9]
    m = mr.metrics(closes, 21.5)
    assert round(m["r1"], 4) == -1.0 and round(m["r5"], 4) == 8.9 and round(m["r20"], 4) == 8.9
    assert round(m["vs50"], 4) == round((108.9 / ((100.0 * 48 + 110.0 + 108.9) / 50.0) - 1) * 100, 4)
    assert round(m["dd"], 4) == -1.0 and m["vix"] == 21.5                           # under the 250-session high 110
    rets = [0.0] * 18 + [0.1, -0.01]
    mu = sum(rets) / 20
    want = math.sqrt(sum((r - mu) ** 2 for r in rets) / 19) * math.sqrt(252) * 100   # sample deviation, annualised
    assert abs(m["vol20"] - want) < 1e-9
    short = mr.metrics([100.0, 101.0], None)
    assert short["r5"] is None and short["vs50"] is None and short["vol20"] is None and round(short["r1"], 6) == 1.0
    old_high = mr.metrics([200.0] + [100.0] * 250, None)                            # the high left the 250-session window
    assert round(old_high["dd"], 6) == 0.0


def test_r4_the_rules_are_the_sql_views():
    sql = (ROOT / "sql/bigquery/77_v_market_regime.sql").read_text(encoding="utf-8")
    for line in ("WHEN m.spy_r1_pct <= -2 THEN 'crash_day'",
                 "WHEN m.spy_r5_pct <= -3 OR m.spy_dd_from_high_pct <= -8 THEN 'drawdown'",
                 "WHEN m.vix_close >= 25 OR m.spy_vol20_pct >= 25 THEN 'stress'",
                 "WHEN m.spy_vs_50d_pct < 0 THEN 'weak'", "ELSE 'calm'",
                 "ROWS BETWEEN 49 PRECEDING AND CURRENT ROW", "ROWS BETWEEN 249 PRECEDING AND CURRENT ROW", "STDDEV_SAMP"):
        assert line in sql, line


def _days(n, end):
    out, d = [], end
    while len(out) < n:
        if d.weekday() < 5:
            out.append(d)
        d -= dt.timedelta(days=1)
    return out[::-1]


def _opener(series):
    def opener(req, timeout):
        sym = req.full_url.split("/chart/")[1].split("?")[0]
        if sym not in series:
            e = Exception("404")
            e.code = 404
            raise e
        days, closes = series[sym]
        ts = [int(dt.datetime(d.year, d.month, d.day, 14, 30, tzinfo=UTC).timestamp()) for d in days]
        return Resp(json.dumps(chart(ts, closes, closes, closes, closes, off=0)).encode())
    return opener


def test_r5_read():
    end = dt.date(2026, 11, 17)
    days = _days(120, end)
    spy = [700.0] * 118 + [690.0, 683.0]                                            # under its average, -1.0% on the day
    vix = [18.0] * 119 + [25.5]
    r = mr.read(NOW, _opener({"SPY": (days, spy), "%5EVIX": (days, vix)}))
    assert r["ok"] and r["date"] == end and r["label"] == "stress" and r["metrics"]["vix"] == 25.5
    assert r["text"].startswith("STRESS as of 2026-11-17 (S&P 500 fund -1.0% on the day, ") and "VIX 25.5" in r["text"]
    assert "under its 50-day average" in r["text"]
    # the VIX is the close of the SPY session, not a later one
    later = mr.read(NOW, _opener({"SPY": (days, spy), "%5EVIX": (days + [dt.date(2026, 11, 18)], vix + [40.0])}))
    assert later["metrics"]["vix"] == 25.5
    # a VIX series that ends before the SPY session is not carried forward (review of #1022): an old 30 is not today's
    behind = mr.read(NOW, _opener({"SPY": (days, spy), "%5EVIX": (days[:-1], [18.0] * 118 + [30.0])}))
    assert behind["metrics"]["vix"] is None and behind["label"] == "weak" and "VIX not read" in behind["text"]
    # no VIX read: the other rules still decide, and the text says so
    nov = mr.read(NOW, _opener({"SPY": (days, spy)}))
    assert nov["ok"] and nov["label"] == "weak" and "VIX not read" in nov["text"]
    calm = mr.read(NOW, _opener({"SPY": (days, [700.0] * 118 + [705.0, 707.0]), "%5EVIX": (days, [15.0] * 120)}))
    assert calm["label"] == "calm" and "over its 50-day average" in calm["text"]
    # not a read: too little history, an old last session, nothing answered
    assert mr.read(NOW, _opener({"SPY": (days[-30:], spy[-30:])})) == {
        "ok": False, "why": "S&P 500 fund history not read (30 sessions)"}
    stale = mr.read(NOW + dt.timedelta(days=30), _opener({"SPY": (days, spy)}))
    assert not stale["ok"] and "2026-11-17" in stale["why"]
    assert mr.read(NOW, _opener({}))["ok"] is False


def test_r6_manifest_and_ci():
    vd = (ROOT / "scripts/verify_deployment.py").read_text(encoding="utf-8")
    assert '("market_regime", "%s", "market regime")' % mr.__version__ in vd and mr.__version__ == "1.0.0"
    ci = yaml.safe_load((ROOT / ".github/workflows/ci.yml").read_text(encoding="utf-8"))
    assert any("tests/test_market_regime.py" in s.get("run", "") for s in ci["jobs"]["lean-unit"]["steps"])
