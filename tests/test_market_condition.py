"""core/market_condition.py 1.0.0: the live condition of five stock markets.

Owner, 21 Nov 2026 19:10 Riyadh: "MODEL EVIDENCE 47.60% INVESTABLE hit rate ...
need to improve". The engine's [MARKET-HOLD] tier (data_engine_v2 5.161.0)
reads these labels; they must be the labels of sql/bigquery/99_v_market_condition.sql.

M1  market_of: suffixes, a bare four-digit code (Tadawul), a bare ticker (US),
    anything else -> no market.
M2  metrics() + classify() mirror the view: the five labels, first match wins,
    an unknown figure never matches, ROUND(4) figures and limits, the US k = 1,
    elsewhere k = normal volatility / 16 (NULL under 120 changes -> no label),
    the VIX only in the US; every figure recomputed here independently.
M3  _bars: venue-local day, null closes skipped, the bars of a session still
    open left out, hourly bars collapse to the day's last close.
M4  series: a short daily answer (Yahoo gives ^TASI.SR one daily bar) is
    rebuilt from hourly bars; the second host is tried after a fault.
M5  read: every market read from fake Yahoo answers; the VIX of the same day
    only; a market more than 7 days old or with a fault is left out alone.
M6  labels: the cache - installed labels are served without a refresh, a
    stale market drops out, a blocking refresh fills it, TTL parsing, never
    raises; describe() text.
M7  version, manifest pin, stdlib only, nothing started at import.
"""
import datetime as dt
import io
import json
import math
import pathlib
import statistics
import threading

import pytest

from core import market_condition as mc

_REPO = pathlib.Path(__file__).resolve().parents[1]
UTC = dt.timezone.utc


@pytest.fixture(autouse=True)
def clean_cache(monkeypatch):
    monkeypatch.delenv("TFB_MARKET_CONDITION_TTL_SEC", raising=False)
    mc.reset()
    yield
    mc.reset()


# ---------------------------------------------------------------- M1
@pytest.mark.parametrize("symbol,market", [
    ("2222.SR", "sa"), ("2222.sr", "sa"), ("1050", "sa"), (" 4190.SR ", "sa"),
    ("EFC.US", "us"), ("BRK-B.US", "us"), ("GOOGL", "us"), ("BRK-B", "us"),
    ("BRK.B", "us"), ("HEI.A", "us"), ("bf.b", "us"),            # US share classes (normalize's LETTERS.LETTER)
    ("ABC.V", ""), ("XYZ.N", ""), ("ABCDEF.B", ""),             # .V / .N are exchanges; six letters is no class
    ("BBOX.L", "eu"), ("ACA.PA", "eu"), ("SAP.XETRA", "eu"), ("GALP.LS", "eu"), ("NOVO-B.CO", "eu"),
    ("7203.T", "jp"), ("8306.TSE", "jp"),
    ("0700.HK", "cn"), ("600519.SS", "cn"), ("000001.SZ", "cn"), ("601318.SHG", "cn"),
    ("PEO.WA", ""), ("T82U.SI", ""), ("RY.TO", ""), ("GC=F", ""), ("EURUSD=X", ""), ("^N225", ""),
    ("", ""), (None, ""), ("12345", ""),
])
def test_m1_market_of(symbol, market):
    assert mc.market_of(symbol) == market


# ---------------------------------------------------------------- M2
def r4(x):
    return None if x is None else round(x, 4)


def view_label(closes, vix, market_id):
    """The view, written out again from its SQL, for one close (the last)."""
    n = len(closes)
    c = closes[-1]
    chg = [None] + [closes[i] / closes[i - 1] - 1 for i in range(1, n)]
    w20 = [x for x in chg[max(0, n - 20):] if x is not None]
    w250 = [x for x in chg[max(0, n - 250):] if x is not None]
    vol20 = r4(statistics.stdev(w20) * math.sqrt(252) * 100) if n >= 20 and len(w20) >= 2 else None
    normal = r4(statistics.stdev(w250) * math.sqrt(252) * 100) if len(w250) >= 120 else None
    k = 1.0 if market_id == "us" else (round(normal / 16, 4) if normal is not None else None)
    fig = {
        "r1": r4((c / closes[-2] - 1) * 100) if n >= 2 else None,
        "r5": r4((c / closes[-6] - 1) * 100) if n >= 6 else None,
        "vs50": r4((c / (sum(closes[-50:]) / 50) - 1) * 100) if n >= 50 else None,
        "vol20": vol20,
        "dd": r4((c / max(closes[-250:]) - 1) * 100),
        "normal_vol": normal,
        "vix": vix if market_id == "us" else None,
        "k": k,
    }
    if k is None:
        return None, fig
    lim1, lim5, limdd, limv = round(-2 * k, 4), round(-3 * k, 4), round(-8 * k, 4), round(25 * k, 4)

    def le(v, lim):
        return v is not None and v <= lim

    if le(fig["r1"], lim1):
        return "crash_day", fig
    if le(fig["r5"], lim5) or le(fig["dd"], limdd):
        return "drawdown", fig
    if (fig["vix"] is not None and fig["vix"] >= 25) or (fig["vol20"] is not None and fig["vol20"] >= limv):
        return "stress", fig
    if fig["vs50"] is not None and fig["vs50"] < 0:
        return "weak", fig
    return "calm", fig


def wave(n, drift=0.0004, amp=0.004, start=100.0):
    """A gently rising series with a small, regular wiggle (about 6% a year normal volatility)."""
    out = [start]
    for i in range(1, n):
        out.append(out[-1] * (1 + drift + amp * (1 if i % 2 else -1)))
    return out


def scaled(n, amp, start=100.0):
    out = [start]
    for i in range(1, n):
        out.append(out[-1] * (1 + 0.0003 + amp * (1 if i % 2 else -1)))
    return out


CASES = {
    "us calm": (wave(300), 14.0, "us", "calm"),
    "us weak": (wave(300)[:-3] + [wave(300)[-4] * f for f in (0.99, 0.985, 0.982)], 14.0, "us", "weak"),
    "us crash": (wave(300)[:-1] + [wave(300)[-2] * 0.975], 14.0, "us", "crash_day"),
    "us crash at the limit": (wave(300)[:-1] + [wave(300)[-2] * 0.98], 14.0, "us", "crash_day"),
    "us vix stress": (wave(300), 25.0, "us", "stress"),
    "us vix just under": (wave(300), 24.99, "us", "calm"),
    "us vix unknown": (wave(300), None, "us", "calm"),
    "us 5-close drawdown": (wave(300)[:-5] + [wave(300)[-6] * f for f in (0.99, 0.98, 0.975, 0.97, 0.965)],
                            14.0, "us", "drawdown"),
    "us short history still reads": (wave(30), 14.0, "us", None),
    "sa calm": (wave(300), None, "sa", "calm"),
    "sa vix ignored": (wave(300), 99.0, "sa", "calm"),
    "sa no k under 120 changes": (wave(120), None, "sa", None),
    "sa k at 120 changes": (wave(121), None, "sa", None),
    # a market twice as volatile as the S&P's normal level: a -2.5% day is not a crash there
    "eu wide k: -2.5% is not a crash": (scaled(300, 0.02)[:-1] + [scaled(300, 0.02)[-2] * 0.975], None, "eu",
                                        None),
}


@pytest.mark.parametrize("name", sorted(CASES))
def test_m2_labels_mirror_the_view(name):
    closes, vix, mid, expected = CASES[name]
    want, fig = view_label(closes, vix, mid)
    got_fig = mc.metrics(closes, vix, mid)
    for key in ("r1", "r5", "vs50", "vol20", "dd", "normal_vol", "vix", "k"):
        a, b = got_fig[key], fig[key]
        assert (a is None and b is None) or (a is not None and b is not None and abs(a - b) < 1e-9), (key, a, b)
    got = mc.classify(got_fig)
    assert got == want, (name, got, want, got_fig)
    if expected is not None:
        assert got == expected, (name, got, got_fig)


def test_m2_view_details():
    # US k = 1 whatever the history; elsewhere k from 120+ changes, ROUND(normal / 16, 4)
    assert mc.metrics(wave(30), 14.0, "us")["k"] == 1.0
    assert mc.metrics(wave(120), None, "sa")["k"] is None
    m = mc.metrics(wave(121), None, "sa")
    assert m["normal_vol"] is not None and m["k"] == round(m["normal_vol"] / 16, 4)
    assert mc.classify(mc.metrics(wave(120), None, "sa")) is None
    # the wide market: the same -2.5% day is a crash in the US
    s = scaled(300, 0.02)
    s = s[:-1] + [s[-2] * 0.975]
    assert mc.classify(mc.metrics(s, 14.0, "us")) == "crash_day"
    wide = mc.metrics(s, None, "eu")
    assert wide["k"] > 1.25 and mc.classify(wide) != "crash_day"
    assert mc.limits(1.0) == {"r1": -2.0, "r5": -3.0, "dd": -8.0, "vol20": 25.0}
    assert mc.limits(1.23456) == {"r1": -2.4691, "r5": -3.7037, "dd": -9.8765, "vol20": 30.864}
    # an empty series and a single close never raise
    assert mc.classify(mc.metrics([], None, "us")) is None
    one = mc.metrics([100.0], None, "us")
    assert one["r1"] is None and one["dd"] == 0.0 and mc.classify(one) == "calm"


# ---------------------------------------------------------------- M3 / M4
def payload(rows, gmtoffset=0, regular=None):
    """rows: [(unix ts, close or None)]."""
    meta = {"gmtoffset": gmtoffset}
    if regular:
        meta["currentTradingPeriod"] = {"regular": {"start": regular[0], "end": regular[1]}}
    return {"chart": {"result": [{"meta": meta, "timestamp": [t for t, _ in rows],
                                  "indicators": {"quote": [{"close": [c for _, c in rows]}]}}], "error": None}}


def ts(y, m, d, hh=0, mm=0):
    return int(dt.datetime(y, m, d, hh, mm, tzinfo=UTC).timestamp())


def test_m3_bars_local_day_open_session_and_hourly():
    # Riyadh (+3): a 21:30 UTC bar belongs to the next local day
    rows = [(ts(2026, 11, 18, 7), 10.0), (ts(2026, 11, 18, 21, 30), 11.0), (ts(2026, 11, 19, 7), None),
            (ts(2026, 11, 19, 8), 12.0), (ts(2026, 11, 19, 11), 12.5)]
    got = mc._bars(payload(rows, gmtoffset=10800), None)
    assert got == [(dt.date(2026, 11, 18), 10.0), (dt.date(2026, 11, 19), 12.5)]
    # the session 19 Nov 07:00-12:00 UTC is open at 10:00: its bars are not final
    live = (ts(2026, 11, 19, 7), ts(2026, 11, 19, 12))
    got = mc._bars(payload(rows, gmtoffset=10800, regular=live), ts(2026, 11, 19, 10))
    assert got == [(dt.date(2026, 11, 18), 10.0), (dt.date(2026, 11, 19), 11.0)]
    # after the close the same session counts
    got = mc._bars(payload(rows, gmtoffset=10800, regular=live), ts(2026, 11, 19, 13))
    assert got[-1] == (dt.date(2026, 11, 19), 12.5)
    # junk never raises
    for bad in (None, {}, {"chart": {}}, {"chart": {"result": None}}, {"chart": {"result": [None]}}, "x"):
        assert mc._bars(bad, None) == []


class FakeResponse(io.BytesIO):
    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False


class FakeYahoo:
    """answers[(symbol, interval)] -> payload dict, or an Exception to raise."""

    def __init__(self, answers, fail_hosts=()):
        self.answers = answers
        self.fail_hosts = set(fail_hosts)
        self.calls = []

    def __call__(self, req, timeout):
        url = req.full_url
        host = url.split("/")[2]
        sym = url.split("/v8/finance/chart/")[1].split("?")[0]
        from urllib.parse import unquote
        sym = unquote(sym)
        interval = "1h" if "interval=1h" in url else "1d"
        self.calls.append((host, sym, interval))
        assert timeout <= 15
        assert req.get_header("User-agent")
        if host in self.fail_hosts:
            raise OSError("connection reset")
        ans = self.answers.get((sym, interval))
        if ans is None:
            err = OSError("404")
            err.code = 404
            raise err
        if isinstance(ans, Exception):
            raise ans
        return FakeResponse(json.dumps(ans).encode())


def daily_rows(closes, last_day, hour=14, minute=30, skip_weekends=True):
    days = []
    d = last_day
    while len(days) < len(closes):
        if not skip_weekends or d.weekday() < 5:
            days.append(d)
        d -= dt.timedelta(days=1)
    days.reverse()
    return [(ts(x.year, x.month, x.day, hour, minute), c) for x, c in zip(days, closes)]


def test_m4_short_daily_answer_is_rebuilt_from_hourly():
    closes = wave(260)
    last = dt.date(2026, 11, 19)
    days = [r for r in daily_rows(closes, last, hour=7)]
    hourly = []
    for (t, c) in days:                       # five hourly bars a day, the last one is the close
        for h in range(5):
            hourly.append((t + h * 3600, c * (1 + 0.001 * (h - 4)) if h < 4 else c))
    one_daily = payload([days[-1]], gmtoffset=10800)
    fake = FakeYahoo({("^TASI.SR", "1d"): one_daily, ("^TASI.SR", "1h"): payload(hourly, gmtoffset=10800)})
    got = mc.series("^TASI.SR", ts(2026, 11, 21, 12), fake, sleep=lambda s: None)
    assert len(got) == 260 and [c for _, c in got] == closes
    assert ("query1.finance.yahoo.com", "^TASI.SR", "1h") in fake.calls
    # a fault on the first host: the second answers
    fake2 = FakeYahoo({("SPY", "1d"): payload(daily_rows(closes, last))}, fail_hosts={"query1.finance.yahoo.com"})
    got2 = mc.series("SPY", ts(2026, 11, 21, 12), fake2, sleep=lambda s: None)
    assert len(got2) == 260
    assert [h for h, _, _ in fake2.calls][:2] == ["query1.finance.yahoo.com", "query2.finance.yahoo.com"]


# ---------------------------------------------------------------- M5
def full_fake(last=dt.date(2026, 11, 20), us_closes=None, vix_rows=None, drop=()):
    us_closes = us_closes or wave(300)
    answers = {
        ("SPY", "1d"): payload(daily_rows(us_closes, last)),
        ("^VIX", "1d"): payload(vix_rows if vix_rows is not None else daily_rows([14.0] * 300, last)),
        ("^STOXX", "1d"): payload(daily_rows(wave(300), last, hour=8)),
        ("^N225", "1d"): payload(daily_rows(wave(300)[:-1] + [wave(300)[-2] * 0.9], last, hour=0)),
        ("^HSI", "1d"): payload(daily_rows(wave(300), last, hour=1, minute=30)),
        ("^TASI.SR", "1d"): payload(daily_rows(wave(300), last, hour=7), gmtoffset=10800),
    }
    for key in drop:
        answers.pop(key, None)
    return FakeYahoo(answers)


def test_m5_read_every_market():
    now = dt.datetime(2026, 11, 21, 12, tzinfo=UTC)
    got = mc.read(now=now, opener=full_fake(), sleep=lambda s: None)
    assert set(got) == {"us", "sa", "eu", "jp", "cn"}
    assert got["us"]["label"] == "calm" and got["us"]["day"] == dt.date(2026, 11, 20)
    assert got["us"]["metrics"]["vix"] == 14.0
    assert got["jp"]["label"] == "crash_day"
    assert got["jp"]["text"].startswith("the Japanese market on a crash day as of 2026-11-20 (Nikkei 225 -10.0%")
    assert got["sa"]["text"] == "the Saudi market calm as of 2026-11-20"


def test_m5_vix_of_the_same_day_only_and_failures_isolated():
    now = dt.datetime(2026, 11, 21, 12, tzinfo=UTC)
    # the VIX's last bar is a day older than SPY's: no VIX that day, so no VIX stress
    vix = daily_rows([30.0] * 300, dt.date(2026, 11, 19))
    got = mc.read(now=now, opener=full_fake(vix_rows=vix), sleep=lambda s: None)
    assert got["us"]["metrics"]["vix"] is None and got["us"]["label"] == "calm"
    vix_same = daily_rows([30.0] * 300, dt.date(2026, 11, 20))
    got = mc.read(now=now, opener=full_fake(vix_rows=vix_same), sleep=lambda s: None)
    assert got["us"]["label"] == "stress" and "VIX 30.0" in got["us"]["text"]
    # one market missing: the others still read
    got = mc.read(now=now, opener=full_fake(drop=[("^HSI", "1d")]), sleep=lambda s: None)
    assert set(got) == {"us", "sa", "eu", "jp"}
    # a market whose last completed session is more than 7 days old is not read
    late = dt.datetime(2026, 11, 28, 12, tzinfo=UTC)
    assert mc.read(now=late, opener=full_fake(), sleep=lambda s: None) == {}
    # an opener that raises something odd never escapes
    assert mc.read(now=now, opener=lambda req, timeout: 1 / 0, sleep=lambda s: None) == {}


# ---------------------------------------------------------------- M6
def test_m6_cache_contract(monkeypatch):
    started = []
    monkeypatch.setattr(mc, "_spawn", lambda target, args: started.append(target))
    info = {"label": "weak", "day": dt.date(2026, 11, 20), "text": "the US market weak as of 2026-11-20"}
    mc.set_labels({"us": info})
    got = mc.labels()
    assert got == {"us": info} and started == [], "installed labels are served without a refresh"
    got["us"]["label"] = "calm"
    assert mc.labels()["us"]["label"] == "weak", "callers get copies"
    # past the TTL a background refresh is started once; the old label is still served
    mc.set_labels({"us": info}, at=mc.time.time() - 2000)
    assert mc.labels()["us"]["label"] == "weak"
    assert started == [mc._refresh]
    mc.labels()
    assert len(started) == 1, "one refresh at a time"
    # past 12 hours the market drops out
    mc.set_labels({"us": info}, at=mc.time.time() - mc.STALE_MAX_SEC - 5)
    assert mc.labels() == {}
    st = mc.state()
    assert st["us"]["label"] == "weak" and st["us"]["age_min"] >= 720


def test_m6_background_refresh_and_spawn_fault(monkeypatch):
    last = dt.datetime.now(UTC).date() - dt.timedelta(days=1)
    fake = full_fake(last=last)
    first = mc.labels(opener=fake, sleep=lambda s: None)     # starts the read; returns at once
    assert first == {} or set(first) == {"us", "sa", "eu", "jp", "cn"}
    for t in threading.enumerate():
        if t.name == "tfb-market-condition":
            t.join(20)
    got = mc.labels(opener=fake)
    assert set(got) == {"us", "sa", "eu", "jp", "cn"} and got["jp"]["label"] == "crash_day"
    mc.reset()

    def no_thread(target, args):
        raise RuntimeError("can't start new thread")

    monkeypatch.setattr(mc, "_spawn", no_thread)
    assert mc.labels() == {}
    assert mc._STATE["running"] is False, "a refused thread is retried after the TTL"


def test_m6_blocking_refresh_and_ttl(monkeypatch):
    calls = []

    def fake_read(opener=None, sleep=None):
        calls.append(1)
        return {"sa": {"label": "drawdown", "day": dt.date(2026, 11, 20), "text": "t", "metrics": {}}}

    monkeypatch.setattr(mc, "read", fake_read)
    got = mc.labels(blocking=True)
    assert got["sa"]["label"] == "drawdown" and calls == [1]
    assert mc.labels(blocking=True)["sa"]["label"] == "drawdown" and calls == [1], "inside the TTL: no new read"
    for raw, want in (("", 1800), ("600", 600), ("59", 1800), ("86401", 1800), ("x", 1800), ("900.5", 900)):
        monkeypatch.setenv("TFB_MARKET_CONDITION_TTL_SEC", raw)
        assert mc.ttl_sec() == want, raw

    def boom(opener=None, sleep=None):
        raise RuntimeError("down")

    mc.reset()
    monkeypatch.setattr(mc, "read", boom)
    assert mc.labels(blocking=True) == {}
    assert mc._STATE["running"] is False, "a failed refresh releases the lock"


def test_m6_describe():
    m = {"r1": -2.5, "r5": -4.0, "dd": -9.25, "vol20": 31.0, "vs50": -3.0, "vix": 27.5, "k": 1.0}
    day = dt.date(2026, 11, 20)
    assert mc.describe("us", "crash_day", m, day) == \
        "the US market on a crash day as of 2026-11-20 (S&P 500 fund -2.5% on the day)"
    assert mc.describe("sa", "drawdown", m, day) == ("the Saudi market in a drawdown as of 2026-11-20 (Tadawul All "
                                                     "Share -4.0% over 5 closes, Tadawul All Share 9.2% under its "
                                                     "250-close high)")
    assert mc.describe("us", "stress", m, day).endswith("(VIX 27.5, S&P 500 fund 20-close volatility 31.0%)")
    assert mc.describe("eu", "weak", m, day) == ("the European market weak as of 2026-11-20 (STOXX Europe 600 "
                                                 "3.0% under its 50-close average)")
    assert mc.describe("cn", "calm", m, None) == "the Hong Kong market calm as of n/a"


# ---------------------------------------------------------------- M7
def test_m7_version_pin_and_import_hygiene():
    assert mc.__version__ == "1.0.0"
    vd = (_REPO / "scripts" / "verify_deployment.py").read_text(encoding="utf-8")
    assert '("core.market_condition", "__version__", "1.0.0", "market condition"),' in vd
    src = (_REPO / "core" / "market_condition.py").read_text(encoding="utf-8")
    import ast
    tree = ast.parse(src)
    mods = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            mods.update(a.name.split(".")[0] for a in node.names)
        elif isinstance(node, ast.ImportFrom):
            mods.add((node.module or "").split(".")[0])
    assert mods <= {"__future__", "datetime", "json", "math", "os", "re", "threading", "time", "urllib", "typing"}
    assert not any(t.name == "tfb-market-condition" for t in threading.enumerate())
    assert mc.MARKETS["sa"][0] == "^TASI.SR" and mc.MARKETS["us"][0] == "SPY"


if __name__ == "__main__":
    raise SystemExit(pytest.main([__file__, "-q"]))
