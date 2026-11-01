#!/usr/bin/env python3
"""tests/test_calendar_last_report_yahoo.py - calendar_provider v1.4.0
(TFB_CAL_LAST_REPORT_YAHOO, default off = v1.3.2 byte for byte).

WHY (2026-11-01): fetch_last_earnings_map skips every KSA symbol (EODHD has
no Saudi coverage) and no Yahoo path reads PAST reports, so a Saudi name
such as 2222.SR never gets a Last Earnings Date on Calendar_Events and the
post-earnings quarantine (16_Decision_Top10 v1.18.0 tag, 10_My_Portfolio
PD_POST_EARN / D1b) cannot hold a KSA Top_10 seat or a KSA holding's ADD.
v1.4.0 fills
the KSA rows' last report from yfinance get_earnings_dates when the gate is
on.

REAL module; the only stubs are the EODHD HTTP seam (_get_json), the clock
(_today), is_enabled, and yfinance (a duck-typed Ticker / frame; Y12 builds
its frame with the REAL yfinance get_earnings_dates parser from a canned
calendar page when yfinance is installed). No network.

Y1   off (unset / off / 0 / blank / junk): results, EODHD requests, zero
     yfinance calls and log lines equal the untouched v1.3.2 (4c2bd640), on a
     mixed list and on a KSA-only list
Y2   on, mixed list: 2222.SR gets the LATEST earnings row in the window
     (the future row, the meeting row and the older in-window row are not
     taken; the window edges are Y13's); the US rows and the EODHD requests
     equal the off run; 'source' only on KSA rows; the summary line
Y3   on, KSA-only list (a bare 4-digit code too): filled with no EODHD
     request; the bare code is asked as '<code>.SR'
Y4   dates on the forward tier's basis: a tz-aware timestamp's own calendar
     date (23:00 New York stays that New York day), naive, date, text, NaT
Y5   surprise: Surprise(%) wins (rounded 0.1); else (actual - estimate) /
     |estimate|; estimate 0 or no actual -> None; NaN is not a number
Y6   a rate-limit exception is counted (throttled=1) and drops only that
     symbol; any other error drops only that symbol (errors=1); never raises;
     Y8: a timeout is counted (errors=1); Y13: no row in the window reads
     '0 of 1 KSA (throttled=0, errors=0): -'
Y7   yfinance not importable: one line, the EODHD rows unchanged
Y8   a per-symbol timeout drops only that symbol
Y9   fill-only: a row already in the map is never overwritten
Y10  Event Type: 'Earnings' / 'Call' and the raw codes 2 / 1 (also 2.0)
     count; 'Meeting' / 11 never; absent or NaN counts. Event Name: a name
     reading as a meeting (meeting, assembly, AGM, EGM) never counts
Y11  end to end with the REAL run_calendar_sync merge_last_report and
     last_report_cells: the 2222.SR row reads '2026-11-02 | 3 | -6.5'
Y12  the REAL yfinance get_earnings_dates parser (cache_get canned, no
     network) on a calendar page with and without an Event Name column: on
     0.2.66 no Event Type column and a New York index; a later meeting row
     never replaces the report - skipped without yfinance
Y13  window edges [today - 21, today - 1]: today out, yesterday in, the
     start day in, the day before it out; only out-of-window rows -> None
Y14  hand-off: over consecutive syncs the same New York evening row is in
     Next Earnings Date (REAL _yf_earnings_dates_future) or in Last Earnings
     Date on every day - no day in neither
Y15  with no logging configured (run_calendar_sync), the summary and the
     yfinance-missing lines reach stderr once; with an INFO handler they go
     to the logger only; a handler at WARNING, or INFO with no handler, also
     means stderr (Y15b)
Y17  a frame holding a NaT date is refused and counted (errors=1): the REAL
     0.2.66 parser shifts every date after a dateless row onto the row above
     (Y17b, skipped without yfinance)
Y16  a row with EPS figures beats a later call or unnamed no-figure row
     (at most 21 days on); further on, or with no figured row, the latest
     row wins (a newer report not filled in yet is not lost)
G1   golden negative: Y2, Y3 and Y11 fail on the untouched v1.3.2 with the
     gate on (2222.SR stays absent)
V1   version floor, verifier pin, calendar_sync step env maps the gate with
     default 'off' (step env only), ci.yml runs this file in the contract
     lane, WHY block above the v1.3.2 block
V1b  against the base: ASCII added lines; exactly the five functions added,
     none removed

Base copy: TFB_TEST_CALENDAR_BASE=<path>, else `git show
4c2bd640:core/providers/calendar_provider.py`; without either, Y1, G1 and
V1b SKIP visibly. Golden negative of the whole file:
TFB_TEST_CALENDAR_DELIVERED=<base copy> runs every case against v1.3.2: the
WORKS cases fail on behaviour, the helper and version cases because the
helpers and the version do not exist there.

Run: python3 tests/test_calendar_last_report_yahoo.py   (prints PASS k/k)
  or python3 -m pytest -q tests/test_calendar_last_report_yahoo.py
"""
from __future__ import annotations

import asyncio
import datetime as dt
import importlib.util
import logging
import os
import re
import subprocess
import sys
import tempfile
import time
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import core.providers.calendar_provider as _CP_REAL  # noqa: E402

BASE_SHA = "4c2bd640"   # main 2026-10-27; calendar_provider v1.3.2 there
GATE = "TFB_CAL_LAST_REPORT_YAHOO"
TODAY = dt.date(2026, 11, 5)               # window [2026-10-15, 2026-11-04]
NY = dt.timezone(dt.timedelta(hours=-5))
RIYADH = dt.timezone(dt.timedelta(hours=3))
NAN = float("nan")
_BASE = {}


def _load(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


_DELIVERED = os.getenv("TFB_TEST_CALENDAR_DELIVERED", "").strip()
CP = _load("calendar_provider_delivered", _DELIVERED) if _DELIVERED else _CP_REAL


def _base_or_none():
    if "mod" not in _BASE:
        path = os.getenv("TFB_TEST_CALENDAR_BASE", "").strip()
        if not path:
            try:
                blob = subprocess.run(["git", "-C", str(ROOT), "show",
                                       BASE_SHA + ":core/providers/calendar_provider.py"],
                                      capture_output=True, timeout=60)
            except Exception:
                blob = None
            if blob is not None and blob.returncode == 0 and b'__version__ = "1.3.2"' in blob.stdout:
                tmp = Path(tempfile.mkdtemp()) / "calendar_provider_v132_base.py"
                tmp.write_bytes(blob.stdout)
                path = str(tmp)
        _BASE["mod"] = _load("calendar_provider_v132_base", path) if path else None
    return _BASE["mod"]


def base():
    if _base_or_none() is None:
        pytest.skip("base copy unavailable (set TFB_TEST_CALENDAR_BASE)")
    return _BASE["mod"]


# ------------------------------------------------------------- fakes -------
class Frame:
    """The slice of a pandas frame the provider reads: .index and .iloc[pos]."""

    def __init__(self, rows):
        self.index = [r[0] for r in rows]
        self._rows = [r[1] for r in rows]
        self.iloc = self

    def __getitem__(self, pos):
        return self._rows[pos]


def er(when, est=None, rep=None, sur=None, kind="Earnings"):
    row = {"EPS Estimate": est, "Reported EPS": rep, "Surprise(%)": sur}
    if kind is not None:
        row["Event Type"] = kind
    return (when, row)


ARAMCO = Frame([
    er(dt.datetime(2027, 2, 10, 7, 0, tzinfo=RIYADH), est=0.5),                 # future
    er(dt.datetime(2026, 11, 2, 23, 30, tzinfo=NY), est=0.46, rep=0.43),        # 3 Nov Riyadh; NY date
    er(dt.datetime(2026, 10, 28, 10, 0, tzinfo=RIYADH), kind="Meeting"),        # not a report
    er(dt.datetime(2026, 10, 20, 7, 0, tzinfo=RIYADH), est=0.4, rep=0.44, sur=10.04),  # older, in window
    er(dt.datetime(2026, 8, 4, 7, 0, tzinfo=RIYADH), est=0.48, rep=0.5, sur=4.2),     # outside
])
ARAMCO_ROW = {"last_earnings_date": "2026-11-02", "eps_surprise_pct": -6.5, "eps_actual": 0.43,
              "eps_estimate": 0.46, "report_timing": None, "source": "yahoo_earnings_dates"}


class YFRateLimitError(Exception):
    pass


class FakeYF:
    """yfinance stand-in: Ticker(sym).get_earnings_dates(limit) -> a frame,
    or raises / sleeps per symbol. Records every call."""

    def __init__(self, frames=None, errors=None, sleeps=None):
        self.frames, self.errors, self.sleeps = frames or {}, errors or {}, sleeps or {}
        self.calls = []
        outer = self

        class _Ticker:
            def __init__(self, sym):
                self.sym = sym

            def get_earnings_dates(self, limit=12):
                outer.calls.append((self.sym, limit))
                if self.sym in outer.sleeps:
                    time.sleep(outer.sleeps[self.sym])
                if self.sym in outer.errors:
                    raise outer.errors[self.sym]
                return outer.frames.get(self.sym)

            @property
            def calendar(self):
                outer.calls.append((self.sym, "calendar"))
                return {}

        self.Ticker = _Ticker


EODHD = {
    "FRST.US": [("2026-10-22", {"actual": 0.35, "estimate": 0.45, "percent": None,
                                "before_after_market": "AfterMarket"})],
    "ZTO.US": [("2026-10-20", {"actual": 2.1, "estimate": 2.0, "percent": 5.0})],
}


def _fake_get_json(calls):
    async def fake(_client, path, params):
        syms = params["symbols"].split(",")
        calls.append((path, params["from"], params["to"], tuple(syms)))
        out = []
        for s in syms:
            for d, extra in EODHD.get(s, []):
                if params["from"] <= d <= params["to"]:
                    out.append(dict({"code": s, "report_date": d}, **extra))
        return {"earnings": out}
    return fake


def _prep(mod, monkeypatch, yf, gate):
    calls = []
    monkeypatch.setattr(mod, "is_enabled", lambda: True)
    monkeypatch.setattr(mod, "_today", lambda: TODAY)
    monkeypatch.setattr(mod, "_get_json", _fake_get_json(calls))
    monkeypatch.setattr(mod, "_yf", yf)
    for k in ("TFB_CALENDAR_DAYS_BACK", "TFB_CALENDAR_EARNINGS_CHUNK", "TFB_CALENDAR_SPLIT_404",
              "TFB_CALENDAR_SPLIT_MAX_CALLS", "TFB_CALENDAR_VENUE_BATCHES", "TFB_CALENDAR_SUFFIX_MAP",
              "TFB_CALENDAR_CONCURRENCY", "TFB_CALENDAR_TIMEOUT_SEC"):
        monkeypatch.delenv(k, raising=False)
    if gate is None:
        monkeypatch.delenv(GATE, raising=False)
    else:
        monkeypatch.setenv(GATE, gate)
    return calls


def _logs(caplog, mod):
    return [(r.levelno, r.getMessage().replace("v" + mod.__version__, "vV")) for r in caplog.records
            if "calendar_provider" in r.getMessage()]


def _run(mod, monkeypatch, caplog, symbols, gate, yf=None):
    yf = yf if yf is not None else FakeYF({"2222.SR": ARAMCO, "5110.SR": ARAMCO})
    calls = _prep(mod, monkeypatch, yf, gate)
    caplog.clear()
    with caplog.at_level(logging.DEBUG, logger="core.providers.calendar_provider"):
        out = mod.fetch_last_earnings_sync(list(symbols))
    return out, calls, yf.calls, _logs(caplog, mod)


MIXED = ["FRST.US", "2222.SR", "ZTO.US"]
KSA_ONLY = ["2222.SR", "5110"]


# ------------------------------------------------------- WORKS checks ------
def check_y2(mod, monkeypatch, caplog):
    off, off_calls, _y, _l = _run(mod, monkeypatch, caplog, MIXED, None)
    out, calls, ycalls, logs = _run(mod, monkeypatch, caplog, MIXED, "on")
    assert out["2222.SR"] == ARAMCO_ROW, out.get("2222.SR")
    assert {k: v for k, v in out.items() if k != "2222.SR"} == off
    assert all("source" not in v for k, v in out.items() if k != "2222.SR")
    assert calls == off_calls and ycalls == [("2222.SR", 12)]
    assert (logging.INFO, "[calendar_provider vV] last-report yahoo fill: 1 of 1 KSA "
                          "(throttled=0, errors=0): 2222.SR") in logs


def check_y3(mod, monkeypatch, caplog):
    out, calls, ycalls, _logs_ = _run(mod, monkeypatch, caplog, KSA_ONLY, "on")
    assert out == {"2222.SR": ARAMCO_ROW, "5110": ARAMCO_ROW}, out
    assert calls == [] and sorted(ycalls) == [("2222.SR", 12), ("5110.SR", 12)]


def check_y11(mod, monkeypatch, caplog):
    sync = _load("calendar_sync_for_yahoo_fill", ROOT / "scripts" / "run_calendar_sync.py")
    out, _c, _y, _l = _run(mod, monkeypatch, caplog, MIXED, "on")
    monkeypatch.setattr(sync, "_today_riyadh", lambda: TODAY)
    merged, n_fresh, n_carried = sync.merge_last_report(MIXED, out, {}, 21)
    assert n_fresh == 3 and n_carried == 0, (n_fresh, n_carried)
    cells = sync.last_report_cells("2222.SR", merged)
    assert cells[0] == "2026-11-02" and cells[1] == 3 and cells[2] == -6.5, cells
    assert sync.last_report_cells("FRST.US", merged)[2] == -22.2


def test_y2_on_mixed_list_fills_the_saudi_row(monkeypatch, caplog):
    check_y2(CP, monkeypatch, caplog)


def test_y3_on_ksa_only_list(monkeypatch, caplog):
    check_y3(CP, monkeypatch, caplog)


@pytest.mark.parametrize("when,want", [
    (dt.datetime(2026, 11, 2, 23, 30, tzinfo=NY), dt.date(2026, 11, 2)),
    (dt.datetime(2026, 11, 2, 21, 0, tzinfo=dt.timezone.utc), dt.date(2026, 11, 2)),
    (dt.datetime(2026, 11, 3, 1, 0, tzinfo=RIYADH), dt.date(2026, 11, 3)),
    (dt.datetime(2026, 11, 2, 23, 30), dt.date(2026, 11, 2)),
    (dt.date(2026, 11, 2), dt.date(2026, 11, 2)),
    ("2026-11-02 07:00:00+03:00", dt.date(2026, 11, 2)),
    ("garbage", None),
    (None, None),
])
def test_y4_dates_on_the_forward_tier_basis(when, want):
    assert CP._yf_report_date(when) == want


def test_y4b_nat_is_no_date():
    pd = pytest.importorskip("pandas")
    assert CP._yf_report_date(pd.NaT) is None
    assert CP._yf_report_date(pd.Timestamp("2026-11-02 23:00", tz="America/New_York")) == \
        dt.date(2026, 11, 2)


@pytest.mark.parametrize("rep,est,sur,want", [
    (0.43, 0.46, 3.14159, 3.1),
    (0.43, 0.46, None, -6.5),
    (0.43, 0.46, NAN, -6.5),
    (0.43, 0.0, None, None),
    (None, 0.46, None, None),
    (NAN, 0.46, NAN, None),
    (0.5, -0.4, None, 225.0),
])
def test_y5_surprise(monkeypatch, rep, est, sur, want):
    frame = Frame([er(dt.datetime(2026, 11, 3, 7, 0, tzinfo=RIYADH), est=est, rep=rep, sur=sur)])
    monkeypatch.setattr(CP, "_yf", FakeYF({"2222.SR": frame}))
    row = CP._yf_earnings_dates_past("2222.SR", dt.date(2026, 10, 15), dt.date(2026, 11, 4))
    assert row["eps_surprise_pct"] == want, row
    assert row["eps_actual"] == CP._num(rep) and row["eps_estimate"] == CP._num(est)


def test_y6_throttle_counted_other_errors_dropped(monkeypatch, caplog):
    yf = FakeYF({"2222.SR": ARAMCO}, errors={"5110.SR": YFRateLimitError("Too Many Requests"),
                                             "4321.SR": RuntimeError("boom")})
    out, _c, _y, logs = _run(CP, monkeypatch, caplog, ["2222.SR", "5110.SR", "4321.SR"], "on", yf)
    assert out == {"2222.SR": ARAMCO_ROW}
    assert (logging.INFO, "[calendar_provider vV] last-report yahoo fill: 1 of 3 KSA "
                          "(throttled=1, errors=1): 2222.SR") in logs


def test_y7_yfinance_missing(monkeypatch, caplog):
    calls = _prep(CP, monkeypatch, None, "on")
    caplog.clear()
    with caplog.at_level(logging.INFO, logger="core.providers.calendar_provider"):
        out = CP.fetch_last_earnings_sync(MIXED)
    assert set(out) == {"FRST.US", "ZTO.US"} and len(calls) == 1
    assert (logging.INFO, "[calendar_provider vV] last-report yahoo fill unavailable (yfinance "
                          "not importable) - 1 KSA symbol(s) stay blank") in _logs(caplog, CP)


def test_y8_timeout_drops_only_that_symbol(monkeypatch, caplog):
    yf = FakeYF({"2222.SR": ARAMCO, "5110.SR": ARAMCO}, sleeps={"5110.SR": 3.0})
    calls = _prep(CP, monkeypatch, yf, "on")
    monkeypatch.setenv("TFB_CALENDAR_TIMEOUT_SEC", "2")
    caplog.clear()
    with caplog.at_level(logging.DEBUG, logger="core.providers.calendar_provider"):
        out = CP.fetch_last_earnings_sync(["2222.SR", "5110.SR"])
    assert out == {"2222.SR": ARAMCO_ROW} and calls == []
    assert (logging.INFO, "[calendar_provider vV] last-report yahoo fill: 1 of 2 KSA "
                          "(throttled=0, errors=1): 2222.SR") in _logs(caplog, CP)


def test_y9_fill_only(monkeypatch):
    monkeypatch.setattr(CP, "_yf", FakeYF({"2222.SR": ARAMCO, "5110.SR": ARAMCO}))
    keep = {"last_earnings_date": "2026-10-30", "eps_surprise_pct": 1.0, "eps_actual": 1.0,
            "eps_estimate": 1.0, "report_timing": "BeforeMarket"}
    out = {"2222.SR": dict(keep)}
    n = asyncio.run(CP._yahoo_last_report_fill(["2222.SR", "5110.SR", "5110.SR"],
                                               dt.date(2026, 10, 15), dt.date(2026, 11, 4), out))
    assert n == 1 and out == {"2222.SR": keep, "5110.SR": ARAMCO_ROW}


@pytest.mark.parametrize("kind,counts", [
    ("Earnings", True), ("Call", True), (2, True), (1, True), (2.0, True), ("2", True),
    (None, True), (NAN, True), ("Meeting", False), (11, False), ("11", False), (3, False),
])
def test_y10_event_type(monkeypatch, kind, counts):
    frame = Frame([er(dt.datetime(2026, 11, 3, 7, 0, tzinfo=RIYADH), est=0.46, rep=0.43, kind=kind),
                   er(dt.datetime(2026, 10, 20, 7, 0, tzinfo=RIYADH), est=0.4, rep=0.44, sur=10.04)])
    monkeypatch.setattr(CP, "_yf", FakeYF({"2222.SR": frame}))
    row = CP._yf_earnings_dates_past("2222.SR", dt.date(2026, 10, 15), dt.date(2026, 11, 4))
    assert row["last_earnings_date"] == ("2026-11-03" if counts else "2026-10-20"), (kind, row)


@pytest.mark.parametrize("name,counts", [
    ("Q3 2026 Earnings Release", True), ("Saudi Arabian Oil Co Earnings Call", True),
    (NAN, True), (None, True),
    ("Saudi Arabian Oil Co Annual General Meeting", False),
    ("Extraordinary General Assembly", False), ("AGM", False), ("2026 EGM", False),
    ("Shareholders' Meeting", False),
])
def test_y10b_event_name(monkeypatch, name, counts):
    first = er(dt.datetime(2026, 11, 3, 7, 0, tzinfo=RIYADH), est=0.46, rep=0.43, kind=None)
    if name is not None:
        first[1]["Event Name"] = name
    frame = Frame([first,
                   er(dt.datetime(2026, 10, 20, 7, 0, tzinfo=RIYADH), est=0.4, rep=0.44, sur=10.04)])
    monkeypatch.setattr(CP, "_yf", FakeYF({"2222.SR": frame}))
    row = CP._yf_earnings_dates_past("2222.SR", dt.date(2026, 10, 15), dt.date(2026, 11, 4))
    assert row["last_earnings_date"] == ("2026-11-03" if counts else "2026-10-20"), (name, row)


def test_y11_end_to_end_sync_cells(monkeypatch, caplog):
    check_y11(CP, monkeypatch, caplog)


_PAGE_ROWS = [   # (Event Name, Earnings Date, EPS Estimate, Reported EPS, Surprise (%))
    ("Q4 2026 Earnings Release", "February 9, 2027 at 11 PM EST", "0.5", "-", "-"),
    ("Saudi Arabian Oil Co Extraordinary General Meeting", "November 4, 2026 at 3 AM EST",
     "-", "-", "-"),
    ("Q3 2026 Earnings Release", "November 2, 2026 at 11 PM EST", "0.46", "0.43", "-6.52"),
    ("Q2 2026 Earnings Release", "August 3, 2026 at 11 PM EDT", "0.48", "0.5", "4.2"),
]


def _page(with_name, rows=None):
    head = ["Symbol", "Company"] + (["Event Name"] if with_name else []) + [
        "Earnings Date", "EPS Estimate", "Reported EPS", "Surprise (%)"]
    body = []
    for name, when, est, rep_, sur in (rows or _PAGE_ROWS):
        cells = ["2222.SR", "Saudi Arabian Oil Co"] + ([name] if with_name else []) + [
            when, est, rep_, sur]
        body.append("<tr>" + "".join("<td>%s</td>" % c for c in cells) + "</tr>")
    return ("<html><body><table><thead><tr>" + "".join("<th>%s</th>" % h for h in head)
            + "</tr></thead><tbody>" + "".join(body) + "</tbody></table></body></html>")


def _real_parser_frame(monkeypatch, with_name, rows=None):
    yf = pytest.importorskip("yfinance")
    pytest.importorskip("pandas")

    class _Resp:
        text = _page(with_name, rows)
    t = yf.Ticker("2222.SR")
    # the only seam, no network; monkeypatch restores YfData (a process-wide
    # singleton) so no later test sees the canned page
    monkeypatch.setattr(t._data, "cache_get", lambda *_a, **_k: _Resp())
    return yf, t.get_earnings_dates(limit=12)


@pytest.mark.parametrize("with_name", [True, False])
def test_y12_real_yfinance_parser_frame(monkeypatch, caplog, with_name):
    yf, df = _real_parser_frame(monkeypatch, with_name)
    if yf.__version__ == "0.2.66":                    # requirements.txt pin
        assert "Event Type" not in df.columns and str(df.index.tz) == "America/New_York"
        assert ("Event Name" in df.columns) is with_name
    out, _c, _y, _l = _run(CP, monkeypatch, caplog, KSA_ONLY, "on", FakeYF({"2222.SR": df}))
    want = dict(ARAMCO_ROW, eps_actual=0.43, eps_estimate=0.46)
    assert out == {"2222.SR": want}, out


@pytest.mark.parametrize("offsets,want", [   # days from TODAY (2026-11-05)
    ((0, -1, -16), "2026-11-04"),       # today out, yesterday in and latest
    ((97, -93), None),                  # a future row and one long past only
    ((-21,), "2026-10-15"),             # the start day is in
    ((-22,), None),                     # the day before it is out
    ((0,), None),                       # today only
])
def test_y13_window_edges(monkeypatch, caplog, offsets, want):
    frame = Frame([er(dt.datetime.combine(TODAY + dt.timedelta(days=n), dt.time(7, 0), RIYADH),
                      est=0.4, rep=0.44, sur=10.0) for n in offsets])
    monkeypatch.setattr(CP, "_yf", FakeYF({"2222.SR": frame}))
    row = CP._yf_earnings_dates_past("2222.SR", dt.date(2026, 10, 15), dt.date(2026, 11, 4))
    assert (row or {}).get("last_earnings_date") == want, (offsets, row)
    out, _c, _y, logs = _run(CP, monkeypatch, caplog, ["2222.SR"], "on", FakeYF({"2222.SR": frame}))
    assert (out.get("2222.SR") or {}).get("last_earnings_date") == want, (offsets, out)
    if want is None:   # no report in the window is not an error
        assert (logging.INFO, "[calendar_provider vV] last-report yahoo fill: 0 of 1 KSA "
                              "(throttled=0, errors=0): -") in logs, logs


def test_y14_next_to_last_hand_off_has_no_gap(monkeypatch, caplog):
    frame = Frame([er(dt.datetime(2027, 2, 9, 23, 0, tzinfo=NY), est=0.5),
                   er(dt.datetime(2026, 11, 2, 23, 0, tzinfo=NY), est=0.46, rep=0.43, kind=None)])
    seen = []
    for day in (1, 2, 3, 4):
        today = dt.date(2026, 11, day)
        yf = FakeYF({"2222.SR": frame})
        _prep(CP, monkeypatch, yf, "on")
        monkeypatch.setattr(CP, "_today", lambda today=today: today)
        nxt = CP._yf_earnings_dates_future("2222.SR")
        last = (CP.fetch_last_earnings_sync(["2222.SR"]).get("2222.SR") or {}).get(
            "last_earnings_date")
        in_next = nxt == dt.date(2026, 11, 2)
        in_last = last == "2026-11-02"
        seen.append((day, in_next, in_last))
        assert in_next != in_last, seen
    assert seen == [(1, True, False), (2, True, False), (3, False, True), (4, False, True)], seen


def test_y15_summary_reaches_stderr_without_logging(monkeypatch, capsys, caplog):
    real = CP.logger
    bare = logging.getLogger("tfb.test.calendar_provider.bare")
    bare.handlers, bare.propagate = [], False
    bare.setLevel(logging.WARNING)
    monkeypatch.setattr(CP, "logger", bare)
    out, _c, _y, _l = _run(CP, monkeypatch, caplog, MIXED, "on")
    err = capsys.readouterr().err.replace("v" + CP.__version__, "vV")
    assert out["2222.SR"] == ARAMCO_ROW
    assert err.splitlines().count("[calendar_provider vV] last-report yahoo fill: 1 of 1 KSA "
                                  "(throttled=0, errors=0): 2222.SR") == 1, err
    _prep(CP, monkeypatch, None, "on")
    CP.fetch_last_earnings_sync(MIXED)
    err = capsys.readouterr().err.replace("v" + CP.__version__, "vV")
    assert err.splitlines() == ["[calendar_provider vV] last-report yahoo fill unavailable "
                                "(yfinance not importable) - 1 KSA symbol(s) stay blank"], err
    monkeypatch.setattr(CP, "logger", real)
    _run(CP, monkeypatch, caplog, ["2222.SR"], "on")
    assert "last-report yahoo fill" not in capsys.readouterr().err   # handler present: logger only


@pytest.mark.parametrize("handler,level", [(True, logging.WARNING), (False, logging.INFO)])
def test_y15b_no_handler_taking_info_means_stderr(monkeypatch, capsys, handler, level):
    lg = logging.getLogger("tfb.test.calendar_provider.y15b.%s" % handler)
    lg.handlers, lg.propagate = ([logging.NullHandler()] if handler else []), False
    lg.setLevel(level)
    monkeypatch.setattr(CP, "logger", lg)
    monkeypatch.setattr(CP, "_yf", None)
    asyncio.run(CP._yahoo_last_report_fill(["2222.SR"], dt.date(2026, 10, 15),
                                           dt.date(2026, 11, 4), {}))
    assert "last-report yahoo fill unavailable" in capsys.readouterr().err


def test_y17_a_frame_with_a_nat_date_is_refused(monkeypatch, caplog):
    frame = Frame([er(dt.datetime(2026, 11, 2, 23, 0, tzinfo=NY), est=0.46, rep=0.43, kind=None),
                   er(NAN, est=0.48, rep=0.5, sur=4.2, kind=None)])
    monkeypatch.setattr(CP, "_yf", FakeYF({"2222.SR": frame}))
    with pytest.raises(ValueError):
        CP._yf_earnings_dates_past("2222.SR", dt.date(2026, 10, 15), dt.date(2026, 11, 4))
    out, _c, _y, logs = _run(CP, monkeypatch, caplog, ["2222.SR"], "on", FakeYF({"2222.SR": frame}))
    assert out == {}
    assert (logging.INFO, "[calendar_provider vV] last-report yahoo fill: 0 of 1 KSA "
                          "(throttled=0, errors=1): -") in logs, logs


def test_y17b_real_parser_shifts_dates_after_a_dateless_row(monkeypatch, caplog):
    rows = [list(r) for r in _PAGE_ROWS]
    rows[1][1] = "-"                                  # the EGM row loses its date
    yf, df = _real_parser_frame(monkeypatch, True, [tuple(r) for r in rows])
    if yf.__version__ == "0.2.66":                    # the shift this case guards against
        names = list(df["Event Name"])
        q3 = names.index("Q3 2026 Earnings Release")
        assert str(df.index[q3])[:10] == "2026-08-03" and str(df.index[-1]) == "NaT", df
    out, _c, _y, logs = _run(CP, monkeypatch, caplog, KSA_ONLY, "on", FakeYF({"2222.SR": df}))
    if df.index.hasnans:
        assert out == {} and any("errors=1" in m for _lv, m in logs), (out, logs)
    else:                                             # a yfinance that keeps dates in place
        assert out.get("2222.SR", {}).get("last_earnings_date") == "2026-11-02", out


@pytest.mark.parametrize("rows,want,sur", [
    ([er(dt.datetime(2026, 11, 3, 7, 0, tzinfo=RIYADH), est=0.46, kind="Call"),
      er(dt.datetime(2026, 11, 2, 7, 0, tzinfo=RIYADH), est=0.46, rep=0.43, kind="Earnings")],
     "2026-11-02", -6.5),
    ([er(dt.datetime(2026, 11, 4, 3, 0, tzinfo=NY), kind=None),
      er(dt.datetime(2026, 11, 2, 23, 0, tzinfo=NY), est=0.46, rep=0.43, kind=None)],
     "2026-11-02", -6.5),
    ([er(dt.datetime(2026, 11, 3, 7, 0, tzinfo=RIYADH), est=0.46, sur=-6.52, kind=None),
      er(dt.datetime(2026, 10, 20, 7, 0, tzinfo=RIYADH), est=0.4, rep=0.44, kind=None)],
     "2026-11-03", -6.5),
    ([er(dt.datetime(2026, 11, 3, 7, 0, tzinfo=RIYADH), est=0.46, kind=None),
      er(dt.datetime(2026, 10, 1, 7, 0, tzinfo=RIYADH), est=0.4, rep=0.44, kind=None)],
     "2026-11-03", None),
])
def test_y16_a_row_with_figures_beats_a_later_one_without(monkeypatch, rows, want, sur):
    monkeypatch.setattr(CP, "_yf", FakeYF({"2222.SR": Frame(rows)}))
    row = CP._yf_earnings_dates_past("2222.SR", dt.date(2026, 10, 15), dt.date(2026, 11, 4))
    assert (row["last_earnings_date"], row["eps_surprise_pct"]) == (want, sur), row


@pytest.mark.parametrize("gap,want", [(21, "2026-10-14"), (22, "2026-11-04"), (92, "2026-11-04")])
def test_y16b_a_newer_report_without_figures_is_not_lost(monkeypatch, gap, want):
    # TFB_CALENDAR_DAYS_BACK can reach 120: last quarter's figured row must not
    # beat a report filed yesterday whose figures Yahoo has not filled in yet.
    rows = [er(dt.datetime(2026, 11, 4, 7, 0, tzinfo=RIYADH), est=0.46, kind=None),
            er(dt.datetime.combine(dt.date(2026, 11, 4) - dt.timedelta(days=gap), dt.time(7, 0),
                                   RIYADH), est=0.4, rep=0.44, kind=None)]
    monkeypatch.setattr(CP, "_yf", FakeYF({"2222.SR": Frame(rows)}))
    row = CP._yf_earnings_dates_past("2222.SR", dt.date(2026, 7, 7), dt.date(2026, 11, 4))
    assert row["last_earnings_date"] == want, (gap, row)


# ------------------------------------------------------------- NEVER -------
@pytest.mark.parametrize("gate", [None, "off", "0", "", "  ", "onn", "enforce", "observe"])
def test_y1_off_is_v132(monkeypatch, caplog, gate):
    b = base()
    for symbols in (MIXED, KSA_ONLY, ["FRST.US"]):
        mine = _run(CP, monkeypatch, caplog, symbols, gate)
        theirs = _run(b, monkeypatch, caplog, symbols, gate)
        assert mine == theirs, (gate, symbols)
        assert mine[2] == [], "no yfinance call when off"
    assert CP._last_report_yahoo_enabled() is False


@pytest.mark.parametrize("gate", ["on", "ON", " on ", "1", "true", "Yes"])
def test_y1b_gate_words(monkeypatch, gate):
    monkeypatch.setenv(GATE, gate)
    assert CP._last_report_yahoo_enabled() is True


def test_y1c_disabled_layer_stays_empty(monkeypatch):
    monkeypatch.setenv(GATE, "on")
    yf = FakeYF({"2222.SR": ARAMCO})
    monkeypatch.setattr(CP, "_yf", yf)
    monkeypatch.setattr(CP, "is_enabled", lambda: False)
    assert CP.fetch_last_earnings_sync(MIXED) == {} and yf.calls == []


# ---------------------------------------------------------------- GOLD -----
def test_g1_golden_negative_base_leaves_the_saudi_row_blank(monkeypatch, caplog):
    b = base()
    failed = []
    for check in (check_y2, check_y3, check_y11):
        try:
            check(b, monkeypatch, caplog)
        except (AssertionError, KeyError, AttributeError):
            failed.append(check.__name__)
    assert failed == ["check_y2", "check_y3", "check_y11"], failed
    out, _c, ycalls, _l = _run(b, monkeypatch, caplog, MIXED, "on")
    assert "2222.SR" not in out and ycalls == []


# ------------------------------------------------------------------- V -----
MAPPING = "${{ vars.TFB_CAL_LAST_REPORT_YAHOO || 'off' }}"


def test_v1_version_pin_workflow_ci_why():
    assert tuple(int(x) for x in CP.__version__.split(".")) >= (1, 4, 0)
    vd = (ROOT / "scripts" / "verify_deployment.py").read_text(encoding="utf-8")
    assert re.search(r'\("core\.providers\.calendar_provider", "__version__", "%s",'
                     % re.escape(CP.__version__), vd)
    wf_text = (ROOT / ".github" / "workflows" / "calendar_sync.yml").read_text(encoding="utf-8")
    # The contract lane has no PyYAML: the text form is checked always (exactly one
    # non-comment mapping line, at step-env depth), the parsed form when yaml exists.
    lines = [ln for ln in wf_text.splitlines()
             if GATE + ":" in ln and not ln.lstrip().startswith("#")]
    assert lines == ["          %s: %s" % (GATE, MAPPING)], lines
    try:
        import yaml
    except ImportError:
        yaml = None
    if yaml is not None:
        wf = yaml.safe_load(wf_text)
        assert GATE not in (wf.get("env") or {})
        hits = []
        for job in wf["jobs"].values():
            assert GATE not in (job.get("env") or {})
            for step in job.get("steps") or []:
                if GATE in (step.get("env") or {}):
                    hits.append((step.get("name"), step["env"][GATE]))
        assert hits == [("\U0001f4c5 Run calendar sync", MAPPING)], hits
    ci = (ROOT / ".github" / "workflows" / "ci.yml").read_text(encoding="utf-8")
    assert re.search(r"tests/test_calendar_last_report_a4\.py \\\s*\n(\s*tests/\S+ \\\s*\n)*"
                     r"\s*tests/test_calendar_last_report_yahoo\.py", ci)
    src = Path(CP.__file__).read_text(encoding="utf-8")
    assert src.index("v1.4.0 (2026-11-01) -- the last-report lane") < src.index("v1.3.2 (2026-10-27)")
    assert all(ord(c) < 128 for c in Path(__file__).read_text(encoding="utf-8"))


def test_v1b_against_the_base_ascii_and_functions():
    import ast
    import difflib
    b = base()
    if CP.__version__ != "1.4.0":
        pytest.skip("the exact 4c2bd640 -> 1.4.0 diff checks apply to 1.4.0 only")
    old = Path(b.__file__).read_text(encoding="utf-8").splitlines()
    new = Path(CP.__file__).read_text(encoding="utf-8").splitlines()
    diff = list(difflib.unified_diff(old, new, lineterm="", n=0))
    added = [ln[1:] for ln in diff if ln.startswith("+") and not ln.startswith("+++")]
    assert added and all(ln.isascii() for ln in added), [ln for ln in added if not ln.isascii()][:3]
    kinds = (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)

    def defs(text):
        return {n.name for n in ast.walk(ast.parse(text)) if isinstance(n, kinds)}

    def top(text):
        return {n.name for n in ast.parse(text).body if isinstance(n, kinds)}
    assert not defs("\n".join(old)) - defs("\n".join(new))
    assert top("\n".join(new)) - top("\n".join(old)) == {
        "_last_report_yahoo_enabled", "_yf_report_date", "_yf_row_value",
        "_yf_earnings_dates_past", "_yahoo_last_report_fill"}


if __name__ == "__main__":
    class _Tally:
        def __init__(self):
            self.passed = self.failed = self.skipped = 0

        def pytest_runtest_logreport(self, report):
            if report.when == "call" and report.passed:
                self.passed += 1
            elif report.skipped:
                self.skipped += 1
            elif report.failed:
                self.failed += 1

    tally = _Tally()
    rc = pytest.main([__file__, "-q", "-p", "no:cacheprovider"], plugins=[tally])
    run = tally.passed + tally.failed
    print("PASS %d/%d" % (tally.passed, run) + (" (%d skipped)" % tally.skipped if tally.skipped else ""))
    sys.exit(rc)
