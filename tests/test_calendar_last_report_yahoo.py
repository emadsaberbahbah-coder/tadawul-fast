#!/usr/bin/env python3
"""tests/test_calendar_last_report_yahoo.py - calendar_provider v1.4.0
(TFB_CAL_LAST_REPORT_YAHOO, default off = v1.3.2 byte for byte).

WHY (2026-11-01): fetch_last_earnings_map skips every KSA symbol (EODHD has
no Saudi coverage) and no Yahoo path reads PAST reports, so a Saudi holding
(2222.SR) never gets a Last Earnings Date on Calendar_Events and the
post-earnings quarantine (16_Decision_Top10 v1.18.0 tag, 10_My_Portfolio
PD_POST_EARN / D1b) cannot hold it. v1.4.0 fills the KSA rows' last report
from yfinance get_earnings_dates when the gate is on.

REAL module; the only stubs are the EODHD HTTP seam (_get_json), the clock
(_today), is_enabled, and yfinance (a duck-typed Ticker / frame; one case
uses a real pandas frame when pandas is installed). No network.

Y1   off (unset / off / 0 / blank / junk): results, EODHD requests, zero
     yfinance calls and log lines equal the untouched v1.3.2 (4c2bd640), on a
     mixed list and on a KSA-only list
Y2   on, mixed list: 2222.SR gets the LATEST earnings row in the window
     (future, older-than-window, today and meeting rows ignored); the US rows
     and the EODHD requests equal the off run; 'source' only on KSA rows
Y3   on, KSA-only list (a bare 4-digit code too): filled with no EODHD
     request; the bare code is asked as '<code>.SR'
Y4   dates: tz-aware timestamps are taken in Riyadh time; naive datetime,
     date and text forms
Y5   surprise: Surprise(%) wins (rounded 0.1); else (actual - estimate) /
     |estimate|; estimate 0 or no actual -> None; NaN is not a number
Y6   a rate-limit exception is counted (throttled=1) and drops only that
     symbol; any other error drops only that symbol, uncounted; never raises
Y7   yfinance not importable: one INFO line, the EODHD rows unchanged
Y8   a per-symbol timeout drops only that symbol
Y9   fill-only: a row already in the map is never overwritten
Y10  Event Type: 'Earnings' / 'Call' and the raw codes 2 / 1 (also 2.0)
     count; 'Meeting' / 11 never; absent or NaN counts
Y11  end to end with the REAL run_calendar_sync merge_last_report and
     last_report_cells: the 2222.SR row reads '2026-11-03 | 2 | -6.5'
Y12  a real pandas frame shaped like yfinance 0.2.66 (tz-aware index, raw
     integer event codes) - skipped without pandas
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
    er(dt.datetime(2026, 11, 2, 23, 30, tzinfo=NY), est=0.46, rep=0.43),        # = 3 Nov Riyadh
    er(dt.datetime(2026, 10, 28, 10, 0, tzinfo=RIYADH), kind="Meeting"),        # not a report
    er(dt.datetime(2026, 10, 20, 7, 0, tzinfo=RIYADH), est=0.4, rep=0.44, sur=10.04),  # older, in window
    er(dt.datetime(2026, 8, 4, 7, 0, tzinfo=RIYADH), est=0.48, rep=0.5, sur=4.2),     # outside
])
ARAMCO_ROW = {"last_earnings_date": "2026-11-03", "eps_surprise_pct": -6.5, "eps_actual": 0.43,
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
                          "(throttled=0): 2222.SR") in logs


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
    assert cells[0] == "2026-11-03" and cells[2] == -6.5, cells
    assert sync.last_report_cells("FRST.US", merged)[2] == -22.2


def test_y2_on_mixed_list_fills_the_saudi_row(monkeypatch, caplog):
    check_y2(CP, monkeypatch, caplog)


def test_y3_on_ksa_only_list(monkeypatch, caplog):
    check_y3(CP, monkeypatch, caplog)


@pytest.mark.parametrize("when,want", [
    (dt.datetime(2026, 11, 2, 23, 30, tzinfo=NY), dt.date(2026, 11, 3)),
    (dt.datetime(2026, 11, 2, 20, 59, tzinfo=dt.timezone.utc), dt.date(2026, 11, 2)),
    (dt.datetime(2026, 11, 2, 21, 0, tzinfo=dt.timezone.utc), dt.date(2026, 11, 3)),
    (dt.datetime(2026, 11, 2, 23, 30), dt.date(2026, 11, 2)),
    (dt.date(2026, 11, 2), dt.date(2026, 11, 2)),
    ("2026-11-02 07:00:00+03:00", dt.date(2026, 11, 2)),
    ("garbage", None),
    (None, None),
])
def test_y4_dates_are_riyadh_dates(when, want):
    assert CP._yf_report_date(when) == want


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
                          "(throttled=1): 2222.SR") in logs


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
    out = CP.fetch_last_earnings_sync(["2222.SR", "5110.SR"])
    assert out == {"2222.SR": ARAMCO_ROW} and calls == []


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


def test_y11_end_to_end_sync_cells(monkeypatch, caplog):
    check_y11(CP, monkeypatch, caplog)


def test_y12_real_pandas_frame(monkeypatch, caplog):
    pd = pytest.importorskip("pandas")
    idx = pd.DatetimeIndex([pd.Timestamp("2027-02-10 07:00", tz="Asia/Riyadh"),
                            pd.Timestamp("2026-11-03 07:30", tz="Asia/Riyadh"),
                            pd.Timestamp("2026-10-28 10:00", tz="Asia/Riyadh"),
                            pd.Timestamp("2026-08-04 07:00", tz="Asia/Riyadh")], name="Earnings Date")
    df = pd.DataFrame({"EPS Estimate": [0.5, 0.46, NAN, 0.48], "Reported EPS": [NAN, 0.43, NAN, 0.5],
                       "Surprise(%)": [NAN, NAN, NAN, 4.2], "Event Type": [2, 2, 11, 2]}, index=idx)
    out, _c, _y, _l = _run(CP, monkeypatch, caplog, KSA_ONLY, "on", FakeYF({"2222.SR": df}))
    assert out == {"2222.SR": ARAMCO_ROW}, out


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
def test_v1_version_pin_workflow_ci_why():
    import yaml
    assert tuple(int(x) for x in CP.__version__.split(".")) >= (1, 4, 0)
    vd = (ROOT / "scripts" / "verify_deployment.py").read_text(encoding="utf-8")
    assert re.search(r'\("core\.providers\.calendar_provider", "__version__", "%s",'
                     % re.escape(CP.__version__), vd)
    wf = yaml.safe_load((ROOT / ".github" / "workflows" / "calendar_sync.yml").read_text(encoding="utf-8"))
    assert GATE not in (wf.get("env") or {})
    hits = []
    for job in wf["jobs"].values():
        assert GATE not in (job.get("env") or {})
        for step in job.get("steps") or []:
            if GATE in (step.get("env") or {}):
                hits.append(step["env"][GATE])
    assert hits == ["${{ vars.TFB_CAL_LAST_REPORT_YAHOO || 'off' }}"], hits
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
