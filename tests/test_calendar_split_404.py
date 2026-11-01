"""calendar_provider v1.3.1: a 404 earnings batch is split so the codes EODHD
accepts are kept (TFB_CALENDAR_SPLIT_404, default off).

REAL module; the only stub is the provider's HTTP seam (_get_json), which
answers like EODHD did on 27 Oct 2026: HTTP 404 for the WHOLE
/calendar/earnings request when one code in `symbols` is unknown. No network.

S1  gate off: a 404 batch is dropped whole - requests, results and log lines
    equal the untouched v1.3.0 module (git show aba4e5eb) on both paths
S2  gate on, last report: one unknown code among eight -> split, the seven
    known codes keep their reports, the unknown one is named once
S3  gate on, forward: the same for next earnings (earliest upcoming kept)
S4  several unknown codes, the request cap (checked before every request -
    review fix, Codex P2 on #853), a non-404 error inside a split (the
    top-level batch is skipped exactly as v1.3.0 skips it)
S5  gate spellings; _is_http_404 on real httpx errors; the workflow arms it
"""
from __future__ import annotations

import importlib.util
import logging
from datetime import date
from pathlib import Path
import subprocess

import httpx
import pytest

ROOT = Path(__file__).resolve().parents[1]
BASE_REV = "aba4e5eb"   # main with calendar_provider v1.3.0 (#850)
TODAY = date(2026, 10, 27)
KNOWN = {"AAA.US", "BBB.US", "CCC.US", "DDD.US", "EEE.US", "FFF.US", "GGG.US", "HHH.US",
         "PHP.LSE", "WDP.BR", "1113.HK"}


def _load(path, name):
    spec = importlib.util.spec_from_file_location(name, path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def _base_provider():
    try:
        blob = subprocess.run(["git", "-C", str(ROOT), "show", BASE_REV + ":core/providers/calendar_provider.py"],
                              capture_output=True, timeout=60)
    except Exception:
        return None
    if blob.returncode != 0 or b'__version__ = "1.3.0"' not in blob.stdout:
        return None
    tmp = ROOT / ".pytest_cache_cal_split_base_provider.py"
    tmp.write_bytes(blob.stdout)
    try:
        return _load(tmp, "calendar_provider_v130_base")
    finally:
        tmp.unlink()


def _http_error(status):
    req = httpx.Request("GET", "https://eodhd.com/api/calendar/earnings")
    return httpx.HTTPStatusError(f"Client error '{status}'", request=req,
                                 response=httpx.Response(status, request=req))


def _fake(calls, reports, extra_fail=None):
    """EODHD-like seam: 404 for the whole request when any code is unknown;
    else the reports of the requested codes inside [from, to]."""
    async def fake_get_json(_client, path, params):
        syms = params["symbols"].split(",")
        calls.append((path, params["from"], params["to"], tuple(syms)))
        if extra_fail and extra_fail(syms):
            raise _http_error(429)
        if any(s not in KNOWN for s in syms):
            raise _http_error(404)
        out = []
        for s in syms:
            for d, extra in reports.get(s, []):
                if params["from"] <= d <= params["to"]:
                    out.append(dict({"code": s, "report_date": d}, **extra))
        return {"earnings": out}
    return fake_get_json


def _prep(mod, monkeypatch, calls, reports, gate=None, extra_fail=None):
    monkeypatch.setattr(mod, "is_enabled", lambda: True)
    monkeypatch.setattr(mod, "_today", lambda: TODAY)
    monkeypatch.setattr(mod, "_get_json", _fake(calls, reports, extra_fail))
    for k in ("TFB_CALENDAR_DAYS_BACK", "TFB_CALENDAR_EARNINGS_CHUNK", "TFB_CALENDAR_SPLIT_MAX_CALLS",
              "TFB_CALENDAR_DAYS_AHEAD"):
        monkeypatch.delenv(k, raising=False)
    if gate is None:
        monkeypatch.delenv("TFB_CALENDAR_SPLIT_404", raising=False)
    else:
        monkeypatch.setenv("TFB_CALENDAR_SPLIT_404", gate)


PAST = {s: [("2026-10-20", {"actual": 1.1, "estimate": 1.0})] for s in KNOWN}
PAST["AAA.US"] = [("2026-10-22", {"actual": 0.35, "estimate": 0.45})]
FWD = {s: [("2026-11-05", {}), ("2026-11-20", {})] for s in KNOWN}
EIGHT = ["AAA.US", "BBB.US", "CCC.US", "DDD.US", "BAD.L", "EEE.US", "FFF.US", "GGG.US"]


@pytest.fixture
def cp():
    import core.providers.calendar_provider as mod
    # 2026-10-27: v1.3.2 (venue batches + EODHD exchange codes, both gated
    # off by default) stacked on this change; pin moved
    # 2026-11-01: v1.4.0 (Saudi past reports from Yahoo, gated off by
    # default) stacked as well; the pin is a floor now
    assert tuple(int(x) for x in mod.__version__.split(".")) >= (1, 3, 2), mod.__version__
    return mod


# --------------------------------------------------------------------- S1
@pytest.mark.parametrize("gate", [None, "off", "0", "", "junk"])
def test_s1_gate_off_is_v130(cp, monkeypatch, caplog, gate):
    base = _base_provider()
    if base is None:
        pytest.skip("git show %s unavailable (shallow clone)" % BASE_REV)
    results = {}
    for name, mod in (("new", cp), ("base", base)):
        calls = []
        _prep(mod, monkeypatch, calls, PAST, gate)
        caplog.clear()
        with caplog.at_level(logging.INFO):
            last = mod.fetch_last_earnings_sync(EIGHT + ["HHH.US"])
        _prep(mod, monkeypatch, calls, FWD, gate)
        with caplog.at_level(logging.INFO):
            fwd = mod._run_sync(mod.fetch_earnings_map(EIGHT + ["HHH.US"]))
        logs = [r.getMessage().replace(mod.__version__, "V") for r in caplog.records
                if "calendar_provider" in r.getMessage()]
        results[name] = (last, fwd, calls, logs)
    assert results["new"] == results["base"]
    last, fwd, calls, logs = results["new"]
    assert last == {} and fwd == {}, "the whole 404 batch is dropped (v1.3.0)"
    assert len(calls) == 2 and all(len(c[3]) == 9 for c in calls)
    assert any("last-earnings batch failed (9 syms)" in m for m in logs)
    assert any("earnings batch failed (9 syms)" in m for m in logs)


# --------------------------------------------------------------------- S2
def test_s2_last_report_split_keeps_known_codes(cp, monkeypatch, caplog):
    calls = []
    _prep(cp, monkeypatch, calls, PAST, "on")
    with caplog.at_level(logging.WARNING):
        out = cp.fetch_last_earnings_sync(EIGHT)
    assert set(out) == {"AAA.US", "BBB.US", "CCC.US", "DDD.US", "EEE.US", "FFF.US", "GGG.US"}
    assert out["AAA.US"] == {"last_earnings_date": "2026-10-22", "eps_surprise_pct": -22.2,
                             "eps_actual": 0.35, "eps_estimate": 0.45, "report_timing": None}
    assert out["BBB.US"]["eps_surprise_pct"] == 10.0
    # sorted codes: AAA BAD.L BBB CCC DDD EEE FFF GGG -> halves until BAD.L stands alone
    assert calls[0][3] == tuple(sorted(EIGHT)) and calls[0][1:3] == ("2026-10-06", "2026-10-26")
    assert all(c[1:3] == ("2026-10-06", "2026-10-26") for c in calls)
    assert ("BAD.L",) in [c[3] for c in calls]
    assert len(calls) == 7, [c[3] for c in calls]      # 8 -> 4+4 -> 2+2 -> 1+1
    msgs = [r.getMessage() for r in caplog.records if r.levelno == logging.WARNING]
    # 2026-10-27: the version in the line moved with v1.3.2 (pin moved; the
    # rest of the line is v1.3.1's - no translation when the suffix map is off)
    # 2026-11-01: v1.4.0 stacked as well; the line carries the module's own version
    assert msgs == ["[calendar_provider v%s] last-earnings: 404 batches split 3 time(s), "
                    "7 request(s); EODHD rejected 1 code(s): BAD.L" % cp.__version__], msgs


# --------------------------------------------------------------------- S3
def test_s3_forward_split_keeps_earliest_upcoming(cp, monkeypatch, caplog):
    calls = []
    _prep(cp, monkeypatch, calls, FWD, "ON")
    with caplog.at_level(logging.WARNING):
        out = cp._run_sync(cp.fetch_earnings_map(EIGHT + ["2222.SR"]))
    assert out == {s: "2026-11-05" for s in EIGHT if s != "BAD.L"}
    assert all("2222" not in ",".join(c[3]) for c in calls), "KSA never sent"
    assert calls[0][1:3] == ("2026-10-27", "2026-12-11")
    assert any("earnings: 404 batches split" in r.getMessage() and "BAD.L" in r.getMessage()
               for r in caplog.records)


# --------------------------------------------------------------------- S4
def test_s4_many_unknown_cap_and_non_404(cp, monkeypatch, caplog):
    many = ["AAA.US", "X1.T", "BBB.US", "X2.KL", "CCC.US", "X3.L", "DDD.US", "AEFES.IS.US",
            "EEE.US", "FFF.US", "PHP.LSE", "WDP.BR", "1113.HK"]
    calls = []
    _prep(cp, monkeypatch, calls, PAST, "1")
    with caplog.at_level(logging.WARNING):
        out = cp.fetch_last_earnings_sync(many)
    assert set(out) == {"AAA.US", "BBB.US", "CCC.US", "DDD.US", "EEE.US", "FFF.US", "PHP.LSE",
                        "WDP.BR", "1113.HK"}
    msg = [r.getMessage() for r in caplog.records if "rejected" in r.getMessage()][0]
    assert "EODHD rejected 4 code(s): AEFES.IS.US, X1.T, X2.KL, X3.L" in msg, msg
    # the cap is checked BEFORE every request (review fix, Codex P2 on #853):
    # two requests, then nothing more - pending halves included
    calls = []
    _prep(cp, monkeypatch, calls, PAST, "on")
    monkeypatch.setenv("TFB_CALENDAR_SPLIT_MAX_CALLS", "2")
    caplog.clear()
    with caplog.at_level(logging.WARNING):
        out = cp.fetch_last_earnings_sync(many)
    assert len(calls) == 2 and out == {}, [c[3] for c in calls]
    assert any("2 request(s)" in r.getMessage() and "13 code(s) dropped at the request cap 2" in r.getMessage()
               for r in caplog.records), [r.getMessage() for r in caplog.records]
    # Codex's case: cap 2, two all-404 batches of four and two codes -> exactly
    # two requests; the later batch is never requested
    calls = []
    _prep(cp, monkeypatch, calls, PAST, "on")
    monkeypatch.setenv("TFB_CALENDAR_SPLIT_MAX_CALLS", "2")
    monkeypatch.setenv("TFB_CALENDAR_EARNINGS_CHUNK", "4")
    caplog.clear()
    with caplog.at_level(logging.WARNING):
        out = cp.fetch_last_earnings_sync(["U1.L", "U2.L", "U3.L", "U4.L", "U5.L", "U6.L"])
    assert len(calls) == 2 and out == {} and calls[0][3] == ("U1.L", "U2.L", "U3.L", "U4.L")
    assert any("6 code(s) dropped at the request cap 2" in r.getMessage() for r in caplog.records)
    monkeypatch.delenv("TFB_CALENDAR_EARNINGS_CHUNK", raising=False)
    # a non-404 error inside a split: that top-level batch is skipped (v1.3.0 handling)
    calls = []
    _prep(cp, monkeypatch, calls, PAST, "on", extra_fail=lambda syms: syms == ["BBB.US", "CCC.US"])
    monkeypatch.delenv("TFB_CALENDAR_SPLIT_MAX_CALLS", raising=False)
    monkeypatch.setenv("TFB_CALENDAR_EARNINGS_CHUNK", "4")
    caplog.clear()
    with caplog.at_level(logging.INFO):
        out = cp.fetch_last_earnings_sync(["AAA.US", "BAD.L", "BBB.US", "CCC.US", "DDD.US", "EEE.US"])
    # batch 1 = AAA BAD.L BBB CCC -> 404 -> [AAA BAD.L] [BBB CCC]: 429 -> batch 1 skipped whole
    assert set(out) == {"DDD.US", "EEE.US"}, out
    assert any("last-earnings batch failed (4 syms)" in r.getMessage() and "429" in r.getMessage()
               for r in caplog.records)


# --------------------------------------------------------------------- S5
@pytest.mark.parametrize("raw,on", [("on", True), ("ON", True), (" on ", True), ("1", True), ("true", True),
                                    ("yes", True), ("off", False), ("0", False), ("", False), ("onn", False)])
def test_s5_gate_spellings(cp, monkeypatch, raw, on):
    monkeypatch.setenv("TFB_CALENDAR_SPLIT_404", raw)
    assert cp._split_404_enabled() is on


def test_s5_http_404_detection_and_workflow(cp):
    assert cp._is_http_404(_http_error(404)) is True
    assert cp._is_http_404(_http_error(500)) is False
    assert cp._is_http_404(RuntimeError("404 Not Found")) is False
    wf = (ROOT / ".github" / "workflows" / "calendar_sync.yml").read_text(encoding="utf-8")
    step = wf[wf.index("- name: 📅 Run calendar sync"):]
    assert 'TFB_CALENDAR_SPLIT_404: "on"' in step.split("run: |")[0], "the workflow arms the split"
