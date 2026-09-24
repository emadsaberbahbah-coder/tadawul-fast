#!/usr/bin/env python3
"""tests/test_sync_eodhd_quota_guard_p154d.py
P-154d - run_dashboard_sync v6.61.0 EODHD QUOTA GUARD (pre-fetch skip +
post-fetch poison refusal). Loads the REAL script (the K-battery loader),
drives the REAL async _run_one_task up to the guard seam with a local
usage-endpoint emulator and a Sheets boundary recorder; a control-flow probe
replaces _read_symbols only to prove where the function went next.
"""
from __future__ import annotations

import asyncio
import importlib.util
import json
import os
import sys
import threading
import time
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, HTTPServer

import pytest

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)
SYNC = os.path.join(ROOT, "scripts", "run_dashboard_sync.py")


def _load(path, name):
    d = os.path.dirname(os.path.abspath(path))
    if d not in sys.path:
        sys.path.insert(0, d)
    spec = importlib.util.spec_from_file_location(name, path)
    mod = importlib.util.module_from_spec(spec)
    sys.modules[name] = mod
    spec.loader.exec_module(mod)
    return mod


# ------------------------------------------------ usage-endpoint emulator
class _Usage(BaseHTTPRequestHandler):
    state = {"used": 200000, "limit": 400000, "extra": 0, "mode": "ok", "hits": 0}

    def log_message(self, *a):
        pass

    def do_GET(self):
        st = _Usage.state
        st["hits"] += 1
        if st["mode"] == "401":
            self.send_response(401)
            self.end_headers()
            self.wfile.write(b"Unauthenticated")
            return
        body = json.dumps({
            "name": "x", "apiRequests": str(st["used"]),
            "apiRequestsDate": datetime.now(timezone.utc).strftime("%Y-%m-%d"),
            "dailyRateLimit": st["limit"], "extraLimit": st["extra"],
        }).encode("utf-8")
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.end_headers()
        self.wfile.write(body)


# ------------------------------------------------ Sheets boundary recorder
class _Exec(object):
    def __init__(self, rec, kw):
        self.rec, self.kw = rec, kw

    def execute(self):
        if self.rec.fail:
            raise RuntimeError("sheets down")
        self.rec.appends.append(self.kw)
        return {}


class _Values(object):
    def __init__(self, rec):
        self.rec = rec

    def append(self, **kw):
        return _Exec(self.rec, kw)


class _Svc(object):
    def __init__(self, rec):
        self.rec = rec

    def spreadsheets(self):
        return self

    def values(self):
        return _Values(self.rec)


class _Recorder(object):
    def __init__(self):
        self.appends = []
        self.fail = False

    def _get_service(self):
        return _Svc(self)


class _Probe(Exception):
    pass


@pytest.fixture(scope="module")
def srv():
    httpd = HTTPServer(("127.0.0.1", 0), _Usage)
    t = threading.Thread(target=httpd.serve_forever, daemon=True)
    t.start()
    yield "http://127.0.0.1:%d/api/user" % httpd.server_port
    httpd.shutdown()


@pytest.fixture()
def env(monkeypatch, srv):
    for k in ("TFB_SYNC_EODHD_QUOTA_GUARD", "TFB_SYNC_EODHD_QUOTA_GUARD_PCT",
              "TFB_SYNC_EODHD_QUOTA_GUARD_POISON_PCT", "TFB_SYNC_EODHD_QUOTA_GUARD_ALLOW_EXTRA",
              "TFB_SYNC_EODHD_QUOTA"):
        monkeypatch.delenv(k, raising=False)
    monkeypatch.setenv("EODHD_API_KEY", "test-key-never-logged")
    monkeypatch.setenv("TFB_SYNC_EODHD_QUOTA_URL", srv)
    monkeypatch.setenv("TFB_SYNC_EODHD_QUOTA_TIMEOUT_S", "3")
    monkeypatch.setenv("TFB_SYNC_DECISION_GUARD", "1")
    _Usage.state.update({"used": 200000, "limit": 400000, "extra": 0, "mode": "ok", "hits": 0})
    return monkeypatch


@pytest.fixture(scope="module")
def m():
    return _load(SYNC, "rds_p154d_new")


def _task(m):
    return m.TaskSpec(key="GLOBAL_MARKETS", sheet_name="Global_Markets", gateway="analysis")


def _run(m, rec, probe):
    """Drive the REAL _run_one_task; the probe marks whether the symbol read
    (the first step after the guard seam) was reached."""
    reached = {"n": 0}

    def _probe(*a, **k):
        reached["n"] += 1
        raise _Probe("reached _read_symbols")
    saved = m._read_symbols
    m._read_symbols = _probe
    try:
        # _run_one_task catches its own exceptions: a reached probe surfaces as
        # status="failed" with the probe's message in res.error, never as a raise.
        res = asyncio.run(m._run_one_task(_task(m), "sheet-id", "A1", -1, False, False, None, rec))
    finally:
        m._read_symbols = saved
    return res, reached["n"]


def _guard_lines(rec):
    return [kw["body"]["values"][0] for kw in rec.appends
            if "[EODHD-QUOTA-GUARD" in kw["body"]["values"][0][5]]


# T1 -- vocabulary and defaults
def test_t1_vocabulary(m, env):
    assert m.SCRIPT_VERSION >= "6.61.0"
    assert m._eodhd_quota_guard_mode() == "off"
    for bad in ("1", "true", "on", "yes", "enforced"):
        env.setenv("TFB_SYNC_EODHD_QUOTA_GUARD", bad)
        assert m._eodhd_quota_guard_mode() == "off", bad
    for good in ("observe", "ENFORCE"):
        env.setenv("TFB_SYNC_EODHD_QUOTA_GUARD", good)
        assert m._eodhd_quota_guard_mode() == good.lower()
    assert m._eodhd_quota_guard_pct() == 97.0 and m._eodhd_quota_guard_poison_pct() == 25.0
    env.setenv("TFB_SYNC_EODHD_QUOTA_GUARD_PCT", "10")
    assert m._eodhd_quota_guard_pct() == 50.0            # floor
    env.setenv("TFB_SYNC_EODHD_QUOTA_GUARD_POISON_PCT", "0")
    assert m._eodhd_quota_guard_poison_pct() == 1.0       # floor
    assert m._eodhd_quota_guard_allow_extra() is False
    assert m._EODHD_QUOTA_GUARD_TAG == "[EODHD-QUOTA-GUARD v%s]" % m.SCRIPT_VERSION
    assert m._eodhd_quota_guard_selftest() == "PASS 8/8"


# T2 -- the pure decision matrix
def test_t2_decide_matrix(m):
    d = m._eodhd_quota_guard_decide
    ok = lambda pct, extra=0: {"ok": True, "pct": pct, "used": int(4000 * pct), "limit": 400000, "extra": extra}
    assert d("GM", "pre", ok(30.2), None, "off", 97, 25, False)["verdict"] == "off"
    assert d("GM", "pre", ok(30.2), None, "enforce", 97, 25, False)["verdict"] == "allow"
    assert d("GM", "pre", ok(96.9), None, "enforce", 97, 25, False)["verdict"] == "allow"
    r = d("Global_Markets", "pre", ok(97.0), None, "enforce", 97, 25, False)
    assert r["verdict"] == "skip" and r["reason"] == "used>=97%" and "leg SKIPPED" in r["note"] and "Global_Markets" in r["note"]
    r = d("Global_Markets", "pre", ok(97.0), None, "observe", 97, 25, False)
    assert r["verdict"] == "would_skip" and "would leg SKIPPED" in r["note"] and "mode=observe" in r["note"]
    assert d("GM", "pre", ok(100.0), None, "enforce", 97, 25, False)["reason"] == "exhausted"
    assert d("GM", "pre", ok(100.0, 5000), None, "enforce", 97, 25, False)["reason"] == "on_extra"
    assert d("GM", "pre", ok(100.0, 5000), None, "enforce", 97, 25, True)["verdict"] == "allow"
    # UNKNOWN always allows (fail-open), with the reason carried
    for q in ({"ok": False, "why": "no_key"}, {"ok": False, "why": "poll_failed:URLError"}, None, {}):
        r = d("GM", "pre", q, None, "enforce", 97, 25, False)
        assert r["verdict"] == "allow" and r["reason"].startswith("unknown:"), r
    # post-fetch on the REAL 2026-09-24 counts: the 00:54 GM replay (6,071 fresh 402 of 6,609)
    r = d("Global_Markets", "post", None, {"q402_new": 6071, "n": 6609}, "enforce", 97, 25, False)
    assert r["verdict"] == "skip" and r["reason"].startswith("poison:6071/6609") and "write REFUSED" in r["note"]
    # the 08:17 GM leg (0 fresh 402, 43 x 404) allows; a trickle below the poison share allows
    assert d("GM", "post", None, {"q402_new": 0, "f404": 43, "n": 6609}, "enforce", 97, 25, False)["reason"] == "no_fresh_402"
    assert d("GM", "post", None, {"q402_new": 47, "n": 6609}, "enforce", 97, 25, False)["verdict"] == "allow"
    assert d("GM", "post", None, {"q402_new": 47, "n": 6609}, "observe", 97, 25, False)["verdict"] == "allow"
    assert d("GM", "post", None, {"q402_new": 2393, "n": 2474}, "observe", 97, 25, False)["verdict"] == "would_skip"
    assert d("GM", "nonsense", None, None, "enforce", 97, 25, False)["verdict"] == "allow"


# T3 -- the _Run_Log line: written only for skip / would_skip, FW-3 shape, never raises
def test_t3_runlog_line(m, env):
    rec = _Recorder()
    ok = {"ok": True, "pct": 100.0, "used": 400000, "limit": 400000, "extra": 0}
    v = m._eodhd_quota_guard_decide("Global_Markets", "pre", ok, None, "enforce", 97, 25, False)
    m._append_runlog_eodhd_quota_guard(rec, "sid", v)
    assert len(rec.appends) == 1
    kw = rec.appends[0]
    assert kw["range"] == "'_Run_Log'!A1" and kw["valueInputOption"] == "USER_ENTERED"
    row = kw["body"]["values"][0]
    assert row[1] == "WARNING" and row[2] == "run_dashboard_sync" and row[3] == "Global_Markets" and row[4] == "SKIPPED"
    assert row[5].startswith("[EODHD-QUOTA-GUARD v") and "verdict=skip" in row[5] and "reason=exhausted" in row[5]
    assert "selftest=PASS 8/8" in row[5] and "test-key-never-logged" not in json.dumps(kw)
    meta = json.loads(row[9]) if row[9].startswith("{") else json.loads(row[9][row[9].index("{"):])
    assert meta["verdict"] == "skip" and meta["version"] == m.SCRIPT_VERSION and meta["skip_pct"] == 97.0
    # allow -> no line; would_skip -> WOULD_SKIP status
    a = m._eodhd_quota_guard_decide("GM", "pre", {"ok": True, "pct": 10.0}, None, "enforce", 97, 25, False)
    m._append_runlog_eodhd_quota_guard(rec, "sid", a)
    assert len(rec.appends) == 1
    w = m._eodhd_quota_guard_decide("GM", "pre", ok, None, "observe", 97, 25, False)
    m._append_runlog_eodhd_quota_guard(rec, "sid", w)
    assert rec.appends[-1]["body"]["values"][0][4] == "WOULD_SKIP"
    # a dead Sheets service is annotated, never raised, never counted
    rec.fail = True
    before = list(m._RUNLOG_APPEND_FAILS)
    m._append_runlog_eodhd_quota_guard(rec, "sid", v)
    assert m._RUNLOG_APPEND_FAILS == before
    m._append_runlog_eodhd_quota_guard(None, "sid", v)


# T4 -- REAL _run_one_task, enforce, counter EXHAUSTED: skipped before any symbol read
def test_t4_real_task_enforce_skips(m, env):
    env.setenv("TFB_SYNC_EODHD_QUOTA_GUARD", "enforce")
    _Usage.state.update({"used": 400000, "extra": 0, "hits": 0})
    rec = _Recorder()
    res, reached = _run(m, rec, probe=True)
    assert res is not None and reached == 0                 # returned BEFORE _read_symbols
    assert res.status == "skipped" and res.rows_written == 0 and res.rows_failed == 0
    assert any("[EODHD-QUOTA-GUARD" in w and "verdict=skip" in w and "reason=exhausted" in w for w in res.warnings)
    lines = _guard_lines(rec)
    assert len(lines) == 1 and lines[0][4] == "SKIPPED" and lines[0][3] == "Global_Markets"
    assert _Usage.state["hits"] == 1                       # exactly one 0-cost poll
    # threshold path: 97.5% used -> skipped; 96.9% -> proceeds to the symbol read
    _Usage.state.update({"used": 390000, "hits": 0})
    res, reached = _run(m, _Recorder(), probe=True)
    assert res is not None and res.status == "skipped" and reached == 0
    _Usage.state.update({"used": 387600, "hits": 0})
    res, reached = _run(m, _Recorder(), probe=True)
    assert reached == 1 and res.status == "failed" and "reached _read_symbols" in str(res.error)   # allowed
    # UNKNOWN (401 from the usage endpoint) never skips
    _Usage.state.update({"mode": "401", "hits": 0})
    res, reached = _run(m, _Recorder(), probe=True)
    assert reached == 1 and res.status == "failed" and not any("[EODHD-QUOTA-GUARD" in w for w in res.warnings)
    _Usage.state.update({"mode": "ok"})


# T5 -- observe: the same exhaustion only DISCLOSES; the leg proceeds exactly as before
def test_t5_real_task_observe_discloses(m, env):
    env.setenv("TFB_SYNC_EODHD_QUOTA_GUARD", "observe")
    _Usage.state.update({"used": 400000, "extra": 0, "hits": 0})
    rec = _Recorder()
    res, reached = _run(m, rec, probe=True)
    assert reached == 1 and res.status == "failed"          # proceeded to the symbol read
    assert any("verdict=would_skip" in w for w in res.warnings)
    lines = _guard_lines(rec)
    assert len(lines) == 1 and lines[0][4] == "WOULD_SKIP" and "verdict=would_skip" in lines[0][5]


# T6 -- off: no poll, no line, the leg proceeds (byte-identical control flow)
def test_t6_real_task_off(m, env):
    _Usage.state.update({"used": 400000, "extra": 0, "hits": 0})
    rec = _Recorder()
    res, reached = _run(m, rec, probe=True)
    assert reached == 1 and res.status == "failed"
    assert _Usage.state["hits"] == 0 and rec.appends == [] and not any("QUOTA-GUARD" in w for w in res.warnings)


# T7 -- decision-owned pages and non-market pages are untouched by the guard
def test_t7_scope(m, env):
    env.setenv("TFB_SYNC_EODHD_QUOTA_GUARD", "enforce")
    _Usage.state.update({"used": 400000, "extra": 0, "hits": 0})
    rec = _Recorder()
    task = m.TaskSpec(key="TOP_10_INVESTMENTS", sheet_name="Top_10_Investments", gateway="analysis")
    res = asyncio.run(m._run_one_task(task, "sheet-id", "A1", -1, False, False, None, rec))
    assert res.status == "skipped" and any("DECISION-GUARD" in w for w in res.warnings)
    assert _Usage.state["hits"] == 0 and _guard_lines(rec) == []   # decision guard first, no poll


# T8 -- dual version: the v6.60.0 base has no guard and, at 100% used, walks into the symbol read
#        exactly like v6.61.0 with the gate off (control-flow parity at the seam)
def test_t8_base_parity(env):
    base_path = os.environ.get("TFB_TEST_SYNC_BASE", "")
    if not base_path or not os.path.exists(base_path):
        pytest.skip("set TFB_TEST_SYNC_BASE=/path/to/v6.60.0/run_dashboard_sync.py to run")
    b = _load(base_path, "rds_p154d_base")
    assert b.SCRIPT_VERSION == "6.60.0" and not hasattr(b, "_eodhd_quota_guard_decide")
    _Usage.state.update({"used": 400000, "extra": 0, "hits": 0})
    env.setenv("TFB_SYNC_EODHD_QUOTA_GUARD", "enforce")   # base ignores it
    res, reached = _run(b, _Recorder(), probe=True)
    assert reached == 1 and res.status == "failed" and _Usage.state["hits"] == 0
