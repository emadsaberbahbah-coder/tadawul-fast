#!/usr/bin/env python3
"""tests/test_sync_fw2_scope.py
run_dashboard_sync v6.71.0 FW-2 SCOPE (gate TFB_SYNC_FW2_SCOPE = off | observe |
enforce). Dual-tree, REAL-module harness: drives the REAL
_row_identity_firewall, _fw2_* helpers, _idfw_selftest and
_append_runlog_idfirewall. The only non-real object is a Sheets append
RECORDER. Fixture rows are real Global_Markets rows copied verbatim from the
2026-10-14 export (sync run 38486122061); each case perturbs copies of them
in a stated, deterministic way (EPS vintage drift, the HKD price / CNY EPS
currency gap, an enrichment block transposed from another issuer).

Base parity: TFB_TEST_SYNC_BASE = main's file before this build (v6.70.2 since
2026-10-21: git show 55b47f24:scripts/run_dashboard_sync.py; authored against
the v6.69.0 file 15812b6, then v6.70.0 f24e9ff3 after main's #799 HK-FX)
-> off output == base output, value
for value, and observe output == base output. Without the variable the base
cases skip. Offline: no network, no credentials, no live Sheet.

Merge note (2026-10-19): main's v6.70.0 HK-FX band (gate
TFB_SYNC_COH_HK_REPORTING_FX, default on) exempts a .HK row whose Price/EPS
sits at the stated P/E times the HKD-per-CNY rate BEFORE FW-2 SCOPE tiers it.
The H-share perturbation (case 3) therefore passes with the default; t11
pins that ordering and, with the band off, the 2026-10-14 drift behaviour.

Run: pytest -q tests/test_sync_fw2_scope.py
  or python3 tests/test_sync_fw2_scope.py   (x3, digest)"""
from __future__ import annotations

import copy
import hashlib
import importlib.util
import json
import os
import re
import sys

import pytest

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)
SYNC = os.environ.get("TFB_TEST_SYNC_DELIVERED") or os.path.join(ROOT, "scripts", "run_dashboard_sync.py")
BASE = os.environ.get("TFB_TEST_SYNC_BASE", "")
FIXTURE = os.path.join(HERE, "fixtures", "fw2_scope_gm_rows_2026-10-14.json")
ENV = ("TFB_SYNC_FW2_SCOPE", "TFB_SYNC_COH_HK_REPORTING_FX")
# Substrings the engine investability gate tests in a lower-cased Warnings
# string (data_engine_v2 _apply_investability_gate, pinned 2026-09-20).
ENGINE_GATE_SUBSTRINGS = ("cap", "forecast", "target", "roi", "drop", "reject",
                          "provider_target", "price_bar_stale", "xprovider_price_conflict")


def _load(path, name):
    d = os.path.dirname(os.path.abspath(path))
    if d not in sys.path:
        sys.path.insert(0, d)
    spec = importlib.util.spec_from_file_location(name, path)
    mod = importlib.util.module_from_spec(spec)
    sys.modules[name] = mod
    spec.loader.exec_module(mod)
    return mod


_M = {}


def m():
    if "d" not in _M:
        _M["d"] = _load(SYNC, "rds_fw2_delivered")
    return _M["d"]


def b():
    if "b" not in _M:
        _M["b"] = _load(BASE, "rds_fw2_base") if BASE and os.path.exists(BASE) else None
    return _M["b"]


class _Env(object):
    def __init__(self, **kw):
        self.kw, self.prev = kw, {}

    def __enter__(self):
        for k in ENV:
            self.prev[k] = os.environ.get(k)
            os.environ.pop(k, None)
        for k, v in self.kw.items():
            self.prev.setdefault(k, os.environ.get(k))
            if v is None:
                os.environ.pop(k, None)
            else:
                os.environ[k] = v
        return self

    def __exit__(self, *_a):
        for k, v in self.prev.items():
            if v is None:
                os.environ.pop(k, None)
            else:
                os.environ[k] = v
        return False


def _fixture():
    with open(FIXTURE, encoding="utf-8") as fh:
        data = json.load(fh)
    return list(data["headers"]), [list(r) for r in data["rows"]]


def _col(h, name):
    return h.index(name)


def _case_matrix(hk_fx=True):
    """Real rows + deterministic perturbations. Returns (headers, rows, expect)
    where expect maps symbol -> 'pass' | 'drift' | 'gross'. hk_fx: the v6.70.0
    HK-FX band is on (default), so the case-3 H-share rows pass."""
    h, rows = _fixture()
    sym, px, eps, pe = (_col(h, c) for c in ("Symbol", "Current Price", "EPS (TTM)", "P/E (TTM)"))
    by = {r[sym]: r for r in rows}
    out, expect = [], {}
    for r in rows:                        # untouched real rows (all pass at HEAD)
        out.append(list(r))
        expect[r[sym]] = "pass"
    # 1. EPS vintage drift: a fresh earnings print on the EPS side only (+12%).
    for s in ("NVDA.US", "AAPL.US", "JPM.US", "C.US"):
        r = list(by[s]); r[sym] = s.replace(".US", "") + "-V.US"; r[eps] = round(float(r[eps]) * 1.12, 4)
        out.append(r); expect[r[sym]] = "drift"
    # 2. stale P/E side (-15%).
    for s in ("KO.US", "PG.US"):
        r = list(by[s]); r[sym] = s.replace(".US", "") + "-P.US"; r[pe] = round(float(r[pe]) * 0.85, 4)
        out.append(r); expect[r[sym]] = "drift"
    # 3. H-share currency gap: HKD price over CNY EPS (EPS / 1.09).
    for s in ("0939.HK", "1398.HK", "3988.HK"):
        r = list(by[s]); r[sym] = s.replace(".HK", "-C.HK"); r[eps] = round(float(r[eps]) / 1.09, 6)
        out.append(r); expect[r[sym]] = "pass" if hk_fx else "drift"
    # 4. transposed enrichment block (EPS + P/E + Name of another issuer):
    #    ratio = price_target / price_donor, far from 1 here -> gross.
    nm = _col(h, "Name")
    for target, donor in (("MSFT.US", "KO.US"), ("WMT.US", "MSFT.US"), ("XOM.US", "DUOL.US")):
        r = list(by[target]); d = by[donor]
        r[sym] = target.replace(".US", "-X.US"); r[eps], r[pe], r[nm] = d[eps], d[pe], d[nm]
        out.append(r)
        ratio = (float(r[px]) / float(r[eps])) / float(r[pe])
        expect[r[sym]] = "gross" if not (0.5 <= ratio <= 2.0) else "drift"
    return h, out, expect


def _run(mod, mode, h, rows, hk=None):
    with _Env(TFB_SYNC_FW2_SCOPE=mode, TFB_SYNC_COH_HK_REPORTING_FX=hk):
        mm, stripped = mod._row_identity_firewall(list(h), copy.deepcopy(rows))
        census = dict(getattr(mod, "_LAST_FW2_CENSUS", {}) or {})
    return mm, stripped, census


# ---------------------------------------------------------------------------
def test_t01_fixture_is_real_and_coherent_at_head():
    h, rows = _fixture()
    assert len(rows) == 50 and "Current Price" in h and "Warnings" in h
    mm, stripped, _c = _run(m(), "off", h, rows)
    assert stripped == [] and mm == rows                 # every real row passes FW-2


def test_t02_off_is_byte_identical_to_v6_69_0():
    if b() is None:
        pytest.skip("TFB_TEST_SYNC_BASE unset")
    h, rows, _e = _case_matrix()
    d_mm, d_s, d_c = _run(m(), "off", h, rows)
    b_mm, b_s, _bc = _run(b(), "off", h, rows)
    assert (d_mm, d_s) == (b_mm, b_s)
    assert json.dumps(d_mm, sort_keys=True, default=str) == json.dumps(b_mm, sort_keys=True, default=str)


def test_t03_observe_strips_like_base_and_counts_tiers():
    h, rows, expect = _case_matrix()
    o_mm, o_s, census = _run(m(), "observe", h, rows)
    f_mm, f_s, _c = _run(m(), "off", h, rows)
    assert (o_mm, o_s) == (f_mm, f_s)
    if b() is not None:
        b_mm, b_s, _bc = _run(b(), "off", h, rows)
        assert (o_mm, o_s) == (b_mm, b_s)
    tripped = {s for s, t in expect.items() if t != "pass"}
    assert set(o_s) == tripped
    assert census["mode"] == "observe" and census["fielded"] == 0
    assert census["drift"] == sum(1 for t in expect.values() if t == "drift")
    assert census["gross"] == sum(1 for t in expect.values() if t == "gross")
    by = {row["symbol"]: row for row in census["rows"]}
    assert set(by) == tripped
    for s, row in by.items():
        assert row["tier"] == expect[s] and row["action"] == "stripped"
        assert set(row) == {"symbol", "currency", "price", "eps", "pe", "ratio", "tier", "action"}


def test_t04_enforce_keeps_drift_rows_and_fences_them():
    h, rows, expect = _case_matrix()
    e_mm, e_s, census = _run(m(), "enforce", h, rows)
    idx = {c: _col(h, c) for c in ("Symbol", "Name", "Current Price", "EPS (TTM)", "P/E (TTM)", "PEG",
                                   "Investability Status", "Final Action", "Block Reason", "Warnings",
                                   "Last Updated (UTC)", "Last Updated (Riyadh)")}
    src = {r[idx["Symbol"]]: r for r in rows}
    drift = {s for s, t in expect.items() if t == "drift"}
    gross = {s for s, t in expect.items() if t == "gross"}
    assert set(e_s) == gross and census["fielded"] == len(drift)
    for r in e_mm:
        s = r[idx["Symbol"]]
        if s in drift:
            o = src[s]
            for c in ("EPS (TTM)", "P/E (TTM)", "PEG"):
                assert r[idx[c]] == ""
            assert r[idx["Investability Status"]] == "BLOCKED"
            assert r[idx["Final Action"]] == "DO_NOT_INVEST"
            assert str(r[idx["Block Reason"]]).startswith(m()._FW2_BLOCK_NOTE)
            assert str(r[idx["Warnings"]]).endswith(m()._FW2_FIELD_TAG)
            changed = {h[i] for i in range(len(h)) if r[i] != o[i]}
            assert changed <= {"EPS (TTM)", "P/E (TTM)", "PEG", "Investability Status", "Final Action",
                               "Block Reason", "Warnings"}
            for c in ("Name", "Current Price", "Last Updated (UTC)", "Last Updated (Riyadh)"):
                assert r[idx[c]] == o[idx[c]]
        elif s in gross:
            assert r[idx["Name"]] == "" and r[idx["Warnings"]] == "identity_quarantined:v6.24.0"
            assert all(v == "" for i, v in enumerate(r) if i not in (idx["Symbol"], idx["Warnings"]))
        else:
            assert r == src[s]


def test_t05_enforce_gross_rows_equal_base_strip():
    if b() is None:
        pytest.skip("TFB_TEST_SYNC_BASE unset")
    h, rows, expect = _case_matrix()
    e_mm, e_s, _c = _run(m(), "enforce", h, rows)
    b_mm, b_s, _bc = _run(b(), "off", h, rows)
    sym = _col(h, "Symbol")
    gross = {s for s, t in expect.items() if t == "gross"}
    assert e_s == [s for s in b_s if s in gross]
    for er, br in zip(e_mm, b_mm):
        if er[sym] in gross or expect.get(er[sym]) == "pass":
            assert er == br


def test_t06_tag_is_substring_safe_and_not_an_invalid_acquisition():
    tag = m()._FW2_FIELD_TAG.lower()
    assert not any(t in tag for t in ENGINE_GATE_SUBSTRINGS)
    from core import data_validity as dv
    assert dv._INVALID_WARNINGS.search("quote_ok; " + m()._FW2_FIELD_TAG) is None
    assert dv._INVALID_WARNINGS.search("identity_quarantined:v6.24.0") is not None


def test_t07_mode_parse_and_selftest_degrade():
    mod = m()
    for raw, want in (("", "off"), ("off", "off"), ("observe", "observe"), (" Enforce ", "enforce"),
                      ("1", "off"), ("yes", "off"), ("on", "off"), ("quarantine", "off")):
        with _Env(TFB_SYNC_FW2_SCOPE=raw):
            assert mod._fw2_scope_mode_raw() == want
    assert mod._fw2_selftest() is True
    saved = mod._FW2_SELFTEST_OK
    try:
        mod._FW2_SELFTEST_OK = False
        with _Env(TFB_SYNC_FW2_SCOPE="enforce"):
            assert mod._fw2_scope_mode() == "observe"
            h, rows, expect = _case_matrix()
            _mm, stripped = mod._row_identity_firewall(list(h), copy.deepcopy(rows))
            assert set(stripped) == {s for s, t in expect.items() if t != "pass"}
            assert mod._LAST_FW2_CENSUS["mode"] == "observe"
    finally:
        mod._FW2_SELFTEST_OK = saved


def test_t08_idfw_selftest_still_passes_in_every_mode():
    mod = m()
    for mode in ("off", "observe", "enforce"):
        with _Env(TFB_SYNC_FW2_SCOPE=mode):
            mod._idfw_selftest_()
            assert mod._IDFW_SELFTEST_OK is True, (mode, mod._IDFW_SELFTEST_MSG)


def test_t09_tiers_boundaries():
    t = m()._fw2_tier
    assert [t(x) for x in (0.4999, 0.5, 1.0, 2.0, 2.0001, 49.9)] == ["gross", "drift", "drift", "drift", "gross", "gross"]


class _Recorder(object):
    def __init__(self):
        self.bodies = []

    def _get_service(self):
        rec = self

        class _Exec(object):
            def __init__(self, body):
                self.body = body

            def execute(self):
                rec.bodies.append(self.body)
                return {}

        class _Values(object):
            def append(self, **kw):
                return _Exec(kw.get("body"))

        class _Sheets(object):
            def values(self):
                return _Values()

        class _Svc(object):
            def spreadsheets(self):
                return _Sheets()

        return _Svc()


def _runlog(mod, **kw):
    rec = _Recorder()
    saved = mod._IDFW_SELFTEST_MSG          # module state other cases may have set
    mod._IDFW_SELFTEST_MSG = "PASS 9/9"
    try:
        with _Env(TFB_SYNC_IDFW_RUNLOG="1"):
            mod._append_runlog_idfirewall(rec, "sid", "Global_Markets", 3, [], ["AAA.US", "BBB.US"],
                                          {}, [], **kw)
    finally:
        mod._IDFW_SELFTEST_MSG = saved
    assert len(rec.bodies) == 1
    return rec.bodies[0]["values"][0]


def test_t10_runlog_default_equals_base_and_census_suffix():
    mod = m()
    row = _runlog(mod)
    assert " | fw2=" not in row[5] and "fw2" not in json.loads(row[9])
    if b() is not None:
        brow = _runlog(b())
        norm = lambda r: [re.sub(r"6\.(69|70|71)\.[0-9]+", "X", str(c)) for c in r[1:]]
        assert norm(row) == norm(brow)
    census = {"mode": "enforce", "drift": 4, "gross": 1, "fielded": 4,
              "rows": [{"symbol": "JNJ", "tier": "drift", "action": "fielded"}]}
    row2 = _runlog(mod, fw2_census=census)
    assert row2[5].endswith(" | fw2=enforce drift=4 gross=1 fielded=4")
    det = json.loads(row2[9])
    assert det["fw2"]["fielded"] == 4 and det["fw2"]["rows"][0]["symbol"] == "JNJ"
    row3 = _runlog(mod, fw2_census={"mode": "observe", "drift": 0, "gross": 0, "fielded": 0, "rows": []})
    assert row3[5].endswith(" | fw2=observe drift=0 gross=0 fielded=0")


def test_t11_hk_fx_exemption_runs_before_the_tiers():
    mod = m()
    h, rows, on = _case_matrix(hk_fx=True)
    _h, _r, off = _case_matrix(hk_fx=False)
    hk = {s for s in on if s.endswith("-C.HK")}
    assert len(hk) == 3 and all(on[s] == "pass" and off[s] == "drift" for s in hk)
    sym = _col(h, "Symbol")
    src = {r[sym]: r for r in rows}
    for mode in ("off", "observe", "enforce"):
        mm, stripped, census = _run(mod, mode, h, rows)
        assert not hk & set(stripped)
        assert sorted(mod._LAST_FW2_DIAG["hk_fx_exempt"]) == sorted(hk)
        assert not hk & {s for s, _r2, _c2 in mod._LAST_FW2_DIAG["stripped"]}
        assert all(r == src[r[sym]] for r in mm if r[sym] in hk)
        if mode != "off":
            assert not hk & {row["symbol"] for row in census["rows"]}
    # band off: the 2026-10-14 behaviour - tiered drift, enforce fields them
    _mm, o_s, o_c = _run(mod, "observe", h, rows, hk="0")
    assert hk <= set(o_s) and o_c["drift"] == sum(1 for t in off.values() if t == "drift")
    e_mm, e_s, e_c = _run(mod, "enforce", h, rows, hk="0")
    assert not hk & set(e_s) and e_c["fielded"] == sum(1 for t in off.values() if t == "drift")
    assert {row["symbol"] for row in e_c["rows"] if row["action"] == "fielded"} >= hk
    # a fielded row is not a strip: it never reaches the HK-FX strip detail
    assert not hk & {s for s, _r2, _c2 in mod._LAST_FW2_DIAG["stripped"]}
    if b() is not None:
        b_mm, b_s, _bc = _run(b(), "off", h, rows, hk="0")
        f_mm, f_s, _fc = _run(mod, "off", h, rows, hk="0")
        assert (f_mm, f_s) == (b_mm, b_s) and hk <= set(b_s)


def _digest():
    h, rows, expect = _case_matrix()
    parts = []
    for mode in ("off", "observe", "enforce"):
        mm, s, c = _run(m(), mode, h, rows)
        parts.append(json.dumps([mm, s, c.get("drift"), c.get("gross"), c.get("fielded")],
                                sort_keys=True, default=str))
    return hashlib.sha256("\n".join(parts).encode("utf-8")).hexdigest()[:16]


if __name__ == "__main__":
    tests = [(k, v) for k, v in sorted(globals().items()) if k.startswith("test_t")]
    passed = skipped = 0
    for name, fn in tests:
        try:
            fn()
            passed += 1
            print("ok  ", name)
        except pytest.skip.Exception as exc:
            skipped += 1
            print("SKIP", name, "::", exc)
    print(f"PASS {passed}/{len(tests)}" + (f" (skipped {skipped})" if skipped else "") + f" digest {_digest()}")
