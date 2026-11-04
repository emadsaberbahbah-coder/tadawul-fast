#!/usr/bin/env python3
"""tests/test_track_outcome_cal_ext.py - track_performance v6.56.0
[OUTCOME-CAL-EXT] (TRACK_OUTCOME_CAL_EXT, default off = v6.53.0 byte for byte
apart from the version string).

WHY (2026-11-04): _outcome_sessions is the tracker's only exchange-calendar
lookup (live-quote session check, target session, v6.47.0 historical close).
It knows 32 suffixes; any other one raises outcome_calendar_unknown and the
record stays ACTIVE / PENDING for ever. MEGACPO.MX is on the 4 Nov board
(Performance Clock run 41795448626); .MX .KL .JO .MC .SA .TW .IR .AT .F .BO
.SS and others have no calendar, although core/analysis/opportunity_builder
already ships reviewed calendars for them (_VENUE_CAL_MAP /
_VENUE_CAL_EXT_MAP). With the gate on the tracker consults those 30 extra
entries when its own map misses.

REAL modules (scripts/track_performance.py, core/analysis/opportunity_builder,
core.symbols.normalize) and the REAL pinned exchange_calendars. The audit
cases run the real PerformanceTrackerApp.audit_active_records loop; the only
stubs are the backend quote call (an AsyncMock returning one witnessed row),
the clock, and the price fallback / CA guard / feed switches. No network.

C1  parity: _OUTCOME_CALENDARS_EXT == (builder _VENUE_CAL_MAP | _VENUE_CAL_EXT_MAP)
    minus the tracker's base keys; no key overlaps the base map; the excluded
    venues are in neither map; every MIC instantiates
C2  off (unset / blank / off / 0 / false / no / enforce / observe / onn / y):
    _outcome_sessions returns or raises exactly as the untouched v6.53.0 over
    a symbol matrix (base suffixes, every extension suffix, excluded venues,
    indices / futures / crypto / bare digits, US share classes, suffix-less)
C2b the on-spellings; junk stays off
C3  on: every extension suffix maps to its MIC; every base-map symbol keeps
    the base calendar (base wins); every refusal still raises; .NS .SZ .CA
    .QA .AE .AD .KW .BH stay unknown; BRK.B stays US; SAP.F is XFRA
C4  on, the REAL audit loop: a due record on MX KL JO MC SA TW IR AT F BO SS
    BK JK with a quote at its target-session close MATURES (WIN), its receipt
    naming the MIC; off, the same record stays ACTIVE with
    outcome_calendar_unknown, byte-identical to v6.53.0 (receipt version aside)
C4b on and off, AAPL.US / 2222.SR / VOD.L / 7203.T records give identical
    records and receipts (the gate never touches a base venue)
C5  on, a quote one hour before the calendar close is refused as
    quote_before_target_close (the record stays ACTIVE): an earlier real close
    than the calendar's loses maturations, it never makes a wrong one
C6  on, .BO / .SS targets whose session window passes exchange_calendars'
    2026-12-31 bound stay outcome_calendar_unknown (fail closed); a November
    target works
C7  ENV lane: both tracker steps map TRACK_OUTCOME_CAL_EXT with || 'off'
    exactly once and identically; the reader's fallback literal is "off";
    ci.yml's heavy job runs this file
G1  golden negative: the untouched v6.53.0 with the gate set leaves the .MX
    record ACTIVE with outcome_calendar_unknown (C4's on-check fails on it)
V1  version floor, verify_deployment pin, title line, WHY order, ASCII file
V1b against the base: ASCII added lines (the title line aside), exactly the
    title and SCRIPT_VERSION lines removed, one function added, none removed

Base copy: TFB_TEST_TRACK_BASE=<path>, else `git show c54eba40:scripts/
track_performance.py`; without either C2, C4's base leg, G1 and V1b SKIP.
Golden negative of the whole file: TFB_TEST_TRACK_DELIVERED=<base copy>.

Run: python3 tests/test_track_outcome_cal_ext.py   (prints PASS k/k)
  or python3 -m pytest -q tests/test_track_outcome_cal_ext.py
"""
from __future__ import annotations

import asyncio
import importlib.util
import json
import os
import re
import subprocess
import sys
import tempfile
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import AsyncMock

import pytest

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

xc = pytest.importorskip("exchange_calendars")

BASE_SHA = "c54eba40"   # main 2026-11-04; track_performance v6.53.0 there
BASE_FULL = "c54eba40fb82e0a8a0cf8c69b69f7f59ffc3f272"
GATE = "TRACK_OUTCOME_CAL_EXT"
UTC = timezone.utc
NOW = datetime(2026, 10, 9, 23, 30, tzinfo=UTC)   # after every venue's 9 Oct close
_MODS = {}


def _load(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    mod = importlib.util.module_from_spec(spec)
    sys.modules[name] = mod
    spec.loader.exec_module(mod)
    return mod


def delivered():
    if "delivered" not in _MODS:
        path = os.getenv("TFB_TEST_TRACK_DELIVERED", "").strip() or str(ROOT / "scripts" / "track_performance.py")
        _MODS["delivered"] = _load("track_performance_v656_delivered", path)
    return _MODS["delivered"]


def _base_or_none():
    if "base" not in _MODS:
        path = os.getenv("TFB_TEST_TRACK_BASE", "").strip()
        if not path:
            try:
                blob = subprocess.run(["git", "-C", str(ROOT), "show", BASE_SHA + ":scripts/track_performance.py"],
                                      capture_output=True, timeout=60)
            except Exception:
                blob = None
            if blob is not None and blob.returncode == 0 and b'SCRIPT_VERSION = "6.53.0"' in blob.stdout:
                tmp = Path(tempfile.mkdtemp()) / "track_performance_v6530_base.py"
                tmp.write_bytes(blob.stdout)
                path = str(tmp)
        _MODS["base"] = _load("track_performance_v6530_base", path) if path else None
    return _MODS["base"]


def base():
    if _base_or_none() is None:
        pytest.skip("base copy unavailable (set TFB_TEST_TRACK_BASE)")
    return _MODS["base"]


def _vt(s):
    return tuple(int(x) for x in str(s).split(".")[:3])


# --------------------------------------------------------------- helpers ---
def _sessions(tp, sym, around=NOW):
    """(calendar name, sessions) or ('RAISES', message) - comparable across copies."""
    tp._outcome_calendar_schedule.cache_clear()
    try:
        name, sess = tp._outcome_sessions(sym, around)
        return name, tuple((d, o.isoformat(), c.isoformat()) for d, o, c in sess)
    except Exception as exc:  # noqa: BLE001
        return "RAISES", "%s:%s" % (type(exc).__name__, exc)


EXT_SAMPLE = {"MX": "WALMEX.MX", "KL": "1155.KL", "JO": "NPN.JO", "MC": "SAN.MC", "SA": "PETR4.SA",
              "TW": "2330.TW", "IR": "RYA.IR", "AT": "ETE.AT", "F": "SAP.F", "BO": "500325.BO",
              "SS": "600519.SS", "BK": "PTT.BK", "JK": "BBCA.JK", "KQ": "035720.KQ", "TWO": "6488.TWO",
              "BA": "GGAL.BA", "SN": "SQM-B.SN", "IC": "MAREL.IC", "PR": "CEZ.PR", "BD": "OTP.BD",
              "LM": "BAP.LM", "PH": "SM.PH", "TL": "LHV1T.TL", "RG": "GRD1R.RG", "VS": "TEL1L.VS",
              "CR": "HT.CR", "RO": "TLV.RO", "PK": "OGDC.PK", "CN": "WEED.CN", "NE": "AW-UN.NE"}
BASE_SAMPLE = ["AAPL.US", "AAPL", "BRK.B", "BRK-B.US", "2222.SR", "VOD.L", "BARC.LSE", "RY.TO", "XYZ.V",
               "BHP.AX", "7203.T", "0700.HK", "SAP.DE", "SAP.XETRA", "MC.PA", "ASML.AS", "UCB.BR",
               "ENI.MI", "NESN.SW", "NOVO-B.CO", "VOLV-B.ST", "EQNR.OL", "NOKIA.HE", "PKO.WA",
               "EDP.LS", "OMV.VI", "THYAO.IS", "NPN.JSE", "TEVA.TA", "D05.SG", "D05.SI", "005930.KS",
               "005930.KO", "FPH.NZ"]
REFUSE_SAMPLE = ["", "^N225", "GC=F", "SAR=X", "BTC-USD", "1234", "7203", "RELIANCE.NS", "000001.SZ",
                 "ABC.CA", "QNBK.QA", "EMAAR.AE", "ADNOC.AD", "ZAIN.KW", "BATELCO.BH", "FOO.ZZ"]
MATRIX = BASE_SAMPLE + list(EXT_SAMPLE.values()) + REFUSE_SAMPLE


def _set_gate(monkeypatch, word):
    if word is None:
        monkeypatch.delenv(GATE, raising=False)
    else:
        monkeypatch.setenv(GATE, word)


def _last_close(mic, before=NOW):
    cal = xc.get_calendar(mic)
    sess = cal.sessions_in_range((before - timedelta(days=12)).date().isoformat(), before.date().isoformat())
    closes = [cal.session_close(s).to_pydatetime() for s in sess]
    closes = [c for c in closes if c <= before - timedelta(minutes=30)]
    assert closes, mic
    return closes[-1]


def _boundaries(tp, monkeypatch):
    monkeypatch.setattr(tp, "_utc_now", lambda: NOW)
    monkeypatch.setattr(tp.RiyadhTime, "now", staticmethod(lambda: NOW))
    monkeypatch.setattr(tp, "_price_fallback_enabled", lambda: False)
    monkeypatch.setattr(tp, "_ca_guard_enabled", lambda: False)
    monkeypatch.setattr(tp, "_feed_legacy", lambda: True)
    monkeypatch.setattr(tp, "urlopen", lambda *a, **k: pytest.fail("network"))
    for key in ("TRACK_HIST_TARGET_CLOSE", "TRACK_AUDIT_DUE_ONLY", "TRACK_HIST_CLOSE_SOURCE",
                "TRACK_DEDUP_MATURED", "TRACK_RISK_BLANK", "TRACK_LOG_UNFORMATTED_READ"):
        monkeypatch.delenv(key, raising=False)
    tp._outcome_calendar_schedule.cache_clear()


def _record(tp, sym, target, entry=100.0):
    return tp.PerformanceRecord(
        record_id="r-" + sym, symbol=sym, horizon=tp.HorizonType.WEEK_1,
        date_recorded=target - timedelta(days=7), entry_price=entry,
        entry_recommendation=tp.RecommendationType.BUY, entry_score=70, entry_risk_bucket="MEDIUM",
        entry_confidence="HIGH", origin_tab="synthetic", target_price=110, target_roi=10,
        target_date=target, status=tp.PerformanceStatus.ACTIVE)


def _app(tp, rows):
    app = tp.PerformanceTrackerApp.__new__(tp.PerformanceTrackerApp)
    app.backend = tp.BackendClient("https://transport.invalid")
    app.backend.post_json = AsyncMock(return_value=({"rows": rows}, None, 200))
    return app


def _row(sym, quote_at, price=110.0):
    return {"symbol": sym, "current_price": price, "data_provider": "yahoo_chart",
            "acquisition_status": "success", "acquisition_provider": "yahoo_chart",
            "acquisition_acquired_at": NOW.isoformat(), "acquisition_quote_asof": quote_at.isoformat()}


def audit_one(tp, monkeypatch, sym, mic, quote_shift=timedelta(0), target=None):
    """The REAL audit loop on one due record -> (status, outcome, receipt dict)."""
    _boundaries(tp, monkeypatch)
    close = _last_close(mic)
    target = target or (close - timedelta(hours=3))
    rec = _record(tp, sym, target)
    asyncio.run(_app(tp, [_row(sym, close + quote_shift)]).audit_active_records([rec]))
    receipt = json.loads(rec.notes.rsplit(tp._OUTCOME_RECEIPT, 1)[1])
    return rec, receipt


def _strip_version(obj):
    return json.loads(re.sub(r'"tracker_version": "[0-9.]+"', '"tracker_version": "X"', json.dumps(obj, sort_keys=True)))


def _record_state(tp, rec):
    notes = rec.notes.rsplit(tp._OUTCOME_RECEIPT, 1)
    receipt = _strip_version(json.loads(notes[1])) if len(notes) == 2 else None
    return {"status": str(rec.status.value if hasattr(rec.status, "value") else rec.status),
            "outcome": rec.outcome, "realized_roi": rec.realized_roi, "notes_head": notes[0],
            "receipt": receipt}


# ------------------------------------------------------------------- C1 ----
def test_c1_parity_with_the_builder_maps():
    tp = delivered()
    from core.analysis import opportunity_builder as ob
    union = dict(ob._VENUE_CAL_MAP)
    union.update(ob._VENUE_CAL_EXT_MAP)
    want = {k: v for k, v in union.items() if k not in tp._OUTCOME_CALENDARS}
    assert tp._OUTCOME_CALENDARS_EXT == want, (set(tp._OUTCOME_CALENDARS_EXT) ^ set(want))
    assert len(tp._OUTCOME_CALENDARS_EXT) >= 30
    assert not set(tp._OUTCOME_CALENDARS_EXT) & set(tp._OUTCOME_CALENDARS)
    for key in tp._OUTCOME_CAL_EXT_EXCLUDED:
        assert key not in tp._OUTCOME_CALENDARS_EXT and key not in tp._OUTCOME_CALENDARS, key
        assert key not in union, key   # the builder rejects them too
    for mic in sorted(set(tp._OUTCOME_CALENDARS_EXT.values())):
        xc.get_calendar(mic)            # raises if the pinned library lacks it
    assert set(EXT_SAMPLE) == set(tp._OUTCOME_CALENDARS_EXT)


# ------------------------------------------------------------------- C2 ----
OFF_WORDS = [None, "", "off", "0", "false", "no", "enforce", "observe", "onn", "y"]


def test_c2_off_equals_the_untouched_v6530(monkeypatch):
    b, d = base(), delivered()
    want = {sym: _sessions(b, sym) for sym in MATRIX}
    for word in OFF_WORDS:
        _set_gate(monkeypatch, word)
        got = {sym: _sessions(d, sym) for sym in MATRIX}
        assert got == want, (word, {s: (got[s], want[s]) for s in MATRIX if got[s] != want[s]})
    assert all(want[s][0] == "RAISES" for s in EXT_SAMPLE.values())    # the defect on the base


@pytest.mark.parametrize("word", ["on", "ON", " on ", "1", "true", "Yes"])
def test_c2b_gate_words(monkeypatch, word):
    _set_gate(monkeypatch, word)
    assert delivered()._outcome_cal_ext_enabled() is True


@pytest.mark.parametrize("word", ["off", "0", "", "enforce", "observe", "onn", "y", "enabled", "2"])
def test_c2c_junk_is_off(monkeypatch, word):
    _set_gate(monkeypatch, word)
    assert delivered()._outcome_cal_ext_enabled() is False


# ------------------------------------------------------------------- C3 ----
def test_c3_on_maps_the_extension_and_keeps_everything_else(monkeypatch):
    tp = delivered()
    _set_gate(monkeypatch, None)
    off = {sym: _sessions(tp, sym) for sym in MATRIX}
    _set_gate(monkeypatch, "on")
    on = {sym: _sessions(tp, sym) for sym in MATRIX}
    for suffix, sym in EXT_SAMPLE.items():
        assert on[sym][0] == tp._OUTCOME_CALENDARS_EXT[suffix], (sym, on[sym][0])
        assert on[sym][1], sym                                    # real sessions came back
        assert off[sym][0] == "RAISES" and "outcome_calendar_unknown" in off[sym][1]
    for sym in BASE_SAMPLE + REFUSE_SAMPLE:
        assert on[sym] == off[sym], sym                           # base wins; refusals unchanged
    for sym in REFUSE_SAMPLE:
        assert on[sym][0] == "RAISES", sym
    assert on["BRK.B"][0] == "XNYS" and on["NPN.JSE"][0] == "XJSE" and on["SAP.F"][0] == "XFRA"


# ------------------------------------------------------------------- C4 ----
AUDIT_CASES = [("WALMEX.MX", "XMEX"), ("1155.KL", "XKLS"), ("NPN.JO", "XJSE"), ("SAN.MC", "XMAD"),
               ("PETR4.SA", "BVMF"), ("2330.TW", "XTAI"), ("RYA.IR", "XDUB"), ("ETE.AT", "ASEX"),
               ("SAP.F", "XFRA"), ("500325.BO", "XBOM"), ("600519.SS", "XSHG"), ("PTT.BK", "XBKK"),
               ("BBCA.JK", "XIDX")]


@pytest.mark.parametrize("sym,mic", AUDIT_CASES)
def test_c4_due_record_matures_only_with_the_gate(monkeypatch, sym, mic):
    tp = delivered()
    _set_gate(monkeypatch, "on")
    rec, receipt = audit_one(tp, monkeypatch, sym, mic)
    assert rec.status == tp.PerformanceStatus.MATURED and rec.outcome == "WIN", (sym, rec.status, receipt)
    assert receipt["state"] == "matured" or receipt.get("calendar") == mic, receipt
    assert receipt.get("calendar") == mic, receipt
    _set_gate(monkeypatch, None)
    rec_off, receipt_off = audit_one(tp, monkeypatch, sym, mic)
    assert rec_off.status == tp.PerformanceStatus.ACTIVE and receipt_off["reason"] == "outcome_calendar_unknown"
    b = _base_or_none()
    if b is not None:
        _set_gate(monkeypatch, "on")    # the base ignores the gate
        rec_b, _rb = audit_one(b, monkeypatch, sym, mic)
        _set_gate(monkeypatch, None)
        assert _record_state(tp, rec_off) == _record_state(b, rec_b)


BASE_AUDIT = [("AAPL.US", "XNYS"), ("2222.SR", "XSAU"), ("VOD.L", "XLON"), ("7203.T", "XTKS")]


@pytest.mark.parametrize("sym,mic", BASE_AUDIT)
def test_c4b_base_venues_identical_on_and_off(monkeypatch, sym, mic):
    tp = delivered()
    states = []
    for word in (None, "on"):
        _set_gate(monkeypatch, word)
        rec, _r = audit_one(tp, monkeypatch, sym, mic)
        states.append(_record_state(tp, rec))
    assert states[0] == states[1]
    assert states[0]["status"].upper().startswith("MATURED"), states[0]
    b = _base_or_none()
    if b is not None:
        rec_b, _rb = audit_one(b, monkeypatch, sym, mic)
        assert _record_state(b, rec_b) == states[0]


# ------------------------------------------------------------------- C5 ----
@pytest.mark.parametrize("sym,mic", [("WALMEX.MX", "XMEX"), ("PETR4.SA", "BVMF")])
def test_c5_quote_before_the_calendar_close_is_refused(monkeypatch, sym, mic):
    tp = delivered()
    _set_gate(monkeypatch, "on")
    rec, receipt = audit_one(tp, monkeypatch, sym, mic, quote_shift=-timedelta(hours=1))
    assert rec.status == tp.PerformanceStatus.ACTIVE, rec.status
    assert receipt["reason"] == "quote_before_target_close", receipt


# ------------------------------------------------------------------- C6 ----
@pytest.mark.parametrize("sym", ["500325.BO", "600519.SS"])
def test_c6_calendar_bound_fails_closed(monkeypatch, sym):
    tp = delivered()
    _set_gate(monkeypatch, "on")
    tp._outcome_calendar_schedule.cache_clear()
    late = tp.OutcomePrice(sym, 110.0, "yahoo_chart", datetime(2026, 12, 29, 12, 0, tzinfo=UTC),
                           datetime(2026, 12, 29, 10, 0, tzinfo=UTC))
    rec = _record(tp, sym, datetime(2026, 12, 28, 6, 0, tzinfo=UTC))
    _ctx, reason = tp._outcome_target_session(rec, late, datetime(2026, 12, 29, 12, 0, tzinfo=UTC))
    assert reason == "outcome_calendar_unknown", reason
    early_target = datetime(2026, 11, 10, 6, 0, tzinfo=UTC)
    name, sess = tp._outcome_sessions(sym, early_target)
    assert name in ("XBOM", "XSHG") and sess


# ------------------------------------------------------------------- C7 ----
STEP_NAMES = {".github/workflows/track_performance.yml": "Run performance clock",
              ".github/workflows/daily_sync.yml": "Track Performance (record + audit)"}
MAPPING = "TRACK_OUTCOME_CAL_EXT: ${{ vars.TRACK_OUTCOME_CAL_EXT || 'off' }}"


def _tracker_step(rel):
    text = (ROOT / rel).read_text(encoding="utf-8")
    parts = re.split(r"^\s{6}- name: ", text, flags=re.M)
    hits = [p for p in parts if p.splitlines() and STEP_NAMES[rel] in p.splitlines()[0]]
    assert len(hits) == 1, (rel, len(hits))
    return text, hits[0]


def test_c7_env_lane_parity_reader_and_ci():
    lines = {}
    for rel in STEP_NAMES:
        text, step = _tracker_step(rel)
        everywhere = [ln.strip() for ln in text.splitlines() if re.match(r"^\s+%s:" % GATE, ln)]
        in_step = [ln.strip() for ln in step.splitlines() if re.match(r"^\s+%s:" % GATE, ln)]
        assert everywhere == [MAPPING] and in_step == [MAPPING], (rel, everywhere, in_step)
        lines[rel] = in_step
    assert len(set(map(tuple, lines.values()))) == 1                      # ENV PARITY
    src = Path(delivered().__file__).read_text(encoding="utf-8")
    m = re.search(r"^def _outcome_cal_ext_enabled\(\)[^\n]*:\n(.*?)(?=^def |\Z)", src, re.M | re.S)
    assert m and re.search(r'os\.getenv\("%s"\)\s*or\s*"off"' % GATE, m.group(1))
    ci = (ROOT / ".github/workflows/ci.yml").read_text(encoding="utf-8")
    heavy = ci.split("\n  heavy:\n", 1)[1].split("\n  summary:\n", 1)[0]
    assert re.search(r"python -m pytest -q tests/test_track_outcome_cal_ext\.py", heavy)
    assert "git fetch --no-tags --depth=1 origin %s" % BASE_FULL in heavy
    assert BASE_FULL.startswith(BASE_SHA)


# ---------------------------------------------------------------- GOLD -----
def test_g1_golden_negative_base_never_matures_mx(monkeypatch):
    b = base()
    _set_gate(monkeypatch, "on")
    rec, receipt = audit_one(b, monkeypatch, "WALMEX.MX", "XMEX")
    assert rec.status == b.PerformanceStatus.ACTIVE and receipt["reason"] == "outcome_calendar_unknown"
    with pytest.raises(AssertionError):
        assert rec.status == b.PerformanceStatus.MATURED


# ------------------------------------------------------------------- V -----
def test_v1_version_pin_title_why_ascii():
    tp = delivered()
    assert _vt(tp.SCRIPT_VERSION) >= (6, 56, 0)
    src = Path(tp.__file__).read_text(encoding="utf-8")
    assert "(v%s)" % tp.SCRIPT_VERSION in src.splitlines()[4]
    vd = (ROOT / "scripts" / "verify_deployment.py").read_text(encoding="utf-8")
    assert '("track_performance", "%s", "track performance")' % tp.SCRIPT_VERSION in vd
    i56 = src.index("# v6.56.0 (2026-11-04) [OUTCOME-CAL-EXT]")
    assert src.index("# v6.53.0 (2026-11-02) [ARCHIVE-BAR + DUE-ONLY]") < i56 < src.index("\nSCRIPT_VERSION = ")
    assert all(ord(c) < 128 for c in Path(__file__).read_text(encoding="utf-8"))


def test_v1b_against_the_base_ascii_and_functions():
    import ast
    import difflib
    b = base()
    tp = delivered()
    if tp.SCRIPT_VERSION != "6.56.0":
        pytest.skip("the exact c54eba40 -> 6.56.0 diff checks apply to 6.56.0 only")
    old = Path(b.__file__).read_text(encoding="utf-8").splitlines()
    new = Path(tp.__file__).read_text(encoding="utf-8").splitlines()
    diff = list(difflib.unified_diff(old, new, lineterm="", n=0))
    added = [ln[1:] for ln in diff if ln.startswith("+") and not ln.startswith("+++")]
    removed = [ln[1:] for ln in diff if ln.startswith("-") and not ln.startswith("---")]
    title = "TADAWUL FAST BRIDGE \u2013 ADVANCED PERFORMANCE ANALYTICS ENGINE (v%s)"
    assert sorted(removed) == sorted([title % "6.53.0", 'SCRIPT_VERSION = "6.53.0"']), removed
    fresh = [ln for ln in added if ln != title % "6.56.0"]
    assert fresh and all(ln.isascii() for ln in fresh), [ln for ln in fresh if not ln.isascii()][:3]
    kinds = (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)

    def defs(text):
        return {n.name for n in ast.walk(ast.parse(text)) if isinstance(n, kinds)}
    assert defs("\n".join(new)) - defs("\n".join(old)) == {"_outcome_cal_ext_enabled"}
    assert not defs("\n".join(old)) - defs("\n".join(new))


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
    run_n = tally.passed + tally.failed
    print("PASS %d/%d" % (tally.passed, run_n) + (" (%d skipped)" % tally.skipped if tally.skipped else ""))
    sys.exit(rc)
