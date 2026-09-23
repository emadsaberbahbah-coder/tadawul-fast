"""tests/test_de_w52_ceiling_scrub_p164.py — data_engine_v2 v5.149.0 [P-164]

The EODHD field ceiling 999999.9999 (any float in [999999.0, 1000000.0)) reaches
week_52_high / week_52_low through two writers the provider's v4.13.0 AS-1 quote
scrub never sees — the 252-bar history max and the fundamentals Technicals block —
and the phase-BB pass then derives week_52_position_pct = 100% from it (2026-09-23
export: 012450.KS INVESTABLE, 009150.KS, YPFD.BA). v5.149.0 drops such a bound at
the engine's single 52W sanitizer (which runs inside scoring's sanitization, after
every writer and before any score reads the bound), clears the derived position,
and tags the row. Default ON; TFB_ENGINE_52W_CEILING_SCRUB=0 restores v5.148.0
byte-identically.

T1  golden negative: on v5.148.0 the sanitizer leaves the ceiling in place
    (nulled == 0); on v5.149.0 the bound is None, the position is None, the tags
    are present, nulled == 2.
T2  kill-switch: TFB_ENGINE_52W_CEILING_SCRUB=0 -> row deep-equal to the untouched
    input for the same fixtures (v5.148.0 behaviour).
T3  genuine values untouched: 000660.KS (2,987,000 / 293,000), an exact
    1,000,000.0, 999,998.99, 12.34 — no tag, no change.
T4  low-bound ceiling, both-bound ceiling, position only cleared when present,
    idempotent second pass (no duplicate tag, nulled == 0).
T5  the pre-existing branches (nonpositive, inverted, scale mismatch) are
    unchanged and still fire after a ceiling drop where applicable.
T6  disclosure: mode helper words; surface_gate_states() carries
    w52_ceiling_scrub; the [GUARDS] boot format string carries the w52_ceiling= leg.
T7  real page replay (opt-in via TFB_TEST_GM_TSV): zero false positives over the
    full Global_Markets export parsed from its display values (the ceiling renders
    as 1,000,000.00 = outside the band by construction), plus the three rows
    reconstructed at their backend value 999999.9999 all fire.

Run as a script for a JSON digest (dual-tree driver, separate processes).
"""
from __future__ import annotations

import copy
import csv
import inspect
import json
import os
import sys

import pytest

import core.data_engine_v2 as de

SAN = de._sanitize_corrupt_52w_bounds
CEIL = 999999.9999
TAG_HI = "sanitized:week_52_high_provider_ceiling"
TAG_LO = "sanitized:week_52_low_provider_ceiling"
TAG_POS = "sanitized:week_52_position_pct_unbounded"


def _row(hi, lo, cp=None, pos=None, warnings=""):
    r = {"symbol": "X", "week_52_high": hi, "week_52_low": lo, "current_price": cp,
         "week_52_position_pct": pos, "warnings": warnings}
    return r


def _tags(r):
    return [t.strip() for t in str(r.get("warnings") or "").split(";") if t.strip()]


def _setmode(monkeypatch, on: bool):
    monkeypatch.setenv("TFB_ENGINE_52W_CEILING_SCRUB", "1" if on else "0")


# T1 --------------------------------------------------------------------------
def test_t1_ceiling_dropped_position_cleared(monkeypatch):
    _setmode(monkeypatch, True)
    r = _row(CEIL, 791000.0, cp=1021000.0, pos=100.0)          # 012450.KS shape
    n = SAN(r)
    assert n == 2                                                # on v5.148.0 this is 0 -> golden negative
    assert r["week_52_high"] is None and r["week_52_low"] == 791000.0
    assert r["week_52_position_pct"] is None
    assert TAG_HI in _tags(r) and TAG_POS in _tags(r) and TAG_LO not in _tags(r)


# T2 --------------------------------------------------------------------------
@pytest.mark.parametrize("hi,lo,cp,pos", [
    (CEIL, 791000.0, 1021000.0, 100.0),
    (999999.0, 137500.0, 1507000.0, 100.0),
    (CEIL, 7600.0, 8530.0, 0.09),
    (CEIL, CEIL, 8530.0, None),
])
def test_t2_kill_switch_is_v5_148_byte_identical(monkeypatch, hi, lo, cp, pos):
    _setmode(monkeypatch, False)
    r = _row(hi, lo, cp=cp, pos=pos)
    before = copy.deepcopy(r)
    n = SAN(r)
    # v5.148.0 has no ceiling rule: only the scale-mismatch rule can fire, and it
    # nulls BOTH bounds with its own tag. Reproduce that expectation exactly.
    exp = copy.deepcopy(before)
    exp_n = 0
    if lo > 0 and hi / lo >= 1000.0:
        exp["week_52_high"] = None
        exp["week_52_low"] = None
        exp["warnings"] = "sanitized:week_52_bounds_scale_mismatch"
        exp_n = 2
    assert (n, r) == (exp_n, exp)


# T3 --------------------------------------------------------------------------
@pytest.mark.parametrize("hi,lo", [
    (2987000.0, 293000.0),      # 000660.KS genuine 7-digit KRW
    (1000000.0, 500000.0),      # exact million: outside the band
    (999998.99, 500000.0),      # just below the band
    (12.34, 8.1),
])
def test_t3_genuine_values_untouched(monkeypatch, hi, lo):
    _setmode(monkeypatch, True)
    r = _row(hi, lo, cp=hi * 0.9, pos=50.0)
    before = copy.deepcopy(r)
    assert SAN(r) == 0 and r == before


# T4 --------------------------------------------------------------------------
def test_t4_low_bound_both_bounds_idempotent(monkeypatch):
    _setmode(monkeypatch, True)
    r = _row(2500000.0, CEIL, cp=1800000.0, pos=10.0)
    assert SAN(r) == 2 and r["week_52_low"] is None and r["week_52_high"] == 2500000.0
    assert TAG_LO in _tags(r) and TAG_POS in _tags(r)
    r2 = _row(CEIL, 999999.5, cp=8530.0, pos=None)               # both bounds, no position present
    assert SAN(r2) == 2 and r2["week_52_high"] is None and r2["week_52_low"] is None
    assert TAG_HI in _tags(r2) and TAG_LO in _tags(r2) and TAG_POS not in _tags(r2)
    snap = copy.deepcopy(r2)
    assert SAN(r2) == 0 and r2 == snap                           # idempotent: nothing left to drop, no duplicate tag


# T5 --------------------------------------------------------------------------
def test_t5_existing_branches_unchanged(monkeypatch):
    _setmode(monkeypatch, True)
    r = _row(-5.0, 1.0); assert SAN(r) == 1 and "sanitized:week_52_high_nonpositive" in _tags(r)
    r = _row(10.0, 20.0); assert SAN(r) == 2 and "sanitized:week_52_bounds_inverted" in _tags(r)
    r = _row(5000.0, 1.0); assert SAN(r) == 2 and "sanitized:week_52_bounds_scale_mismatch" in _tags(r)
    # ceiling on the high + inverted remainder: ceiling drops first, then the low stands alone (no pair rule)
    r = _row(CEIL, 1200000.0, cp=1100000.0, pos=None)
    assert SAN(r) == 1 and r["week_52_high"] is None and r["week_52_low"] == 1200000.0


# T6 --------------------------------------------------------------------------
def test_t6_disclosure(monkeypatch):
    for word, exp in (("1", "on"), ("", "on"), ("on", "on"), ("0", "off"), ("false", "off"), ("OFF", "off"), ("no", "off")):
        monkeypatch.setenv("TFB_ENGINE_52W_CEILING_SCRUB", word)
        assert de._w52_ceiling_scrub_mode() == exp
    monkeypatch.delenv("TFB_ENGINE_52W_CEILING_SCRUB", raising=False)
    assert de._w52_ceiling_scrub_mode() == "on"
    gates = de.surface_gate_states()
    assert gates.get("w52_ceiling_scrub") == "on"
    assert "w52_ceiling=%s" in inspect.getsource(de)               # the [GUARDS] boot-line leg


# T7 --------------------------------------------------------------------------
def _num(s):
    s = str(s).strip().replace(",", "").replace("▲", "").replace("▼", "").replace("%", "").strip()
    try:
        return float(s)
    except ValueError:
        return None


def _gm_rows(path):
    with open(path, encoding="utf-8", newline="") as fh:
        rd = csv.DictReader(fh, delimiter="\t", quoting=csv.QUOTE_NONE)
        for r in rd:
            yield {"symbol": r.get("Symbol"), "week_52_high": _num(r.get("52W High")),
                   "week_52_low": _num(r.get("52W Low")), "current_price": _num(r.get("Current Price")),
                   "week_52_position_pct": _num(r.get("52W Position %")), "warnings": ""}


def _replay(path, on):
    os.environ["TFB_ENGINE_52W_CEILING_SCRUB"] = "1" if on else "0"
    fired = []
    n_rows = 0
    for r in _gm_rows(path):
        n_rows += 1
        before = copy.deepcopy(r)
        SAN(r)
        if TAG_HI in _tags(r) or TAG_LO in _tags(r):
            fired.append(r["symbol"])
    recon = [("012450.KS", CEIL, 791000.0, 1021000.0, 100.0), ("009150.KS", CEIL, 137500.0, 1507000.0, 100.0),
             ("YPFD.BA", CEIL, 7600.0, 8530.0, 0.09)]
    recon_fired = []
    for sym, hi, lo, cp, pos in recon:
        rr = _row(hi, lo, cp=cp, pos=pos); rr["symbol"] = sym
        SAN(rr)
        if rr["week_52_high"] is None and rr["week_52_position_pct"] is None and TAG_HI in _tags(rr):
            recon_fired.append(sym)
    return {"rows": n_rows, "page_fired": fired, "recon_fired": recon_fired}


@pytest.mark.skipif(not os.getenv("TFB_TEST_GM_TSV"), reason="set TFB_TEST_GM_TSV to a Global_Markets TSV export")
def test_t7_real_page_zero_false_positives_three_positives():
    out = _replay(os.environ["TFB_TEST_GM_TSV"], on=True)
    assert out["rows"] > 6000 and out["page_fired"] == []
    assert sorted(out["recon_fired"]) == ["009150.KS", "012450.KS", "YPFD.BA"]


# script mode ------------------------------------------------------------------
def _digest():
    out = {"version": de.__version__}
    for on in (True, False):
        os.environ["TFB_ENGINE_52W_CEILING_SCRUB"] = "1" if on else "0"
        fx = {}
        for name, (hi, lo, cp, pos) in {
            "ceiling_hi": (CEIL, 791000.0, 1021000.0, 100.0),
            "ceiling_lo": (2500000.0, CEIL, 1800000.0, 10.0),
            "ceiling_both": (CEIL, 999999.5, 8530.0, None),
            "genuine_krw": (2987000.0, 293000.0, 1876000.0, 58.76),
            "exact_million": (1000000.0, 500000.0, 900000.0, 80.0),
            "inverted": (10.0, 20.0, 15.0, None),
            "scale": (5000.0, 1.0, 100.0, None),
        }.items():
            r = _row(hi, lo, cp=cp, pos=pos)
            n = SAN(r)
            fx[name] = [n, r]
        out["mode_%s" % on] = fx
        p = os.getenv("TFB_TEST_GM_TSV")
        if p:
            out["replay_%s" % on] = _replay(p, on)
    return out


if __name__ == "__main__":
    print(json.dumps(_digest(), sort_keys=True, default=str))
