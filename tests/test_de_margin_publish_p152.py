"""P-152 / P-146b (data_engine_v2 v5.151.0) -- MARGIN PUBLISH CONTRACT at the publish boundary.

The three margin columns leave the engine in three units at once (yahoo
fractions, EODHD/sentry percent points, a few x100 artefacts) and the sheet
renders every margin cell under a percent number format, so points display
x100 (2026-09-28 export: 3,579 GM gross cells > 100 %, DDI profit 3,290.80 %).
v5.151.0 runs _margin_publish_contract(row) immediately AFTER
_apply_investability_gate at all three publish boundaries.

  off (unset) -> inert, rows byte-identical
  observe     -> ONE tag per margin field (margin_publish:<field>:<kind>:observe), values untouched
  enforce     -> value-bound explicit units determine conversion; legacy rows
                 with only magnitude/warning hints remain unresolved/blank.
                 Current producer/cache unit contracts have separate tests.

Run: python -m pytest -q tests/test_de_margin_publish_p152.py   (or python tests/...)
"""
from __future__ import annotations

import copy
import inspect
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import core.data_engine_v2 as de  # noqa: E402

ENV = "TFB_MARGIN_PUBLISH"
FORBIDDEN = ("cap", "forecast", "target", "roi", "drop", "reject", "provider_target",
             "price_bar_stale", "xprovider_price_conflict")

# real 2026-09-28 export rows (stored values; warnings = the witnesses)
ROWS = [
    {"symbol": "DDI.US", "gross_margin": 0.7336, "operating_margin": 0.3872, "profit_margin": 32.908,
     "warnings": "yahoo_enrichment_applied; fund_coherence_repaired:profit_margin:x100"},           # yahoo fractions + repaired points
    {"symbol": "FISV.US", "gross_margin": 47.2156, "operating_margin": 21.3745, "profit_margin": 0.0,
     "warnings": "eodhd_fundamentals_fallback_applied; fund_unit_contract:eodhd:profit_margin"},   # EODHD points; converted thin profit
    {"symbol": "1120.SR", "gross_margin": 0.6795, "operating_margin": 0.6104, "profit_margin": 67.951,
     "warnings": "yahoo_enrichment_applied; fund_coherence_repaired:profit_margin:x100"},
    {"symbol": "TOP.CO", "gross_margin": 100.3553, "operating_margin": 56.9584, "profit_margin": None,
     "warnings": "eodhd_fundamentals_fallback_applied"},                                            # > 100 points, <= 150: pts
    {"symbol": "THIN.X", "gross_margin": 1.2, "operating_margin": 0.9, "profit_margin": -0.15,
     "warnings": "eodhd_fundamentals_fallback_applied"},                                            # frac_amb: EODHD-only provenance, <= 1.5
    {"symbol": "OOB.X", "gross_margin": 7336.0, "operating_margin": -250.0, "profit_margin": 0.25,
     "warnings": ""},                                                                               # oob / oob / frac
]
MF = ("gross_margin", "operating_margin", "profit_margin")


def _env(mode):
    if mode is None:
        os.environ.pop(ENV, None)
    else:
        os.environ[ENV] = mode


def _tags(row):
    w = row.get("warnings") or ""
    return [p.strip() for p in str(w).split(";") if p.strip().startswith("margin_publish:")]


def _kinds(row):
    return {t.split(":")[1]: t.split(":")[2] for t in _tags(row)}


def test_t1_gate_explicit_words_only():
    for raw, want in (("", "off"), ("1", "off"), ("true", "off"), ("on", "off"), ("OBSERVE", "observe"),
                      ("enforce", "enforce"), (" Enforce ", "enforce"), ("log", "off")):
        os.environ[ENV] = raw
        assert de._margin_publish_mode() == want, (raw, want)
    _env(None)
    assert de._margin_publish_mode() == "off"


def test_t2_off_inert():
    _env(None)
    for r in ROWS:
        rr = copy.deepcopy(r)
        assert de._margin_publish_contract(rr) == 0
        assert rr == r


def test_t3_observe_tag_only():
    _env("observe")
    exp = {"DDI.US": {"gross_margin": "frac", "operating_margin": "frac", "profit_margin": "pts"},
           "FISV.US": {"gross_margin": "pts", "operating_margin": "pts", "profit_margin": "pts_thin"},
           "1120.SR": {"gross_margin": "frac", "operating_margin": "frac", "profit_margin": "pts"},
           "TOP.CO": {"gross_margin": "pts", "operating_margin": "pts"},
           "THIN.X": {"gross_margin": "frac_amb", "operating_margin": "frac_amb", "profit_margin": "frac_amb"},
           "OOB.X": {"gross_margin": "oob", "operating_margin": "oob", "profit_margin": "frac"}}
    for r in ROWS:
        rr = copy.deepcopy(r)
        n = de._margin_publish_contract(rr)
        assert n == len(exp[r["symbol"]])
        for f in MF:
            assert rr.get(f) == r.get(f)                      # values untouched
        assert all(t.endswith(":observe") for t in _tags(rr))
        assert _kinds(rr) == exp[r["symbol"]], (r["symbol"], _kinds(rr))
        # a second observe pass adds no duplicate tag
        w1 = rr["warnings"]
        de._margin_publish_contract(rr)
        assert rr["warnings"] == w1


def test_t4_enforce_converts_and_is_idempotent():
    # These immutable legacy specimens have diagnostic warning strings, not
    # a unit witness bound to the current field value. Never guess their unit.
    _env("enforce")
    for r in ROWS:
        rr = copy.deepcopy(r)
        de._margin_publish_contract(rr)
        k = _kinds(rr)
        for f in MF:
            v = r.get(f)
            if v is None:
                assert f not in k and rr.get(f) is None
                continue
            assert k[f] == "unresolved" and rr[f] is None
        assert all(not t.endswith(":observe") for t in _tags(rr))
        # idempotent: a second boundary pass changes nothing
        snap = copy.deepcopy(rr)
        assert de._margin_publish_contract(rr) == 0 and rr == snap
    # Observe tags cannot become a numerical unit proof when enforcement arms.
    _env("observe"); tr = copy.deepcopy(ROWS[0]); de._margin_publish_contract(tr)
    _env("enforce"); de._margin_publish_contract(tr)
    assert tr["profit_margin"] is None and "margin_publish:profit_margin:unresolved" in _tags(tr)
    snap = copy.deepcopy(tr); de._margin_publish_contract(tr); assert tr == snap


def test_t5_points_witness_rules():
    _env("enforce")
    thin = {"operating_margin": 1.2, "warnings": "fund_unit_contract:eodhd:operating_margin"}
    de._margin_publish_contract(thin)
    assert thin["operating_margin"] is None and _kinds(thin) == {"operating_margin": "unresolved"}
    obs = {"operating_margin": 1.2, "warnings": "fund_unit_contract:eodhd:operating_margin:observe"}
    de._margin_publish_contract(obs)
    assert obs["operating_margin"] is None and _kinds(obs) == {"operating_margin": "unresolved"}
    rep = {"profit_margin": 0.9, "warnings": "fund_coherence_repaired:profit_margin:d100"}
    de._margin_publish_contract(rep)
    assert rep["profit_margin"] is None and _kinds(rep) == {"profit_margin": "unresolved"}
    other = {"gross_margin": 0.9, "warnings": "fund_unit_contract:eodhd:profit_margin"}          # witness for ANOTHER field
    de._margin_publish_contract(other)
    assert other["gross_margin"] is None and _kinds(other) == {"gross_margin": "unresolved"}
    lst = {"gross_margin": 45.0, "warnings": ["fund_unit_contract:eodhd:gross_margin", "x"]}      # list-typed warnings
    de._margin_publish_contract(lst)
    assert lst["gross_margin"] is None and "margin_publish:gross_margin:unresolved" in str(lst["warnings"])


def test_t6_kinds_and_guards():
    _env("enforce")
    neg = {"gross_margin": -0.15, "profit_margin": -12.5, "warnings": "yahoo_enrichment_applied"}
    de._margin_publish_contract(neg)
    assert (neg["gross_margin"], neg["profit_margin"]) == (None, None)
    amb = {"gross_margin": 1.2, "warnings": "eodhd_fundamentals_fallback_applied"}
    de._margin_publish_contract(amb)
    assert amb["gross_margin"] is None and _kinds(amb) == {"gross_margin": "unresolved"}
    both = {"gross_margin": 1.2, "warnings": "eodhd_fundamentals_fallback_applied; yahoo_enrichment_applied"}
    de._margin_publish_contract(both)
    assert _kinds(both) == {"gross_margin": "unresolved"}
    edge = {"gross_margin": 1.5, "operating_margin": 1.5000001, "profit_margin": 150.0, "warnings": ""}
    de._margin_publish_contract(edge)
    assert _kinds(edge) == {f: "unresolved" for f in MF}
    assert edge["profit_margin"] is None
    junk = {"gross_margin": "junk", "operating_margin": True, "profit_margin": float("nan"), "warnings": None}
    assert de._margin_publish_contract(junk) == 3
    assert all(junk[field] is None for field in MF)
    assert _kinds(junk) == {field: "unresolved" for field in MF}
    assert de._margin_publish_contract("not-a-row") == 0
    assert de._margin_publish_contract({}) == 0
    # never raises even if a helper is broken
    orig = de._mpc_warning_parts
    de._mpc_warning_parts = lambda r: (_ for _ in ()).throw(RuntimeError("boom"))
    try:
        assert de._margin_publish_contract({"gross_margin": 45.0}) == 0
    finally:
        de._mpc_warning_parts = orig
    _env(None)


def test_t7_tags_substring_safe():
    for kind in ("frac", "frac_amb", "pts", "pts_thin", "oob"):
        for f in MF:
            for suffix in ("", ":observe"):
                tag = "margin_publish:%s:%s%s" % (f, kind, suffix)
                for bad in FORBIDDEN:
                    assert bad not in tag, (tag, bad)


def test_t8_wiring_and_version():
    src = inspect.getsource(de)
    assert src.count("_margin_publish_contract(row)  # v5.151.0") == 1
    assert src.count("_margin_publish_contract(_r)  # v5.151.0") == 2
    for gate, seam in (("    _apply_investability_gate(row)  # v5.78.0", "    _margin_publish_contract(row)  # v5.151.0"),
                       ("            _apply_investability_gate(_r)  # v5.78.0", "            _margin_publish_contract(_r)  # v5.151.0"),
                       ("                    _apply_investability_gate(_r)\n", "                    _margin_publish_contract(_r)  # v5.151.0")):
        i = src.index(gate)
        j = src.index(seam, i)
        assert 0 < j - i < 200, (gate, j - i)          # immediately AFTER the gate
    assert src.count('"margin_publish": _margin_publish_mode(),') == 1
    assert src.count("margin_publish=%s") == 1
    assert tuple(int(x) for x in de.__version__.split(".")[:3]) >= (5, 151, 0)


TESTS = [test_t1_gate_explicit_words_only, test_t2_off_inert, test_t3_observe_tag_only,
         test_t4_enforce_converts_and_is_idempotent, test_t5_points_witness_rules,
         test_t6_kinds_and_guards, test_t7_tags_substring_safe, test_t8_wiring_and_version]

if __name__ == "__main__":
    import traceback
    fails = 0
    for t in TESTS:
        try:
            t()
            print("PASS", t.__name__)
        except Exception:
            fails += 1
            print("FAIL", t.__name__)
            traceback.print_exc()
    print("RESULT", "PASS" if not fails else "FAIL", "%d/%d" % (len(TESTS) - fails, len(TESTS)))
    sys.exit(1 if fails else 0)
