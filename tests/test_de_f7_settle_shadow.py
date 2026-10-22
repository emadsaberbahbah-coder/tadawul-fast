"""F-7 v2b (data_engine_v2 v5.158.0) - the settle passes re-derive the
scorer's own confidence under enforce; observe publishes exactly what
v5.157.0 publishes and shows the v2 result only in its tag. core.scoring is
unchanged (5.11.4).

G1 (21/22 Oct exports): core.scoring.compute_confidence_score reuses any
existing confidence. A cold pass 1 (target restored inside Phase-II, the 3M
forecast fields absent when it was scored) derives 0.6323 where the warm row
derives 0.6641, and every settle pass reused it: soft-capped cold rows
publish forecast reliability 69.3 under enforce where the warm twin publishes
71.5 (the board floor is 70). From pass 2 on every row also counted its own
pass-1 confidence as a completeness field (quality +1.02).

  [F7-CONF-RECOMPUTE] v2 trajectory (_f7_settle_result(row, v2=True)):
    - provider rows: a pass-1 confidence the scorer DERIVED (the input carried
      none; marker _f7_conf_derived, made only while TFB_SCORING_SETTLE !=
      off) has forecast_confidence, confidence_score and confidence_bucket
      cleared before each re-score, so the scorer re-derives it (warm);
    - synthetic rows (Phase-II replaced that confidence: _f7_conf_synthetic):
      ONLY forecast_confidence is cleared - the scorer reuses Phase-II's
      confidence_score (equal at 4 dp) and completeness no longer counts it.
    v2=False is exactly the v5.157.0 trajectory.
  _f7_settle_pass: enforce (in scope) publishes ONE v2 result as v5.157.0
    enforce publishes its result; observe (and enforce out of scope)
    publishes from the v1 result exactly as v5.157.0 observe - the v2 SHADOW
    result supplies only the f7_settle tag text (st<k>:p<n> + pass 1 -> final
    when stable, stx:p<n> + pass 1 -> 2 when enforce would hold the row back),
    appended after the holdback on rows the v1 result withholds.

REAL engine module, the six real 2026-09-22 Global_Markets fixtures and the
helpers of tests/test_de_f7_scoring_settle.py and tests/test_de_f7_settle_v2.py;
the marker cases also drive the REAL orchestrator offline. Every test clears
all TFB_* variables, freezes the scorer clock and restores the environment and
the target stores afterwards.

Optional environment (read at import, never by the engine):
  TFB_F7V2B_BASE=<dir | .py>  a source copy of v5.157.0 (c5075579), e.g.
      mkdir -p <dir> && git archive c5075579 | tar -x -C <dir>
      enables the identity comparisons against v5.157.0 (off byte for byte;
      observe, scoped observe and out-of-scope enforce identical except the
      f7_settle tags at MAX_PASSES 3-5; the v1 trajectory) and the golden
      negative: every gate case FAILS on v5.157.0.
  TFB_F7V2B_ENGINE=<.py>      run the whole suite against another engine file.
  Unset: those cases SKIP cleanly.

Run: python -m pytest -q tests/test_de_f7_settle_shadow.py   (or python tests/test_de_f7_settle_shadow.py,
     which also prints an evidence digest that is identical run to run)
"""
from __future__ import annotations

import asyncio
import contextlib
import copy
import functools
import hashlib
import inspect
import json
import os
import sys
from types import SimpleNamespace
from unittest.mock import patch

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import core.data_engine_v2 as _REAL_DE  # noqa: E402
import core.scoring as _SCORING  # noqa: E402
from tests import test_de_f7_scoring_settle as f7  # noqa: E402
from tests import test_de_f7_settle_v2 as v2s  # noqa: E402

try:
    import pytest
except ImportError:  # the __main__ runner needs no pytest
    pytest = None

VERSION = (5, 158, 0)
BASE_COMMIT = "c5075579"
ENGINE_ENV = "TFB_F7V2B_ENGINE"
BASE_ENV = "TFB_F7V2B_BASE"
GM, ML, PF = "Global_Markets", "Market_Leaders", "My_Portfolio"
MARKER, SYNTH = "_f7_conf_derived", "_f7_conf_synthetic"
TRANSIENT = (MARKER, SYNTH)
CONF_KEYS = ("forecast_confidence", "confidence_score", "confidence_bucket")
PUBLISHED = ("forecast_reliability_score", "forecast_confidence", "data_quality_score",
             "investability_status", "final_action")
GATE_ENV = {"TFB_RELIABILITY_RECALIBRATION": "1", "TFB_INVESTABILITY_GATE": "1"}

_ENGINE_PATH = os.environ.get(ENGINE_ENV, "").strip()
_BASE_PATH = os.environ.get(BASE_ENV, "").strip()

DE = v2s._load_module(_ENGINE_PATH, "tfb_f7v2b_engine_under_test") if _ENGINE_PATH else _REAL_DE


class _Skip(Exception):
    pass


def _base():
    if not _BASE_PATH:
        raise _Skip("%s unset" % BASE_ENV)
    return v2s._load_module(_BASE_PATH, "tfb_f7v2b_base_engine")


def _skip_or_raise(exc):
    # pytest.skip only inside a pytest run; the __main__ runner reports SKIP.
    if pytest is not None and os.environ.get("PYTEST_CURRENT_TEST"):
        pytest.skip(str(exc))
    raise exc


_engine = v2s._engine
_bytes = v2s._bytes
_untagged = v2s._untagged
_fx = v2s._fx
_one_tag = v2s._one_tag
_parse = v2s._parse
_differs = v2s._differs
_set = v2s._set
ORDER, CODE_KEY, FIELD = v2s.ORDER, v2s.CODE_KEY, v2s.FIELD


def _isolated(fn):
    """v2s._isolated (TFB_* cleared, scorer clock frozen, environment and
    every loaded engine's target store restored), also over this module's
    engines."""
    @functools.wraps(fn)
    def wrapper(*args, **kwargs):
        engines = [m for m in (DE, *v2s._LOADED.values()) if hasattr(m, "_TGT_LKG_STORE")]
        stores = [(m, dict(m._TGT_LKG_STORE)) for m in engines]
        try:
            return v2s._isolated(fn)(*args, **kwargs)
        finally:
            for m, store in stores:
                m._TGT_LKG_STORE.clear()
                m._TGT_LKG_STORE.update(store)
    return wrapper


def _settle(de, fx, mode, page=GM, pages=None, scope_page=None, max_passes=4, **extra):
    return v2s._settle(de, fx, mode, page, pages, scope_page, max_passes, **extra)


def _twins(symbol, target_mult=None):
    """Cold (target restored inside Phase-II) and warm (target at ingestion)."""
    fx = _fx(symbol)
    src = fx["row"]
    fp12 = fx["lkg"]["fp12"] if target_mult is None else round(src["current_price"] * target_mult, 4)
    cold = {"cohort": "late", "lkg": {"fp12": fp12, "name": fx["lkg"]["name"]}, "row": dict(src)}
    warm = {"cohort": "early", "lkg": None,
            "row": dict(src, forecast_price_12m=fp12, forecast_source="provider_target")}
    return cold, warm


def _sig(res):
    """Everything an F7SettlementResult carries, the settled row byte for byte."""
    return (res.status, res.settled_at, res.passes_run, res.reason, res.error_type, tuple(res.first_diff),
            tuple(res.last_diff), _bytes(res.row) if res.row is not None else None)


def _project(de, row):
    return de._strict_project_row(PUBLISHED, copy.deepcopy(row))


def _quality_without_confidence(row):
    """core.scoring's quality for this row with forecast_confidence ABSENT -
    the pass-1 basis of a row whose input carried no confidence."""
    return _SCORING.compute_scores(dict(row, forecast_confidence=None))["quality_score"]


def _synthetic_twins(symbol):
    syn = _fx(symbol)
    prov = _fx(symbol, forecast_price_12m=round(syn["row"]["current_price"] * 1.1, 4),
               forecast_source="provider_target")
    return syn, prov


def _shadow_expect(de, pass1, enforced):
    """The observe tag's disclosure the v2 contract requires: every field that
    moved from pass 1 to what enforce publishes, (before, after) as the
    engine formats them."""
    want = {}
    for code in ORDER:
        key = CODE_KEY[code]
        if _differs(de, key, pass1.get(key), enforced.get(key)):
            kind = FIELD[key][0]
            fmt = de._f7_settle_fmt if kind == "num" else (lambda _k, v: de._f7_settle_token(v))
            want[code] = (fmt(kind, pass1.get(key)), fmt(kind, enforced.get(key)))
    return want


def _boom(r):
    raise RuntimeError("settle boom")


def _drift(r):
    r["overall_score"] += 1.0


def _oscillate(r):
    r["overall_score"] = 20.0 if r["overall_score"] == 80.0 else 80.0


# =============================================================================
# Gate cases: each takes an engine module; each FAILS on v5.157.0.
# =============================================================================
def case_c1_cold_twin_settles_to_warm_under_enforce(de):
    """C1: under enforce the cold row's settled forecast_confidence and its
    published forecast reliability (recalibrated gate) equal the warm twin's:
    SUBC.OL 74.3 -> 76.5 with its real target, 69.3 -> 71.5 with a +40%
    (soft-capped) target; FCN.US likewise."""
    with _engine(de):
        _set("enforce", **GATE_ENV)
        got = {}
        for symbol, mult in (("SUBC.OL", None), ("SUBC.OL", 1.40), ("FCN.US", None), ("FCN.US", 1.40)):
            cold_fx, warm_fx = _twins(symbol, mult)
            cold_p1, cold, _ = _settle(de, cold_fx, "enforce", **GATE_ENV)
            warm_p1, warm, _ = _settle(de, warm_fx, "enforce", **GATE_ENV)
            assert cold_p1["forecast_source"] == "provider_target"
            assert any(p.startswith("analyst_lkg:") for p in str(cold_p1.get("warnings")).split("; "))
            assert cold_p1["forecast_confidence"] < warm_p1["forecast_confidence"], (symbol, mult)
            assert cold["forecast_confidence"] == warm["forecast_confidence"] == warm_p1["forecast_confidence"], \
                (symbol, mult, cold["forecast_confidence"], warm["forecast_confidence"])
            assert (cold["confidence_score"], cold["confidence_bucket"]) == \
                   (warm["confidence_score"], warm["confidence_bucket"])
            pc, pw = _project(de, cold), _project(de, warm)
            assert pc["forecast_reliability_score"] == pw["forecast_reliability_score"], (symbol, mult)
            got[(symbol, mult)] = (cold_p1["forecast_confidence"], warm_p1["forecast_confidence"],
                                   pw["forecast_reliability_score"])
            assert not any(k in cold or k in warm for k in TRANSIENT)
        assert got[("SUBC.OL", None)] == (0.6323, 0.6641, 76.5), got
        assert got[("SUBC.OL", 1.40)] == (0.6323, 0.6641, 71.5), got


def case_c1_observe_shows_what_enforce_publishes(de):
    """C1 in observe: the cold row publishes its pass-1 values (as v5.157.0)
    while its tag shows the v2 enforce row: the warm confidence (SUBC.OL fcf
    0.6323 -> 0.6641, FCN.US 0.6164 -> 0.6482) and the settled values, st / p
    equal to the enforce tag's."""
    warm = {"SUBC.OL": ("0.6323", "0.6641"), "FCN.US": ("0.6164", "0.6482")}
    with _engine(de):
        for symbol, mult in (("SUBC.OL", None), ("SUBC.OL", 1.40), ("FCN.US", None), ("FCN.US", 1.40)):
            cold_fx, _ = _twins(symbol, mult)
            p1, observed, row = _settle(de, cold_fx, "observe")
            _, enforced, _ = _settle(de, cold_fx, "enforce")
            assert observed is row and _untagged(observed) == _untagged(_strip(p1)), (symbol, mult)
            mode, st, passes, disclosed = _parse(_one_tag(observed))
            emode, est, epasses, _ = _parse(_one_tag(enforced))
            assert (mode, emode, st, passes) == ("observe", "enforce", est, epasses), (symbol, mult)
            got = {code: (b, a) for code, b, a in disclosed}
            assert got == _shadow_expect(de, p1, enforced), (symbol, mult, got)
            assert got["fcf"] == warm[symbol], (symbol, mult, got)


def case_fix1p_synthetic_rows_keep_phase_ii_confidence_without_completeness_gap(de):
    """FIX-1' (no core.scoring change): the settled quality of synthetic and
    provider twins of the same data is the same and equals the quality
    scored with the confidence absent (the pass-1 basis); on a synthetic v2
    iterate only forecast_confidence is cleared and the scorer reuses
    Phase-II's confidence_score - equal to Phase-II's confidence at 4 dp."""
    with _engine(de):
        for symbol in ("BRK-B.US", "SKG.L"):
            syn_fx, prov_fx = _synthetic_twins(symbol)
            syn_p1, syn, _ = _settle(de, syn_fx, "enforce")
            prov_p1, prov, _ = _settle(de, prov_fx, "enforce")
            assert syn["forecast_source"] == "phase_ii_synthetic" and prov["forecast_source"] == "provider_target"
            assert syn["quality_score"] == prov["quality_score"], (symbol, syn["quality_score"], prov["quality_score"])
            assert syn["quality_score"] == _quality_without_confidence(syn), (symbol, syn["quality_score"])
            assert prov["quality_score"] == _quality_without_confidence(prov), symbol
            assert syn_p1.get(SYNTH) is True and MARKER not in syn_p1, symbol
            assert prov_p1.get(MARKER) is True and SYNTH not in prov_p1, symbol
            assert not any(k in syn or k in prov for k in TRANSIENT)
    reused = []
    real_iter = de._f7_conf_v2_iterate

    def spy(cand):
        synth = bool(cand.get(SYNTH)) and not cand.get(MARKER)
        phase_ii = cand.get("forecast_confidence")
        kept = cand.get("confidence_score")
        real_iter(cand)
        if synth:
            assert cand["forecast_confidence"] is None and cand["confidence_score"] == kept
            reused.append((round(float(phase_ii), 4), _SCORING.compute_confidence_score(cand)[1]))

    with _engine(de), patch.object(de, "_f7_conf_v2_iterate", spy):
        for fx in [f for f in f7.FIXTURES if f["cohort"] == "synth"] + [_synthetic_twins("BRK-B.US")[0]]:
            _settle(de, fx, "enforce")
    assert reused and all(a == b for a, b in reused), reused


def case_enforce_is_the_v2_trajectory(de):
    """Enforce publishes the independent v2 golden on every fixture; observe
    publishes the pass-1 row and its tag's after-values are exactly what
    enforce publishes (the shadow and enforce agree on st / p and status)."""
    with _engine(de):
        for fx in f7.FIXTURES:
            _set("enforce")
            row = f7._prep(fx)
            golden = f7._golden(copy.deepcopy(row), 4, fx["row"])
            enforced = de._f7_settle_pass(row, row["symbol"], GM)
            assert f7._norm(enforced) == f7._norm(golden), fx["row"]["symbol"]
            p1, observed, orow = _settle(de, fx, "observe")
            assert observed is orow and _untagged(observed) == _untagged(_strip(p1))
            _, st, passes, disclosed = _parse(_one_tag(observed))
            assert _parse(_one_tag(enforced))[1:3] == (st, passes)
            assert {code: (b, a) for code, b, a in disclosed} == _shadow_expect(de, p1, enforced), fx["row"]["symbol"]


def case_rows_enforce_would_hold_back_carry_an_stx_shadow_tag(de):
    """A row whose v2 result is not stable: observe publishes exactly the v1
    handling (holdback or unresolved receipt) and then the shadow tag
    stx:p<n> + pass 1 -> 2; enforce holds the row back. MAX_PASSES 2 makes
    every fixture's v1 and v2 results non_converged (pass_limit)."""
    with _engine(de):
        for fx in f7.FIXTURES:
            for unresolved in (None, "observe", "enforce"):
                _set("observe", max_passes=2, TFB_SETTLE_UNRESOLVED=unresolved)
                row = f7._prep(fx)
                observed = de._f7_settle_pass(row, row["symbol"], GM)
                assert de._f7_settle_failed(observed) or de._f7_settle_unresolved(observed)
                tag = _one_tag(observed)
                assert tag.startswith("f7_settle:observe:stx:p1") and str(observed["warnings"]).endswith(tag), tag
                _set("enforce", max_passes=2, TFB_SETTLE_UNRESOLVED=unresolved)
                row = f7._prep(fx)
                v2 = de._f7_settle_result(copy.deepcopy(row), v2=True)
                assert v2.status == "non_converged" and v2.reason == "pass_limit"
                assert tag == de._f7_settle_tag("observe", None, v2.passes_run, list(v2.first_diff)), tag
                enforced = de._f7_settle_pass(row, row["symbol"], GM)
                assert de._f7_settle_failed(enforced) or de._f7_settle_unresolved(enforced)


def case_markers_made_only_while_armed_and_never_published(de):
    """The provenance markers: made on pass 1 only while TFB_SCORING_SETTLE !=
    off and only for a confidence the scorer derived; converted by Phase-II's
    synthetic writer; never made on a settlement iterate; never published -
    observe / enforce, stable / held back / unresolved, a failure receipt,
    off after a mode flip, the test-only delegator and the real factory."""
    with _engine(de):
        for mode in (None, "off", "1"):
            _set(mode)
            for fx in f7.FIXTURES:
                assert not any(k in f7._prep(fx) for k in TRANSIENT), (mode, fx["row"]["symbol"])
        for mode in ("observe", "enforce"):
            _set(mode)
            for fx in f7.FIXTURES:
                row = f7._prep(fx)
                key = SYNTH if fx["cohort"] == "synth" else MARKER
                assert row.get(key) is True and len([k for k in TRANSIENT if k in row]) == 1, (mode, fx["row"]["symbol"])
        _set("enforce")
        for extra in ({"forecast_confidence": 0.8}, {"confidence": 70.0}, {"ai_confidence": 0.7}, {"confidence_score": 66.41}):
            row = f7._prep(_fx("SUBC.OL", **extra))
            assert not any(k in row for k in TRANSIENT), extra
        for blank in ("", "  "):
            assert f7._prep(_fx("BNY.US", forecast_confidence=blank)).get(MARKER) is True, repr(blank)
        row = dict(_fx("BNY.US")["row"], _f7_settlement_active=True)
        de._compute_scores_canonical_first(row)
        assert MARKER not in row
        for mode in ("observe", "enforce"):
            for unresolved in (None, "observe", "enforce"):
                for scorer, passes in ((_boom, 4), (_drift, 2), (_oscillate, 4), (None, 4), (None, 2)):
                    for symbol in ("BNY.US", "BRK-B.US"):
                        _set(mode, max_passes=passes, TFB_SETTLE_UNRESOLVED=unresolved)
                        row = f7._prep(_fx(symbol))
                        assert any(k in row for k in TRANSIENT)
                        if scorer is _oscillate:
                            row["overall_score"] = 80.0
                        with contextlib.ExitStack() as stack:
                            if scorer is not None:
                                stack.enter_context(patch.object(de, "_compute_scores_canonical_first", scorer))
                                stack.enter_context(patch.object(de, "_apply_phase_dd_enhancements", lambda r: r))
                            out = de._f7_settle_pass(row, row["symbol"], GM)
                        assert not any(k in out or k in row for k in TRANSIENT), (mode, unresolved, scorer, symbol)
            _set(mode)
            row = f7._prep(_fx("BRK-B.US"))
            row["warnings"] = (row.get("warnings") or "") + "; f7_settle_failed:error"
            out = de._f7_settle_pass(row, row["symbol"], GM)
            assert de._f7_settle_failed(out) and not any(k in out for k in TRANSIENT)
        for symbol in ("FCN.US", "SKG.L"):
            _set("observe")
            row = f7._prep(_fx(symbol))
            before = _strip(copy.deepcopy(row))
            _set("off")
            out = de._f7_settle_pass(row, row["symbol"], GM)
            assert out is row and not any(k in row for k in TRANSIENT) and _bytes(row) == _bytes(before)
        _set("enforce")
        row = de._apply_phase_bb_sanity(dict(_fx("BNY.US")["row"]))
        de._compute_scores_fallback(row)
        assert not any(k in row for k in TRANSIENT) and row.get("forecast_confidence") is not None
    for mode in ("off", "observe", "enforce"):
        for symbol, page in (("SUBC.OL", GM), ("BRK-B.US", PF), ("2222.SR", "")):
            v2s._set(mode, **v2s.FACTORY_ENV)
            facts = _fx("BRK-B.US" if symbol != "SUBC.OL" else "SUBC.OL")["row"]
            published = asyncio.run(v2s._factory_engine(de, facts).get_enriched_quote_dict(symbol, page))
            assert published.get("overall_score") is not None and not any(k in published for k in TRANSIENT), (mode, symbol)


def _strip(row):
    return {key: value for key, value in row.items() if key not in TRANSIENT}


GATE_CASES = (
    ("C1 cold twin settles to warm under enforce", case_c1_cold_twin_settles_to_warm_under_enforce),
    ("C1 observe shows what enforce publishes", case_c1_observe_shows_what_enforce_publishes),
    ("FIX-1' synthetic rows keep Phase-II confidence without completeness gap",
     case_fix1p_synthetic_rows_keep_phase_ii_confidence_without_completeness_gap),
    ("enforce is the v2 trajectory", case_enforce_is_the_v2_trajectory),
    ("rows enforce would hold back carry an stx shadow tag", case_rows_enforce_would_hold_back_carry_an_stx_shadow_tag),
    ("markers made only while armed and never published", case_markers_made_only_while_armed_and_never_published),
)


# =============================================================================
# Identity against v5.157.0 (need TFB_F7V2B_BASE).
# =============================================================================
def case_identity_with_5157(de, base):
    """off: byte-identical (scorer labels, frozen timestamps). observe, observe
    in / out of scope and enforce out of scope: every published field equals
    v5.157.0 observe except the f7_settle tags, at MAX_PASSES 3, 4 and 5 on
    102 fixture variants - including variants whose v2 status differs from
    v1 - and on the error / oscillation / pass_limit paths under every
    TFB_SETTLE_UNRESOLVED mode. The v1 trajectory equals v5.157.0's."""
    v1_v2_status_differs = 0
    for max_passes in (3, 4, 5):
        for fx in v2s._trajectory_cases():
            with _engine(base):
                _set("off", max_passes=max_passes)
                b_off = base._f7_settle_pass(f7._prep(fx), fx["row"]["symbol"], GM)
                _set("observe", max_passes=max_passes)
                brow = f7._prep(fx)
                b_res = base._f7_settle_result(copy.deepcopy(brow))
                b_obs = base._f7_settle_pass(brow, brow["symbol"], GM)
            with _engine(de):
                _set("off", max_passes=max_passes)
                n_off = de._f7_settle_pass(f7._prep(fx), fx["row"]["symbol"], GM, scope_page=GM)
                assert _bytes(n_off) == _bytes(b_off), ("off", max_passes, fx["row"]["symbol"])
                _set("observe", max_passes=max_passes)
                nrow = f7._prep(fx)
                v1 = de._f7_settle_result(copy.deepcopy(nrow), v2=False)
                v2 = de._f7_settle_result(copy.deepcopy(nrow), v2=True)
                assert _sig(v1) == _sig(b_res), (max_passes, fx["row"]["symbol"])
                v1_v2_status_differs += (v1.status, v1.settled_at) != (v2.status, v2.settled_at)
                runs = {
                    "observe": de._f7_settle_pass(nrow, nrow["symbol"], GM, scope_page=GM),
                    "observe in scope": _settle(de, fx, "observe", pages=GM, scope_page=GM, max_passes=max_passes)[1],
                    "observe out of scope": _settle(de, fx, "observe", pages=ML, scope_page=GM, max_passes=max_passes)[1],
                    "enforce out of scope": _settle(de, fx, "enforce", pages=ML, scope_page=GM, max_passes=max_passes)[1],
                }
                for label, out in runs.items():
                    assert _untagged(out) == _untagged(b_obs), (label, max_passes, fx["row"]["symbol"])
    assert v1_v2_status_differs > 0
    for unresolved in (None, "observe", "enforce"):
        for scorer, max_passes in ((_boom, 4), (_drift, 2), (_oscillate, 4)):
            for symbol in ("BNY.US", "BRK-B.US"):
                outs = []
                for engine in (de, base):
                    with _engine(engine):
                        _set("observe", max_passes=max_passes, TFB_SETTLE_UNRESOLVED=unresolved)
                        row = f7._prep(_fx(symbol))
                        if scorer is _oscillate:
                            row["overall_score"] = 80.0
                        with patch.object(engine, "_compute_scores_canonical_first", scorer), \
                                patch.object(engine, "_apply_phase_dd_enhancements", lambda r: r):
                            outs.append(_untagged(engine._f7_settle_pass(row, row["symbol"], GM)))
                assert outs[0] == outs[1], (unresolved, scorer.__name__, symbol)


def evidence(de):
    """Canonical summary (twins, fixture tags and enforce values, synthetic
    twins) - its digest is printed by the runner and must be identical run to
    run."""
    with _engine(de):
        out = {"version": de.__version__, "scoring": _SCORING.__version__, "twins": {}, "fixtures": {},
               "synthetic_twins": {}}
        for symbol, mult in (("SUBC.OL", None), ("SUBC.OL", 1.40), ("FCN.US", None), ("FCN.US", 1.40)):
            pair = {}
            for label, fx in zip(("cold", "warm"), _twins(symbol, mult)):
                p1, settled, _ = _settle(de, fx, "enforce", **GATE_ENV)
                pair[label] = {"pass1_fc": p1.get("forecast_confidence"), "fc": settled.get("forecast_confidence"),
                               "rel": _project(de, settled)["forecast_reliability_score"]}
            out["twins"]["%s x%s" % (symbol, mult or "lkg")] = pair
        for fx in f7.FIXTURES:
            _, obs, _ = _settle(de, fx, "observe")
            _, enf, _ = _settle(de, fx, "enforce")
            out["fixtures"][fx["row"]["symbol"]] = {
                "observe_tag": _one_tag(obs), "enforce_tag": _one_tag(enf),
                "enforce": {key: enf.get(key) for key in CODE_KEY.values()}, "quality_score": enf.get("quality_score")}
        for symbol in ("BRK-B.US", "SKG.L"):
            out["synthetic_twins"][symbol] = [_settle(de, f, "enforce")[1].get("quality_score")
                                              for f in _synthetic_twins(symbol)]
        return out


def _digest(payload):
    return hashlib.sha256(json.dumps(payload, sort_keys=True, default=repr).encode("utf-8")).hexdigest()


# =============================================================================
# pytest glue
# =============================================================================
def _wrap(case):
    @_isolated
    def test():
        case(DE)
    test.__name__ = "test_" + case.__name__[len("case_"):]
    return test


test_c1_cold_twin_settles_to_warm_under_enforce = _wrap(case_c1_cold_twin_settles_to_warm_under_enforce)
test_c1_observe_shows_what_enforce_publishes = _wrap(case_c1_observe_shows_what_enforce_publishes)
test_fix1p_synthetic_rows_keep_phase_ii_confidence_without_completeness_gap = _wrap(
    case_fix1p_synthetic_rows_keep_phase_ii_confidence_without_completeness_gap)
test_enforce_is_the_v2_trajectory = _wrap(case_enforce_is_the_v2_trajectory)
test_rows_enforce_would_hold_back_carry_an_stx_shadow_tag = _wrap(case_rows_enforce_would_hold_back_carry_an_stx_shadow_tag)
test_markers_made_only_while_armed_and_never_published = _wrap(case_markers_made_only_while_armed_and_never_published)


@_isolated
def test_off_is_byte_identical_and_unmarked():
    """off (unset, off, any non-mode word): the row object comes back
    untouched and no marker is ever made."""
    with _engine(DE):
        for mode in (None, "off", "1", "true"):
            for fx in f7.FIXTURES:
                _set(mode)
                row = f7._prep(fx)
                before = _bytes(row)
                out = DE._f7_settle_pass(row, row["symbol"], GM, scope_page=GM)
                assert out is row and _bytes(row) == before and not f7._tags(row)
                assert not any(k in row for k in TRANSIENT)


@_isolated
def test_identity_with_5157():
    try:
        base = _base()
    except _Skip as exc:
        return _skip_or_raise(exc)
    assert base.__version__ == "5.157.0", base.__version__
    case_identity_with_5157(DE, base)


def test_scoring_module_is_5157s():
    """FIX-1' needs no scoring change: core.scoring is v5.157.0's file."""
    if not _BASE_PATH or not os.path.isdir(_BASE_PATH):
        return _skip_or_raise(_Skip("%s is not a base source directory" % BASE_ENV))
    with open(os.path.join(_BASE_PATH, "core", "scoring.py"), "rb") as fh:
        want = hashlib.sha256(fh.read()).hexdigest()
    with open(inspect.getsourcefile(_SCORING), "rb") as fh:
        assert hashlib.sha256(fh.read()).hexdigest() == want


def test_golden_negative_gate_cases_fail_on_5157():
    try:
        reference = _base()
    except _Skip as exc:
        return _skip_or_raise(exc)
    passed, failures = [], []
    for name, case in GATE_CASES:
        try:
            _isolated(case)(reference)
        except Exception as exc:  # noqa: BLE001 - any failure on the reference is the point
            failures.append("%s: %s" % (name, type(exc).__name__))
        else:
            passed.append(name)
    assert not passed, "gate cases did not fail on the %s copy: %s" % (BASE_COMMIT, passed)
    assert len(failures) == len(GATE_CASES)


@_isolated
def test_evidence_is_deterministic():
    first = evidence(DE)
    assert evidence(DE) == first
    assert first["twins"]["SUBC.OL x1.4"] == {"cold": {"pass1_fc": 0.6323, "fc": 0.6641, "rel": 71.5},
                                              "warm": {"pass1_fc": 0.6641, "fc": 0.6641, "rel": 71.5}}
    assert first["twins"]["SUBC.OL xlkg"]["cold"]["rel"] == first["twins"]["SUBC.OL xlkg"]["warm"]["rel"] == 76.5
    assert all(a == b for a, b in first["synthetic_twins"].values())


def test_wiring_and_version():
    src = inspect.getsource(DE)
    assert tuple(int(x) for x in DE.__version__.split(".")[:3]) >= VERSION
    assert "Data Engine V2 - GLOBAL-FIRST ORCHESTRATOR - v" + DE.__version__ in src
    assert "WHY v5.158.0 - F-7 v2b" in src
    params = inspect.signature(DE._f7_settle_result).parameters
    assert list(params) == ["row", "v2"] and params["v2"].default is True
    assert src.count("merged = _f7_settle_pass(merged, sym, page_ctx, scope_page=self._f7_scope_page(sym))") == 1
    assert src.count("result = _f7_settle_result(row, v2=(mode == \"enforce\"))") == 1
    assert src.count("shadow = _f7_settle_result(row, v2=True) if _f7_conf_marked(row) else result") == 1
    assert src.count("_f7_conf_v2_iterate(cand)") == 1
    assert (DE._F7_CONF_DERIVED_KEY, DE._F7_CONF_SYNTH_KEY) == TRANSIENT == DE._F7_CONF_TRANSIENT_KEYS
    assert DE._F7_CONF_SCORER_KEYS == CONF_KEYS
    assert DE._F7_SETTLE_TAG_ORDER == ORDER and DE._F7_SETTLE_TAG_MAX == 8
    # FIX-1' is engine-only: core.scoring has no confidence flag
    assert not hasattr(_SCORING, "_CONF_NOT_DATA_FLAG") and "_scoring_conf_not_data" not in src


TESTS = [test_c1_cold_twin_settles_to_warm_under_enforce, test_c1_observe_shows_what_enforce_publishes,
         test_fix1p_synthetic_rows_keep_phase_ii_confidence_without_completeness_gap,
         test_enforce_is_the_v2_trajectory, test_rows_enforce_would_hold_back_carry_an_stx_shadow_tag,
         test_markers_made_only_while_armed_and_never_published, test_off_is_byte_identical_and_unmarked,
         test_identity_with_5157, test_scoring_module_is_5157s, test_golden_negative_gate_cases_fail_on_5157,
         test_evidence_is_deterministic, test_wiring_and_version]

if __name__ == "__main__":
    import traceback
    fails = skips = 0
    for t in TESTS:
        try:
            t()
            print("PASS", t.__name__)
        except _Skip as exc:
            skips += 1
            print("SKIP", t.__name__, "|", exc)
        except Exception:
            fails += 1
            print("FAIL", t.__name__)
            traceback.print_exc()
    if _BASE_PATH:
        reference = _base()
        for name, case in GATE_CASES:
            try:
                _isolated(case)(reference)
                print("REF-PASS", reference.__version__, name)
            except Exception as exc:  # noqa: BLE001
                print("REF-FAIL", reference.__version__, name, "|", type(exc).__name__,
                      str(exc).splitlines()[0][:160] if str(exc) else "")
    print("EVIDENCE", _digest(_isolated(evidence)(DE)))
    ran = len(TESTS) - skips
    print("RESULT", "PASS" if not fails else "FAIL", "%d/%d" % (ran - fails, ran),
          "(%d skipped)" % skips if skips else "", "engine", DE.__version__, "scoring", _SCORING.__version__)
    sys.exit(1 if fails else 0)
