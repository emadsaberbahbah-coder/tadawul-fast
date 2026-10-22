"""F-7 v2 (data_engine_v2 v5.157.0) - settle re-derives scorer confidence,
final-pass tag, page scope.

Three gaps found on the 21/22 Oct exports block arming TFB_SCORING_SETTLE=
enforce (see WHY v5.157.0 in core/data_engine_v2.py):

  G1 core.scoring.compute_confidence_score reuses any existing confidence. A
     cold pass 1 lacks the 3M forecast fields, so the derived confidence is
     lower (0.6323 vs 0.6641 warm) and every settle pass reused it:
     reliability 69.3 instead of 71.5 on soft-capped rows.
  G2 the observe tag showed pass 1 -> pass 2, not what enforce publishes for
     rows settling at pass 3/4, and at most 6 fields (os= could be cut).
  G3 enforce must be scopable by page (Market_Leaders churn).

  [F7-CONF-RECOMPUTE] marker _f7_conf_derived (only while settle != off) on a
      scorer-derived confidence; settle passes clear the scorer's confidence
      keys on a marked iterate so they are re-derived; Phase-II's synthetic
      writer clears the marker; the marker is never published.
  [F7-FINAL-TAG]      the tag discloses pass 1 -> FINAL, up to 8 fields in the
      order ov op os rc va fcf r12 fs.
  [F7-PAGE-SCOPE]     TFB_SCORING_SETTLE_PAGES (csv, case-insensitive; unset =
      every page); enforce off-scope is handled exactly as observe.

REAL engine module, the six real 2026-09-22 Global_Markets fixtures and the
helpers of tests/test_de_f7_scoring_settle.py (FIXTURES, _prep, _pair,
_golden, _norm, _tags, _env, _failure_row). Cold/warm twins are built from the
two real late-target rows (SUBC.OL, FCN.US): cold = the 12M target restored
from the keep-last-good store inside Phase-II (after pass-1 scoring), warm =
the same target present at ingestion. Every test clears all TFB_* variables
and restores the environment and the target store afterwards.

Optional environment (read at import, never by the engine):
  TFB_F7V2_BASE=<dir | .py>   a source copy of the base commit (or its
      core/data_engine_v2.py), e.g.
        mkdir -p <dir> && git archive df99bcf3 | tar -x -C <dir>
      enables the base comparisons (off / observe published values and the
      synthetic enforce rows byte-identical to 5.156.0) and the golden
      negative (every C1 / C2 / C3 gate case FAILS on the base engine).
      Unset: those cases SKIP cleanly.
  TFB_F7V2_ENGINE=<.py>       run the whole suite against another engine file
      (e.g. the base copy, to watch the gate cases fail one by one).

Run: python -m pytest -q tests/test_de_f7_settle_v2.py   (or python tests/test_de_f7_settle_v2.py,
     which also prints an evidence digest that is identical run to run)
"""
from __future__ import annotations

import contextlib
import copy
import functools
import hashlib
import importlib.util
import inspect
import json
import os
import re
import sys
from unittest.mock import patch

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import core.data_engine_v2 as _REAL_DE  # noqa: E402
from tests import test_de_f7_scoring_settle as f7  # noqa: E402

try:
    import pytest
except ImportError:  # the __main__ runner needs no pytest
    pytest = None

VERSION = "5.157.0"
BASE_COMMIT = "df99bcf3"
ENGINE_ENV = "TFB_F7V2_ENGINE"
BASE_ENV = "TFB_F7V2_BASE"
PAGES_ENV = "TFB_SCORING_SETTLE_PAGES"
MARKER = "_f7_conf_derived"
GM, ML = "Global_Markets", "Market_Leaders"
CONF_KEYS = ("forecast_confidence", "confidence_score", "confidence_bucket")
# Priority order of the tag disclosures and the field behind each code; the
# kinds and tolerances are f7.FIELDS (the engine's _F7_SETTLE_FIELDS).
ORDER = ("ov", "op", "os", "rc", "va", "fcf", "r12", "fs")
CODE_KEY = {"ov": "overall_score", "op": "opportunity_score", "os": "opportunity_source",
            "rc": "recommendation", "va": "valuation_score", "fcf": "forecast_confidence",
            "r12": "expected_roi_12m", "fs": "forecast_source"}
FIELD = {key: (kind, tol) for key, kind, tol in f7.FIELDS}
TAG_PARTS_RE = re.compile(r"^f7_settle:(observe|enforce):st(\d+|x):p(\d+)((?::[a-z0-9]+=[^:;]*>[^:;]*)*)$")
PUBLISHED = ("forecast_reliability_score", "forecast_confidence", "data_quality_score",
             "investability_status", "final_action")

_ENGINE_PATH = os.environ.get(ENGINE_ENV, "").strip()
_BASE_PATH = os.environ.get(BASE_ENV, "").strip()
_LOADED = {}


def _load_engine(path, name):
    """Load an engine source file as its own module (shared core.* imports)."""
    path = os.path.abspath(path)
    if os.path.isdir(path):
        path = os.path.join(path, "core", "data_engine_v2.py")
    if path not in _LOADED:
        spec = importlib.util.spec_from_file_location(name, path)
        module = importlib.util.module_from_spec(spec)
        sys.modules[name] = module
        spec.loader.exec_module(module)
        _LOADED[path] = module
    return _LOADED[path]


DE = _load_engine(_ENGINE_PATH, "tfb_f7v2_engine_under_test") if _ENGINE_PATH else _REAL_DE


def _base():
    return _load_engine(_BASE_PATH, "tfb_f7v2_base_engine") if _BASE_PATH else None


class _Skip(Exception):
    pass


@contextlib.contextmanager
def _engine(de):
    """Point the reused f7 helpers (_prep / _pair / _golden) at engine `de`."""
    prev = f7.de
    f7.de = de
    try:
        yield de
    finally:
        f7.de = prev


def _isolated(fn):
    """Clear every TFB_* variable, restore the exact environment and every
    engine's keep-last-good target store afterwards."""
    @functools.wraps(fn)
    def wrapper(*args, **kwargs):
        saved = dict(os.environ)
        engines = {id(m): m for m in (_REAL_DE, DE, *_LOADED.values())}
        stores = {key: dict(m._TGT_LKG_STORE) for key, m in engines.items()}
        try:
            for key in list(os.environ):
                if key.startswith("TFB_"):
                    del os.environ[key]
            return fn(*args, **kwargs)
        finally:
            os.environ.clear()
            os.environ.update(saved)
            for key, m in engines.items():
                m._TGT_LKG_STORE.clear()
                m._TGT_LKG_STORE.update(stores[key])
    return wrapper


def _set(settle=None, pages=None, max_passes=4, **extra):
    f7._env(settle, max_passes)
    for name, value in ((PAGES_ENV, pages), *extra.items()):
        if value is None:
            os.environ.pop(name, None)
        else:
            os.environ[name] = str(value)


def _fx(symbol):
    return next(fx for fx in f7.FIXTURES if fx["row"]["symbol"] == symbol)


def _ordered(row, strip_tag=False):
    """Byte-level view: ordered (key, value) pairs minus the scorer clock.
    strip_tag drops f7_settle:* parts (and a warnings cell the tag created)."""
    out = []
    for key, value in row.items():
        if key in f7.VOLATILE:
            continue
        if strip_tag and key == "warnings" and isinstance(value, str):
            value = "; ".join(p.strip() for p in value.split(";")
                              if p.strip() and not p.strip().startswith("f7_settle:"))
            if not value:
                continue
        out.append((key, value))
    return json.dumps(out, default=repr)


def _one_tag(row):
    tags = f7._tags(row)
    assert len(tags) == 1, tags
    return tags[0]


def _parse(tag):
    m = TAG_PARTS_RE.match(tag)
    assert m, tag
    body = [p for p in m.group(4).split(":") if p]
    disclosed = []
    for part in body:
        code, rest = part.split("=", 1)
        before, after = rest.split(">", 1)
        disclosed.append((code, before, after))
    return m.group(1), m.group(2), m.group(3), disclosed


def _differs(de, key, a, b):
    """The engine's comparison rule for one decision field (f7.FIELDS)."""
    kind, tol = FIELD[key]
    if kind == "num":
        fa, fb = de._as_float(a), de._as_float(b)
        if fa is None and fb is None:
            return False
        return fa is None or fb is None or abs(fa - fb) > tol
    return de._safe_str(a).strip().upper() != de._safe_str(b).strip().upper()


def _settle(de, fx, mode, page=GM, pages=None):
    """(pass-1 snapshot, published row, pass-1 row object) for one fixture."""
    _set(mode, pages)
    row = f7._prep(fx)
    pass1 = copy.deepcopy(row)
    out = de._f7_settle_pass(row, row["symbol"], page)
    return pass1, out, row


def _twins(symbol, target_mult=None):
    """Cold (target restored inside Phase-II) and warm (target at ingestion)."""
    fx = _fx(symbol)
    src = fx["row"]
    fp12 = fx["lkg"]["fp12"] if target_mult is None else round(src["current_price"] * target_mult, 4)
    cold = {"cohort": "late", "lkg": {"fp12": fp12, "name": fx["lkg"]["name"]}, "row": dict(src)}
    warm = {"cohort": "early", "lkg": None,
            "row": dict(src, forecast_price_12m=fp12, forecast_source="provider_target")}
    return cold, warm


def _published(de, row):
    return de._strict_project_row(PUBLISHED, copy.deepcopy(row))


# =============================================================================
# Gate cases: each takes an engine module; each FAILS on the base engine.
# =============================================================================
def case_c1_cold_twin_settles_to_warm_confidence_and_reliability(de):
    """C1: under enforce the cold row's settled forecast_confidence and its
    published forecast_reliability_score equal the warm twin's."""
    with _engine(de):
        # the production gate formula (0.7 * confidence + 0.3 * DQ, soft-cap -5)
        _set("enforce", TFB_RELIABILITY_RECALIBRATION="1", TFB_INVESTABILITY_GATE="1")
        for symbol, mult in (("SUBC.OL", None), ("SUBC.OL", 1.40), ("FCN.US", None), ("FCN.US", 1.40)):
            cold_fx, warm_fx = _twins(symbol, mult)
            probe = de._apply_phase_bb_sanity(dict(cold_fx["row"]))
            assert all(probe.get(k) is None for k in ("expected_roi_3m", "forecast_price_3m", "forecast_price_12m"))
            cold_p1, cold, _ = _settle(de, cold_fx, "enforce")
            warm_p1, warm, _ = _settle(de, warm_fx, "enforce")
            # the cold fixture is cold: the target arrived inside Phase-II, the
            # 3M legs were filled there, and pass 1 scored without them
            assert any(p.startswith("analyst_lkg:") for p in str(cold_p1.get("warnings")).split("; "))
            assert cold_p1["forecast_source"] == "provider_target"
            assert cold_p1["expected_roi_3m"] is not None and cold_p1["forecast_price_3m"] is not None
            assert cold_p1["forecast_confidence"] < warm_p1["forecast_confidence"], (symbol, mult)
            assert cold["forecast_confidence"] == warm["forecast_confidence"] == warm_p1["forecast_confidence"], \
                (symbol, mult, cold_p1["forecast_confidence"], cold["forecast_confidence"], warm["forecast_confidence"])
            assert cold["confidence_score"] == warm["confidence_score"]
            assert cold["confidence_bucket"] == warm["confidence_bucket"]
            pc, pw = _published(de, cold), _published(de, warm)
            assert pc["forecast_reliability_score"] == pw["forecast_reliability_score"], \
                (symbol, mult, pc["forecast_reliability_score"], pw["forecast_reliability_score"])
            assert MARKER not in cold and MARKER not in warm


def case_c1_g1_evidence_soft_capped_row(de):
    """C1: the reviewed numbers - a soft-capped cold row read 69.3 (< the 70
    board floor) where its warm twin read 71.5; settled, both read 71.5."""
    with _engine(de):
        _set("enforce", TFB_RELIABILITY_RECALIBRATION="1", TFB_INVESTABILITY_GATE="1")
        cold_fx, warm_fx = _twins("SUBC.OL", 1.40)
        cold_p1, cold, _ = _settle(de, cold_fx, "enforce")
        warm_p1, warm, _ = _settle(de, warm_fx, "enforce")
        assert (cold_p1["forecast_confidence"], warm_p1["forecast_confidence"]) == (0.6323, 0.6641)
        assert (cold["forecast_confidence"], warm["forecast_confidence"]) == (0.6641, 0.6641), \
            (cold["forecast_confidence"], warm["forecast_confidence"])
        assert "provider_target_capped_for_short_horizon_derivation" in str(cold.get("warnings"))
        assert (_published(de, cold)["forecast_reliability_score"],
                _published(de, warm)["forecast_reliability_score"]) == (71.5, 71.5)


def case_c1_marker_provenance(de):
    """C1: the marker records a confidence the SCORER derived - set on
    provider-target rows while settle is armed, never in off, never for a
    reusable confidence, cleared by the Phase-II synthetic writer."""
    with _engine(de):
        for mode in (None, "off", "1"):
            _set(mode)
            for fx in f7.FIXTURES:
                assert MARKER not in f7._prep(fx), (mode, fx["row"]["symbol"])
        for mode in ("observe", "enforce"):
            _set(mode)
            for fx in f7.FIXTURES:
                row = f7._prep(fx)
                if fx["cohort"] == "synth":
                    assert row["forecast_source"] == "phase_ii_synthetic" and MARKER not in row
                else:
                    assert row.get(MARKER) is True, (mode, fx["row"]["symbol"])
        _set("enforce")
        for blank in ("", "  "):
            fx = dict(_fx("BNY.US"), row=dict(_fx("BNY.US")["row"], forecast_confidence=blank, confidence=None))
            assert f7._prep(fx).get(MARKER) is True, repr(blank)
        # a confidence the scorer REUSES is never marked and never re-derived
        for extra in ({"forecast_confidence": 0.8}, {"confidence": 70.0}, {"ai_confidence": 0.7}):
            fx = dict(_fx("SUBC.OL"), row=dict(_fx("SUBC.OL")["row"], **extra))
            row = f7._prep(fx)
            assert MARKER not in row, extra
            reused = row["forecast_confidence"]
            out = de._f7_settle_pass(row, row["symbol"], GM)
            assert out["forecast_confidence"] == reused, extra
        # a settlement call never creates the marker
        row = dict(_fx("BNY.US")["row"], _f7_settlement_active=True)
        de._compute_scores_canonical_first(row)
        assert MARKER not in row


def case_c2_tag_discloses_final_values_enforce_publishes(de):
    """C2: for every fixture (st3 and st4 rows) the observe tag's 'after'
    values are exactly what enforce publishes, in the priority order."""
    with _engine(de):
        settled_at, orders = set(), []
        for fx in f7.FIXTURES:
            p1, obs, _ = _settle(de, fx, "observe")
            _, enf, _ = _settle(de, fx, "enforce")
            otag, etag = _one_tag(obs), _one_tag(enf)
            mode, st, _passes, disclosed = _parse(otag)
            assert mode == "observe" and etag == otag.replace("f7_settle:observe:", "f7_settle:enforce:", 1)
            settled_at.add(st)
            fmt = {code: (lambda v, kind=FIELD[CODE_KEY[code]][0]: de._f7_settle_fmt(kind, v)) for code in ORDER}
            want = {code: (fmt[code](p1.get(CODE_KEY[code])), fmt[code](enf.get(CODE_KEY[code])))
                    for code in ORDER if _differs(de, CODE_KEY[code], p1.get(CODE_KEY[code]), enf.get(CODE_KEY[code]))}
            got = {code: (before, after) for code, before, after in disclosed}
            # every field that moved, pass 1 (= what observe publishes) -> what enforce publishes
            assert got == want, (fx["row"]["symbol"], "st" + st, got, want)
            assert all(before == fmt[code](obs.get(CODE_KEY[code])) for code, (before, _a) in got.items())
            orders.append((fx["row"]["symbol"], [code for code, _b, _a in disclosed], [c for c in ORDER if c in want]))
        assert {"3", "4"} <= settled_at, settled_at
        for symbol, disclosed_order, priority_order in orders:
            assert disclosed_order == priority_order, (symbol, disclosed_order)


def case_c2_tag_order_limit_and_degrade(de):
    """C2: up to 8 disclosures in the order ov op os rc va fcf r12 fs (the
    pre-v5.157.0 composer kept 6 in field order and could drop os=); the
    forbidden-substring degrade is unchanged."""
    old_order = [("ov", "1", "2"), ("op", "3", "4"), ("va", "5", "6"), ("fcf", "0.6", "0.7"),
                 ("r12", "0.1", "0.2"), ("rc", "hold", "sell"), ("os", "bpf", "rb"), ("fs", "pt", "sy")]
    want = "ov=1>2:op=3>4:os=bpf>rb:rc=hold>sell:va=5>6:fcf=0.6>0.7:r12=0.1>0.2:fs=pt>sy"
    got = de._f7_settle_tag("observe", 3, 2, old_order)
    assert got == "f7_settle:observe:st3:p2:" + want, got
    assert de._f7_settle_tag("enforce", None, 3, old_order + [("zz", "a", "b"), ("yy", "c", "d")]) == \
        "f7_settle:enforce:stx:p3:" + want
    assert de._f7_settle_tag("observe", 4, 3, list(reversed(old_order))) == "f7_settle:observe:st4:p3:" + want
    assert de._f7_settle_tag("observe", 3, 2, [("ov", "1", "2"), ("rc", "roi_x", "b")]) == \
        "f7_settle:observe:st3:p2:chg2"
    assert de._f7_settle_tag("observe", 3, 2, old_order + [("rc", "roi_x", "b")]) == \
        "f7_settle:observe:st3:p2:chg9"
    assert de._f7_settle_tag("enforce", None, 3, [("ov", "76.78", "65.71"), ("os", "bpf", "rb")]) == \
        "f7_settle:enforce:stx:p3:ov=76.78>65.71:os=bpf>rb"


def case_c3_enforce_publishes_only_on_scoped_pages(de):
    """C3: enforce + TFB_SCORING_SETTLE_PAGES=Global_Markets settles a GM row;
    the same row on Market_Leaders is handled exactly as observe."""
    with _engine(de):
        fx = _fx("SUBC.OL")
        for pages in ("Global_Markets", "global_markets", " GLOBAL_MARKETS ", "Market_Leaders_X, global_markets"):
            _set("enforce", pages)
            row = f7._prep(fx)
            golden = f7._golden(copy.deepcopy(row), 4)
            out = de._f7_settle_pass(row, row["symbol"], GM)
            assert out is not row and f7._norm(out) == f7._norm(golden), pages
            assert _one_tag(out).startswith("f7_settle:enforce:"), pages
            _, observed, obs_row = _settle(de, fx, "observe", page=ML)
            pass1, scoped, ml_row = _settle(de, fx, "enforce", page=ML, pages=pages)
            assert scoped is ml_row and obs_row is observed, (pages, "Market_Leaders row settled", f7._tags(scoped))
            assert f7._norm(scoped) == f7._norm(pass1) == f7._norm(observed), pages
            assert _one_tag(scoped) == _one_tag(observed) and _one_tag(scoped).startswith("f7_settle:observe:")
            _, blank, blank_row = _settle(de, fx, "enforce", page="", pages=pages)
            assert blank is blank_row and _one_tag(blank).startswith("f7_settle:observe:"), pages
        for pages in (None, "", " , ", "*", "all", "ALL", "Market_Leaders"):
            _, out, row = _settle(de, fx, "enforce", page=ML, pages=pages)
            assert out is not row and _one_tag(out).startswith("f7_settle:enforce:"), pages
        # non-stable results are fail-closed on every page, in or out of scope
        outs = []
        for page in (GM, ML):
            _set("enforce", "Global_Markets", max_passes=2)
            row = f7._prep(fx)
            with patch.object(de, "_compute_scores_canonical_first", lambda r: r.update(overall_score=r["overall_score"] + 1.0)), \
                    patch.object(de, "_apply_phase_dd_enhancements", lambda r: r):
                outs.append(de._f7_settle_pass(row, row["symbol"], page))
        assert de._f7_settle_failed(outs[0]) and f7._norm(outs[0]) == f7._norm(outs[1])


def case_c3_scope_disclosed_in_health_and_boot_line(de):
    """C3: /health engine_gates.scoring_settle_pages and the [GUARDS] leg."""
    for raw, want in ((None, "all"), ("", "all"), ("*", "all"), ("all", "all"),
                      ("Global_Markets", "global_markets"),
                      (" Global_Markets , Commodities_FX ", "global_markets,commodities_fx"),
                      ("Global_Markets,global_markets", "global_markets")):
        _set("enforce", raw)
        assert de.surface_gate_states()["scoring_settle_pages"] == want, raw
    src = inspect.getsource(de)
    assert src.count("scoring_settle_pages=%s") == 1
    assert src.count('"scoring_settle_pages": _f7_settle_pages_label(),') == 1
    assert src.count("_f7_settle_pages_label(),   # v5.157.0") == 1


GATE_CASES = (
    ("C1 cold twin settles to the warm confidence and reliability", case_c1_cold_twin_settles_to_warm_confidence_and_reliability),
    ("C1 G1 evidence: soft-capped 69.3 -> 71.5", case_c1_g1_evidence_soft_capped_row),
    ("C1 marker provenance", case_c1_marker_provenance),
    ("C2 tag discloses the final values enforce publishes", case_c2_tag_discloses_final_values_enforce_publishes),
    ("C2 tag order, limit and degrade", case_c2_tag_order_limit_and_degrade),
    ("C3 enforce publishes only on scoped pages", case_c3_enforce_publishes_only_on_scoped_pages),
    ("C3 scope disclosed in /health and the boot line", case_c3_scope_disclosed_in_health_and_boot_line),
)


# =============================================================================
# Identity and hygiene cases (hold on the base engine as well).
# =============================================================================
def case_off_is_byte_identical(de, base=None):
    """off: no marker anywhere, the row object returned untouched; with a base
    copy, every row is byte-identical to the base engine's (minus the scorer
    clock)."""
    with _engine(de):
        for mode in (None, "off", "1", "true"):
            for fx in f7.FIXTURES:
                _set(mode)
                row = f7._prep(fx)
                before = copy.deepcopy(row)
                out = de._f7_settle_pass(row, row["symbol"], GM)
                assert out is row and row == before and _ordered(row) == _ordered(before)
                assert MARKER not in row and not f7._tags(row)
                if base is not None:
                    with _engine(base):
                        brow = f7._prep(fx)
                        bout = base._f7_settle_pass(brow, brow["symbol"], GM)
                    assert _ordered(out) == _ordered(bout), (mode, fx["row"]["symbol"])


def case_observe_publishes_pass1_values(de, base=None):
    """observe: the published row is the pass-1 row byte-for-byte (minus the
    transient marker) plus ONE tag; with a base copy it is byte-identical to
    the base engine's observe row apart from the tag text."""
    with _engine(de):
        for fx in f7.FIXTURES:
            pass1, out, row = _settle(de, fx, "observe")
            assert out is row and MARKER not in out
            pass1.pop(MARKER, None)
            assert _ordered(out, strip_tag=True) == _ordered(pass1, strip_tag=True), fx["row"]["symbol"]
            assert len(f7._tags(out)) == 1 and not any(f in _one_tag(out).lower() for f in f7.FORBIDDEN)
            if base is not None:
                with _engine(base):
                    _, bout, _ = _settle(base, fx, "observe")
                assert _ordered(out, strip_tag=True) == _ordered(bout, strip_tag=True), fx["row"]["symbol"]
                assert _parse(_one_tag(out))[:3] == _parse(_one_tag(bout))[:3]


def case_synthetic_rows_settle_as_before(de, base=None):
    """C1 leaves Phase-II's confidence alone: the synthetic rows carry no
    marker, settle to the unmodified golden, and (with a base copy) publish
    values byte-identical to base 5.156.0 enforce; only the tag text moves."""
    with _engine(de):
        for fx in f7.FIXTURES:
            if fx["cohort"] != "synth":
                continue
            _set("enforce")
            row = f7._prep(fx)
            assert MARKER not in row and row["forecast_source"] == "phase_ii_synthetic"
            golden = f7._golden(copy.deepcopy(row), 4)
            out = de._f7_settle_pass(row, row["symbol"], GM)
            assert out is not row and f7._norm(out) == f7._norm(golden)
            if base is not None:
                with _engine(base):
                    _, bout, _ = _settle(base, fx, "enforce")
                assert _ordered(out, strip_tag=True) == _ordered(bout, strip_tag=True), fx["row"]["symbol"]
                assert _parse(_one_tag(out))[:3] == _parse(_one_tag(bout))[:3]


def case_marker_never_published(de):
    """The marker never leaves _f7_settle_pass: settled (st3/st4) in observe
    and enforce, stable at pass 2, error, pass_limit / oscillation under every
    TFB_SETTLE_UNRESOLVED mode, a failure receipt, off after a mode flip."""
    with _engine(de):
        for mode in ("observe", "enforce"):
            for fx in f7.FIXTURES:
                pass1, out, row = _settle(de, fx, mode)
                assert MARKER not in out and MARKER not in row, (mode, fx["row"]["symbol"])
            _set(mode)
            row = f7._prep(_fx("SUBC.OL"))
            result = de._f7_settle_result(row)
            assert result.status == "stable" and result.settled_at == 3
            assert row.get(MARKER) is True and MARKER not in result.row   # caller's row untouched
        # stable at pass 2: a settled row re-scored with its confidence cleared
        _, settled, _ = _settle(de, _fx("BNY.US"), "enforce")
        again = copy.deepcopy(settled)
        for key in CONF_KEYS:
            again[key] = None
        f7._pair(again)
        assert again.get(MARKER) is True
        result = de._f7_settle_result(again)
        assert result.status == "stable" and result.settled_at == 2 and result.row is again
        out = de._f7_settle_pass(again, again["symbol"], GM)
        assert out is again and MARKER not in out

        def boom(r):
            raise RuntimeError("settle boom")

        def drift(r):
            r["overall_score"] += 1.0

        def oscillate(r):
            r["overall_score"] = 20.0 if r["overall_score"] == 80.0 else 80.0

        for mode in ("observe", "enforce"):
            for unres in (None, "observe", "enforce"):
                for scorer, passes in ((boom, 4), (drift, 2), (oscillate, 4)):
                    _set(mode, max_passes=passes, TFB_SETTLE_UNRESOLVED=unres)
                    row = f7._prep(_fx("BNY.US"))
                    if scorer is oscillate:
                        row["overall_score"] = 80.0
                    assert row.get(MARKER) is True
                    with patch.object(de, "_compute_scores_canonical_first", scorer), \
                            patch.object(de, "_apply_phase_dd_enhancements", lambda r: r):
                        out = de._f7_settle_pass(row, row["symbol"], GM)
                    assert MARKER not in out, (mode, unres, scorer.__name__)
            _set(mode)
            row = f7._prep(_fx("BNY.US"))
            row["warnings"] = (row.get("warnings") or "") + "; f7_settle_failed:error"
            out = de._f7_settle_pass(row, row["symbol"], GM)
            assert de._f7_settle_failed(out) and MARKER not in out
        _set("observe")
        row = f7._prep(_fx("FCN.US"))
        assert row.get(MARKER) is True
        before = copy.deepcopy(row)
        _set("off")
        out = de._f7_settle_pass(row, row["symbol"], GM)
        before.pop(MARKER)
        assert out is row and MARKER not in row and _ordered(row) == _ordered(before)
        # the one scoring entry point no settle pass follows (tests only)
        _set("enforce")
        row = de._apply_phase_bb_sanity(dict(_fx("BNY.US")["row"]))
        de._compute_scores_fallback(row)
        assert MARKER not in row and row.get("forecast_confidence") is not None


def evidence(de):
    """Canonical summary of what the suite measures (tags, settled decision
    fields, cold/warm twins, page scope) - its digest is printed by the
    runner and must be identical run to run."""
    with _engine(de):
        out = {"version": de.__version__, "fixtures": {}, "twins": {}, "scope": {}}
        for fx in f7.FIXTURES:
            _, obs, _ = _settle(de, fx, "observe")
            _, enf, _ = _settle(de, fx, "enforce")
            out["fixtures"][fx["row"]["symbol"]] = {
                "observe_tag": _one_tag(obs), "enforce_tag": _one_tag(enf),
                "enforce": {key: enf.get(key) for key in CODE_KEY.values()},
                "quality_score": enf.get("quality_score")}
        _set("enforce", TFB_RELIABILITY_RECALIBRATION="1", TFB_INVESTABILITY_GATE="1")
        for symbol, mult in (("SUBC.OL", None), ("SUBC.OL", 1.40), ("FCN.US", None), ("FCN.US", 1.40)):
            pair = {}
            for label, fx in zip(("cold", "warm"), _twins(symbol, mult)):
                p1, settled, _ = _settle(de, fx, "enforce")
                pair[label] = {"pass1_fc": p1.get("forecast_confidence"), "fc": settled.get("forecast_confidence"),
                               "rel": _published(de, settled)["forecast_reliability_score"]}
            out["twins"]["%s x%s" % (symbol, mult or "lkg")] = pair
        for key in ("TFB_RELIABILITY_RECALIBRATION", "TFB_INVESTABILITY_GATE"):
            os.environ.pop(key, None)
        for page in (GM, ML):
            _, scoped, row = _settle(de, _fx("SUBC.OL"), "enforce", page=page, pages=GM)
            out["scope"][page] = {"settled": scoped is not row, "tag": _one_tag(scoped)}
        return out


def _digest(payload):
    return hashlib.sha256(json.dumps(payload, sort_keys=True, default=repr).encode("utf-8")).hexdigest()


# =============================================================================
# pytest glue
# =============================================================================
@_isolated
def test_c1_cold_twin_settles_to_warm_confidence_and_reliability():
    case_c1_cold_twin_settles_to_warm_confidence_and_reliability(DE)


@_isolated
def test_c1_g1_evidence_soft_capped_row():
    case_c1_g1_evidence_soft_capped_row(DE)


@_isolated
def test_c1_marker_provenance():
    case_c1_marker_provenance(DE)


@_isolated
def test_c2_tag_discloses_final_values_enforce_publishes():
    case_c2_tag_discloses_final_values_enforce_publishes(DE)


@_isolated
def test_c2_tag_order_limit_and_degrade():
    case_c2_tag_order_limit_and_degrade(DE)


@_isolated
def test_c3_enforce_publishes_only_on_scoped_pages():
    case_c3_enforce_publishes_only_on_scoped_pages(DE)


@_isolated
def test_c3_scope_disclosed_in_health_and_boot_line():
    case_c3_scope_disclosed_in_health_and_boot_line(DE)


@_isolated
def test_off_is_byte_identical():
    case_off_is_byte_identical(DE)


@_isolated
def test_observe_publishes_pass1_values():
    case_observe_publishes_pass1_values(DE)


@_isolated
def test_synthetic_rows_settle_as_before():
    case_synthetic_rows_settle_as_before(DE)


@_isolated
def test_marker_never_published():
    case_marker_never_published(DE)


def _need_base():
    base = _base()
    if base is None:
        raise _Skip("%s unset" % BASE_ENV)
    return base


def _skip_or_raise(exc):
    if pytest is not None:
        pytest.skip(str(exc))
    raise exc


@_isolated
def test_base_off_observe_and_synthetic_rows_byte_identical():
    try:
        base = _need_base()
    except _Skip as exc:
        return _skip_or_raise(exc)
    assert base.__version__ == "5.156.0", base.__version__
    case_off_is_byte_identical(DE, base)
    case_observe_publishes_pass1_values(DE, base)
    case_synthetic_rows_settle_as_before(DE, base)


@_isolated
def test_golden_negative_gate_cases_fail_on_base():
    try:
        base = _need_base()
    except _Skip as exc:
        return _skip_or_raise(exc)
    passed, failures = [], []
    for name, case in GATE_CASES:
        try:
            _isolated(case)(base)
        except Exception as exc:  # noqa: BLE001 - any failure on the base is the point
            failures.append("%s: %s" % (name, type(exc).__name__))
        else:
            passed.append(name)
    assert not passed, "gate cases did not fail on the %s copy: %s" % (BASE_COMMIT, passed)
    assert len(failures) == len(GATE_CASES)


@_isolated
def test_evidence_is_deterministic():
    first = evidence(DE)
    assert evidence(DE) == first
    assert first["scope"][GM]["settled"] and not first["scope"][ML]["settled"]


def test_wiring_and_version():
    src = inspect.getsource(DE)
    assert DE.__version__ == VERSION
    assert "Data Engine V2 - GLOBAL-FIRST ORCHESTRATOR - v" + DE.__version__ in src
    assert "WHY v5.157.0 - F-7 v2" in src
    # the one call site and the v5.147.0 / v5.153.0 contracts are unchanged
    assert src.count("merged = _f7_settle_pass(merged, sym, page_ctx)") == 1
    assert src.count('"scoring_settle": _f7_settle_mode(),') == 1 and src.count("scoring_settle=%s") == 1
    assert src.count("settle_unresolved=%s") == 1
    assert DE._F7_SETTLE_TAG_ORDER == ORDER and DE._F7_SETTLE_TAG_MAX == 8
    assert DE._F7_CONF_DERIVED_KEY == MARKER
    assert [code for _k, code, _kind, _tol in DE._F7_SETTLE_FIELDS] == ["ov", "op", "va", "fcf", "r12", "rc", "os", "fs"]
    assert "final_diff" in DE.F7SettlementResult.__dataclass_fields__


TESTS = [test_c1_cold_twin_settles_to_warm_confidence_and_reliability, test_c1_g1_evidence_soft_capped_row,
         test_c1_marker_provenance, test_c2_tag_discloses_final_values_enforce_publishes,
         test_c2_tag_order_limit_and_degrade, test_c3_enforce_publishes_only_on_scoped_pages,
         test_c3_scope_disclosed_in_health_and_boot_line, test_off_is_byte_identical,
         test_observe_publishes_pass1_values, test_synthetic_rows_settle_as_before,
         test_marker_never_published, test_base_off_observe_and_synthetic_rows_byte_identical,
         test_golden_negative_gate_cases_fail_on_base, test_evidence_is_deterministic,
         test_wiring_and_version]

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
    if _base() is not None:
        for name, case in GATE_CASES:
            try:
                _isolated(case)(_base())
                print("BASE-PASS", name)
            except Exception as exc:  # noqa: BLE001
                print("BASE-FAIL", name, "|", type(exc).__name__, str(exc).splitlines()[0][:160] if str(exc) else "")
    print("EVIDENCE", _digest(_isolated(evidence)(DE)))
    ran = len(TESTS) - skips
    print("RESULT", "PASS" if not fails else "FAIL", "%d/%d" % (ran - fails, ran),
          "(%d skipped)" % skips if skips else "", "engine", DE.__version__)
    sys.exit(1 if fails else 0)
