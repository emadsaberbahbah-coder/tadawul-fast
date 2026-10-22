"""F-7 v2 (data_engine_v2 v5.157.0, core.scoring v5.11.5) - settle re-derives
engine-made confidence, final-pass tag, page scope by market.

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
      keys on a marked iterate so they are re-derived. FIX-1: when Phase-II's
      synthetic writer replaces it the marker becomes _f7_conf_synthetic and
      the settle passes keep Phase-II's value but score it as no data
      (core.scoring _completeness_factor flag), so an engine-made confidence is
      never a completeness field when the input had none - provider and
      synthetic rows alike. No transient key is ever published.
  [F7-FINAL-TAG]      the tag discloses pass 1 -> FINAL, up to 8 fields in the
      order ov op os rc va fcf r12 fs.
  [F7-PAGE-SCOPE]     TFB_SCORING_SETTLE_PAGES, canonical page names (FIX-2:
      "Global Markets" == global-markets == Global_Markets; unknown names are
      ignored and logged once; only unknown names -> no page); enforce off
      scope is handled exactly as observe; the orchestrator decides the scope
      by the symbol's MARKET page (FIX-3: My_Portfolio / Top_10 contexts).
  FIX-4 tests/test_de_f7_scoring_settle.py _golden decides from the fixture
      input, never from the engine's own marker.

REAL engine module, the six real 2026-09-22 Global_Markets fixtures and the
helpers of tests/test_de_f7_scoring_settle.py (FIXTURES, _prep, _pair,
_golden, _norm, _tags, _env, _scorer_derives_confidence). Cold/warm twins come
from the two real late-target rows (SUBC.OL, FCN.US); synthetic/provider twins
from the real synthetic rows (BRK-B.US, SKG.L). Every test clears all TFB_*
variables and restores the environment and the target stores afterwards.

Optional environment (read at import, never by the engine):
  TFB_F7V2_BASE=<dir | .py>   a source copy of base df99bcf3 (engine 5.156.0,
      scoring 5.11.4), e.g.  mkdir -p <dir> && git archive df99bcf3 | tar -x -C <dir>
      enables the base comparisons (off / observe rows, input-confidence
      synthetic rows, core.scoring with the flag absent) and the golden
      negative: every C1 / C2 / C3 gate case FAILS on the base engine.
  TFB_F7V2_PREV=<dir | .py>   a copy of the PR head before the review fixes
      (87e4e52): every FIX-1 / FIX-2 / FIX-3 gate case FAILS on it.
  TFB_F7V2_ENGINE=<.py>       run the whole suite against another engine file.
  Unset: those cases SKIP cleanly.

Run: python -m pytest -q tests/test_de_f7_settle_v2.py   (or python tests/test_de_f7_settle_v2.py,
     which also prints an evidence digest that is identical run to run)
"""
from __future__ import annotations

import ast
import asyncio
import contextlib
import copy
import functools
import hashlib
import importlib.util
import inspect
import json
import os
import random
import re
import sys
from types import SimpleNamespace
from unittest.mock import patch

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import core.data_engine_v2 as _REAL_DE  # noqa: E402
import core.scoring as _SCORING  # noqa: E402
from tests import test_de_f7_scoring_settle as f7  # noqa: E402

try:
    import pytest
except ImportError:  # the __main__ runner needs no pytest
    pytest = None

VERSION = "5.157.0"
SCORING_VERSION = "5.11.5"
BASE_COMMIT = "df99bcf3"
PREV_COMMIT = "87e4e52"
ENGINE_ENV = "TFB_F7V2_ENGINE"
BASE_ENV = "TFB_F7V2_BASE"
PREV_ENV = "TFB_F7V2_PREV"
PAGES_ENV = "TFB_SCORING_SETTLE_PAGES"
MARKER = "_f7_conf_derived"
SYNTH = "_f7_conf_synthetic"
CONF_NOT_DATA = "_scoring_conf_not_data"
TRANSIENT = (MARKER, SYNTH, CONF_NOT_DATA)
GM, ML, MF, PF = "Global_Markets", "Market_Leaders", "Mutual_Funds", "My_Portfolio"
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
# core.scoring v5.11.4 _completeness_factor, verbatim, as the property reference.
CORE_FIELDS = (
    "symbol", "name", "currency", "exchange", "current_price", "previous_close",
    "day_high", "day_low", "week_52_high", "week_52_low", "volume", "market_cap",
    "pe_ttm", "pb_ratio", "ps_ratio", "dividend_yield", "rsi_14",
    "volatility_30d", "volatility_90d", "expected_roi_3m", "forecast_price_3m",
    "forecast_confidence",
)
LABELS = ("scoring_schema_version", "recommendation_source")

_ENGINE_PATH = os.environ.get(ENGINE_ENV, "").strip()
_BASE_PATH = os.environ.get(BASE_ENV, "").strip()
_PREV_PATH = os.environ.get(PREV_ENV, "").strip()
_LOADED = {}


def _load_module(path, name, member=os.path.join("core", "data_engine_v2.py")):
    """Load a source file as its own module (shared core.* imports)."""
    path = os.path.abspath(path)
    if os.path.isdir(path):
        path = os.path.join(path, member)
    elif os.path.basename(member) != os.path.basename(path):
        path = os.path.join(os.path.dirname(path), os.path.basename(member))
    if path not in _LOADED:
        spec = importlib.util.spec_from_file_location(name, path)
        module = importlib.util.module_from_spec(spec)
        sys.modules[name] = module
        spec.loader.exec_module(module)
        _LOADED[path] = module
    return _LOADED[path]


DE = _load_module(_ENGINE_PATH, "tfb_f7v2_engine_under_test") if _ENGINE_PATH else _REAL_DE


class _Skip(Exception):
    pass


def _need(path, env, name, member=os.path.join("core", "data_engine_v2.py")):
    if not path:
        raise _Skip("%s unset" % env)
    return _load_module(path, name, member)


def _base():
    return _need(_BASE_PATH, BASE_ENV, "tfb_f7v2_base_engine")


def _base_scoring():
    return _need(_BASE_PATH, BASE_ENV, "tfb_f7v2_base_scoring", os.path.join("core", "scoring.py"))


def _prev():
    return _need(_PREV_PATH, PREV_ENV, "tfb_f7v2_prev_engine")


def _skip_or_raise(exc):
    if pytest is not None:
        pytest.skip(str(exc))
    raise exc


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
        engines = {id(m): m for m in (_REAL_DE, DE, *_LOADED.values()) if hasattr(m, "_TGT_LKG_STORE")}
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


def _fx(fixture_symbol, **row_changes):
    """A real f7 fixture, optionally with changed input fields (incl. symbol)."""
    fx = next(fx for fx in f7.FIXTURES if fx["row"]["symbol"] == fixture_symbol)
    return dict(fx, row=dict(fx["row"], **row_changes)) if row_changes else fx


def _strip(row):
    """A row as published: without the transient F-7 keys."""
    return {key: value for key, value in row.items() if key not in TRANSIENT}


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


def _settle(de, fx, mode, page=GM, pages=None, scope_page=None):
    """(pass-1 snapshot, published row, pass-1 row object) for one fixture."""
    _set(mode, pages)
    row = f7._prep(fx)
    pass1 = copy.deepcopy(row)
    if scope_page is None:
        out = de._f7_settle_pass(row, row["symbol"], page)
    else:
        out = de._f7_settle_pass(row, row["symbol"], page, scope_page=scope_page)
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


def _project(de, row):
    return de._strict_project_row(PUBLISHED, copy.deepcopy(row))


def _scored_quality_without_confidence(row):
    """core.scoring's quality for this row with forecast_confidence counted as
    ABSENT - the pass-1 basis of a row whose input carried no confidence."""
    return _SCORING.compute_scores(dict(row, forecast_confidence=None))["quality_score"]


def _factory_engine(de, facts):
    """The REAL orchestrator (get_enriched_quote_dict -> factory) with offline
    provider I/O: every quote fetch returns `facts` for the requested symbol."""
    eng = de.DataEngineV5(settings=SimpleNamespace(), providers=["yahoo_chart"])

    async def fetch(provider, symbol, page=""):
        return copy.deepcopy(dict(facts, symbol=symbol))

    async def unchanged(row, *_args):
        return row

    async def empty(*_args, **_kwargs):
        return {}

    eng._fetch_patch = fetch
    eng._apply_yahoo_enrichment_pass = unchanged
    eng._apply_eodhd_fundamentals_fallback = unchanged
    eng._get_history_patch_best_effort = empty
    eng._apply_news_veto = empty
    return eng


FACTORY_ENV = {"TFB_ENGINE_TARGET_KLG": "0", "TFB_ENGINE_FUND_LKG": "0", "TFB_ENGINE_BAR_AGE_GATE": "0",
               "TFB_ENGINE_XPROVIDER_VERIFY": "0", "TFB_MARGIN_PUBLISH": "off"}


# =============================================================================
# C1 / C2 / C3 gate cases: each takes an engine module; each FAILS on base.
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
            pc, pw = _project(de, cold), _project(de, warm)
            assert pc["forecast_reliability_score"] == pw["forecast_reliability_score"], \
                (symbol, mult, pc["forecast_reliability_score"], pw["forecast_reliability_score"])
            assert not any(k in cold or k in warm for k in TRANSIENT)


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
        assert (_project(de, cold)["forecast_reliability_score"],
                _project(de, warm)["forecast_reliability_score"]) == (71.5, 71.5)


def case_c1_marker_provenance(de):
    """C1: the marker records a confidence the SCORER derived - set on
    provider-target rows while settle is armed, never in off, never for a
    reusable confidence; on synthetic rows it becomes _f7_conf_synthetic."""
    with _engine(de):
        for mode in (None, "off", "1"):
            _set(mode)
            for fx in f7.FIXTURES:
                row = f7._prep(fx)
                assert not any(k in row for k in TRANSIENT), (mode, fx["row"]["symbol"])
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
            assert f7._prep(_fx("BNY.US", forecast_confidence=blank, confidence=None)).get(MARKER) is True, repr(blank)
        # a confidence the scorer REUSES is never marked and never re-derived
        for extra in ({"forecast_confidence": 0.8}, {"confidence": 70.0}, {"ai_confidence": 0.7}):
            row = f7._prep(_fx("SUBC.OL", **extra))
            assert not any(k in row for k in TRANSIENT), extra
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
    forbidden-substring degrade is unchanged; a tag may carry no field at all
    when pass 1 -> final nets inside the (non-transitive) tolerance."""
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
    assert de._f7_settle_tag("observe", 3, 2, []) == "f7_settle:observe:st3:p2"


def case_c3_enforce_publishes_only_on_scoped_pages(de):
    """C3: enforce + TFB_SCORING_SETTLE_PAGES=Global_Markets settles a GM row;
    the same row on Market_Leaders is handled exactly as observe."""
    with _engine(de):
        fx = _fx("SUBC.OL")
        for pages in ("Global_Markets", "global_markets", " GLOBAL_MARKETS ", "Market_Leaders_X, global_markets"):
            _set("enforce", pages)
            row = f7._prep(fx)
            golden = f7._golden(copy.deepcopy(row), 4, fx["row"])
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
    """C3: /health engine_gates.scoring_settle_pages and the [GUARDS] leg
    carry the canonical scope ("all" = every page, "none" = no valid name)."""
    for raw, want in ((None, "all"), ("", "all"), ("*", "all"), ("all", "all"),
                      ("Global_Markets", "Global_Markets"),
                      (" Global Markets , commodities-fx ", "Global_Markets,Commodities_FX"),
                      ("Global_Markets,global_markets", "Global_Markets")):
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
# FIX-1 / FIX-2 / FIX-3 gate cases: each FAILS on the pre-fix head (87e4e52).
# =============================================================================
def case_fix1_synthetic_and_provider_twins_score_confidence_alike(de):
    """FIX-1: an engine-made confidence is never a completeness field when the
    input had none - the settled quality of a synthetic row equals the
    no-confidence basis and its provider-target twin's (same data)."""
    with _engine(de):
        for symbol in ("BRK-B.US", "SKG.L"):
            syn_fx = _fx(symbol)
            prov_fx = _fx(symbol, forecast_price_12m=round(syn_fx["row"]["current_price"] * 1.1, 4),
                          forecast_source="provider_target")
            syn_p1, syn, _ = _settle(de, syn_fx, "enforce")
            prov_p1, prov, _ = _settle(de, prov_fx, "enforce")
            assert syn["forecast_source"] == "phase_ii_synthetic" and prov["forecast_source"] == "provider_target"
            assert syn["quality_score"] == prov["quality_score"], (symbol, syn["quality_score"], prov["quality_score"])
            assert syn["quality_score"] == _scored_quality_without_confidence(syn), (symbol, syn["quality_score"])
            assert prov["quality_score"] == _scored_quality_without_confidence(prov), symbol
            assert syn_p1.get(SYNTH) is True and MARKER not in syn_p1, symbol
            assert prov_p1.get(MARKER) is True and SYNTH not in prov_p1, symbol
            assert not any(k in syn or k in prov for k in TRANSIENT)


def case_fix1_flag_reaches_scoring_only_on_synthetic_iterates(de):
    """FIX-1: Phase-II's confidence is KEPT on a synthetic iterate; the scoring
    flag is set around that iterate's re-score only - never on a provider
    iterate, never seen by Phase-II, never left on a result row."""
    with _engine(de):
        for fx in f7.FIXTURES:
            _set("enforce")
            row = f7._prep(fx)
            scored, enhanced = [], []
            real_score, real_dd = de._compute_scores_canonical_first, de._apply_phase_dd_enhancements

            def score_spy(r, _real=real_score, _seen=scored):
                _seen.append((r.get(CONF_NOT_DATA), r.get("forecast_confidence")))
                return _real(r)

            def dd_spy(r, _real=real_dd, _seen=enhanced):
                _seen.append(r.get(CONF_NOT_DATA))
                return _real(r)

            with patch.object(de, "_compute_scores_canonical_first", score_spy), \
                    patch.object(de, "_apply_phase_dd_enhancements", dd_spy):
                result = de._f7_settle_result(row)
            assert result.status == "stable" and scored and len(scored) == len(enhanced)
            if fx["cohort"] == "synth":
                assert all(flag is True and conf is not None for flag, conf in scored), (fx["row"]["symbol"], scored)
            else:
                assert all(flag is None and conf is None for flag, conf in scored), (fx["row"]["symbol"], scored)
            assert not any(enhanced) and not any(k in result.row for k in TRANSIENT)


def case_fix2_page_names_canonical(de):
    """FIX-2: scope names are canonicalized ("Global Markets", "global-markets",
    "GLOBAL_MARKETS" all mean Global_Markets); unknown names are ignored with
    ONE warning; a list of only unknown names scopes enforce to no page."""
    with _engine(de):
        fx = _fx("SUBC.OL")
        for pages in ("Global Markets", "global-markets", "GLOBAL_MARKETS", " Global Markets , Glob_Mkts "):
            _, gm, gm_row = _settle(de, fx, "enforce", page=GM, pages=pages)
            assert gm is not gm_row and _one_tag(gm).startswith("f7_settle:enforce:"), pages
            _, ml, ml_row = _settle(de, fx, "enforce", page="Market Leaders", pages=pages)
            assert ml is ml_row and _one_tag(ml).startswith("f7_settle:observe:"), pages
            assert de.surface_gate_states()["scoring_settle_pages"] == GM, pages
        de._F7_SETTLE_PAGES_WARNED.clear()
        with patch.object(de.logger, "warning") as warn:
            for _ in range(3):
                _set("enforce", "Glob_Mkts, Market Leaderz")
                assert de._f7_settle_pages() == () and de._f7_settle_pages_label() == "none"
        assert warn.call_count == 1 and "Glob_Mkts, Market Leaderz" in str(warn.call_args)
        _, gm, gm_row = _settle(de, fx, "enforce", page=GM, pages="Glob_Mkts")
        assert gm is gm_row and _one_tag(gm).startswith("f7_settle:observe:")


def case_fix3_scope_follows_the_symbol_market(de):
    """FIX-3: with scope Global_Markets, a US symbol on My_Portfolio is
    settled, a .SR symbol on My_Portfolio and a fund on Mutual_Funds stay
    observe - through the REAL factory call site, and via _f7_scope_page."""
    eng = de.DataEngineV5(settings=SimpleNamespace(), providers=["yahoo_chart"])
    assert [eng._f7_scope_page(s, p) for s, p in (
        ("BRK-B.US", PF), ("2222.SR", PF), ("SPY", MF), ("EURUSD=X", PF), ("BNY.US", "Top_10_Investments"),
        ("BRK-B.US", ML), ("2222.SR", GM))] == [GM, ML, MF, "Commodities_FX", GM, ML, GM]
    with _engine(de):
        brk = _fx("BRK-B.US")
        _, out, row = _settle(de, brk, "enforce", page=PF, pages=GM, scope_page=eng._f7_scope_page("BRK-B.US", PF))
        assert out is not row and _one_tag(out).startswith("f7_settle:enforce:")
        sr = _fx("BRK-B.US", symbol="2222.SR")
        _, out, row = _settle(de, sr, "enforce", page=PF, pages=GM, scope_page=eng._f7_scope_page("2222.SR", PF))
        assert out is row and _one_tag(out).startswith("f7_settle:observe:")
    _set("enforce", GM, **FACTORY_ENV)
    seen = {}
    for symbol, page in (("BRK-B.US", PF), ("2222.SR", PF), ("SPY", MF), ("BRK-B.US", GM)):
        published = asyncio.run(_factory_engine(de, brk["row"]).get_enriched_quote_dict(symbol, page))
        assert not any(k in published for k in TRANSIENT)
        seen[(symbol, page)] = _one_tag(published).split(":")[1]
    assert seen == {("BRK-B.US", PF): "enforce", ("2222.SR", PF): "observe",
                    ("SPY", MF): "observe", ("BRK-B.US", GM): "enforce"}, seen


FIX_CASES = (
    ("FIX-1 synthetic and provider twins score confidence alike", case_fix1_synthetic_and_provider_twins_score_confidence_alike),
    ("FIX-1 flag reaches scoring only on synthetic iterates", case_fix1_flag_reaches_scoring_only_on_synthetic_iterates),
    ("FIX-2 page names canonical", case_fix2_page_names_canonical),
    ("FIX-3 scope follows the symbol's market", case_fix3_scope_follows_the_symbol_market),
)


# =============================================================================
# Identity and hygiene cases (hold on the base engine as well).
# =============================================================================
def case_off_is_byte_identical(de, base=None):
    """off: no transient key anywhere, the row object returned untouched; with
    a base copy every row is byte-identical to the base engine's (minus the
    scorer clock; both engines share the current core.scoring, whose own
    flag-absent identity with 5.11.4 is proven separately)."""
    with _engine(de):
        for mode in (None, "off", "1", "true"):
            for fx in f7.FIXTURES:
                _set(mode)
                row = f7._prep(fx)
                before = copy.deepcopy(row)
                out = de._f7_settle_pass(row, row["symbol"], GM)
                assert out is row and row == before and _ordered(row) == _ordered(before)
                assert not any(k in row for k in TRANSIENT) and not f7._tags(row)
                if base is not None:
                    with _engine(base):
                        brow = f7._prep(fx)
                        bout = base._f7_settle_pass(brow, brow["symbol"], GM)
                    assert _ordered(out) == _ordered(bout), (mode, fx["row"]["symbol"])


def case_observe_publishes_pass1_values(de, base=None):
    """observe: the published row is the pass-1 row byte-for-byte (minus the
    transient keys) plus ONE tag; with a base copy it is byte-identical to the
    base engine's observe row apart from the tag text. (A row whose settle
    status changes would differ - none of the fixtures does.)"""
    with _engine(de):
        for fx in f7.FIXTURES:
            pass1, out, row = _settle(de, fx, "observe")
            assert out is row and not any(k in out for k in TRANSIENT)
            assert _ordered(out, strip_tag=True) == _ordered(_strip(pass1), strip_tag=True), fx["row"]["symbol"]
            assert len(f7._tags(out)) == 1 and not any(f in _one_tag(out).lower() for f in f7.FORBIDDEN)
            if base is not None:
                with _engine(base):
                    _, bout, _ = _settle(base, fx, "observe")
                assert _ordered(out, strip_tag=True) == _ordered(bout, strip_tag=True), fx["row"]["symbol"]


def case_input_confidence_rows_settle_as_before(de, base=None):
    """FIX-1 / C1 leave a confidence the INPUT carried alone: no transient key
    is created and the row settles on the v5.156.0 loop (synthetic rows keep
    counting Phase-II's confidence); with a base copy the published enforce
    row is byte-identical to base 5.156.0 enforce apart from the tag text."""
    with _engine(de):
        for symbol in ("BRK-B.US", "SKG.L", "SUBC.OL"):
            for extra in ({"forecast_confidence": 0.8}, {"confidence": 70.0}):
                fx = _fx(symbol, **extra)
                assert not f7._scorer_derives_confidence(fx["row"])
                _set("enforce")
                row = f7._prep(fx)
                assert not any(k in row for k in TRANSIENT), (symbol, extra)
                golden = f7._golden(copy.deepcopy(row), 4)
                assert f7._norm(golden) == f7._norm(f7._golden(copy.deepcopy(row), 4, fx["row"]))
                out = de._f7_settle_pass(row, row["symbol"], GM)
                assert f7._norm(out) == f7._norm(golden), (symbol, extra)
                if base is not None:
                    with _engine(base):
                        _, bout, _ = _settle(base, fx, "enforce")
                    assert _ordered(out, strip_tag=True) == _ordered(bout, strip_tag=True), (symbol, extra)


def case_marker_never_published(de):
    """No transient key leaves _f7_settle_pass: settled (st3/st4) in observe
    and enforce, stable at pass 2, error, pass_limit / oscillation under every
    TFB_SETTLE_UNRESOLVED mode, a failure receipt, off after a mode flip, the
    test-only delegator, and the real factory."""
    with _engine(de):
        for mode in ("observe", "enforce"):
            for fx in f7.FIXTURES:
                pass1, out, row = _settle(de, fx, mode)
                assert not any(k in out or k in row for k in TRANSIENT), (mode, fx["row"]["symbol"])
            for symbol, key in (("SUBC.OL", MARKER), ("BRK-B.US", SYNTH)):
                _set(mode)
                row = f7._prep(_fx(symbol))
                result = de._f7_settle_result(row)
                assert result.status == "stable" and result.settled_at in (3, 4)
                assert row.get(key) is True and not any(k in result.row for k in TRANSIENT)  # caller's row untouched
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
        assert out is again and not any(k in out for k in TRANSIENT)

        def boom(r):
            raise RuntimeError("settle boom")

        def drift(r):
            r["overall_score"] += 1.0

        def oscillate(r):
            r["overall_score"] = 20.0 if r["overall_score"] == 80.0 else 80.0

        for mode in ("observe", "enforce"):
            for unres in (None, "observe", "enforce"):
                for scorer, passes in ((boom, 4), (drift, 2), (oscillate, 4)):
                    for symbol, key in (("BNY.US", MARKER), ("BRK-B.US", SYNTH)):
                        _set(mode, max_passes=passes, TFB_SETTLE_UNRESOLVED=unres)
                        row = f7._prep(_fx(symbol))
                        if scorer is oscillate:
                            row["overall_score"] = 80.0
                        assert row.get(key) is True
                        with patch.object(de, "_compute_scores_canonical_first", scorer), \
                                patch.object(de, "_apply_phase_dd_enhancements", lambda r: r):
                            out = de._f7_settle_pass(row, row["symbol"], GM)
                        assert not any(k in out for k in TRANSIENT), (mode, unres, scorer.__name__, symbol)
            _set(mode)
            row = f7._prep(_fx("BRK-B.US"))
            row["warnings"] = (row.get("warnings") or "") + "; f7_settle_failed:error"
            out = de._f7_settle_pass(row, row["symbol"], GM)
            assert de._f7_settle_failed(out) and not any(k in out for k in TRANSIENT)
        for symbol in ("FCN.US", "SKG.L"):
            _set("observe")
            row = f7._prep(_fx(symbol))
            assert any(k in row for k in TRANSIENT)
            before = _strip(copy.deepcopy(row))
            _set("off")
            out = de._f7_settle_pass(row, row["symbol"], GM)
            assert out is row and not any(k in row for k in TRANSIENT) and _ordered(row) == _ordered(before)
        # the one scoring entry point no settle pass follows (tests only)
        _set("enforce")
        row = de._apply_phase_bb_sanity(dict(_fx("BNY.US")["row"]))
        de._compute_scores_fallback(row)
        assert not any(k in row for k in TRANSIENT) and row.get("forecast_confidence") is not None


def evidence(de):
    """Canonical summary of what the suite measures (tags, settled decision
    fields, twins, page scope) - its digest is printed by the runner and must
    be identical run to run."""
    with _engine(de):
        out = {"version": de.__version__, "scoring": _SCORING.__version__, "fixtures": {}, "twins": {},
               "synthetic_twins": {}, "scope": {}}
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
                               "rel": _project(de, settled)["forecast_reliability_score"]}
            out["twins"]["%s x%s" % (symbol, mult or "lkg")] = pair
        for key in ("TFB_RELIABILITY_RECALIBRATION", "TFB_INVESTABILITY_GATE"):
            os.environ.pop(key, None)
        for symbol in ("BRK-B.US", "SKG.L"):
            syn_fx = _fx(symbol)
            prov_fx = _fx(symbol, forecast_price_12m=round(syn_fx["row"]["current_price"] * 1.1, 4),
                          forecast_source="provider_target")
            out["synthetic_twins"][symbol] = [_settle(de, f, "enforce")[1].get("quality_score") for f in (syn_fx, prov_fx)]
        eng = de.DataEngineV5(settings=SimpleNamespace(), providers=["yahoo_chart"])
        for symbol, page in (("SUBC.OL", GM), ("SUBC.OL", ML), ("BRK-B.US", PF), ("2222.SR", PF)):
            fx = _fx("SUBC.OL") if symbol == "SUBC.OL" else _fx("BRK-B.US", symbol=symbol)
            _, scoped, row = _settle(de, fx, "enforce", page=page, pages=GM, scope_page=eng._f7_scope_page(symbol, page))
            out["scope"]["%s@%s" % (symbol, page)] = {"settled": scoped is not row, "tag": _one_tag(scoped)}
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


test_c1_cold_twin_settles_to_warm_confidence_and_reliability = _wrap(case_c1_cold_twin_settles_to_warm_confidence_and_reliability)
test_c1_g1_evidence_soft_capped_row = _wrap(case_c1_g1_evidence_soft_capped_row)
test_c1_marker_provenance = _wrap(case_c1_marker_provenance)
test_c2_tag_discloses_final_values_enforce_publishes = _wrap(case_c2_tag_discloses_final_values_enforce_publishes)
test_c2_tag_order_limit_and_degrade = _wrap(case_c2_tag_order_limit_and_degrade)
test_c3_enforce_publishes_only_on_scoped_pages = _wrap(case_c3_enforce_publishes_only_on_scoped_pages)
test_c3_scope_disclosed_in_health_and_boot_line = _wrap(case_c3_scope_disclosed_in_health_and_boot_line)
test_fix1_synthetic_and_provider_twins_score_confidence_alike = _wrap(case_fix1_synthetic_and_provider_twins_score_confidence_alike)
test_fix1_flag_reaches_scoring_only_on_synthetic_iterates = _wrap(case_fix1_flag_reaches_scoring_only_on_synthetic_iterates)
test_fix2_page_names_canonical = _wrap(case_fix2_page_names_canonical)
test_fix3_scope_follows_the_symbol_market = _wrap(case_fix3_scope_follows_the_symbol_market)
test_off_is_byte_identical = _wrap(case_off_is_byte_identical)
test_observe_publishes_pass1_values = _wrap(case_observe_publishes_pass1_values)
test_input_confidence_rows_settle_as_before = _wrap(case_input_confidence_rows_settle_as_before)
test_marker_never_published = _wrap(case_marker_never_published)


@_isolated
def test_fix4_golden_decides_from_input_not_engine_marker():
    """FIX-4: the f7 golden reads the fixture INPUT. An engine that never marks
    (mutant) is caught by it; a golden keyed on the engine's own marker (the
    pre-fix rule) agrees with the mutant and would hide the defect."""
    for fx in f7.FIXTURES:
        assert f7._scorer_derives_confidence(fx["row"]) is True
    for extra in ({"forecast_confidence": 0.8}, {"confidence": " 70 "}, {"ai_confidence": 0.7}):
        assert f7._scorer_derives_confidence(dict(f7.FIXTURES[0]["row"], **extra)) is False
    assert f7._scorer_derives_confidence(dict(f7.FIXTURES[0]["row"], forecast_confidence=" ")) is True

    def marker_keyed_golden(row, max_passes):
        prev, last, first = row, row, None
        for k in range(2, max_passes + 1):
            cand = copy.deepcopy(prev)
            if cand.get(MARKER):
                for key in CONF_KEYS:
                    if key in cand:
                        cand[key] = None
            f7._pair(cand)
            d = f7._differs(prev, cand)
            first = d if first is None else first
            if not d:
                last = cand if k > 2 else last
                break
            last = prev = cand
        return row if not first else last

    with _engine(DE):
        for symbol in ("SUBC.OL", "BRK-B.US"):
            fx = _fx(symbol)
            _set("enforce")
            with patch.object(DE, "_f7_conf_unset", lambda row: False):
                row = f7._prep(fx)
                assert not any(k in row for k in TRANSIENT)
                by_input = f7._golden(copy.deepcopy(row), 4, fx["row"])
                by_marker = marker_keyed_golden(copy.deepcopy(row), 4)
                out = DE._f7_settle_pass(row, row["symbol"], GM)
            assert f7._norm(out) != f7._norm(by_input), symbol      # the mutant is caught
            assert f7._norm(out) == f7._norm(by_marker), symbol     # a marker-keyed golden is blind


def test_scoring_flag_absent_property_and_single_reader():
    """core.scoring 5.11.5: _completeness_factor equals the 5.11.4 formula
    whenever the flag is absent or falsy, counts forecast_confidence as absent
    (same 22-field denominator) when it is truthy, and nothing else reads it."""
    def reference(row):
        return sum(1 for k in CORE_FIELDS if row.get(k) not in (None, "", [], {})) / len(CORE_FIELDS)

    rng = random.Random(20261022)
    values = [None, "", [], {}, 0, 0.0, -1.5, 1.5, "x", "0", " ", False, True, [1], {"a": 1}, float("nan")]
    for _ in range(3000):
        row = {k: rng.choice(values) for k in rng.sample(CORE_FIELDS, rng.randint(0, len(CORE_FIELDS)))}
        for extra in rng.sample(["warnings", MARKER, SYNTH, "overall_score"], rng.randint(0, 2)):
            row[extra] = rng.choice(values)
        want = reference(row)
        assert _SCORING._completeness_factor(row) == want
        for falsy in (None, False, 0, "", [], {}):
            assert _SCORING._completeness_factor(dict(row, **{CONF_NOT_DATA: falsy})) == want
        no_conf = dict(row)
        no_conf.pop("forecast_confidence", None)
        for truthy in (True, 1, "1", "yes"):
            assert _SCORING._completeness_factor(dict(row, **{CONF_NOT_DATA: truthy})) == reference(no_conf)
    assert _SCORING.__version__ == SCORING_VERSION and _SCORING._CONF_NOT_DATA_FLAG == CONF_NOT_DATA
    assert DE._F7_SCORING_CONF_NOT_DATA == _SCORING._CONF_NOT_DATA_FLAG
    tree = ast.parse(inspect.getsource(_SCORING))
    readers, literals = set(), 0
    for fn in ast.walk(tree):
        if isinstance(fn, (ast.FunctionDef, ast.AsyncFunctionDef)):
            if any(isinstance(n, ast.Name) and n.id == "_CONF_NOT_DATA_FLAG" for n in ast.walk(fn)):
                readers.add(fn.name)
    for node in ast.walk(tree):
        if isinstance(node, ast.Constant) and node.value == CONF_NOT_DATA:
            literals += 1
    assert readers == {"_completeness_factor"} and literals == 1, (readers, literals)


@_isolated
def test_scoring_flag_absent_byte_identical_to_5_11_4():
    """core.scoring 5.11.5 vs 5.11.4 (base copy) on the real fixtures - input,
    pass-1, settled and confidence-blank rows: every output key is identical
    except this module's own version labels; with the flag the only moved
    outputs are the completeness consumers."""
    try:
        base_scoring = _base_scoring()
    except _Skip as exc:
        return _skip_or_raise(exc)
    assert base_scoring.__version__ == "5.11.4"
    rows = []
    with _engine(DE):
        for fx in f7.FIXTURES:
            rows.append(DE._apply_phase_bb_sanity(copy.deepcopy(fx["row"])))
            for mode in ("observe", "enforce"):
                pass1, out, _ = _settle(DE, fx, mode)
                rows.extend([_strip(pass1), _strip(out), dict(_strip(out), **{k: None for k in CONF_KEYS})])
    for row in rows:
        new, old = _SCORING.compute_scores(copy.deepcopy(row)), base_scoring.compute_scores(copy.deepcopy(row))
        moved = sorted(k for k in set(new) | set(old) if k not in f7.VOLATILE and new.get(k) != old.get(k))
        assert moved == sorted(LABELS), (row.get("symbol"), moved)
        assert (old["scoring_schema_version"], new["scoring_schema_version"]) == ("5.11.4", SCORING_VERSION)
        assert (old["recommendation_source"], new["recommendation_source"]) == ("scoring.py v5.11.4", "scoring.py v5.11.5")
        for fn in ("_completeness_factor", "compute_quality_score", "compute_confidence_score"):
            assert getattr(_SCORING, fn)(row) == getattr(base_scoring, fn)(row), (fn, row.get("symbol"))


@_isolated
def test_base_off_observe_and_input_confidence_rows_byte_identical():
    try:
        base = _base()
    except _Skip as exc:
        return _skip_or_raise(exc)
    assert base.__version__ == "5.156.0", base.__version__
    case_off_is_byte_identical(DE, base)
    case_observe_publishes_pass1_values(DE, base)
    case_input_confidence_rows_settle_as_before(DE, base)


def _golden_negative(load, cases, label):
    try:
        reference = load()
    except _Skip as exc:
        return _skip_or_raise(exc)
    passed, failures = [], []
    for name, case in cases:
        try:
            _isolated(case)(reference)
        except Exception as exc:  # noqa: BLE001 - any failure on the reference is the point
            failures.append("%s: %s" % (name, type(exc).__name__))
        else:
            passed.append(name)
    assert not passed, "gate cases did not fail on the %s copy: %s" % (label, passed)
    assert len(failures) == len(cases)


def test_golden_negative_gate_cases_fail_on_base():
    _golden_negative(_base, GATE_CASES, BASE_COMMIT)


def test_golden_negative_fix_cases_fail_on_prev_head():
    _golden_negative(_prev, FIX_CASES, PREV_COMMIT)


@_isolated
def test_evidence_is_deterministic():
    first = evidence(DE)
    assert evidence(DE) == first
    assert first["scope"]["SUBC.OL@%s" % GM]["settled"] and not first["scope"]["SUBC.OL@%s" % ML]["settled"]
    assert first["scope"]["BRK-B.US@%s" % PF]["settled"] and not first["scope"]["2222.SR@%s" % PF]["settled"]


def test_wiring_and_version():
    src = inspect.getsource(DE)
    assert DE.__version__ == VERSION
    assert "Data Engine V2 - GLOBAL-FIRST ORCHESTRATOR - v" + DE.__version__ in src
    assert "WHY v5.157.0 - F-7 v2" in src
    # the one call site (FIX-3: market scope page) and the v5.147.0 / v5.153.0 contracts
    assert src.count("merged = _f7_settle_pass(merged, sym, page_ctx, scope_page=self._f7_scope_page(sym, page_ctx))") == 1
    calls = [n for n in ast.walk(ast.parse(src)) if isinstance(n, ast.Call)
             and isinstance(n.func, ast.Name) and n.func.id == "_f7_settle_pass"]
    assert len(calls) == 1 and [kw.arg for kw in calls[0].keywords] == ["scope_page"]
    assert src.count('"scoring_settle": _f7_settle_mode(),') == 1 and src.count("scoring_settle=%s") == 1
    assert src.count("settle_unresolved=%s") == 1
    assert DE._F7_SETTLE_TAG_ORDER == ORDER and DE._F7_SETTLE_TAG_MAX == 8
    assert DE._F7_CONF_DERIVED_KEY == MARKER and DE._F7_CONF_SYNTH_KEY == SYNTH
    assert DE._F7_CONF_TRANSIENT_KEYS == TRANSIENT
    assert DE._F7_MARKET_PAGES == (GM, ML, "Commodities_FX", MF)
    assert [code for _k, code, _kind, _tol in DE._F7_SETTLE_FIELDS] == ["ov", "op", "va", "fcf", "r12", "rc", "os", "fs"]
    assert "final_diff" in DE.F7SettlementResult.__dataclass_fields__


TESTS = [test_c1_cold_twin_settles_to_warm_confidence_and_reliability, test_c1_g1_evidence_soft_capped_row,
         test_c1_marker_provenance, test_c2_tag_discloses_final_values_enforce_publishes,
         test_c2_tag_order_limit_and_degrade, test_c3_enforce_publishes_only_on_scoped_pages,
         test_c3_scope_disclosed_in_health_and_boot_line,
         test_fix1_synthetic_and_provider_twins_score_confidence_alike,
         test_fix1_flag_reaches_scoring_only_on_synthetic_iterates, test_fix2_page_names_canonical,
         test_fix3_scope_follows_the_symbol_market, test_fix4_golden_decides_from_input_not_engine_marker,
         test_off_is_byte_identical, test_observe_publishes_pass1_values,
         test_input_confidence_rows_settle_as_before, test_marker_never_published,
         test_scoring_flag_absent_property_and_single_reader, test_scoring_flag_absent_byte_identical_to_5_11_4,
         test_base_off_observe_and_input_confidence_rows_byte_identical,
         test_golden_negative_gate_cases_fail_on_base, test_golden_negative_fix_cases_fail_on_prev_head,
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
    for env_path, load, cases in ((_BASE_PATH, _base, GATE_CASES), (_PREV_PATH, _prev, FIX_CASES)):
        if env_path:
            reference = load()
            for name, case in cases:
                try:
                    _isolated(case)(reference)
                    print("REF-PASS", reference.__version__, name)
                except Exception as exc:  # noqa: BLE001
                    print("REF-FAIL", reference.__name__, name, "|", type(exc).__name__,
                          str(exc).splitlines()[0][:160] if str(exc) else "")
    print("EVIDENCE", _digest(_isolated(evidence)(DE)))
    ran = len(TESTS) - skips
    print("RESULT", "PASS" if not fails else "FAIL", "%d/%d" % (ran - fails, ran),
          "(%d skipped)" % skips if skips else "", "engine", DE.__version__, "scoring", _SCORING.__version__)
    sys.exit(1 if fails else 0)
