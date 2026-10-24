"""F-7 v2 (data_engine_v2 v5.157.0) - final-pass tag and enforce scope by the
symbol's inferred market. core.scoring is unchanged (5.11.4).

Gaps found on the 21/22 Oct exports (see WHY v5.157.0 in core/data_engine_v2.py):

  G1 core.scoring reuses an existing confidence (cold pass 1 0.6323 vs warm
     0.6641). DEFERRED: re-deriving it on the settle passes changes the settle
     trajectory and with it which rows observe holds back; a dual-trajectory
     observe design is planned. This version keeps the v5.156.0 trajectory.
  G2 the observe tag showed pass 1 -> pass 2 (not what enforce publishes for
     rows settling at pass 3/4) and at most 6 fields (os= could be cut).
  G3 enforce must be scopable (Market_Leaders churns more under settlement).

  [F7-FINAL-TAG]  the tag discloses pass 1 -> FINAL, up to 8 fields in the
      order ov op os rc va fcf r12 fs. Which rows carry a tag is unchanged.
  [F7-PAGE-SCOPE] TFB_SCORING_SETTLE_PAGES lists MARKET pages (canonicalized:
      "Global Markets" == global-markets == Global_Markets); any other name,
      My_Portfolio and Top_10_Investments included, is ignored with one
      warning; only ignored names -> "none". A row's scope page is its symbol's
      INFERRED market page whatever page requested it
      (DataEngineV5._f7_scope_page): funds and crypto pairs without an ETF hint
      (SPLG, SUI-USD) count as Global_Markets. Out of scope, enforce == observe.

The settle trajectory is v5.156.0's: status, pass counts, held-back rows and
every published value are identical; only the f7_settle tag's field
disclosures differ. enforce == base enforce + scope.
v5.158.0 (F-7 v2b): enforce publishes the v2 trajectory (the scorer's own
confidence re-derived; tests/test_de_f7_settle_shadow.py), observe still
publishes the v1 trajectory above and takes only its tag text from the v2
shadow - so the base comparisons below cover observe, scoped observe and
out-of-scope enforce with every f7_settle tag removed, and the v1 trajectory.

REAL engine module, the six real 2026-09-22 Global_Markets fixtures and the
helpers of tests/test_de_f7_scoring_settle.py (FIXTURES, _prep, _pair, _golden,
_norm, _tags, _env); the scope cases also drive the REAL orchestrator
(get_enriched_quote_dict) offline. Every test clears all TFB_* variables and
restores the environment and the target stores afterwards.

Optional environment (read at import, never by the engine):
  TFB_F7V2_BASE=<dir | .py>   a source copy of base df99bcf3 (engine 5.156.0,
      scoring 5.11.4), e.g.  mkdir -p <dir> && git archive df99bcf3 | tar -x -C <dir>
      enables the identity comparisons against base (off byte for byte;
      observe / enforce / scoped rows identical except the tag text; settle
      trajectory incl. held-back rows) and the golden negative: every C2 / C3
      gate case FAILS on the base engine.
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
import datetime as _dt
import functools
import hashlib
import importlib.util
import inspect
import json
import os
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

VERSION = (5, 157, 0)
BASE_COMMIT = "df99bcf3"
ENGINE_ENV = "TFB_F7V2_ENGINE"
BASE_ENV = "TFB_F7V2_BASE"
PAGES_ENV = "TFB_SCORING_SETTLE_PAGES"
GM, ML, CFX, MF = "Global_Markets", "Market_Leaders", "Commodities_FX", "Mutual_Funds"
PF, T10 = "My_Portfolio", "Top_10_Investments"
MARKETS = (GM, ML, CFX, MF)
REALISTIC = "Global_Markets,Commodities_FX,Mutual_Funds"
# The five symbols the scope contract is shown on: (symbol, its market page in
# the workbook, the market page it is inferred to).
SURFACE_SYMBOLS = (("SPLG", MF, GM), ("SUI-USD", CFX, GM), ("SPY", MF, MF),
                   ("2222.SR", ML, ML), ("BRK-B.US", GM, GM))
# Priority order of the tag disclosures and the field behind each code; the
# kinds and tolerances are f7.FIELDS (the engine's _F7_SETTLE_FIELDS).
ORDER = ("ov", "op", "os", "rc", "va", "fcf", "r12", "fs")
CODE_KEY = {"ov": "overall_score", "op": "opportunity_score", "os": "opportunity_source",
            "rc": "recommendation", "va": "valuation_score", "fcf": "forecast_confidence",
            "r12": "expected_roi_12m", "fs": "forecast_source"}
FIELD = {key: (kind, tol) for key, kind, tol in f7.FIELDS}
TAG_PARTS_RE = re.compile(r"^f7_settle:(observe|enforce):st(\d+|x):p(\d+)((?::[a-z0-9]+=[^:;]*>[^:;]*)*)$")

_ENGINE_PATH = os.environ.get(ENGINE_ENV, "").strip()
_BASE_PATH = os.environ.get(BASE_ENV, "").strip()
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


def _base():
    if not _BASE_PATH:
        raise _Skip("%s unset" % BASE_ENV)
    return _load_module(_BASE_PATH, "tfb_f7v2_base_engine")


def _skip_or_raise(exc):
    # pytest.skip only inside a pytest run; the __main__ runner reports SKIP.
    if pytest is not None and os.environ.get("PYTEST_CURRENT_TEST"):
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


class _FrozenDateTime(_dt.datetime):
    """core.scoring's clock, frozen so whole rows compare byte for byte."""

    @classmethod
    def now(cls, tz=None):
        at = _dt.datetime(2026, 10, 22, 9, 30, tzinfo=_dt.timezone.utc)
        return at.astimezone(tz) if tz is not None else at.replace(tzinfo=None)


def _isolated(fn):
    """Clear every TFB_* variable, freeze the scorer clock (both engines share
    core.scoring), restore the exact environment and every engine's
    keep-last-good target store afterwards."""
    @functools.wraps(fn)
    def wrapper(*args, **kwargs):
        saved = dict(os.environ)
        engines = {id(m): m for m in (_REAL_DE, DE, *_LOADED.values()) if hasattr(m, "_TGT_LKG_STORE")}
        stores = {key: dict(m._TGT_LKG_STORE) for key, m in engines.items()}
        try:
            for key in list(os.environ):
                if key.startswith("TFB_"):
                    del os.environ[key]
            with patch.object(_SCORING, "datetime", _FrozenDateTime):
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
    """A real f7 fixture, optionally with changed input fields."""
    fx = next(fx for fx in f7.FIXTURES if fx["row"]["symbol"] == fixture_symbol)
    return dict(fx, row=dict(fx["row"], **row_changes)) if row_changes else fx


def _bytes(row):
    """The whole row, byte for byte: ordered (key, value) pairs, nothing dropped
    (the scorer clock is frozen by _isolated)."""
    return json.dumps(list(row.items()), default=repr)


def _prefix(tag):
    return ":".join(tag.split(":")[:4])


def _tagless(row):
    """_bytes with each f7_settle tag cut to its prefix (mode:st:p): every
    published field must match base except the tag's field disclosures."""
    out = []
    for key, value in row.items():
        if key == "warnings" and isinstance(value, str):
            value = "; ".join(_prefix(p.strip()) if p.strip().startswith("f7_settle:") else p.strip()
                              for p in value.split(";") if p.strip())
        out.append((key, value))
    return json.dumps(out, default=repr)


def _untagged(row):
    """_bytes with every f7_settle tag removed (and a warnings cell that held
    only tags dropped): v5.158.0 observe takes tag text and presence from the
    v2 shadow, every other published byte from the v1 (base) result."""
    out = []
    for key, value in row.items():
        if key == "warnings" and isinstance(value, str):
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


def _settle(de, fx, mode, page=GM, pages=None, scope_page=None, max_passes=4, **extra):
    """(pass-1 snapshot, published row, pass-1 row object) for one fixture."""
    _set(mode, pages, max_passes, **extra)
    row = f7._prep(fx)
    pass1 = copy.deepcopy(row)
    if scope_page is None:
        out = de._f7_settle_pass(row, row["symbol"], page)
    else:
        out = de._f7_settle_pass(row, row["symbol"], page, scope_page=scope_page)
    return pass1, out, row


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


def _fetch(de, symbol, page, mode, pages=None):
    """One published row from the real orchestrator (facts: BRK-B.US fixture,
    a synthetic row that settles at pass 4)."""
    _set(mode, pages, **FACTORY_ENV)
    return asyncio.run(_factory_engine(de, _fx("BRK-B.US")["row"]).get_enriched_quote_dict(symbol, page))


def _fetched(row):
    """A fetched row minus its acquisition clock (acquisition_acquired_at)."""
    out = []
    for key, value in row.items():
        if key == "warnings" and isinstance(value, str):
            value = "; ".join(p.strip() for p in value.split(";")
                              if p.strip() and not p.strip().startswith("acquisition_acquired_at:"))
        out.append((key, value))
    return json.dumps(out, default=repr)


# =============================================================================
# C2 / C3 gate cases: each takes an engine module; each FAILS on base.
# =============================================================================
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


def case_c3_scope_lists_market_pages_only(de):
    """C3: TFB_SCORING_SETTLE_PAGES entries are canonicalized and only the four
    market pages are valid; any other name (My_Portfolio, Top_10_Investments,
    another sheet, a typo) is ignored with ONE warning per distinct set; only
    ignored names -> "none". Disclosed in /health engine_gates and [GUARDS]."""
    for raw, want in ((None, "all"), ("", "all"), (" , ", "all"), ("*", "all"), ("all", "all"), ("ALL", "all"),
                      ("all, My_Portfolio", "all"),
                      ("Global_Markets", GM), ("global-markets", GM), ("GLOBAL_MARKETS", GM),
                      (" Global Markets , commodities-fx ", "Global_Markets,Commodities_FX"),
                      ("Global_Markets,global_markets", GM), ("market leaders", ML), ("mutual-funds", MF),
                      (REALISTIC, REALISTIC), ("My_Portfolio, Global Markets", GM),
                      ("Top_10_Investments,Market_Leaders,My_Portfolio", ML),
                      ("My_Portfolio", "none"), ("Top_10_Investments", "none"),
                      ("My_Portfolio,Top_10_Investments,Insights_Analysis,Data_Dictionary,My_Investments", "none"),
                      ("Glob_Mkts", "none")):
        _set("enforce", raw)
        assert de.surface_gate_states()["scoring_settle_pages"] == want, raw
        assert de._f7_settle_pages_label() == want, raw
    de._F7_SETTLE_PAGES_WARNED.clear()
    with patch.object(de.logger, "warning") as warn:
        for _ in range(3):
            _set("enforce", "My_Portfolio, Top_10_Investments")
            assert de._f7_settle_pages() == () and de._f7_settle_pages_label() == "none"
        assert warn.call_count == 1 and "My_Portfolio, Top_10_Investments" in str(warn.call_args)
        _set("enforce", "Glob_Mkts, Global_Markets")
        assert de._f7_settle_pages() == (GM,)
        assert warn.call_count == 2 and "Glob_Mkts" in str(warn.call_args)
    src = inspect.getsource(de)
    assert src.count("scoring_settle_pages=%s") == 1
    assert src.count('"scoring_settle_pages": _f7_settle_pages_label(),') == 1
    assert src.count("_f7_settle_pages_label(),   # v5.157.0") == 1


def case_c3_enforce_is_observe_out_of_scope(de):
    """C3: under enforce, a row whose scope page is listed publishes the
    settled row (== the independent golden); any other scope page - another
    market, "" or a list of only ignored names - publishes exactly what
    observe publishes. Non-stable results stay fail-closed everywhere."""
    with _engine(de):
        for symbol in ("SUBC.OL", "BRK-B.US"):
            fx = _fx(symbol)
            _, observed, obs_row = _settle(de, fx, "observe")
            assert observed is obs_row
            for pages in (GM, "global markets", REALISTIC, "My_Portfolio, Global_Markets"):
                _set("enforce", pages)
                row = f7._prep(fx)
                golden = f7._golden(copy.deepcopy(row), 4, fx["row"])   # v5.158.0: enforce = v2
                out = de._f7_settle_pass(row, row["symbol"], PF, scope_page=GM)
                assert out is not row and f7._norm(out) == f7._norm(golden), (symbol, pages)
                assert _one_tag(out) == _one_tag(observed).replace(":observe:", ":enforce:", 1)
                for scope_page in (ML, "", PF):
                    pass1, scoped, row = _settle(de, fx, "enforce", page=GM, pages=pages, scope_page=scope_page)
                    assert scoped is row, (symbol, pages, scope_page)
                    assert _bytes(scoped) == _bytes(observed), (symbol, pages, scope_page)
            for pages in ("My_Portfolio", "Top_10_Investments, Glob_Mkts"):
                _, scoped, row = _settle(de, fx, "enforce", pages=pages, scope_page=GM)
                assert scoped is row and _bytes(scoped) == _bytes(observed), (symbol, pages)
            for pages in (None, "", "*", "all"):
                for scope_page in (GM, ML, ""):
                    _, out, row = _settle(de, fx, "enforce", pages=pages, scope_page=scope_page)
                    assert out is not row and _one_tag(out).startswith("f7_settle:enforce:"), (pages, scope_page)
            # scope_page omitted (direct callers): the page decides
            _, out, row = _settle(de, fx, "enforce", page=GM, pages=GM)
            assert out is not row
            _, out, row = _settle(de, fx, "enforce", page=ML, pages=GM)
            assert out is row and _bytes(out) == _bytes(observed)
        # non-stable results are fail-closed in and out of scope (v5.158.0: the
        # in-scope receipt is the v2 result's, out of scope the v1 result's
        # plus the shadow tag; every other field is identical)
        outs = []
        for scope_page in (GM, ML):
            _set("enforce", GM, max_passes=2)
            row = f7._prep(_fx("SUBC.OL"))
            with patch.object(de, "_compute_scores_canonical_first", lambda r: r.update(overall_score=r["overall_score"] + 1.0)), \
                    patch.object(de, "_apply_phase_dd_enhancements", lambda r: r):
                outs.append(de._f7_settle_pass(row, row["symbol"], PF, scope_page=scope_page))
        assert de._f7_settle_failed(outs[0]) and de._f7_settle_failed(outs[1])
        assert [(k, v) for k, v in outs[0].items() if k not in ("warnings", "scoring_errors")] == \
               [(k, v) for k, v in outs[1].items() if k not in ("warnings", "scoring_errors")]


def case_c3_scope_is_the_inferred_market_on_every_surface(de):
    """C3: through the REAL orchestrator, SPLG, SUI-USD, SPY, a .SR and a US
    symbol are treated identically on their market page, My_Portfolio and a
    page-less fetch: the scope is the symbol's inferred market page, so SPLG
    (a fund without an ETF hint) and SUI-USD (a crypto pair) count as
    Global_Markets even on Mutual_Funds / Commodities_FX. In scope the row is
    the surface's enforce row, out of scope its observe row, byte for byte."""
    seen = {}
    for symbol, market, inferred in SURFACE_SYMBOLS:
        for page in (market, PF, ""):
            observed = _fetch(de, symbol, page, "observe")
            enforced = _fetch(de, symbol, page, "enforce")
            assert _one_tag(observed).startswith("f7_settle:observe:st4:p3:")
            assert _one_tag(enforced) == _one_tag(observed).replace(":observe:", ":enforce:", 1)
            assert observed["overall_score"] != enforced["overall_score"]
            for pages in (REALISTIC, "Commodities_FX,Mutual_Funds", "Market_Leaders", "My_Portfolio,Top_10_Investments"):
                scoped = _fetch(de, symbol, page, "enforce", pages)
                in_scope = inferred in pages.split(",")
                assert _fetched(scoped) == _fetched(enforced if in_scope else observed), (symbol, page, pages)
                seen.setdefault((symbol, pages), set()).add(_one_tag(scoped).split(":")[1])
    for (symbol, pages), modes in seen.items():
        assert len(modes) == 1, (symbol, pages, modes)          # one treatment on every surface
    assert {key: modes.pop() for key, modes in seen.items() if key[1] == REALISTIC} == {
        ("SPLG", REALISTIC): "enforce", ("SUI-USD", REALISTIC): "enforce", ("SPY", REALISTIC): "enforce",
        ("2222.SR", REALISTIC): "observe", ("BRK-B.US", REALISTIC): "enforce"}
    eng = de.DataEngineV5(settings=SimpleNamespace(), providers=["yahoo_chart"])
    table = {"SPLG": GM, "SUI-USD": GM, "BTC-USD": GM, "SPY": MF, "QQQ": MF, "EURUSD=X": CFX, "GC=F": CFX,
             "QG=F": CFX, "2222.SR": ML, "2222": ML, "4050.SR": ML, "BRK-B.US": GM, "VOD.L": GM, "AAPL": GM}
    assert {s: eng._f7_scope_page(s) for s in table} == table
    assert all(eng._f7_scope_page(s) == eng._resolve_quote_page_context(s, "")[0] for s in table)


GATE_CASES = (
    ("C2 tag discloses the final values enforce publishes", case_c2_tag_discloses_final_values_enforce_publishes),
    ("C2 tag order, limit and degrade", case_c2_tag_order_limit_and_degrade),
    ("C3 scope lists market pages only", case_c3_scope_lists_market_pages_only),
    ("C3 enforce is observe out of scope", case_c3_enforce_is_observe_out_of_scope),
    ("C3 scope is the inferred market on every surface", case_c3_scope_is_the_inferred_market_on_every_surface),
)


# =============================================================================
# Identity cases: the trajectory is v5.156.0's (hold on the base engine too).
# =============================================================================
def case_off_is_byte_identical(de, base=None):
    """off (unset, off, any non-mode word), with or without a scope: the row
    object comes back untouched; with a base copy every row is byte-identical
    to base, scorer labels and (frozen) timestamps included."""
    with _engine(de):
        for mode in (None, "off", "1", "true"):
            for pages in (None, GM, "none-such"):
                for fx in f7.FIXTURES:
                    _set(mode, pages)
                    row = f7._prep(fx)
                    before = _bytes(row)
                    out = de._f7_settle_pass(row, row["symbol"], GM, scope_page=ML)
                    assert out is row and _bytes(row) == before and not f7._tags(row)
                    if base is not None:
                        with _engine(base):
                            _set(mode, pages)
                            brow = f7._prep(fx)
                            bout = base._f7_settle_pass(brow, brow["symbol"], GM)
                        assert _bytes(out) == _bytes(bout), (mode, pages, fx["row"]["symbol"])


def case_published_rows_identical_except_tag_text(de, base=None):
    """observe publishes the pass-1 row plus ONE tag; enforce publishes the
    independent golden (v5.158.0: the v2 golden). With a base copy, at
    MAX_PASSES 2-5: observe (any scope) and enforce out of scope (== base
    observe) are byte-identical to base apart from the f7_settle tags
    (v5.158.0: their text and presence come from the v2 shadow)."""
    with _engine(de):
        for fx in f7.FIXTURES:
            pass1, out, row = _settle(de, fx, "observe")
            assert out is row and _bytes(f7._norm(out)) == _bytes(f7._norm(pass1)), fx["row"]["symbol"]
            assert len(f7._tags(out)) == 1 and not any(f in _one_tag(out).lower() for f in f7.FORBIDDEN)
            _set("enforce")
            row = f7._prep(fx)
            golden = f7._golden(copy.deepcopy(row), 4, fx["row"])
            assert f7._norm(de._f7_settle_pass(row, row["symbol"], GM)) == f7._norm(golden)
        if base is None:
            return
        for max_passes in (2, 3, 4, 5):
            for fx in f7.FIXTURES:
                with _engine(base):
                    _, b_obs, _ = _settle(base, fx, "observe", max_passes=max_passes)
                runs = {
                    "observe": (_settle(de, fx, "observe", max_passes=max_passes)[1], b_obs),
                    "observe in scope": (_settle(de, fx, "observe", pages=GM, scope_page=GM, max_passes=max_passes)[1], b_obs),
                    "observe out of scope": (_settle(de, fx, "observe", pages=ML, scope_page=GM, max_passes=max_passes)[1], b_obs),
                    "enforce out of scope": (_settle(de, fx, "enforce", pages=ML, scope_page=GM, max_passes=max_passes)[1], b_obs),
                }
                for label, (new, old) in runs.items():
                    assert _untagged(new) == _untagged(old), (label, max_passes, fx["row"]["symbol"])


def _trajectory_cases():
    """The six fixtures, target early / late at 4 multiples, price x RSI."""
    cases = []
    for fx in f7.FIXTURES:
        src = dict(fx["row"])
        for key in ("forecast_price_12m", "forecast_source", "target_mean_price"):
            src.pop(key, None)
        price = float(src["current_price"])
        for mult in (0.6, 1.1, 1.4, 2.0):
            fp12 = round(price * mult, 4)
            cases.append({"cohort": "early", "lkg": None,
                          "row": dict(src, forecast_price_12m=fp12, forecast_source="provider_target",
                                      target_mean_price=fp12)})
            cases.append({"cohort": "late", "lkg": {"fp12": fp12, "name": src.get("name")}, "row": dict(src)})
        for scale in (0.8, 1.0, 1.25):
            for rsi in (None, 25.0, 80.0):
                row = dict(src, current_price=round(price * scale, 4))
                if rsi is not None:
                    row["rsi_14"] = rsi
                cases.append({"cohort": "synth", "lkg": None, "row": row})
    return cases


def case_settle_trajectory_identical(de, base):
    """The settle trajectory is base's: on 102 fixture variants at MAX_PASSES
    2-5 the F7SettlementResult (status, settled_at, passes_run, reason,
    first/last diff, settled row) is identical, held-back rows included; the
    error / oscillation / pass_limit paths publish byte-identical rows under
    every TFB_SETTLE_UNRESOLVED mode. v5.158.0: this is the v1 trajectory
    (v2=False), which observe publishes; the failure paths are compared under
    observe with the f7_settle tags removed (enforce publishes v2)."""
    seen = set()
    for max_passes in (2, 3, 4, 5):
        for fx in _trajectory_cases():
            got = []
            for engine in (de, base):
                with _engine(engine):
                    _set("enforce", max_passes=max_passes)
                    res = (engine._f7_settle_result(f7._prep(fx), v2=False) if engine is de
                           else engine._f7_settle_result(f7._prep(fx)))
                got.append((res.status, res.settled_at, res.passes_run, res.reason, res.error_type,
                            res.first_diff, res.last_diff, _bytes(res.row) if res.row is not None else None))
            assert got[0] == got[1], (max_passes, fx["row"]["symbol"], got[0][:4], got[1][:4])
            seen.add((got[0][0], got[0][3]))
    assert {("stable", ""), ("non_converged", "pass_limit")} <= seen, seen

    def boom(r):
        raise RuntimeError("settle boom")

    def drift(r):
        r["overall_score"] += 1.0

    def oscillate(r):
        r["overall_score"] = 20.0 if r["overall_score"] == 80.0 else 80.0

    for mode in ("observe",):
        for unresolved in (None, "observe", "enforce"):
            for scorer, max_passes in ((boom, 4), (drift, 2), (oscillate, 4)):
                for symbol in ("BNY.US", "BRK-B.US"):
                    outs = []
                    for engine in (de, base):
                        with _engine(engine):
                            _set(mode, max_passes=max_passes, TFB_SETTLE_UNRESOLVED=unresolved)
                            row = f7._prep(_fx(symbol))
                            if scorer is oscillate:
                                row["overall_score"] = 80.0
                            with patch.object(engine, "_compute_scores_canonical_first", scorer), \
                                    patch.object(engine, "_apply_phase_dd_enhancements", lambda r: r):
                                outs.append(_untagged(engine._f7_settle_pass(row, row["symbol"], GM)))
                    assert outs[0] == outs[1], (mode, unresolved, scorer.__name__, symbol)


def evidence(de):
    """Canonical summary of what the suite measures (tags, settled decision
    fields, scope labels and decisions) - its digest is printed by the runner
    and must be identical run to run."""
    with _engine(de):
        out = {"version": de.__version__, "scoring": _SCORING.__version__, "fixtures": {}, "labels": {},
               "scope": {}}
        for fx in f7.FIXTURES:
            _, obs, _ = _settle(de, fx, "observe")
            _, enf, _ = _settle(de, fx, "enforce")
            out["fixtures"][fx["row"]["symbol"]] = {
                "observe_tag": _one_tag(obs), "enforce_tag": _one_tag(enf),
                "enforce": {key: enf.get(key) for key in CODE_KEY.values()}}
        for raw in (None, REALISTIC, "global markets,My_Portfolio", "My_Portfolio"):
            _set("enforce", raw)
            out["labels"][repr(raw)] = de._f7_settle_pages_label()
        eng = de.DataEngineV5(settings=SimpleNamespace(), providers=["yahoo_chart"])
        for symbol, market, _inferred in SURFACE_SYMBOLS:
            fx = _fx("BRK-B.US", symbol=symbol)
            _, scoped, row = _settle(de, fx, "enforce", page=market, pages=REALISTIC,
                                     scope_page=eng._f7_scope_page(symbol))
            out["scope"]["%s@%s" % (symbol, market)] = {"scope_page": eng._f7_scope_page(symbol),
                                                         "settled": scoped is not row, "tag": _one_tag(scoped)}
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


test_c2_tag_discloses_final_values_enforce_publishes = _wrap(case_c2_tag_discloses_final_values_enforce_publishes)
test_c2_tag_order_limit_and_degrade = _wrap(case_c2_tag_order_limit_and_degrade)
test_c3_scope_lists_market_pages_only = _wrap(case_c3_scope_lists_market_pages_only)
test_c3_enforce_is_observe_out_of_scope = _wrap(case_c3_enforce_is_observe_out_of_scope)
test_c3_scope_is_the_inferred_market_on_every_surface = _wrap(case_c3_scope_is_the_inferred_market_on_every_surface)
test_off_is_byte_identical = _wrap(case_off_is_byte_identical)
test_published_rows_identical_except_tag_text = _wrap(case_published_rows_identical_except_tag_text)


@_isolated
def test_base_off_published_rows_and_trajectory_identical():
    try:
        base = _base()
    except _Skip as exc:
        return _skip_or_raise(exc)
    assert base.__version__ == "5.156.0", base.__version__
    case_off_is_byte_identical(DE, base)
    case_published_rows_identical_except_tag_text(DE, base)
    case_settle_trajectory_identical(DE, base)


def test_base_scoring_module_byte_identical():
    """C1 is deferred: core.scoring is base's file, byte for byte."""
    if not _BASE_PATH or not os.path.isdir(_BASE_PATH):
        return _skip_or_raise(_Skip("%s is not a base source directory" % BASE_ENV))
    with open(os.path.join(_BASE_PATH, "core", "scoring.py"), "rb") as fh:
        want = hashlib.sha256(fh.read()).hexdigest()
    with open(inspect.getsourcefile(_SCORING), "rb") as fh:
        assert hashlib.sha256(fh.read()).hexdigest() == want


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


@_isolated
def test_evidence_is_deterministic():
    first = evidence(DE)
    assert evidence(DE) == first
    assert {k: v["settled"] for k, v in first["scope"].items()} == {
        "SPLG@Mutual_Funds": True, "SUI-USD@Commodities_FX": True, "SPY@Mutual_Funds": True,
        "2222.SR@Market_Leaders": False, "BRK-B.US@Global_Markets": True}
    assert first["labels"] == {"None": "all", repr(REALISTIC): REALISTIC,
                               "'global markets,My_Portfolio'": GM, "'My_Portfolio'": "none"}


def test_wiring_and_version():
    src = inspect.getsource(DE)
    assert tuple(int(x) for x in DE.__version__.split(".")[:3]) >= VERSION
    assert "Data Engine V2 - GLOBAL-FIRST ORCHESTRATOR - v" + DE.__version__ in src
    assert "WHY v5.157.0 - F-7 v2" in src
    # the one call site: scope = the symbol's inferred market page, whatever the page
    assert src.count("merged = _f7_settle_pass(merged, sym, page_ctx, scope_page=self._f7_scope_page(sym))") == 1
    calls = [n for n in ast.walk(ast.parse(src)) if isinstance(n, ast.Call)
             and isinstance(n.func, ast.Name) and n.func.id == "_f7_settle_pass"]
    assert len(calls) == 1 and [kw.arg for kw in calls[0].keywords] == ["scope_page"]
    assert list(inspect.signature(DE.DataEngineV5._f7_scope_page).parameters) == ["self", "symbol"]
    assert src.count('"scoring_settle": _f7_settle_mode(),') == 1 and src.count("scoring_settle=%s") == 1
    assert src.count("settle_unresolved=%s") == 1
    assert DE._F7_SETTLE_TAG_ORDER == ORDER and DE._F7_SETTLE_TAG_MAX == 8
    assert DE._F7_MARKET_PAGES == MARKETS
    assert [code for _k, code, _kind, _tol in DE._F7_SETTLE_FIELDS] == ["ov", "op", "va", "fcf", "r12", "rc", "os", "fs"]
    assert "final_diff" in DE.F7SettlementResult.__dataclass_fields__
    # v5.158.0: C1 (confidence provenance) is tests/test_de_f7_settle_shadow.py's;
    # core.scoring still has no confidence flag of its own.
    assert not hasattr(_SCORING, "_CONF_NOT_DATA_FLAG")


TESTS = [test_c2_tag_discloses_final_values_enforce_publishes, test_c2_tag_order_limit_and_degrade,
         test_c3_scope_lists_market_pages_only, test_c3_enforce_is_observe_out_of_scope,
         test_c3_scope_is_the_inferred_market_on_every_surface, test_off_is_byte_identical,
         test_published_rows_identical_except_tag_text, test_base_off_published_rows_and_trajectory_identical,
         test_base_scoring_module_byte_identical, test_golden_negative_gate_cases_fail_on_base,
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
