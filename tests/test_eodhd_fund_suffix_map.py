#!/usr/bin/env python3
"""tests/test_eodhd_fund_suffix_map.py - eodhd_provider v4.20.0
(TFB_EODHD_FUND_SUFFIX_MAP = off | observe | enforce, default off).

WHY (2026-10-28): fundamentals for Bursa Malaysia and Euronext Lisbon rows are
requested under codes EODHD does not use for those venues. normalize_eodhd_symbol
keeps '1295.KL' as is and turns 'SON.LS' into 'SON.LS.US' (the shared
normalizer does not list .LS and appends the default exchange). On EODHD's
earnings-calendar endpoint, calendar_sync run 40498374271 (27 Oct) logged both
shapes among the rejected codes and run 40678905106 (28 Oct) sent 'KL->KLSE x2,
LS.US->LS x3' with no rejection. The Yahoo fundamentals breaker has re-tripped
in every sync since 21 Oct, so EODHD is often these rows' only fundamentals
source: 13 of the 2,033 Global_Markets rows in the 10 Oct fixture. The supplier
stub below rejects the old codes, as the calendar endpoint did; that is the
premise, not a measurement of the fundamentals endpoint.

Pinned here (REAL provider and engine modules; the supplier HTTP boundary is
the only replacement, and it answers only the codes EODHD knows):
  W1 the pure code map: X.KL -> X.KLSE, X.LS.US -> X.LS, nothing else
  W2 enforce: the request, cache key and symbol_normalized use the EODHD
     code; 'symbol' stays the row's own; the identity guard accepts the
     declared Code / Exchange; one INFO line per translated code
  W3 observe: the request is the v4.19.0 one; one INFO line per code names
     the translation it would make
  W4 the engine fallback end to end (_apply_eodhd_fundamentals_fallback):
     off -> nothing fills (the stub rejects the old code); enforce -> the
     ratios fill, eodhd_fundamentals_fallback_applied
  W5 the engine's fund cache (TFB_EODHD_FUND_CACHE=enforce, live in
     production): an empty answer under the old code leaves a negative mark
     on the ROW symbol; while it lives, enforce does not reach the provider
     at all (the rollout delay the owner steps state); once it has expired
     (TFB_EODHD_FUND_NEG_TTL_H, default 168 h), enforce asks EODHD under the
     venue code and the row fills
  N1 off (11 spellings x all 2,033 fixture Global_Markets symbols): request
     paths, cache keys, patches and log records equal the v4.19.0 copy (git
     show BASE_SHA; without git, the OFF_GOLDEN digest of that copy's output,
     and the run says the live comparison was skipped)
  N2 enforce never touches quotes or history, nor any other venue code
     (.IS, .T, .MI, .SI, .L, .SR, US, FX, crypto)
  N3 version floor, manifest pin, zero removed functions, ASCII added lines
  G1 golden negative: W2, W3, W4 and W5 fail on the v4.19.0 copy

Run: python tests/test_eodhd_fund_suffix_map.py   or   pytest -q <this file>
"""
from __future__ import annotations

import ast
import asyncio
import hashlib
import importlib.util
import json
import logging
import os
import re
import subprocess
import sys
import tempfile
import types
from pathlib import Path

import pytest
from _pytest.monkeypatch import MonkeyPatch

_REPO = Path(__file__).resolve().parent.parent
if str(_REPO) not in sys.path:
    sys.path.insert(0, str(_REPO))

from core import data_engine_v2 as de  # noqa: E402
from core.providers import eodhd_provider as ep  # noqa: E402

FLAG = "TFB_EODHD_FUND_SUFFIX_MAP"
BASE_SHA = "0c3d3e21"          # main before v4.20.0 (eodhd_provider v4.19.0)
OFF_GOLDEN = "dd8d68d6d0971802"   # sha256[:16] of the v4.19.0 copy's off trace (2,033 symbols x 11 spellings)
NOTES = []   # what a run without the v4.19.0 copy could not compare (printed by the script runner)
FIXTURE = _REPO / "tests" / "fixtures" / "wp3ab_tuple_rows_2026-10-10.json"
LOGGER = "core.providers.eodhd_provider"
OFF_SPELLINGS = (None, "", "off", "OFF", "0", "1", "on", "true", "yes", "observe-ish", "enforced")
TIME_KEYS = re.compile(r"(_utc|_riyadh|collected_at)$")
# Codes EODHD answers (the calendar run's evidence): US, KLSE, LS, LSE and the
# rest of the world it already accepted. It rejects X.KL and X.LS.US.
REJECTED = re.compile(r"\.(KL|LS\.US|IS\.US)$")


# ----------------------------------------------------------------- fixtures
def _gm_symbols():
    data = json.loads(FIXTURE.read_text(encoding="utf-8"))
    return sorted({r["row"]["symbol"] for r in data["rows"]
                   if r["meta"].get("page") == "Global_Markets" and r["row"].get("symbol")})


def _payload(code):
    base, _, exch = code.partition(".")
    cur = {"KLSE": "MYR", "LS": "EUR", "LSE": "GBP"}.get(exch, "USD")
    return {
        "General": {"Code": base, "Exchange": exch or "US", "CurrencyCode": cur,
                    "Name": "Name " + base, "Sector": "Financial Services",
                    "Industry": "Banks", "UpdatedAt": "2026-10-27"},
        "Highlights": {"MarketCapitalization": 1.2e10, "EarningsShare": 0.55,
                       "PERatio": 10.0, "ProfitMargin": 0.3, "OperatingMarginTTM": 0.4,
                       "MostRecentQuarter": "2026-09-30"},
        "Valuation": {"PriceBookMRQ": 1.1, "PriceSalesTTM": 2.5},
        "SharesStats": {"SharesFloat": 3.0e9},
        "Technicals": {"Beta": 0.8},
    }


class _Server:
    """The supplier boundary: answers what EODHD knows, rejects the rest."""

    def __init__(self):
        self.calls = []

    async def __call__(self, path, params):
        self.calls.append(path)
        code = path.split("/", 1)[1] if "/" in path else path
        if REJECTED.search(code):
            return None, "not_found"
        if path.startswith("fundamentals/"):
            return json.loads(json.dumps(_payload(code))), None
        return None, "not_found"


def _client(mp, mod):
    mp.setattr(mod.httpx, "AsyncClient", lambda **kwargs: object())
    client = mod.EODHDClient()
    server = _Server()
    mp.setattr(client, "_request_json", server)
    return client, server


class _Logs(logging.Handler):
    def __init__(self):
        super().__init__(level=logging.DEBUG)
        self.lines = []

    def emit(self, record):
        self.lines.append((record.levelname, record.getMessage()))


def _capture():
    h = _Logs()
    lg = logging.getLogger(LOGGER)
    lg.addHandler(h)
    old = lg.level
    lg.setLevel(logging.DEBUG)
    return h, (lambda: (lg.removeHandler(h), lg.setLevel(old)))


def _set(mp, value):
    if value is None:
        mp.delenv(FLAG, raising=False)
    else:
        mp.setenv(FLAG, value)


def _fresh(mod):
    seen = getattr(mod, "_FUND_SUFFIX_MAP_SEEN", None)
    if seen is not None:
        seen.clear()


def _strip(obj):
    if isinstance(obj, dict):
        return {k: _strip(v) for k, v in obj.items() if not TIME_KEYS.search(str(k))}
    if isinstance(obj, list):
        return [_strip(v) for v in obj]
    return obj


def _fetch(client, sym):
    return asyncio.run(client.fetch_fundamentals(sym))


# ----------------------------------------------------------------- base copy
_BASE = {}


def _base_module():
    if "mod" in _BASE:
        return _BASE["mod"]
    _BASE["mod"] = None
    try:
        src = subprocess.run(["git", "-C", str(_REPO), "show", BASE_SHA + ":core/providers/eodhd_provider.py"],
                             capture_output=True, text=True, timeout=60, check=False)
    except Exception:  # noqa: BLE001 - no git
        return None
    if src.returncode != 0 or "PROVIDER_VERSION = \"4.19.0\"" not in src.stdout:
        return None
    tmp = Path(tempfile.mkdtemp(prefix="eodhd_base_")) / "eodhd_provider_base_4190.py"
    tmp.write_text(src.stdout, encoding="utf-8")
    spec = importlib.util.spec_from_file_location("eodhd_provider_base_4190", tmp)
    mod = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = mod
    spec.loader.exec_module(mod)
    _BASE["mod"] = mod
    return mod


def _off_trace(mp, mod, symbols, spellings=OFF_SPELLINGS):
    """Everything fetch_fundamentals does with the gate off, normalized."""
    out = []
    for value in spellings:
        _set(mp, value)
        _fresh(mod)
        client, server = _client(mp, mod)
        logs, done = _capture()
        try:
            for sym in symbols:
                patch, err = _fetch(client, sym)
                out.append([value, sym, err, _strip(patch)])
        finally:
            done()
        out.append([value, "calls", server.calls])
        out.append([value, "keys", sorted(client.fund_cache._d)])
        out.append([value, "logs", logs.lines])
    text = json.dumps(out, sort_keys=True, default=str)
    return text.replace(getattr(mod, "PROVIDER_VERSION", "?"), "<v>")


def _digest(text):
    return hashlib.sha256(text.encode("utf-8")).hexdigest()[:16]


# ----------------------------------------------------------------- cases
def case_w1_code_map(_mp, mod=ep):
    f = mod._eodhd_fund_venue_code
    assert f("1295.KL") == "1295.KLSE"
    assert f("SON.LS.US") == "SON.LS"
    for same in ("AAPL.US", "1120.SR", "VOD.LSE", "AEFES.IS.US", "7203.T", "IG.MI",
                 "T82U.SI", "1295.KLSE", "SON.LS", "BRK.B.US", "X.LS.UK", "A.KL.US",
                 ".KL", "KL", "", ".LS.US", "EURUSD.FOREX", "BTC-USD.CC", "^GSPC"):
        assert f(same) == same, same


def case_w2_enforce_requests_the_eodhd_code(mp, mod=ep):
    _set(mp, "enforce")
    _fresh(mod)
    client, server = _client(mp, mod)
    logs, done = _capture()
    try:
        k, kerr = _fetch(client, "1295.KL")
        s, serr = _fetch(client, "SON.LS")
        _fetch(client, "1295.KL")                          # cached: no new request, no new line
    finally:
        done()
    assert kerr is None and serr is None, (kerr, serr)
    assert server.calls == ["fundamentals/1295.KLSE", "fundamentals/SON.LS"], server.calls
    assert k["symbol"] == "1295.KL" and k["symbol_normalized"] == "1295.KLSE"
    assert s["symbol"] == "SON.LS" and s["symbol_normalized"] == "SON.LS"
    assert k["pe_ttm"] == 10.0 and k["currency"] == "MYR" and k["name"] == "Name 1295"
    assert s["eps_ttm"] == 0.55 and s["currency"] == "EUR"
    assert "eodhd_identity_unverified" not in str(k.get("warnings") or "")
    keys = sorted(client.fund_cache._d)
    assert any(x.endswith(":1295.KLSE") for x in keys) and any(x.endswith(":SON.LS") for x in keys), keys
    info = [m for lvl, m in logs.lines if "FUND-SUFFIX-MAP" in m]
    assert info == ["[eodhd v%s] FUND-SUFFIX-MAP enforce: fundamentals for 1295.KL requested as 1295.KLSE" % mod.PROVIDER_VERSION,
                    "[eodhd v%s] FUND-SUFFIX-MAP enforce: fundamentals for SON.LS.US requested as SON.LS" % mod.PROVIDER_VERSION], info
    assert all(lvl == "INFO" for lvl, m in logs.lines if "FUND-SUFFIX-MAP" in m)


def case_w3_observe_logs_and_changes_nothing(mp, mod=ep):
    _set(mp, " Observe ")
    _fresh(mod)
    client, server = _client(mp, mod)
    logs, done = _capture()
    try:
        k, kerr = _fetch(client, "1295.KL")
        client2, server2 = _client(mp, mod)
        _fetch(client2, "1295.KL")                         # a new client, same process: no second line
        _fetch(client, "AAPL.US")                          # nothing to translate: no line
    finally:
        done()
    assert server.calls == ["fundamentals/1295.KL", "fundamentals/AAPL.US"], server.calls
    assert server2.calls == ["fundamentals/1295.KL"]
    assert kerr == "not_found" and k.get("symbol_normalized") in (None, "1295.KL")
    info = [m for lvl, m in logs.lines if "FUND-SUFFIX-MAP" in m]
    assert info == ["[eodhd v%s] FUND-SUFFIX-MAP observe: fundamentals for 1295.KL would be requested as 1295.KLSE" % mod.PROVIDER_VERSION], info


ROW = {"symbol": "1295.KL", "name": "Public Bank Berhad", "current_price": 4.4, "price": 4.4,
       "currency": "MYR", "country": "Malaysia", "exchange": "KLSE", "sector": "Financial Services",
       "industry": "Banks"}


def _engine(mp, mod):
    client, server = _client(mp, mod)
    reg = types.ModuleType("eodhd_suffix_fixture")

    async def get_client():
        return client

    reg.get_client = get_client
    eng = de.DataEngineV5()
    eng._provider_registry._modules["eodhd"] = reg
    return eng, server


def _tags(row):
    return [p.strip() for p in (row.get("warnings") or "").split(";") if p.strip()]


def case_w4_engine_fallback_end_to_end(mp, mod=ep):
    mp.setenv("TFB_ENGINE_FUND_LKG", "0")
    mp.setenv("TFB_FUND_UNIT_SENTRY", "off")
    for k in ("TFB_EODHD_FUND_CACHE", "TFB_EODHD_ANALYST_TARGET"):
        mp.delenv(k, raising=False)
    for value, want in ((None, False), ("enforce", True)):
        _set(mp, value)
        _fresh(mod)
        eng, server = _engine(mp, mod)
        out = asyncio.run(eng._apply_eodhd_fundamentals_fallback(dict(ROW), ROW["symbol"], "Global_Markets"))
        tags = _tags(out)
        assert not any(t.startswith("identity_patch_refused") for t in tags), tags
        if want:
            assert server.calls == ["fundamentals/1295.KLSE"], server.calls
            assert out.get("pe_ttm") == 10.0 and out.get("market_cap") == 1.2e10, out
            assert "eodhd_fundamentals_fallback_applied" in tags, tags
            assert out["symbol"] == "1295.KL" and out["name"] == "Public Bank Berhad"
        else:
            assert server.calls == ["fundamentals/1295.KL"], server.calls
            assert out.get("pe_ttm") is None and out.get("market_cap") is None, out
            assert "eodhd_fundamentals_fallback_applied" not in tags, tags


def _hermetic(mp):
    """Every TFB_* / EODHD* / NORMALIZE_* variable unset, so the trace and its
    digest are the code defaults wherever this runs (script, pytest +
    conftest, CI). NORMALIZE_STRIP_SUFFIXES / _PREFIXES are read when
    core.symbols.normalize is imported; set them and the digest moves."""
    for k in list(os.environ):
        if k.startswith(("TFB_", "EODHD", "NORMALIZE_")):
            mp.delenv(k, raising=False)


def case_w5_fund_cache_negative_mark_delays_then_enforce_fills(mp, mod=ep):
    mp.setenv("TFB_EODHD_FUND_CACHE", "enforce")
    mp.setenv("TFB_ENGINE_FUND_LKG", "1")
    mp.setenv("TFB_ENGINE_FUND_LKG_REDIS", "0")
    mp.setenv("TFB_FUND_UNIT_SENTRY", "off")
    for k in ("TFB_EODHD_ANALYST_TARGET", "TFB_EODHD_FUND_NEG_TTL_H"):
        mp.delenv(k, raising=False)
    de._FUND_NEG_STORE.clear()
    de._FUND_LKG_STORE.clear()
    sym = ROW["symbol"]
    try:
        # 1. today (map off): the old code lands nothing, so the row is marked
        _set(mp, None)
        _fresh(mod)
        eng, server = _engine(mp, mod)
        out = asyncio.run(eng._apply_eodhd_fundamentals_fallback(dict(ROW), sym, "Global_Markets"))
        assert server.calls == ["fundamentals/1295.KL"], server.calls
        assert out.get("pe_ttm") is None and sym in de._FUND_NEG_STORE, (out, de._FUND_NEG_STORE)
        # 2. armed while the mark lives: the provider is not reached at all
        _set(mp, "enforce")
        _fresh(mod)
        eng, server = _engine(mp, mod)
        logs, done = _capture()
        try:
            out = asyncio.run(eng._apply_eodhd_fundamentals_fallback(dict(ROW), sym, "Global_Markets"))
        finally:
            done()
        assert server.calls == [] and out.get("pe_ttm") is None, (server.calls, out)
        assert any(t.startswith("fund_cache:neg") for t in _tags(out)), _tags(out)
        assert not [m for _lvl, m in logs.lines if "FUND-SUFFIX-MAP" in m]
        # 3. the mark has expired (default TTL 168 h): enforce asks under the venue code
        de._FUND_NEG_STORE[sym] = de._FUND_NEG_STORE[sym] - (168 * 3600 + 60)
        out = asyncio.run(eng._apply_eodhd_fundamentals_fallback(dict(ROW), sym, "Global_Markets"))
        assert server.calls == ["fundamentals/1295.KLSE"], server.calls
        assert out.get("pe_ttm") == 10.0 and "eodhd_fundamentals_fallback_applied" in _tags(out), out
        assert sym not in de._FUND_NEG_STORE
    finally:
        de._FUND_NEG_STORE.clear()
        de._FUND_LKG_STORE.clear()


def case_n1_off_is_the_v4190_copy(mp, mod=ep):
    _hermetic(mp)
    syms = _gm_symbols()
    assert len(syms) == 2033
    assert sum(1 for s in syms if s.endswith((".KL", ".LS"))) == 13
    if hasattr(mod, "_fund_suffix_map_mode"):
        for value in OFF_SPELLINGS:
            _set(mp, value)
            assert mod._fund_suffix_map_mode() == "off", value
    trace = _off_trace(mp, mod, syms, spellings=OFF_SPELLINGS)
    base = _base_module()
    if base is not None and mod is ep:
        assert trace == _off_trace(mp, base, syms, spellings=OFF_SPELLINGS)
    elif mod is ep:
        NOTES.append("N1: v4.19.0 copy unavailable (git show %s); compared with OFF_GOLDEN only" % BASE_SHA)
    assert _digest(trace) == OFF_GOLDEN, _digest(trace)


def case_n2_enforce_scope(mp, mod=ep):
    others = ["AEFES.IS", "7203.T", "IG.MI", "T82U.SI", "VOD.L", "1120.SR", "AAPL",
              "AAPL.US", "EURUSD=X", "BTC-USD", "^GSPC", "MBB.VN", "BHP.AX", "1295.KLSE"]
    traces = {}
    for value in (None, "enforce"):
        _set(mp, value)
        _fresh(mod)
        client, server = _client(mp, mod)
        for sym in others:
            _fetch(client, sym)
        for sym in ("1295.KL", "SON.LS") + tuple(others):
            asyncio.run(client.fetch_quote(sym))
            asyncio.run(client.fetch_history_stats(sym))
        traces[value] = list(server.calls)
    assert traces[None] == traces["enforce"], (traces[None], traces["enforce"])
    assert "fundamentals/AEFES.IS.US" in traces["enforce"]


def case_n3_version_pin_inventory_ascii(_mp, mod=ep):
    v = tuple(int(x) for x in ep.PROVIDER_VERSION.split("."))
    assert v >= (4, 20, 0), ep.PROVIDER_VERSION
    vd = (_REPO / "scripts" / "verify_deployment.py").read_text(encoding="utf-8")
    assert ("(\"core.providers.eodhd_provider\", \"PROVIDER_VERSION\", '%s'," % ep.PROVIDER_VERSION) in vd
    src = (_REPO / "core" / "providers" / "eodhd_provider.py").read_text(encoding="utf-8")
    for fn in ("_fund_suffix_map_mode", "_eodhd_fund_venue_code", "_fund_suffix_map"):
        assert re.search(r"^def %s\(" % fn, src, re.M), fn
    block = src[src.index("# v4.20.0 WHY (2026-10-28)"):src.index("# v4.19.0 WHY (2026-10-28)")]
    helpers = src[src.index("_FUND_SUFFIX_MAP_SEEN: set = set()"):src.index("def _eodhd_analyst_target_enabled")]
    for text in (block, helpers):
        assert all(ord(ch) < 128 for ch in text)
    base = _base_module()
    if base is None:
        NOTES.append("N3: v4.19.0 copy unavailable; the zero-removal AST check did not run")
    if base is not None:
        base_src = Path(base.__file__).read_text(encoding="utf-8")

        def names(s):
            tree = ast.parse(s)
            return {n.name for n in ast.walk(tree) if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef))}
        assert names(base_src) <= names(src)
        assert names(src) - names(base_src) == {"_fund_suffix_map_mode", "_eodhd_fund_venue_code", "_fund_suffix_map"}


def case_g1_golden_negative_on_the_v4190_copy(mp, _mod=ep):
    base = _base_module()
    if base is None:
        pytest.skip("v4.19.0 copy unavailable (git show %s)" % BASE_SHA)
    failed = []
    for case in (case_w2_enforce_requests_the_eodhd_code, case_w3_observe_logs_and_changes_nothing,
                 case_w4_engine_fallback_end_to_end, case_w5_fund_cache_negative_mark_delays_then_enforce_fills):
        try:
            case(mp, base)
        except (AssertionError, AttributeError):
            failed.append(case.__name__)
    assert failed == ["case_w2_enforce_requests_the_eodhd_code", "case_w3_observe_logs_and_changes_nothing",
                      "case_w4_engine_fallback_end_to_end",
                      "case_w5_fund_cache_negative_mark_delays_then_enforce_fills"], failed


CASES = [case_w1_code_map, case_w2_enforce_requests_the_eodhd_code, case_w3_observe_logs_and_changes_nothing,
         case_w4_engine_fallback_end_to_end, case_w5_fund_cache_negative_mark_delays_then_enforce_fills,
         case_n1_off_is_the_v4190_copy, case_n2_enforce_scope,
         case_n3_version_pin_inventory_ascii, case_g1_golden_negative_on_the_v4190_copy]


@pytest.mark.parametrize("case", CASES, ids=[c.__name__[5:] for c in CASES])
def test_eodhd_fund_suffix_map(case, monkeypatch):
    monkeypatch.delenv(FLAG, raising=False)
    case(monkeypatch)


if __name__ == "__main__":
    passed = skipped = 0
    for case in CASES:
        mp = MonkeyPatch()
        try:
            mp.delenv(FLAG, raising=False)
            case(mp)
            passed += 1
            print("ok   " + case.__name__)
        except pytest.skip.Exception as exc:
            skipped += 1
            print("skip " + case.__name__ + ": " + str(exc))
        except Exception as exc:  # noqa: BLE001
            print("FAIL " + case.__name__ + ": " + repr(exc)[:400])
        finally:
            mp.undo()
    for note in NOTES:
        print("note " + note)
    ran = len(CASES) - skipped
    print("PASS %d/%d%s" % (passed, ran, " (%d skipped)" % skipped if skipped else ""))
    sys.exit(0 if passed == ran else 1)
