"""data_engine_v2 v5.156.0 (authored as 5.155.0) -- SUFFIX LOCALE FALLBACK (TFB_ENGINE_SUFFIX_LOCALE_FALLBACK).

The 2026-10-13 20:37 Riyadh Top_10 export published MWG.VN as Market
NASDAQ/NYSE, Ccy USD, Price 74,700, Price SAR 280,653.13 (true ~11.06 SAR).
The engine stamped USD / NASDAQ/NYSE because its suffix table lacks .VN and
the two inferrers fall through to US defaults, while the country inferrer
already asks core.symbols.normalize (Vietnam). Armed, the exchange and
currency inferrers ask normalize too. Off, nothing changes.

Battery over the REAL module (no stand-ins):
WORKS   W1-W7   armed: inferrers, provider-row canonicalization, the full
                offline engine path (EODHD price-only patch + Yahoo VND/HOSE
                fundamentals), the identity guard and the builder's Price SAR;
                arm spellings; fixture census changes .VN rows only.
NEVER   N1-N6   off spellings; shapes that must never reach normalize; a
                raising/None/US/malformed normalize answer; never raises;
                no new health key; ASCII added lines.
GOLD    G1-G3   against an untouched base copy of main's engine, v5.155.0
                since 2026-10-21 (TFB_TEST_DE_BASE = path to
                `git show 55b47f24:core/data_engine_v2.py`; authored against
                v5.154.0 at 030e22eb): OFF census and
                canonicalization byte-identical; GOLDEN NEGATIVE - the W1
                check fails on the base with the gate armed; zero
                function/class removals.

Zero network: providers are stubbed and httpx is blocked.
Run:  python tests/test_de_suffix_locale_fallback.py   (prints PASS k/k)
      pytest -q tests/test_de_suffix_locale_fallback.py
"""
from __future__ import annotations

import ast
import asyncio
import glob
import importlib
import importlib.util
import json
import logging
import os
import re
import subprocess
import sys
import tempfile
import traceback
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
de = importlib.import_module("core.data_engine_v2")
nz = importlib.import_module("core.symbols.normalize")

ENV = "TFB_ENGINE_SUFFIX_LOCALE_FALLBACK"
DE_BASE = os.environ.get("TFB_TEST_DE_BASE", "")
BASE_VERSION = "5.155.0"   # merge note 2026-10-21 (#778): main #810 is the new base
# 2026-10-15 (v5.157.0 since 2026-10-21): default armed in code, so unset (None) now arms;
# an explicit blank still reads off in the engine and stays in OFF.
ARMED = (None, "1", "true", "yes", "on", " ON ", "True")
OFF = ("", "0", "false", "off", "no", "observe", "enforce", "2")
LEGACY_OFF = "0"  # explicit disarm; unset no longer means off

# The engine already names the right country for these, then stamps USD.
CLASS = (".VN", ".HOSE", ".SET", ".PS", ".PSE", ".AU", ".KO", ".BE", ".DU",
         ".DUB", ".FP", ".IM", ".VX", ".BU", ".CNQ")
# Shapes that must never reach the normalize fallback.
UNTOUCHED = ("BRK.B", "BF.B", "AAPL", "BTC-USD", "ETH-EUR", "BTC-USD.CC",
             "DX-Y.NYB", "^GSPC", "X.PH", "X.MA", "X.ETR", "DANGCEM.LG",
             "NBB.BH", "ABC.N", "2222.SR", "1120", "EURUSD=X", "GC=F",
             "AKO-B.US", "GRT-UN.TO", "0700.HK", "MOH.AT", "EREGL.IS",
             "VOD.L", "SAP.DE", "")
FX = {"USD": 3.75707, "VND": 0.000148}


def _arm(mp, value="1"):
    if value is None:
        mp.delenv(ENV, raising=False)
    else:
        mp.setenv(ENV, value)


def _infer(module, symbol):
    return (module._infer_exchange_from_symbol(symbol),
            module._infer_currency_from_symbol(symbol),
            module._infer_country_from_symbol(symbol))


def _canon(module, symbol, price=74700.0, provider="eodhd"):
    row = module._canonicalize_provider_row(
        {"symbol": symbol, "current_price": price}, provider=provider)
    return {k: v for k, v in row.items()
            if k not in ("last_updated_utc", "last_updated_riyadh")}


def _fixture_symbols():
    syms = set()

    def walk(obj):
        if isinstance(obj, dict):
            for key, value in obj.items():
                if key in ("symbol", "Symbol") and isinstance(value, str):
                    syms.add(value.strip())
                else:
                    walk(value)
        elif isinstance(obj, list):
            for value in obj:
                walk(value)

    for path in sorted(glob.glob(str(ROOT / "tests" / "fixtures" / "*.json"))):
        with open(path, encoding="utf-8") as fh:
            walk(json.load(fh))
    return sorted(s for s in syms if s)


def _census():
    syms = {"ABC" + suffix for suffix in de._SUFFIX_TO_LOCALE}
    syms |= {"ABC" + suffix for suffix in nz.EXCHANGE_SUFFIXES}
    syms |= set(UNTOUCHED) | {"MWG" + suffix for suffix in CLASS}
    syms |= set(_fixture_symbols())
    return sorted(syms)


def assert_vn_fixed(module):
    """The W1 property: armed, MWG.VN reads HOSE / VND / Vietnam and every
    class suffix takes normalize's exchange and currency."""
    assert _infer(module, "MWG.VN") == ("HOSE", "VND", "Vietnam"), _infer(module, "MWG.VN")
    for suffix in CLASS:
        sym = "MWG" + suffix
        got = _infer(module, sym)
        want = (nz.get_exchange_display(sym), nz.get_currency_from_symbol(sym),
                nz.get_country_from_symbol(sym))
        assert got == want, (sym, got, want)
        assert got[1] != "USD" and "NASDAQ" not in got[0], (sym, got)


# ---------------------------------------------------------------- WORKS ----
def case_w1_inferrers_armed(mp, _tmp):
    _arm(mp, LEGACY_OFF)  # 2026-10-15: default armed in code; this case tests the legacy/off path
    assert _infer(de, "MWG.VN") == ("NASDAQ/NYSE", "USD", "Vietnam")  # the defect
    _arm(mp)
    assert_vn_fixed(de)


def case_w2_provider_row_canonicalized(mp, _tmp):
    _arm(mp, LEGACY_OFF)  # 2026-10-15: default armed in code; this case tests the legacy/off path
    off = _canon(de, "MWG.VN")
    assert (off["exchange"], off["currency"], off["country"]) == ("NASDAQ/NYSE", "USD", "Vietnam")
    _arm(mp)
    on = _canon(de, "MWG.VN")
    assert (on["exchange"], on["currency"], on["country"]) == ("HOSE", "VND", "Vietnam")
    # only the two labels move
    assert {k for k in on if on[k] != off.get(k)} == {"exchange", "currency"}
    # a provider-declared currency is never overwritten by the fallback
    declared = de._canonicalize_provider_row(
        {"symbol": "MWG.VN", "current_price": 74700.0, "currency": "VND",
         "exchange": "HOSE"}, provider="yahoo_chart")
    assert (declared["currency"], declared["exchange"]) == ("VND", "HOSE")


def _engine_row(mp, page):
    """Full offline _get_enriched_quote_impl: the REAL EODHD adapter over a
    mocked real-time payload (no exchange or currency, as live), Yahoo
    fundamentals returning VND / HOSE, every other provider and httpx blocked."""
    import httpx
    eo = importlib.import_module("core.providers.eodhd_provider")
    mp.setenv("EODHD_API_KEY", "offline-test-key")
    mp.delenv("FINNHUB_API_KEY", raising=False)

    async def fake_request_json(self, path, params=None):
        if path.startswith("real-time/"):
            return ({"code": path.split("/", 1)[1], "timestamp": 1760360000,
                     "gmtoffset": 0, "open": 74000.0, "high": 75000.0,
                     "low": 73800.0, "close": 74700.0, "volume": 1200000,
                     "previousClose": 74000.0, "change": 700.0,
                     "change_p": 0.946}, None)
        return (None, "HTTP 404 not_found")

    async def fake_yf(self, symbol, page=""):
        return {"symbol": symbol, "currency": "VND", "exchange": "HOSE",
                "name": "Mobile World Investment Corporation",
                "sector": "Consumer Discretionary", "market_cap": 1.1e14}

    async def empty(*_a, **_k):
        return {}

    async def none(*_a, **_k):
        return None

    async def no_net(*_a, **_k):
        raise RuntimeError("network blocked in test")

    engine_cls = de.DataEngineV5
    orig_fetch = engine_cls._fetch_patch

    async def guarded(self, provider_name, symbol, page=""):
        if provider_name != "eodhd":
            return {}
        return await orig_fetch(self, provider_name, symbol, page)

    mp.setattr(eo.EODHDClient, "_request_json", fake_request_json)
    mp.setattr(engine_cls, "_fetch_yahoo_fundamentals_patch", fake_yf)
    mp.setattr(engine_cls, "_fetch_yahoo_chart_patch", empty)
    for name in ("_get_symbol_snapshot_row", "_get_best_snapshot_row"):
        mp.setattr(engine_cls, name, none)
    for name in ("_get_history_patch_best_effort", "_fetch_history_patch"):
        if hasattr(engine_cls, name):
            mp.setattr(engine_cls, name, empty)
    mp.setattr(engine_cls, "_fetch_patch", guarded)
    mp.setattr(httpx.AsyncClient, "send", no_net)
    previous = logging.root.manager.disable
    logging.disable(logging.CRITICAL)
    try:
        return asyncio.run(engine_cls()._get_enriched_quote_impl("MWG.VN", page))
    finally:
        logging.disable(previous)


def case_w3_full_engine_path(mp, _tmp):
    for page in ("Global_Markets", "Top_10_Investments"):
        _arm(mp, LEGACY_OFF)  # 2026-10-15: default armed in code; this case tests the legacy/off path
        off = _engine_row(mp, page)
        assert (off["exchange"], off["currency"]) == ("NASDAQ/NYSE", "USD"), page
        assert "market_cap_currency_suspect" in str(off.get("warnings"))
        _arm(mp)
        on = _engine_row(mp, page)
        assert (on["exchange"], on["currency"], on["country"]) == ("HOSE", "VND", "Vietnam"), page
        assert on["current_price"] == off["current_price"] == 74700.0
        assert on["data_provider"] == "eodhd"
        assert "market_cap_currency_suspect" not in str(on.get("warnings"))


def case_w4_identity_guard_agrees(mp, _tmp):
    ig = importlib.import_module("core.analysis.identity_guard")
    _arm(mp, LEGACY_OFF)  # 2026-10-15: default armed in code; this case tests the legacy/off path
    assert ig.currency_is_consistent("MWG.VN", _infer(de, "MWG.VN")[1]) is False
    _arm(mp)
    assert ig.currency_is_consistent("MWG.VN", _infer(de, "MWG.VN")[1]) is True


def case_w5_builder_price_sar(mp, _tmp):
    ob = importlib.import_module("core.analysis.opportunity_builder")
    crit = ob.make_criteria()
    got = {}
    for mode in (LEGACY_OFF, "1"):  # 2026-10-15: default armed in code; this case tests the legacy/off path
        _arm(mp, mode)
        row = _canon(de, "MWG.VN")
        cand = ob.normalize_candidate(
            {"Symbol": "MWG.VN", "Exchange": row["exchange"],
             "Currency": row["currency"], "Current Price": 74700.0}, FX, crit)
        got[mode] = (cand["currency"], round(cand["price_sar"], 2))
    assert got[LEGACY_OFF] == ("USD", 280653.13), got  # the published number
    assert got["1"] == ("VND", 11.06), got


def case_w6_arm_spellings(mp, _tmp):
    for value in ARMED:
        _arm(mp, value)
        assert de._suffix_locale_fallback_enabled() is True, value
        assert _infer(de, "MWG.VN")[:2] == ("HOSE", "VND"), value


def case_w7_fixture_census_moves_vn_only(mp, _tmp):
    syms = _fixture_symbols()
    assert len(syms) >= 2000
    _arm(mp, LEGACY_OFF)  # 2026-10-15: default armed in code; this case tests the legacy/off path
    off = {s: _infer(de, s) for s in syms}
    _arm(mp)
    on = {s: _infer(de, s) for s in syms}
    moved = sorted(s for s in syms if on[s] != off[s])
    assert moved and all(s.endswith(".VN") for s in moved), moved
    assert len(moved) == len([s for s in syms if s.endswith(".VN")]) == 10
    for s in moved:
        assert on[s] == ("HOSE", "VND", "Vietnam") and off[s][2] == "Vietnam"


# ---------------------------------------------------------------- NEVER ----
def case_n1_off_spellings_are_off(mp, _tmp):
    for value in OFF:
        _arm(mp, value)
        assert de._suffix_locale_fallback_enabled() is False, value
        assert de._suffix_locale_nz_for("MWG.VN") is None, value
        for suffix in CLASS:
            assert _infer(de, "MWG" + suffix)[:2] == ("NASDAQ/NYSE", "USD"), (value, suffix)


def case_n2_untouched_shapes(mp, _tmp):
    _arm(mp, LEGACY_OFF)  # 2026-10-15: default armed in code; this case tests the legacy/off path
    off = {s: _infer(de, s) for s in UNTOUCHED}
    _arm(mp)
    for s in UNTOUCHED:
        assert _infer(de, s) == off[s], (s, _infer(de, s), off[s])
        assert de._suffix_locale_nz_for(s) is None, s
    for suffix in de._SUFFIX_TO_LOCALE:  # a table hit never asks normalize
        assert de._suffix_locale_nz_for("ABC" + suffix) is None, suffix


def case_n3_bad_normalize_answers_fall_back(mp, _tmp):
    _arm(mp)

    def boom(_s):
        raise RuntimeError("normalize failed")

    for name in ("_nz_get_exchange_display", "_nz_get_currency_from_symbol",
                 "_nz_get_country_from_symbol"):
        with pytest.MonkeyPatch.context() as inner:
            inner.setattr(de, name, boom)
            assert _infer(de, "MWG.VN")[:2] == ("NASDAQ/NYSE", "USD"), name
        with pytest.MonkeyPatch.context() as inner:
            inner.setattr(de, name, None)
            assert _infer(de, "MWG.VN")[:2] == ("NASDAQ/NYSE", "USD"), name
    for exch, ccy, country in (("NASDAQ", "VND", "Vietnam"), ("HOSE", "USD", "Vietnam"),
                               ("HOSE", "VND", "USA"), ("", "VND", "Vietnam"),
                               ("HOSE", "VNDX", "Vietnam"), ("HOSE", "V1D", "Vietnam"),
                               ("HOSE", None, "Vietnam"), ("HOSE", "VND", None)):
        with pytest.MonkeyPatch.context() as inner:
            inner.setattr(de, "_nz_get_exchange_display", lambda _s, v=exch: v)
            inner.setattr(de, "_nz_get_currency_from_symbol", lambda _s, v=ccy: v)
            inner.setattr(de, "_nz_get_country_from_symbol", lambda _s, v=country: v)
            assert de._suffix_locale_nz_for("MWG.VN") is None, (exch, ccy, country)
            assert _infer(de, "MWG.VN")[:2] == ("NASDAQ/NYSE", "USD")


def case_n4_never_raises(mp, _tmp):
    _arm(mp)
    for odd in (None, "", ".", "A.", ".VN", 123, "mwg.vn", " MWG.VN ", "MWG..VN"):
        de._suffix_locale_nz_for(odd)
    assert de._suffix_locale_nz_for(" mwg.vn ") == ("HOSE", "VND", "Vietnam")


def case_n5_no_new_health_key(mp, _tmp):
    _arm(mp, LEGACY_OFF)  # 2026-10-15: default armed in code; this case tests the legacy/off path
    off = de.surface_gate_states()
    _arm(mp)
    on = de.surface_gate_states()
    assert on == off
    assert not any("suffix" in str(k).lower() for k in on)


def case_n6_ascii_added_lines(_mp, _tmp):
    src = (ROOT / "core" / "data_engine_v2.py").read_text(encoding="utf-8")
    start = src.index("WHY v5.156.0 (2026-10-14")
    block = src[start:src.index("WHY v5.154.0 - INTEGRATED")]
    helper = src[src.index("# v5.156.0 - SUFFIX LOCALE FALLBACK"):
                 src.index("def _infer_exchange_from_symbol")]
    for text in (block, helper):
        assert all(ord(ch) < 128 for ch in text)
    assert src.count("_nzl = _suffix_locale_nz_for(s)  # v5.156.0: off -> None") == 2
    assert tuple(int(x) for x in de.__version__.split(".")[:3]) >= (5, 156, 0)
    if (ROOT / ".git").exists():
        try:
            diff = subprocess.run(
                ["git", "-C", str(ROOT), "diff", "--unified=0", "030e22eb", "--",
                 "core/data_engine_v2.py"], capture_output=True, text=True,
                timeout=60, check=False).stdout
        except Exception:  # noqa: BLE001 - shallow clone or no git
            diff = ""
        added = [ln for ln in diff.splitlines() if ln.startswith("+") and not ln.startswith("+++")]
        assert all(ord(ch) < 128 for ln in added for ch in ln)


# ----------------------------------------------------------------- GOLD ----
def _base():
    if not DE_BASE:
        pytest.skip("TFB_TEST_DE_BASE unset")
    name = "de_base_5154"
    if name in sys.modules:
        return sys.modules[name]
    spec = importlib.util.spec_from_file_location(name, DE_BASE)
    mod = importlib.util.module_from_spec(spec)
    sys.modules[name] = mod
    spec.loader.exec_module(mod)
    assert mod.__version__ == BASE_VERSION, "TFB_TEST_DE_BASE is not v" + BASE_VERSION
    return mod


def case_g1_off_census_byte_identical_to_base(mp, _tmp):
    base = _base()
    syms = _census()
    assert len(syms) >= 2100
    for value in OFF:
        _arm(mp, value)
        for s in syms:
            assert _infer(de, s) == _infer(base, s), (value, s)
    _arm(mp, LEGACY_OFF)  # 2026-10-15: default armed in code; this case tests the legacy/off path
    for s in ["MWG" + suffix for suffix in CLASS] + list(UNTOUCHED[:-1]):
        assert _canon(de, s) == _canon(base, s), s
    assert de.surface_gate_states() == base.surface_gate_states()


def case_g2_golden_negative_base_fails_armed(mp, _tmp):
    base = _base()
    _arm(mp)
    with pytest.raises(AssertionError):
        assert_vn_fixed(base)  # base ignores the gate: MWG.VN stays USD
    assert _infer(base, "MWG.VN") == ("NASDAQ/NYSE", "USD", "Vietnam")
    assert_vn_fixed(de)


def _defs(path):
    tree = ast.parse(Path(path).read_text(encoding="utf-8"))
    return {n.name for n in ast.walk(tree)
            if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef))}


def case_g3_zero_removals(_mp, _tmp):
    _base()
    head = _defs(ROOT / "core" / "data_engine_v2.py")
    base = _defs(DE_BASE)
    assert base - head == set(), sorted(base - head)
    assert head - base == {"_suffix_locale_fallback_enabled", "_suffix_locale_nz_for"}


CASES = [value for _name, value in sorted(
    ((name, value) for name, value in globals().items()
     if name.startswith("case_") and callable(value)),
    key=lambda kv: ({"w": 0, "n": 1, "g": 2}[kv[0][5]],
                    int(re.sub(r"\D", "", kv[0].split("_")[1]) or 0), kv[0]))]


@pytest.mark.parametrize("case", CASES, ids=[c.__name__[5:] for c in CASES])
def test_de_suffix_locale_fallback(case, monkeypatch, tmp_path):
    case(monkeypatch, tmp_path)


def test_case_count_is_self_consistent():
    names = [c.__name__ for c in CASES]
    assert len(names) == len(set(names)) == len(CASES)
    assert {n[5] for n in names} == {"w", "n", "g"}


def _main():
    passed = failed = skipped = 0
    for case in CASES:
        with pytest.MonkeyPatch.context() as mp, tempfile.TemporaryDirectory() as tmp:
            try:
                case(mp, Path(tmp))
            except pytest.skip.Exception as exc:
                skipped += 1
                print("SKIP " + case.__name__ + " :: " + str(exc))
                continue
            except Exception:  # noqa: BLE001
                failed += 1
                print("FAIL " + case.__name__)
                traceback.print_exc()
                continue
        passed += 1
        print("ok   " + case.__name__)
    total = passed + failed
    assert total + skipped == len(CASES)
    print("PASS %d/%d%s" % (passed, total, ("  (skipped %d)" % skipped) if skipped else ""))
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(_main())
