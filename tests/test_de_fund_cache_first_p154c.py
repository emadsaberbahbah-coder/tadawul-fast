#!/usr/bin/env python3
"""tests/test_de_fund_cache_first_p154c.py
Battery for core/data_engine_v2.py v5.150.0 [P-154c EODHD FUNDAMENTALS
CACHE-FIRST + NEGATIVE CACHE + PAGE SKIP]. Runs the REAL
DataEngineV5._apply_eodhd_fundamentals_fallback and the REAL
_fund_lkg_capture over real 2026-09-24 Global_Markets rows; only the EODHD
wire is a counting fixture module registered in the REAL ProviderRegistry.
No Redis: TFB_ENGINE_FUND_LKG_REDIS=0 (memory layer). Stateless per test:
the module stores are cleared in a fixture.
"""
from __future__ import annotations

import asyncio
import copy
import os
import time
import types

import pytest

import core.data_engine_v2 as de

REAL_ROWS = {
    "hit": [
        {
            "input": {
                "symbol": "WABC.US",
                "industry": "Banks - Regional",
                "sector": "Financial Services",
                "currency": "USD",
                "country": "USA",
                "name": "Westamerica Bancorporation",
                "market_cap": 1364482432.0,
                "float_shares": 21862689.0,
                "pe_ttm": 13.0331,
                "pe_forward": 21.097,
                "eps_ttm": 4.53,
                "dividend_yield": 0.000323,
                "payout_ratio": 0.41,
                "beta_5y": 0.548,
                "gross_margin": 94.9476,
                "operating_margin": 56.07,
                "revenue_ttm": 265716000.0,
                "revenue_growth_yoy": -0.024948,
                "pb_ratio": 1.61,
                "ps_ratio": 5.4103,
                "ev_ebitda": 0.0,
                "target_mean_price": 64.0,
                "current_price": 59.23
            },
            "provider": {
                "debt_to_equity": 0.18052,
                "free_cash_flow_ttm": 108123000.0,
                "gross_margin": 94.9476,
                "operating_margin": 56.07,
                "revenue_ttm": 265716000.0
            }
        },
        {
            "input": {
                "symbol": "B.US",
                "industry": "Gold",
                "sector": "Basic Materials",
                "currency": "USD",
                "country": "USA",
                "name": "Barrick Mining Corporation",
                "market_cap": 69933631726.0,
                "float_shares": 1640057988.0,
                "pe_ttm": 11.330668,
                "pe_forward": 9.693875,
                "eps_ttm": 3.75,
                "dividend_yield": 0.016,
                "payout_ratio": 23.77,
                "beta_5y": 1.15,
                "gross_margin": 56.27,
                "operating_margin": 51.34,
                "profit_margin": 31.6,
                "revenue_ttm": 20654999552.0,
                "revenue_growth_yoy": 0.438,
                "pb_ratio": 2.56,
                "ps_ratio": 3.385797,
                "peg_ratio": 2.04,
                "ev_ebitda": 6.063,
                "target_mean_price": 51.70438,
                "current_price": 42.49
            },
            "provider": {
                "debt_to_equity": 0.12706,
                "free_cash_flow_ttm": 5088625152.0,
                "gross_margin": 56.27,
                "operating_margin": 51.34,
                "profit_margin": 31.6,
                "revenue_ttm": 20654999552.0
            }
        },
        {
            "input": {
                "symbol": "7741.T",
                "industry": "Medical Instruments & Supplies",
                "sector": "Healthcare",
                "currency": "JPY",
                "country": "Japan",
                "name": "HOYA Corporation",
                "market_cap": 7611881165980.0,
                "float_shares": 334613541.0,
                "pe_ttm": 30.684416,
                "pe_forward": 33.4,
                "eps_ttm": 743.7,
                "payout_ratio": 39.66,
                "gross_margin": 0.48337,
                "operating_margin": 0.3239,
                "profit_margin": 27.176,
                "revenue_ttm": 983639982080.0,
                "revenue_growth_yoy": 0.162,
                "pb_ratio": 7.518995,
                "ps_ratio": 7.738482,
                "peg_ratio": 2.38,
                "ev_ebitda": 18.331,
                "current_price": 23055.0
            },
            "provider": {
                "debt_to_equity": 0.04404,
                "free_cash_flow_ttm": 178765873152.0,
                "gross_margin": 0.48337,
                "operating_margin": 0.3239,
                "profit_margin": 27.176,
                "revenue_ttm": 983639982080.0
            }
        },
        {
            "input": {
                "symbol": "ALX.US",
                "industry": "REIT - Retail",
                "sector": "Real Estate",
                "currency": "USD",
                "country": "USA",
                "name": "Alexander's, Inc.",
                "market_cap": 1265943936.0,
                "float_shares": 2118606.0,
                "pe_ttm": 7.5135,
                "pe_forward": 35.5872,
                "eps_ttm": 32.99,
                "dividend_yield": 0.000725,
                "payout_ratio": 0.000308,
                "beta_5y": 0.75,
                "gross_margin": 64.1009,
                "operating_margin": 28.6096,
                "profit_margin": 79.06,
                "revenue_ttm": 214802000.0,
                "revenue_growth_yoy": 0.060517,
                "pb_ratio": 5.6732,
                "ps_ratio": 5.8935,
                "ev_ebitda": 7.3705,
                "target_mean_price": 212.0,
                "current_price": 247.87
            },
            "provider": {
                "debt_to_equity": 3.83055,
                "free_cash_flow_ttm": -38407000.0,
                "gross_margin": 64.1009,
                "operating_margin": 28.6096,
                "profit_margin": 79.06,
                "revenue_ttm": 214802000.0
            }
        }
    ],
    "noop": [
        {
            "input": {
                "symbol": "BRES.QA",
                "industry": "Real Estate - Diversified",
                "sector": "Real Estate",
                "currency": "QAR",
                "country": "Qatar",
                "name": "Barwa Real Estate Company Q.P.S.C.",
                "market_cap": 8623001956.0,
                "float_shares": 2135087768.0,
                "pe_ttm": 7.64138,
                "pe_forward": 0.865625,
                "eps_ttm": 0.29,
                "dividend_yield": 0.0781,
                "payout_ratio": 56.37,
                "beta_5y": 0.25,
                "gross_margin": 0.6674,
                "operating_margin": 0.57988,
                "profit_margin": 62.706,
                "debt_to_equity": 0.58464,
                "revenue_ttm": 1982947968.0,
                "revenue_growth_yoy": 0.254,
                "pb_ratio": 0.38,
                "ps_ratio": 4.348578,
                "ev_ebitda": 18.306,
                "current_price": 2.22
            },
            "provider": {}
        },
        {
            "input": {
                "symbol": "4527.T",
                "industry": "Household & Personal Products",
                "sector": "Consumer Defensive",
                "currency": "JPY",
                "country": "Japan",
                "name": "Rohto Pharmaceutical Co.,Ltd.",
                "market_cap": 627712597224.0,
                "float_shares": 200751196.0,
                "pe_ttm": 19.157206,
                "pe_forward": 16.663673,
                "eps_ttm": 145.35,
                "dividend_yield": 0.018,
                "payout_ratio": 31.66,
                "beta_5y": -0.432,
                "gross_margin": 0.56153,
                "operating_margin": 0.14812,
                "profit_margin": 9.374,
                "debt_to_equity": 0.17689,
                "revenue_ttm": 353384988672.0,
                "revenue_growth_yoy": 0.118,
                "pb_ratio": 2.03,
                "ps_ratio": 1.776285,
                "ev_ebitda": 10.17,
                "current_price": 2797.0
            },
            "provider": {
                "market_cap": 627712597224.0,
                "pe_ttm": 19.157206
            }
        }
    ]
}

FORBIDDEN = ("cap", "forecast", "target", "roi", "drop", "reject", "provider_target",
             "price_bar_stale", "xprovider_price_conflict", "fundamentals_lkg")


class _Calls:
    def __init__(self):
        self.n = 0
        self.per = {}


def _provider(table, calls):
    mod = types.ModuleType("eodhd_fixture_provider")

    def fetch_fundamentals_patch(symbol):
        calls.n += 1
        calls.per[symbol] = calls.per.get(symbol, 0) + 1
        return dict(table.get(symbol, {}))

    mod.fetch_fundamentals_patch = fetch_fundamentals_patch
    return mod


def _engine(table, calls):
    eng = de.DataEngineV5()
    eng._provider_registry._modules["eodhd"] = _provider(table, calls)
    return eng


def _run(eng, row, page="Global_Markets"):
    out = asyncio.run(eng._apply_eodhd_fundamentals_fallback(dict(row), row["symbol"], page))
    de._fund_lkg_capture(row["symbol"], out)   # the pipeline seam right after the fallback
    return out


def _tags(row):
    return [p.strip() for p in (row.get("warnings") or "").split(";") if p.strip()]


def _fc(row):
    return [t for t in _tags(row) if t.startswith("fund_cache:")]


@pytest.fixture(autouse=True)
def _clean_env(monkeypatch):
    for k in ("TFB_EODHD_FUND_CACHE", "TFB_EODHD_FUND_CACHE_TTL_H", "TFB_EODHD_FUND_NEG_TTL_H",
              "TFB_EODHD_FUND_FALLBACK_SKIP_PAGES", "TFB_FUND_UNIT_SENTRY"):
        monkeypatch.delenv(k, raising=False)
    monkeypatch.setenv("TFB_ENGINE_FUND_LKG", "1")
    monkeypatch.setenv("TFB_ENGINE_FUND_LKG_REDIS", "0")
    monkeypatch.setenv("TFB_FUND_UNIT_SENTRY", "off")
    de._FUND_LKG_STORE.clear()
    de._FUND_NEG_STORE.clear()
    for k in list(de._FUND_CACHE_STATS):
        de._FUND_CACHE_STATS[k] = 0
    yield
    de._FUND_LKG_STORE.clear()
    de._FUND_NEG_STORE.clear()


def _table():
    t = {}
    for cls in ("hit", "noop"):
        for it in REAL_ROWS[cls]:
            t[it["input"]["symbol"]] = it["provider"]
    return t


# T1 -- gate vocabulary: explicit words only, defaults
def test_t1_mode_vocabulary(monkeypatch):
    assert de.__version__ >= "5.150.0"
    assert de._fund_cache_mode() == "off"
    for bad in ("1", "true", "on", "yes", "ENFORCED", "obs"):
        monkeypatch.setenv("TFB_EODHD_FUND_CACHE", bad)
        assert de._fund_cache_mode() == "off", bad
    for good in ("observe", "ENFORCE", " Observe "):
        monkeypatch.setenv("TFB_EODHD_FUND_CACHE", good)
        assert de._fund_cache_mode() == good.strip().lower()
    assert de._fund_cache_ttl_h() == 24.0
    assert de._fund_neg_ttl_h() == 168.0
    monkeypatch.setenv("TFB_EODHD_FUND_CACHE_TTL_H", "0.1")
    assert de._fund_cache_ttl_h() == 1.0
    monkeypatch.setenv("TFB_EODHD_FUND_CACHE_TTL_H", "9999")
    assert de._fund_cache_ttl_h() == de._fund_lkg_ttl_h()   # ceiling = the LKG TTL
    monkeypatch.setenv("TFB_EODHD_FUND_FALLBACK_SKIP_PAGES", " Mutual_Funds , commodities_fx ")
    assert de._fund_fb_skip_pages() == {"MUTUAL_FUNDS", "COMMODITIES_FX"}
    assert de._fund_fb_skip_pages.__name__  # helper exists


# T2 -- off: byte-identical rows, the provider paid on every pass, no tag
def test_t2_off_is_byte_identical():
    calls = _Calls()
    eng = _engine(_table(), calls)
    rows = [it["input"] for it in REAL_ROWS["hit"]]
    p1 = [_run(eng, r) for r in rows]
    p2 = [_run(eng, r) for r in rows]
    assert calls.n == 2 * len(rows)
    for a, b in zip(p1, p2):
        assert a == b
        assert not _fc(a)
        assert "eodhd_fundamentals_fallback_applied" in _tags(a)
    assert de._fund_cache_stats()["miss"] == 0   # off never even counts


# T3 -- observe: would_* tags only; values and calls identical to off
def test_t3_observe_tag_only(monkeypatch):
    calls_off = _Calls()
    eng_off = _engine(_table(), calls_off)
    rows = [it["input"] for it in REAL_ROWS["hit"]] + [it["input"] for it in REAL_ROWS["noop"]]
    off = [[_run(eng_off, r) for r in rows] for _ in range(2)]
    de._FUND_LKG_STORE.clear()
    monkeypatch.setenv("TFB_EODHD_FUND_CACHE", "observe")
    calls_obs = _Calls()
    eng_obs = _engine(_table(), calls_obs)
    obs = [[_run(eng_obs, r) for r in rows] for _ in range(2)]
    assert calls_obs.n == calls_off.n == 2 * len(rows)
    for pi in range(2):
        for a, b in zip(off[pi], obs[pi]):
            fa = {k: v for k, v in a.items() if k != "warnings"}
            fb = {k: v for k, v in b.items() if k != "warnings"}
            assert fa == fb                         # values byte-identical
            extra = set(_tags(b)) - set(_tags(a))
            assert extra and all(t.startswith("fund_cache:would_") for t in extra) or not extra
    # pass 2: every hit-class row is a would_hit, every noop row a would_neg
    hits = [t for r in obs[1][:4] for t in _fc(r)]
    assert len(hits) == 4 and all(t.startswith("fund_cache:would_hit:0h:") for t in hits)
    negs = [t for r in obs[1][4:] for t in _fc(r)]
    assert negs == ["fund_cache:would_neg:empty", "fund_cache:would_neg:nofill"]
    st = de._fund_cache_stats()
    assert st["would_hit"] == 4 and st["would_neg"] == 4 and st["hit"] == 0 and st["neg_writes"] == 0
    assert not de._FUND_NEG_STORE                    # observe never writes a mark


# T4 -- enforce: cold pass pays and seeds, warm pass is served from the snapshot
def test_t4_enforce_hit(monkeypatch):
    monkeypatch.setenv("TFB_EODHD_FUND_CACHE", "enforce")
    calls = _Calls()
    eng = _engine(_table(), calls)
    rows = [it["input"] for it in REAL_ROWS["hit"]]
    p1 = [_run(eng, r) for r in rows]
    assert calls.n == len(rows)
    p2 = [_run(eng, r) for r in rows]
    assert calls.n == len(rows)                      # zero new requests
    for a, b in zip(p1, p2):
        assert {k: b.get(k) for k in de._YAHOO_FUNDAMENTAL_NEEDS_CHECK_FIELDS} == \
               {k: a.get(k) for k in de._YAHOO_FUNDAMENTAL_NEEDS_CHECK_FIELDS}
        assert b["debt_to_equity"] == a["debt_to_equity"] and b["free_cash_flow_ttm"] == a["free_cash_flow_ttm"]
        tag = _fc(b)
        assert len(tag) == 1 and tag[0].startswith("fund_cache:hit:0h:")
        assert "eodhd_fundamentals_fallback_applied" not in _tags(b)   # honest: no provider call
    # the row lacking target_mean_price discloses ":nt"
    nt = [_fc(b)[0].endswith(":nt") for b in p2]
    assert nt == [de._is_missing_or_unknown_field(r.get("target_mean_price")) for r in rows]
    assert de._fund_cache_stats()["hit"] == len(rows)


# T5 -- enforce: an empty answer is remembered; nofill and refusal too
def test_t5_enforce_negative_cache(monkeypatch):
    monkeypatch.setenv("TFB_EODHD_FUND_CACHE", "enforce")
    calls = _Calls()
    eng = _engine(_table(), calls)
    rows = [it["input"] for it in REAL_ROWS["noop"]]
    p1 = [_run(eng, r) for r in rows]
    assert calls.n == 2
    assert [_fc(r) for r in p1] == [["fund_cache:neg_mark:empty"], ["fund_cache:neg_mark:nofill"]]
    assert set(de._FUND_NEG_STORE) == {r["symbol"].upper() for r in rows}
    p2 = [_run(eng, r) for r in rows]
    assert calls.n == 2                              # skipped
    assert all(_fc(r) == ["fund_cache:neg:0h"] for r in p2)
    # AW-1 refusal path: a payload declaring a DIFFERENT identity is refused and marked
    sym = rows[0]["symbol"]
    de._FUND_NEG_STORE.clear()
    table = {sym: {"symbol": "ZZZZ.US", "name": "Other Co", "debt_to_equity": 1.0, "free_cash_flow_ttm": 2.0}}
    eng2 = _engine(table, calls)
    out = _run(eng2, rows[0])
    assert "identity_patch_refused:eodhd_fundamentals" in _tags(out)
    assert _fc(out) == ["fund_cache:neg_mark:refused"]
    assert de._fund_cache_stats()["neg_writes"] == 3


# T6 -- page skip: enforce skips, observe only discloses
def test_t6_page_skip(monkeypatch):
    monkeypatch.setenv("TFB_EODHD_FUND_FALLBACK_SKIP_PAGES", "Mutual_Funds,Commodities_FX")
    row = REAL_ROWS["hit"][0]["input"]
    monkeypatch.setenv("TFB_EODHD_FUND_CACHE", "enforce")
    calls = _Calls()
    eng = _engine(_table(), calls)
    out = _run(eng, row, page="Mutual_Funds")
    assert calls.n == 0 and _fc(out) == ["fund_cache:skip_page"]
    out2 = _run(eng, row, page="Global_Markets")
    assert calls.n == 1 and _fc(out2) == []          # a listed page only
    monkeypatch.setenv("TFB_EODHD_FUND_CACHE", "observe")
    calls2 = _Calls()
    eng2 = _engine(_table(), calls2)
    de._FUND_LKG_STORE.clear()
    out3 = _run(eng2, row, page="Commodities_FX")
    assert calls2.n == 1 and _fc(out3) == ["fund_cache:would_skip_page"]


# T7 -- the capture guard: a cache-served row never refreshes its own TTL
def test_t7_capture_guard(monkeypatch):
    monkeypatch.setenv("TFB_EODHD_FUND_CACHE", "enforce")
    calls = _Calls()
    eng = _engine(_table(), calls)
    row = REAL_ROWS["hit"][1]["input"]
    sym = row["symbol"].upper()
    _run(eng, row)
    ts1 = de._FUND_LKG_STORE[sym]["ts"]
    time.sleep(0.01)
    out = _run(eng, row)
    assert _fc(out)[0].startswith("fund_cache:hit:")
    assert de._FUND_LKG_STORE[sym]["ts"] == ts1     # not refreshed
    # direct proof on the helper: an ENFORCE hit tag blocks capture, an observe tag does not
    organic = dict(out); organic["warnings"] = "fund_cache:would_hit:0h:2"
    assert de._fund_cache_row_is_hit(out) is True
    assert de._fund_cache_row_is_hit(organic) is False
    assert de._fund_lkg_capture(sym, out) is False
    assert de._fund_lkg_capture(sym, organic) is True


# T8 -- TTLs: a snapshot older than the cache TTL misses, a mark older than the neg TTL expires
def test_t8_ttl(monkeypatch):
    monkeypatch.setenv("TFB_EODHD_FUND_CACHE", "enforce")
    calls = _Calls()
    eng = _engine(_table(), calls)
    row = REAL_ROWS["hit"][2]["input"]
    sym = row["symbol"].upper()
    _run(eng, row)
    de._FUND_LKG_STORE[sym]["ts"] = time.time() - 25 * 3600.0      # 25h old, LKG TTL 72h
    out = _run(eng, row)
    assert calls.n == 2 and _fc(out) == []                          # miss -> provider again
    assert "eodhd_fundamentals_fallback_applied" in _tags(out)
    de._FUND_NEG_STORE[sym] = time.time() - 169 * 3600.0
    assert de._fund_neg_lookup(sym) is None and sym not in de._FUND_NEG_STORE
    de._FUND_NEG_STORE[sym] = time.time() - 3600.0
    assert 3599.0 < de._fund_neg_lookup(sym) < 3700.0


# T9 -- disclosure surfaces + tag hygiene
def test_t9_disclosure_and_tag_safety(monkeypatch):
    monkeypatch.setenv("TFB_EODHD_FUND_CACHE", "observe")
    gates = de.surface_gate_states()
    assert gates["eodhd_fund_cache"] == "observe"
    assert set(gates["fund_cache_stats"]) == {"hit", "would_hit", "neg", "would_neg",
                                              "skip_page", "would_skip_page", "miss", "neg_writes"}
    monkeypatch.setenv("TFB_EODHD_FUND_CACHE", "enforce")
    monkeypatch.setenv("TFB_EODHD_FUND_FALLBACK_SKIP_PAGES", "Mutual_Funds")
    calls = _Calls()
    eng = _engine(_table(), calls)
    seen = set()
    for it in REAL_ROWS["hit"] + REAL_ROWS["noop"]:
        for page in ("Global_Markets", "Global_Markets", "Mutual_Funds"):
            seen.update(_fc(_run(eng, it["input"], page=page)))
    assert seen
    for t in seen:
        assert not any(f in t for f in FORBIDDEN), t
        assert t.startswith("fund_cache:")
    # idempotent: appending the same tag twice never duplicates it
    r = {"warnings": "fund_cache:neg:0h"}
    de._v573_append_warning(r, "fund_cache:neg:0h")
    assert r["warnings"] == "fund_cache:neg:0h"
