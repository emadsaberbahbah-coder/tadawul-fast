"""P-115 / v5.140.0 closure tests: fundamentals unit contracts + margin coherence.

Golden rows lifted VERBATIM from the 2026-09-10 production My_Portfolio exports:
DDI.US at 08:45:11 (EODHD-fallback path, fraction-unit margin) vs 08:58:11
(Yahoo-enrichment path, percent-unit D/E) -- 34 fields flipped in 13 minutes at
an unchanged price 12.72. These two snapshots are the MR-03 regression fixtures.
Run from repo root: pytest tests/test_fund_unit_sentry.py -q
"""
import copy
import os

import core.data_engine_v2 as de

# ---- production goldens (2026-09-10, DDI.US @ 12.72 unchanged) --------------
EODHD_PATCH = {"profit_margin": 0.3291, "debt_to_equity": 0.037,
               "free_cash_flow_ttm": 146325170.0}
YAHOO_PATCH = {"profit_margin": 32.91, "debt_to_equity": 3.70,
               "free_cash_flow_ttm": 97248496.0}
ROW_0845 = {"symbol": "DDI.US", "pe_ttm": 5.04, "market_cap": 631806336.0,
            "revenue_ttm": 380037581.0, "profit_margin": 0.33}
ROW_0858 = {"symbol": "DDI.US", "pe_ttm": 5.05, "market_cap": 630319770.0,
            "revenue_ttm": 380043008.0, "profit_margin": 32.91}


def setup_function(_fn=None):
    os.environ.pop("TFB_FUND_UNIT_SENTRY", None)


def test_off_is_inert_byte_identical():
    assert de._fund_unit_sentry_mode() == "off"
    p1, p2 = copy.deepcopy(EODHD_PATCH), copy.deepcopy(YAHOO_PATCH)
    assert de._fund_unit_contract_apply(p1, "eodhd_fundamentals", "off") == []
    assert de._fund_unit_contract_apply(p2, "yahoo_fundamentals", "off") == []
    assert p1 == EODHD_PATCH and p2 == YAHOO_PATCH
    r = copy.deepcopy(ROW_0845)
    assert de._fund_coherence_sentry(r, "off") is None
    assert r == ROW_0845


def test_mode_parsing():
    os.environ["TFB_FUND_UNIT_SENTRY"] = "observe"
    assert de._fund_unit_sentry_mode() == "observe"
    os.environ["TFB_FUND_UNIT_SENTRY"] = "1"
    assert de._fund_unit_sentry_mode() == "enforce"
    os.environ["TFB_FUND_UNIT_SENTRY"] = "off"
    assert de._fund_unit_sentry_mode() == "off"
    os.environ.pop("TFB_FUND_UNIT_SENTRY", None)


def test_yahoo_de_percent_to_ratio():
    p = copy.deepcopy(YAHOO_PATCH)
    touched = de._fund_unit_contract_apply(p, "yahoo_fundamentals", "enforce")
    assert touched == ["debt_to_equity"]
    assert abs(p["debt_to_equity"] - 0.037) < 1e-9      # 3.70 -> 0.037
    assert p["profit_margin"] == 32.91                   # never touched


def test_yahoo_sbgi_class():
    p = {"debt_to_equity": 1151.0}                       # SBGI.US live example
    de._fund_unit_contract_apply(p, "yahoo_fundamentals", "enforce")
    assert abs(p["debt_to_equity"] - 11.51) < 1e-9


def test_eodhd_margin_fraction_to_percent():
    p = copy.deepcopy(EODHD_PATCH)
    touched = de._fund_unit_contract_apply(p, "eodhd_fundamentals", "enforce")
    assert touched == ["profit_margin"]
    assert abs(p["profit_margin"] - 32.91) < 1e-9        # 0.3291 -> 32.91
    assert abs(p["debt_to_equity"] - 0.037) < 1e-12      # ratio: untouched


def test_eodhd_percent_points_pass_through():
    p = {"operating_margin": 38.14, "profit_margin": 0.3291}
    touched = de._fund_unit_contract_apply(p, "eodhd_fundamentals", "enforce")
    assert touched == ["profit_margin"]                  # 38.14 > 1.5 bound
    assert p["operating_margin"] == 38.14


def test_observe_tags_without_mutation():
    p = copy.deepcopy(YAHOO_PATCH)
    touched = de._fund_unit_contract_apply(p, "yahoo_fundamentals", "observe")
    assert touched == ["debt_to_equity"] and p == YAHOO_PATCH


def test_coherence_fires_on_0845_shape():
    r = copy.deepcopy(ROW_0845)
    tag = de._fund_coherence_sentry(r, "enforce")
    assert tag == "fund_coherence_quarantined:profit_margin"
    assert r["profit_margin"] is None
    r2 = copy.deepcopy(ROW_0845)
    tag2 = de._fund_coherence_sentry(r2, "observe")
    assert tag2 == "fund_coherence_quarantined:profit_margin:observe"
    assert r2["profit_margin"] == 0.33                   # observe never mutates


def test_coherence_quiet_on_0858_shape():
    assert de._fund_coherence_sentry(copy.deepcopy(ROW_0858), "enforce") is None


def test_coherence_quiet_on_incomplete_and_tiny():
    assert de._fund_coherence_sentry({"pe_ttm": 5.0, "market_cap": 1e9},
                                     "enforce") is None
    r = {"pe_ttm": 100.0, "market_cap": 1e8, "revenue_ttm": 1e9,
         "profit_margin": 90.0}   # implied 0.1pp < 2pp floor -> never judged
    assert de._fund_coherence_sentry(r, "enforce") is None


def test_tags_substring_safe():
    banned = ("cap", "forecast", "target", "roi", "drop", "reject")
    for t in (de._FUND_SENTRY_TAG_PREFIX, de._FUND_SENTRY_QUARANTINE_TAG):
        low = t.lower()
        assert not any(b in low for b in banned), t


def test_wiring_present_in_source():
    import inspect
    src = inspect.getsource(de)
    assert src.count("_fund_unit_contract_apply(") >= 3   # def + 2 call sites
    assert src.count("_fund_coherence_sentry(") >= 2      # def + 1 call site
