"""Configured sukuk cannot inherit the generic Saudi equity-industry label."""
from copy import deepcopy

import pytest

from core import data_engine_v2 as engine


@pytest.fixture(autouse=True)
def fixed_income_policy(monkeypatch):
    monkeypatch.delenv("TFB_ENGINE_SUKUK_CLASS", raising=False)
    monkeypatch.delenv("TFB_FIXED_INCOME_SYMBOLS", raising=False)


def priced_row(industry=None):
    row = {"symbol": "5023.SR", "current_price": 94.0, "eps_ttm": None,
           "asset_class": "Equity", "industry": industry, "position_qty": 100,
           "avg_cost": 100.0, "overall_score": 50.0}
    return row


def test_provider_normalization_no_longer_infers_equity_industry_for_sukuk():
    source = {"symbol": "5023.SR", "current_price": 94.0}
    row = engine._canonicalize_provider_row(source, requested_symbol="5023.SR")
    assert row["industry"] == "Fixed Income / Sukuk"
    assert source == {"symbol": "5023.SR", "current_price": 94.0}


@pytest.mark.parametrize("industry", [None, "", "Listed Equities", " listed equities "])
def test_final_classifier_repairs_existing_generic_fallback_and_is_idempotent(industry):
    row = priced_row(industry)
    engine._classify_recommendation_8tier(row)
    assert row["industry"] == row["asset_class"] == "Fixed Income / Sukuk"
    assert row["recommendation"] == "HOLD"
    assert (row["current_price"], row["position_qty"], row["avg_cost"]) == (94.0, 100, 100.0)
    first = deepcopy(row)
    engine._classify_recommendation_8tier(row)
    assert row == first


def test_specific_provider_industry_survives_classification():
    row = priced_row("Real Estate Financing")
    engine._classify_recommendation_8tier(row)
    assert row["industry"] == "Real Estate Financing"


def test_configured_symbols_and_existing_policy_kill_switch(monkeypatch):
    monkeypatch.setenv("TFB_FIXED_INCOME_SYMBOLS", "9999.SR")
    assert engine._infer_industry_from_symbol("9999.SR") == "Fixed Income / Sukuk"
    assert engine._infer_industry_from_symbol("5023.SR") == "Listed Equities"
    monkeypatch.setenv("TFB_ENGINE_SUKUK_CLASS", "0")
    assert engine._infer_industry_from_symbol("9999.SR") == "Listed Equities"


@pytest.mark.parametrize("industry", [None, "", "Listed Equities", "Real Estate Financing"])
def test_intrinsic_source_identity_stays_bound_across_final_classification(industry):
    row = priced_row(industry)
    before = engine._intrinsic_display_source_signature(row)
    engine._classify_recommendation_8tier(row)
    assert engine._intrinsic_display_source_signature(row) == before


def test_unconfigured_saudi_equity_retains_existing_industry():
    row = {"symbol": "1120.SR", "current_price": 100.0}
    normalized = engine._canonicalize_provider_row(row, requested_symbol="1120.SR")
    assert normalized["industry"] == "Listed Equities"
