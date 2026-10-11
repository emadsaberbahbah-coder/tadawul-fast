"""Raw quote fallback must retain the adapter's publication safeguards."""
from __future__ import annotations

import asyncio
import copy
from types import SimpleNamespace

import pytest
from fastapi import Request

from core import data_engine_v2 as de
from routes import analysis_sheet_rows as route


SYMBOL = "SYNTHETIC.US"


def quote(**changes):
    row = {
        "symbol": SYMBOL, "name": "Synthetic publication fixture",
        "asset_class": "Equity", "currency": "USD", "exchange": "NASDAQ",
        "current_price": 100.0, "forecast_price_12m": 170.0,
        "expected_roi_12m": 0.70, "forecast_source": "provider_target",
        "recommendation": "BUY", "recommendation_detailed": "BUY",
        "investability_status": "INVESTABLE", "final_action": "INVEST",
        "overall_score": 90.0, "risk_score": 30.0,
        "quality_score": 85.0, "forecast_confidence": 0.8,
        "market_cap": 1e9, "pe_ttm": 20.0, "eps_ttm": 5.0,
        "debt_to_equity": 0.5, "free_cash_flow_ttm": 1e8,
        "week_52_low": 80.0, "week_52_high": 175.0,
        "avg_volume_30d": 100000.0, "volatility_30d": 0.2,
        "profit_margin": 20.0,
        "_margin_unit_basis": {"profit_margin": {
            "unit": "percent_points", "value": 20.0, "published": False,
        }},
        "warnings": "acquisition_status:success", "rank_overall": 7,
    }
    row.update(changes)
    return row


@pytest.fixture(autouse=True)
def bounded_environment(monkeypatch):
    monkeypatch.setenv("TFB_FC_TUPLE_COHERENT", "off")
    monkeypatch.setenv("TFB_SCORING_SETTLE", "off")
    monkeypatch.setenv("TFB_ROI12_SOFTCAP_GATE", "off")
    monkeypatch.setenv("TFB_TUPLE_NOISE_RELEASE", "off")
    monkeypatch.setenv("TFB_INVESTABILITY_GATE", "0")
    monkeypatch.setenv("TFB_GATE_RECO_COHERENCE", "0")
    monkeypatch.setenv("TFB_MARGIN_PUBLISH", "off")
    monkeypatch.setenv("TFB_FORECAST_PAIR_COHERENCE", "0")
    monkeypatch.setenv("TFB_PRICE_PRECISION", "off")
    monkeypatch.setenv("TFB_PROVIDER_TARGET_SOFT_CAP", "1")
    monkeypatch.setenv("TFB_PROVIDER_TARGET_SOFT_CAP_BAND", "0.05")
    monkeypatch.setenv("TFB_ANALYSIS_GLOBAL_RANK", "1")
    monkeypatch.setattr(route, "get_settings_cached", lambda: None)
    monkeypatch.setattr(route, "_auth_passed", lambda **kwargs: True)


def run_route(monkeypatch, source, *, page="Global_Markets", adapter_payload=None):
    class Engine:
        async def get_enriched_quotes(self, symbols, **kwargs):
            return [source]  # intentionally reuse a cache-like source dict

    async def unavailable_proxy(**kwargs):
        return None, {"proxy_call_outcome": "raised"}

    async def unavailable_adapter(**kwargs):
        return adapter_payload, "synthetic-adapter", [], "raised" if adapter_payload is None else "success"

    async def engine_binding(request):
        return Engine(), "synthetic-offline"

    monkeypatch.setattr(route, "_proxy_callable", unavailable_proxy)
    monkeypatch.setattr(route, "_call_core_sheet_rows_best_effort", unavailable_adapter)
    monkeypatch.setattr(route, "_get_engine", engine_binding)
    request = Request({"type": "http", "method": "POST", "path": "/analysis/sheet-rows",
                       "headers": [], "query_string": b"",
                       "app": SimpleNamespace(state=SimpleNamespace())})
    return asyncio.run(route._analysis_sheet_rows_impl(
        request, {"page": page, "symbols": [SYMBOL], "limit": 10}, "", True,
        None, None, None, None, "synthetic-request",
    ))


@pytest.mark.parametrize("mode", ["off", "observe", "enforce"])
def test_fallback_applies_provider_target_ceiling_once_with_adapter_parity(monkeypatch, mode):
    monkeypatch.setenv("TFB_ROI12_SOFTCAP_GATE", mode)
    source = quote()
    original = copy.deepcopy(source)
    payload = run_route(monkeypatch, source)
    assert payload["meta"]["dispatch"] == "instrument_mode"
    row = payload["row_objects"][0]
    assert payload["meta"]["publication_boundary"] == "core.data_engine_v2._strict_project_row"
    if mode == "enforce":
        assert 100 < row["forecast_price_12m"] < original["forecast_price_12m"]
        assert row["expected_roi_12m"] == pytest.approx(row["forecast_price_12m"] / 100 - 1, abs=1e-6)
        assert "r12_ceiling:enforce:" in row["warnings"]
    else:
        assert row["forecast_price_12m"] == 170.0
        assert row["expected_roi_12m"] == 0.70
        assert ("r12_ceiling:observe:" in row["warnings"]) == (mode == "observe")
    keys = de.get_sheet_spec("Global_Markets")[1]
    adapter_row = de._strict_project_row(keys, copy.deepcopy(source))
    for key in ("forecast_price_12m", "expected_roi_12m", "profit_margin", "recommendation", "final_action"):
        assert row[key] == adapter_row[key]
    assert source == original
    assert payload["rows_matrix"][0][payload["keys"].index("forecast_price_12m")] == row["forecast_price_12m"]


@pytest.mark.parametrize("mode", ["off", "observe", "enforce"])
def test_fallback_applies_noise_release_and_retains_margin_unit_receipt(monkeypatch, mode):
    monkeypatch.setenv("TFB_TUPLE_NOISE_RELEASE", mode)
    source = quote(forecast_price_12m=110.0, expected_roi_12m=0.10002)
    original = copy.deepcopy(source)
    payload = run_route(monkeypatch, source)
    row = payload["row_objects"][0]
    assert row["profit_margin"] == 0.20  # private receipt must reach presentation
    assert row["expected_roi_12m"] == (0.10 if mode == "enforce" else None)
    if mode != "off":
        assert "tuple_noise:12m:drift:" + mode in row["warnings"]
    assert not any(key.startswith("_") for key in row)
    assert source == original


@pytest.mark.parametrize("page", ["Global_Markets", "Top_10_Investments"])
def test_fallback_cannot_restore_a_seat_or_rank_to_an_enforced_tuple_holdback(monkeypatch, page):
    monkeypatch.setenv("TFB_FC_TUPLE_COHERENT", "enforce")
    source = quote(forecast_price_12m=120.0, expected_roi_12m=-0.2)
    payload = run_route(monkeypatch, source, page=page)
    if page == "Top_10_Investments":
        assert payload["row_objects"] == []
    else:
        row = payload["row_objects"][0]
        assert row["investability_status"] == "BLOCKED"
        assert row["final_action"] == "DO_NOT_INVEST"
        assert row["recommendation"] == "HOLD"
        assert row["overall_score"] is None and row["rank_overall"] == ""
        assert "fctuple_basis_unproven" in row["warnings"]


def test_fallback_rank_guard_preserves_unresolved_research_holdback(monkeypatch):
    source = quote(warnings="settle_unresolved:enforce:pass_limit:p3:ov=90:rc=BUY")
    payload = run_route(monkeypatch, source)
    row = payload["row_objects"][0]
    assert row["rank_overall"] == ""
    assert "settle_unresolved:enforce:" in row["warnings"]


def test_finalization_failure_cannot_publish_the_raw_buy_row(monkeypatch):
    def failed(*args, **kwargs):
        raise RuntimeError("synthetic finalizer unavailable")

    monkeypatch.setattr(de, "_strict_project_row", failed)
    payload = run_route(monkeypatch, quote())
    assert payload["status"] == "partial" and payload["row_objects"] == []
    assert payload["meta"]["dispatch"] == "analysis_sheet_rows_emergency_fallback"


def test_published_adapter_result_does_not_cross_raw_publication_seams_again(monkeypatch):
    monkeypatch.setenv("TFB_ROI12_SOFTCAP_GATE", "enforce")
    source = quote()
    row = de._strict_project_row(de.get_sheet_spec("Global_Markets")[1], copy.deepcopy(source))

    def unexpected(*args, **kwargs):
        raise AssertionError("already published adapter row was finalized again")

    monkeypatch.setattr(de, "_strict_project_row", unexpected)
    payload = run_route(monkeypatch, source, adapter_payload={"status": "success", "row_objects": [row]})
    assert payload["status"] == "success"
    assert payload["row_objects"][0]["forecast_price_12m"] == row["forecast_price_12m"]
