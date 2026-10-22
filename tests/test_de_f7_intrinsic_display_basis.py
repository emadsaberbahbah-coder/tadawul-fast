"""Actual canonical settlement with bounded display copies, no external I/O."""
import asyncio
import copy
import math
import os
from types import SimpleNamespace
from unittest.mock import AsyncMock
import urllib.request

import pytest

from core import data_engine_v2 as de
from core.analysis import portfolio_actions as pa
from core.analysis import opportunity_builder as ob
from tests.portfolio_reconciliation_fixtures import build_certified_portfolio_actions

SOURCE = {
    "symbol": "SYNTH.US", "name": "Synthetic equity", "asset_class": "Equity", "currency": "USD",
    "exchange": "NASDAQ", "country": "USA", "sector": "Technology", "current_price": 100.0,
    "previous_close": 99.0, "open_price": 99.5, "day_high": 101.0, "day_low": 99.0,
    "week_52_high": 120.0, "week_52_low": 80.0, "eps_ttm": 10.0, "pe_ttm": 10.0,
    "pe_forward": 10.0, "pb_ratio": 1.0, "ps_ratio": 2.0, "ev_ebitda": 10.0, "peg_ratio": 1.0,
    "gross_margin": .5, "operating_margin": .2, "profit_margin": .15, "debt_to_equity": .5,
    "revenue_growth_yoy": .1, "dividend_yield": .01, "revenue_ttm": 1e9, "free_cash_flow_ttm": 1e8,
    "volume": 1e6, "avg_volume_10d": 1e6, "avg_volume_30d": 1e6, "market_cap": 1e10,
    "float_shares": 1e8, "rsi_14": 55.0, "volatility_30d": .2, "volatility_90d": .2,
    "max_drawdown_1y": -.1, "beta_5y": 1.0, "var_95_1d": -.02, "sharpe_1y": 1.0,
    "data_provider": "eodhd", "data_quality": "GOOD",
}
MODEL_FIELDS = ("overall_score", "opportunity_score", "valuation_score", "forecast_confidence",
                "expected_roi_1m", "expected_roi_3m", "expected_roi_12m", "forecast_price_1m",
                "forecast_price_3m", "forecast_price_12m", "recommendation", "opportunity_source", "forecast_source")


@pytest.fixture(autouse=True)
def isolated(monkeypatch):
    for key in tuple(os.environ):
        if key.startswith("TFB_"):
            monkeypatch.delenv(key)
    monkeypatch.setenv("TFB_SCORING_SETTLE", "enforce")
    monkeypatch.setenv("TFB_SCORING_SETTLE_MAX_PASSES", "4")
    monkeypatch.setenv("TFB_INTRINSIC_DISPLAY_CAP", "1")
    monkeypatch.setenv("TFB_INTRINSIC_DISPLAY_SOFTCAP", "1")
    monkeypatch.setenv("TFB_ENGINE_TARGET_KLG", "0")
    monkeypatch.setattr(urllib.request, "urlopen", lambda *a, **kw: pytest.fail("unexpected external HTTP"))


def pair(row):
    de._compute_scores_canonical_first(row)
    de._apply_phase_dd_enhancements(row)


def first(source=None):
    row = de._apply_phase_bb_sanity(copy.deepcopy(SOURCE if source is None else source))
    pair(row)
    return row


def raw_reference(monkeypatch):
    """Independent actual-pair reference: display mapping never enters its inputs."""
    with monkeypatch.context() as context:
        context.setenv("TFB_INTRINSIC_DISPLAY_CAP", "0")
        row = first()
        pair(row)
        pair(row)
    return row


@pytest.mark.parametrize("soft", ["0", "1"])
@pytest.mark.parametrize("mode", ["off", "observe", "enforce"])
def test_actual_canonical_passes_read_fixed_raw_basis_and_preserve_mode_semantics(soft, mode, monkeypatch):
    monkeypatch.setenv("TFB_INTRINSIC_DISPLAY_SOFTCAP", soft)
    monkeypatch.setenv("TFB_SCORING_SETTLE", mode)
    reference = raw_reference(monkeypatch)
    row = first()
    before = copy.deepcopy(row)
    result = de._f7_settle_result(row)
    assert result.status == "stable" and result.settled_at == 3 and result.passes_run == 2
    assert row == before
    assert result.row["intrinsic_value"] == (140 if soft == "1" else 135)
    assert result.row["upside_pct"] == (.4 if soft == "1" else .35)
    for field in MODEL_FIELDS:
        assert result.row[field] == reference[field], field
    # Synthetic hand-computed canonical baseline; the old softcap loop instead
    # ended pass 4 at overall67.98/ROI.08959 and withheld a nonconverged row.
    assert result.row["overall_score"] == 72.47 and result.row["expected_roi_12m"] == .3
    out = de._f7_settle_pass(row, "SYNTH.US", "Global_Markets")
    if mode == "off":
        assert out is row and row == before
    elif mode == "observe":
        assert out is row
        assert {key: value for key, value in out.items() if key != "warnings"} == \
               {key: value for key, value in before.items() if key != "warnings"}
        assert "f7_settle:observe:st3:p2" in out["warnings"]
    else:
        assert out is not row and not de._f7_settle_failed(out)
        for field in MODEL_FIELDS:
            assert out[field] == reference[field]
    assert {key: out.get(key) for key in de._F7_SETTLE_SOURCE_FIELDS} == \
           {key: before.get(key) for key in de._F7_SETTLE_SOURCE_FIELDS}


@pytest.mark.parametrize("soft", ["0", "1"])
def test_repeated_cap_and_actual_settlement_are_idempotent(soft, monkeypatch):
    monkeypatch.setenv("TFB_INTRINSIC_DISPLAY_SOFTCAP", soft)
    row = first()
    before = copy.deepcopy(row)
    for _ in range(12):
        de._cap_intrinsic_display(row)
    assert row == before
    settled = de._f7_settle_pass(row)
    again = de._f7_settle_pass(copy.deepcopy(settled))
    assert all(again.get(key) == settled.get(key) for key in MODEL_FIELDS)
    assert again["intrinsic_value"] == settled["intrinsic_value"]


def test_cap_off_and_unmarked_settlement_off_keep_pristine_rows_untouched(monkeypatch):
    monkeypatch.setenv("TFB_INTRINSIC_DISPLAY_CAP", "0")
    row = first()
    before = copy.deepcopy(row)
    de._cap_intrinsic_display(row)
    assert row == before and de._INTRINSIC_DISPLAY_BASIS_KEY not in row
    monkeypatch.setenv("TFB_INTRINSIC_DISPLAY_CAP", "1")
    monkeypatch.setenv("TFB_SCORING_SETTLE", "off")
    assert de._f7_settle_pass(row) is row and row == before


def test_known_raw_receipt_renews_current_policy_using_raw_value_not_last_display(monkeypatch):
    row = {**SOURCE, "intrinsic_value": 180.0, "upside_pct": .8}
    de._cap_intrinsic_display(row)
    monkeypatch.setenv("TFB_INTRINSIC_DISPLAY_MAX_PCT", "40")
    monkeypatch.setenv("TFB_INTRINSIC_DISPLAY_SOFTCAP_BAND", "4")
    expected_pct = 40 + 4 * (1 - math.exp(-(80-40)/4))
    de._cap_intrinsic_display(row)
    assert row["intrinsic_value"] == round(100*(1+expected_pct/100), 4)
    assert row["upside_pct"] == round(expected_pct/100, 6)
    assert row[de._INTRINSIC_DISPLAY_BASIS_KEY]["raw_intrinsic"] == 180
    before = copy.deepcopy(row)
    de._cap_intrinsic_display(row)
    assert row == before


def test_coupled_invalid_display_receipt_cannot_bypass_the_current_cap():
    row = first()
    row.pop(de._INTRINSIC_DISPLAY_BASIS_KEY)
    de._cap_intrinsic_display(row)
    receipt = row[de._INTRINSIC_DISPLAY_BASIS_KEY]
    row.update(intrinsic_value=300.0, upside_pct=2.0)
    receipt.update(display_intrinsic=300.0, display_upside=2.0)
    de._cap_intrinsic_display(row)
    assert row["intrinsic_value"] <= 140 and row["upside_pct"] <= .4
    assert de._intrinsic_display_basis(row) is None and receipt.get("unproven") is True
    result = de._f7_settle_result(row)
    assert result.status == "error" and result.reason == "intrinsic_model_basis_unproven"


@pytest.mark.parametrize("field,value", [("version", True), ("raw_intrinsic", True), ("raw_upside", float("inf")),
                                         ("display_intrinsic", float("nan")), ("source_signature", "wrong"),
                                         ("unproven", True)])
def test_malformed_or_unproven_model_receipt_never_certifies_a_raw_basis(field, value):
    row = first()
    row[de._INTRINSIC_DISPLAY_BASIS_KEY][field] = value
    result = de._f7_settle_result(row)
    assert result.status == "error" and result.reason == "intrinsic_model_basis_unproven"


@pytest.mark.parametrize("change", [{"current_price": 90}, {"currency": "SAR"}, {"symbol": "OTHER.US"},
                                    {"sector": "Utilities"}, {"eps_ttm": 11}, {"pb_ratio": 2},
                                    {"target_mean_price": 180}, {"asset_class": "Fund"},
                                    {"name": "Different equity"}, {"exchange": "NYSE"}, {"country": "CA"},
                                    {"industry": "Synthetic new industry"}, {"data_provider": "yahoo_chart"},
                                    {"intrinsic_value": 160}, {"upside_pct": .38}])
def test_changed_source_or_visible_value_cannot_resurrect_an_old_raw_basis(change):
    row = first()
    row.update(change)
    result = de._f7_settle_result(row)
    assert result.status == "error" and result.reason == "intrinsic_model_basis_unproven" and result.row is None
    out = de._f7_settle_pass(row)
    assert out["overall_score"] is None and out["final_action"] == "DO_NOT_INVEST"
    assert not de._top10_row_is_eligible(out)
    assert out["intrinsic_value"] <= row["current_price"] * 1.4
    assert de._intrinsic_display_basis(out) is None
    before = copy.deepcopy(out)
    de._cap_intrinsic_display(out)
    assert out == before


@pytest.mark.parametrize("change", [{"current_price": 90}, {"eps_ttm": 5}, {"sector": "Utilities"}, {"pb_ratio": 3},
                                    {"target_mean_price": 180}, {"asset_class": "Fund"},
                                    {"name": "Different equity"}, {"exchange": "NYSE"}, {"country": "CA"},
                                    {"industry": "Synthetic new industry"}, {"data_provider": "yahoo_chart"}])
def test_new_raw_factory_inputs_mint_their_own_receipt(change):
    source = {**SOURCE, **change}
    row = first(source)
    result = de._f7_settle_result(row)
    assert result.status == "stable" and result.row is not None
    assert de._intrinsic_display_basis(result.row) is not None
    assert result.row[de._INTRINSIC_DISPLAY_BASIS_KEY]["source_signature"] != \
           first()[de._INTRINSIC_DISPLAY_BASIS_KEY]["source_signature"]


@pytest.mark.parametrize("mode", ["off", "observe", "enforce"])
def test_legacy_unwitnessed_display_is_bounded_but_never_minted_as_raw_model(mode, monkeypatch):
    row = first()
    row.pop(de._INTRINSIC_DISPLAY_BASIS_KEY)
    row["current_price"] = 90
    monkeypatch.setenv("TFB_SCORING_SETTLE", mode)
    de._cap_intrinsic_display(row)
    assert row["intrinsic_value"] <= 126
    assert row[de._INTRINSIC_DISPLAY_BASIS_KEY]["unproven"] is True
    assert "raw_intrinsic" not in row[de._INTRINSIC_DISPLAY_BASIS_KEY]
    before = copy.deepcopy(row)
    de._cap_intrinsic_display(row)
    assert row == before
    result = de._f7_settle_result(row)
    assert result.status == "error" and result.reason == "intrinsic_model_basis_unproven"
    out = de._f7_settle_pass(row)
    if mode == "off":
        assert row == before
    else:
        assert out["overall_score"] is None and out["final_action"] == "DO_NOT_INVEST"


@pytest.mark.parametrize("mode", ["off", "observe", "enforce"])
@pytest.mark.parametrize("rec,action", [("BUY", "INVEST"), ("SELL", "EXIT")])
def test_known_failure_stays_withheld_and_current_display_caps_preserve_exits(mode, rec, action, monkeypatch):
    row = first()
    row.update(current_price=90, recommendation=rec, recommendation_detailed=rec, final_action=action,
               warnings=row["warnings"] + "; f7_settle_failed:non_converged; acquisition_status:success", rank_overall=1)
    monkeypatch.setenv("TFB_SCORING_SETTLE", mode)
    out = de._f7_settle_pass(row)
    assert out["current_price"] == 90 and "acquisition_status:success" in out["warnings"]
    assert out["overall_score"] is None and out["rank_overall"] is None and de._f7_settle_failed(out)
    assert out["final_action"] == ("EXIT" if action == "EXIT" else "DO_NOT_INVEST")
    assert out["intrinsic_value"] <= 126 and not de._top10_row_is_eligible(out)
    headers, keys = de.get_sheet_spec("Global_Markets")
    projected = de._strict_project_row(keys, out)
    assert len(projected) == 115 and de._INTRINSIC_DISPLAY_BASIS_KEY not in projected
    assert projected["overall_score"] is None and de._f7_settle_failed(projected)


@pytest.mark.parametrize("fixed_income", [False, True])
def test_engine_owned_hold_never_becomes_an_analyst_rating_or_unpriced_funding(fixed_income, monkeypatch):
    source = copy.deepcopy(SOURCE)
    source["current_price"] = None
    if fixed_income:
        monkeypatch.setenv("TFB_FIXED_INCOME_SYMBOLS", "SYNTH.US")
    row = first(source)
    assert row["recommendation_source"] == ("fixed_income_sukuk" if fixed_income else "price_unavailable")
    assert row.get("provider_rating") in (None, "")
    result = de._f7_settle_result(row)
    assert result.status == "stable" and result.row.get("provider_rating") in (None, "")
    out = de._f7_settle_pass(row)
    de._apply_investability_gate(out)
    assert out["investability_status"] == "BLOCKED" and out["final_action"] == "DO_NOT_INVEST"
    assert not de._top10_row_is_eligible(out)
    # Actual held quantity/cost with a carried exposure. The default-off cost
    # quarantine must not be required to prevent a quote-free ADD display.
    out.update(position_qty=10, avg_cost=30, position_value=400, target_weight=90)
    de._compute_portfolio_fields([out, {"symbol": "OTHER.US", "position_value": 3600, "currency": "USD"}])
    assert out["action_flag"] != "ADD" and out["decision"] != "ADD"
    funded = build_certified_portfolio_actions(pa, [out], {"cash_available_sar": 100000}, {"USD": 3.75})
    assert funded["meta"]["execution_ready"] is False
    assert funded["meta"]["input_certification"]["funding_eligible"] is False
    assert funded["actions"][0]["action"] == "BLOCK"
    for key in ("deployable_sar", "adds_funded_sar", "proceeds_pending_sar", "capital_unallocated_sar"):
        assert funded["kpis"][key] == 0
    monkeypatch.setenv("TFB_OPP_ENABLED", "1")
    board = ob.build_opportunity_payload([out], portfolio={"cash_available_sar": 100000,
        "holdings": [out]}, fx_rates={"USD": 3.75})
    assert board["selected"] == [] and board["meta"]["execution_ready"] is False
    assert board["meta"]["input_certification"]["funding_eligible"] is False
    assert all(board["kpis"][key] == 0 for key in ("deployable_sar", "capital_unallocated_sar",
                                                 "expected_gain_12m_sar"))


@pytest.mark.parametrize("price", [None, 0, -1, True, float("nan"), float("inf")])
@pytest.mark.parametrize("fx_complete", [True, False])
def test_final_portfolio_guard_withholds_only_quote_free_add_and_preserves_facts(price, fx_complete):
    row = {"symbol": "SYNTH.US", "currency": "USD" if fx_complete else "UNKNOWN",
           "current_price": price, "position_qty": 10, "avg_cost": 30,
           "position_value": 400, "target_weight": 90, "overall_score": 80,
           "recommendation": "BUY", "expected_roi_12m": .2, "risk_bucket": "LOW"}
    de._compute_portfolio_fields([row, {"symbol": "OTHER.US", "currency": "USD",
                                       "current_price": 100, "position_value": 3600}])
    assert row["action_flag"] == "HOLD" and row["decision"] == "HOLD"
    assert row["position_qty"] == 10 and row["avg_cost"] == 30 and row["target_weight"] == 90
    if price is None:
        assert row["position_value"] == 400
        assert row["actual_weight"] == (10 if fx_complete else None)
        assert row["weight_gap"] == (80 if fx_complete else None)


def test_valid_price_still_supports_existing_add_policy_and_price_alias():
    for price_fields in ({"current_price": 40}, {"price": 40}):
        row = {"symbol": "SYNTH.US", "currency": "USD", "position_qty": 10,
               "avg_cost": 30, "target_weight": 90, "overall_score": 80,
               "recommendation": "BUY", "expected_roi_12m": .2, "risk_bucket": "LOW", **price_fields}
        de._compute_portfolio_fields([row, {"symbol": "OTHER.US", "currency": "USD",
                                           "current_price": 100, "position_value": 3600}])
        assert row["action_flag"] == "ADD" and row["decision"] == "ADD"
        assert row["actual_weight"] == 10 and row["weight_gap"] == 80


@pytest.mark.parametrize("fx_complete", [True, False])
def test_missing_price_guard_preserves_signal_sell_and_drift_reduction(fx_complete):
    row = {"symbol": "SYNTH.US", "currency": "USD" if fx_complete else "UNKNOWN",
           "current_price": None, "position_qty": 10, "avg_cost": 30,
           "position_value": 3600, "target_weight": 10, "recommendation": "SELL"}
    de._compute_portfolio_fields([row, {"symbol": "OTHER.US", "currency": "USD",
                                       "current_price": 100, "position_value": 400}])
    assert row["decision"] == "SELL"
    assert row["action_flag"] == ("REDUCE" if fx_complete else "HOLD")
    for action in ("SELL", "REDUCE", "TRIM", "EXIT"):
        row.update(action_flag=action, decision=action)
        de._portfolio_missing_price_holdback(row)
        assert row["action_flag"] == row["decision"] == action


@pytest.mark.parametrize("fixed_income", [False, True])
def test_real_upstream_rating_remains_captured_when_engine_writes_a_neutral_hold(fixed_income, monkeypatch):
    if fixed_income:
        monkeypatch.setenv("TFB_FIXED_INCOME_SYMBOLS", "SYNTH.US")
    row = first({**SOURCE, "current_price": None, "recommendation": "BUY", "recommendation_source": "analyst"})
    assert row["provider_rating"] == "BUY"
    result = de._f7_settle_result(row)
    assert result.status == "stable" and result.row["provider_rating"] == "BUY"
    de._apply_investability_gate(result.row)
    assert result.row["final_action"] == "DO_NOT_INVEST"


@pytest.mark.parametrize("mode", ["observe", "enforce"])
def test_fresh_priced_fixed_income_classification_does_not_invalidate_its_own_model_receipt(mode, monkeypatch):
    monkeypatch.setenv("TFB_SCORING_SETTLE", mode)
    monkeypatch.setenv("TFB_FIXED_INCOME_SYMBOLS", "SYNTH.US")
    row = first()
    assert row["asset_class"] == de._FIXED_INCOME_ASSET_CLASS
    result = de._f7_settle_result(row)
    assert result.status == "stable" and result.row.get("provider_rating") in (None, "")
    reference = raw_reference(monkeypatch)
    assert all(result.row[field] == reference[field] for field in MODEL_FIELDS)
    out = de._f7_settle_pass(row)
    de._apply_investability_gate(out)
    assert not de._f7_settle_failed(out) and out["recommendation_source"] == "fixed_income_sukuk"
    assert out["recommendation"] == "HOLD" and out["final_action"] != "INVEST"


@pytest.mark.parametrize("mode", ["observe", "enforce"])
def test_changed_fixed_income_identity_cannot_rebind_an_existing_equity_receipt(mode, monkeypatch):
    monkeypatch.setenv("TFB_SCORING_SETTLE", mode)
    row = first()
    monkeypatch.setenv("TFB_FIXED_INCOME_SYMBOLS", "SYNTH.US")
    result = de._f7_settle_result(row)
    assert result.status == "error" and result.reason == "intrinsic_model_basis_unproven"
    out = de._f7_settle_pass(row)
    assert de._f7_settle_failed(out) and out["overall_score"] is None
    assert out["final_action"] == "DO_NOT_INVEST"


@pytest.mark.parametrize("mode", ["observe", "enforce"])
def test_actual_provider_factory_rebuilds_after_old_failed_cache_without_inheriting_snapshot(mode, monkeypatch):
    monkeypatch.setenv("TFB_SCORING_SETTLE", mode)
    monkeypatch.setenv("TFB_ENGINE_XPROVIDER_VERIFY", "0")
    monkeypatch.setenv("TFB_ENGINE_BAR_AGE_GATE", "0")
    monkeypatch.setenv("TFB_ENGINE_FUND_LKG", "0")
    source = copy.deepcopy(SOURCE)
    source["price_bar_ts"] = de._now_utc_iso()
    async def unchanged(row, *args):
        return row
    def factory():
        engine = de.DataEngineV5(settings=SimpleNamespace(), providers=["eodhd"])
        provider = AsyncMock(side_effect=lambda *a, **kw: copy.deepcopy(source))
        monkeypatch.setattr(engine, "_providers_for_instrument", lambda *args: ["eodhd"])
        monkeypatch.setattr(engine, "_fetch_patch", provider)
        monkeypatch.setattr(engine, "_apply_yahoo_enrichment_pass", unchanged)
        monkeypatch.setattr(engine, "_apply_eodhd_fundamentals_fallback", unchanged)
        monkeypatch.setattr(engine, "_get_history_patch_best_effort", AsyncMock(return_value={}))
        monkeypatch.setattr(engine, "_apply_news_veto", AsyncMock(return_value=None))
        return engine, provider
    engine, provider = factory()

    async def run():
        old = first()
        de._f7_settle_holdback(old, de.F7SettlementResult("non_converged", reason="pass_limit", passes_run=3))
        context, _ = engine._resolve_quote_page_context("SYNTH.US", "Global_Markets")
        key = de._make_cache_key("SYNTH.US", context, engine._provider_profile_key())
        await engine._cache.set(key, old)
        await engine._store_sheet_snapshot("Global_Markets", [old])
        cached = await engine._get_enriched_quote_impl("SYNTH.US", "Global_Markets")
        assert de._f7_settle_failed(cached) and cached["overall_score"] is None
        assert provider.await_count == 0
        await engine._cache.invalidate(key)
        fresh = await engine._get_enriched_quote_impl("SYNTH.US", "Global_Markets")
        assert provider.await_count == 1
        assert not de._f7_settle_failed(fresh) and fresh["overall_score"] is not None
        assert "acquisition_status:success" in de._mpc_warning_parts(fresh)
        assert fresh["current_price"] == 100 and de._intrinsic_display_basis(fresh) is not None
        assert fresh["intrinsic_value"] <= 140
        if mode == "enforce":
            # Both controls pass through real provider canonicalization and
            # the actual factory; only the presentation cap differs.
            with monkeypatch.context() as context:
                context.setenv("TFB_INTRINSIC_DISPLAY_CAP", "0")
                reference_engine, reference_provider = factory()
                reference = await reference_engine._get_enriched_quote_impl("SYNTH.US", "Global_Markets")
                assert reference_provider.await_count == 1
            for field in MODEL_FIELDS:
                assert fresh[field] == reference[field], field
        headers, keys = de.get_sheet_spec("Global_Markets")
        projected = de._strict_project_row(keys, fresh)
        assert len(projected) == 115 and de._INTRINSIC_DISPLAY_BASIS_KEY not in projected
        assert not de._f7_settle_failed(projected)
        assert "Scoring settlement failed" not in str(projected.get("block_reason"))
    asyncio.run(run())
