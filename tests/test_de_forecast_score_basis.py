"""Synthetic offline regressions through the real factory and final row exits."""
import asyncio
import copy
import time
from types import SimpleNamespace

import pytest

from core import data_engine_v2 as de


SYMBOL = "SYNTHETIC.US"
FACTS = {
    "symbol": SYMBOL, "name": "Synthetic contract fixture", "asset_class": "Equity",
    "current_price": 100.0, "previous_close": 99.0, "currency": "USD", "exchange": "NASDAQ",
    "forecast_price_12m": 120.0, "forecast_price_3m": 105.0, "forecast_price_1m": 102.0,
    "expected_roi_12m": -0.20, "expected_roi_3m": -0.05, "expected_roi_1m": -0.02,
    "forecast_source": "provider_target", "target_mean_price": 120.0,
    "market_cap": 1_000_000_000.0, "eps_ttm": 5.0, "pe_ttm": 20.0,
    "pe_forward": 18.0, "pb_ratio": 2.0, "debt_to_equity": 0.5,
    "free_cash_flow_ttm": 100_000_000.0, "revenue_ttm": 500_000_000.0,
    "profit_margin": 0.2, "operating_margin": 0.25, "gross_margin": 0.4,
    "rsi_14": 50.0, "risk_score": 30.0, "risk_bucket": "MEDIUM",
    "volatility_30d": 0.2, "volatility_90d": 0.2, "max_drawdown_1y": -0.1,
    "avg_volume_10d": 10000.0, "avg_volume_30d": 10000.0,
    "week_52_low": 80.0, "week_52_high": 130.0, "data_provider": "yahoo_chart",
}
ACQUISITION = (
    "acquisition_status:success; acquisition_acquired_at:2026-10-08T06:00:00+00:00; "
    "acquisition_provider:yahoo_chart; acquisition_quote_asof:2026-10-07T20:00:00+00:00"
)


@pytest.fixture(autouse=True)
def bounded_environment(monkeypatch):
    monkeypatch.setenv("TFB_SCORING_SETTLE", "enforce")
    monkeypatch.setenv("TFB_FC_TUPLE_COHERENT", "enforce")
    monkeypatch.setenv("TFB_FORECAST_PAIR_COHERENCE", "0")
    monkeypatch.setenv("TFB_ENGINE_TARGET_KLG", "0")
    monkeypatch.setenv("TFB_ENGINE_FUND_LKG", "0")
    monkeypatch.setenv("TFB_MARGIN_PUBLISH", "off")
    monkeypatch.setenv("TFB_ENGINE_BAR_AGE_GATE", "0")
    monkeypatch.setenv("TFB_ENGINE_XPROVIDER_VERIFY", "0")


def _engine(monkeypatch, quote=None):
    engine = de.DataEngineV5(settings=SimpleNamespace(), providers=["yahoo_chart"])
    calls = []

    async def fetch(provider, symbol, page=""):
        calls.append((provider, symbol))
        return copy.deepcopy(FACTS if quote is None else quote)

    async def unchanged(row, *_args):
        return row

    async def empty(*_args, **_kwargs):
        return {}

    monkeypatch.setattr(engine, "_fetch_patch", fetch)
    monkeypatch.setattr(engine, "_apply_yahoo_enrichment_pass", unchanged)
    monkeypatch.setattr(engine, "_apply_eodhd_fundamentals_fallback", unchanged)
    monkeypatch.setattr(engine, "_get_history_patch_best_effort", empty)
    monkeypatch.setattr(engine, "_apply_news_veto", empty)
    return engine, calls


@pytest.mark.parametrize("tuple_mode", ["off", "observe", "enforce"])
@pytest.mark.parametrize("settle_mode", ["off", "observe", "enforce"])
@pytest.mark.parametrize("display_pair", ["0", "1"])
def test_actual_factory_scores_on_the_authorized_final_tuple_basis(
    monkeypatch, tuple_mode, settle_mode, display_pair
):
    monkeypatch.setenv("TFB_FC_TUPLE_COHERENT", tuple_mode)
    monkeypatch.setenv("TFB_SCORING_SETTLE", settle_mode)
    monkeypatch.setenv("TFB_FORECAST_PAIR_COHERENCE", display_pair)
    score_inputs = []
    scorer = de._scoring_compute_scores

    def observe_scorer(row):
        score_inputs.append(copy.deepcopy(row))
        return scorer(row)

    monkeypatch.setattr(de, "_scoring_compute_scores", observe_scorer)
    engine, calls = _engine(monkeypatch)
    row = asyncio.run(engine.get_enriched_quote_dict(SYMBOL, "Global_Markets"))
    assert calls == [("yahoo_chart", SYMBOL)]
    expected_roi = 0.20 if tuple_mode == "enforce" and display_pair == "0" else -0.20
    assert score_inputs[0]["expected_roi_12m"] == pytest.approx(expected_roi)
    assert row["expected_roi_12m"] == pytest.approx(expected_roi)
    if settle_mode == "off":
        assert len(score_inputs) == 1  # no hidden additional settlement pass
    if tuple_mode == "enforce":
        for seen in score_inputs:
            assert seen["expected_roi_12m"] == pytest.approx((seen["forecast_price_12m"] - 100) / 100)
        assert de._FCT_BASIS_TAG not in de._mpc_warning_parts(row)
    assert row["current_price"] == 100.0
    assert row["currency"] == "USD"
    assert "acquisition_status:success" in de._mpc_warning_parts(row)


def _old_scored_row(**changes):
    row = dict(FACTS, overall_score=90.0, overall_score_raw=92.0,
               valuation_score=85.0, value_score=85.0, opportunity_score=90.0,
               conviction_score=88.0, rank_overall=1, top10_rank=1,
               value_view="CHEAP", top_factors="Old modeled return", score=90.0,
               compositeScore=90.0, quality_score=80.0, growth_score=75.0,
               momentum_score=60.0, confidence_score=85.0, forecast_confidence=0.85,
               recommendation="BUY", recommendation_detailed="BUY",
               recommendation_reason="BUY: old model", recommendation_source="engine",
               investability_status="INVESTABLE", final_action="INVEST", block_reason="",
               signal="BUY", trend_12m="DOWN", expected_return_12m=-0.2,
               warnings=ACQUISITION)
    row.update(changes)
    return row


def _assert_held(row, quality=80.0, risk=30.0):
    assert de._FCT_BASIS_TAG in de._mpc_warning_parts(row)
    assert row["recommendation"] == row["recommendation_detailed"] == "HOLD"
    assert row["investability_status"] == "BLOCKED"
    assert row["final_action"] == "DO_NOT_INVEST"
    assert row["expected_roi_12m"] == pytest.approx(0.2)
    for key in de._FCT_ROI_SCORE_FIELDS:
        if key in row:
            assert row[key] is None, key
    assert row["current_price"] == 100.0
    assert row["quality_score"] == quality
    assert row["risk_score"] == risk
    assert "acquisition_status:success" in de._mpc_warning_parts(row)


@pytest.mark.parametrize("seam", ["projection", "page_rows", "direct_top10"])
@pytest.mark.parametrize("guard_value", ["0", "1"])
@pytest.mark.parametrize("tuple_mode", ["off", "observe", "enforce"])
@pytest.mark.parametrize("settle_mode", ["off", "observe", "enforce"])
@pytest.mark.parametrize("display_pair", ["0", "1"])
def test_all_three_publication_paths_withhold_unknown_basis_even_with_guards_disabled(
    monkeypatch, seam, guard_value, tuple_mode, settle_mode, display_pair
):
    monkeypatch.setenv("TFB_INVESTABILITY_GATE", guard_value)
    monkeypatch.setenv("TFB_GATE_RECO_COHERENCE", guard_value)
    monkeypatch.setenv("TFB_FC_TUPLE_COHERENT", tuple_mode)
    monkeypatch.setenv("TFB_SCORING_SETTLE", settle_mode)
    monkeypatch.setenv("TFB_FORECAST_PAIR_COHERENCE", display_pair)
    row = _old_scored_row()

    def assert_basis(output):
        if tuple_mode == "enforce":
            _assert_held(output)
        else:
            assert output["overall_score"] == 90.0
            assert output["expected_roi_12m"] == -0.2
            assert de._FCT_BASIS_TAG not in de._mpc_warning_parts(output)

    if seam == "projection":
        keys = tuple(row)
        projected = de._strict_project_row(keys, row)
        assert_basis(projected)
        if tuple_mode == "enforce":
            assert row["score"] is None and row["compositeScore"] is None
            assert row["expected_return_12m"] is None
        first = copy.deepcopy(projected)
        second = de._strict_project_row(keys, row)
        if tuple_mode == "enforce":
            assert second == first
        else:
            # Legacy negative-ROI BUY -> HOLD changes its block-reason wording
            # on the next pass. This repair preserves that off/observe policy.
            assert_basis(second)
        return
    engine, calls = _engine(monkeypatch)

    async def symbols(*_args, **_kwargs):
        return [SYMBOL]

    async def quotes(*_args, **_kwargs):
        return [row]

    monkeypatch.setattr(engine, "list_symbols_for_page", symbols)
    monkeypatch.setattr(engine, "get_enriched_quotes", quotes)
    if seam == "page_rows":
        returned = asyncio.run(engine.get_page_rows("Global_Markets"))
        assert len(returned) == 1
        assert_basis(returned[0])
    else:
        monkeypatch.setattr(de, "_extract_requested_symbols_from_body", lambda *_a, **_k: [SYMBOL])
        returned = asyncio.run(engine.get_sheet_rows("Top_10_Investments", body={"symbols": [SYMBOL]}))
        assert returned["rows"] == []  # the held candidate cannot take a board seat
        assert_basis(row)
    assert calls == []


@pytest.mark.parametrize("tuple_mode", ["off", "observe", "enforce"])
@pytest.mark.parametrize("settle_mode", ["off", "observe", "enforce"])
@pytest.mark.parametrize("display_pair", ["0", "1"])
def test_cached_origin_is_withheld_idempotently_without_provider_calls(monkeypatch, tuple_mode, settle_mode, display_pair):
    monkeypatch.setenv("TFB_FC_TUPLE_COHERENT", tuple_mode)
    monkeypatch.setenv("TFB_SCORING_SETTLE", settle_mode)
    monkeypatch.setenv("TFB_FORECAST_PAIR_COHERENCE", display_pair)
    engine, calls = _engine(monkeypatch)
    row = _old_scored_row(warnings=ACQUISITION.split("; "))

    async def run():
        page, _ = engine._resolve_quote_page_context(SYMBOL, "Global_Markets")
        key = de._make_cache_key(SYMBOL, page, engine._provider_profile_key())
        await engine._cache.set(key, row)
        first = copy.deepcopy(await engine.get_enriched_quote_dict(SYMBOL, "Global_Markets"))
        second = await engine.get_enriched_quote_dict(SYMBOL, "Global_Markets")
        assert second == first
        return second

    output = asyncio.run(run())
    if tuple_mode == "enforce" and display_pair == "0":
        _assert_held(output)
    else:
        assert output["overall_score"] == 90.0
        assert output["expected_roi_12m"] == -0.2
        assert de._FCT_BASIS_TAG not in de._mpc_warning_parts(output)
    assert calls == []


@pytest.mark.parametrize("recommendation,action", [("SELL", "EXIT"), ("SELL", "SELL"), ("AVOID", "DO_NOT_INVEST")])
def test_higher_priority_exits_and_veto_reason_survive_publication(monkeypatch, recommendation, action):
    row = _old_scored_row(recommendation=recommendation, recommendation_detailed=recommendation,
                          final_action=action, block_reason="Shariah veto: explicit prior policy")
    de._strict_project_row(tuple(row), row)
    assert row["recommendation"] == row["recommendation_detailed"] == recommendation
    assert row["final_action"] == action
    assert row["block_reason"] == "Shariah veto: explicit prior policy"
    assert row["overall_score"] is None
    assert row["quality_score"] == 80.0 and row["risk_score"] == 30.0
    assert "acquisition_status:success" in de._mpc_warning_parts(row)


@pytest.mark.parametrize("mode", ["off", "observe"])
def test_off_observe_leave_score_and_decision_basis_untouched(monkeypatch, mode):
    monkeypatch.setenv("TFB_FC_TUPLE_COHERENT", mode)
    row = _old_scored_row()
    before = copy.deepcopy(row)
    de._fc_tuple_finalize(row)
    assert row == before
    de._fc_tuple_coherence(row)
    de._fc_tuple_holdback(row, tuple(before.get(k) for _h, _fp, k in de._FCT_LEGS))
    assert {k: v for k, v in row.items() if k != "warnings"} == {k: v for k, v in before.items() if k != "warnings"}
    assert de._FCT_BASIS_TAG not in de._mpc_warning_parts(row)


def test_direct_scoring_call_never_releases_a_held_cached_basis():
    row = _old_scored_row(warnings=ACQUISITION + "; " + de._FCT_BASIS_TAG)
    de._compute_scores_canonical_first(row)
    quality, risk = row["quality_score"], row["risk_score"]
    assert de._FCT_BASIS_TAG in de._mpc_warning_parts(row)
    de._fc_tuple_finalize(row)
    _assert_held(row, quality, risk)


def test_buy_family_shariah_veto_is_preserved_when_basis_is_withheld():
    row = _old_scored_row(block_reason="Shariah veto: explicit prior policy", final_action="DO_NOT_INVEST")
    de._strict_project_row(tuple(row), row)
    _assert_held(row)
    assert row["block_reason"] == "Shariah veto: explicit prior policy"
    assert row["recommendation_reason"] == "HOLD: Shariah veto: explicit prior policy"


def test_explicit_exit_action_cannot_leave_a_stale_buy_eligible():
    row = _old_scored_row(final_action="EXIT", block_reason="Explicit exit policy")
    de._strict_project_row(tuple(row), row)
    assert row["final_action"] == "EXIT"
    assert row["recommendation"] == "HOLD"
    assert row["investability_status"] == "WATCHLIST"
    assert row["block_reason"] == "Explicit exit policy"
    assert not de._top10_row_is_eligible(row)


def test_late_basis_holdback_preserves_exit_through_actual_page_surface_invariants(monkeypatch):
    monkeypatch.setenv("TFB_SURFACE_BLOCKED_INVARIANT", "1")
    row = _old_scored_row(recommendation="SELL", recommendation_detailed="SELL",
                          final_action="EXIT", block_reason="Explicit exit policy")
    engine, _calls = _engine(monkeypatch)

    async def symbols(*_args, **_kwargs):
        return [SYMBOL]

    async def quotes(*_args, **_kwargs):
        return [row]

    monkeypatch.setattr(engine, "list_symbols_for_page", symbols)
    monkeypatch.setattr(engine, "get_enriched_quotes", quotes)
    returned = asyncio.run(engine.get_page_rows("Global_Markets"))[0]
    assert returned["final_action"] == "EXIT"
    assert returned["recommendation"] == "SELL"
    assert returned["investability_status"] == "WATCHLIST"
    assert returned["overall_score"] is None


@pytest.mark.parametrize("unit_proof", [True, False], ids=["value_bound_published", "legacy_unitless_unresolved"])
def test_cached_published_margin_is_never_rescored_or_rescaled(monkeypatch, unit_proof):
    monkeypatch.setenv("TFB_MARGIN_PUBLISH", "enforce")
    engine, calls = _engine(monkeypatch)
    row = _old_scored_row(profit_margin=0.009, warnings=ACQUISITION + "; margin_publish:profit_margin:pts")
    if unit_proof:
        row["_margin_unit_basis"] = {
            "profit_margin": {"unit": "fraction", "value": 0.009, "published": True},
        }

    async def run():
        page, _ = engine._resolve_quote_page_context(SYMBOL, "Global_Markets")
        await engine._cache.set(de._make_cache_key(SYMBOL, page, engine._provider_profile_key()), row)
        cached = await engine.get_enriched_quote_dict(SYMBOL, "Global_Markets")
        return de._strict_project_row(tuple(cached), cached)

    output = asyncio.run(run())
    _assert_held(output)
    assert output["profit_margin"] == (0.009 if unit_proof else None)
    if not unit_proof:
        assert "margin_publish:profit_margin:unresolved" in de._mpc_warning_parts(output)
    assert calls == []


@pytest.mark.parametrize("receipt,error,released", [
    ("", "", True), ("margin_publish:profit_margin:pts:observe", "", True),
    ("margin_publish:profit_margin:pts", "", False), ("", "fetch_failed", False),
])
def test_only_actual_clean_factory_acquisition_can_release_held_basis(monkeypatch, receipt, error, released):
    quote = dict(FACTS, warnings="; ".join(p for p in (de._FCT_BASIS_TAG, receipt) if p))
    if error:
        quote["error"] = error
    engine, calls = _engine(monkeypatch, quote)
    row = asyncio.run(engine.get_enriched_quote_dict(SYMBOL, "Global_Markets"))
    assert calls == [("yahoo_chart", SYMBOL)]
    assert (de._FCT_BASIS_TAG not in de._mpc_warning_parts(row)) is released
    if released:
        assert row["overall_score"] is not None
    else:
        assert row["overall_score"] is None
    assert row["current_price"] == 100.0 and row["currency"] == "USD"


def test_final_tuple_correction_does_not_leave_pre_repair_scoring_claims():
    row = dict(FACTS)
    de._compute_scores_canonical_first(row)
    de._apply_phase_dd_enhancements(row)
    row = de._f7_settle_pass(row, SYMBOL, "Global_Markets")
    before = {key: row.get(key) for key in de._FCT_ROI_SCORE_FIELDS}
    de._strict_project_row(tuple(row), row)
    assert de._FCT_BASIS_TAG not in de._mpc_warning_parts(row)
    assert {key: row.get(key) for key in de._FCT_ROI_SCORE_FIELDS} == before
    assert row["expected_roi_12m"] == pytest.approx(0.2)


@pytest.mark.parametrize("settle_mode", ["off", "observe", "enforce"])
def test_actual_phase_dd_late_target_restore_is_repaired_before_final_decision(monkeypatch, settle_mode):
    monkeypatch.setenv("TFB_ENGINE_TARGET_KLG", "1")
    monkeypatch.setenv("TFB_SCORING_SETTLE", settle_mode)
    quote = dict(FACTS, forecast_price_12m=0.0, forecast_price_3m=None,
                 forecast_price_1m=None, target_mean_price=None)
    monkeypatch.setitem(de._TGT_LKG_STORE, SYMBOL, {
        "ts": time.time(), "name": quote["name"], "fp12": 120.0,
    })
    transitions = []
    original = de._apply_phase_dd_enhancements

    def observe_actual_phase_dd(row):
        before = (row.get("forecast_price_12m"), row.get("expected_roi_12m"))
        result = original(row)
        transitions.append((before, (row.get("forecast_price_12m"), row.get("expected_roi_12m"))))
        return result

    monkeypatch.setattr(de, "_apply_phase_dd_enhancements", observe_actual_phase_dd)
    engine, _calls = _engine(monkeypatch, quote)
    row = asyncio.run(engine.get_enriched_quote_dict(SYMBOL, "Global_Markets"))
    # Observe the actual LKG/honor branch, with no injected row mutation.
    assert transitions[0] == ((0.0, -0.2), (120.0, -0.2))
    assert row["expected_roi_12m"] == pytest.approx(0.2)
    assert row["forecast_price_12m"] == 120.0
    if settle_mode == "enforce":
        assert row["overall_score"] is not None
        assert de._FCT_BASIS_TAG not in de._mpc_warning_parts(row)
    else:
        assert row["overall_score"] is None
        assert de._FCT_BASIS_TAG in de._mpc_warning_parts(row)
        assert row["final_action"] == "DO_NOT_INVEST"
        assert row["trend_12m"] == "UP"


@pytest.mark.parametrize("display_pair", ["0", "1"])
def test_positive_price_alias_uses_existing_gate_fill_before_scoring(monkeypatch, display_pair):
    monkeypatch.setenv("TFB_FORECAST_PAIR_COHERENCE", display_pair)
    row = dict(FACTS, current_price=None, price=100.0)
    de._compute_scores_canonical_first(row)
    assert row["current_price"] == row["price"] == 100.0
    expected_roi = 0.2 if display_pair == "0" else -0.2
    assert row["expected_roi_12m"] == pytest.approx(expected_roi)
    assert row["expected_roi_12m"] == pytest.approx((row["forecast_price_12m"] - 100.0) / 100.0)


def test_price_alias_late_external_repair_cannot_bypass_holdback():
    row = _old_scored_row(current_price=None, price=100.0)
    de._strict_project_row(tuple(row), row)
    _assert_held(row)
