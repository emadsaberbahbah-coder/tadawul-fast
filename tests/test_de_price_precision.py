"""WP-03: real engine/scorer/presentation paths, entirely offline."""
from __future__ import annotations

import copy
import math

import pytest

from core import data_engine_v2 as de, scoring
from core.price_precision import price_precision_enabled
from core.sheet_presentation import present_instrument_row


PRICES = (1e-8, 1.23456789e-8, 5.47e-6, 5.594e-5, .00987654321,
          .123456789, 1.23456789, 9.87654321, 10.0, 12.3456789, 1e6)
HORIZONS = ("1m", "3m", "12m")


@pytest.fixture(autouse=True)
def offline_modes(monkeypatch):
    monkeypatch.delenv("TFB_PRICE_PRECISION", raising=False)
    monkeypatch.setenv("TFB_ENGINE_TARGET_KLG", "0")
    monkeypatch.setenv("TFB_FORECAST_PAIR_COHERENCE", "1")


@pytest.mark.parametrize("raw,enabled", [
    (None, False), ("", False), ("off", False), ("0", False),
    ("observe", False), ("invalid", False), ("on", True),
    ("1", True), ("true", True), (" YES ", True),
])
def test_explicit_gate_words_and_health_readback(monkeypatch, raw, enabled):
    if raw is not None:
        monkeypatch.setenv("TFB_PRICE_PRECISION", raw)
    assert price_precision_enabled() is enabled
    assert de.surface_gate_states()["price_precision"] == ("on" if enabled else "off")


@pytest.mark.parametrize("nd", [4, 6])
def test_off_exact_legacy_rounding_grid(monkeypatch, nd):
    for mode in ("", "off", "0", "invalid"):
        monkeypatch.setenv("TFB_PRICE_PRECISION", mode)
        for number in (*PRICES, *(-price for price in PRICES), 0.0, -0.0):
            assert de._round_price(number, nd).hex() == round(number, nd).hex()
            assert scoring._round_price(number, nd) == scoring._round(number, nd)


def test_on_seven_significant_digits_and_large_price_contract(monkeypatch):
    monkeypatch.setenv("TFB_PRICE_PRECISION", "on")
    for number in (*PRICES, *(-price for price in PRICES)):
        rounded = de._round_price(number)
        assert rounded == scoring._round_price(number)
        if abs(number) < 10:
            assert rounded != 0
            assert abs((rounded - number) / number) <= 5e-7
        else:
            assert rounded == round(number, 4)


@pytest.mark.parametrize("mode", ["off", "on"])
def test_zero_and_nonfinite_contract(monkeypatch, mode):
    monkeypatch.setenv("TFB_PRICE_PRECISION", mode)
    assert de._round_price(-0.0).hex() == (-0.0).hex()
    assert math.isnan(de._round_price(float("nan")))
    for number in (float("inf"), -float("inf")):
        assert de._round_price(number) == number
    for number in (None, "invalid", float("nan"), float("inf")):
        assert scoring._round_price(number) is None


def _synthetic_row(price):
    return {"symbol": "SHIB-USD", "asset_class": "Crypto", "current_price": price,
            "momentum_score": 75.0, "quality_score": 60.0, "growth_score": 60.0,
            "forecast_confidence": .7}


@pytest.mark.parametrize("path", ["phase_ii", "provider_12m", "provider_3m", "fallback"])
def test_real_engine_paths_keep_legacy_default_and_precise_pairs(monkeypatch, path):
    for price in PRICES:
        original = _synthetic_row(price)
        if path.startswith("provider_"):
            horizon = path.removeprefix("provider_")
            original.update(forecast_source="provider_target")
            original["forecast_price_" + horizon] = price * 1.1
        mutate = de._compute_scores_local_fallback if path == "fallback" else de._phase_ii_quality_forecast
        default_row = copy.deepcopy(original)
        mutate(default_row)
        # Compare the real path against its exact old rounding, including
        # derived returns and warning tags, rather than a parallel model.
        with monkeypatch.context() as legacy:
            legacy.setattr(de, "_round_price", lambda value, nd=4: round(value, nd))
            legacy_row = copy.deepcopy(original)
            mutate(legacy_row)
        assert default_row == legacy_row
        monkeypatch.setenv("TFB_PRICE_PRECISION", "on")
        precise = copy.deepcopy(original)
        mutate(precise)
        displayed = present_instrument_row(precise)
        for horizon in HORIZONS:
            forecast = precise["forecast_price_" + horizon]
            roi = precise["expected_roi_" + horizon]
            assert forecast > 0
            error_bound = 5e-6 + (.00005 / price if forecast >= 10 else 5e-7)
            assert abs((forecast - price) / price - roi) <= error_bound
            assert displayed["expected_roi_" + horizon] == roi
        monkeypatch.delenv("TFB_PRICE_PRECISION")


def test_real_scorer_keeps_subcent_forecasts_and_aliases(monkeypatch):
    for price in PRICES:
        row = {"current_price": price, "expected_roi_12m": .2}
        default, errors = scoring.derive_forecast_patch(row, scoring.ForecastParameters())
        with monkeypatch.context() as legacy:
            legacy.setattr(scoring, "_round_price", lambda value, nd=4: scoring._round(value, nd))
            old, old_errors = scoring.derive_forecast_patch(row, scoring.ForecastParameters())
        assert (default, errors) == (old, old_errors)
        monkeypatch.setenv("TFB_PRICE_PRECISION", "1")
        patch, errors = scoring.derive_forecast_patch(row, scoring.ForecastParameters())
        assert not errors
        displayed = present_instrument_row(dict(row, **patch))
        for horizon in HORIZONS:
            assert patch["forecast_price_" + horizon] > 0
            assert patch["forecast_price_" + horizon] == patch["expected_price_" + horizon]
            assert displayed["expected_roi_" + horizon] == patch["expected_roi_" + horizon]
        monkeypatch.delenv("TFB_PRICE_PRECISION")


def test_pair_rewrite_uses_precise_price_without_moving_roi(monkeypatch):
    monkeypatch.setenv("TFB_PRICE_PRECISION", "on")
    row = {"current_price": 5.47e-6, "forecast_price_12m": 7e-6, "expected_roi_12m": .2}
    de._apply_forecast_pair_coherence(row)
    assert row["forecast_price_12m"] == pytest.approx(6.564e-6)
    assert row["expected_roi_12m"] == .2
    again = copy.deepcopy(row)
    de._apply_forecast_pair_coherence(row)
    assert row == again


@pytest.mark.parametrize("soft", ["0", "1"])
def test_intrinsic_model_and_display_receipt_keep_tiny_prices(monkeypatch, soft):
    monkeypatch.setenv("TFB_PRICE_PRECISION", "1")
    monkeypatch.setenv("TFB_INTRINSIC_DISPLAY_CAP", "1")
    monkeypatch.setenv("TFB_INTRINSIC_DISPLAY_SOFTCAP", soft)
    price = 5.47e-6
    model = {"symbol": "SHIB-USD", "current_price": price,
             "forecast_price_12m": price * 1.2}
    de._compute_intrinsic_and_upside(model)
    assert model["intrinsic_value"] == pytest.approx(price * 1.2)
    assert present_instrument_row(model)["upside_pct"] == model["upside_pct"]
    row = {"symbol": "SHIB-USD", "current_price": price,
           "intrinsic_value": price * 2, "upside_pct": 1.0}
    de._cap_intrinsic_display(row)
    assert row["intrinsic_value"] > 0
    assert de._intrinsic_display_basis(row)["raw_intrinsic"] == price * 2
    assert present_instrument_row(row)["upside_pct"] == row["upside_pct"]
    before = copy.deepcopy(row)
    de._cap_intrinsic_display(row)
    assert row == before


def test_provider_target_cap_never_turns_tiny_price_into_zero(monkeypatch):
    monkeypatch.setenv("TFB_PRICE_PRECISION", "on")
    row = {"current_price": 5.47e-6, "forecast_source": "provider_target",
           "forecast_price_12m": 5.47e-6 * 2}
    de._cap_provider_target_forecasts(row)
    assert row["forecast_price_12m"] > 0
    implied = (row["forecast_price_12m"] - row["current_price"]) / row["current_price"]
    assert row["expected_roi_12m"] == pytest.approx(implied, abs=5e-7)


def test_six_decimal_analyst_target_anchor_joins_precision_lane(monkeypatch):
    row = {"symbol": "SHIB-USD", "current_price": 5.47e-8,
           "price_target_12m": 6.564e-8}
    legacy = de._canonicalize_provider_row(row, requested_symbol="SHIB-USD", provider="fixture")
    assert legacy["target_mean_price"] == 0.0
    monkeypatch.setenv("TFB_PRICE_PRECISION", "1")
    precise = de._canonicalize_provider_row(row, requested_symbol="SHIB-USD", provider="fixture")
    assert precise["target_mean_price"] == pytest.approx(6.564e-8)
    precise.pop("target_price", None)
    de._apply_analyst_trend_block(precise)
    assert precise["target_price"] == pytest.approx(6.564e-8)


def test_presentation_rounding_allowance_is_gated_and_bounded(monkeypatch):
    # A legacy four-decimal price above the precision threshold has a proved
    # 5e-5 error bound; the default strict presentation contract stays intact.
    row = {"current_price": 10.0, "forecast_price_12m": 11.2346,
           "expected_roi_12m": .1234539}
    assert present_instrument_row(row)["expected_roi_12m"] is None
    monkeypatch.setenv("TFB_PRICE_PRECISION", "on")
    assert present_instrument_row(row)["expected_roi_12m"] == row["expected_roi_12m"]
    tiny = {"current_price": 5.47e-6, "forecast_price_12m": 6.564e-6,
            "expected_roi_12m": .99}
    shown = present_instrument_row(tiny)
    assert shown["expected_roi_12m"] is None
    assert "sheet_tuple_conflict:expected_roi_12m" in shown["warnings"]
