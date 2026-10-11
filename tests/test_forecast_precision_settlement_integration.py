"""Combined rollout gates use one price quantum and retain funding exclusions."""
import copy

import pytest

from core import data_engine_v2 as de
from core.sheet_presentation import present_instrument_row


@pytest.mark.parametrize("precision", ["off", "on"])
@pytest.mark.parametrize("soft_cap", ["0", "1"])
def test_late_provider_target_softcap_uses_shared_price_rounding(
        monkeypatch, precision, soft_cap):
    monkeypatch.setenv("TFB_PRICE_PRECISION", precision)
    monkeypatch.setenv("TFB_ROI12_SOFTCAP_GATE", "enforce")
    monkeypatch.setenv("TFB_PROVIDER_TARGET_SOFT_CAP", soft_cap)
    price = 5.47e-6
    row = {"symbol": "SHIB-USD", "current_price": price,
           "forecast_source": "provider_target", "forecast_price_12m": price * 2.,
           "expected_roi_12m": 1.}
    de._publish_tuple_seams(row)
    expected_return = de._roi12_softcap_target(
        1., de._PHASE_II_MAX_12M_ABS_RETURN,
        de._provider_target_soft_cap_band() if soft_cap == "1" else 0.)
    assert row["forecast_price_12m"] == de._round_price(price * (1. + expected_return))
    if precision == "on":
        assert row["forecast_price_12m"] > 0.
        assert row["expected_roi_12m"] == pytest.approx(expected_return, abs=5e-7)
        assert present_instrument_row(row)["expected_roi_12m"] == row["expected_roi_12m"]
        previous = copy.deepcopy(row)
        de._publish_tuple_seams(row)
        assert row == previous
    else:
        assert row["forecast_price_12m"] == round(price * (1. + expected_return), 4)


def test_armed_tuple_seam_rejects_legacy_tiny_price_drift(monkeypatch):
    monkeypatch.setenv("TFB_PRICE_PRECISION", "on")
    monkeypatch.setenv("TFB_TUPLE_NOISE_RELEASE", "enforce")
    # A cached low-precision forecast cannot justify an oversized release once
    # the shared significant-digit contract is armed.
    row = {"current_price": .0003001941, "forecast_price_12m": .0003,
           "expected_roi_12m": -.025}
    assert de._tuple_noise_release(row) == 1
    assert row["expected_roi_12m"] == -.025
    assert "tuple_noise:12m:gap" in row["warnings"]
    assert present_instrument_row(row)["expected_roi_12m"] is None


def test_armed_tuple_seam_skips_return_already_accepted_by_presenter(monkeypatch):
    monkeypatch.setenv("TFB_PRICE_PRECISION", "on")
    monkeypatch.setenv("TFB_TUPLE_NOISE_RELEASE", "enforce")
    row = {"current_price": 10., "forecast_price_12m": 11.2346,
           "expected_roi_12m": .1234539}
    assert present_instrument_row(row)["expected_roi_12m"] == row["expected_roi_12m"]
    before = copy.deepcopy(row)
    assert de._tuple_noise_release(row) == 0
    assert row == before


def test_precise_softcap_keeps_unresolved_rank_exclusion(monkeypatch):
    monkeypatch.setenv("TFB_PRICE_PRECISION", "on")
    monkeypatch.setenv("TFB_ROI12_SOFTCAP_GATE", "enforce")
    row = {"symbol": "SHIB-USD", "current_price": 5.47e-6,
           "forecast_source": "provider_target", "forecast_price_12m": 1.094e-5,
           "expected_roi_12m": 1., "overall_score": 90.,
           "warnings": "settle_unresolved:enforce:pass_limit:p3:ov=90:rc=buy"}
    de._publish_tuple_seams(row)
    assert row["forecast_price_12m"] > 0.
    assert de._f7_settle_unresolved(row)
    assert de._top10_row_is_eligible(row) is False
    de._apply_rank_overall([row])
    assert row["rank_overall"] is None
