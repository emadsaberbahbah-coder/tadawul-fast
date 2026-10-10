"""Fix P uses the real gate; its owner-controlled switch stays default off."""
import copy

import pytest


@pytest.fixture(autouse=True)
def gate_environment(monkeypatch):
    for name in ("TFB_STRICT_INVEST_GATE", "TFB_STRICT_INVEST_DQ_MIN", "TFB_STRICT_INVEST_RELIABILITY_MIN",
                 "TFB_CALIBRATION_ADJUST", "TFB_RELIABILITY_RECALIBRATION", "TFB_CONSERVATIVE_GATE"):
        monkeypatch.delenv(name, raising=False)
    monkeypatch.setenv("TFB_INVESTABILITY_GATE", "1")


def approved_row():
    return {"symbol": "SYNTH.US", "name": "Synthetic", "asset_class": "Equity", "current_price": 100,
            "forecast_price_3m": 110, "forecast_price_12m": 120, "expected_roi_3m": .10,
            "expected_roi_12m": .20, "forecast_source": "provider_target", "forecast_confidence": .90,
            "week_52_low": 80, "week_52_high": 130, "avg_volume_30d": 10000, "pe_ttm": 12,
            "eps_ttm": 8, "market_cap": 1e9, "volatility_30d": .1, "overall_score": 80,
            "debt_to_equity": .2, "free_cash_flow_ttm": 1e6, "recommendation": "BUY", "risk_bucket": "LOW"}


def apply(row):
    from core import data_engine_v2 as de
    result = copy.deepcopy(row)
    de._apply_investability_gate(result)
    return result


@pytest.mark.parametrize("changes,reason", [
    ({"pe_ttm": None, "eps_ttm": None, "market_cap": None, "volatility_30d": None}, "data quality"),
    ({"forecast_confidence": .60}, "forecast reliability"),
    ({"risk_bucket": "HIGH"}, "risk bucket HIGH"),
    ({"provider_rating": "SELL"}, "unreviewed provider/engine conflict"),
])
def test_strict_gate_only_demotes_when_armed(monkeypatch, changes, reason):
    row = {**approved_row(), **changes}
    default = apply(row)
    assert (default["investability_status"], default["final_action"]) == ("INVESTABLE", "INVEST")
    monkeypatch.setenv("TFB_STRICT_INVEST_GATE", "off")
    assert apply(row) == default
    monkeypatch.setenv("TFB_STRICT_INVEST_GATE", "1")
    strict = apply(row)
    assert (strict["investability_status"], strict["final_action"]) == ("WATCHLIST", "WATCH")
    assert strict["block_reason"].startswith("Strict gate:") and reason in strict["block_reason"]
    assert strict["data_quality_score"] == default["data_quality_score"]
    assert strict["forecast_reliability_score"] == default["forecast_reliability_score"]


def test_strict_gate_preserves_qualified_and_never_promotes_blocked(monkeypatch):
    monkeypatch.setenv("TFB_STRICT_INVEST_GATE", "1")
    good = apply(approved_row())
    assert (good["investability_status"], good["final_action"]) == ("INVESTABLE", "INVEST")
    blocked = apply({**approved_row(), "current_price": None})
    assert (blocked["investability_status"], blocked["final_action"]) == ("BLOCKED", "DO_NOT_INVEST")


if __name__ == "__main__":
    raise SystemExit(pytest.main([__file__, "-q"]))
