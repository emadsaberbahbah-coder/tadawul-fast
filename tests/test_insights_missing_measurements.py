"""Absent risk/quality inputs cannot support a clean research verdict."""
import pytest

from core.analysis import insights_builder as builder


def context():
    return builder.BuildContext(keys=list(builder._FALLBACK_KEYS), ts="2026-10-14T12:00:00+03:00", norm_criteria={})


def test_price_only_quote_has_unknown_risk_and_quality_not_zero_or_clean():
    quotes = {"SYNTH.US": {"symbol": "SYNTH.US", "current_price": 100}}
    risk = builder._build_risk_alert_rows(context(), quotes)
    assert len(risk) == 1 and risk[0]["value"] == "Unknown"
    assert risk[0]["metric"] == "risk_measurement_status"
    assert "risk_score=0 of 1" in risk[0]["notes"]
    quality = builder._build_data_quality_rows(context(), quotes)
    assert len(quality) == 1 and quality[0]["value"] == "Unknown"
    assert "0 of 1" in quality[0]["notes"]
    snapshot = builder._build_universe_snapshot_rows(context(), "Research", ["SYNTH.US"], quotes)
    row = next(row for row in snapshot if row["metric"] == "data_quality_clean_count")
    assert row["value"] == "0 of 1" and "unknown" in row["notes"]


@pytest.mark.parametrize("value", [float("nan"), float("inf"), True, "missing"])
def test_invalid_risk_and_quality_inputs_remain_unknown(value):
    quotes = {"SYNTH.US": {"risk_score": value, "max_drawdown_1y": value,
                           "volatility_30d": value, "data_quality_score": value, "current_price": 100}}
    assert builder._build_risk_alert_rows(context(), quotes)[0]["value"] == "Unknown"
    assert builder._build_data_quality_rows(context(), quotes)[0]["value"] == "Unknown"


def test_legitimate_zero_risk_and_measured_clean_quality_remain_available():
    quotes = {"SYNTH.US": {"risk_score": 0, "max_drawdown_1y": 0, "volatility_30d": 0,
                           "rsi_signal": "Neutral", "data_quality_score": 90, "current_price": 100}}
    risk = builder._build_risk_alert_rows(context(), quotes)
    assert len(risk) == 1 and risk[0]["metric"] == "risk_alert_count" and risk[0]["value"] == 0
    assert "risk_score=1 of 1" in risk[0]["notes"]
    assert builder._build_data_quality_rows(context(), quotes)[0]["value"] == "Clean"
    snapshot = builder._build_universe_snapshot_rows(context(), "Research", ["SYNTH.US"], quotes)
    assert next(row for row in snapshot if row["metric"] == "data_quality_clean_count")["value"] == "1 of 1"


def test_partial_coverage_reports_unknown_without_dropping_real_risk_alert():
    quotes = {"MEASURED.US": {"risk_score": 85, "data_quality_score": 90, "current_price": 100},
              "UNKNOWN.US": {"current_price": 50}}
    risk = builder._build_risk_alert_rows(context(), quotes)
    assert any(row["metric"] == "risk_measurement_status" and row["value"] == "Unknown" for row in risk)
    assert any(row["metric"] == "risk_score" and row["value"] == 85 for row in risk)
    assert builder._build_data_quality_rows(context(), quotes)[0]["value"] == "Unknown"


@pytest.mark.parametrize("score,price", [(0, 100), (90, 0), (101, 100), (90, float("inf"))])
def test_zero_or_invalid_quality_evidence_never_claims_clean(score, price):
    quotes = {"SYNTH.US": {"data_quality_score": score, "current_price": price}}
    assert builder._build_data_quality_rows(context(), quotes)[0]["value"] == "Unknown"


def test_absent_risk_never_hides_an_explicit_avoid_recommendation():
    rows = builder._build_risk_alert_rows(context(), {"SYNTH.US": {"recommendation": "AVOID"}})
    assert any(row["value"] == "Unknown" for row in rows)
    assert any(row["item"].startswith("AVOID") for row in rows)
