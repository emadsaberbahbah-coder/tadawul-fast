"""Single-tree replacement for the archived enriched-quote dual-tree battery."""
import copy

import pytest

KEYS = ["symbol", "current_price", "forecast_price_12m", "expected_roi_12m", "warnings"]
ROWS = [{"symbol": "FRAC.US", "current_price": 10., "forecast_price_12m": 13.4, "expected_roi_12m": .34},
        {"symbol": "PTS.US", "current_price": 10., "forecast_price_12m": 13.4, "expected_roi_12m": 34.},
        {"symbol": "TINY.US", "current_price": 100., "forecast_price_12m": 100.55, "expected_roi_12m": .55},
        {"symbol": "NOGT.US", "current_price": 10., "expected_roi_12m": .34}]


def run(rows):
    from core import enriched_quote as eq
    return eq.normalize_rows(copy.deepcopy(rows), KEYS, "Global_Markets")


def test_off_and_observe_preserve_values_with_countable_tags(monkeypatch):
    monkeypatch.delenv("TFB_EQ_ROI_UNIT_SENTRY", raising=False)
    off = run(ROWS)
    monkeypatch.setenv("TFB_EQ_ROI_UNIT_SENTRY", "off")
    assert run(ROWS) == off
    monkeypatch.setenv("TFB_EQ_ROI_UNIT_SENTRY", "observe")
    obs = run(ROWS)
    assert [{k: v for k, v in row.items() if k != "warnings"} for row in off] == [
        {k: v for k, v in row.items() if k != "warnings"} for row in obs]
    for row in obs[:2]:
        assert row["expected_roi_12m"] == pytest.approx(.34)
        assert "roi_unit_points:eq:expected_roi_12m:observe" in row["warnings"]


def test_enforce_preserves_fraction_schema_and_is_idempotent(monkeypatch):
    from core import enriched_quote as eq
    monkeypatch.setenv("TFB_EQ_ROI_UNIT_SENTRY", "enforce")
    rows = copy.deepcopy(ROWS)
    for row in rows:
        eq._normalize_percent_units(row)
        eq._roi_points_output_sentry(row)
    assert [row["expected_roi_12m"] for row in rows] == pytest.approx([.34, .34, .0055, .34])
    for row in rows[:3]:
        assert "roi_unit_fraction:eq:expected_roi_12m:enforce" in row["warnings"]
    assert "roi_unit_ambiguous:eq:expected_roi_12m:enforce" in rows[3]["warnings"]
    before = copy.deepcopy(rows[0])
    eq._roi_points_output_sentry(rows[0])
    assert rows[0] == before
    published = run(ROWS)
    assert [row["expected_roi_12m"] for row in published[:3]] == pytest.approx([.34, .34, .0055])
    assert all("sheet_tuple_conflict:expected_roi_12m" not in row["warnings"]
               for row in published[:3])
    assert published[3]["expected_roi_12m"] is None


@pytest.mark.parametrize("horizon", ["1m", "3m", "12m"])
@pytest.mark.parametrize("fraction", [-.25, 0., .0055, .34, 1.2])
@pytest.mark.parametrize("input_scale", [1., 100.])
def test_enforce_real_publication_preserves_all_roi_horizons(
        monkeypatch, horizon, fraction, input_scale):
    from core import enriched_quote as eq
    monkeypatch.setenv("TFB_EQ_ROI_UNIT_SENTRY", "enforce")
    roi_key, price_key = "expected_roi_" + horizon, "forecast_price_" + horizon
    row = {"symbol": "WITNESS.US", "current_price": 100.,
           price_key: 100. * (1. + fraction), roi_key: fraction * input_scale}
    keys = ["symbol", "current_price", price_key, roi_key, "warnings"]
    result = eq.normalize_rows([row], keys, "Global_Markets")[0]
    assert result[roi_key] == pytest.approx(fraction)
    assert row[roi_key] == fraction * input_scale
    assert "sheet_tuple_conflict:" + roi_key not in (result["warnings"] or "")
    again = eq.normalize_rows([result], keys, "Global_Markets")[0]
    assert again == result


@pytest.mark.parametrize("current_price,forecast,stored", [
    (0., 13.4, .34), (None, 13.4, .34), (10., None, .34),
    (10., 13.4, .99), (10., float("nan"), .34),
])
def test_enforce_does_not_invent_or_rewrite_unproved_roi(
        monkeypatch, current_price, forecast, stored):
    from core import enriched_quote as eq
    monkeypatch.setenv("TFB_EQ_ROI_UNIT_SENTRY", "enforce")
    row = {"symbol": "UNPROVED.US", "current_price": current_price,
           "forecast_price_12m": forecast, "expected_roi_12m": stored}
    eq._roi_points_output_sentry(row)
    assert row["expected_roi_12m"] == stored
    assert eq.normalize_rows([row], KEYS, "Global_Markets")[0]["expected_roi_12m"] is None


if __name__ == "__main__":
    raise SystemExit(pytest.main([__file__, "-q"]))
