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


def test_enforce_scales_ground_truth_only_and_is_idempotent(monkeypatch):
    from core import enriched_quote as eq
    monkeypatch.setenv("TFB_EQ_ROI_UNIT_SENTRY", "enforce")
    # The historic sentry emits points. The newer publication contract expects
    # fractions and withholds these mismatched tuples; test the real sentry
    # seam as well as that publication behavior, without bypassing the writer.
    rows = copy.deepcopy(ROWS)
    for row in rows:
        eq._normalize_percent_units(row)
        eq._roi_points_output_sentry(row)
    assert [row["expected_roi_12m"] for row in rows] == pytest.approx([34., 34., .55, .34])
    for row in rows[:2]:
        assert "roi_unit_points:eq:expected_roi_12m:enforce" in row["warnings"]
    assert "roi_unit_ambiguous:eq:expected_roi_12m:enforce" in rows[3]["warnings"]
    eq._roi_points_output_sentry(rows[0])
    assert rows[0]["expected_roi_12m"] == pytest.approx(34.)
    published = run(ROWS)
    assert published[0]["expected_roi_12m"] is None
    assert "sheet_tuple_conflict:expected_roi_12m" in published[0]["warnings"]


if __name__ == "__main__":
    raise SystemExit(pytest.main([__file__, "-q"]))
