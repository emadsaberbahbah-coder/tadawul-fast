"""Synthetic contracts through the real publication and Sheets value writer."""
from __future__ import annotations

import asyncio
import copy
from datetime import datetime, timezone
from types import SimpleNamespace

import pytest

from core import data_engine_v2 as de
from core import enriched_quote as eq
from core.data_validity import row_acquisition
from core.sheet_presentation import present_instrument_row
from integrations import google_sheets_service as sheets


@pytest.fixture(autouse=True)
def bounded_modes(monkeypatch):
    for name in ("TFB_MARGIN_PUBLISH", "TFB_FC_TUPLE_COHERENT", "TFB_SCORING_SETTLE"):
        monkeypatch.setenv(name, "observe")
    monkeypatch.setenv("TFB_EQ_ROI_UNIT_SENTRY", "off")
    monkeypatch.setenv("TFB_SVC_CAPACITY_GUARD", "0")


def source_row(value=0.9, unit="percent_points"):
    return {
        "symbol": "SYNTH.US", "name": "Synthetic presentation fixture", "asset_class": "ETF",
        "currency": "USD", "current_price": 100.0, "profit_margin": value,
        "_margin_unit_basis": {"profit_margin": {"unit": unit, "value": value,
            "provider": "synthetic", "raw_unit": unit, "raw_value": value,
            "unit_basis": "supplier_field_contract", "source_field": "fixture.profit"}},
        "horizon_days": 365, "invest_period_label": "3M",
        "horizon_days_effective": 90, "horizon_label": "1Y",
        "forecast_price_12m": 120.0, "expected_roi_12m": 0.2,
        "target_price": 120.0, "upside_downside_pct": 0.2,
        "overall_score": 80.0, "quality_score": 75.0,
        "recommendation": "HOLD", "recommendation_detailed": "HOLD",
        "data_provider": "yahoo_chart", "last_updated_utc": "2026-10-09T00:00:00+00:00",
        "warnings": "acquisition_status:success; acquisition_acquired_at:2026-10-09T00:00:00+00:00; "
                    "acquisition_provider:yahoo_chart; acquisition_quote_asof:2026-10-08T20:00:00+00:00",
    }


@pytest.mark.parametrize("value", [0.0, 0.005, 0.9, -0.4, 180.0, -250.0])
def test_percent_point_receipts_serialize_quantity_once_without_source_mutation(value):
    row = source_row(value)
    original = copy.deepcopy(row)
    displayed = present_instrument_row(row)
    assert displayed["profit_margin"] == value / 100.0
    assert present_instrument_row(displayed) == displayed
    projected = {k: v for k, v in displayed.items() if not k.startswith("_")}
    assert present_instrument_row(projected) == projected
    assert row == original
    assert displayed["overall_score"] == row["overall_score"]
    assert displayed["current_price"] == row["current_price"]
    assert displayed["_margin_unit_basis"]["profit_margin"]["raw_value"] == value


@pytest.mark.parametrize("value", [0.009, -0.004, 1.8, -2.5])
def test_explicit_fraction_receipt_never_uses_magnitude(value):
    row = source_row(value, "fraction")
    assert present_instrument_row(row)["profit_margin"] == value


@pytest.mark.parametrize("kind", ["missing", "unknown", "stale", "malformed", "bool", "nonfinite"])
def test_unproven_margin_is_withdrawn_without_trusting_legacy_pts_tags(kind):
    row = source_row()
    row["warnings"] += "; margin_publish:profit_margin:pts:observe"
    if kind == "missing":
        row.pop("_margin_unit_basis")
    elif kind == "unknown":
        row["_margin_unit_basis"]["profit_margin"]["unit"] = "unknown"
    elif kind == "stale":
        row["_margin_unit_basis"]["profit_margin"]["value"] = 1.5
    elif kind == "malformed":
        row["_margin_unit_basis"] = "invalid"
    elif kind == "bool":
        row["profit_margin"] = True
    else:
        row["profit_margin"] = float("inf")
    original = copy.deepcopy(row)
    displayed = present_instrument_row(row)
    assert displayed["profit_margin"] is None
    assert "sheet_margin_unknown:profit_margin" in displayed["warnings"]
    assert row == original


def test_projected_unit_receipt_must_match_current_value_and_cannot_conflict():
    row = source_row()
    row.pop("_margin_unit_basis")
    row["warnings"] += "; sheet_margin_unit:profit_margin:percent_points:0.9"
    assert present_instrument_row(row)["profit_margin"] == pytest.approx(0.009)
    row["warnings"] += "; sheet_margin_unit:profit_margin:fraction:0.9"
    assert present_instrument_row(row)["profit_margin"] is None
    row["warnings"] = "sheet_margin_unit:profit_margin:fraction:0.009"
    assert present_instrument_row(row)["profit_margin"] is None


@pytest.mark.parametrize("days,label", [(1, "1D"), (7, "1W"), (30, "1M"), (90, "3M"),
                                       (180, "6M"), (365, "1Y"), (14, "14D"),
                                       (None, None), (True, None), (float("nan"), None), (90.5, None)])
def test_labels_follow_actual_days_and_effective_horizon_is_separate(days, label):
    row = source_row()
    row["horizon_days"] = days
    displayed = present_instrument_row(row)
    assert displayed["invest_period_label"] == label
    assert displayed["horizon_label"] == "3M"
    assert row["invest_period_label"] == "3M"
    assert row["horizon_label"] == "1Y"


@pytest.mark.parametrize("price_field,return_field", [
    ("target_price", "upside_downside_pct"), ("intrinsic_value", "upside_pct"),
    ("forecast_price_1m", "expected_roi_1m"), ("forecast_price_3m", "expected_roi_3m"),
    ("forecast_price_12m", "expected_roi_12m"),
])
def test_conflicting_derived_return_is_withdrawn_without_inventing_tuple(price_field, return_field):
    row = source_row()
    row[price_field], row[return_field] = 120.0, -0.2
    original = copy.deepcopy(row)
    displayed = present_instrument_row(row)
    assert displayed[price_field] == 120.0
    assert displayed[return_field] is None
    assert "sheet_tuple_conflict:" + return_field in displayed["warnings"]
    assert row == original
    row[return_field] = None
    assert present_instrument_row(row)[return_field] is None


def test_alias_conflicts_are_not_resolved_by_order_or_overwriting():
    row = source_row()
    row.update({"Profit Margin": 2.0, "Expected ROI 12M": -0.2})
    displayed = present_instrument_row(row)
    assert displayed["profit_margin"] is displayed["Profit Margin"] is None
    assert displayed["expected_roi_12m"] is displayed["Expected ROI 12M"] is None


def test_tiny_prices_do_not_hide_return_conflicts_and_raw_rounding_remains_valid():
    row = source_row()
    row.update(current_price=0.0006, forecast_price_12m=0.00072, expected_roi_12m=0.1)
    assert present_instrument_row(row)["expected_roi_12m"] is None
    row["expected_roi_12m"] = 0.2
    assert present_instrument_row(row)["expected_roi_12m"] == 0.2
    row.update(current_price=100.0, forecast_price_12m=123.45674, expected_roi_12m=0.234567)
    assert present_instrument_row(row)["expected_roi_12m"] == 0.234567


def test_native_upside_header_is_validated_against_intrinsic_basis():
    row = {"Current Price": 100.0, "Intrinsic Value": 120.0, "Upside %": -0.2}
    assert present_instrument_row(row)["Upside %"] is None


def test_publication_preserves_actual_acquisition_and_known_failure_evidence():
    row = source_row()
    row["warnings"] = [row["warnings"], "f7_settle_failed:non_converged", {"fetch_failed": "synthetic"}]
    original = copy.deepcopy(row)
    displayed = present_instrument_row(row)
    assert "f7_settle_failed:non_converged" in displayed["warnings"]
    assert "fetch_failed" in displayed["warnings"]
    assert row == original
    assert row_acquisition(row, datetime(2026, 10, 9, tzinfo=timezone.utc), 86400).status == \
           row_acquisition(displayed, datetime(2026, 10, 9, tzinfo=timezone.utc), 86400).status == "INVALID"


@pytest.mark.parametrize("mode", ["off", "observe", "enforce"])
def test_actual_strict_schema_and_matrix_display_keep_receipts_and_source_quantity(monkeypatch, mode):
    monkeypatch.setenv("TFB_MARGIN_PUBLISH", mode)
    row = source_row()
    headers, keys = de.get_sheet_spec("Global_Markets")
    projected = de._strict_project_row(keys, row)
    assert set(projected) == set(keys) and len(projected) == 115
    assert projected["profit_margin"] == pytest.approx(0.009)
    assert projected["invest_period_label"] == "1Y"
    # Existing enforce mode may convert internally; presentation never arms it.
    assert row["profit_margin"] == (0.009 if mode == "enforce" else 0.9)
    assert de._margin_publish_mode() == mode
    matrix = de._rows_matrix_from_rows([projected], keys)[0]
    displayed = de._rows_display_objects_from_rows([projected], headers, keys)[0]
    assert matrix[keys.index("profit_margin")] == projected["profit_margin"]
    assert displayed[headers[keys.index("profit_margin")]] == projected["profit_margin"]
    assert "acquisition_status:success" in projected["warnings"]
    assert row_acquisition(projected, datetime(2026, 10, 9, tzinfo=timezone.utc), 86400).successful


def test_actual_strict_projection_retains_known_scoring_failure_block():
    row = source_row()
    row["warnings"] += "; f7_settle_failed:non_converged"
    row.update(recommendation="BUY", recommendation_detailed="BUY", final_action="INVEST")
    _, keys = de.get_sheet_spec("Global_Markets")
    projected = de._strict_project_row(keys, row)
    assert projected["overall_score"] is None
    assert projected["final_action"] == "DO_NOT_INVEST"
    assert projected["investability_status"] != "INVESTABLE"
    assert "f7_settle_failed:non_converged" in projected["warnings"]


def test_actual_page_fallback_returns_display_copies_and_does_not_mutate_quote_cache(monkeypatch):
    row = source_row()
    original_margin = copy.deepcopy(row["_margin_unit_basis"])
    engine = de.DataEngineV5(settings=SimpleNamespace(), providers=["yahoo_chart"])
    async def fetched(*args, **kwargs):
        return [row]
    async def no_veto(*args, **kwargs):
        return None
    async def symbols(*args, **kwargs):
        return ["SYNTH.US"]
    monkeypatch.setattr(engine, "get_enriched_quotes", fetched)
    monkeypatch.setattr(engine, "_apply_news_veto", no_veto)
    monkeypatch.setattr(engine, "list_symbols_for_page", symbols)
    result = asyncio.run(engine.get_page_rows("Global_Markets", limit=1))
    assert result[0]["profit_margin"] == pytest.approx(0.009)
    assert row["profit_margin"] == 0.9
    assert row["_margin_unit_basis"] == original_margin


def test_actual_enriched_serializer_keeps_unit_evidence_through_projection():
    row = source_row()
    keys = ["symbol", "current_price", "profit_margin", "horizon_days", "invest_period_label", "warnings"]
    normalized = eq.normalize_rows([row], keys, "Global_Markets")[0]
    assert normalized["profit_margin"] == pytest.approx(0.009)
    assert normalized["invest_period_label"] == "1Y"
    assert present_instrument_row(normalized) == normalized
    assert row["profit_margin"] == 0.9


def test_actual_complete_sheet_payload_agrees_across_all_three_views(monkeypatch):
    row = source_row()
    engine = de.DataEngineV5(settings=SimpleNamespace(), providers=["yahoo_chart"])
    async def fetch(*args, **kwargs):
        return [row]
    async def symbols(*args, **kwargs):
        return ["SYNTH.US"]
    async def no_veto(*args, **kwargs):
        return None
    monkeypatch.setattr(engine, "get_enriched_quotes", fetch)
    monkeypatch.setattr(engine, "list_symbols_for_page", symbols)
    monkeypatch.setattr(engine, "_bind_rows_reader", lambda: None)
    monkeypatch.setattr(engine, "_apply_news_veto", no_veto)
    payload = asyncio.run(engine.get_sheet_rows("Global_Markets", body={"symbols": ["SYNTH.US"]}))
    headers, keys = de.get_sheet_spec("Global_Markets")
    assert len(payload["rows"]) == 1
    canonical = payload["rows"][0]
    assert canonical["profit_margin"] == pytest.approx(0.009)
    assert canonical["invest_period_label"] == "1Y"
    assert payload["rows_matrix"][0][keys.index("profit_margin")] == canonical["profit_margin"]
    assert payload["rows_display"][0][headers[keys.index("profit_margin")]] == canonical["profit_margin"]
    assert row["profit_margin"] == 0.9


class FakeSheetsAPI:
    def __init__(self):
        self.requests = []
    def spreadsheets(self):
        return self
    def values(self):
        return self
    def batchUpdate(self, **kwargs):
        self.requests.append(copy.deepcopy(kwargs))
        return self
    def execute(self):
        body = self.requests[-1]["body"]
        return {"responses": [{"updatedCells": sum(len(row) for row in item["values"])}
                              for item in body.get("data", [])]}


def test_real_sdk_writer_payload_is_fractional_and_idempotent(monkeypatch):
    api = FakeSheetsAPI()
    monkeypatch.setattr(sheets, "get_sheets_service", lambda: api)
    monkeypatch.setattr(sheets._CONFIG, "use_batch_update", True)
    headers = ["Symbol", "Current Price", "Profit Margin", "Horizon Days", "Invest Period Label", "Warnings"]
    _, rows = sheets.rows_to_grid(headers, [source_row()])
    original = copy.deepcopy(rows)
    written = sheets.write_grid_chunked("SYNTHETIC_BOOK", "Global_Markets", "A5", [headers] + rows)
    assert written == 12
    payload = api.requests[0]["body"]["data"][0]["values"]
    assert payload[1][2] == pytest.approx(0.009) and payload[1][4] == "1Y"
    assert "sheet_margin_unit:profit_margin:fraction:" + repr(payload[1][2]) in payload[1][5]
    sheets.write_grid_chunked("SYNTHETIC_BOOK", "Global_Markets", "A5", payload)
    assert api.requests[1]["body"]["data"][0]["values"] == payload
    assert rows == original


def test_duplicate_exact_grid_header_does_not_hide_unit_conflict():
    headers = ["Symbol", "Profit Margin", "Profit Margin", "Warnings"]
    row = ["SYNTH.US", 0.009, 0.5, "sheet_margin_unit:profit_margin:fraction:0.009"]
    _, rows = sheets.rows_to_grid(headers, [row])
    assert rows[0][1] is rows[0][2] is None


def test_actual_refresh_preservation_cannot_resurrect_unknown_margin(monkeypatch):
    api = FakeSheetsAPI()
    headers = ["Symbol", "Current Price", "Profit Margin", "Position Qty", "Warnings"]
    row = source_row()
    row.pop("_margin_unit_basis")
    response = {"status": "success", "headers": headers, "rows": [row]}
    monkeypatch.setattr(sheets, "get_sheets_service", lambda: api)
    monkeypatch.setattr(sheets, "get_canonical_headers", lambda page: headers)
    monkeypatch.setattr(sheets._CONFIG, "ensure_headers_match_schema", False)
    monkeypatch.setattr(sheets._CONFIG, "use_batch_update", True)
    monkeypatch.setattr(sheets._CONFIG, "preserve_columns", ["Profit Margin", "Position Qty"])
    monkeypatch.setattr(sheets._safe_mode_validator, "validate_backend_response", lambda *a, **kw: None)
    monkeypatch.setattr(sheets._backend_client, "call_api_chunked", lambda *a, **kw: response)
    monkeypatch.setattr(sheets, "_build_preserve_map", lambda *a, **kw: {"SYNTH.US": {"profitmargin": 90.0, "positionqty": 5}})
    result = sheets._refresh_logic("/synthetic", "SYNTHETIC_BOOK", "Global_Markets", ["SYNTH.US"])
    assert result["rows_written"] == 1
    values = api.requests[0]["body"]["data"][0]["values"]
    assert values[1][2] is None and values[1][3] == 5


def test_actual_formatter_uses_native_percent_format_only_on_fraction_columns(monkeypatch):
    headers = ["Symbol", "Current Price", "Gross Margin", "Profit Margin", "Expected ROI 12M", "Position Qty"]
    requests = []
    monkeypatch.setattr(sheets._CONFIG, "ensure_tabs_exist", False)
    monkeypatch.setattr(sheets, "get_canonical_headers", lambda page: headers)
    monkeypatch.setattr(sheets, "_read_header_row", lambda *a: headers)
    monkeypatch.setattr(sheets, "_seed_insights_criteria_if_empty", lambda *a: None)
    monkeypatch.setattr(sheets, "_get_sheet_title_to_id", lambda *a: {"Global_Markets": 1})
    monkeypatch.setattr(sheets, "_batch_update", lambda book, reqs: requests.extend(reqs))
    sheets.ensure_headers_and_formatting("SYNTHETIC_BOOK", "Global_Markets", "C5")
    formats = [r["repeatCell"] for r in requests if "repeatCell" in r]
    assert [r["range"]["startColumnIndex"] for r in formats] == [4, 5, 6]
    assert all(r["range"]["startRowIndex"] == 5 for r in formats)
    assert all(r["cell"]["userEnteredFormat"]["numberFormat"]["type"] == "PERCENT" for r in formats)
