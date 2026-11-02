"""Known listed debt cannot inherit an equity model or an unverified quote."""
from __future__ import annotations

import copy
import asyncio
import json
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace

import pytest

from core.sheets.schema_registry import get_sheet_headers, get_sheet_keys
from core import reviewed_instrument_classes as policy
from core import data_engine_v2 as engine
from scripts import run_dashboard_sync as sync
from tests.test_verified_name_labels import FakeSheetsAPI


FIXTURE = json.loads((Path(__file__).parent / "fixtures/dcbg_source_20261023.json").read_text())
HEADERS, LIVE_ROW = FIXTURE["headers"], FIXTURE["row"]
KEYS = get_sheet_keys("Global_Markets")
REGISTRY = json.loads(policy.REGISTRY_PATH.read_text())
REVIEWED = datetime.fromisoformat(REGISTRY["bindings"][0]["reviewed_at_utc"])
NOW = REVIEWED + timedelta(minutes=1)


@pytest.fixture(autouse=True)
def deterministic_production_clock(monkeypatch):
    monkeypatch.setattr(policy, "_now_utc", lambda: NOW)


def observed_row():
    return dict(zip(KEYS, copy.deepcopy(LIVE_ROW)))


def assert_blocked(row):
    assert row["investability_status"] == "BLOCKED"
    assert row["final_action"] == "DO_NOT_INVEST"
    assert row["current_price"] is None
    assert row["forecast_price_12m"] is None
    assert row["expected_roi_3m"] is None
    assert row["overall_score"] is None


def test_writer_withholds_observed_debt_quote_and_equity_forecasts(monkeypatch):
    # This is a live canonical row, not a row manufactured from the new policy.
    assert LIVE_ROW[HEADERS.index("Current Price")] == 26.52
    assert LIVE_ROW[HEADERS.index("Forecast Source")] == "phase_ii_synthetic"
    api = FakeSheetsAPI()
    writer = sync.SheetsWriter()
    monkeypatch.setattr(writer, "_get_service", lambda: api)
    monkeypatch.setenv("TFB_WARNINGS_COMPACT", "off")
    monkeypatch.setenv("TFB_RANK_OVERALL_PAGE", "off")
    before = copy.deepcopy(LIVE_ROW)
    writer.write_table("test", "Global_Markets", "A1", HEADERS, [LIVE_ROW])
    row = dict(zip(HEADERS, api.requests[-1]["body"]["values"][1]))
    assert row["Asset Class"] == "Bond"
    assert "Subordinated Notes due 2034" in row["Name"]
    assert row["Investability Status"] == "BLOCKED"
    assert row["Final Action"] == "DO_NOT_INVEST"
    for field in ("Current Price", "Previous Close", "P/E (TTM)", "Intrinsic Value", "Target Price",
                  "Overall Score", "Rank (Overall)", "Forecast Source", "Forecast Price 1M",
                  "Forecast Price 3M", "Forecast Price 12M", "Expected ROI 1M", "Expected ROI 3M",
                  "Expected ROI 12M", "Data Quality Score", "Forecast Reliability Score"):
        assert row[field] in (None, ""), field
    assert row["Recommendation"] in (None, "")
    assert "unsupported_instrument_model" in row["Warnings"]
    assert row["Data Provider"] == before[HEADERS.index("Data Provider")]
    assert row["Last Updated (UTC)"] == before[HEADERS.index("Last Updated (UTC)")]
    assert LIVE_ROW == before
    receipt = writer._write_receipts[("test", "Global_Markets")]["instrument_classes"]
    assert receipt["quarantined"] == receipt["captioned"] == 1
    assert receipt["live_readback"] is False


@pytest.mark.parametrize("page", ["Global_Markets", "Mutual_Funds", "Market_Leaders", "Commodities_FX",
                                  "Top_10_Investments"])
def test_same_review_on_every_final_market_surface_is_idempotent_and_copy_only(page):
    row = observed_row()
    before = copy.deepcopy(row)
    out, stats = policy.apply_reviewed_instrument_rows(page, [row])
    assert_blocked(out[0])
    assert out[0]["asset_class"] == "Bond" and stats["captioned"] == 1
    again, _ = policy.apply_reviewed_instrument_rows(page, out)
    assert again == out and row == before


@pytest.mark.parametrize("field,value", [("currency", "GBX"), ("currency", ""), ("country", "Canada"),
                                        ("exchange", "NASDAQ"), ("exchange", "NOT_NYSE"),
                                        ("name", "DCB Financial Corp."), ("name", False)])
def test_missing_or_conflicting_metadata_withholds_caption_and_class_but_still_blocks(field, value):
    row = observed_row(); row[field] = value
    out, stats = policy.apply_reviewed_instrument_rows("Global_Markets", [row])
    assert_blocked(out[0])
    assert out[0]["name"] is out[0]["asset_class"] is None
    assert stats["captioned"] == 0


@pytest.mark.parametrize("clock", [REVIEWED - timedelta(seconds=1), REVIEWED + timedelta(days=7),
                                  "2026-10-23", False, datetime(2026, 10, 23)])
def test_unverified_or_expired_clock_never_restores_an_equity_or_price(clock):
    out, stats = policy.apply_reviewed_instrument_rows("Global_Markets", [observed_row()], now=clock)
    assert_blocked(out[0])
    assert out[0]["name"] is out[0]["asset_class"] is None
    assert stats["registry_status"] == "UNKNOWN"


@pytest.mark.parametrize("damage", ["missing", "digest", "source", "duplicate_source", "duplicate_json"])
def test_registry_failure_protects_known_identifier_without_claiming_verified_class(tmp_path, damage):
    path = tmp_path / "classes.json"
    data = copy.deepcopy(REGISTRY)
    if damage == "digest": data["bindings"][0]["name"] = "Poisoned issuer"
    if damage in ("source", "duplicate_source"):
        item = data["bindings"][0]
        if damage == "source": item["sources"][0]["url"] = "https://example.org/false"
        else: item["sources"][1] = item["sources"][0]
        item["binding_sha256"] = policy._digest(item)
    if damage == "duplicate_json":
        path.write_text('{"schema":"first","schema":"duplicate","bindings":[]}')
    elif damage != "missing": path.write_text(json.dumps(data))
    out, stats = policy.apply_reviewed_instrument_rows("Global_Markets", [observed_row()], registry_path=path)
    assert_blocked(out[0])
    assert out[0]["name"] is out[0]["asset_class"] is None
    assert stats["registry_status"] == "UNKNOWN"


def test_duplicate_exact_rows_are_blocked_without_selecting_one_reviewed_identity():
    out, stats = policy.apply_reviewed_instrument_rows("Global_Markets", [observed_row(), observed_row()])
    assert len(out) == stats["quarantined"] == 2 and stats["captioned"] == 0
    for row in out:
        assert_blocked(row)
        assert row["name"] is row["asset_class"] is None


@pytest.mark.parametrize("symbol", ["DCBG", "DCBG.L", "DCOM.US", "dcbg.us", "DCBG.US ", [], None])
def test_other_namespaces_and_unlisted_equity_are_exact_original_objects(symbol):
    row = observed_row(); row["symbol"] = symbol
    rows = [row]
    out, stats = policy.apply_reviewed_instrument_rows("Global_Markets", rows)
    assert out is rows and out[0] is row and stats is None


def test_portfolio_manual_inputs_survive_while_unverified_price_derived_values_are_withheld():
    row = observed_row(); row.update(position_qty=100, avg_cost=25, position_cost=2500,
                                    buy_date="2026-10-01", target_weight=10, user_notes="Manual",
                                    position_value=2652, unrealized_pl=152, unrealized_pl_pct=6.08,
                                    actual_weight=5, weight_gap=5, action_flag="ADD", decision="BUY")
    rows = [row]
    out, stats = policy.apply_reviewed_instrument_rows("My_Portfolio", rows)
    assert_blocked(out[0])
    for key in ("position_qty", "avg_cost", "position_cost", "buy_date", "target_weight", "user_notes"):
        assert out[0][key] == row[key]
    for key in ("position_value", "unrealized_pl", "unrealized_pl_pct", "actual_weight", "weight_gap",
                "action_flag", "decision"):
        assert out[0][key] is None
    assert policy.reviewed_quote_rows("My_Portfolio", rows) == out


def test_surface_protection_runs_even_if_optional_surface_invariants_fail(monkeypatch):
    monkeypatch.setattr(engine, "_sai_probe", lambda: None)
    out = engine._apply_surface_invariants_safe([observed_row()], "Global_Markets")
    assert_blocked(out[0])


@pytest.mark.parametrize("method", ["get_enriched_quote_dict", "get_enriched_quote", "get_enriched_quotes",
                                    "get_enriched_quotes_batch", "get_quote", "quote", "fetch_quote"])
def test_public_cached_quote_and_batch_apis_cannot_bypass_quarantine(monkeypatch, method):
    obj = engine.DataEngineV5.__new__(engine.DataEngineV5)
    stale_cached = observed_row()
    stale_cached.update(price=26.52, target_mean_price=35, requested_symbol="DCBG.US")
    before = copy.deepcopy(stale_cached)
    async def cached(_symbol, _page): return stale_cached
    monkeypatch.setattr(obj, "_get_enriched_quote_impl", cached)
    monkeypatch.setattr(engine, "_engine_batch_fprint_enabled", lambda: False)
    target = ["DCBG.US"] if method in {"get_enriched_quotes", "get_enriched_quotes_batch"} else "DCBG.US"
    result = asyncio.run(getattr(obj, method)(target))
    if isinstance(result, list): result = result[0]
    if hasattr(result, "model_dump"): result = result.model_dump()
    assert_blocked(result)
    assert result.get("price") is result.get("target_mean_price") is None
    assert stale_cached == before


def test_noncanonical_writer_geometry_is_rejected_before_unsafe_protection():
    with pytest.raises(ValueError, match="115-column"):
        policy.apply_reviewed_instrument_matrix("Global_Markets", ["Symbol"], [["DCBG.US"]])


def test_stateful_native_update_clears_old_values_instead_of_skipping_json_nulls(monkeypatch):
    class NativeUpdate(FakeSheetsAPI):
        def __init__(self):
            super().__init__()
            self.persisted = [list(HEADERS), list(LIVE_ROW)]

        def execute(self):
            for row_index, row in enumerate(self.requests[-1]["body"]["values"]):
                for col, value in enumerate(row):
                    if value is not None:  # Native Sheets skips JSON null cells.
                        self.persisted[row_index][col] = value
            return {"updatedRows": 2}

    api, writer = NativeUpdate(), sync.SheetsWriter()
    monkeypatch.setattr(writer, "_get_service", lambda: api)
    monkeypatch.setenv("TFB_WARNINGS_COMPACT", "off")
    monkeypatch.setenv("TFB_RANK_OVERALL_PAGE", "enforce")
    for _ in range(2):  # Primary update, then recovery/retry of the original row.
        writer.write_table("test", "Global_Markets", "A1", HEADERS, [LIVE_ROW])
        values = dict(zip(HEADERS, api.persisted[1]))
        assert values["Current Price"] == values["Overall Score"] == values["Recommendation"] == ""
        assert values["Forecast Price 12M"] == values["Rank (Overall)"] == ""
        assert values["Final Action"] == "DO_NOT_INVEST"


def test_final_acquisition_census_cannot_count_a_copied_away_supplier_price_as_valid():
    result = SimpleNamespace(sheet_name="Global_Markets", _stamp_meta={})
    source = [list(LIVE_ROW)]
    acquisition_now = datetime.fromisoformat(LIVE_ROW[HEADERS.index("Last Updated (UTC)")]) + timedelta(minutes=1)
    raw = sync._record_acquisition_census(result, HEADERS, source, ["DCBG.US"], now=acquisition_now)
    assert raw == {"DCBG.US"}  # Reproduces the misleading original supplier success.
    final = sync._record_acquisition_census(result, HEADERS, source, ["DCBG.US"],
                                            origins=raw, now=NOW)
    assert final == set()
    assert result._stamp_meta["requested"] == 1
    assert result._stamp_meta["acquired_fresh"] == 0
    assert result._stamp_meta["acquisition_invalid"] == 1
    assert result._stamp_meta["instrument_class_quarantined"] == 1
    assert source == [LIVE_ROW]


def test_native_portfolio_quarantine_preserves_manual_inputs_without_json_null_retention():
    headers, keys = get_sheet_headers("My_Portfolio"), get_sheet_keys("My_Portfolio")
    data = observed_row(); data.update(position_qty=100, avg_cost=25, position_cost=2500,
                                       buy_date="2026-10-01", target_weight=10, user_notes="Manual",
                                       position_value=2652, unrealized_pl=152, action_flag="ADD", decision="BUY")
    before = [[data.get(key) for key in keys]]
    matrix, _ = policy.apply_reviewed_instrument_matrix("My_Portfolio", headers, before)
    row = dict(zip(keys, matrix[0]))
    assert row["current_price"] == row["position_value"] == row["unrealized_pl"] == ""
    assert row["action_flag"] == row["decision"] == ""
    for key in ("position_qty", "avg_cost", "position_cost", "buy_date", "target_weight", "user_notes"):
        assert row[key] == data[key]


def test_unrelated_portfolio_custom_header_writer_contract_is_unchanged():
    rows = [["OTHER.US", 3, "Manual"]]
    out, stats = policy.apply_reviewed_instrument_matrix("My_Portfolio", ["Symbol", "Qty", "Notes"], rows)
    assert out is rows and stats is None


def test_actual_enriched_route_fallback_preserves_withholding_after_generic_gate_recomputation():
    from routes.enriched_quote import _publish_fallback_row
    guarded = policy.reviewed_quote_rows("", [observed_row()])[0]
    published = _publish_fallback_row(KEYS, HEADERS, guarded, "DCBG.US")
    assert_blocked(published)
    assert published["data_quality_score"] is published["forecast_reliability_score"] is None
    assert published["block_reason"] == policy.REASON
    assert published["final_decision_basis"] == "Reviewed instrument-class quarantine"


def test_strict_portfolio_projection_keeps_manual_inputs_and_reasserts_instrument_reason():
    keys = get_sheet_keys("My_Portfolio")
    row = observed_row(); row.update(position_qty=100, avg_cost=25, user_notes="Manual")
    published = engine._strict_project_row(keys, row)
    assert_blocked(published)
    assert published["position_qty"] == 100 and published["avg_cost"] == 25
    assert published["user_notes"] == "Manual"
    assert published["data_quality_score"] is None and published["block_reason"] == policy.REASON


def test_mixed_native_matrix_does_not_apply_target_logic_to_unhashable_other_symbol():
    other = list(LIVE_ROW); other[HEADERS.index("Symbol")] = []
    rows = [list(LIVE_ROW), other]
    out, stats = policy.apply_reviewed_instrument_matrix("Global_Markets", HEADERS, rows)
    assert stats["quarantined"] == 1
    assert out[0][HEADERS.index("Current Price")] == ""
    assert out[1] == other


@pytest.mark.parametrize("page", ["Global Markets", "global-markets", "My Portfolio", "my-portfolio"])
def test_engine_canonicalizes_alias_context_before_final_and_cached_quote_guards(monkeypatch, page):
    obj = engine.DataEngineV5.__new__(engine.DataEngineV5)
    raw = observed_row(); raw.update(position_qty=100, avg_cost=25, user_notes="Manual")
    async def cached(_symbol, _page): return raw
    monkeypatch.setattr(obj, "_get_enriched_quote_impl", cached)
    result = asyncio.run(obj.get_enriched_quote_dict("DCBG.US", page))
    assert_blocked(result)
    final = engine._apply_surface_invariants_safe([raw], page)[0]
    assert_blocked(final)
    if engine._canonicalize_sheet_name(page) == "My_Portfolio":
        assert result["position_qty"] == final["position_qty"] == 100
        assert result["avg_cost"] == final["avg_cost"] == 25
        assert result["user_notes"] == final["user_notes"] == "Manual"
