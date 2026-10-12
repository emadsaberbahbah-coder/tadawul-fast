"""Bounded authority captions cannot repair price, identity or eligibility data."""
from __future__ import annotations

import copy
import hashlib
import json
from datetime import datetime, timedelta

import pytest

from core import verified_name_labels as labels
from core.sheets.schema_registry import get_sheet_headers
from scripts import run_dashboard_sync as sync

DATA = json.loads(labels.REGISTRY_PATH.read_text())
BINDINGS = DATA["bindings"]
NOW = max(datetime.fromisoformat(item["reviewed_at_utc"]) for item in BINDINGS) + timedelta(minutes=1)


def binding(symbol="HWAY.US"):
    return copy.deepcopy(next(x for x in BINDINGS if x["symbol"] == symbol))


def row_for(item, *, headers=None):
    headers = headers or get_sheet_headers(item["page"])
    values = {field: "" for field in headers}
    values.update(item["native_metadata"])
    values.update({"Symbol": item["symbol"], "Current Price": None, "Name": None,
                   "Warnings": "low_data_trust; acquisition_status:unavailable; rank_skipped_low_trust",
                   "Investability Status": "BLOCKED", "Final Action": "DO_NOT_INVEST",
                   "Overall Score": 59.45, "Quality Score": 49.36, "Rank (Overall)": 17,
                   "Expected ROI 1M": -0.009, "Expected ROI 3M": 0.14, "Expected ROI 12M": None,
                   "Last Updated (UTC)": "2026-10-12T10:27:41.214883+00:00"})
    if item["requires_quarantine"]:
        # Real producer tag/blank state in the prior ADVB/RYOJ readback receipt.
        values.update({"Warnings": "identity_quarantined:v6.24.0; sheet_horizon_label:invest_period_label:unknown",
                       "Investability Status": "", "Final Action": ""})
    return [values[field] for field in headers]


def apply(item, rows, *, headers=None, **kwargs):
    return labels.apply_verified_name_labels(item["page"], headers or get_sheet_headers(item["page"]),
                                             rows, now=NOW, **kwargs)


def registry_file(tmp_path, transform):
    data = {"schema": DATA["schema"], "label_only": True, "current_authority_only": True,
            "bindings": [binding()]}
    transform(data)
    path = tmp_path / "bindings.json"
    path.write_text(json.dumps(data))
    return path


def test_all_54_reviewed_bindings_change_only_name_without_promoting_any_guard():
    assert len(BINDINGS) == 54
    assert sum(x["page"] == "Mutual_Funds" for x in BINDINGS) == 39
    for item in BINDINGS:
        headers = get_sheet_headers(item["page"])
        row = row_for(item)
        before = copy.deepcopy(row)
        output, receipt = apply(item, [row])
        expected = list(before)
        expected[headers.index("Name")] = item["name"]
        assert output == [expected], item["symbol"]
        assert row == before
        assert receipt["applied"] == 1 and receipt["held"] == 0
        assert receipt["map_sha256"] == hashlib.sha256(labels.REGISTRY_PATH.read_bytes()).hexdigest()
        assert receipt["evidence"] == "producer_name_only" and receipt["live_readback"] is False


@pytest.mark.parametrize("price", [1, 0.000000000001, "0.000000000001", True, float("nan"),
                                  float("inf"), "NaN", "Infinity", "bad", {}, [], " 0"])
def test_positive_and_malformed_prices_hold_inactive_caption(price):
    item = binding()
    headers = get_sheet_headers(item["page"])
    row = row_for(item)
    row[headers.index("Current Price")] = price
    original = copy.deepcopy(row)
    out, stats = apply(item, [row])
    assert out[0][headers.index("Name")] is None
    assert stats["held"] == 1 and stats["reasons"] == {"priced_or_invalid_price": 1}
    # NaN intentionally cannot be compared for equality; the exact input row is retained.
    assert out[0] is row
    assert row[headers.index("Warnings")] == original[headers.index("Warnings")]


@pytest.mark.parametrize("field", ["Asset Class", "Exchange", "Currency", "Country"])
@pytest.mark.parametrize("value", ["conflicting", None, True])
def test_metadata_drift_or_unavailable_country_never_borrows_a_caption(field, value):
    item = binding("NVEI.TO")
    headers = get_sheet_headers(item["page"])
    row = row_for(item)
    row[headers.index(field)] = value
    out, stats = apply(item, [row])
    assert out == [row] and stats["reasons"] == {"metadata_conflict": 1}


def test_only_explicit_quarantine_may_match_semantically_empty_native_metadata():
    item = binding("ADVB.US")
    headers = get_sheet_headers(item["page"])
    row = row_for(item)
    for field in item["native_metadata"]:
        row[headers.index(field)] = None
    out, stats = apply(item, [row])
    assert stats["applied"] == 1 and out[0][headers.index("Name")] == item["name"]
    assert all(out[0][headers.index(field)] is None for field in item["native_metadata"])
    row[headers.index("Warnings")] = "low_data_trust; not_identity_quarantined:v6.24.0"
    out, stats = apply(item, [row])
    assert out == [row] and stats["reasons"] == {"quarantine_fence_missing": 1}


@pytest.mark.parametrize("field,value", [("Final Action", "INVEST"), ("Final Action", "BUY_NEW"),
                                         ("Final Action", "invest"), ("Final Action", "HOLD"),
                                         ("Investability Status", "ELIGIBLE"),
                                         ("Investability Status", "UNKNOWN"), ("Final Action", {})])
def test_actionable_or_unknown_status_is_held_even_with_low_trust(field, value):
    item = binding()
    headers = get_sheet_headers(item["page"])
    row = row_for(item)
    row[headers.index(field)] = value
    out, stats = apply(item, [row])
    assert out == [row] and stats["reasons"] == {"actionable_or_invalid_status": 1}


def test_low_trust_substrings_and_missing_protective_states_do_not_match():
    item = binding()
    headers = get_sheet_headers(item["page"])
    row = row_for(item)
    for field in ("Investability Status", "Final Action"):
        row[headers.index(field)] = ""
    row[headers.index("Warnings")] = "not_low_data_trust; not_identity_quarantined:v6.24.0"
    out, stats = apply(item, [row])
    assert out == [row] and stats["reasons"] == {"nonactionable_fence_missing": 1}


@pytest.mark.parametrize("name", ["Current provider name", " ", False, {"Name": "bad"}])
def test_any_nonblank_existing_name_is_never_overwritten(name):
    item = binding()
    headers = get_sheet_headers(item["page"])
    row = row_for(item)
    row[headers.index("Name")] = name
    out, stats = apply(item, [row])
    assert out == [row] and stats["reasons"] == {"existing_name": 1}


def test_exact_symbol_page_and_duplicate_fences_do_not_alias_or_choose_a_row():
    item = binding()
    headers = get_sheet_headers(item["page"])
    row = row_for(item)
    for symbol in ("hway.us", "HWAY", "HWAY.US ", "NEW.US"):
        changed = list(row)
        changed[headers.index("Symbol")] = symbol
        out, stats = apply(item, [changed])
        assert out == [changed] and stats["skipped"] == 1
    out, stats = apply(item, [row, list(row)])
    assert out == [row, row] and stats["reasons"] == {"duplicate_symbol": 2}
    out, stats = labels.apply_verified_name_labels("Global_Markets", headers, [row], now=NOW)
    assert out == [row] and stats["reasons"] == {"unregistered_symbol": 1}


@pytest.mark.parametrize("page", ["My_Portfolio", "Market_Leaders", "Commodities_FX", {}, []])
def test_portfolio_and_other_pages_are_the_exact_original_objects(page):
    rows = [["HWAY.US", "Manual holding Name", 100, 41.7, "USD", "My account"]]
    out, stats = labels.apply_verified_name_labels(page, ["Symbol", "Name", "Quantity"], rows, now=NOW)
    assert out is rows and stats is None


@pytest.mark.parametrize("clock", [False, 0, "", "2026-10-12", datetime(2026, 10, 12)])
def test_invalid_explicit_clock_never_defaults_to_a_live_clock(clock):
    item = binding()
    row = row_for(item)
    out, stats = labels.apply_verified_name_labels(item["page"], get_sheet_headers(item["page"]),
                                                  [row], now=clock)
    assert out == [row] and stats["reasons"] == {"invalid_matrix_or_clock": 1}


def test_review_window_uses_actual_publication_clock_and_expires_without_deleting_names():
    item = binding()
    row = row_for(item)
    for now in (datetime.fromisoformat(item["reviewed_at_utc"]) - timedelta(seconds=1),
                datetime.fromisoformat(item["expires_at_utc"])):
        out, stats = labels.apply_verified_name_labels(item["page"], get_sheet_headers(item["page"]),
                                                      [row], now=now)
        assert out == [row] and stats["reasons"] == {"outside_review_window": 1}
    row[get_sheet_headers(item["page"]).index("Name")] = item["name"]
    out, stats = labels.apply_verified_name_labels(item["page"], get_sheet_headers(item["page"]),
                                                  [row], now=datetime.fromisoformat(item["expires_at_utc"]))
    assert out == [row] and stats["reasons"] == {"existing_name": 1}


@pytest.mark.parametrize("change", [
    lambda d: d.update(schema="unknown"), lambda d: d.update(label_only=False),
    lambda d: d["bindings"].append(copy.deepcopy(d["bindings"][0])),
    lambda d: d["bindings"][0].update(name={"bad": "name"}),
    lambda d: d["bindings"][0].update(name="bad\ncontrol"),
    lambda d: d["bindings"][0].update(name="x" * 257),
    lambda d: d["bindings"][0].update(native_metadata={}),
    lambda d: d["bindings"][0]["native_metadata"].update(Country=""),
    lambda d: d["bindings"][0].update(review_receipt_sha256="bad"),
    lambda d: d["bindings"][0].update(expires_at_utc=(NOW + timedelta(days=8)).isoformat()),
    lambda d: d["bindings"][0]["sources"][0].update(sha256="bad"),
    lambda d: d["bindings"][0]["sources"][0].update(url="https://user:secret@www.sec.gov/a"),
    lambda d: d["bindings"][0]["sources"][0].update(url="https://www.sec.gov:invalid/a"),
    lambda d: d["bindings"][0]["sources"][0].update(url="https://www.sec.gov/a?api_token=secret"),
    lambda d: d["bindings"][0]["sources"][0].update(captured_at_utc=(NOW + timedelta(days=1)).isoformat()),
])
def test_malformed_registry_or_unverifiable_source_holds_entire_map(tmp_path, change):
    path = registry_file(tmp_path, change)
    item = binding()
    row = row_for(item)
    out, stats = apply(item, [row], registry_path=path)
    assert out == [row] and stats["registry_status"] == "unavailable"
    assert stats["reasons"] == {"invalid_registry": 1}


@pytest.mark.parametrize("raw", ['{"schema":"x","schema":"y"}', '{', '', 'x' * 256001])
def test_missing_corrupt_duplicate_and_oversized_map_hold(tmp_path, raw):
    path = tmp_path / "bad.json"
    path.write_text(raw)
    item = binding()
    row = row_for(item)
    out, stats = apply(item, [row], registry_path=path)
    assert out == [row] and stats["reasons"] == {"invalid_registry": 1}
    path.unlink()
    out, stats = apply(item, [row], registry_path=path)
    assert out == [row] and stats["reasons"] == {"invalid_registry": 1}


def test_well_formed_public_binding_tampering_is_held_without_a_matching_review_digest(tmp_path):
    path = registry_file(tmp_path, lambda data: data["bindings"][0].update(name="Another issuer"))
    item = binding()
    row = row_for(item)
    out, stats = apply(item, [row], registry_path=path)
    assert out == [row] and stats["reasons"] == {"invalid_registry": 1}


class FakeSheetsAPI:
    def __init__(self):
        self.requests = []

    def spreadsheets(self):
        return self

    def values(self):
        return self

    def update(self, **kwargs):
        self.requests.append(copy.deepcopy(kwargs))
        return self

    def execute(self):
        return {"updatedRows": len(self.requests[-1]["body"]["values"])}


def test_final_primary_and_repeat_recovery_writer_retains_caption_and_truthful_receipt(monkeypatch):
    item = binding("PE&OLES.MX")
    headers = get_sheet_headers(item["page"])
    rows = [row_for(item)]
    for flag in ("TFB_SYNC_OHLC_FILLGUARD", "TFB_SYNC_OHLC_FILL_GUARD", "TFB_WARNINGS_COMPACT"):
        monkeypatch.setenv(flag, "off")
    monkeypatch.setenv("TFB_RANK_OVERALL_PAGE", "enforce")
    # Stable tests use the actual reviewed window, not a fabricated authority timestamp.
    original = labels.apply_verified_name_labels
    monkeypatch.setattr(labels, "apply_verified_name_labels", lambda p, h, r: original(p, h, r, now=NOW))
    api = FakeSheetsAPI()
    writer = sync.SheetsWriter()
    monkeypatch.setattr(writer, "_get_service", lambda: api)
    # Compare the genuine final ranking/presentation against a no-caption writer.
    monkeypatch.setattr(labels, "apply_verified_name_labels", lambda p, h, r: (r, None))
    writer.write_table("test-sheet", item["page"], "A1", headers, rows)
    baseline = api.requests[-1]["body"]["values"]
    monkeypatch.setattr(labels, "apply_verified_name_labels", lambda p, h, r: original(p, h, r, now=NOW))
    for _ in range(2):
        writer.write_table("test-sheet", item["page"], "A1", headers, rows)
        expected = copy.deepcopy(baseline)
        expected[1][headers.index("Name")] = item["name"]
        assert api.requests[-1]["body"]["values"] == expected
        assert rows[0][headers.index("Name")] is None
        receipt = writer._write_receipts[("test-sheet", item["page"])]
        assert receipt["name_labels"]["applied_symbols"] == ["PE&OLES.MX"]
        assert receipt["name_labels"]["live_readback"] is False
        assert receipt["written_payload_sha8"] == sync._payload_sha8(headers, expected[1:])
        assert receipt["evidence"] == "successful_RAW_update"


def test_committed_registry_is_only_public_authority_identity_data():
    raw = labels.REGISTRY_PATH.read_text()
    assert all(word not in raw for word in ("/workspace/", "account", "holdings", "broker", "api_token", "eodhd.com"))
    loaded, _ = labels._load_registry(labels.REGISTRY_PATH)
    assert len(loaded) == 54
    nvei = binding("NVEI.TO")
    assert nvei["native_metadata"] == {"Asset Class": "Equity", "Exchange": "TSX", "Currency": "CAD", "Country": "Canada"}
    assert "Subordinate voting" in nvei["identifiers"]["security_class"]
    assert binding("TRTN.US")["symbol"] == "TRTN.US"
    assert binding("SMAR.US")["identifiers"]["security_class"].startswith("Class A")
    assert binding("SVA.US")["identifiers"]["security_class"] == "caption only; security class not certified"
    assert binding("RYOJ.US")["identifiers"]["security_class"].startswith("Common Shares")
    assert nvei["reviewed_at_utc"] == "2026-10-12T12:24:43.455134+00:00"
