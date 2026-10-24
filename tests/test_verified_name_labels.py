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


# Renewal-proof clock (2026-10-21). A weekly renewal (name_label_renewal.yml)
# re-reviews most bindings and keeps the rest unchanged, so one committed
# registry holds several review windows: run 39584281713 renewed 118 and held 4
# that still expire on 19 Oct. A test that applies a specific binding therefore
# evaluates it one minute into THAT binding's own review window: still the
# actual publication clock, never a fabricated authority timestamp.
def clock_for(item):
    return datetime.fromisoformat(item["reviewed_at_utc"]) + timedelta(minutes=1)


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
                                             rows, now=clock_for(item), **kwargs)


def registry_file(tmp_path, transform):
    data = {"schema": DATA["schema"], "label_only": True, "current_authority_only": True,
            "bindings": [binding()]}
    transform(data)
    path = tmp_path / "bindings.json"
    path.write_text(json.dumps(data))
    return path


def test_all_reviewed_bindings_change_only_name_without_promoting_any_guard():
    assert len(BINDINGS) == 125
    assert sum(x["page"] == "Mutual_Funds" for x in BINDINGS) == 41
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
    now = clock_for(item)
    monkeypatch.setattr(labels, "apply_verified_name_labels", lambda p, h, r: original(p, h, r, now=now))
    api = FakeSheetsAPI()
    writer = sync.SheetsWriter()
    monkeypatch.setattr(writer, "_get_service", lambda: api)
    # Compare the genuine final ranking/presentation against a no-caption writer.
    monkeypatch.setattr(labels, "apply_verified_name_labels", lambda p, h, r: (r, None))
    writer.write_table("test-sheet", item["page"], "A1", headers, rows)
    baseline = api.requests[-1]["body"]["values"]
    monkeypatch.setattr(labels, "apply_verified_name_labels", lambda p, h, r: original(p, h, r, now=now))
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
    assert all(word not in raw for word in ("/workspace/", "account", "holdings", "broker", "api_token"))
    loaded, _ = labels._load_registry(labels.REGISTRY_PATH)
    assert len(loaded) == 125
    assert all("eodhd.com" not in source["url"] for item in BINDINGS for source in item["sources"])
    nvei = binding("NVEI.TO")
    assert nvei["native_metadata"] == {"Asset Class": "Equity", "Exchange": "TSX", "Currency": "CAD", "Country": "Canada"}
    assert "Subordinate voting" in nvei["identifiers"]["security_class"]
    assert binding("TRTN.US")["symbol"] == "TRTN.US"
    assert binding("SMAR.US")["identifiers"]["security_class"].startswith("Class A")
    assert binding("SVA.US")["identifiers"]["security_class"] == "caption only; security class not certified"
    assert binding("RYOJ.US")["identifiers"]["security_class"].startswith("Common Shares")
    stm = binding("STM.PA")
    assert stm["name"] == "STMicroelectronics N.V."
    # Review metadata is renewed weekly; identity stays pinned (above and below).
    # The first reviews are lower bounds (a renewal only takes a NEWER review),
    # the window stays within 7 days and the receipt is the binding's own digest.
    for item, first_review in ((nvei, "2026-10-12T12:24:43.455134+00:00"),
                               (stm, "2026-10-12T13:21:08.737969+00:00")):
        reviewed = datetime.fromisoformat(item["reviewed_at_utc"])
        assert reviewed >= datetime.fromisoformat(first_review)
        assert timedelta(0) < datetime.fromisoformat(item["expires_at_utc"]) - reviewed <= timedelta(days=7)
        assert item["review_receipt_sha256"] == labels._binding_digest(item)
    assert stm["identifiers"]["namespace_bridge_receipt_source"] == "https://eodhd.com/api/exchange-symbol-list/PA"
    public_without_approved_bridge = copy.deepcopy(DATA)
    approved = next(item for item in public_without_approved_bridge["bindings"] if item["symbol"] == "STM.PA")
    approved["identifiers"].pop("namespace_bridge_receipt_source")
    assert "eodhd.com" not in json.dumps(public_without_approved_bridge)
    assert "not been independently rehashed" in stm["identifiers"]["namespace_bridge_limitation"]
    assert len(stm["sources"]) == 5


def test_new_us_authority_captions_keep_the_entire_identity_quarantine():
    # The 14 Oct US-authority cohort, selected by its identity marker: its
    # review date moves with every renewal, its exact authority ticker does not.
    reviewed = [item for item in BINDINGS
                if "exact_authority_ticker" in item["identifiers"]]
    assert len(reviewed) == 67
    headers = get_sheet_headers("Global_Markets")
    rows = [row_for(item) for item in reviewed]
    original = copy.deepcopy(rows)
    output = []
    for item, row in zip(reviewed, rows):
        out, receipt = labels.apply_verified_name_labels(
            "Global_Markets", headers, [row], now=clock_for(item))
        assert receipt["applied"] == 1 and receipt["held"] == 0, item["symbol"]
        output.extend(out)
    name_i = headers.index("Name")
    for item, before, after in zip(reviewed, original, output):
        assert item["requires_quarantine"] is True
        assert not any(item["native_metadata"].values())
        assert item["identifiers"]["exact_authority_ticker"] + ".US" == item["symbol"]
        assert len(item["sources"]) == 2
        assert {source["url"].split("/")[2] for source in item["sources"]} == {
            "www.sec.gov", "www.nasdaqtrader.com"}
        assert before[:name_i] + before[name_i + 1:] == after[:name_i] + after[name_i + 1:]
        assert after[headers.index("Warnings")].startswith("identity_quarantined:")
        assert after[headers.index("Current Price")] is None
        assert after[headers.index("Data Provider")] == ""
        assert after[headers.index("Final Action")] == ""
    assert rows == original


def test_registry_entry_capacity_is_finite_independently_of_byte_limit(tmp_path):
    item = binding("ADVB.US")
    item.update(state="Caption only.", identifiers={"CIK": "1"},
                sources=[{"url": "https://www.sec.gov/a", "sha256": "0" * 64,
                          "captured_at_utc": item["reviewed_at_utc"]}])
    entries = []
    for index in range(labels.MAX_BINDINGS + 1):
        entry = copy.deepcopy(item)
        entry["symbol"] = str(index) + ".US"
        entry["review_receipt_sha256"] = labels._binding_digest(entry)
        entries.append(entry)
    data = {"schema": DATA["schema"], "label_only": True, "current_authority_only": True,
            "bindings": entries}
    path = tmp_path / "over-capacity.json"
    path.write_text(json.dumps(data, separators=(",", ":")))
    assert path.stat().st_size < 256_000
    with pytest.raises(ValueError, match="invalid registry schema"):
        labels._load_registry(path)
