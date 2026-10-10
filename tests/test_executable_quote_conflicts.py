"""Supplier contradictions survive native projection and cannot fund a ticket."""
from datetime import datetime, timedelta, timezone
import copy
import json
from pathlib import Path
import shutil
import subprocess

import pytest

from core.analysis import opportunity_builder as ob
from core.data_validity import acquisition_census, row_acquisition
from tests.decision_evidence_fixtures import observed_portfolio


def quote_row():
    instant = datetime.now(timezone.utc).replace(microsecond=0).isoformat()
    return {
        "Symbol": "NEW.SR", "Name": "Synthetic Energy", "Sector": "Energy",
        "Exchange": "Tadawul", "Currency": "SAR", "Current Price": 100,
        "Data Provider": "yahoo", "Last Updated (UTC)": instant,
        "acquisition_status": "success", "acquisition_provider": "yahoo",
        "acquisition_acquired_at": instant, "acquisition_quote_asof": instant,
        "Intrinsic Value": 125, "Expected ROI 12M": 25,
        "Forecast Reliability Score": 85, "Data Quality Score": 95,
        "Risk Bucket": "Low", "Volatility 30D": 4,
        "Avg Volume 30D": 2_500_000, "Recommendation Detail": "BUY",
        "Investability Status": "INVESTABLE",
    }


def check_acquisition(row):
    return row_acquisition(row, datetime.now(timezone.utc), 3600)


@pytest.mark.parametrize("header", ["Primary Provider", "primary_provider", "PRIMARY-PROVIDER"])
@pytest.mark.parametrize("provider", ["snapshot:yahoo", "history", "fallback_error", "none", "eodhd"])
def test_primary_provider_cannot_be_hidden_by_a_clean_provider_receipt(header, provider):
    row = quote_row()
    row[header] = provider
    assert not check_acquisition(row).successful


def test_primary_provider_can_supply_the_only_live_provider_alias():
    row = quote_row()
    row["Primary Provider"] = row.pop("Data Provider")
    assert check_acquisition(row).successful


@pytest.mark.parametrize("header", ["quote_timestamp", "Price Bar TS", "regularMarketTime"])
@pytest.mark.parametrize("bad", ["stale", "date", "naive", "boolean", "zero", "nan", "malformed_epoch",
                                  "stale_epoch", "stale_epoch_ms", "infinite_epoch", "huge_epoch"])
def test_supplier_quote_alias_must_be_a_precise_agreeing_market_instant(header, bad):
    row = quote_row()
    stamp = datetime.fromisoformat(row["acquisition_quote_asof"])
    row[header] = {
        "stale": (stamp - timedelta(days=7)).isoformat(),
        "date": stamp.date().isoformat(), "naive": stamp.replace(tzinfo=None).isoformat(),
        "boolean": True, "zero": 0, "nan": float("nan"),
        "malformed_epoch": str(int(stamp.timestamp())) + ".0",
        "stale_epoch": str(int((stamp - timedelta(days=7)).timestamp())),
        "stale_epoch_ms": str(int((stamp - timedelta(days=7)).timestamp()) * 1000),
        "infinite_epoch": "inf", "huge_epoch": "9" * 400,
    }[bad]
    proof = check_acquisition(row)
    assert proof.status == "INVALID"
    assert proof.reason in {"quote_timestamp_invalid", "quote_timestamp_conflict"}


def test_quote_aliases_agree_by_instant_across_timezone_and_epoch_units():
    row = quote_row()
    stamp = datetime.fromisoformat(row["acquisition_quote_asof"])
    row.update({"quote_timestamp": stamp.astimezone(timezone(timedelta(hours=3))).isoformat(),
                "price_bar_ts": stamp.timestamp(), "regularMarketTime": stamp.timestamp() * 1000})
    proof = check_acquisition(row)
    assert proof.successful and proof.quote_asof == stamp


def test_supplier_epoch_digit_strings_keep_their_declared_units():
    row = quote_row()
    stamp = datetime.fromisoformat(row["acquisition_quote_asof"])
    row.update({"quote_timestamp": str(int(stamp.timestamp())),
                "price_bar_ts": str(int(stamp.timestamp()) * 1000)})
    assert check_acquisition(row).successful


def test_subsecond_supplier_difference_is_not_rounded_into_agreement():
    row = quote_row()
    stamp = datetime.fromisoformat(row["acquisition_quote_asof"])
    row["regularMarketTime"] = (stamp.timestamp() + .001) * 1000
    assert check_acquisition(row).reason == "quote_timestamp_conflict"


def test_duplicate_normalized_quote_headers_retain_the_bad_instant():
    row = quote_row()
    row["quote_timestamp"] = row["acquisition_quote_asof"]
    row["Quote Timestamp"] = "2001-01-01T00:00:00Z"
    assert check_acquisition(row).reason == "quote_timestamp_conflict"


def test_retrieval_and_market_time_remain_distinct_evidence():
    row = quote_row()
    acquired = datetime.fromisoformat(row["acquisition_acquired_at"])
    market = acquired - timedelta(minutes=3)
    row.update({"acquisition_quote_asof": market.isoformat(), "price_bar_ts": market.timestamp()})
    proof = check_acquisition(row)
    assert proof.successful and proof.acquired_at == acquired and proof.quote_asof == market


def test_supplier_time_does_not_create_a_missing_acquisition_receipt():
    row = quote_row()
    row["quote_timestamp"] = row["acquisition_quote_asof"]
    for name in list(row):
        if name.startswith("acquisition_"):
            del row[name]
    proof = check_acquisition(row)
    assert proof.successful and proof.quote_asof is None
    assert proof.acquired_at == datetime.fromisoformat(row["Last Updated (UTC)"])


@pytest.mark.parametrize("header,value", [
    ("Primary Provider", "snapshot:yahoo"),
    ("quote_timestamp", "2001-01-01T00:00:00Z"),
    ("price_bar_ts", "2001-01-01T00:00:00Z"),
    ("regularMarketTime", 978307200),
])
def test_generic_acquisition_census_excludes_contradictory_quote_evidence(header, value):
    clean = quote_row()
    bad = dict(clean, Symbol="BAD.SR")
    bad[header] = value
    headers = list(dict.fromkeys([*clean, *bad]))
    census = acquisition_census(headers, [[row.get(name, "") for name in headers]
                                         for row in (clean, bad)],
                                now=datetime.now(timezone.utc), max_age_seconds=3600,
                                requested=["NEW.SR", "BAD.SR"])
    assert census.successful == {"NEW.SR"}
    assert census.invalid == {"BAD.SR"} and census.unknown == set()


_NATIVE_PROJECTOR = r"""
const fs = require('node:fs'), vm = require('node:vm');
const context = {Logger: {log() {}}, PropertiesService: {getScriptProperties() {
  return {getProperty() {return null;}};
}}};
vm.createContext(context);
vm.runInContext(fs.readFileSync(process.argv[1], 'utf8'), context);
const inputs = JSON.parse(fs.readFileSync(process.argv[2], 'utf8'));
const projected = inputs.map(input => context.dt10PoolRowFromSheetRow_(
  input.values, context.dt10MapHeaderCols_(input.headers), 'Market_Leaders'));
process.stdout.write(JSON.stringify(projected));
"""


@pytest.fixture(scope="module")
def native_quotes(tmp_path_factory):
    """Run the complete checked-in Apps Script, not a Python projection copy."""
    base = quote_row()
    stamp = datetime.fromisoformat(base["acquisition_quote_asof"])
    old = stamp - timedelta(days=7)
    variants = {
        "clean": {}, "primary_only": {"Primary Provider": "yahoo"},
        "primary_snapshot": {"Primary Provider": "snapshot:yahoo"},
        "primary_conflict": {"Primary Provider": "eodhd"},
        "quote_old": {"quote_timestamp": old.isoformat()},
        "quote_date": {"quote_timestamp": stamp.date().isoformat()},
        "bar_old": {"price_bar_ts": old.isoformat()},
        "market_old": {"regularMarketTime": old.timestamp()},
        "market_old_string": {"regularMarketTime": str(int(old.timestamp()))},
        "mixed_quote": {"quote_timestamp": stamp.isoformat(), "Quote Timestamp": old.isoformat()},
        "offset_clean": {"quote_timestamp": stamp.astimezone(timezone(timedelta(hours=3))).isoformat()},
        "epoch_clean": {"price_bar_ts": stamp.timestamp(), "regularMarketTime": stamp.timestamp() * 1000},
        "epoch_strings_clean": {"quote_timestamp": str(int(stamp.timestamp())),
                                 "price_bar_ts": str(int(stamp.timestamp()) * 1000)},
    }
    inputs = []
    for name, changes in variants.items():
        row = {**base, **changes}
        if name == "primary_only":
            del row["Data Provider"]
        inputs.append({"headers": list(row), "values": list(row.values())})
    inputs_path = tmp_path_factory.mktemp("native-quote") / "inputs.json"
    inputs_path.write_text(json.dumps(inputs))
    node = shutil.which("node")
    assert node is not None, "Node is required to validate the native quote adapter"
    source = Path(__file__).resolve().parents[1] / "apps_script/16_Decision_Top10.gs"
    result = subprocess.run([node, "-e", _NATIVE_PROJECTOR, str(source), str(inputs_path)],
                            capture_output=True, text=True, timeout=20, check=True)
    projected = dict(zip(variants, json.loads(result.stdout)))
    for name, changes in variants.items():
        for header, value in changes.items():
            assert projected[name][header] == value
    return projected


@pytest.fixture(autouse=True)
def offline_builder(monkeypatch):
    monkeypatch.setenv("TFB_OPP_ENABLED", "1")
    monkeypatch.setenv("TFB_TICKET_FRESHNESS_GATE", "1")
    monkeypatch.setenv("TFB_T10_PRICE_XCHECK", "off")
    monkeypatch.setattr(ob, "_venue_state", lambda *_args: None)


def build(source):
    return ob.build_opportunity_payload(
        [source], criteria={"trust_gate_enabled": False, "max_weight_pct": 100, "pf_max_sector_pct": 100},
        portfolio=observed_portfolio({"cash_available_sar": 50_000}, {"SAR": 1}),
        fx_rates={"SAR": 1})


@pytest.mark.parametrize("case", ["primary_snapshot", "primary_conflict", "quote_old", "quote_date",
                                  "bar_old", "market_old", "market_old_string", "mixed_quote"])
@pytest.mark.parametrize("age_gate", ["0", "1"])
def test_native_projected_supplier_conflict_cannot_fund_a_public_builder_ticket(
        native_quotes, case, age_gate, monkeypatch):
    monkeypatch.setenv("TFB_TICKET_FRESHNESS_GATE", age_gate)
    source = native_quotes[case]
    before = copy.deepcopy(source)
    payload = build(source)
    assert payload["selected"] == []
    assert any(g["gate"] == "Quote Freshness" and not g["passed"]
               for g in payload["candidates_rows"][0]["gates"])
    assert source == before


@pytest.mark.parametrize("case", ["clean", "primary_only", "offset_clean", "epoch_clean", "epoch_strings_clean"])
def test_clean_native_projected_quote_still_funds_a_public_builder_ticket(native_quotes, case):
    payload = build(native_quotes[case])
    assert payload["selected"] and payload["meta"]["execution_ready"]
    assert payload["selected"][0]["suggested_sar"] > 0
