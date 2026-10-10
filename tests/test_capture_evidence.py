"""Synthetic offline captures exercise the actual private packet builder."""
import copy
import csv
import datetime as dt
import io
import json
import os
import subprocess
import sys

import pytest

from core.execution_accounting import AccountingError
from core.portfolio_reconciliation import certify_portfolio_inputs
from scripts import tfb_capture_evidence as cli
from tests.test_portfolio_reconciliation import FX, NOW, ROWS, STAMP, evidence


def _body(packet=None):
    return {"holdings": copy.deepcopy(ROWS), "reconciliation_evidence": evidence() if packet is None else packet,
            "fx_rates": copy.deepcopy(FX)}


def _run(tmp_path, body=None, extra=(), *, output=None):
    source = tmp_path / "capture.json"
    source.write_text(json.dumps(_body() if body is None else body))
    output = tmp_path / "private-evidence.json" if output is None else output
    result = subprocess.run([sys.executable, cli.__file__, str(source), "--now", NOW.isoformat(),
                             "--output", str(output), *extra], capture_output=True, text=True, timeout=10, check=False)
    return result, source, output


def _csv_records():
    return [
        {"record_type": "capture", "source_ref": "synthetic://declared-export", "captured_at": STAMP},
        {"record_type": "account", "account_id": "synthetic-account", "positions_asof": STAMP,
         "positions_complete": "true", "cash_asof": STAMP, "cash_complete": "true"},
        {"record_type": "position", "account_id": "synthetic-account", "instrument_id": "synthetic-contract",
         "symbol": "SYNTH", "currency": "USD", "quantity": "12"},
        {"record_type": "cash", "account_id": "synthetic-account", "currency": "USD", "settled_cash": "200",
         "reserved_cash": "20", "reservations_complete": "true"},
        {"record_type": "holding", "row_index": "0", "symbol": "SYNTH.US", "currency": "USD", "quantity": "12"},
        {"record_type": "link", "row_index": "0", "account_id": "synthetic-account",
         "instrument_id": "synthetic-contract", "position_symbol": "SYNTH", "symbol": "SYNTH.US", "currency": "USD"},
        {"record_type": "funding", "account_id": "synthetic-account"},
        {"record_type": "fx", "currency": "USD", "rate_to_sar": "3.75", "asof": STAMP,
         "source_ref": "synthetic://FX-capture"},
        {"record_type": "caller_fx", "currency": "USD", "rate_to_sar": "3.75"},
    ]


def _csv(records):
    handle = io.StringIO()
    writer = csv.DictWriter(handle, fieldnames=cli.CSV_FIELDS)
    writer.writeheader()
    writer.writerows(records)
    return handle.getvalue()


def test_json_cli_writes_only_exact_private_packet_and_nonidentifying_counts(tmp_path):
    result, source, output = _run(tmp_path, extra=("--max-age-check",))
    assert result.returncode == 0 and not result.stderr
    summary = json.loads(result.stdout)
    assert summary["funding_eligible"] and summary["accounts"] == summary["positions"] == 1
    assert summary["evidence_origin"] == "declared_not_authenticated"
    assert "synthetic-account" not in result.stdout and "SYNTH" not in result.stdout
    assert "settled_cash" not in result.stdout and "675" not in result.stdout
    assert json.loads(output.read_text()) == evidence()
    assert output.stat().st_mode & 0o777 == 0o600
    assert json.loads(source.read_text()) == _body()


def test_csv_cli_builds_packet_without_inferring_reserved_or_settled_cash(tmp_path):
    source = tmp_path / "capture.csv"
    source.write_text(_csv(_csv_records()))
    output = tmp_path / "private.json"
    result = subprocess.run([sys.executable, cli.__file__, str(source), "--now", NOW.isoformat(),
                             "--max-age-check", "--output", str(output)], capture_output=True, text=True, timeout=10, check=False)
    assert result.returncode == 0
    packet = json.loads(output.read_text())
    assert packet == evidence()
    report = certify_portfolio_inputs(ROWS, packet, FX, now=NOW)
    assert report["certified_cash_available_sar_exact"] == "675"


@pytest.mark.parametrize("path", ["captured_at", "positions_asof", "cash_asof", "fx_asof"])
@pytest.mark.parametrize("age,eligible", [(900, True), (901, False), (-1, False)])
def test_each_timestamp_uses_900_second_bound_at_explicit_clock(tmp_path, path, age, eligible):
    packet = evidence()
    boundary = (NOW - dt.timedelta(seconds=age)).isoformat()
    # Capture cannot precede component evidence: put every witness at the
    # chosen boundary when testing the capture clock itself.
    if path == "captured_at":
        packet["captured_at"] = boundary
        packet["accounts"][0]["positions_asof"] = boundary
        packet["accounts"][0]["cash_asof"] = boundary
        packet["fx_rates"][0]["asof"] = boundary
    elif path == "fx_asof":
        packet["fx_rates"][0]["asof"] = boundary
    else:
        packet["accounts"][0][path] = boundary
    result, _, output = _run(tmp_path, _body(packet), extra=("--max-age-check",))
    assert (result.returncode == 0) is eligible
    assert output.exists() is eligible


def test_max_age_check_also_checks_nonfunding_cash(tmp_path):
    packet = evidence()
    packet["accounts"].append({"account_id": "synthetic-nonfunding", "positions_complete": True,
                               "positions_asof": STAMP, "positions": [], "cash_complete": True,
                               "cash_asof": (NOW - dt.timedelta(seconds=901)).isoformat(), "cash": []})
    assert certify_portfolio_inputs(ROWS, packet, FX, now=NOW)["funding_eligible"]
    result, _, output = _run(tmp_path, _body(packet), extra=("--max-age-check",))
    assert result.returncode == 2 and not output.exists()
    assert "synthetic-nonfunding" not in result.stdout + result.stderr


@pytest.mark.parametrize("mutation", ["reserve_missing", "coverage_text", "quantity_changed", "fx_basis_changed"])
def test_invalid_capture_leaves_existing_private_destination_untouched(tmp_path, mutation):
    body = _body()
    account = body["reconciliation_evidence"]["accounts"][0]
    if mutation == "reserve_missing":
        del account["cash"][0]["reserved_cash"]
    elif mutation == "coverage_text":
        account["positions_complete"] = "true"
    elif mutation == "quantity_changed":
        account["positions"][0]["quantity"] = "11"
    else:
        body["fx_rates"]["USD"] = "3.76"
    output = tmp_path / "private-evidence.json"
    output.write_text("preserved private contents")
    result, _, _ = _run(tmp_path, body, output=output)
    assert result.returncode == 2
    assert output.read_text() == "preserved private contents"


@pytest.mark.parametrize("alias", ["same", "symlink", "hardlink"])
def test_output_aliases_cannot_overwrite_capture_input(tmp_path, alias):
    source = tmp_path / "capture.json"
    source.write_text(json.dumps(_body()))
    output = source if alias == "same" else tmp_path / "private.json"
    if alias == "symlink":
        output.symlink_to(source)
    elif alias == "hardlink":
        os.link(source, output)
    result, _, _ = _run(tmp_path, output=output)
    assert result.returncode == 2 and json.loads(source.read_text()) == _body()


@pytest.mark.parametrize("mutation", ["truthy_boolean", "missing_reserve", "implicit_account", "extra_balance",
                                       "holding_gap", "duplicate_fx", "unknown_record"])
def test_csv_mapping_refuses_missing_or_ambiguous_export_fields(mutation):
    records = _csv_records()
    if mutation == "truthy_boolean":
        records[1]["positions_complete"] = "1"
    elif mutation == "missing_reserve":
        del records[3]["reserved_cash"]
    elif mutation == "implicit_account":
        records[2]["account_id"] = "synthetic-undeclared"
    elif mutation == "extra_balance":
        records[3]["quantity"] = "999"
    elif mutation == "holding_gap":
        records[4]["row_index"] = "1"
    elif mutation == "duplicate_fx":
        records.append(copy.deepcopy(records[-1]))
    else:
        records[-1]["record_type"] = "available_to_trade"
    with pytest.raises(AccountingError):
        cli.csv_capture(_csv(records))


def test_private_write_is_atomic_and_cleans_staged_file_on_failure(tmp_path, monkeypatch, capsys):
    source = tmp_path / "capture.json"
    source.write_text(json.dumps(_body()))
    output = tmp_path / "private.json"
    output.write_text("original")
    def fail_replace(*args):
        raise OSError("synthetic private path detail")
    monkeypatch.setattr(cli.os, "replace", fail_replace)
    assert cli.main([str(source), "--now", NOW.isoformat(), "--output", str(output)]) == 2
    assert output.read_text() == "original"
    assert not list(tmp_path.glob(".custodian-evidence-*"))
    assert "synthetic private path" not in capsys.readouterr().out


@pytest.mark.parametrize("field,value", [
    ("accounts", None), ("accounts", "PRIVATE-SENTINEL"), ("accounts", [None]),
    ("accounts", [{"account_id": "PRIVATE-SENTINEL", "positions": None}]),
    ("fx_rates", None), ("fx_rates", "PRIVATE-SENTINEL"), ("fx_rates", [None]),
    ("fx_rates", [{"currency": "USD", "rate_to_sar": {"PRIVATE-SENTINEL": "secret"}}]),
    ("holding_links", [None]), ("funding_accounts", [["PRIVATE-SENTINEL"]]),
])
def test_malformed_account_and_fx_shapes_withhold_without_private_logs(tmp_path, field, value):
    packet = evidence()
    packet[field] = value
    result, _, output = _run(tmp_path, _body(packet), extra=("--max-age-check",))
    assert result.returncode == 2 and not output.exists()
    assert "PRIVATE-SENTINEL" not in result.stdout + result.stderr
    assert "Traceback" not in result.stderr


def test_duplicate_json_fields_fail_instead_of_hiding_conflicting_custodian_values(tmp_path, capsys):
    source = tmp_path / "capture.json"
    raw = json.dumps(_body()).replace('"settled_cash": "200"', '"settled_cash": "200", "settled_cash": "PRIVATE-SENTINEL"')
    source.write_text(raw)
    output = tmp_path / "private.json"
    assert cli.main([str(source), "--now", NOW.isoformat(), "--output", str(output)]) == 2
    assert not output.exists() and "PRIVATE-SENTINEL" not in capsys.readouterr().out
