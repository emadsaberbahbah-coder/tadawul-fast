"""Offline broker comparisons retain exact values without creating funding truth."""
import copy
from decimal import Decimal
import hashlib
import json
from pathlib import Path
import stat
import zipfile

import pytest

from scripts import compare_broker_observation as compare
from core.portfolio_reconciliation import certify_portfolio_inputs


def workbook(path, *, duplicate=False):
    """A minimal real XLSX package; no third-party spreadsheet dependency."""
    def cell(ref, value):
        return f'<c r="{ref}" t="inlineStr"><is><t>{value}</t></is></c>' if isinstance(value, str) else f'<c r="{ref}"><v>{value}</v></c>'
    header = ["Symbol", "Currency", "Position Qty", "Current Price", "Position Value", "Avg Cost"]
    rows = [header, ["SYNTH.US", "USD", 3, Decimal("12.34567890123456789"), Decimal("37.03703670370370367"), 10],
            ["OTHER.SR", "SAR", 2, 100, 200, 100]]
    if duplicate:
        rows[0].append("Position_Qty")
        rows[1].append(999)
    body = ''.join(f'<row r="{i}">' + ''.join(cell(f'{chr(65+j)}{i}', value) for j, value in enumerate(row)) + '</row>'
                   for i, row in enumerate(rows, 1))
    main = 'http://schemas.openxmlformats.org/spreadsheetml/2006/main'
    with zipfile.ZipFile(path, "w") as z:
        z.writestr('xl/workbook.xml', f'<workbook xmlns="{main}" xmlns:r="http://schemas.openxmlformats.org/officeDocument/2006/relationships"><sheets><sheet name="My_Portfolio" sheetId="1" r:id="rId1"/><sheet name="_Cash_Snapshot" sheetId="2" r:id="rId2"/></sheets></workbook>')
        z.writestr('xl/_rels/workbook.xml.rels', '<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships"><Relationship Id="rId1" Target="worksheets/sheet1.xml"/><Relationship Id="rId2" Target="worksheets/sheet2.xml"/></Relationships>')
        z.writestr('xl/worksheets/sheet1.xml', f'<worksheet xmlns="{main}"><sheetData>{body}</sheetData></worksheet>')
        z.writestr('xl/worksheets/sheet2.xml', f'<worksheet xmlns="{main}"><sheetData><row r="1">{cell("A1", "Date")}{cell("B1", "Balance SAR")}</row><row r="2">{cell("A2", 46000)}{cell("B2", Decimal("375.0000000000000000375"))}</row></sheetData></worksheet>')
    return path


def observation():
    return {"retrieved_at_utc": "2026-10-14 09:00:00 UTC", "source_asof_utc": None, "account_id": None,
        "positions": {"positions": [{"contract_id": 123, "contract_description": "SYNTH", "currency": "USD",
            "position": "3", "market_price": "12.34567890123456789", "market_value": "37.03703670370370367", "average_price": "10"}]},
        "balances": {"balances": [{"currency": "BASE", "settled_cash": "100.00000000000000001"},
                                   {"currency": "USD", "settled_cash": "100.00000000000000001"}]},
        "summary": {"currency": "USD", "gross_position_value": "37.03703670370370367", "net_liquidation": "137.03703670370370368"},
        "orders": {"orders": [
            {"order_type": "LIMIT", "side": "SELL", "remaining_shares_qty": "3", "limit_price": "13.50", "primary_description": "Sell 3 SYNTH"},
            {"order_type": "STOP", "side": "SELL", "remaining_shares_qty": "3", "primary_description": "Sell 3 SYNTH"}]}}


def run(tmp_path, capsys, *, raw=None, explicit=True, private=True, fx="3.75"):
    source = tmp_path / "capture.json"
    source.write_text(json.dumps(observation() if raw is None else raw))
    sheet = workbook(tmp_path / "export.xlsx")
    args = [str(source), str(sheet)]
    if explicit:
        mapping = tmp_path / "links.json"
        mapping.write_text('{"123":"SYNTH.US"}')
        args += ["--contract-map", str(mapping)]
    if fx is not None:
        args += ["--usd-sar-rate", fx]
    output = tmp_path / "private.json"
    if private:
        args += ["--private", str(output)]
    before = {path: hashlib.sha256(path.read_bytes()).hexdigest() for path in (source, sheet)}
    code = compare.main(args)
    stdout = capsys.readouterr().out
    assert before == {path: hashlib.sha256(path.read_bytes()).hexdigest() for path in before}
    return code, json.loads(stdout), json.loads(output.read_text()) if output.exists() else None, stdout


def test_explicit_mapping_exact_decimal_cash_and_positions_with_private_atomic_report(tmp_path, capsys):
    code, public, report, stdout = run(tmp_path, capsys)
    assert code == 0
    assert public["counts"]["quantity_matches"] == 1
    assert public["counts"]["explicit_contract_links"] == 1
    assert public["counts"]["sheet_outside_observed_cohort"] == 1
    assert public["counts"]["observed_orders"] == 2 and public["counts"]["possible_protective_pairs"] == 1
    assert public["checks"]["entered_cash_matches_explicit_fx_scenario"] is True
    assert public["checks"]["position_row_values_match_summary_gross_to_cent"] is True
    assert public["execution_ready"] is public["funding_eligible"] is False
    assert report["positions"][0]["broker_price"] == "12.34567890123456789"
    assert report["positions"][0]["reported_vs_implied_value_difference"] == "0"
    assert report["cash_comparison"]["difference_sar"] == "0"
    assert report["cash_comparison"]["reserved_cash"] is report["cash_comparison"]["reservations_complete"] is None
    assert report["source_asof_utc"] is None and report["account_id"] is None
    assert report["retrieved_at_utc"] == observation()["retrieved_at_utc"]
    assert stat.S_IMODE((tmp_path / "private.json").stat().st_mode) == 0o600
    assert "reconciliation_evidence" not in report and "schema_version" not in report
    rows = [{"symbol": "SYNTH.US", "currency": "USD", "quantity": 3}]
    assert certify_portfolio_inputs(rows, report, {"USD": 3.75})["funding_eligible"] is False
    for sensitive in ("SYNTH", "OTHER", "123", "100.000", "375", "capture.json", str(tmp_path)):
        assert sensitive not in stdout


def test_default_stdout_no_balances_identifiers_or_private_file_and_no_inferred_fx(tmp_path, capsys):
    code, public, report, stdout = run(tmp_path, capsys, explicit=False, private=False, fx=None)
    assert code == 0 and report is None
    assert public["counts"]["provisional_ticker_links"] == 1
    assert public["checks"]["entered_cash_comparison_available"] is False
    assert public["checks"]["entered_cash_matches_explicit_fx_scenario"] is None
    assert not (tmp_path / "private.json").exists()


def test_successful_orders_do_not_establish_reserves_or_oca_membership(tmp_path, capsys):
    code, public, report, _ = run(tmp_path, capsys)
    assert public["checks"]["orders_observed"] is True
    assert "OCA linkage is unproven" in public["hypotheses"][0]
    assert report["cash_comparison"]["reservations_complete"] is None
    assert report["cash_comparison"]["reserved_cash"] is None
    assert report["execution_ready"] is report["funding_eligible"] is False


def test_failed_orders_are_unknown_instead_of_confirmed_empty(tmp_path, capsys):
    raw = observation()
    raw["orders"] = {"isError": True, "error": "PRIVATE_ACCOUNT_ERROR"}
    code, public, report, stdout = run(tmp_path, capsys, raw=raw)
    assert code == 0 and public["checks"]["orders_observed"] is False
    assert public["counts"]["observed_orders"] == 0
    assert report["cash_comparison"]["reserved_cash"] is None
    assert "PRIVATE_ACCOUNT_ERROR" not in stdout


@pytest.mark.parametrize("field,bad", [("position", True), ("market_price", "NaN"), ("market_price", "Infinity"),
                                      ("contract_id", True), ("position", "1e9999")])
def test_invalid_broker_numbers_fail_without_output_or_input_leaks(tmp_path, capsys, field, bad):
    raw = observation()
    raw["positions"]["positions"][0][field] = bad
    code, public, report, stdout = run(tmp_path, capsys, raw=raw)
    assert code == 2 and public["status"] == "withheld" and report is None
    assert "SYNTH" not in stdout


def test_quantity_disagreement_stays_research_and_does_not_rewrite_workbook(tmp_path, capsys):
    raw = observation()
    raw["positions"]["positions"][0]["position"] = "2.5"
    code, public, report, _ = run(tmp_path, capsys, raw=raw)
    assert code == 0 and public["counts"]["quantity_mismatches"] == 1
    assert report["positions"][0]["broker_quantity"] == "2.5"
    assert report["positions"][0]["sheet_quantity"] == "3"
    assert public["execution_ready"] is False


def test_separate_broker_position_and_summary_values_are_compared_without_forcing_same_vintage(tmp_path, capsys):
    raw = observation()
    raw["summary"]["gross_position_value"] = "36.03"
    code, public, report, stdout = run(tmp_path, capsys, raw=raw)
    assert code == 0
    assert public["checks"]["position_row_values_match_summary_gross_to_cent"] is False
    assert report["native_value_comparison"]["broker_position_rows_total"] == "37.03703670370370367"
    assert report["native_value_comparison"]["summary_gross_minus_position_rows_total"] == "-1.00703670370370367"
    assert report["source_asof_utc"] is None
    assert public["funding_eligible"] is False
    assert "36.03" not in stdout


def test_different_native_currency_never_subtracts_costs_or_compares_partial_values_to_base_summary(tmp_path, capsys):
    raw = observation()
    raw["positions"]["positions"][0]["currency"] = "EUR"
    code, public, report, _ = run(tmp_path, capsys, raw=raw)
    assert code == 0 and public["counts"]["currency_mismatches"] == 1
    assert report["positions"][0]["sheet_currency"] == "USD"
    assert report["positions"][0]["broker_minus_sheet_average_cost"] is None
    assert report["native_value_comparison"]["broker_position_rows_total"] is None
    assert public["checks"]["position_row_values_match_summary_gross_to_cent"] is None


def test_duplicate_native_balance_fails_and_base_row_never_double_counts(tmp_path, capsys):
    raw = observation()
    raw["balances"]["balances"].append(copy.deepcopy(raw["balances"]["balances"][1]))
    code, public, report, _ = run(tmp_path, capsys, raw=raw)
    assert code == 2 and report is None


def test_ambiguous_workbook_quantity_header_refuses_report(tmp_path, capsys):
    source = tmp_path / "capture.json"
    source.write_text(json.dumps(observation()))
    sheet = workbook(tmp_path / "export.xlsx", duplicate=True)
    assert compare.main([str(source), str(sheet)]) == 2
    assert json.loads(capsys.readouterr().out)["status"] == "withheld"


@pytest.mark.parametrize("symbol", ["../../private", "synth.us", "SYNTH US", "MISSING.US"])
def test_invalid_or_unmatched_explicit_symbol_link_is_not_silently_guessed(tmp_path, capsys, symbol):
    source = tmp_path / "capture.json"
    source.write_text(json.dumps(observation()))
    sheet = workbook(tmp_path / "export.xlsx")
    mapping = tmp_path / "mapping.json"
    mapping.write_text(json.dumps({"123": symbol}))
    assert compare.main([str(source), str(sheet), "--contract-map", str(mapping)]) == 2
    assert json.loads(capsys.readouterr().out)["status"] == "withheld"


def test_private_destination_cannot_replace_or_link_to_input(tmp_path, capsys):
    source = tmp_path / "capture.json"
    source.write_text(json.dumps(observation()))
    before = source.read_bytes()
    sheet = workbook(tmp_path / "export.xlsx")
    for destination in (source, tmp_path / "linked.json"):
        if destination != source:
            destination.symlink_to(source)
        assert compare.main([str(source), str(sheet), "--private", str(destination)]) == 2
        assert json.loads(capsys.readouterr().out)["status"] == "withheld"
        assert source.read_bytes() == before


def test_duplicate_json_fields_rejected_without_replacing_existing_private_report(tmp_path, capsys):
    source = tmp_path / "capture.json"
    source.write_text('{"positions":{},"positions":{}}')
    sheet = workbook(tmp_path / "export.xlsx")
    output = tmp_path / "previous.json"
    output.write_text("PRIOR_PRIVATE_REPORT")
    assert compare.main([str(source), str(sheet), "--private", str(output)]) == 2
    assert json.loads(capsys.readouterr().out)["status"] == "withheld"
    assert output.read_text() == "PRIOR_PRIVATE_REPORT"
