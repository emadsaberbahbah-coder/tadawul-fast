"""Synthetic execution IDs/accounts exercise the real offline importer.

Economic witnesses reproduce TFB-03; no live broker IDs or records are fixtures.
"""
from __future__ import annotations

import copy
import datetime as dt
from decimal import Decimal
import json
import os
import subprocess
import sys

import pytest

from core import execution_accounting as accounting
from scripts import tfb_import_executions as importer

NOW = dt.datetime(2026, 10, 8, 12, tzinfo=dt.timezone.utc)
ACCOUNT = "synthetic-account"


def _fill(trade_id="synthetic-fill-1", **changes):
    fill = {"trade_id": trade_id, "order_id": "synthetic-order", "symbol": "DDI.US",
            "currency": "USD", "side": "BUY", "size": 100, "price": "12.71",
            "commission": "1.99", "trade_time": "2026-09-08T13:55:45Z", "net_amount": "1271"}
    fill.update(changes)
    return fill


def _snapshot():
    return {"trades": [_fill(), _fill("synthetic-fill-2", size=129, price="12.72",
                                     commission="2.5671", net_amount="1640.88")]}


def _replay(snapshot=None, **kwargs):
    return accounting.replay_executions(_snapshot() if snapshot is None else snapshot,
        source_ref="synthetic://captured-export", account_id=ACCOUNT, now_utc=NOW, **kwargs)


def _reference():
    return {"account_id": ACCOUNT, "symbol": "DDI.US", "currency": "USD", "quantity": 229,
            "total_cost_native": "2917.1207", "source_ref": "synthetic://total-basis-witness"}


def _write(path, value):
    path.write_text(json.dumps(value), encoding="utf-8")


def _arguments(tmp_path):
    source = tmp_path / "synthetic-snapshot.json"
    state = tmp_path / "private-replay.json"
    report = tmp_path / "private-report.json"
    _write(source, _snapshot())
    args = [str(source), "--account-id", ACCOUNT, "--state", str(state), "--report", str(report),
            "--now", NOW.isoformat()]
    return source, state, report, args


def _cli(args, *, cwd=None):
    return subprocess.run([sys.executable, importer.__file__, *args], cwd=cwd,
                          text=True, capture_output=True, timeout=20)


def test_split_fill_replay_exactly_reconciles_principal_and_keeps_charges_unclassified():
    snapshot = _snapshot()
    snapshot["cost_basis_references"] = [_reference()]
    state, report = _replay(snapshot)
    summary = report["summaries"][0]
    assert report["status"] == "partial" and report["execution_count"] == 2
    assert summary["quantity"] == "229" and summary["gross_native"] == "2911.88"
    assert summary["reported_commission_native"] == "4.5571"
    assert summary["gross_plus_reported_commission_native"] == "2916.4371"
    assert summary["referenced_total_cost_native"] == "2917.1207"
    assert summary["unclassified_residual_native"] == "0.6836"
    assert summary["fee_reconciliation_status"] == "unverified"
    assert summary["net_proceeds_native"] is None
    assert state["executions"][0]["commission_status"] == "broker_reported_unreconciled"
    assert json.loads(state["executions"][0]["raw_payload_json"])["net_amount"] == "1271"


def test_current_native_lot_reader_witness_still_differs_from_exact_execution_replay():
    from scripts import run_dashboard_sync as sync

    class Reader:
        def read_values(self, *args):
            return [["Symbol", "Ccy", "Status", "Buy Price", "Shares", "Buy Fees"],
                    ["DDI.US", "USD", "Active", "12.72", 229, "5.25"]]

    old = sync._read_cost_basis(Reader(), "synthetic-workbook")
    _, report = _replay()
    assert old["DDI.US"]["native_cost"] == 2918.13
    assert report["summaries"][0]["gross_native"] == "2911.88"
    # The importer does not overwrite the existing lot reader or workbook.
    assert old["DDI.US"]["buy_fees"] == 5.25


def test_reimport_duplicate_fills_and_new_capture_provenance_do_not_change_state_or_report():
    first, report = _replay()
    snapshot = _snapshot()
    snapshot["trades"].extend(copy.deepcopy(snapshot["trades"]))
    again, second_report = _replay(snapshot, previous_state=first)
    assert again == first and second_report == report


@pytest.mark.parametrize("field,value", [("price", "12.73"), ("commission", "2"),
    ("trade_time", "2026-09-08T13:56:45Z"), ("order_id", "different-synthetic-order"),
    ("net_amount", "unknown changed source value")])
def test_existing_trade_id_with_changed_payload_refuses_replay_without_mutating_caller(field, value):
    previous, _ = _replay()
    before = copy.deepcopy(previous)
    snapshot = _snapshot()
    snapshot["trades"][0][field] = value
    with pytest.raises(accounting.AccountingError, match="conflicting execution"):
        _replay(snapshot, previous_state=previous)
    assert previous == before


def test_trade_id_is_scoped_by_explicit_account_and_missing_context_never_infers_it():
    snapshot = {"trades": [_fill(account_id="synthetic-account-a"),
                            _fill(account_id="synthetic-account-b")]}
    state, report = accounting.replay_executions(snapshot, source_ref="synthetic://export", now_utc=NOW)
    assert len(state["executions"]) == report["execution_count"] == 2
    with pytest.raises(accounting.AccountingError, match="account context"):
        accounting.replay_executions(_snapshot(), source_ref="synthetic://export", now_utc=NOW)
    with pytest.raises(accounting.AccountingError, match="account context"):
        _replay(snapshot)


def test_explicit_per_trade_account_mapping_handles_missing_feed_accounts():
    mapping = {fill["trade_id"]: ACCOUNT for fill in _snapshot()["trades"]}
    state, _ = accounting.replay_executions(_snapshot(), source_ref="synthetic://export",
                                          account_mapping=mapping, now_utc=NOW)
    assert {event["account_id"] for event in state["executions"]} == {ACCOUNT}
    with pytest.raises(accounting.AccountingError, match="account context"):
        accounting.replay_executions(_snapshot(), source_ref="synthetic://export",
                                    account_mapping={"synthetic-fill-1": ACCOUNT}, now_utc=NOW)


@pytest.mark.parametrize("mapping", [[], "", False, 0])
def test_falsey_malformed_account_mapping_cannot_become_an_empty_default(mapping):
    with pytest.raises(accounting.AccountingError, match="mapping must be an object"):
        accounting.replay_executions({"trades": [_fill(account_id=ACCOUNT)]},
                                    source_ref="synthetic://export", account_mapping=mapping, now_utc=NOW)


@pytest.mark.parametrize("field", ["size", "price"])
@pytest.mark.parametrize("value", [0, -0.0, "-0", -1, True, False, "", "NaN", "Infinity", "1E100000"])
def test_invalid_quantity_and_price_fail_closed(field, value):
    with pytest.raises(accounting.AccountingError):
        _replay({"trades": [_fill(**{field: value})]})


@pytest.mark.parametrize("value", [-0.0, "-0", -1, True, False, "", "unknown", "NaN", "Infinity"])
def test_invalid_reported_commission_cannot_become_zero(value):
    with pytest.raises(accounting.AccountingError):
        _replay({"trades": [_fill(commission=value)]})


@pytest.mark.parametrize("commission,reported_count", [(None, 0), ("0", 1)])
def test_unreported_and_explicit_zero_commission_remain_distinct(commission, reported_count):
    state, report = _replay({"trades": [_fill(commission=commission, net_amount="1271")]})
    assert report["summaries"][0]["commission_reported_count"] == reported_count
    assert report["summaries"][0]["commission_unreported_count"] == 1 - reported_count
    assert report["summaries"][0]["fee_reconciliation_status"] == "unverified"
    assert state["executions"][0]["reported_commission_native"] == (None if commission is None else "0")


@pytest.mark.parametrize("field", ["commission_currency", "fee_currency"])
@pytest.mark.parametrize("value", ["SAR", "EUR", "", None, False, 0, "US", "US dollars"])
def test_explicit_mismatched_or_unsupported_fee_currency_cannot_be_summed_as_native(field, value):
    with pytest.raises(accounting.AccountingError, match=field):
        _replay({"trades": [_fill(**{field: value})]})


@pytest.mark.parametrize("fields", [{"commission_currency": "USD"}, {"fee_currency": "usd"},
                                   {"commission_currency": "USD", "fee_currency": "USD"}])
def test_explicit_same_currency_commission_retains_exact_amount_and_raw_evidence(fields):
    state, report = _replay({"trades": [_fill(**fields)]})
    summary = report["summaries"][0]
    assert summary["currency"] == "USD" and summary["gross_plus_reported_commission_native"] == "1272.99"
    raw = json.loads(state["executions"][0]["raw_payload_json"])
    assert all(raw[field] == value for field, value in fields.items())


def test_explicit_cross_currency_fee_is_refused_even_when_commission_is_unreported():
    with pytest.raises(accounting.AccountingError, match="commission_currency"):
        _replay({"trades": [_fill(commission=None, commission_currency="SAR")]})


def test_two_disagreeing_fee_currency_aliases_cannot_hide_each_other():
    with pytest.raises(accounting.AccountingError, match="fee_currency"):
        _replay({"trades": [_fill(commission_currency="USD", fee_currency="SAR")]})


def test_previously_accepted_cross_currency_commission_state_is_refused_even_with_consistent_hashes():
    state, _ = _replay()
    event = state["executions"][0]
    raw = json.loads(event["raw_payload_json"])
    raw["commission_currency"] = "SAR"
    event["raw_payload_json"] = json.dumps(raw, sort_keys=True, separators=(",", ":"))
    event["raw_payload_sha256"] = accounting.payload_fingerprint(raw)
    state["state_digest"] = accounting.payload_fingerprint({key: value for key, value in state.items()
                                                           if key != "state_digest"})
    with pytest.raises(accounting.AccountingError, match="commission_currency"):
        _replay(previous_state=state)


@pytest.mark.parametrize("value", [None, "", "2026-09-08", "2026-09-08T13:55:45",
    "2026-09-08T13:55Z", "2026-02-30T13:55:45Z", "2026-10-09T13:55:45Z"])
def test_missing_incomplete_invalid_and_future_execution_times_refuse(value):
    with pytest.raises(accounting.AccountingError):
        _replay({"trades": [_fill(trade_time=value)]})


def test_explicit_timezone_offset_normalizes_to_full_utc():
    state, _ = _replay({"trades": [_fill(trade_time="2026-09-08T16:55:45+03:00")]})
    assert state["executions"][0]["trade_time_utc"] == "2026-09-08T13:55:45Z"


@pytest.mark.parametrize("value", [None, "", "US", "USDD", True])
def test_currency_is_required_and_explicit(value):
    with pytest.raises(accounting.AccountingError):
        _replay({"trades": [_fill(currency=value)]})


def test_conflicting_same_symbol_currencies_or_quantity_aliases_refuse():
    with pytest.raises(accounting.AccountingError, match="currencies"):
        _replay({"trades": [_fill(), _fill("synthetic-fill-2", currency="SAR")]})
    with pytest.raises(accounting.AccountingError, match="quantity"):
        _replay({"trades": [_fill(quantity=99)]})


def test_state_digest_and_stored_economics_are_validated_on_replay():
    state, _ = _replay()
    state["executions"][0]["gross_native"] = "1"
    with pytest.raises(accounting.AccountingError, match="digest"):
        _replay(previous_state=state)
    state["state_digest"] = accounting.payload_fingerprint({key: value for key, value in state.items()
                                                           if key != "state_digest"})
    with pytest.raises(accounting.AccountingError, match="stored execution"):
        _replay(previous_state=state)


def _with_proposals():
    snapshot = _snapshot()
    snapshot["trades"].append(_fill("synthetic-sbac-sell", symbol="SBAC.US", side="SELL", size=21,
        price="167.3405", commission="1.99", net_amount="3514.1505", trade_time="2026-09-24T13:52:01Z"))
    ddi = {"Symbol": "DDI.US", "Ccy": "USD", "Shares": 229, "Buy Price": "12.72",
           "Buy Fees": "5.25", "Cost Basis": "2918.13", "Notes": "original synthetic ledger text"}
    sbac = {"Symbol": "SBAC.US", "Ccy": "USD", "Shares": 21, "Sell Date": "2026-08-24",
            "Notes": "preserve original synthetic row"}
    snapshot["original_rows"] = [{"row_ref": "synthetic://ledger/DDI-row", "account_id": ACCOUNT, "raw_row": ddi},
                                  {"row_ref": "synthetic://ledger/SBAC-row", "account_id": ACCOUNT, "raw_row": sbac}]
    snapshot["cost_basis_references"] = [_reference()]
    snapshot["amendment_requests"] = [
        {"kind": "purchase_cost", "original_row_ref": "synthetic://ledger/DDI-row",
         "expected_original_sha256": accounting.payload_fingerprint(ddi),
         "execution_keys": [{"account_id": ACCOUNT, "trade_id": "synthetic-fill-1"},
                            {"account_id": ACCOUNT, "trade_id": "synthetic-fill-2"}]},
        {"kind": "sale_date", "original_row_ref": "synthetic://ledger/SBAC-row",
         "expected_original_sha256": accounting.payload_fingerprint(sbac),
         "execution_keys": [{"account_id": ACCOUNT, "trade_id": "synthetic-sbac-sell"}]}]
    return snapshot


def test_append_only_conditional_sbac_and_ddi_proposals_preserve_original_rows():
    snapshot = _with_proposals()
    original_snapshot = copy.deepcopy(snapshot)
    state, report = _replay(snapshot)
    proposals = {proposal["kind"]: proposal for proposal in report["amendment_proposals"]}
    assert proposals["sale_date"]["details"]["original_value"] == "2026-08-24"
    assert proposals["sale_date"]["details"]["proposed_execution_date_utc"] == "2026-09-24"
    assert proposals["sale_date"]["details"]["proposed_execution_time_utc"] == "2026-09-24T13:52:01Z"
    assert proposals["purchase_cost"]["details"]["gross_native"] == "2911.88"
    assert proposals["purchase_cost"]["details"]["unclassified_residual_native"] == "0.6836"
    assert all(proposal["application_status"] == "conditional_proposal_only" for proposal in proposals.values())
    assert snapshot == original_snapshot
    again, again_report = _replay(snapshot, previous_state=state)
    assert again == state and again_report == report


@pytest.mark.parametrize("record,field,value", [
    ("original_rows", "sha256", "0" * 64),
    ("cost_basis_references", "raw_payload_sha256", "0" * 64),
    ("cost_basis_references", "total_cost_native", "1"),
    ("amendment_proposals", "proposal_id", "0" * 64),
    ("amendment_proposals", "application_status", "applied"),
])
def test_recomputed_outer_digest_cannot_hide_contradictory_stored_proofs(record, field, value):
    state, _ = _replay(_with_proposals())
    state[record][0][field] = value
    state["state_digest"] = accounting.payload_fingerprint({key: value for key, value in state.items()
                                                           if key != "state_digest"})
    with pytest.raises(accounting.AccountingError):
        _replay(previous_state=state)


@pytest.mark.parametrize("mutation", ["fingerprint", "unknown_original", "unknown_fill", "wrong_account",
                                      "wrong_currency", "wrong_quantity", "duplicate_fill"])
def test_amendment_requires_original_fingerprint_and_exact_execution_linkage(mutation):
    snapshot = _with_proposals()
    if mutation == "fingerprint":
        snapshot["amendment_requests"][0]["expected_original_sha256"] = "0" * 64
    elif mutation == "unknown_original":
        snapshot["amendment_requests"][0]["original_row_ref"] = "synthetic://missing"
    elif mutation == "unknown_fill":
        snapshot["amendment_requests"][0]["execution_keys"][0]["trade_id"] = "synthetic-missing"
    elif mutation == "wrong_account":
        snapshot["original_rows"][0]["account_id"] = "synthetic-wrong-account"
    else:
        raw = snapshot["original_rows"][0]["raw_row"]
        if mutation == "wrong_currency": raw["Ccy"] = "SAR"
        elif mutation == "wrong_quantity": raw["Shares"] = 230
        else:
            snapshot["amendment_requests"][0]["execution_keys"][1] = copy.deepcopy(
                snapshot["amendment_requests"][0]["execution_keys"][0])
        snapshot["amendment_requests"][0]["expected_original_sha256"] = accounting.payload_fingerprint(raw)
    with pytest.raises(accounting.AccountingError):
        _replay(snapshot)


def test_total_cost_reference_cannot_claim_a_different_buy_quantity():
    snapshot = _snapshot()
    reference = _reference()
    reference["quantity"] = 230
    snapshot["cost_basis_references"] = [reference]
    with pytest.raises(accounting.AccountingError, match="reference quantity"):
        _replay(snapshot)


def test_real_cli_preserves_exact_json_numeric_precision_and_raw_hash(tmp_path):
    source, state_path, report_path, args = _arguments(tmp_path)
    source.write_text('{"trades":[{"trade_id":"synthetic-precision","symbol":"SYNTH.US",'
        '"currency":"USD","side":"BUY","size":100,"price":0.12345678901234567890123456789,'
        '"trade_time":"2026-09-08T13:55:45Z"}]}')
    result = _cli(args, cwd=tmp_path)
    assert result.returncode == 0, result.stderr
    state = json.loads(state_path.read_text())
    report = json.loads(report_path.read_text())
    assert report["summaries"][0]["gross_native"] == "12.345678901234567890123456789"
    raw = json.loads(state["executions"][0]["raw_payload_json"], parse_float=Decimal)
    assert raw["price"] == Decimal("0.12345678901234567890123456789")
    assert accounting.payload_fingerprint(raw) == state["executions"][0]["raw_payload_sha256"]
    assert "synthetic-precision" not in result.stdout


def test_real_cli_reimport_is_byte_identical_and_conflict_retains_both_files(tmp_path):
    source, state, report, args = _arguments(tmp_path)
    assert _cli(args).returncode == 0
    state_before, report_before = state.read_bytes(), report.read_bytes()
    assert _cli(args).returncode == 0
    assert state.read_bytes() == state_before and report.read_bytes() == report_before
    snapshot = _snapshot()
    snapshot["trades"][1]["price"] = "12.73"
    _write(source, snapshot)
    result = _cli(args)
    assert result.returncode == 2 and "conflicting execution" in result.stderr
    assert state.read_bytes() == state_before and report.read_bytes() == report_before


@pytest.mark.parametrize("field,value", [("commission_currency", "SAR"), ("fee_currency", "EUR"),
                                        ("commission_currency", ""), ("fee_currency", None)])
def test_real_cli_refuses_bad_fee_currency_without_replacing_accepted_files(tmp_path, field, value):
    source, state, report, args = _arguments(tmp_path)
    assert _cli(args).returncode == 0
    accepted = state.read_bytes(), report.read_bytes()
    snapshot = {"trades": [_fill("synthetic-new-fee-currency-witness", **{field: value})]}
    _write(source, snapshot)
    result = _cli(args)
    assert result.returncode == 2 and field in result.stderr
    assert (state.read_bytes(), report.read_bytes()) == accepted


@pytest.mark.parametrize("alias", ["source_state", "source_report", "state_report", "symlink", "hardlink", "lock_source"])
def test_real_cli_refuses_path_aliases_without_changing_input(tmp_path, alias):
    source, state, report, args = _arguments(tmp_path)
    original = source.read_bytes()
    if alias == "source_state": args[args.index("--state") + 1] = str(source)
    elif alias == "source_report": args[args.index("--report") + 1] = str(source)
    elif alias == "state_report": args[args.index("--report") + 1] = str(state)
    elif alias == "symlink": state.symlink_to(source)
    elif alias == "hardlink": os.link(source, report)
    else:
        lock_source = state.with_name(state.name + ".lock")
        os.rename(source, lock_source)
        source = lock_source
        args[0] = str(lock_source)
    result = _cli(args)
    assert result.returncode == 2
    assert source.read_bytes() == original


def test_real_cli_refuses_concurrent_writer_lock_without_lost_updates(tmp_path):
    import fcntl
    _, state, report, args = _arguments(tmp_path)
    with state.with_name(state.name + ".lock").open("a+") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        result = _cli(args)
        assert result.returncode == 2 and "busy" in result.stderr
        assert not state.exists() and not report.exists()
    assert _cli(args).returncode == 0


@pytest.mark.parametrize("failed_path", ["stage_report", "replace_state"])
def test_io_failure_before_state_acceptance_retains_previous_state_and_report(tmp_path, monkeypatch, failed_path):
    source, state, report, args = _arguments(tmp_path)
    assert importer.main(args) == 0
    previous = state.read_bytes(), report.read_bytes()
    snapshot = _snapshot()
    snapshot["trades"].append(_fill("synthetic-fill-3"))
    _write(source, snapshot)
    if failed_path == "stage_report":
        stage = importer._stage
        def fail(path, value):
            if path == report: raise OSError("synthetic stage failure")
            return stage(path, value)
        monkeypatch.setattr(importer, "_stage", fail)
    else:
        replace = importer.os.replace
        def fail(first, second):
            if second == state: raise OSError("synthetic replace failure")
            return replace(first, second)
        monkeypatch.setattr(importer.os, "replace", fail)
    assert importer.main(args) == 2
    assert (state.read_bytes(), report.read_bytes()) == previous
    assert not list(tmp_path.glob(".*.tmp"))


def test_report_failure_after_state_commit_is_explicit_and_retry_regenerates_sidecar(tmp_path, monkeypatch):
    source, state, report, args = _arguments(tmp_path)
    assert importer.main(args) == 0
    old_report = report.read_bytes()
    snapshot = _snapshot()
    snapshot["trades"].append(_fill("synthetic-fill-3"))
    _write(source, snapshot)
    replace = importer.os.replace
    def fail(first, second):
        if second == report: raise OSError("synthetic report failure")
        return replace(first, second)
    with monkeypatch.context() as patch:
        patch.setattr(importer.os, "replace", fail)
        assert importer.main(args) == 3
    assert len(json.loads(state.read_text())["executions"]) == 3
    assert report.read_bytes() == old_report
    assert json.loads(old_report)["state_digest"] != json.loads(state.read_text())["state_digest"]
    accepted_state = state.read_bytes()
    assert importer.main(args) == 0
    assert state.read_bytes() == accepted_state
    assert json.loads(report.read_text())["state_digest"] == json.loads(state.read_text())["state_digest"]


def test_cli_account_mapping_and_conditional_proposals_use_the_same_real_flow(tmp_path):
    source, state, report, args = _arguments(tmp_path)
    snapshot = _with_proposals()
    _write(source, snapshot)
    mapping_path = tmp_path / "synthetic-accounts.json"
    _write(mapping_path, {fill["trade_id"]: ACCOUNT for fill in snapshot["trades"]})
    index = args.index("--account-id")
    args[index:index + 2] = ["--account-map", str(mapping_path)]
    assert _cli(args).returncode == 0
    assert len(json.loads(report.read_text())["amendment_proposals"]) == 2
    first = state.read_bytes(), report.read_bytes()
    assert _cli(args).returncode == 0
    assert (state.read_bytes(), report.read_bytes()) == first


def test_real_cli_incremental_buys_keep_historic_reference_and_proposals_immutable(tmp_path):
    source, state, report, args = _arguments(tmp_path)
    initial = _with_proposals()
    _write(source, initial)
    assert _cli(args).returncode == 0
    old_state = json.loads(state.read_text())
    followup = _fill("synthetic-followup", size=1, price="12.73", commission=None, net_amount="12.73")
    _write(source, {"trades": [followup]})
    result = _cli(args)
    assert result.returncode == 0, result.stderr
    accepted = json.loads(state.read_text())
    reconciliation = json.loads(report.read_text())
    assert len(accepted["executions"]) == 4
    assert accepted["cost_basis_references"] == old_state["cost_basis_references"]
    assert accepted["amendment_proposals"] == old_state["amendment_proposals"]
    ddi = next(summary for summary in reconciliation["summaries"] if summary["symbol"] == "DDI.US")
    assert ddi["quantity"] == "230" and ddi["gross_native"] == "2924.61"
    assert ddi["referenced_total_cost_native"] is None
    assert reconciliation["cost_reconciliations"][0]["gross_native"] == "2911.88"
    assert reconciliation["cost_reconciliations"][0]["unclassified_residual_native"] == "0.6836"
    # Replaying the original amendment snapshot cannot rebind its historical cohort.
    before = state.read_bytes(), report.read_bytes()
    _write(source, initial)
    assert _cli(args).returncode == 0
    assert (state.read_bytes(), report.read_bytes()) == before


def test_disjoint_later_reference_is_allowed_but_same_reference_identity_conflicts():
    old, _ = _replay(_with_proposals())
    followup = _fill("synthetic-later-lot", size=1, price="12.73", commission="0", net_amount="12.73")
    reference = {**_reference(), "source_ref": "synthetic://later-lot-basis", "quantity": 1,
                 "total_cost_native": "12.73", "execution_keys": [{"account_id": ACCOUNT,
                                                                      "trade_id": "synthetic-later-lot"}]}
    snapshot = {"trades": [followup], "cost_basis_references": [reference]}
    updated, report = _replay(snapshot, previous_state=old)
    assert len(updated["cost_basis_references"]) == len(report["cost_reconciliations"]) == 2
    assert updated["amendment_proposals"] == old["amendment_proposals"]
    assert _replay(snapshot, previous_state=updated)[0] == updated
    changed = copy.deepcopy(snapshot)
    changed["cost_basis_references"][0]["total_cost_native"] = "12.74"
    with pytest.raises(accounting.AccountingError, match="conflicting cost-basis reference"):
        _replay(changed, previous_state=updated)


def test_later_same_cohort_reference_does_not_rebind_existing_purchase_proposal():
    snapshot = _with_proposals()
    old, _ = _replay(snapshot)
    later = {**_reference(), "source_ref": "synthetic://later-same-cohort-witness",
             "total_cost_native": "2917.1307", "execution_keys": old["cost_basis_references"][0]["execution_keys"]}
    updated, report = _replay({"trades": _snapshot()["trades"], "cost_basis_references": [later]}, previous_state=old)
    assert len(report["cost_reconciliations"]) == 2
    assert updated["amendment_proposals"] == old["amendment_proposals"]
    assert _replay(snapshot, previous_state=updated)[0] == updated


@pytest.mark.parametrize("starts_with_reference", [True, False])
def test_real_cli_later_total_witness_cannot_rebind_historical_proposal(tmp_path, starts_with_reference):
    source, state, report, args = _arguments(tmp_path)
    initial = _with_proposals()
    if not starts_with_reference:
        initial.pop("cost_basis_references")
    _write(source, initial)
    assert _cli(args).returncode == 0
    original_proposals = json.loads(state.read_text())["amendment_proposals"]
    purchase = next(proposal for proposal in original_proposals if proposal["kind"] == "purchase_cost")
    assert purchase["cost_reference_source_ref"] == (_reference()["source_ref"] if starts_with_reference else None)
    later = {**_reference(), "source_ref": "synthetic://later-same-cohort-witness",
             "total_cost_native": "2917.1307"}
    _write(source, {"trades": _snapshot()["trades"], "cost_basis_references": [later]})
    result = _cli(args)
    assert result.returncode == 0, result.stderr
    assert json.loads(state.read_text())["amendment_proposals"] == original_proposals
    accepted = state.read_bytes(), report.read_bytes()
    assert _cli(args).returncode == 0
    assert (state.read_bytes(), report.read_bytes()) == accepted
    # Original requests retain their explicit reference pin or its absence.
    _write(source, initial)
    result = _cli(args)
    assert result.returncode == 0, result.stderr
    assert (state.read_bytes(), report.read_bytes()) == accepted


@pytest.mark.parametrize("invalid_hash", ["", False, 0, [], "not-a-sha256"])
def test_explicit_malformed_source_digest_cannot_be_replaced_with_generated_provenance(invalid_hash):
    with pytest.raises(accounting.AccountingError):
        _replay(source_sha256=invalid_hash)


@pytest.mark.parametrize("invalid_now", [False, 0, "", dt.datetime(2026, 10, 8)])
def test_explicit_invalid_clock_cannot_be_replaced_with_system_time(invalid_now):
    with pytest.raises(accounting.AccountingError, match="now_utc"):
        accounting.replay_executions(_snapshot(), source_ref="synthetic://captured-export",
                                    account_id=ACCOUNT, now_utc=invalid_now)


def test_missing_instrument_mapping_keeps_supplier_and_native_symbols_unlinked():
    snapshot = _with_proposals()
    for fill in snapshot["trades"]:
        fill["symbol"] = fill["symbol"].removesuffix(".US")
    with pytest.raises(accounting.AccountingError):
        _replay(snapshot)


def test_bounds_reject_oversized_inputs_without_partial_state(tmp_path, monkeypatch):
    _, state, report, args = _arguments(tmp_path)
    monkeypatch.setattr(importer, "MAX_INPUT_BYTES", 32)
    assert importer.main(args) == 2
    assert not state.exists() and not report.exists()
    monkeypatch.setattr(accounting, "MAX_RECORDS", 1)
    with pytest.raises(accounting.AccountingError, match="oversized trades"):
        _replay()
