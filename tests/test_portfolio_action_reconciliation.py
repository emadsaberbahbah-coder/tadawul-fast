"""Real portfolio builder enforces position/cash proof before policy or sizing."""
import copy
import datetime as dt
import json

import pytest

from core.analysis import portfolio_actions as pa


def holding(symbol="SYNTH.SR", quantity=10, **updates):
    stamp = dt.datetime.now(dt.timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")
    row = {"Symbol": symbol, "Currency": "SAR", "Position Qty": quantity, "Buy Price": 90,
           "Current Price": 100, "Intrinsic Value": 130, "Sector": "Energy", "Market": "Tadawul",
           "Forecast Reliability Score": 90, "Data Quality Score": 90,
           "Recommendation": "BUY", "Investability Status": "INVESTABLE", "Risk Bucket": "Moderate"}
    row.update({"Data Provider": "EODHD", "Last Updated": stamp,
                "Warnings": "acquisition_status:success; acquisition_provider:EODHD; acquisition_acquired_at:" + stamp + "; acquisition_quote_asof:" + stamp})
    row.update(updates)
    return row


def packet(rows, cash=10_000, reserved=20):
    stamp = dt.datetime.now(dt.timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")
    return {"schema_version": 1, "source_ref": "synthetic://declared-capture", "captured_at": stamp,
            "accounts": [{"account_id": "synthetic-account", "positions_complete": True, "positions_asof": stamp,
                          "positions": [{"instrument_id": "synthetic-" + str(index), "symbol": row["Symbol"],
                                         "currency": row["Currency"], "quantity": row["Position Qty"]} for index, row in enumerate(rows)],
                          "cash_complete": True, "cash_asof": stamp,
                          "cash": [{"currency": "SAR", "settled_cash": cash + reserved, "reserved_cash": reserved, "reservations_complete": True}]}],
            "holding_links": [{"row_index": index, "account_id": "synthetic-account", "instrument_id": "synthetic-" + str(index),
                               "position_symbol": row["Symbol"], "symbol": row["Symbol"], "currency": row["Currency"]} for index, row in enumerate(rows)],
            "funding_accounts": ["synthetic-account"]}


@pytest.fixture(autouse=True)
def policy(monkeypatch):
    monkeypatch.setenv("TFB_PF_ENABLED", "1")
    monkeypatch.setenv("TFB_EXIT_BY_RULE_GATE", "0")
    monkeypatch.setenv("TFB_PF_ADD_CONFIRM_DAYS", "0")
    monkeypatch.setenv("TFB_PF_CONFIRM_PERSIST", "0")
    monkeypatch.setenv("TFB_PF_TRUST_GATE", "0")


def build(rows=None, evidence="valid", cash=10_000, controls=None):
    rows = [holding()] if rows is None else rows
    meta = {"rows_supplied_by": "synthetic"}
    if evidence == "valid":
        meta["reconciliation_evidence"] = packet(rows, cash)
    elif evidence is not None:
        meta["reconciliation_evidence"] = evidence
    ctl = {"cash_available_sar": cash, "target_cash_pct": 10,
           "max_position_pct": 20, "max_sector_pct": 30, "trust_gate_enabled": False}
    ctl.update(controls or {})
    return pa.build_portfolio_actions(rows, ctl, {"SAR": 1}, meta)


def assert_withheld(result):
    assert result["status"] == "ok" and result["meta"]["execution_ready"] is False
    assert not result["meta"]["input_certification"]["funding_eligible"]
    for key in ("portfolio_value_sar", "cash_sar", "cash_pct", "holdings_value_sar", "pnl_sar"):
        assert result["kpis"][key] is None
    for key in ("deployable_sar", "adds_funded_sar", "proceeds_pending_sar", "capital_unallocated_sar"):
        assert result["kpis"][key] == 0
    for row in result["actions"]:
        assert row["action"] == "BLOCK"
        assert row["suggested_delta_sar"] == 0 and row["suggested_delta_shares"] == 0
        assert row["weight_pct"] is None and row["detail"]["execution_ready"] is False
        assert row["stop_sar"] is row["tp1_sar"] is row["tp2_sar"] is None


def test_valid_certificate_preserves_policy_and_sizes_only_after_reservations():
    result = build()
    assert result["status"] == "ok" and result["meta"]["execution_ready"]
    assert result["meta"]["input_certification"]["trusted_rows"] == 1
    assert result["kpis"]["cash_sar"] == 10_000
    assert result["kpis"]["adds_funded_sar"] > 0
    assert result["actions"][0]["action"] == "ADD"
    assert result["meta"]["controls_snapshot"]["rebalance_mode"] == pa.REBALANCE_NEW_CASH


@pytest.mark.parametrize("observed", [0, 4])
def test_full_and_partial_sales_block_stale_hold_add_before_confirmation(observed, monkeypatch):
    rows = [holding()]
    evidence = packet(rows)
    evidence["accounts"][0]["positions"][0]["quantity"] = observed
    monkeypatch.setattr(pa, "_apply_add_confirmation", lambda *_args: pytest.fail("unverified position must not enter confirmation"))
    result = build(rows, evidence)
    assert_withheld(result)
    assert result["actions"][0]["detail"]["reconciliation_status"] == ("position_closed" if observed == 0 else "quantity_mismatch")
    assert result["actions"][0]["quantity"] == 10  # Observed ledger input; never rewritten.


@pytest.mark.parametrize("evidence", [None, {}, "not-a-capture"])
def test_missing_or_invalid_evidence_has_renderable_block_rows(evidence):
    result = build(evidence=evidence)
    assert_withheld(result)
    assert result["alerts"][0]["type"] == "portfolio_inputs_unverified"


def test_cash_date_or_balance_on_paper_controls_cannot_substitute_for_capture():
    result = build(evidence=None, controls={"cash_date": "2026-10-09", "cash_available_sar": 500_000})
    assert_withheld(result)


def test_requested_cash_cannot_disagree_with_settled_minus_reserved_capture():
    result = build(evidence=packet([holding()], cash=100), cash=10_000)
    assert_withheld(result)
    assert result["meta"]["input_certification"]["reason_counts"] == {"cash_request_mismatch": 1}


def test_external_custody_gap_blocks_entire_cross_sectional_denominator():
    rows = [holding(), holding("EXTERNAL.SR", 30)]
    evidence = packet(rows)
    evidence["holding_links"].pop()
    evidence["accounts"][0]["positions"].pop()
    result = build(rows, evidence)
    assert_withheld(result)
    assert result["meta"]["input_certification"]["reason_counts"]["custody_unknown"] == 1
    assert result["actions"][0]["detail"]["position_evidence_matched"] is True
    assert result["actions"][1]["detail"]["position_evidence_matched"] is False


def test_proposed_exit_proceeds_never_fund_a_buy_before_settlement():
    rows = [holding("SELL.SR", 10, **{"Recommendation": "STRONG_SELL", "Current Price": 140, "Intrinsic Value": 100}), holding("ADD.SR", 10)]
    result = build(rows, cash=0, controls={"rebalance_mode": "Advisory", "target_cash_pct": 0,
                                        "max_position_pct": 70, "max_sector_pct": 100})
    assert result["meta"]["execution_ready"]
    assert any(row["action"] == "EXIT" and row["proceeds_sar"] > 0 for row in result["actions"])
    assert result["kpis"]["adds_funded_sar"] == result["kpis"]["deployable_sar"] == result["kpis"]["proceeds_pending_sar"] == 0
    assert all(not row["funds_from"] for row in result["actions"])


def test_max_holdings_never_certifies_a_truncated_portfolio():
    rows = [holding(), holding("SECOND.SR")]
    result = build(rows, controls={"max_holdings": 1})
    assert_withheld(result)
    assert result["meta"]["input_certification"]["reason_counts"]["holdings_input_incomplete"] == 1


def test_oversized_direct_input_has_bounded_blocked_research_output():
    rows = [holding("SYNTH%d.SR" % index) for index in range(501)]
    result = build(rows, evidence=None)
    assert_withheld(result)
    assert len(result["actions"]) == 500 and result["meta"]["counts"]["rows_in"] == 501


def test_no_raw_source_account_cash_capture_or_proposals_in_output():
    evidence = packet([holding()])
    before = copy.deepcopy(evidence)
    result = build(evidence=evidence)
    serialized = json.dumps(result)
    for private in ("synthetic-account", "synthetic://declared-capture", "positions_complete", "reconciliation_evidence", "reserved_cash"):
        assert private not in serialized
    assert evidence == before


def test_repeated_invalid_capture_never_advances_add_confirmation():
    first = build(evidence=None)
    second = build(evidence=None)
    assert first["actions"] == second["actions"]
    assert_withheld(first)


@pytest.mark.parametrize("mutation", ["missing", "preserved", "old", "future"])
def test_fresh_positions_and_cash_cannot_override_missing_or_stale_actual_quotes(mutation):
    rows = [holding()]
    if mutation == "missing":
        rows[0].pop("Warnings")
    elif mutation == "preserved":
        rows[0]["Warnings"] += "; kept_last_good"
    elif mutation == "old":
        old = (dt.datetime.now(dt.timezone.utc) - dt.timedelta(days=10)).isoformat()
        rows[0]["Warnings"] = rows[0]["Warnings"].split("; acquisition_quote_asof:")[0] + "; acquisition_quote_asof:" + old
    else:
        future = (dt.datetime.now(dt.timezone.utc) + dt.timedelta(days=1)).isoformat()
        rows[0]["Warnings"] = rows[0]["Warnings"].split("; acquisition_quote_asof:")[0] + "; acquisition_quote_asof:" + future
    result = build(rows)
    assert_withheld(result)
    assert result["meta"]["input_certification"]["reason_counts"]["holding_quote_unverified"] == 1


@pytest.mark.parametrize("rate", [1, True, "3.74"])
def test_native_row_fx_override_cannot_escape_exact_fresh_fx_witness(rate):
    rows = [holding("SYNTH.US", **{"Currency": "USD", "FX To SAR": rate})]
    evidence = packet(rows)
    evidence["fx_rates"] = [{"currency": "USD", "rate_to_sar": "3.75", "asof": evidence["captured_at"], "source_ref": "synthetic://FX"}]
    result = pa.build_portfolio_actions(rows, {"cash_available_sar": 10_000, "trust_gate_enabled": False},
                                        {"SAR": 1, "USD": 3.75}, {"reconciliation_evidence": evidence})
    assert_withheld(result)
    assert result["meta"]["input_certification"]["reason_counts"]["holding_fx_mismatch"] == 1
