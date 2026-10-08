"""Synthetic account captures validate the pure production certification API."""
import copy
import datetime as dt
import json

import pytest

from core.portfolio_reconciliation import certify_portfolio_inputs, certification_summary

NOW = dt.datetime(2026, 10, 9, 9, tzinfo=dt.timezone.utc)
STAMP = "2026-10-09T08:59:00Z"
ROWS = [{"symbol": "SYNTH.US", "currency": "USD", "quantity": "12"}]
FX = {"SAR": 1, "USD": "3.75"}


def evidence():
    return {
        "schema_version": 1, "source_ref": "synthetic://declared-export", "captured_at": STAMP,
        "accounts": [{
            "account_id": "synthetic-account", "positions_complete": True, "positions_asof": STAMP,
            "positions": [{"instrument_id": "synthetic-contract", "symbol": "SYNTH", "currency": "USD", "quantity": "12"}],
            "cash_complete": True, "cash_asof": STAMP,
            "cash": [{"currency": "USD", "settled_cash": "200", "reserved_cash": "20", "reservations_complete": True}],
        }],
        "holding_links": [{"row_index": 0, "account_id": "synthetic-account", "instrument_id": "synthetic-contract",
                           "position_symbol": "SYNTH", "symbol": "SYNTH.US", "currency": "USD"}],
        "funding_accounts": ["synthetic-account"],
        "fx_rates": [{"currency": "USD", "rate_to_sar": "3.75", "asof": STAMP, "source_ref": "synthetic://FX-capture"}],
    }


def certify(packet=None, rows=None, **kwargs):
    return certify_portfolio_inputs(ROWS if rows is None else rows,
                                   evidence() if packet is None else packet, FX, now=NOW, **kwargs)


def test_exact_account_instrument_currency_quantity_and_settled_reservation_basis():
    packet = evidence()
    before = copy.deepcopy(packet)
    result = certify(packet)
    assert result["funding_eligible"] and result["holdings_certified"] and result["cash_certified"]
    assert result["certified_cash_available_sar_exact"] == "675"
    assert result["certified_cash_available_sar"] == 675
    assert result["row_results"] == [{"row_index": 0, "trusted": True, "status": "matched"}]
    assert packet == before and ROWS[0]["quantity"] == "12"
    assert "authenticated" in result["evidence_origin"] and not result["proposals"]


@pytest.mark.parametrize("quantity,status,kind", [("0", "position_closed", "close_review"), ("5", "quantity_mismatch", "quantity_review")])
def test_full_or_partial_sale_never_authorizes_stale_quantity_or_spends_proceeds(quantity, status, kind):
    packet = evidence()
    packet["accounts"][0]["positions"][0]["quantity"] = quantity
    packet["accounts"][0]["cash"][0]["unsettled_sale_proceeds"] = "99999"
    result = certify(packet, include_proposals=True)
    assert not result["funding_eligible"] and result["certified_cash_available_sar"] == 0
    assert result["row_results"][0]["status"] == status
    proposal = result["proposals"][0]
    assert proposal["kind"] == kind and proposal["observed_quantity"] == quantity
    assert proposal["expected_quantity"] == "12" and proposal["application"] == "review_only"
    assert proposal["fee_reconciliation"] == "unverified" and len(proposal["original_row_sha256"]) == 64
    assert "realized_pnl" not in proposal


def test_missing_position_is_unknown_even_when_account_declares_complete_positions():
    packet = evidence()
    packet["accounts"][0]["positions"] = []
    result = certify(packet, include_proposals=True)
    assert result["row_results"][0]["status"] == "position_missing"
    assert not result["funding_eligible"] and result["proposals"] == []


def test_other_custody_never_becomes_zero_from_an_unrelated_account_capture():
    packet = evidence()
    rows = ROWS + [{"symbol": "OTHER.SR", "currency": "SAR", "quantity": 30}]
    result = certify(packet, rows, include_proposals=True)
    assert result["row_results"][0]["trusted"] is True
    assert result["row_results"][1]["status"] == "custody_unknown"
    assert result["proposals"] == [] and not result["funding_eligible"]


def test_explicit_other_custodian_evidence_can_cover_an_external_holding():
    packet = evidence()
    rows = ROWS + [{"symbol": "OTHER.SR", "currency": "SAR", "quantity": 30}]
    packet["accounts"].append({"account_id": "synthetic-external", "positions_complete": True,
                              "positions_asof": STAMP, "positions": [{"instrument_id": "external-contract", "symbol": "OTHER.SR", "currency": "SAR", "quantity": 30}]})
    packet["holding_links"].append({"row_index": 1, "account_id": "synthetic-external", "instrument_id": "external-contract", "position_symbol": "OTHER.SR", "symbol": "OTHER.SR", "currency": "SAR"})
    assert certify(packet, rows)["funding_eligible"]


@pytest.mark.parametrize("field", ["positions_asof", "cash_asof", "captured_at"])
@pytest.mark.parametrize("value", [None, "2026-10-09", "2026-10-09T08:59", "2026-10-09T08:59:00", "2026-10-09T07:00:00Z", "2026-10-09T09:01:00Z"])
def test_missing_date_only_old_and_future_evidence_cannot_be_current(field, value):
    packet = evidence()
    target = packet if field == "captured_at" else packet["accounts"][0]
    target[field] = value
    result = certify(packet)
    assert not result["funding_eligible"] and result["certified_cash_available_sar"] == 0


@pytest.mark.parametrize("field,value", [
    ("settled_cash", None), ("settled_cash", "NaN"), ("settled_cash", "-1"), ("settled_cash", True),
    ("reserved_cash", None), ("reserved_cash", "-0"), ("reserved_cash", "201"),
    ("reservations_complete", None), ("reservations_complete", "true"),
    ("currency", None), ("currency", "BASE"), ("currency", "GBp"),
])
def test_cash_and_reservation_ambiguity_cannot_authorize_funding(field, value):
    packet = evidence()
    packet["accounts"][0]["cash"][0][field] = value
    # GBp is an ambiguous minor-unit token, not a GBP balance declaration.
    result = certify(packet)
    assert not result["funding_eligible"] and result["certified_cash_available_sar"] == 0


def test_unknown_fx_no_static_fallback_and_sar_rate_must_equal_one():
    packet = evidence()
    assert not certify_portfolio_inputs(ROWS, packet, {}, now=NOW)["funding_eligible"]
    packet["accounts"][0]["cash"] = [{"currency": "SAR", "settled_cash": 40, "reserved_cash": 0, "reservations_complete": True}]
    assert not certify_portfolio_inputs(ROWS, packet, {"SAR": 3.75}, now=NOW)["funding_eligible"]


@pytest.mark.parametrize("kind", ["account", "position", "link", "cash"])
def test_duplicate_scopes_cannot_be_summed_or_relinked(kind):
    packet = evidence()
    if kind == "account":
        packet["accounts"].append(copy.deepcopy(packet["accounts"][0]))
    elif kind == "position":
        packet["accounts"][0]["positions"].append(copy.deepcopy(packet["accounts"][0]["positions"][0]))
    elif kind == "link":
        packet["holding_links"].append(copy.deepcopy(packet["holding_links"][0]))
    else:
        packet["accounts"][0]["cash"].append(copy.deepcopy(packet["accounts"][0]["cash"][0]))
    assert not certify(packet)["funding_eligible"]


def test_unrepresented_positive_position_prevents_partial_nav_certification():
    packet = evidence()
    packet["accounts"][0]["positions"].append({"instrument_id": "missing-contract", "symbol": "MISSING", "currency": "USD", "quantity": 4})
    result = certify(packet)
    assert not result["holdings_certified"] and result["reason_counts"]["unrepresented_positions"] == 1


def test_empty_confirmed_account_and_explicit_zero_cash_are_valid_inputs():
    packet = evidence()
    packet["accounts"][0]["positions"] = []
    packet["holding_links"] = []
    packet["accounts"][0]["cash"] = [{"currency": "SAR", "settled_cash": 0, "reserved_cash": 0, "reservations_complete": True}]
    result = certify(packet, [])
    assert result["funding_eligible"] and result["certified_cash_available_sar"] == 0


def test_summary_never_exposes_accounts_cash_raw_evidence_or_proposals():
    packet = evidence()
    packet["accounts"][0]["positions"][0]["quantity"] = 0
    result = certify(packet, include_proposals=True)
    rendered = json.dumps(certification_summary(result))
    for private in ("synthetic-account", "synthetic-contract", "source_ref", "675", "expected_quantity", "proposals"):
        assert private not in rendered


@pytest.mark.parametrize("packet", [None, {}, [], "invalid", {"schema_version": 1}])
def test_missing_or_malformed_packet_has_only_safe_reason_codes(packet):
    result = certify_portfolio_inputs(ROWS, packet, FX, now=NOW)
    assert not result["funding_eligible"] and result["proposals"] == []
    assert set(result["reason_counts"]).issubset({"missing_evidence", "invalid_evidence"})


def test_conflicting_quantity_alias_or_explicit_link_currency_is_rejected():
    rows = [dict(ROWS[0], **{"Position Qty": "11"})]
    assert not certify(rows=rows)["funding_eligible"]
    packet = evidence()
    packet["holding_links"][0]["currency"] = "SAR"
    assert not certify(packet)["funding_eligible"]


def test_complete_flags_require_boolean_true_not_text_or_numbers():
    packet = evidence()
    packet["accounts"][0]["positions_complete"] = "true"
    assert not certify(packet)["funding_eligible"]


@pytest.mark.parametrize("change", ["missing", "duplicate", "stale", "date_only", "mismatch", "bool", "no_source"])
def test_foreign_cash_and_nav_require_explicit_fresh_exact_fx_evidence(change):
    packet = evidence()
    proof = packet["fx_rates"][0]
    if change == "missing":
        packet.pop("fx_rates")
    elif change == "duplicate":
        packet["fx_rates"].append(copy.deepcopy(proof))
    elif change == "stale":
        proof["asof"] = "2026-10-09T07:00:00Z"
    elif change == "date_only":
        proof["asof"] = "2026-10-09"
    elif change == "mismatch":
        proof["rate_to_sar"] = "3.76"
    elif change == "bool":
        proof["rate_to_sar"] = True
    else:
        proof.pop("source_ref")
    result = certify(packet)
    assert not result["funding_eligible"] and result["certified_cash_available_sar"] == 0


def test_minor_currency_token_never_becomes_major_currency_even_with_matching_rate():
    packet = evidence()
    packet["accounts"][0]["cash"][0]["currency"] = "GBp"
    packet["fx_rates"].append({"currency": "GBP", "rate_to_sar": 4, "asof": STAMP, "source_ref": "synthetic://FX"})
    assert not certify_portfolio_inputs(ROWS, packet, dict(FX, GBP=4), now=NOW)["funding_eligible"]
    packet = evidence()
    packet["accounts"][0]["cash_complete"] = 1
    assert not certify(packet)["funding_eligible"]
