"""Offline cross-lane USX contracts: quotes are cents, cash remains native.

Only the USD-cent lane changes when TFB_OPP_FX_USX_SUBUNIT is armed.
Use real surface quarantine, cash certification and workbook audit paths.
"""
import copy

import pytest

from core import surface_action_invariants as sai
from core.portfolio_reconciliation import certify_portfolio_inputs
from core.providers.eodhd_provider import _canonicalize_currency_code
from scripts import audit_workbook
from tests.test_portfolio_reconciliation import NOW, ROWS, FX, evidence

GATE = "TFB_OPP_FX_USX_SUBUNIT"


@pytest.fixture(autouse=True)
def isolated_usx_flag(monkeypatch):
    monkeypatch.delenv(GATE, raising=False)


def _surface_row():
    return {"symbol": "ZC=F", "name": "Corn future", "currency": "USX",
            "exchange": "CBT", "current_price": 479.75,
            "investability_status": "WATCHLIST", "final_action": "WATCH",
            "recommendation": "HOLD", "warnings": ""}


def test_usx_is_optional_quote_currency_at_surface_quarantine(monkeypatch):
    monkeypatch.setenv("TFB_SURFACE_ROW_SANITY", "1")
    monkeypatch.setenv("TFB_SURFACE_BLOCKED_INVARIANT", "1")
    row = _surface_row()
    before = copy.deepcopy(row)
    result = sai.apply_surface_action_invariants([row])
    assert result[4] == 1
    assert row["investability_status"] == "BLOCKED"
    assert "unsupported_currency_code" in row["warnings"]

    monkeypatch.setenv(GATE, "1")
    row = _surface_row()
    result = sai.apply_surface_action_invariants([row])
    assert result[4] == 0 and row == before
    # The same token in an exchange cell remains displaced identity data.
    row["exchange"] = "USX"
    result = sai.apply_surface_action_invariants([row])
    assert result[4] == 1 and "currency_in_exchange" in row["warnings"]


def test_armed_usx_does_not_bypass_quote_failure_demotions(monkeypatch):
    monkeypatch.setenv(GATE, "1")
    monkeypatch.setenv("TFB_SURFACE_FETCHFAIL_BLOCKED", "1")
    row = _surface_row()
    row.update(warnings="fetch_failed", final_action="INVEST",
               investability_status="INVESTABLE", recommendation="BUY")
    result = sai.apply_surface_action_invariants([row])
    assert result[2] == 1 and row["final_action"] == "DO_NOT_INVEST"


def _usx_capture():
    packet = evidence()
    packet["accounts"][0]["positions"][0]["currency"] = "USX"
    packet["accounts"][0]["cash"][0]["currency"] = "USX"
    packet["holding_links"][0]["currency"] = "USX"
    packet["fx_rates"][0].update(currency="USX", rate_to_sar="0.0375")
    rows = [dict(ROWS[0], currency="USX")]
    return packet, rows, {"SAR": 1, "USX": "0.0375"}


def test_new_quote_relationship_never_authorizes_minor_native_cash(monkeypatch):
    packet, rows, rates = _usx_capture()
    # Preserve the original explicit three-letter-native-currency parser off.
    result = certify_portfolio_inputs(rows, packet, rates, now=NOW)
    assert result["funding_eligible"] and result["certified_cash_available_sar"] == 6.75
    monkeypatch.setenv(GATE, "1")
    result = certify_portfolio_inputs(rows, packet, rates, now=NOW)
    assert not result["funding_eligible"] and result["proposals"] == []
    assert result["certified_cash_available_sar"] == 0
    assert result["reason_counts"] == {"invalid_evidence": 1}
    # Native USD evidence and its established conversion remain admissible.
    result = certify_portfolio_inputs(ROWS, evidence(), FX, now=NOW)
    assert result["funding_eligible"] and result["certified_cash_available_sar"] == 675


@pytest.mark.parametrize("armed", ["0", "1"])
def test_provider_canonicalization_preserves_cents_and_quote_price_units(monkeypatch, armed):
    monkeypatch.setenv(GATE, armed)
    assert _canonicalize_currency_code("USX") == "USX"
    assert _canonicalize_currency_code("usx") == "USX"
    assert _canonicalize_currency_code("USD") == "USD"
    assert _canonicalize_currency_code("GBp") == "GBX"
    assert _canonicalize_currency_code("GBP") == "GBP"


@pytest.mark.parametrize("currency", ["USX", "usx", "UsX"])
def test_workbook_audit_only_checks_usx_cap_scale_in_armed_lane(monkeypatch, currency):
    headers = ["Symbol", "Currency", "Market Cap", "Current Price", "Float Shares"]
    rows = [("SYNTH=F", currency, 479_750, 479.75, 1000),
            ("CORRECT=F", currency, 4797.5, 479.75, 1000),
            ("GBP.L", "GBp", 479_750, 479.75, 1000)]
    page = audit_workbook.PageData("Commodities_FX", headers, len(headers), rows,
        {header: index for index, header in enumerate(headers)},
        [2, 3, 4], rows, len(rows), 0)
    findings = []
    audit_workbook.check_market_cap_subunit(page, findings)
    assert [finding.symbol for finding in findings] == ["GBP.L"]
    monkeypatch.setenv(GATE, "1")
    findings = []
    audit_workbook.check_market_cap_subunit(page, findings)
    assert [finding.symbol for finding in findings] == ["SYNTH=F", "GBP.L"]
    assert all(finding.code == "C7_SUBUNIT_MCAP" for finding in findings)


@pytest.mark.parametrize("currency", ["USX", "usx", "UsX"])
def test_engine_usx_market_cap_requires_both_flags_and_share_witness(monkeypatch, currency):
    from core import data_engine_v2 as engine
    original = {"symbol": "SYNTH=F", "currency": currency, "current_price": 479.75,
                "market_cap": 479_750, "float_shares": 1000, "warnings": ""}
    for usx_gate, normalizer_gate in (("0", "1"), ("1", "0")):
        monkeypatch.setenv(GATE, usx_gate)
        monkeypatch.setenv("TFB_SUBUNIT_MARKET_CAP_NORMALIZE", normalizer_gate)
        row = copy.deepcopy(original)
        engine._normalize_subunit_market_cap(row)
        assert row == original

    monkeypatch.setenv(GATE, "1")
    monkeypatch.setenv("TFB_SUBUNIT_MARKET_CAP_NORMALIZE", "1")
    row = copy.deepcopy(original)
    engine._normalize_subunit_market_cap(row)
    assert row["market_cap"] == 4797.5
    assert row["current_price"] == original["current_price"]
    assert "market_cap_subunit_normalized_to_major" in row["warnings"]
    once = copy.deepcopy(row)
    engine._normalize_subunit_market_cap(row)
    assert row == once

    # Correct major-unit cap must not be divided again, even with fresh tags.
    row = dict(original, market_cap=4797.5)
    before = copy.deepcopy(row)
    engine._normalize_subunit_market_cap(row)
    assert row == before
    row = dict(original, float_shares=None)
    engine._normalize_subunit_market_cap(row)
    assert row["market_cap"] == original["market_cap"]
    assert "market_cap_subunit_unverified_no_shares" in row["warnings"]
