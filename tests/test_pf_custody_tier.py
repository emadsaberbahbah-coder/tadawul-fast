"""portfolio_actions 1.18.1 [CUSTODY-TIER] + portfolio_reconciliation 1.1.0.

Owner, 15 Nov 2026: the page read BLOCK on 6 of 6 holdings ("missing_evidence") while every rule verdict
was HOLD, because the hourly IBKR capture is accepted for 900 s and each certified page is overwritten by the
next refresh. A valid packet now leaves a custody receipt; a later request whose rows match it (symbol,
currency, exact quantity) within 30 h shows the rule verdict. Nothing here may fund or size new money.

All holdings, quantities, prices and cash are invented; no sheet, no network, no Redis server.
"""
from __future__ import annotations

import copy
import datetime as dt
import json

import pytest

from core import portfolio_reconciliation as pr
from core.analysis import portfolio_actions as pa

UTC = dt.timezone.utc


def _stamp(when=None):
    return (when or dt.datetime.now(UTC)).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def holding(symbol="SYNTH.SR", quantity=10, **updates):
    stamp = _stamp()
    row = {"Symbol": symbol, "Currency": "SAR", "Position Qty": quantity, "Buy Price": 90,
           "Current Price": 100, "Intrinsic Value": 130, "Sector": "Energy", "Market": "Tadawul",
           "Forecast Reliability Score": 90, "Data Quality Score": 90,
           "Recommendation": "BUY", "Investability Status": "INVESTABLE", "Risk Bucket": "Moderate"}
    row.update({"Data Provider": "EODHD", "Last Updated": stamp,
                "Warnings": "acquisition_status:success; acquisition_provider:EODHD; acquisition_acquired_at:"
                            + stamp + "; acquisition_quote_asof:" + stamp})
    row.update(updates)
    return row


def packet(rows, cash=10_000, reserved=20, when=None, extra_positions=()):
    stamp = _stamp(when)
    positions = [{"instrument_id": "synthetic-" + str(i), "symbol": r["Symbol"], "currency": r["Currency"],
                  "quantity": r["Position Qty"]} for i, r in enumerate(rows)]
    positions += [{"instrument_id": "extra-" + str(i), "symbol": s, "currency": "SAR", "quantity": q}
                  for i, (s, q) in enumerate(extra_positions)]
    return {"schema_version": 1, "source_ref": "synthetic://declared-capture", "captured_at": stamp,
            "accounts": [{"account_id": "synthetic-account", "positions_complete": True, "positions_asof": stamp,
                          "positions": positions, "cash_complete": True, "cash_asof": stamp,
                          "cash": [{"currency": "SAR", "settled_cash": cash + reserved, "reserved_cash": reserved,
                                    "reservations_complete": True}]}],
            "holding_links": [{"row_index": i, "account_id": "synthetic-account",
                               "instrument_id": "synthetic-" + str(i), "position_symbol": r["Symbol"],
                               "symbol": r["Symbol"], "currency": r["Currency"]} for i, r in enumerate(rows)],
            "funding_accounts": ["synthetic-account"]}


@pytest.fixture(autouse=True)
def policy(monkeypatch):
    for name, value in (("TFB_PF_ENABLED", "1"), ("TFB_EXIT_BY_RULE_GATE", "0"), ("TFB_PF_ADD_CONFIRM_DAYS", "0"),
                        ("TFB_PF_CONFIRM_PERSIST", "0"), ("TFB_PF_TRUST_GATE", "0")):
        monkeypatch.setenv(name, value)
    for name in ("TFB_PF_CUSTODY_TIER", "TFB_PF_CUSTODY_MAX_AGE_H", "TFB_PF_RESEARCH_VIEW", "REDIS_URL"):
        monkeypatch.delenv(name, raising=False)
    monkeypatch.setattr(pa, "_CUSTODY_RECEIPT", None)


def build(rows, evidence="valid", cash=10_000, request_cash=None, upstream=None):
    meta = {"rows_supplied_by": "synthetic"}
    meta.update(upstream or {})
    if evidence == "valid":
        meta["reconciliation_evidence"] = packet(rows, cash)
    elif evidence is not None:
        meta["reconciliation_evidence"] = evidence
    ctl = {"cash_available_sar": cash if request_cash is None else request_cash, "target_cash_pct": 10,
           "max_position_pct": 20, "max_sector_pct": 30, "trust_gate_enabled": False}
    return pa.build_portfolio_actions(rows, ctl, {"SAR": 1}, meta)


def by_symbol(result):
    return {row["symbol"]: row for row in result["actions"]}


def assert_never_funds(result):
    assert result["status"] == "ok" and result["meta"]["execution_ready"] is False
    assert result["meta"]["input_certification"]["funding_eligible"] is False
    for key in ("deployable_sar", "adds_funded_sar", "proceeds_pending_sar", "capital_unallocated_sar"):
        assert result["kpis"][key] == 0
    for key in ("portfolio_value_sar", "cash_sar", "cash_pct"):
        assert result["kpis"][key] is None
    for row in result["actions"]:
        assert row["action"] != "ADD" and row["weight_pct"] is None and row["funds_from"] is None
        assert row["detail"]["execution_ready"] is False
        assert not (row["suggested_delta_sar"] or 0) > 0 and not (row["suggested_delta_shares"] or 0) > 0


# ----------------------------------------------------------------------------- the pure receipt
ROWS = [{"symbol": "AAA.SR", "currency": "SAR", "quantity": "10"}, {"symbol": "BBB.SR", "currency": "SAR", "quantity": 7}]
NOW = dt.datetime(2026, 11, 14, 6, 0, tzinfo=UTC)


def pure_packet(when=NOW, rows=ROWS, quantities=None, extra=()):
    stamp = _stamp(when)
    q = quantities or [r["quantity"] for r in rows]
    positions = [{"instrument_id": "i" + str(i), "symbol": "P" + r["symbol"], "currency": r["currency"], "quantity": q[i]}
                 for i, r in enumerate(rows)] + [{"instrument_id": "x" + str(i), "symbol": s, "currency": "SAR", "quantity": n}
                                                 for i, (s, n) in enumerate(extra)]
    return {"schema_version": 1, "source_ref": "synthetic://capture", "captured_at": stamp,
            "accounts": [{"account_id": "acct-secret", "positions_complete": True, "positions_asof": stamp,
                          "positions": positions, "cash_complete": True, "cash_asof": stamp,
                          "cash": [{"currency": "SAR", "settled_cash": 4321, "reserved_cash": 0,
                                    "reservations_complete": True}]}],
            "holding_links": [{"row_index": i, "account_id": "acct-secret", "instrument_id": "i" + str(i),
                               "position_symbol": "P" + r["symbol"], "symbol": r["symbol"], "currency": r["currency"]}
                              for i, r in enumerate(rows)],
            "funding_accounts": ["acct-secret"]}


def receipt_for(evidence, rows=ROWS, now=NOW):
    cert = pr.certify_portfolio_inputs(rows, evidence, {"SAR": 1}, now=now)
    return pr.custody_receipt(rows, evidence, cert), cert


def test_c1_a_valid_packet_leaves_a_receipt_of_positions_only():
    receipt, cert = receipt_for(pure_packet(extra=[("ZZZ.SR", 3), ("GONE.SR", 0)]))
    assert cert["row_results"][0]["status"] == "matched"
    assert receipt == {"schema": "tfb.custody-receipt/v1", "captured_at": "2026-11-14T06:00:00Z",
                       "holdings": [{"symbol": "AAA.SR", "currency": "SAR", "quantity": "10"},
                                    {"symbol": "BBB.SR", "currency": "SAR", "quantity": "7"}],
                       "unrepresented": 1}                       # the positive unlinked position; a zero one is not
    text = json.dumps(receipt)
    for secret in ("acct-secret", "4321", "cash", "instrument", "i0", "source", "synthetic://"):
        assert secret not in text
    # the BROKER quantity is kept, not the sheet's: a mismatch is remembered as what the broker holds
    receipt, cert = receipt_for(pure_packet(quantities=["10", 4]))
    assert cert["row_results"][1]["status"] == "quantity_mismatch"
    assert receipt["holdings"][1] == {"symbol": "BBB.SR", "currency": "SAR", "quantity": "4"}


@pytest.mark.parametrize("label", ["missing", "not_a_dict", "stale", "future", "incomplete", "bad_schema"])
def test_c2_no_receipt_without_a_valid_fresh_complete_packet(label):
    evidence = pure_packet()
    now = NOW
    if label == "missing":
        evidence = None
    elif label == "not_a_dict":
        evidence = "capture"
    elif label == "stale":
        now = NOW + dt.timedelta(seconds=901)
    elif label == "future":
        now = NOW - dt.timedelta(seconds=5)
    elif label == "incomplete":
        evidence["accounts"][0]["positions_complete"] = False
    else:
        evidence["schema_version"] = 2
    receipt, _ = receipt_for(evidence, now=now)
    assert receipt is None
    # a reading that is not the certifier's own answer for these rows is refused too
    assert pr.custody_receipt(ROWS, pure_packet(), None) is None
    assert pr.custody_receipt(ROWS, pure_packet(), {"reason_counts": {}, "row_results": []}) is None


def test_c2b_a_link_that_proves_nothing_is_left_out():
    evidence = pure_packet()
    evidence["accounts"][0]["positions"][1]["symbol"] = "OTHER"          # identity mismatch on row 1
    receipt, cert = receipt_for(evidence)
    assert cert["row_results"][1]["status"] == "position_identity_mismatch"
    assert [h["symbol"] for h in receipt["holdings"]] == ["AAA.SR"]
    evidence = pure_packet()
    del evidence["accounts"][0]["positions"][1]                          # linked position missing
    receipt, cert = receipt_for(evidence)
    assert cert["row_results"][1]["status"] == "position_missing" and [h["symbol"] for h in receipt["holdings"]] == ["AAA.SR"]


def test_c3_rows_against_the_receipt_never_fund_and_match_exactly():
    receipt, _ = receipt_for(pure_packet())
    later = NOW + dt.timedelta(hours=16)
    got = pr.certify_recent_custody(ROWS, receipt, now=later)
    assert [r["status"] for r in got["row_results"]] == ["matched", "matched"]
    assert got["status"] == "withheld" and got["funding_eligible"] is False and got["cash_certified"] is False
    assert got["holdings_certified"] is False and got["certified_cash_available_sar"] == 0.0
    assert got["certified_cash_available_sar_exact"] == "0" and got["proposals"] == []
    assert got["reason_counts"] == {"funding_evidence_stale": 1}
    assert got["custody_recent"] == {"captured_at": "2026-11-14T06:00:00Z", "age_seconds": 16 * 3600}
    rows = [{"symbol": "aaa.sr", "currency": "SAR", "quantity": 10.0},      # same quantity, other spelling
            {"symbol": "BBB.SR", "currency": "SAR", "quantity": 8},         # changed since the capture
            {"symbol": "NEW.SR", "currency": "SAR", "quantity": 5}]         # not on the receipt
    got = pr.certify_recent_custody(rows, receipt, now=later)
    assert [r["status"] for r in got["row_results"]] == ["matched", "quantity_mismatch", "custody_unknown"]
    assert [r["trusted"] for r in got["row_results"]] == [True, False, False]
    assert got["reason_counts"] == {"custody_unknown": 1, "funding_evidence_stale": 1, "quantity_mismatch": 1}
    lower = dict(receipt, holdings=[dict(h, symbol=h["symbol"].lower()) for h in receipt["holdings"]])
    assert [r["status"] for r in pr.certify_recent_custody(ROWS, lower, now=later)["row_results"]] == ["matched", "matched"]
    closed = dict(receipt, holdings=[dict(receipt["holdings"][0], quantity="0"), receipt["holdings"][1]])
    got = pr.certify_recent_custody(ROWS, closed, now=later)
    assert got["row_results"][0]["status"] == "position_closed"
    # a broker position the page no longer lists, or one the capture never linked, is reported
    got = pr.certify_recent_custody(ROWS[:1], receipt, now=later)
    assert got["row_results"][0]["status"] == "matched" and got["reason_counts"]["unrepresented_positions"] == 1
    got = pr.certify_recent_custody(ROWS, dict(receipt, unrepresented=2), now=later)
    assert got["reason_counts"]["unrepresented_positions"] == 1
    zero = [{"symbol": "AAA.SR", "currency": "SAR", "quantity": 0}]
    got = pr.certify_recent_custody(zero, dict(receipt, holdings=[dict(receipt["holdings"][0], quantity="0")]), now=later)
    assert got["row_results"][0]["status"] == "inactive_zero" and got["row_results"][0]["trusted"] is False


def test_c4_the_receipt_window_and_malformed_receipts():
    receipt, _ = receipt_for(pure_packet())
    edge = NOW + dt.timedelta(seconds=pr.CUSTODY_MAX_AGE_SECONDS)
    assert pr.CUSTODY_MAX_AGE_SECONDS == 30 * 3600 and pr.CUSTODY_MAX_AGE_CEILING == 48 * 3600
    assert pr.certify_recent_custody(ROWS, receipt, now=edge) is not None
    assert pr.certify_recent_custody(ROWS, receipt, now=edge + dt.timedelta(seconds=1)) is None
    assert pr.certify_recent_custody(ROWS, receipt, now=NOW - dt.timedelta(seconds=1)) is None       # from the future
    assert pr.certify_recent_custody(ROWS, receipt, now=NOW + dt.timedelta(hours=40), max_age_seconds=48 * 3600)
    for bad_limit in (0, -1, 48 * 3600 + 1, True, 3600.0, "3600", None):
        assert pr.certify_recent_custody(ROWS, receipt, now=NOW, max_age_seconds=bad_limit) is None
    assert pr.certify_recent_custody(ROWS, receipt, now=NOW.replace(tzinfo=None)) is None           # naive clock
    for bad in (None, "x", {}, dict(receipt, schema="other"), dict(receipt, captured_at="2026-11-14"),
                dict(receipt, unrepresented=True), dict(receipt, unrepresented=-1), dict(receipt, unrepresented="0"),
                dict(receipt, holdings="x"), dict(receipt, holdings=receipt["holdings"] + receipt["holdings"][:1]),
                dict(receipt, holdings=[{"symbol": "AAA.SR", "currency": "sar", "quantity": "1"}]),
                dict(receipt, holdings=[{"symbol": "AAA.SR", "currency": "SAR", "quantity": "-1"}])):
        assert pr.certify_recent_custody(ROWS, bad, now=NOW) is None
    assert pr.certify_recent_custody(ROWS + ROWS[:1], receipt, now=NOW) is None                      # duplicate rows
    assert pr.certify_recent_custody("rows", receipt, now=NOW) is None


# ----------------------------------------------------------------------------- the page
def test_t1_after_a_certified_run_the_same_rows_without_a_packet_show_their_verdict(monkeypatch):
    rows = [holding("AAA.SR"), holding("BBB.SR", quantity=4, Recommendation="HOLD", **{"Intrinsic Value": 104})]
    first = build(rows)
    assert first["meta"]["execution_ready"] is True and pa._CUSTODY_RECEIPT is not None
    assert "custody_tier" not in first["meta"]
    monkeypatch.setattr(pa, "_apply_add_confirmation", lambda *a: pytest.fail("the confirmation clock was read"))
    monkeypatch.setattr(pa, "fund_adds", lambda *a: pytest.fail("funding ran"))
    got = build(rows, evidence=None)
    assert_never_funds(got)
    r = by_symbol(got)
    assert r["AAA.SR"]["action"] == "HOLD" and r["BBB.SR"]["action"] == "HOLD"
    assert r["AAA.SR"]["action_reason"].startswith(pa.RESEARCH_ADD_WITHHELD + " | holding matches the IBKR capture of ")
    assert "ADD" not in r["AAA.SR"]["action_reason"].split(" | ")[0].replace("ADD-", "")       # the verdict is not named
    assert " Riyadh (" in r["AAA.SR"]["action_reason"] and r["AAA.SR"]["action_reason"].endswith(
        "old); new money waits for a fresh capture")
    assert "RECONCILIATION_REQUIRED" not in r["BBB.SR"]["action_reason"] and "BLOCKED" not in r["BBB.SR"]["advisor_note"]
    for row in r.values():
        assert row["market_value_sar"] and row["stop_sar"] and row["detail"]["position_evidence_matched"] is True
        assert row["detail"]["reconciliation_status"] == "matched" and row["suggested_delta_sar"] is None
        assert row["suggested_delta_shares"] is None and row["proceeds_sar"] == 0
    assert r["AAA.SR"]["detail"]["custody_tier"] == {"source": "receipt", "verdict": "NEW_MONEY_WITHHELD",
                                                     "price": "live"}
    assert r["BBB.SR"]["detail"]["custody_tier"] == {"source": "receipt", "verdict": "HOLD", "price": "live"}
    assert got["meta"]["custody_tier"]["prices"] == {"live": 2}
    assert got["kpis"]["action_counts"] == {"ADD": 0, "HOLD": 2, "TRIM": 0, "EXIT": 0, "BLOCK": 0}
    assert [a["type"] for a in got["alerts"]] == ["custody_matched_funding_withheld"] and got["alerts"][0]["count"] == 2
    tier = got["meta"]["custody_tier"]
    assert tier["mode"] == "enforce" and tier["status"] == "ok" and tier["source"] == "receipt"
    assert tier["matched"] == 2 and tier["shown"] == 2 and tier["verdicts"] == {"HOLD": 1, "NEW_MONEY_WITHHELD": 1}
    assert tier["kept"] == {} and 0 <= tier["age_seconds"] < 120


def test_t2_without_a_receipt_the_page_is_the_protective_page():
    rows = [holding("AAA.SR")]
    got = build(rows, evidence=None)
    monkey = pytest.MonkeyPatch()
    try:
        monkey.setenv("TFB_PF_CUSTODY_TIER", "off")
        want = build(rows, evidence=None)
    finally:
        monkey.undo()
    assert got["meta"].pop("custody_tier") == {"mode": "enforce", "status": "no_recent_capture", "source": None,
                                               "matched": 0, "shown": 0, "verdicts": {}, "kept": {},
                                               "prices": {}}
    assert "custody_tier" not in want["meta"]
    for page in (got, want):
        page["meta"].pop("generated_utc", None)
    assert got == want and got["actions"][0]["action"] == "BLOCK"
    assert got["actions"][0]["action_reason"] == "RECONCILIATION_REQUIRED: missing_evidence"


@pytest.mark.parametrize("word", ["off", "OFF", " 0 ", "false", "no"])
def test_t3_off_writes_and_reads_no_receipt(monkeypatch, word):
    monkeypatch.setenv("TFB_PF_CUSTODY_TIER", word)
    monkeypatch.setattr(pa, "custody_receipt", lambda *a: pytest.fail("a receipt was built under off"))
    monkeypatch.setattr(pa, "_custody_receipt_get", lambda: pytest.fail("a receipt was read under off"))
    rows = [holding("AAA.SR")]
    assert build(rows)["meta"]["execution_ready"] is True and pa._CUSTODY_RECEIPT is None
    got = build(rows, evidence=None)
    assert got["actions"][0]["action"] == "BLOCK" and "custody_tier" not in got["meta"]


def test_t3b_the_gate_reader_and_the_window_reader(monkeypatch):
    for value, mode in (("", "enforce"), ("enforce", "enforce"), ("on", "enforce"), ("typo", "enforce"),
                        ("observe", "observe"), (" Observe ", "observe"), ("off", "off"), ("0", "off"),
                        ("FALSE", "off"), ("no", "off")):
        monkeypatch.setenv("TFB_PF_CUSTODY_TIER", value)
        assert pa._env_custody_tier_mode() == mode, value
    monkeypatch.delenv("TFB_PF_CUSTODY_TIER")
    assert pa._env_custody_tier_mode() == "enforce"
    for value, seconds in (("", 30 * 3600), ("30", 30 * 3600), ("12.5", 45000), ("0", 3600), ("-4", 3600),
                           ("0.2", 3600), ("48", 48 * 3600), ("500", 48 * 3600), ("x", 30 * 3600), ("nan", 30 * 3600)):
        monkeypatch.setenv("TFB_PF_CUSTODY_MAX_AGE_H", value)
        assert pa._env_custody_max_age_s() == seconds, value


def test_t4_sell_verdicts_are_shown_with_share_deltas_and_the_broker_note():
    rows = [holding("SELL.SR", Recommendation="STRONG_SELL", **{"Investability Status": "BLOCKED"}),
            holding("TRIM.SR", quantity=40, Recommendation="HOLD", **{"Intrinsic Value": 92}),
            holding("KEEP.SR", Recommendation="HOLD", **{"Intrinsic Value": 104})]
    build(rows)
    got = build(rows, evidence=None)
    assert_never_funds(got)
    r = by_symbol(got)
    verdicts = {s: row["detail"]["custody_tier"]["verdict"] for s, row in r.items()}
    assert verdicts["SELL.SR"] == "EXIT" and verdicts["TRIM.SR"] == "TRIM" and verdicts["KEEP.SR"] == "HOLD"
    assert r["SELL.SR"]["action"] == "EXIT" and r["SELL.SR"]["suggested_delta_shares"] == -10
    assert r["SELL.SR"]["proceeds_sar"] == r["SELL.SR"]["market_value_sar"] == 1000
    assert r["TRIM.SR"]["action"] == "TRIM" and -40 < r["TRIM.SR"]["suggested_delta_shares"] < 0
    assert 0 < r["TRIM.SR"]["proceeds_sar"] < r["TRIM.SR"]["market_value_sar"]
    for symbol in ("SELL.SR", "TRIM.SR"):
        assert r[symbol]["action_reason"].endswith("; " + pa.CUSTODY_SELL_NOTE)
        assert r[symbol]["post_trade_weight_pct"] is None and r[symbol]["weight_pct"] is None
    assert pa.CUSTODY_SELL_NOTE not in r["KEEP.SR"]["action_reason"]
    assert got["kpis"]["action_counts"] == {"ADD": 0, "HOLD": 1, "TRIM": 1, "EXIT": 1, "BLOCK": 0}
    assert got["kpis"]["proceeds_pending_sar"] == 0                      # a custody-tier sale is never pre-spent


def test_t5_a_row_changed_since_the_capture_stays_blocked_and_says_so():
    rows = [holding("AAA.SR"), holding("BBB.SR", quantity=4)]
    build(rows)
    rows[1] = holding("BBB.SR", quantity=9)                              # bought more after the capture
    rows.append(holding("NEW.SR", quantity=3))                           # a row the capture never saw
    got = build(rows, evidence=None)
    assert_never_funds(got)
    r = by_symbol(got)
    assert r["AAA.SR"]["action"] == "HOLD"
    assert r["BBB.SR"]["action"] == "BLOCK" and r["BBB.SR"]["action_reason"].startswith("RECONCILIATION_REQUIRED: ")
    assert " | the last IBKR capture (" in r["BBB.SR"]["action_reason"] and r["BBB.SR"]["action_reason"].endswith(
        "shows a different share count for this row")
    assert r["BBB.SR"]["advisor_note"].endswith("shows a different share count for this row")
    assert r["NEW.SR"]["action"] == "BLOCK" and "IBKR capture" not in r["NEW.SR"]["action_reason"]
    assert got["meta"]["custody_tier"]["kept"] == {"capture_differs": 1} and got["meta"]["custody_tier"]["shown"] == 1
    assert [a["type"] for a in got["alerts"]] == ["portfolio_inputs_unverified"] and got["alerts"][0]["count"] == 2
    assert got["kpis"]["action_counts"]["BLOCK"] == 2 and got["kpis"]["action_counts"]["HOLD"] == 1


def test_t6_rows_whose_own_inputs_are_unproven_stay_blocked():
    rows = [holding("AAA.SR", Recommendation="HOLD", **{"Intrinsic Value": 104}),
            holding("NOQUOTE.SR", Recommendation="HOLD", **{"Intrinsic Value": 104}),
            holding("BADFX.SR", Recommendation="HOLD", **{"Intrinsic Value": 104})]
    build(rows)
    rows[1] = dict(rows[1], Warnings="")                                 # no acquisition proof for the price
    rows[2] = dict(rows[2], **{"FX to SAR": 3.75})                       # a SAR row at a non-1 rate
    got = build(rows, evidence=None)
    r = by_symbol(got)
    assert r["AAA.SR"]["action"] == "HOLD"
    assert r["NOQUOTE.SR"]["action"] == "BLOCK" and r["NOQUOTE.SR"]["action_reason"].endswith(" | " + pa.CUSTODY_QUOTE_NOTE)
    assert r["BADFX.SR"]["action"] == "BLOCK" and pa.CUSTODY_QUOTE_NOTE not in r["BADFX.SR"]["action_reason"]
    assert got["meta"]["custody_tier"]["kept"] == {"quote_unverified": 1, "fx_rejected": 1}
    assert got["meta"]["custody_tier"]["matched"] == 3 and got["meta"]["custody_tier"]["shown"] == 1


def test_t6b_a_rule_block_verdict_stays_the_protective_row(monkeypatch):
    rows = [holding("AAA.SR")]
    build(rows)
    monkeypatch.setattr(pa, "_research_verdict", lambda cand, ctl: ("BLOCK", "cost basis implausible", 0.0))
    got = build(rows, evidence=None)
    assert got["actions"][0]["action"] == "BLOCK" and got["meta"]["custody_tier"]["kept"] == {"rule_block": 1}
    assert got["actions"][0]["action_reason"] == "RECONCILIATION_REQUIRED: missing_evidence"
    assert [a["type"] for a in got["alerts"]] == ["portfolio_inputs_unverified"]


def test_t7_a_fresh_packet_with_funding_withheld_still_proves_custody():
    rows = [holding("AAA.SR", Recommendation="HOLD", **{"Intrinsic Value": 104})]
    got = build(rows, request_cash=9_999)                                # one riyal off the certified cash
    assert_never_funds(got)
    row = got["actions"][0]
    assert got["meta"]["input_certification"]["reason_counts"] == {"cash_request_mismatch": 1}
    assert row["action"] == "HOLD" and row["action_reason"].endswith(
        " | holding matches the fresh IBKR capture; new money withheld (cash_request_mismatch)")
    assert got["meta"]["custody_tier"]["source"] == "packet" and "captured_at" not in got["meta"]["custody_tier"]
    # a packet that contradicts the row is never overruled by an older receipt
    build(rows)
    evidence = packet(rows)
    evidence["accounts"][0]["positions"][0]["quantity"] = 3
    got = build(rows, evidence=evidence)
    assert got["actions"][0]["action"] == "BLOCK" and got["meta"]["custody_tier"]["shown"] == 0
    assert got["actions"][0]["detail"]["reconciliation_status"] == "quantity_mismatch"
    assert "IBKR capture" not in got["actions"][0]["action_reason"] and got["meta"]["custody_tier"]["kept"] == {}
    assert pa._CUSTODY_RECEIPT["holdings"] == [{"symbol": "AAA.SR", "currency": "SAR", "quantity": "3"}]
    assert build(rows, evidence=None)["actions"][0]["action"] == "BLOCK"   # the newest capture now differs


def test_t8_observe_changes_text_only(monkeypatch):
    rows = [holding("AAA.SR", Recommendation="HOLD", **{"Intrinsic Value": 104})]
    build(rows)
    monkeypatch.setenv("TFB_PF_CUSTODY_TIER", "off")
    want = build(rows, evidence=None)
    monkeypatch.setenv("TFB_PF_CUSTODY_TIER", "observe")
    got = build(rows, evidence=None)
    tier = got["meta"].pop("custody_tier")
    assert tier["mode"] == "observe" and tier["shown"] == 0 and tier["verdicts"] == {"HOLD": 1}
    suffix = " | [custody-observe] would show HOLD"
    for key in ("action_reason", "advisor_note"):
        assert got["actions"][0][key] == want["actions"][0][key] + suffix
        got["actions"][0][key] = want["actions"][0][key]
    for page in (got, want):
        page["meta"].pop("generated_utc", None)
    assert got == want
    rows[0] = holding("AAA.SR", quantity=11, Recommendation="HOLD", **{"Intrinsic Value": 104})
    got = build(rows, evidence=None)                                    # observe never edits a blocked row either
    assert got["meta"]["custody_tier"]["kept"] == {"capture_differs": 1}
    assert got["actions"][0]["action_reason"] == "RECONCILIATION_REQUIRED: missing_evidence"
    rows[0] = holding("AAA.SR", Recommendation="HOLD", Warnings="", **{"Intrinsic Value": 104})
    got = build(rows, evidence=None)
    assert got["meta"]["custody_tier"]["kept"] == {"quote_unverified": 1}
    assert got["actions"][0]["action_reason"] == "RECONCILIATION_REQUIRED: missing_evidence"


def test_t9_an_old_receipt_and_a_truncated_input_change_nothing(monkeypatch):
    rows = [holding("AAA.SR")]
    build(rows)
    fresh = copy.deepcopy(pa._CUSTODY_RECEIPT)
    old = dict(fresh, captured_at=_stamp(dt.datetime.now(UTC) - dt.timedelta(hours=30, minutes=2)))
    monkeypatch.setattr(pa, "_CUSTODY_RECEIPT", old)
    got = build(rows, evidence=None)
    assert got["actions"][0]["action"] == "BLOCK" and got["meta"]["custody_tier"]["status"] == "no_recent_capture"
    monkeypatch.setenv("TFB_PF_CUSTODY_MAX_AGE_H", "31")
    assert build(rows, evidence=None)["actions"][0]["action"] == "HOLD"
    monkeypatch.setattr(pa, "_CUSTODY_RECEIPT", fresh)
    got = build(rows, evidence=None, upstream={"holdings_input_incomplete": True})
    assert got["actions"][0]["action"] == "BLOCK"
    assert got["meta"]["custody_tier"]["status"] == "skipped_holdings_input_incomplete"


def test_t10_a_fault_inside_the_tier_returns_the_protective_page(monkeypatch):
    rows = [holding("AAA.SR")]
    build(rows)
    monkeypatch.setattr(pa, "_custody_quote_level", lambda cand: 1 / 0)
    got = build(rows, evidence=None)
    assert got["actions"][0]["action"] == "BLOCK" and got["meta"]["custody_tier"] == {
        "mode": "enforce", "status": "error", "error_class": "ZeroDivisionError"}
    assert [a["type"] for a in got["alerts"]] == ["portfolio_inputs_unverified"]
    monkeypatch.setattr(pa, "custody_receipt", lambda *a: 1 / 0)       # a fault while remembering never breaks a run
    assert build(rows)["meta"]["execution_ready"] is True


def test_t11_the_research_view_and_the_tier_compose(monkeypatch):
    monkeypatch.setenv("TFB_PF_RESEARCH_VIEW", "enforce")
    rows = [holding("AAA.SR", Recommendation="HOLD", **{"Intrinsic Value": 104}), holding("BBB.SR", quantity=4)]
    build(rows)
    rows[1] = holding("BBB.SR", quantity=5)
    got = build(rows, evidence=None)
    r = by_symbol(got)
    assert r["AAA.SR"]["action"] == "HOLD" and "research verdict" not in r["AAA.SR"]["action_reason"]
    assert r["BBB.SR"]["action"] == "BLOCK" and "[custody-unverified]" in r["BBB.SR"]["action_reason"]
    assert r["BBB.SR"]["action_reason"].endswith("shows a different share count for this row")
    assert got["meta"]["research_view"]["status"] == "ok" and got["meta"]["custody_tier"]["shown"] == 1


# ----------------------------------------------------------------------------- the store
class FakeRedis:
    def __init__(self, fail=False):
        self.data, self.calls, self.fail, self.ttl = {}, 0, fail, None

    def set(self, key, value, ex=None):
        self.calls += 1
        if self.fail:
            raise RuntimeError("redis down")
        self.data[key], self.ttl = value, ex

    def get(self, key):
        self.calls += 1
        if self.fail:
            raise RuntimeError("redis down")
        return self.data.get(key)

    def eval(self, script, numkeys, *args):
        # The same decision as pa._CUSTODY_PUT_LUA; the script itself runs on a real server in test_s2c.
        self.calls += 1
        if self.fail:
            raise RuntimeError("redis down")
        assert script is pa._CUSTODY_PUT_LUA and numkeys == 2
        body_key, at_key, body, at, ttl = args
        assert (body_key, at_key) == ("tfb:pf:custody_receipt:v1", "tfb:pf:custody_receipt:v1:at")
        held = self.data.get(at_key)
        if held is not None and int(held) > int(at):
            return 0
        self.data[body_key], self.data[at_key], self.ttl = body, at, int(ttl)
        return 1

    def delete(self, key):
        self.calls += 1
        self.data.pop(key, None)


def _redis(monkeypatch, fake):
    monkeypatch.setenv("TFB_PF_CONFIRM_PERSIST", "1")
    monkeypatch.setattr(pa, "_CONFIRM_REDIS", fake)
    monkeypatch.setattr(pa, "_CONFIRM_REDIS_DEAD", False)


def test_s1_the_receipt_survives_a_restart_through_redis(monkeypatch):
    fake = FakeRedis()
    _redis(monkeypatch, fake)
    rows = [holding("AAA.SR", Recommendation="HOLD", **{"Intrinsic Value": 104})]
    build(rows)
    stored = json.loads(fake.data["tfb:pf:custody_receipt:v1"])
    assert stored == pa._CUSTODY_RECEIPT and fake.ttl == 48 * 3600
    assert set(stored) == {"schema", "captured_at", "holdings", "unrepresented"}
    for secret in ("synthetic-account", "10000", "10020", "cash", "synthetic://"):
        assert secret not in fake.data["tfb:pf:custody_receipt:v1"]
    monkeypatch.setattr(pa, "_CUSTODY_RECEIPT", None)                    # a new process
    got = build(rows, evidence=None)
    assert got["actions"][0]["action"] == "HOLD" and pa._CUSTODY_RECEIPT == stored


def test_s2_newer_wins_and_redis_trouble_never_blocks(monkeypatch):
    now = dt.datetime.now(UTC)
    newer = {"schema": "tfb.custody-receipt/v1", "captured_at": _stamp(now), "holdings": [], "unrepresented": 0}
    older = dict(newer, captured_at=_stamp(now - dt.timedelta(hours=2)))
    pa._custody_receipt_put(newer)
    pa._custody_receipt_put(older)
    assert pa._CUSTODY_RECEIPT is newer
    pa._custody_receipt_put({"captured_at": "not a time"})
    assert pa._CUSTODY_RECEIPT is newer
    fake = FakeRedis()
    fake.data["tfb:pf:custody_receipt:v1"] = json.dumps(older)
    _redis(monkeypatch, fake)
    assert pa._custody_receipt_get() is newer                           # an older stored copy does not replace it
    fake.data["tfb:pf:custody_receipt:v1"] = "not json"
    assert pa._custody_receipt_get() is newer and pa._CONFIRM_REDIS_DEAD is True      # latched, fail-open
    down = FakeRedis(fail=True)
    _redis(monkeypatch, down)
    rows = [holding("AAA.SR", Recommendation="HOLD", **{"Intrinsic Value": 104})]
    assert build(rows)["meta"]["execution_ready"] is True               # the write failed; the run did not
    assert build(rows, evidence=None)["actions"][0]["action"] == "HOLD"  # memory still serves

    class WriteDown(FakeRedis):                                          # the read works, the write does not
        def eval(self, *args):
            raise RuntimeError("redis write down")

    _redis(monkeypatch, WriteDown())
    monkeypatch.setattr(pa, "_CUSTODY_RECEIPT", None)
    later = dict(newer, captured_at=_stamp(now + dt.timedelta(minutes=1)))
    pa._custody_receipt_put(later)
    assert pa._CUSTODY_RECEIPT is later and pa._CONFIRM_REDIS_DEAD is True   # kept in memory; Redis latched off


def test_s2b_a_worker_with_empty_memory_never_rolls_the_shared_receipt_back(monkeypatch):
    # Review on PR #989: worker A stored a capture showing 3 shares; worker B (empty memory) then receives a
    # still-valid packet captured a minute earlier showing 10. The shared receipt must stay the newer one.
    fake = FakeRedis()
    _redis(monkeypatch, fake)
    now = dt.datetime.now(UTC)
    rows = [holding("AAA.SR", Recommendation="HOLD", **{"Intrinsic Value": 104})]
    newer = {"schema": "tfb.custody-receipt/v1", "captured_at": _stamp(now), "unrepresented": 0,
             "holdings": [{"symbol": "AAA.SR", "currency": "SAR", "quantity": "3"}]}
    fake.data["tfb:pf:custody_receipt:v1"] = json.dumps(newer)
    older = packet(rows, when=now - dt.timedelta(minutes=1))
    got = build(rows, evidence=older)                                    # worker B: valid, fresh, older
    assert got["meta"]["execution_ready"] is True
    assert json.loads(fake.data["tfb:pf:custody_receipt:v1"]) == newer and pa._CUSTODY_RECEIPT == newer
    blocked = build(rows, evidence=None)["actions"][0]                   # 10 on the page, 3 at the newest capture
    assert blocked["action"] == "BLOCK" and blocked["action_reason"].endswith("shows a different share count for this row")
    later = packet(rows, when=now + dt.timedelta(seconds=1))             # a newer packet does replace it
    later["accounts"][0]["positions_asof"] = later["accounts"][0]["cash_asof"] = later["captured_at"] = _stamp(now)
    monkeypatch.setattr(pa, "_CUSTODY_RECEIPT", None)
    fake.data["tfb:pf:custody_receipt:v1"] = json.dumps(dict(newer, captured_at=_stamp(now - dt.timedelta(minutes=5))))
    build(rows, evidence=later)
    assert json.loads(fake.data["tfb:pf:custody_receipt:v1"])["holdings"][0]["quantity"] == "10"


def test_s2c_the_write_is_one_redis_step_and_the_newest_capture_wins(monkeypatch, tmp_path):
    # The race the pre-read cannot close: worker B read "nothing newer", worker A then stored a newer capture,
    # and B writes last. The decision is taken inside Redis, so B's older receipt is refused there.
    now = dt.datetime.now(UTC)
    mine = {"schema": "tfb.custody-receipt/v1", "captured_at": _stamp(now), "holdings": [], "unrepresented": 0}
    theirs = dict(mine, captured_at=_stamp(now + dt.timedelta(minutes=3)), unrepresented=7)

    class Raced(FakeRedis):
        def eval(self, *args):
            if "tfb:pf:custody_receipt:v1" not in self.data:             # worker A lands between B's read and write
                at = (dt.datetime.fromisoformat(theirs["captured_at"].replace("Z", "+00:00"))
                      - dt.datetime(1970, 1, 1, tzinfo=UTC)) // dt.timedelta(microseconds=1)
                self.data.update({"tfb:pf:custody_receipt:v1": json.dumps(theirs),
                                  "tfb:pf:custody_receipt:v1:at": str(at)})
            return super().eval(*args)

    fake = Raced()
    _redis(monkeypatch, fake)
    pa._custody_receipt_put(mine)
    assert json.loads(fake.data["tfb:pf:custody_receipt:v1"]) == theirs
    assert pa._CUSTODY_RECEIPT is None and pa._custody_receipt_get() == theirs   # memory never kept the loser
    pa._custody_receipt_put(dict(theirs, unrepresented=8))               # the same instant may be written again
    assert json.loads(fake.data["tfb:pf:custody_receipt:v1"])["unrepresented"] == 8 and fake.ttl == 48 * 3600
    exact = dict(mine, captured_at=now.replace(microsecond=250000).isoformat().replace("+00:00", "Z"))
    fake2 = FakeRedis()
    _redis(monkeypatch, fake2)
    monkeypatch.setattr(pa, "_CUSTODY_RECEIPT", None)
    pa._custody_receipt_put(exact)                                       # the instant is kept to the microsecond
    assert int(fake2.data["tfb:pf:custody_receipt:v1:at"]) % 1_000_000 == 250000


def test_s2d_the_script_itself_on_a_real_redis_server(monkeypatch, tmp_path):
    import shutil
    import subprocess
    import time
    redis = pytest.importorskip("redis")
    server = shutil.which("redis-server") or shutil.which("valkey-server")
    if not server:
        pytest.skip("no redis-server on this machine; FakeRedis.eval carries the same decision")
    sock = str(tmp_path / "r.sock")
    proc = subprocess.Popen([server, "--port", "0", "--unixsocket", sock, "--save", "", "--appendonly", "no",
                             "--dir", str(tmp_path)], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    try:
        cli = redis.Redis(unix_socket_path=sock, decode_responses=True)
        for _ in range(100):
            try:
                cli.ping()
                break
            except Exception:  # noqa: BLE001
                time.sleep(0.05)
        else:
            pytest.skip("the local redis-server did not start")
        _redis(monkeypatch, cli)
        now = dt.datetime.now(UTC)
        first = {"schema": "tfb.custody-receipt/v1", "captured_at": _stamp(now), "holdings": [], "unrepresented": 0}
        older = dict(first, captured_at=_stamp(now - dt.timedelta(minutes=1)), unrepresented=1)
        newer = dict(first, captured_at=_stamp(now + dt.timedelta(minutes=1)), unrepresented=2)
        key, at_key = "tfb:pf:custody_receipt:v1", "tfb:pf:custody_receipt:v1:at"
        pa._custody_receipt_put(first)
        assert json.loads(cli.get(key)) == first and 0 < cli.ttl(key) <= 48 * 3600 and 0 < cli.ttl(at_key) <= 48 * 3600
        args = (2, key, at_key)
        stale = str(int(cli.get(at_key)) - 60_000_000)
        assert cli.eval(pa._CUSTODY_PUT_LUA, *args, json.dumps(older), stale, "172800") == 0   # refused in Redis
        assert json.loads(cli.get(key)) == first
        monkeypatch.setattr(pa, "_CUSTODY_RECEIPT", None)                # another worker, empty memory
        pa._custody_receipt_put(older)
        assert json.loads(cli.get(key)) == first and pa._CUSTODY_RECEIPT == first
        pa._custody_receipt_put(newer)
        assert json.loads(cli.get(key)) == newer and pa._CUSTODY_RECEIPT is newer and pa._CONFIRM_REDIS_DEAD is False
        again = dict(newer, unrepresented=9)
        pa._custody_receipt_put(again)                                   # the same instant is not "older"
        assert json.loads(cli.get(key)) == again
    finally:
        proc.terminate()
        proc.wait(timeout=10)


def test_s3_the_persist_kill_switch_keeps_the_receipt_in_memory_only(monkeypatch):
    fake = FakeRedis()
    _redis(monkeypatch, fake)
    monkeypatch.setenv("TFB_PF_CONFIRM_PERSIST", "0")
    rows = [holding("AAA.SR", Recommendation="HOLD", **{"Intrinsic Value": 104})]
    build(rows)
    assert build(rows, evidence=None)["actions"][0]["action"] == "HOLD"
    assert fake.calls == 0 and fake.data == {}


# ----------------------------------------------------------------------------- 1.18.1: the price level
def _quoted(symbol, when, **updates):
    stamp = when.replace(microsecond=0).isoformat().replace("+00:00", "Z")
    return holding(symbol, **dict({"Warnings": "acquisition_status:success; acquisition_provider:EODHD; "
                                               "acquisition_acquired_at:" + _stamp() + "; acquisition_quote_asof:"
                                               + stamp}, **updates))


def _venue(monkeypatch, is_open, close):
    monkeypatch.setattr(pa._ob, "_venue_state", lambda *_args: (is_open, close))


def test_p1_while_the_venue_trades_the_verdict_stays_and_says_how_old_the_price_is(monkeypatch):
    # Every evening from the US open: the sheet quote is hours old, the live rule fails, 1.18.0 printed BLOCK.
    now = dt.datetime.now(UTC)
    _venue(monkeypatch, True, now - dt.timedelta(hours=20))
    rows = [_quoted("AAA.SR", now - dt.timedelta(hours=3), Recommendation="HOLD", **{"Intrinsic Value": 104}),
            holding("BBB.SR", Recommendation="HOLD", **{"Intrinsic Value": 104})]
    live = [holding("AAA.SR", Recommendation="HOLD", **{"Intrinsic Value": 104}), rows[1]]
    assert build(live)["meta"]["execution_ready"] is True                # a capture certified these holdings
    got = build(rows, evidence=None)
    assert_never_funds(got)
    r = by_symbol(got)
    assert r["AAA.SR"]["action"] == "HOLD" and r["BBB.SR"]["action"] == "HOLD"
    note = r["AAA.SR"]["action_reason"].split(" | ")[-1]
    assert note.startswith("price of ") and " Riyadh (3.0 h old) is the latest session's, not a live quote; " in note
    assert note.endswith("confirm it at the broker before acting")
    quoted = (now - dt.timedelta(hours=3) + dt.timedelta(hours=3)).strftime("%Y-%m-%d %H:%M")
    assert ("price of %s Riyadh" % quoted) in note
    assert "holding matches the IBKR capture of " in r["AAA.SR"]["action_reason"]
    assert "latest session" not in r["BBB.SR"]["action_reason"]
    assert r["AAA.SR"]["detail"]["custody_tier"]["price"] == "session"
    assert r["BBB.SR"]["detail"]["custody_tier"]["price"] == "live"
    tier = got["meta"]["custody_tier"]
    assert tier["prices"] == {"session": 1, "live": 1} and tier["kept"] == {} and tier["shown"] == 2
    assert got["kpis"]["action_counts"]["BLOCK"] == 0
    assert pa._custody_quote_level(pa.normalize_holding(rows[0], {"SAR": 1}, pa.make_controls())) == "session"
    assert pa._custody_quote_ok(pa.normalize_holding(rows[0], {"SAR": 1}, pa.make_controls())) is True


def test_p2_a_capture_taken_in_session_shows_the_verdicts_too(monkeypatch):
    # A fresh packet with an in-session quote: funding is withheld (holding_quote_unverified), custody is proven.
    now = dt.datetime.now(UTC)
    _venue(monkeypatch, True, now - dt.timedelta(hours=20))
    rows = [_quoted("AAA.SR", now - dt.timedelta(hours=3), Recommendation="HOLD", **{"Intrinsic Value": 104})]
    got = build(rows)
    assert_never_funds(got)
    assert got["meta"]["input_certification"]["reason_counts"] == {"holding_quote_unverified": 1}
    row = got["actions"][0]
    assert row["action"] == "HOLD" and got["meta"]["custody_tier"]["source"] == "packet"
    assert " | holding matches the fresh IBKR capture; new money withheld (holding_quote_unverified) | price of " \
        in row["action_reason"]
    assert got["meta"]["custody_tier"]["prices"] == {"session": 1}


def test_p3_a_sale_is_never_sized_from_a_session_price(monkeypatch):
    now = dt.datetime.now(UTC)
    rows = [holding("SELL.SR", Recommendation="STRONG_SELL", **{"Investability Status": "BLOCKED"}),
            holding("TRIM.SR", quantity=40, Recommendation="HOLD", **{"Intrinsic Value": 92})]
    build(rows)
    live = by_symbol(build(rows, evidence=None))
    assert live["SELL.SR"]["suggested_delta_shares"] == -10 and live["TRIM.SR"]["proceeds_sar"] > 0
    _venue(monkeypatch, True, now - dt.timedelta(hours=20))
    old = now - dt.timedelta(hours=2)
    stale = [_quoted("SELL.SR", old, Recommendation="STRONG_SELL", **{"Investability Status": "BLOCKED"}),
             _quoted("TRIM.SR", old, quantity=40, Recommendation="HOLD", **{"Intrinsic Value": 92})]
    got = build(stale, evidence=None)
    assert_never_funds(got)
    r = by_symbol(got)
    assert r["SELL.SR"]["action"] == "EXIT" and r["TRIM.SR"]["action"] == "TRIM"
    for symbol in ("SELL.SR", "TRIM.SR"):
        assert r[symbol]["suggested_delta_shares"] is None and r[symbol]["suggested_delta_sar"] is None
        assert r[symbol]["proceeds_sar"] == 0 and r[symbol]["detail"]["custody_tier"]["price"] == "session"
        assert r[symbol]["action_reason"].endswith(
            "confirm it at the broker before acting; no sale size is computed from it")
        assert pa.CUSTODY_SELL_NOTE in r[symbol]["action_reason"]
    assert got["kpis"]["proceeds_pending_sar"] == 0
    assert got["kpis"]["action_counts"] == {"ADD": 0, "HOLD": 0, "TRIM": 1, "EXIT": 1, "BLOCK": 0}


def test_p4_a_price_older_than_the_session_and_the_switch_off_keep_the_block(monkeypatch):
    now = dt.datetime.now(UTC)
    close = now - dt.timedelta(hours=20)
    _venue(monkeypatch, True, close)
    fresh = [holding("AAA.SR", Recommendation="HOLD", **{"Intrinsic Value": 104})]
    build(fresh)
    older = [_quoted("AAA.SR", close - dt.timedelta(hours=5), Recommendation="HOLD", **{"Intrinsic Value": 104})]
    got = build(older, evidence=None)                                    # yesterday's intraday price
    assert got["actions"][0]["action"] == "BLOCK" and got["meta"]["custody_tier"]["kept"] == {"quote_unverified": 1}
    assert got["actions"][0]["action_reason"].endswith(" | " + pa.CUSTODY_QUOTE_NOTE)
    assert got["meta"]["custody_tier"]["prices"] == {}
    session = [_quoted("AAA.SR", now - dt.timedelta(hours=3), Recommendation="HOLD", **{"Intrinsic Value": 104})]
    assert build(session, evidence=None)["actions"][0]["action"] == "HOLD"
    monkeypatch.setenv("TFB_T10_RESEARCH_QUOTE", "off")                  # 1.18.0
    off = build(session, evidence=None)
    assert off["actions"][0]["action"] == "BLOCK" and off["meta"]["custody_tier"]["kept"] == {"quote_unverified": 1}
    assert pa._custody_quote_level(pa.normalize_holding(session[0], {"SAR": 1}, pa.make_controls())) is None
    assert pa._custody_quote_ok(pa.normalize_holding(session[0], {"SAR": 1}, pa.make_controls())) is False


def test_p5_the_level_and_the_note_never_raise(monkeypatch):
    assert pa._custody_quote_level({}) is None and pa._custody_quote_level({"quote_evidence": {"status": "SUCCESS"}}) is None
    monkeypatch.setattr(pa._ob, "_quote_freshness_assessment", lambda cand: 1 / 0)
    assert pa._custody_quote_level({"quote_evidence": {"status": "SUCCESS", "quote_asof": _stamp()}}) is None
    assert pa._custody_price_note({}, False) == ("price of an earlier time is the latest session's, not a live quote; "
                                                 "confirm it at the broker before acting")
    half = {"quote_evidence": {"quote_asof": _stamp(dt.datetime.now(UTC) - dt.timedelta(minutes=30))}}
    assert " Riyadh (30 min old) is the latest session's" in pa._custody_price_note(half, False)
    assert pa._custody_price_note(half, True).endswith("; no sale size is computed from it")


def test_v1_versions_and_pins():
    from pathlib import Path
    root = Path(__file__).resolve().parents[1]
    assert pa.PORTFOLIO_ACTIONS_VERSION == "1.20.0" and pr.__version__ == "1.1.0"   # 1.20.0 [TOTAL-RETURN]
    manifest = (root / "scripts/verify_deployment.py").read_text(encoding="utf-8")
    assert '"1.20.0", "portfolio actions")' in manifest
    assert '"1.31.0", "opportunity builder")' in manifest
    assert """("core.portfolio_reconciliation", "__version__", '1.1.0'""" in manifest
    ci = (root / ".github/workflows/ci.yml").read_text(encoding="utf-8")
    assert "tests/test_pf_custody_tier.py" in ci
