"""Board allocation regressions using the real opportunity builder.

The research pass is deliberately independent of cash and seat allocation.
Only the symbols chosen by the cockpit's stability pass enter the existing
allocator, using the signed research snapshot rather than refreshed inputs.
"""

from __future__ import annotations

import ast
import copy
import inspect
import json
import os
from typing import Any

import pytest

from core.analysis import opportunity_builder as ob


from decision_evidence_fixtures import build_with_observed_inputs, observed_portfolio

CRITERIA = {
    "max_selected": 2,
    "max_per_sector": 1,
    "max_per_market": 10,
    "max_weight_pct": 100.0,
    "pf_max_sector_pct": 100.0,
    "min_ticket_sar": 5_000.0,
    "rank_by_engine_roi_enabled": True,
    "trust_gate_enabled": False,
}
PORTFOLIO = {"cash_available_sar": 10_000.0}
FX = {"SAR": 1.0}
PORTFOLIO = observed_portfolio(PORTFOLIO, FX)
FUNDING_ALERTS = {"capital_call", "rotation_proposal", "unfunded_candidates"}
AUTH_ENV_KEYS = (
    "APP_TOKEN", "TFB_APP_TOKEN", "BACKEND_TOKEN", "BACKUP_APP_TOKEN",
    "ALLOWED_TOKENS", "TFB_ALLOWED_TOKENS", "APP_TOKENS",
    "X_APP_TOKEN", "API_KEY", "TFB_TOKEN",
)
POLICY_ENV_CHANGES = (
    ("TFB_FORECAST_BASIS", "legacy", "plan3m"),
    ("TFB_TICKET_FRESHNESS_GATE", "1", "0"),
    ("TFB_TICKET_MAX_QUOTE_AGE_MIN", "15", "30"),
    ("TFB_TICKET_FALLBACK_MAX_AGE_H", "78", "1"),
    ("TFB_COMPLIANCE_SURFACE_GATE", "1", "0"),
    ("TFB_ELIGIBILITY_GATE", "1", "0"),
    ("TFB_GLOBAL_ACTIVITY_SCREEN", "", "full"),
    ("TFB_SHARIAH_FAIL_LIST", "UNRELATED.SR", "LATER.SR"),
    ("TFB_EXIT_BY_RULE_EXTRA", "", "LATER.SR"),
    ("TFB_KSA_FOREIGN_RESTRICTED", "UNRELATED.SR", "LATER.SR"),
)


def _row(symbol: str, roi: float = 24.0, **overrides: Any) -> dict[str, Any]:
    row = {
        "symbol": symbol,
        "name": "Synthetic " + symbol,
        "sector": "Energy",
        "market": "Tadawul",
        "currency": "SAR",
        "current_price": 100.0,
        "intrinsic_value": 130.0,
        "forecast_reliability_score": 82.0,
        "data_quality_score": 91.0,
        "risk_bucket": "Moderate",
        "provider_engine_conflict": "No",
        "volatility_30d": 4.0,
        "avg_volume_30d": 2_500_000,
        "expected_roi_12m": roi,
        "recommendation_detailed": "STRONG BUY",
        "investability_status": "INVESTABLE",
        "block_reason": "",
    }
    from decision_evidence_fixtures import observed_price_fields
    row.update(observed_price_fields())
    row.update(overrides)
    return row


@pytest.fixture(autouse=True)
def _isolated_builder_environment(monkeypatch):
    # Prevent ambient production flags or earlier tests from changing the
    # synthetic fixtures. All production threshold defaults remain intact.
    for key in list(os.environ):
        if key.startswith("TFB_OPP_") or key.startswith("TFB_T10_"):
            monkeypatch.delenv(key, raising=False)
    for key in AUTH_ENV_KEYS + tuple(item[0] for item in POLICY_ENV_CHANGES):
        monkeypatch.delenv(key, raising=False)
    monkeypatch.delenv("RENDER_GIT_COMMIT", raising=False)
    monkeypatch.setenv("TFB_OPP_ENABLED", "1")
    monkeypatch.setenv("TFB_OPP_FUNDING_PLAN", "1")
    monkeypatch.setenv("APP_TOKEN", "synthetic-board-test")


def _build(rows, criteria=None, portfolio=None):
    return build_with_observed_inputs(ob,
        copy.deepcopy(rows),
        criteria={**CRITERIA, **(criteria or {})},
        portfolio=copy.deepcopy(PORTFOLIO if portfolio is None else portfolio),
        fx_rates=dict(FX),
    )


def _research(rows, criteria=None, portfolio=None):
    payload = _build(
        rows,
        {**(criteria or {}), "board_funding_stage": "research"},
        portfolio,
    )
    assert payload["status"] == "ok", payload.get("message")
    return payload


def _allocate(research, symbols, *, criteria=None, portfolio=None, snapshot=None, rows=None):
    frozen = copy.deepcopy(
        snapshot if snapshot is not None else research["meta"]["board_funding"]["snapshot"]
    )
    return _build(
        frozen["rows"] if rows is None else rows,
        {
            **(criteria or {}),
            "board_funding_stage": "allocate",
            "board_funding_symbols": symbols,
            "board_funding_snapshot": frozen,
        },
        portfolio,
    )


def _assert_no_execution(payload):
    kpis = payload["kpis"]
    assert kpis.get("selected_count", 0) == 0
    assert kpis.get("expected_gain_12m_sar", 0) == 0
    assert kpis.get("fundable_now", 0) == 0
    assert kpis.get("fundable_by_rotation", 0) == 0
    assert kpis.get("capital_call", 0) == 0
    assert kpis.get("capital_call_topn_sar", 0) == 0
    assert not (FUNDING_ALERTS & {a["type"] for a in payload["alerts"]})
    for ticket in payload["selected"]:
        assert not ticket.get("suggested_shares")
        assert not ticket.get("suggested_sar")
        assert not ticket.get("exp_gain_12m_sar")


def test_ordinary_requests_keep_existing_allocation_behavior():
    ordinary = _build([_row("HIGH.SR"), _row("LATER.SR", 20.0)])
    assert [t["symbol"] for t in ordinary["selected"]] == ["HIGH.SR"]
    assert ordinary["selected"][0]["suggested_sar"] == 10_000.0
    assert ordinary["kpis"]["selected_count"] == 1
    assert "board_funding" not in ordinary["meta"]


def test_research_retains_all_qualified_names_without_cash_or_sector_reservations():
    rows = [_row("HIGH.SR"), _row("LATER.SR", 20.0), _row("THIRD.SR", 18.0)]
    payload = _research(rows)
    assert [t["symbol"] for t in payload["selected"]] == [
        "HIGH.SR", "LATER.SR", "THIRD.SR"
    ]
    assert payload["kpis"]["passed"] == 3
    assert payload["kpis"]["capital_unallocated_sar"] == 10_000.0
    _assert_no_execution(payload)
    assert not any(row.get("failed_gate") == "Funding" for row in payload["near_miss"])


def test_research_zero_cash_produces_no_capital_solicitation():
    payload = _research([_row("HIGH.SR"), _row("LATER.SR", 20.0)], portfolio={
        "cash_available_sar": 0.0
    })
    assert len(payload["selected"]) == 2
    assert payload["kpis"]["capital_unallocated_sar"] == 0
    _assert_no_execution(payload)


def test_research_snapshot_preserves_exact_qualified_input_and_signed_basis():
    rows = [_row("HIGH.SR"), _row("LATER.SR", 20.0), _row("BLOCKED.SR", 30.0,
        investability_status="BLOCKED", block_reason="synthetic identity mismatch")]
    payload = _research(rows)
    funding = payload["meta"]["board_funding"]
    assert funding["contract_version"] == 1
    assert funding["stage"] == "research"
    assert funding["snapshot_available"] is True
    frozen = funding["snapshot"]
    assert frozen["rows"] == rows[:2]
    assert frozen["snapshot_id"] == funding["snapshot_id"]
    assert frozen["issued_at"]
    assert frozen["basis_fingerprint"]
    assert len(frozen["rows"]) <= 500
    assert isinstance(frozen["xchecks"], dict)


def test_snapshot_overflow_keeps_research_display_and_disables_allocation():
    rows = [_row("S%03d.SR" % i) for i in range(501)]
    payload = _research(rows)
    funding = payload["meta"]["board_funding"]
    assert funding["snapshot_available"] is False
    assert funding.get("reason")
    assert not funding.get("snapshot")
    assert len(payload["selected"]) == 501
    _assert_no_execution(payload)


def test_later_confirmed_name_receives_cash_and_sector_capacity_before_research_names():
    research = _research([_row("HIGH.SR"), _row("LATER.SR", 20.0)])
    allocated = _allocate(research, ["LATER.SR"])
    assert allocated["status"] == "ok", allocated.get("message")
    assert [t["symbol"] for t in allocated["selected"]] == ["LATER.SR"]
    ticket = allocated["selected"][0]
    assert ticket["suggested_sar"] == 10_000.0
    assert ticket["suggested_shares"] == 100
    assert allocated["kpis"]["fundable_now"] == 1
    assert allocated["kpis"]["selected_count"] == 1
    assert allocated["kpis"]["capital_unallocated_sar"] == 0.0
    assert allocated["kpis"]["expected_gain_12m_sar"] == ticket["exp_gain_12m_sar"]
    assert allocated["meta"]["board_funding"]["snapshot_id"] == research[
        "meta"]["board_funding"]["snapshot_id"]
    assert not any(row.get("symbol") == "HIGH.SR" and row.get("failed_gate") == "Funding"
                   for row in allocated["near_miss"])


def test_empty_final_eligibility_reserves_nothing_and_requests_no_money():
    research = _research([_row("HIGH.SR"), _row("LATER.SR", 20.0)])
    allocated = _allocate(research, [])
    assert allocated["status"] == "ok", allocated.get("message")
    assert allocated["selected"] == []
    assert allocated["kpis"]["capital_unallocated_sar"] == 10_000.0
    _assert_no_execution(allocated)


def test_funding_shortfall_and_capital_call_cover_only_final_eligible_names():
    portfolio = {"cash_available_sar": 4_000.0}
    research = _research([_row("HIGH.SR"), _row("LATER.SR", 20.0)], portfolio=portfolio)
    allocated = _allocate(research, ["LATER.SR"], portfolio=portfolio)
    assert allocated["status"] == "ok", allocated.get("message")
    assert allocated["selected"] == []
    assert allocated["kpis"]["capital_call"] == 1
    calls = [a for a in allocated["alerts"] if a["type"] == "capital_call"]
    assert len(calls) == 1
    assert "LATER.SR" in calls[0]["required_action"]
    assert "HIGH.SR" not in calls[0]["required_action"]
    funding_rows = [row for row in allocated["near_miss"] if row.get("failed_gate") == "Funding"]
    assert {row["symbol"] for row in funding_rows} == {"LATER.SR"}


@pytest.mark.parametrize("change", ["rows", "portfolio", "criteria", "signature"])
def test_allocation_rejects_changed_research_basis_without_money(change):
    research = _research([_row("HIGH.SR"), _row("LATER.SR", 20.0)])
    kwargs = {}
    if change == "rows":
        kwargs["rows"] = copy.deepcopy(research["meta"]["board_funding"]["snapshot"]["rows"])
        kwargs["rows"][0]["current_price"] = 99.0
    elif change == "portfolio":
        kwargs["portfolio"] = {"cash_available_sar": 20_000.0}
    elif change == "criteria":
        kwargs["criteria"] = {"max_per_sector": 2}
    else:
        kwargs["snapshot"] = copy.deepcopy(research["meta"]["board_funding"]["snapshot"])
        kwargs["snapshot"]["snapshot_id"] = "invalid-synthetic-signature"
    allocated = _allocate(research, ["LATER.SR"], **kwargs)
    assert allocated["status"] == "board_funding_mismatch"
    _assert_no_execution(allocated)


def test_eligibility_cannot_add_a_candidate_absent_from_qualified_snapshot():
    research = _research([_row("HIGH.SR"), _row("BLOCKED.SR", 30.0,
        investability_status="BLOCKED", block_reason="synthetic identity mismatch")])
    allocated = _allocate(research, ["BLOCKED.SR"])
    assert allocated["selected"] == []
    _assert_no_execution(allocated)


def test_allocation_reuses_existing_weight_lot_and_funding_policy():
    criteria = {"max_weight_pct": 25.0, "min_ticket_sar": 0.0, "lot_size": 3}
    research = _research([_row("HIGH.SR"), _row("LATER.SR", 20.0)], criteria=criteria)
    allocated = _allocate(research, ["LATER.SR"], criteria=criteria)
    ordinary = _build([_row("LATER.SR", 20.0)], criteria=criteria)
    assert allocated["selected"] == ordinary["selected"]
    assert allocated["selected"][0]["suggested_shares"] == 24
    for key in ("expected_gain_12m_sar", "capital_unallocated_sar", "fundable_now"):
        assert allocated["kpis"][key] == ordinary["kpis"][key]


def test_same_frozen_allocation_is_idempotent():
    research = _research([_row("HIGH.SR"), _row("LATER.SR", 20.0)])
    first = _allocate(research, ["LATER.SR"])
    second = _allocate(research, ["LATER.SR"])
    first["meta"].pop("generated_at_utc", None)
    second["meta"].pop("generated_at_utc", None)
    assert json.dumps(first, sort_keys=True) == json.dumps(second, sort_keys=True)


def test_expired_snapshot_is_rejected_without_funding(monkeypatch):
    research = _research([_row("HIGH.SR"), _row("LATER.SR", 20.0)])
    issued = research["meta"]["board_funding"]["snapshot"]["issued_at"]
    monkeypatch.setattr(ob.time, "time", lambda: issued + 181.0)
    allocated = _allocate(research, ["LATER.SR"])
    assert allocated["status"] == "board_funding_mismatch"
    assert "expired" in allocated.get("message", "")
    _assert_no_execution(allocated)


def test_changed_environment_policy_invalidates_snapshot(monkeypatch):
    research = _research([_row("HIGH.SR"), _row("LATER.SR", 20.0)])
    monkeypatch.setenv("TFB_OPP_FUNDING_SETTLED_ONLY", "1")
    allocated = _allocate(research, ["LATER.SR"])
    assert allocated["status"] == "board_funding_mismatch"
    _assert_no_execution(allocated)


@pytest.mark.parametrize("key,before,after", POLICY_ENV_CHANGES,
    ids=[change[0] for change in POLICY_ENV_CHANGES])
def test_non_opportunity_policy_change_rejects_replay_before_allocation(
    monkeypatch, key, before, after,
):
    monkeypatch.setenv(key, before)
    research = _research([_row("HIGH.SR"), _row("LATER.SR", 20.0)])
    assert research["meta"]["board_funding"]["snapshot_available"] is True
    monkeypatch.setenv(key, after)

    def forbidden_allocator(*_args, **_kwargs):
        raise AssertionError("changed policy must be rejected before allocation")

    monkeypatch.setattr(ob, "_select_and_size", forbidden_allocator)
    rejected = _allocate(research, ["LATER.SR"])
    assert rejected["status"] == "board_funding_mismatch"
    assert "basis changed" in rejected.get("message", "")
    _assert_no_execution(rejected)


def test_all_direct_builder_policy_reads_are_bound_to_replay_fingerprint(monkeypatch):
    # Behavioral parameter cases above cover the discovered omissions. This
    # source guard catches a newly introduced execution setting before it can
    # silently bypass snapshot validation. Acquisition credentials are outside
    # this TFB policy family; their results are already frozen as signed rows.
    policy_names = set()
    for node in ast.walk(ast.parse(inspect.getsource(ob))):
        if not isinstance(node, ast.Call) or not node.args:
            continue
        function = (node.func.attr if isinstance(node.func, ast.Attribute)
                    else node.func.id if isinstance(node.func, ast.Name) else "")
        if function != "getenv" and not function.startswith("_env_"):
            continue
        first = node.args[0]
        if (isinstance(first, ast.Constant) and isinstance(first.value, str)
                and first.value.startswith("TFB_")):
            policy_names.add(first.value)
    assert len(policy_names) >= 80  # Ensure the scan traversed the full builder.
    baseline = ob._board_funding_basis(CRITERIA, PORTFOLIO, FX)
    unbound = []
    for name in sorted(policy_names):
        with monkeypatch.context() as isolated:
            isolated.setenv(name, "synthetic-distinct-policy-value")
            if ob._board_funding_basis(CRITERIA, PORTFOLIO, FX) == baseline:
                unbound.append(name)
    assert not unbound, "Policy reads missing from replay fingerprint: " + ", ".join(unbound)


@pytest.mark.parametrize("rotated_key,initial", [
    ("APP_TOKEN", {"APP_TOKEN": "primary-v1", "BACKUP_APP_TOKEN": "backup-v1"}),
    ("BACKUP_APP_TOKEN", {"APP_TOKEN": "primary-v1", "BACKUP_APP_TOKEN": "backup-v1"}),
    ("ALLOWED_TOKENS", {"APP_TOKEN": "inactive-primary", "ALLOWED_TOKENS": "active-v1,active-backup"}),
])
def test_any_active_token_rotation_rejects_existing_snapshot_before_allocation(
    monkeypatch, rotated_key, initial,
):
    for key, value in initial.items():
        monkeypatch.setenv(key, value)
    research = _research([_row("LATER.SR", 20.0)])
    assert _allocate(research, ["LATER.SR"])["status"] == "ok"
    monkeypatch.setenv(rotated_key, "rotated-active-token")

    def forbidden_allocator(*_args, **_kwargs):
        raise AssertionError("active token rotation must invalidate before allocation")

    monkeypatch.setattr(ob, "_select_and_size", forbidden_allocator)
    rejected = _allocate(research, ["LATER.SR"])
    assert rejected["status"] == "board_funding_mismatch"
    assert "signature mismatch" in rejected.get("message", "")
    _assert_no_execution(rejected)
    serialized = json.dumps([research, rejected])
    assert all(token.strip() not in serialized
               for value in initial.values() for token in value.split(","))
    assert "rotated-active-token" not in serialized


def test_inactive_app_token_rotation_does_not_change_active_allowed_token_set(monkeypatch):
    monkeypatch.setenv("APP_TOKEN", "inactive-primary-v1")
    monkeypatch.setenv("ALLOWED_TOKENS", "active-primary,active-backup")
    research = _research([_row("LATER.SR", 20.0)])
    first = _allocate(research, ["LATER.SR"])
    monkeypatch.setenv("APP_TOKEN", "inactive-primary-v2")
    second = _allocate(research, ["LATER.SR"])
    assert first["status"] == second["status"] == "ok"
    assert first["selected"] == second["selected"]
    assert first["kpis"] == second["kpis"]


def test_active_csv_token_order_whitespace_and_duplicates_preserve_signature(monkeypatch):
    monkeypatch.delenv("APP_TOKEN")
    monkeypatch.setenv("ALLOWED_TOKENS", "active-primary,active-backup")
    research = _research([_row("LATER.SR", 20.0)])
    first = _allocate(research, ["LATER.SR"])
    monkeypatch.setenv("ALLOWED_TOKENS", " active-backup , active-primary , active-backup ")
    second = _allocate(research, ["LATER.SR"])
    assert first["status"] == second["status"] == "ok"
    assert first["selected"] == second["selected"]
    assert first["kpis"] == second["kpis"]
    assert all(token not in json.dumps([research, first, second])
               for token in ("active-primary", "active-backup"))


@pytest.mark.parametrize("initial_release", (None, "synthetic-release-a"),
    ids=("local-to-render", "render-release-change"))
def test_changed_backend_release_identity_rejects_snapshot_before_allocation(
    monkeypatch, initial_release,
):
    if initial_release is not None:
        monkeypatch.setenv("RENDER_GIT_COMMIT", initial_release)
    research = _research([_row("LATER.SR", 20.0)])
    assert _allocate(research, ["LATER.SR"])["status"] == "ok"
    monkeypatch.setenv("RENDER_GIT_COMMIT", "synthetic-release-b")

    def forbidden_allocator(*_args, **_kwargs):
        raise AssertionError("release identity drift must reject before allocation")

    monkeypatch.setattr(ob, "_select_and_size", forbidden_allocator)
    rejected = _allocate(research, ["LATER.SR"])
    assert rejected["status"] == "board_funding_mismatch"
    assert "basis changed" in rejected.get("message", "")
    _assert_no_execution(rejected)


def test_same_release_snapshot_is_valid_across_worker_instance_contexts(monkeypatch):
    monkeypatch.setenv("RENDER_GIT_COMMIT", "synthetic-same-release")
    monkeypatch.setenv("RENDER_INSTANCE_ID", "synthetic-worker-a")
    research = _research([_row("LATER.SR", 20.0)])
    first = _allocate(research, ["LATER.SR"])
    monkeypatch.setenv("RENDER_INSTANCE_ID", "synthetic-worker-b")
    second = _allocate(research, ["LATER.SR"])
    assert first["status"] == second["status"] == "ok"
    assert first["selected"] == second["selected"]
    assert first["kpis"] == second["kpis"]


def test_missing_shared_authentication_key_leaves_research_unsized(monkeypatch):
    for key in ("APP_TOKEN", "TFB_APP_TOKEN", "BACKEND_TOKEN", "BACKUP_APP_TOKEN",
                "ALLOWED_TOKENS", "TFB_ALLOWED_TOKENS", "APP_TOKENS"):
        monkeypatch.delenv(key, raising=False)
    research = _research([_row("HIGH.SR")])
    funding = research["meta"]["board_funding"]
    assert funding["snapshot_available"] is False
    assert "key unavailable" in funding["reason"]
    assert not funding.get("snapshot")
    _assert_no_execution(research)


def test_snapshot_transport_is_byte_bounded_and_does_not_disclose_signing_key():
    research = _research([_row("HIGH.SR", name="x" * 1_000_001)])
    funding = research["meta"]["board_funding"]
    assert funding["snapshot_available"] is False
    assert "transport bound" in funding["reason"]
    assert not funding.get("snapshot")
    assert "synthetic-board-test" not in json.dumps(research)
    _assert_no_execution(research)


def test_allocation_replays_signed_price_verification_without_network_fetch(monkeypatch):
    monkeypatch.setenv("TFB_T10_PRICE_XCHECK", "enforce")
    monkeypatch.setenv("TFB_T10_PRICE_XCHECK_STRICT", "1")
    calls = []

    def _quote(symbol, _timeout):
        calls.append(symbol)
        return 100.0, "2026-10-07T00:00:00+00:00", 99.0

    monkeypatch.setattr(ob, "_XCHECK_FETCH_OVERRIDE", _quote)
    research = _research([_row("HIGH.SR"), _row("LATER.SR", 20.0)])
    assert calls == ["HIGH.SR", "LATER.SR"]
    assert research["meta"]["board_funding"]["snapshot"]["xchecks"][
        "LATER.SR"]["verdict"] == "verified"

    def _forbidden_fetch(*_args):
        raise AssertionError("the allocation pass must replay verified quotes")

    monkeypatch.setattr(ob, "_XCHECK_FETCH_OVERRIDE", _forbidden_fetch)
    allocated = _allocate(research, ["LATER.SR"])
    assert allocated["status"] == "ok", allocated.get("message")
    assert [t["symbol"] for t in allocated["selected"]] == ["LATER.SR"]
    assert allocated["selected"][0]["detail"]["price_xcheck"]["verdict"] == "verified"
    assert calls == ["HIGH.SR", "LATER.SR"]


def test_scan_uncapped_normalizes_same_effective_policy_in_both_passes(monkeypatch):
    monkeypatch.setenv("TFB_OPP_SCAN_UNCAPPED", "1")
    criteria = {"max_candidates": 1}
    research = _research([_row("HIGH.SR"), _row("LATER.SR", 20.0)], criteria=criteria)
    assert research["kpis"]["scanned"] == 2
    assert research["meta"]["criteria_snapshot"]["max_candidates"] == 0
    allocated = _allocate(research, ["LATER.SR"], criteria=criteria)
    assert allocated["status"] == "ok", allocated.get("message")
    assert allocated["meta"]["criteria_snapshot"]["max_candidates"] == 0
    assert [t["symbol"] for t in allocated["selected"]] == ["LATER.SR"]


def test_zero_cash_minimum_ticket_policy_exclusion_does_not_solicit_capital():
    portfolio = {
        "cash_available_sar": 0.0,
        "portfolio_value_sar": 100_000.0,
        "holdings": [{
            "symbol": "HELD.SR", "sector": "Energy", "market": "Tadawul",
            "value_sar": 30_000.0,
        }],
    }
    criteria = {"pf_max_sector_pct": 30.0}
    research = _research([_row("LATER.SR", 20.0)], criteria=criteria, portfolio=portfolio)
    assert research["kpis"]["passed"] == 1
    allocated = _allocate(research, ["LATER.SR"], criteria=criteria, portfolio=portfolio)
    assert allocated["status"] == "ok", allocated.get("message")
    assert allocated["selected"] == []
    _assert_no_execution(allocated)
    assert any(row["symbol"] == "LATER.SR" and row["failed_gate"] == "Diversification"
               for row in allocated["near_miss"])
    assert not any(row["failed_gate"] == "Funding" for row in allocated["near_miss"])


@pytest.mark.parametrize("state", ["error", "non_converged"])
@pytest.mark.parametrize("investability", ["INVESTABLE", "WATCHLIST", ""])
def test_failed_scoring_settlement_cannot_rank_or_fund_with_policy_gates_off(
    monkeypatch, state, investability,
):
    monkeypatch.setenv("TFB_SCORING_SETTLE", "off")
    monkeypatch.setenv("TFB_OPP_INVESTABILITY_GATE", "0")
    monkeypatch.setenv("TFB_T10_REL_FLOOR_MODE", "display")
    row = _row("FAILED.SR", warnings="provider_ok;f7_settle_failed:" + state,
               investability_status=investability)
    criteria = {"investability_gate_enabled": False, "sell_class_gate_enabled": False}
    ordinary = _build([row], criteria)
    assert ordinary["status"] == "ok", ordinary.get("message")
    assert ordinary["selected"] == []
    _assert_no_execution(ordinary)
    failed = ordinary["candidates_rows"][0]
    assert failed["verdict"] == "DO_NOT_INVEST"
    assert failed["opportunity_score"] is None
    assert failed["first_fail"]["gate"] == "Scoring Settlement"
    assert failed["first_fail"]["current"] == state
    research = _research([row], criteria)
    allocated = _allocate(research, ["FAILED.SR"], criteria=criteria)
    assert allocated["selected"] == []
    _assert_no_execution(allocated)


@pytest.mark.parametrize("failure_field", ["Warnings", "warning", "scoring_errors"])
def test_scoring_failure_cannot_be_hidden_by_an_earlier_clean_warning_alias(failure_field):
    row = _row("FAILED.SR", warnings="provider_ok")
    row[failure_field] = "f7_settle_failed:error"
    payload = _build([row], {"investability_gate_enabled": False})
    assert payload["selected"] == []
    _assert_no_execution(payload)
    assert payload["candidates_rows"][0]["first_fail"]["gate"] == "Scoring Settlement"


def test_settlement_observation_is_not_a_failure_marker():
    clean = _row("STABLE.SR")
    observed = {**clean, "warnings": "f7_settle:observe:st2:p2:chg1"}
    assert _build([observed])["selected"] == _build([clean])["selected"]


def test_failed_settlement_does_not_consume_capped_scan_capacity(monkeypatch):
    monkeypatch.setenv("TFB_OPP_PREGATE_ORDER", "1")
    failed = _row("FAILED.SR", warnings="f7_settle_failed:error",
                  forecast_reliability_score=99.0)
    clean = _row("CLEAN.SR")
    payload = _build([failed, clean], {"max_candidates": 1})
    assert [ticket["symbol"] for ticket in payload["selected"]] == ["CLEAN.SR"]
    assert payload["kpis"]["pregate"]["fail_scoring_settlement"] == 1
