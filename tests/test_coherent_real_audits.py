"""Coherent display capture against actual coverage and freshness audits.

The scope and narrow schema are synthetic, but retain every current configured
source cardinality. No audit verdict, membership receipt or publication result
is fabricated. The real auditors read the exact frozen grids being archived.
"""
import copy
import datetime as dt
from functools import partial
from types import SimpleNamespace

import pytest

from core.sheets.schema_registry import get_sheet_headers
from scripts import archive_coherent_snapshot as co
from scripts import finalize_decision_publication as final
from tests.test_archive_coherent_snapshot import Session, SOURCE, SID, provenance
from tests.test_final_decision_publication import (
    complete_source_grids,
    current_publication_grids,
)
from tests.test_full_refresh_coverage import HEADERS, synthetic_approved_scope


@pytest.fixture
def real_cohort(tmp_path, monkeypatch):
    scope = synthetic_approved_scope(tmp_path)
    monkeypatch.setattr(final, "load_scope", lambda: scope)
    # Eliminate unrelated operator overrides; retain the documented full
    # current floors and let the source auditor construct the actual policy.
    for name in list(co.os.environ):
        if name.startswith(("TFB_EXPECTED_MIN_ROWS_", "TFB_REFRESH_")) or name in {
            "TFB_DECISION_SOURCE_MAX_AGE_H", "TFB_DECISION_SURFACE_MAX_AGE_H"
        }:
            monkeypatch.delenv(name, raising=False)
    now = dt.datetime.now(dt.timezone.utc)
    source_at = now - dt.timedelta(minutes=10)
    decision_at = now - dt.timedelta(minutes=5)
    riyadh = dt.timezone(dt.timedelta(hours=3))
    source_stamp = source_at.astimezone(riyadh).strftime("%Y-%m-%d %H:%M:%S")
    decision_stamp = decision_at.astimezone(riyadh).strftime("%Y-%m-%d %H:%M:%S")
    statuses, portfolio, top10 = current_publication_grids()
    for row in statuses[1:]:
        row[1] = source_stamp
    for surface in (portfolio, top10):
        surface[1][1] = surface[1][1].replace(
            "2026-07-31 20:05:00", decision_stamp
        ).replace("2026-07-31 20:10:00", decision_stamp)
    grids = {tab: [["auxiliary snapshot"]] for tab in co.CAPTURE_TABS}
    grids.update({"_Status": statuses, "Portfolio_Decision": portfolio,
                  "Top_10_Investments": top10})
    for page, physical in complete_source_grids().items():
        grids[page] = [HEADERS, *[
            [row[0] if page in scope.pages else f"{page[0]}{index:05d}.US",
             "Synthetic instrument", 50, source_at.isoformat(), "", "", "eodhd"]
            for index, row in enumerate(physical[1:])
        ]]
    holdings = [f"PF{i:02d}.SR" for i in range(10)]
    grids["_Portfolio_CostBasis"] = [
        ["Symbol", "Status", "Shares"], *[[symbol, "Active", 1] for symbol in holdings]
    ]
    grids["My_Portfolio"] = [HEADERS, *[
        [symbol, "Synthetic holding", 50, source_at.isoformat(), 1, 40, "eodhd"]
        for symbol in holdings
    ]]
    insight_headers = get_sheet_headers("Insights_Analysis")
    grids["Insights_Analysis"] = [
        insight_headers, ["Overview", "Sources", "count", 0, "", "", 1]
    ]
    grids["Data_Dictionary"] = [HEADERS, ["Synthetic dictionary row"]]
    registry = SimpleNamespace(
        SCHEMA_VERSION="synthetic-current-source-contract",
        get_sheet_headers=lambda page: insight_headers if page == "Insights_Analysis" else HEADERS,
    )
    session = Session(grids)
    reports = []
    actual_auditor = partial(co.audit_capture, registry=registry)

    async def audit(*args, **kwargs):
        # Both source contracts run for real, separately from trading release.
        # Keep the evidence so tests assert which real invariant failed.
        result = await actual_auditor(*args, **kwargs)
        reports.append(result)
        return result

    def capture():
        return co.capture(session, SID, SOURCE, provenance=lambda _: provenance(),
                          inflight=lambda: [], audit=audit, now=now)

    return session, reports, capture


def test_real_source_contracts_accept_one_complete_frozen_current_cohort(real_cohort):
    session, reports, capture = real_cohort
    rows, receipt = capture()
    assert reports[0]["audit_exit_codes"] == [0, 0]
    assert receipt["capture_contract"]["accepted"] is True
    assert receipt["capture_contract"]["trading_permission_granted"] is False
    coverage = reports[0]["audits"]["coverage"]
    assert {page["page"]: page["rows"] for page in coverage["pages"]} == {
        "Market_Leaders": 255, "Global_Markets": 6512,
        "Commodities_FX": 453, "Mutual_Funds": 2474,
        "My_Portfolio": 10, "Insights_Analysis": 1, "Data_Dictionary": 1,
    }
    assert coverage["active_holdings"] == [f"PF{i:02d}.SR" for i in range(10)]
    for page in coverage["pages"]:
        if page["page"] in {"Market_Leaders", "Mutual_Funds"}:
            assert page["approved_scope"]["complete_column_proof"]["crossbound"] is True
    decision = reports[0]["audits"]["decision"]
    assert decision["top10_pool_reconciliation"]["valid"] is True
    assert decision["findings"] == []
    assert receipt["audit_exit_codes"] == [0, 0] and receipt["feed_promoted"] is False
    archived = next(row for row in rows if row["tab"] == "My_Portfolio" and row["row_num"] == 2)
    assert archived["cells"][2] == "50"
    assert all(method == "GET" for method, *_ in session.calls)


def test_real_freshness_rejects_recent_timestamp_with_wrong_consumed_source(real_cohort):
    session, reports, capture = real_cohort
    session.grids["Portfolio_Decision"][1][1] = session.grids["Portfolio_Decision"][1][1].replace(
        "(run 90001)", "(run 80000)"
    )
    with pytest.raises(co.Refused, match="captured cohort withheld"):
        capture()
    assert reports[0]["audit_exit_codes"] == [0, 2]
    assert "PF_SOURCE_CONSUMPTION_UNPROVEN" in {
        finding["code"] for finding in reports[0]["audits"]["decision"]["findings"]
    }
    assert not session.jobs and all(method == "GET" for method, *_ in session.calls)


@pytest.mark.parametrize("reason", ["NO_NEW_ENTRY", "LEARNING_PENDING"])
def test_real_coherent_business_withholding_is_archived_without_trade_permission(real_cohort, reason):
    session, reports, capture = real_cohort
    session.grids["Top_10_Investments"][1][1] += (
        " | output: WITHHELD | Selected=0 exec | " + reason
    )
    exact_status = session.grids["Top_10_Investments"][1][1]
    rows, receipt = capture()
    assert reports[0]["audit_exit_codes"] == [0, 0]
    contract = receipt["capture_contract"]
    assert contract["accepted"] is True
    assert contract["display"]["executable_count"] == 0
    assert reason in contract["display"]["business_flags"]
    assert contract["trading_permission_granted"] is False
    persisted, current = co.publish(session, "proj-x", "tfb_archive", "US", rows, receipt)
    assert current is True and persisted["capture_contract"] == contract
    archived = next(row for row in session.production["today"]
                    if row["tab"] == "Top_10_Investments" and row["row_num"] == 2)
    assert archived["cells"][1] == exact_status
    assert receipt["feed_promoted"] is False


def two_published_holdings_with_six_consumed(session):
    """Keep the actual source and ledger coherent; only its consumer is wrong."""
    session.grids["My_Portfolio"] = session.grids["My_Portfolio"][:3]
    session.grids["_Portfolio_CostBasis"] = session.grids["_Portfolio_CostBasis"][:3]
    row = next(row for row in session.grids["_Status"][1:] if row[0] == "My_Portfolio")
    row[3] = row[3].replace("acquired=10/10", "acquired=2/2")
    row[6] = 2
    session.grids["Portfolio_Decision"][1][1] = (
        session.grids["Portfolio_Decision"][1][1].replace("holdings 10", "holdings 6")
    )


def test_real_native_consumed_count_withholding_preserves_last_coherent_data(real_cohort):
    session, reports, capture = real_cohort
    two_published_holdings_with_six_consumed(session)
    session.grids["Portfolio_Decision"][1][1] = session.grids["Portfolio_Decision"][1][1].replace(
        "source readiness VERIFIED (run 90001)",
        "source readiness WITHHELD: consumed 6 holdings vs 2 published rows",
    )
    session.grids["Top_10_Investments"][1][1] += " | output: WITHHELD | Selected=0 exec"
    before = copy.deepcopy(session.production)
    with pytest.raises(co.Refused, match="captured cohort withheld") as caught:
        capture()
    assert reports[0]["audit_exit_codes"] == [0, 2]
    assert {"SOURCE_CONSUMPTION_UNPROVEN", "PORTFOLIO_CONSUMED_COUNT_MISMATCH"}.issubset(
        caught.value.details["reason_codes"]
    )
    assert caught.value.details["display"]["consumed_holdings"] == 6
    assert caught.value.details["display"]["published_holdings"] == 2
    assert session.production == before and not session.jobs


def test_forged_verified_count_mismatch_is_rejected_even_when_legacy_audits_pass(real_cohort):
    session, reports, capture = real_cohort
    two_published_holdings_with_six_consumed(session)
    before = copy.deepcopy(session.production)
    with pytest.raises(co.Refused, match="PORTFOLIO_CONSUMED_COUNT_MISMATCH") as caught:
        capture()
    # This is the separate account-count invariant the legacy freshness
    # auditor does not enforce: timestamps and matching run IDs are not enough.
    assert reports[0]["audit_exit_codes"] == [0, 0]
    assert caught.value.details["accepted"] is False
    assert session.production == before and not session.jobs


def test_display_admission_does_not_consult_legacy_executable_boolean(real_cohort, monkeypatch):
    session, reports, capture = real_cohort
    actual_run_live = final.freshness.run_live

    async def legacy_flag_false(*args, **kwargs):
        report = await actual_run_live(*args, **kwargs)
        assert report.exit_code == 0 and report.findings == []
        report.executable = False
        return report

    monkeypatch.setattr(final.freshness, "run_live", legacy_flag_false)
    rows, receipt = capture()
    assert reports[0]["audits"]["decision"]["executable"] is False
    assert receipt["capture_contract"]["accepted"] is True
    assert receipt["capture_contract"]["trading_permission_granted"] is False
    assert receipt["feed_promoted"] is False and rows
    assert all(method == "GET" for method, *_ in session.calls)


def test_real_coverage_rejects_missing_holding_price_before_publication(real_cohort):
    session, reports, capture = real_cohort
    session.grids["My_Portfolio"][1][2] = None
    with pytest.raises(co.Refused, match="captured cohort withheld"):
        capture()
    assert reports[0]["audit_exit_codes"] == [2, 0]
    page = next(page for page in reports[0]["audits"]["coverage"]["pages"]
                if page["page"] == "My_Portfolio")
    assert page["price_pct"] == 90
    assert page["status"] == "FAIL"
    assert not session.jobs and all(method == "GET" for method, *_ in session.calls)


def test_real_audits_reject_a_sparse_symbol_beyond_the_wide_read_bound(real_cohort):
    session, reports, capture = real_cohort
    grid = session.grids["Market_Leaders"]
    grid.extend([[] for _ in range(20050 - len(grid))])
    grid.append(["UNAPPROVED.SR", "Unexpected tail", 50])
    before = copy.deepcopy(grid)
    with pytest.raises(co.Refused, match="captured cohort withheld"):
        capture()
    assert reports[0]["audit_exit_codes"] == [2, 2]
    page = next(page for page in reports[0]["audits"]["coverage"]["pages"]
                if page["page"] == "Market_Leaders")
    assert page["approved_scope"]["complete_column_proof"]["valid"] is False
    assert session.grids["Market_Leaders"] == before
    assert not session.jobs and all(method == "GET" for method, *_ in session.calls)
