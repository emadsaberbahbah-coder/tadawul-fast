from __future__ import annotations

import unittest
import copy
import asyncio
import json
from datetime import datetime, timedelta, timezone

import pytest

from scripts.audit_decision_surface_freshness import audit_surfaces
from scripts import audit_decision_surface_freshness as freshness

NOW = datetime(2026, 7, 31, 18, 0, 0, tzinfo=timezone.utc)  # 21:00 Riyadh
FLOORS = {
    "Market_Leaders": 1025,
    "Global_Markets": 6512,
    "Commodities_FX": 453,
    "Mutual_Funds": 4496,
}
HEADER = [
    "Page",
    "Last Updated",
    "Status",
    "Message",
    "Endpoint",
    "HTTP Code",
    "Rows",
    "Columns",
]


def status_grid(overrides=None):
    data = {
        "Market_Leaders": ["2026-07-31 20:00:00", "SUCCESS", "complete", 1356, 115],
        "Global_Markets": ["2026-07-31 20:00:00", "SUCCESS", "complete", 6512, 115],
        "Commodities_FX": ["2026-07-31 20:00:00", "SUCCESS", "complete", 453, 115],
        "Mutual_Funds": ["2026-07-31 20:00:00", "SUCCESS", "complete", 4496, 115],
        "My_Portfolio": ["2026-07-31 20:00:00", "VALID", "complete", 10, 122],
    }
    for key, value in (overrides or {}).items():
        data[key] = value
    rows = [HEADER]
    for page, (stamp, state, message, count, columns) in data.items():
        message += f" acquired={count}/{count} acquisition=COMPLETE data=COMPLETE fetchfail_requested=observe fetchfail_effective=observe"
        rows.append([page, stamp, state, message, "/read-only", 200, count, columns])
    return rows


def portfolio_grid(stamp="2026-07-31 20:05:00", state="ok"):
    return [
        ["MY PORTFOLIO — DECISION"],
        ["Status:", f"Last run {stamp} | status: {state} | holdings 10"],
    ]


def top10_grid(stamp="2026-07-31 20:10:00", state="ok", full=True):
    label = " (full universe)" if full else ""
    return [
        ["TOP 10 INVESTMENTS — DECISION"],
        [
            "Status:",
            (
                f"Last run {stamp} | status: {state} | sheets pool 11817 rows "
                "[Market_Leaders 1356/1356, Global_Markets 6512/6512, "
                "Commodities_FX 453/453, Mutual_Funds 4496/4496]"
                f"{label} | held=10 sent"
            ),
        ],
    ]


def publication_grids(run="90001"):
    """Synthetic producer receipts; no broker/custody authenticity claim."""
    sources, portfolio, top10 = status_grid(), portfolio_grid(), top10_grid()
    for row in sources[1:]:
        row[3] += f" run={run} sha=0123abcd"
    for surface in (portfolio, top10):
        surface[1][1] += f" | source readiness VERIFIED (run {run})"
    return sources, portfolio, top10


def strict_audit(grids):
    return audit_surfaces(*grids, now_utc=NOW, min_rows=FLOORS,
                          expected_source_run="90001")


def test_matching_publication_and_consumption_receipts_pass_strict_barrier():
    report = strict_audit(publication_grids())
    assert report.exit_code == 0 and report.executable and report.findings == []


@pytest.mark.parametrize("surface,code", [
    (1, "PF_SOURCE_CONSUMPTION_UNPROVEN"), (2, "T10_SOURCE_CONSUMPTION_UNPROVEN"),
])
@pytest.mark.parametrize("receipt", [
    "", " | run=90001 sha=0123abcd", " | source readiness VERIFIED (run 80000)",
    " | source readiness VERIFIED (run 90001) | source readiness VERIFIED (run 90001)",
    " | source readiness VERIFIED (run 90001) | source readiness VERIFIED (run 80000)",
    " | source readiness VERIFIED (run 90001) | source readiness WITHHELD: source stale",
    " | source readiness VERIFIED (run local)", " | source readiness VERIFIED (run 0)",
])
def test_recent_decision_timestamp_cannot_replace_unique_consumption_receipt(surface, code, receipt):
    grids = list(publication_grids())
    grids[surface][1][1] = grids[surface][1][1].split(" | source readiness")[0] + receipt
    report = strict_audit(grids)
    assert report.exit_code == 2 and not report.executable
    assert code in {finding.code for finding in report.findings}


@pytest.mark.parametrize("receipt", [
    "", " run=80000", " run=0", " run=local", " run=90001 run=90001",
    " run=90001 run=80000", " run=90001,run=80000",
])
def test_portfolio_requires_unique_current_source_publication(receipt):
    grids = list(publication_grids())
    source = next(row for row in grids[0][1:] if row[0] == "My_Portfolio")
    source[3] = source[3].split(" run=")[0] + receipt
    report = strict_audit(grids)
    assert report.exit_code == 2 and not report.executable
    codes = {finding.code for finding in report.findings}
    assert "PF_SOURCE_PUBLICATION_COHORT" in codes
    assert "PF_SOURCE_CONSUMPTION_UNPROVEN" in codes


@pytest.mark.parametrize("page,code", [
    ("My_Portfolio", "PF_SOURCE_PUBLICATION_COHORT"),
    ("Global_Markets", "SOURCE_PUBLICATION_COHORT"),
])
def test_repeated_status_rows_cannot_certify_one_source_run(page, code):
    grids = list(publication_grids())
    grids[0].append(copy.deepcopy(next(row for row in grids[0][1:] if row[0] == page)))
    report = strict_audit(grids)
    assert report.exit_code == 2 and not report.executable
    assert code in {finding.code for finding in report.findings}


@pytest.mark.parametrize("expected", ["", "local", "0", "90001 90001"])
def test_unknown_expected_run_is_explicitly_blocked(expected):
    report = audit_surfaces(*publication_grids(), now_utc=NOW, min_rows=FLOORS,
                            expected_source_run=expected)
    assert report.exit_code == 2 and not report.executable
    assert "EXPECTED_SOURCE_RUN_INVALID" in {finding.code for finding in report.findings}


def test_standalone_policy_does_not_require_new_producer_receipts():
    assert audit_surfaces(status_grid(), portfolio_grid(), top10_grid(), now_utc=NOW,
                          min_rows=FLOORS, expected_source_run=None).exit_code == 0


@pytest.fixture
def approved_fixture(tmp_path):
    """A synthetic typed approval exercises policy mechanics, never live scope."""
    from scripts.approved_universe_scope import (
        load_scope, membership_sha256, symbol_fingerprint, symbol_set_sha256,
    )
    symbols = {
        "Market_Leaders": [f"SYNTH_ML_{i:04d}.SR" for i in range(255)],
        "Mutual_Funds": [f"SYNTH_MF_{i:04d}" for i in range(2474)],
    }
    pages = {}
    for page, identities in symbols.items():
        fingerprints = sorted(symbol_fingerprint(identity) for identity in identities)
        pages[page] = {
            "scope": "saudi_only" if page == "Market_Leaders" else "current_fund_tab",
            "approved_count": len(identities),
            "supersedes_min_rows": FLOORS[page],
            "symbol_fingerprints_sha256": fingerprints,
            "membership_sha256": membership_sha256(fingerprints),
            "symbol_set_sha256": symbol_set_sha256(identities),
        }
    path = tmp_path / "synthetic_scope.json"
    path.write_text(json.dumps({
        "schema": "tfb.approved-universe-scope/v1",
        "policy_revision": "synthetic-test-v1",
        "approved_on": "2026-10-11",
        "approval_basis": "Synthetic test fixture; no live roster approval.",
        "pages": pages,
    }), encoding="utf-8")
    return load_scope(path), symbols


def approved_grids(symbols):
    sources, portfolio, top10 = publication_grids()
    for page, identities in symbols.items():
        row = next(row for row in sources[1:] if row[0] == page)
        count = len(identities)
        row[3] = (f"complete acquired={count}/{count} acquisition=COMPLETE "
                  "data=COMPLETE fetchfail_requested=observe fetchfail_effective=observe "
                  "run=90001 sha=0123abcd")
        row[6] = count
    top10[1][1] = (
        "Last run 2026-07-31 20:10:00 | status: ok | sheets pool "
        "[Market_Leaders 255/255, Global_Markets 6512/6512, "
        "Commodities_FX 453/453, Mutual_Funds 2474/2474] (full universe) "
        "| source readiness VERIFIED (run 90001)"
    )
    source_grids = {page: [["Symbol", "Name"]] + [[identity, "Synthetic"]
                    for identity in identities] for page, identities in symbols.items()}
    return (sources, portfolio, top10), source_grids


def approved_audit(approved_fixture, *, mutate=None):
    scope, symbols = approved_fixture
    grids, source_grids = approved_grids(symbols)
    if mutate:
        mutate(grids, source_grids)
    return audit_surfaces(*grids, now_utc=NOW, approved_scope=scope,
                          source_grids=source_grids, source_columns=source_columns_for(source_grids),
                          expected_source_run="90001")


def source_columns_for(source_grids):
    return {page: [[row[0]] if row else [] for row in grid]
            for page, grid in source_grids.items()}


def test_approved_physical_membership_supersedes_only_two_legacy_floors(approved_fixture, monkeypatch):
    monkeypatch.setenv("TFB_EXPECTED_MIN_ROWS_MARKET_LEADERS", "5000")
    monkeypatch.setenv("TFB_EXPECTED_MIN_ROWS_MUTUAL_FUNDS", "9999")
    def normal_case(_grids, source_grids):
        for grid in source_grids.values():
            for row in grid[1:]:
                row[0] = " " + row[0].lower() + " "
    report = approved_audit(approved_fixture, mutate=normal_case)
    assert report.exit_code == 0 and report.executable
    assert report.approved_scope["policy_revision"] == "synthetic-test-v1"
    from scripts.approved_universe_scope import receipt_matches_policy
    for page, approved_count, configured in (("Market_Leaders", 255, "5000"),
                                             ("Mutual_Funds", 2474, "9999")):
        receipt = report.source_status[page]["approved_scope"]
        assert receipt_matches_policy(receipt, policy=approved_fixture[0])
        assert receipt["actual_count"] == receipt["approved_count"] == approved_count
        assert receipt["applied_min_rows"] == approved_count
        assert receipt["supersedes_min_rows"] == FLOORS[page]
        assert receipt["superseded_configured_floor"]["value"] == configured
        assert receipt["superseded_configured_floor"]["legacy_effective_min_rows"] == int(configured)
        assert receipt["complete_column_proof"]["valid"] is True
        assert receipt["complete_column_proof"]["crossbound"] is True
        assert receipt["complete_column_proof"]["observed_symbol_set_sha256"] == receipt["observed_symbol_set_sha256"]
    assert report.payload()["approved_scope"] == report.approved_scope


@pytest.mark.parametrize("page", ["Market_Leaders", "Mutual_Funds"])
@pytest.mark.parametrize("fault", ["loss", "substitution", "extra", "duplicate", "invalid", "blank"])
def test_exact_source_membership_faults_fail_even_with_healthy_status_and_counts(approved_fixture, page, fault):
    def corrupt(_grids, source_grids):
        grid = source_grids[page]
        if fault == "loss":
            grid.pop()
        elif fault == "substitution":
            grid[-1][0] = "UNAPPROVED_SYNTH.SR"
        elif fault == "extra":
            grid.append(["UNAPPROVED_SYNTH.SR", "Synthetic"])
        elif fault == "duplicate":
            grid[-1][0] = grid[1][0]
        elif fault == "invalid":
            grid[-1][0] = 123456
        else:
            grid[-1][0] = ""
    report = approved_audit(approved_fixture, mutate=corrupt)
    assert report.exit_code == 2 and not report.executable
    expected_code = "SOURCE_APPROVED_MEMBERSHIP_UNPROVEN" if fault == "blank" else "SOURCE_APPROVED_MEMBERSHIP"
    assert expected_code in {finding.code for finding in report.findings}
    receipt = report.source_status[page]["approved_scope"]
    assert receipt["valid"] is False
    assert receipt["actual_membership_sha256"] != receipt["expected_membership_sha256"] or receipt["duplicate_count"] or receipt["bad_symbol_count"]
    assert "FALSE_FULL_UNIVERSE_CLAIM" in {finding.code for finding in report.findings}


@pytest.mark.parametrize("fault", ["missing_grid", "missing_header", "duplicate_header", "repeated_header"])
def test_physical_symbol_evidence_must_be_present_and_unambiguous(approved_fixture, fault):
    def corrupt(_grids, source_grids):
        page = "Market_Leaders"
        if fault == "missing_grid":
            source_grids.pop(page)
        elif fault == "missing_header":
            source_grids[page][0][0] = "Ticker"
        elif fault == "duplicate_header":
            source_grids[page][0] = ["Symbol", "Symbol"]
        else:
            source_grids[page].insert(1, ["Symbol", "Name"])
    report = approved_audit(approved_fixture, mutate=corrupt)
    assert report.exit_code == 2 and not report.executable
    assert "SOURCE_APPROVED_MEMBERSHIP_UNPROVEN" in {finding.code for finding in report.findings}


@pytest.mark.parametrize("field,code", [
    ("rows", "SOURCE_APPROVED_ROW_COUNT"),
    ("acquired", "SOURCE_APPROVED_ACQUISITION_DENOMINATOR"),
    ("pool", "T10_POOL_APPROVED_COUNT"),
])
def test_legacy_metadata_cannot_describe_an_approved_current_roster(approved_fixture, field, code):
    def stale_metadata(grids, _source_grids):
        row = next(row for row in grids[0][1:] if row[0] == "Market_Leaders")
        if field == "rows":
            row[6] = 1025
        elif field == "acquired":
            row[3] = row[3].replace("acquired=255/255", "acquired=1025/1025")
        else:
            grids[2][1][1] = grids[2][1][1].replace("Market_Leaders 255/255", "Market_Leaders 1025/1025")
    report = approved_audit(approved_fixture, mutate=stale_metadata)
    assert report.exit_code == 2 and not report.executable
    assert code in {finding.code for finding in report.findings}


def test_strict_scope_cannot_lower_global_or_commodities_floors(approved_fixture):
    scope, symbols = approved_fixture
    grids, source_grids = approved_grids(symbols)
    for page in ("Global_Markets", "Commodities_FX"):
        row = next(row for row in grids[0][1:] if row[0] == page)
        row[6] = 1
    report = audit_surfaces(*grids, now_utc=NOW, approved_scope=scope,
                           source_grids=source_grids, source_columns=source_columns_for(source_grids),
                           min_rows={page: 1 for page in FLOORS})
    assert report.exit_code == 2 and not report.executable
    assert {finding.code for finding in report.findings} >= {"SOURCE_ROW_FLOOR"}
    assert report.source_status["Market_Leaders"]["approved_scope"]["applied_min_rows"] == 255


@pytest.mark.parametrize("fault,code", [
    ("market_age", "SOURCE_STALE"), ("decision_age", "PF_RUN_STALE"),
    ("market_acquisition", "SOURCE_ACQUISITION_INCOMPLETE"),
    ("portfolio_acquisition", "PF_SOURCE_ACQUISITION_INCOMPLETE"),
])
def test_strict_scope_preserves_age_and_acquisition_limits(approved_fixture, fault, code):
    scope, symbols = approved_fixture
    grids, source_grids = approved_grids(symbols)
    if fault == "market_age":
        source = next(row for row in grids[0][1:] if row[0] == "Market_Leaders")
        source[1] = "2026-07-30 13:00:00"  # 32 hours before NOW in Riyadh
    elif fault == "decision_age":
        grids[1][1][1] = grids[1][1][1].replace("2026-07-31 20:05:00", "2026-07-31 11:00:00")
    else:
        page = "Market_Leaders" if fault == "market_acquisition" else "My_Portfolio"
        source = next(row for row in grids[0][1:] if row[0] == page)
        source[3] = (source[3].replace("acquired=255/255", "acquired=240/255")
                     if page == "Market_Leaders" else source[3].replace("acquired=10/10", "acquired=9/10"))
    report = audit_surfaces(*grids, now_utc=NOW, approved_scope=scope,
                           source_grids=source_grids, source_columns=source_columns_for(source_grids),
                           market_max_age_h=9999,
                           decision_max_age_h=9999, min_fresh_percent=0)
    assert report.exit_code == 2 and not report.executable
    assert code in {finding.code for finding in report.findings}


def test_default_live_scope_failure_is_closed_before_sheet_reads(monkeypatch):
    def missing_approval():
        raise ValueError("approval file missing or malformed")
    monkeypatch.setattr(freshness, "_load_approved_scope", missing_approval)
    reads = []
    report = asyncio.run(freshness.run_live("synthetic-sheet", reader=lambda sid, a1: reads.append(a1)))
    assert report.exit_code == 3 and not report.executable
    assert "approved universe scope unavailable" in report.fatal
    assert reads == []


def test_strict_membership_requires_independent_complete_column_evidence(approved_fixture):
    scope, symbols = approved_fixture
    grids, source_grids = approved_grids(symbols)
    report = audit_surfaces(*grids, now_utc=NOW, approved_scope=scope, source_grids=source_grids)
    assert report.exit_code == 2 and not report.executable
    assert "SOURCE_APPROVED_MEMBERSHIP_UNPROVEN" in {finding.code for finding in report.findings}


@pytest.mark.parametrize("tail", [["UNAPPROVED_BEYOND_BOUND.SR"], []])
def test_full_column_extent_beyond_bound_fails_before_membership_validation(approved_fixture, monkeypatch, tail):
    from scripts import approved_universe_scope as scope_module
    scope, symbols = approved_fixture
    grids, source_grids = approved_grids(symbols)
    source_columns = source_columns_for(source_grids)
    column = source_columns["Market_Leaders"]
    column.extend([[] for _ in range(20050 - len(column))])
    column.append(tail)
    validations = []
    actual_validator = scope_module.validate_symbols
    def record_validator(page, values, **kwargs):
        validations.append(page)
        return actual_validator(page, values, **kwargs)
    monkeypatch.setattr(scope_module, "validate_symbols", record_validator)
    report = audit_surfaces(*grids, now_utc=NOW, approved_scope=scope,
                           source_grids=source_grids, source_columns=source_columns)
    assert report.exit_code == 2 and not report.executable
    assert "Market_Leaders" not in validations
    receipt = report.source_status["Market_Leaders"]["approved_scope"]
    assert receipt["complete_column_proof"]["returned_rows"] == 20051
    assert receipt["complete_column_proof"]["valid"] is False
    assert "extends beyond row 20050" in receipt["error"]


def test_sparse_approved_member_at_last_bounded_row_is_completely_verified(approved_fixture):
    scope, symbols = approved_fixture
    grids, source_grids = approved_grids(symbols)
    wide = source_grids["Market_Leaders"]
    final_member = wide.pop()
    wide.extend([[] for _ in range(20049 - len(wide))])
    wide.append(final_member)
    source_columns = source_columns_for(source_grids)
    source_columns["Market_Leaders"][-1][0] = " " + final_member[0].lower() + " "
    report = audit_surfaces(*grids, now_utc=NOW, approved_scope=scope,
                           source_grids=source_grids, source_columns=source_columns)
    assert report.exit_code == 0 and report.executable
    receipt = report.source_status["Market_Leaders"]["approved_scope"]
    proof = receipt["complete_column_proof"]
    assert proof["returned_rows"] == proof["bound"] == 20050
    assert proof["header_row"] == 1 and proof["symbol_column"] == 1
    assert proof["valid"] is True and proof["crossbound"] is True
    assert proof["bounded_membership_sha256"] == proof["actual_membership_sha256"] == receipt["actual_membership_sha256"]
    assert proof["bounded_symbol_set_sha256"] == proof["observed_symbol_set_sha256"] == receipt["observed_symbol_set_sha256"]


@pytest.mark.parametrize("change", ["reorder", "substitution", "header_row", "symbol_column"])
def test_independent_column_and_wide_snapshots_must_match_physical_rows(approved_fixture, change):
    scope, symbols = approved_fixture
    grids, source_grids = approved_grids(symbols)
    source_columns = source_columns_for(source_grids)
    column = source_columns["Market_Leaders"]
    if change == "reorder":
        column[1], column[2] = column[2], column[1]  # exact same membership hash
    elif change == "substitution":
        column[-1][0] = "SYNTH_CONCURRENT_SUBSTITUTION.SR"
    elif change == "header_row":
        column.insert(0, [])
    else:
        source_grids["Market_Leaders"] = [list(reversed(row)) for row in source_grids["Market_Leaders"]]
    report = audit_surfaces(*grids, now_utc=NOW, approved_scope=scope,
                           source_grids=source_grids, source_columns=source_columns)
    assert report.exit_code == 2 and not report.executable
    assert "SOURCE_APPROVED_MEMBERSHIP_UNPROVEN" in {finding.code for finding in report.findings}
    proof = report.source_status["Market_Leaders"]["approved_scope"]["complete_column_proof"]
    assert proof["valid"] is False and proof["crossbound"] is False


def test_live_complete_column_get_failure_remains_closed(approved_fixture):
    scope, symbols = approved_fixture
    grids, source_grids = approved_grids(symbols)
    responses = {
        "_Status!A1:J100": grids[0], "Portfolio_Decision!A1:B3": grids[1],
        "Top_10_Investments!A1:B3": grids[2],
        **{f"{page}!A1:EZ20050": grid for page, grid in source_grids.items()},
    }
    def read(_sid, a1):
        if a1.endswith("!A:A"):
            raise RuntimeError("complete column GET unavailable")
        return responses[a1]
    report = asyncio.run(freshness.run_live("synthetic-sheet", reader=read, approved_scope=scope))
    assert report.exit_code == 3 and not report.executable
    assert "complete column GET unavailable" in report.fatal


def test_default_live_audit_reads_actual_source_columns_and_checks_fingerprints(approved_fixture, monkeypatch):
    scope, symbols = approved_fixture
    grids, source_grids = approved_grids(symbols)
    now_riyadh = datetime.now(timezone.utc).astimezone(timezone(timedelta(hours=3)))
    source_stamp = (now_riyadh - timedelta(minutes=30)).strftime("%Y-%m-%d %H:%M:%S")
    surface_stamp = (now_riyadh - timedelta(minutes=15)).strftime("%Y-%m-%d %H:%M:%S")
    for row in grids[0][1:]:
        row[1] = source_stamp
    for surface in grids[1:]:
        surface[1][1] = surface[1][1].replace("2026-07-31 20:05:00", surface_stamp).replace("2026-07-31 20:10:00", surface_stamp)
    responses = {
        "_Status!A1:J100": grids[0], "Portfolio_Decision!A1:B3": grids[1],
        "Top_10_Investments!A1:B3": grids[2],
        **{f"{page}!A1:EZ20050": grid for page, grid in source_grids.items()},
        **{f"{page}!A:A": grid for page, grid in source_columns_for(source_grids).items()},
    }
    reads = []
    def read(_sid, a1):
        reads.append(a1)
        return responses[a1]
    monkeypatch.setattr(freshness, "_load_approved_scope", lambda: scope)
    report = asyncio.run(freshness.run_live("synthetic-sheet", reader=read,
                                           expected_source_run="90001"))
    assert report.exit_code == 0 and report.executable
    assert set(reads) == set(responses)
    responses["Market_Leaders!A1:EZ20050"][-1][0] = "SYNTH_SUBSTITUTED.SR"
    responses["Market_Leaders!A:A"][-1][0] = "SYNTH_SUBSTITUTED.SR"
    report = asyncio.run(freshness.run_live("synthetic-sheet", reader=read,
                                           expected_source_run="90001"))
    assert report.exit_code == 2 and not report.executable
    assert "SOURCE_APPROVED_MEMBERSHIP" in {finding.code for finding in report.findings}


class DecisionSurfaceFreshnessTests(unittest.TestCase):
    def test_clean_synchronized_surfaces_pass(self):
        result = audit_surfaces(
            status_grid(),
            portfolio_grid(),
            top10_grid(),
            now_utc=NOW,
            min_rows=FLOORS,
        )
        self.assertEqual(result.exit_code, 0)
        self.assertTrue(result.executable)
        self.assertEqual(result.findings, [])

    def test_portfolio_decision_older_than_portfolio_source_fails(self):
        result = audit_surfaces(
            status_grid(),
            portfolio_grid("2026-07-31 19:00:00"),
            top10_grid(),
            now_utc=NOW,
            min_rows=FLOORS,
        )
        codes = {item.code for item in result.findings}
        self.assertIn("PF_OLDER_THAN_SOURCE", codes)
        self.assertEqual(result.exit_code, 2)
        self.assertFalse(result.executable)

    def test_full_universe_claim_fails_on_partial_stale_and_short_sources(self):
        grid = status_grid(
            {
                "Global_Markets": [
                    "2026-07-31 17:30:00",
                    "PARTIAL",
                    "Batch refresh paused at 840 of 6199",
                    6199,
                    115,
                ],
                "Mutual_Funds": [
                    "2026-07-22 11:00:00",
                    "ROW_REFRESH_OK",
                    "Selected row refreshed",
                    1,
                    115,
                ],
                "Commodities_FX": [
                    "2026-07-14 11:00:00",
                    "SUCCESS",
                    "Batch refresh completed",
                    251,
                    115,
                ],
            }
        )
        result = audit_surfaces(
            grid,
            portfolio_grid(),
            top10_grid(),
            now_utc=NOW,
            min_rows=FLOORS,
        )
        codes = {item.code for item in result.findings}
        self.assertIn("SOURCE_NOT_COMPLETE", codes)
        self.assertIn("SOURCE_STALE", codes)
        self.assertIn("SOURCE_ROW_FLOOR", codes)
        self.assertIn("FALSE_FULL_UNIVERSE_CLAIM", codes)
        self.assertEqual(result.exit_code, 2)

    def test_unparseable_status_table_is_fatal(self):
        result = audit_surfaces(
            [["wrong", "headers"]],
            portfolio_grid(),
            top10_grid(),
            now_utc=NOW,
            min_rows=FLOORS,
        )
        self.assertEqual(result.exit_code, 3)
        self.assertIn("could not be parsed", result.fatal)


if __name__ == "__main__":
    unittest.main()
