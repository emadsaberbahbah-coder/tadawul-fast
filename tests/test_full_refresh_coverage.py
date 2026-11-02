from __future__ import annotations

import unittest
import asyncio
import copy
import json
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import pytest

from scripts.audit_full_refresh_coverage import (
    Rule, audit_grid, ledger_symbols, parse_dt, parse_dt_precision,
)
from scripts import audit_full_refresh_coverage as coverage
from scripts.approved_universe_scope import (
    SCHEMA, ScopePolicyError, load_scope, membership_sha256, symbol_fingerprint, symbol_set_sha256,
)

NOW = datetime(2026, 7, 30, 8, 0, 0, tzinfo=timezone.utc)
HEADERS = ["Symbol", "Name", "Current Price", "Last Updated (UTC)", "Position Qty", "Avg Cost", "Data Provider"]


def scoped_symbols():
    """Synthetic current memberships retain the policy's full cardinalities."""
    return {"Market_Leaders": [f"{900001+i}.SR" for i in range(255)],
            "Mutual_Funds": [f"SYNTHFUND{i:04d}.US" for i in range(2474)]}


def synthetic_approved_scope(tmp_path):
    pages = {}
    for page, symbols in scoped_symbols().items():
        fingerprints = sorted(symbol_fingerprint(symbol) for symbol in symbols)
        pages[page] = {
            "scope": "saudi_only" if page == "Market_Leaders" else "current_fund_tab",
            "approved_count": len(symbols),
            "supersedes_min_rows": 1025 if page == "Market_Leaders" else 4496,
            "symbol_fingerprints_sha256": fingerprints,
            "membership_sha256": membership_sha256(fingerprints),
            "symbol_set_sha256": symbol_set_sha256(symbols),
        }
    path = tmp_path / "synthetic-approved-scope.json"
    path.write_text(json.dumps({"schema": SCHEMA, "policy_revision": "synthetic-current-v1",
        "approved_on": "2026-10-12", "approval_basis": "Synthetic test membership only",
        "pages": pages}), encoding="utf-8")
    return load_scope(path)


def scoped_grid(page, *, now=NOW):
    return [HEADERS, *[[symbol, "Synthetic instrument", 50, now.isoformat(), "", "", "eodhd"]
                      for symbol in scoped_symbols()[page]]]


def symbol_column(grid):
    return [copy.deepcopy(row[:1]) for row in grid]


@pytest.mark.parametrize("page", ["Market_Leaders", "Mutual_Funds"])
def test_approved_current_membership_supersedes_only_declared_legacy_floor(tmp_path, monkeypatch, page):
    scope = synthetic_approved_scope(tmp_path)
    legacy = scope.pages[page].supersedes_min_rows
    setting = "TFB_EXPECTED_MIN_ROWS_" + page.upper()
    monkeypatch.setenv(setting, str(legacy + 100))
    result = audit_grid(scoped_grid(page), Rule(page, legacy, 30, 95, 99, 95),
                        HEADERS, NOW, approved_scope=scope, source_column=symbol_column(scoped_grid(page)))
    assert result.status == "PASS" and result.min_rows == len(scoped_symbols()[page])
    receipt = result.approved_scope
    assert receipt["valid"] and receipt["supersedes_min_rows"] == legacy
    assert receipt["superseded_configured_floor"] == {
        "environment": setting, "value": str(legacy + 100), "legacy_effective_min_rows": legacy + 100}
    assert receipt["actual_membership_sha256"] == receipt["expected_membership_sha256"]


@pytest.mark.parametrize("page", ["Market_Leaders", "Mutual_Funds"])
@pytest.mark.parametrize("fault", ["substitution", "loss", "extra", "duplicate", "bare_alias", "blank", "numeric"])
def test_approved_membership_rejects_physical_identity_changes(tmp_path, page, fault):
    scope = synthetic_approved_scope(tmp_path)
    grid = scoped_grid(page)
    if fault == "loss":
        grid.pop()
    elif fault == "extra":
        grid.append(["UNAPPROVED.US", "Synthetic", 50, NOW.isoformat(), "", "", "eodhd"])
    elif fault == "duplicate":
        grid[-1][0] = grid[1][0]
    elif fault == "bare_alias":
        grid[1][0] = grid[1][0].split(".")[0]
    else:
        grid[1][0] = {"substitution": "UNAPPROVED.US", "blank": "", "numeric": 900001}[fault]
    result = audit_grid(grid, Rule(page, 1, 30, 95, 99, 95), HEADERS, NOW, approved_scope=scope,
                        source_column=symbol_column(grid))
    assert result.status == "FAIL" and result.approved_scope["valid"] is False
    assert any("approved universe membership failed" in failure for failure in result.failures)


def test_default_live_audit_loads_current_policy_and_records_membership_receipts(tmp_path, monkeypatch):
    scope = synthetic_approved_scope(tmp_path)
    monkeypatch.setattr(coverage, "load_scope", lambda: scope)
    monkeypatch.setenv("TFB_EXPECTED_MIN_ROWS_MARKET_LEADERS", "1025")
    monkeypatch.setenv("TFB_EXPECTED_MIN_ROWS_MUTUAL_FUNDS", "4496")
    monkeypatch.setenv("TFB_EXPECTED_MIN_ROWS_GLOBAL_MARKETS", "1")
    monkeypatch.setenv("TFB_EXPECTED_MIN_ROWS_COMMODITIES_FX", "1")
    rules = {rule.page: rule for rule in coverage.rules()}
    assert {page: rule.min_rows for page, rule in rules.items() if page in scope.pages} == {
        "Market_Leaders": 255, "Mutual_Funds": 2474}
    assert rules["Global_Markets"].min_rows == 6512 and rules["Commodities_FX"].min_rows == 453
    assert {page: (rules[page].min_name, rules[page].min_price) for page in
            ("Market_Leaders", "Global_Markets", "Commodities_FX", "Mutual_Funds")} == {
        "Market_Leaders": (99, 95), "Global_Markets": (99, 95),
        "Commodities_FX": (95, 95), "Mutual_Funds": (99, 90),
    }
    ledger = [["Symbol", "Status", "Shares"], ["PF.SR", "Active", 1]]
    grids = {"_Portfolio_CostBasis": ledger,
             "Market_Leaders": scoped_grid("Market_Leaders"), "Mutual_Funds": scoped_grid("Mutual_Funds")}
    for page, count in (("Global_Markets", 6512), ("Commodities_FX", 453), ("My_Portfolio", 1)):
        grids[page] = [HEADERS, *[["PF.SR" if page == "My_Portfolio" else f"{page[0]}{i:05d}.US",
            "Synthetic", 50, NOW.isoformat(), 1, 40, "eodhd"] for i in range(count)]]
    from core.sheets.schema_registry import get_sheet_headers
    insights_headers = get_sheet_headers("Insights_Analysis")
    grids["Insights_Analysis"] = [insights_headers, ["Overview", "Sources", "count", 0, "", "", 1]]
    grids["Data_Dictionary"] = [HEADERS, ["synthetic"]]
    reads = []
    def reader(_sid, a1):
        reads.append(a1)
        grid = grids[a1.split("!")[0]]
        return symbol_column(grid) if a1.endswith("!A:A") else copy.deepcopy(grid)
    report = asyncio.run(coverage.run("synthetic-sheet", reader=reader,
        registry=SimpleNamespace(SCHEMA_VERSION="synthetic", get_sheet_headers=lambda page:
            insights_headers if page == "Insights_Analysis" else HEADERS), now=NOW))
    assert report.code == 0, [(page.page, page.failures) for page in report.pages]
    assert report.approved_scope == scope.receipt()
    assert all(result.approved_scope["valid"] for result in report.pages if result.page in scope.pages)
    assert len(reads) == 10
    assert {page + "!A:A" for page in scope.pages}.issubset(reads)
    assert all(result.approved_scope["complete_column_proof"]["valid"]
               for result in report.pages if result.page in scope.pages)


@pytest.mark.parametrize("page", ["Market_Leaders", "Mutual_Funds"])
@pytest.mark.parametrize("fault", ["sparse_tail", "extra_inside_bound", "reordered", "relocated", "missing", "malformed"])
def test_full_symbol_column_and_bounded_input_must_agree(tmp_path, page, fault):
    scope = synthetic_approved_scope(tmp_path)
    grid = scoped_grid(page)
    column = symbol_column(grid)
    if fault == "sparse_tail":
        column.extend([[] for _ in range(20050 - len(column))])
        column.append(["UNAPPROVED.SR"])
    elif fault == "extra_inside_bound":
        column.extend([[], [], ["UNAPPROVED.SR"]])
    elif fault == "reordered":
        column[1], column[2] = column[2], column[1]
    elif fault == "relocated":
        original = column[1]
        column[1] = []
        column.append(original)
    elif fault == "missing":
        column = None
    else:
        column[0].append("unrequested second column")
    result = audit_grid(grid, Rule(page, 1, 30, 95, 99, 95), HEADERS, NOW,
                        approved_scope=scope, source_column=column)
    assert result.status == "FAIL"
    proof = result.approved_scope["complete_column_proof"]
    assert proof["valid"] is False and proof["crossbound"] is False
    assert result.approved_scope["valid"] is False
    assert any("complete approved universe membership unproven" in failure for failure in result.failures)
    if fault == "sparse_tail":
        assert proof["returned_rows"] == 20051 and "exceeds" in proof["error"]
    elif fault in {"reordered", "relocated"}:
        assert result.approved_scope["actual_membership_sha256"] == result.approved_scope["expected_membership_sha256"]
        assert "changed between reads" in proof["error"]


def test_sparse_approved_symbol_at_last_audited_row_has_complete_evidence(tmp_path):
    scope = synthetic_approved_scope(tmp_path)
    grid = scoped_grid("Market_Leaders")
    last = grid.pop()
    grid.extend([[] for _ in range(20049 - len(grid))])
    grid.append(last)
    result = audit_grid(grid, Rule("Market_Leaders", 255, 30, 95, 99, 95), HEADERS, NOW,
                        approved_scope=scope, source_column=symbol_column(grid))
    assert result.status == "PASS"
    proof = result.approved_scope["complete_column_proof"]
    assert proof["valid"] is True and proof["crossbound"] is True and proof["returned_rows"] == 20050


def test_actual_live_complete_column_read_failure_cannot_certify_membership(tmp_path):
    scope = synthetic_approved_scope(tmp_path)
    reads = []
    def reader(_sid, a1):
        reads.append(a1)
        if a1 == "_Portfolio_CostBasis!A1:EZ20050":
            return [["Symbol", "Status", "Shares"], ["PF.SR", "Active", 1]]
        if a1 == "Market_Leaders!A1:EZ20050":
            return scoped_grid("Market_Leaders")
        if a1 == "Market_Leaders!A:A":
            raise RuntimeError("synthetic full-column read failure")
        pytest.fail("unexpected read " + a1)
    report = asyncio.run(coverage.run("synthetic-sheet", reader=reader,
        registry=SimpleNamespace(get_sheet_headers=lambda _page: HEADERS), now=NOW,
        policy_rules=[Rule("Market_Leaders", 255, 30, 95, 99, 95)], approved_scope=scope))
    assert report.code == 2 and report.pages[0].status == "FAIL"
    assert "full-column read failure" in report.pages[0].failures[0]
    assert "Market_Leaders!A:A" in reads and not report.pages[0].approved_scope


@pytest.mark.parametrize("error", ["missing", "invalid"])
def test_missing_or_invalid_default_policy_blocks_before_reading(monkeypatch, error):
    def bad_policy():
        raise ScopePolicyError("Synthetic " + error + " approval")
    monkeypatch.setattr(coverage, "load_scope", bad_policy)
    def reader(*_args):
        pytest.fail("a missing approval must block before any Sheet read")
    report = asyncio.run(coverage.run("synthetic-sheet", reader=reader,
        registry=SimpleNamespace(get_sheet_headers=lambda _page: HEADERS), now=NOW))
    assert report.code == 3 and "approved universe policy unavailable" in report.fatal


class FullRefreshCoverageTests(unittest.TestCase):
    def test_clean_market_page_passes(self) -> None:
        grid = [HEADERS, ["AAA", "Alpha", 10, "2026-07-30 07:30:00", "", "", "eodhd"], ["BBB", "Beta", 20, "2026-07-30 07:00:00", "", "", "eodhd"]]
        result = audit_grid(grid, Rule("Market_Leaders", 2, 30, 100, 100, 100), HEADERS, NOW)
        self.assertEqual(result.status, "PASS")
        self.assertEqual(result.rows, 2)
        self.assertEqual(result.fresh_pct, 100.0)

    def test_duplicate_and_stale_page_fails(self) -> None:
        grid = [HEADERS, ["AAA", "Alpha", 10, "2026-07-20 07:30:00", "", "", "eodhd"], ["AAA", "Alpha", 10, "2026-07-30 07:00:00", "", "", "eodhd"]]
        result = audit_grid(grid, Rule("Global_Markets", 2, 30, 95, 100, 100), HEADERS, NOW)
        self.assertEqual(result.status, "FAIL")
        self.assertEqual(result.duplicates, ["AAA"])
        self.assertLess(result.fresh_pct or 0, 95)

    def test_portfolio_requires_all_active_ledger_symbols(self) -> None:
        grid = [HEADERS, ["AAA", "Alpha", 10, "2026-07-30 07:30:00", 5, 8, "eodhd"]]
        result = audit_grid(grid, Rule("My_Portfolio", 1, 8, 100, 100, 100, portfolio=True), HEADERS, NOW, active=["AAA", "BBB"])
        self.assertEqual(result.status, "FAIL")
        self.assertEqual(result.missing_portfolio, ["BBB"])

    def test_ledger_reader_uses_active_positive_lots(self) -> None:
        grid = [["Portfolio Ledger"], [], [], ["Symbol", "Status", "Shares"], ["AAA", "Active", 5], ["BBB", "Inactive", 10], ["CCC", "Active", 0]]
        active, warnings = ledger_symbols(grid)
        self.assertEqual(active, ["AAA"])
        self.assertEqual(warnings, [])

    def test_parse_google_serial_and_iso(self) -> None:
        self.assertEqual(parse_dt(46200.5), datetime(1899, 12, 30) + timedelta(days=46200.5))
        # P-104: output is always naive Riyadh, including UTC Z inputs.
        self.assertEqual(parse_dt("2026-07-30T07:30:00Z"), datetime(2026, 7, 30, 10, 30))

    def test_same_instant_has_one_riyadh_representation(self) -> None:
        expected = datetime(2026, 7, 30, 10, 30)
        for value in (
            "2026-07-30T07:30:00Z",
            "2026-07-30T07:30:00+00:00",
            "2026-07-30T10:30:00+03:00",
            datetime(2026, 7, 30, 7, 30, tzinfo=timezone.utc),
            "2026-07-30 10:30:00",
        ):
            with self.subTest(value=value):
                self.assertEqual(parse_dt_precision(value), (expected, "datetime"))

    def test_date_only_does_not_claim_intraday_precision(self) -> None:
        self.assertEqual(parse_dt_precision("2026-07-30")[1], "date")
        self.assertEqual(parse_dt_precision(46200.0)[1], "date")
        self.assertEqual(parse_dt_precision(46200.5)[1], "datetime")
        self.assertEqual(parse_dt_precision("not-a-date"), (None, "none"))

    def test_utc_freshness_age_is_not_shifted_three_hours(self) -> None:
        for header in ("Last Updated (UTC)", "Last Updated (Riyadh)"):
            headers = ["Symbol", "Name", "Current Price", header, "Data Provider"]
            grid = [headers, ["AAA", "Alpha", 10, "2026-07-30T07:30:00Z", "eodhd"]]
            with self.subTest(header=header):
                result = audit_grid(grid, Rule("Global_Markets", 1, 1, 100, 100, 100), headers, NOW)
                self.assertEqual(result.status, "PASS")
                self.assertEqual(result.oldest_age_h, 0.5)


if __name__ == "__main__":
    unittest.main()
