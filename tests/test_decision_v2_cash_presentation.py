"""Offline consumer regressions; all amounts and account state are synthetic."""
from __future__ import annotations

import datetime as dt
import importlib.util
import os
import sys

import pytest

openpyxl = pytest.importorskip("openpyxl")

if os.environ.get("TFB_EXPORT_AUDIT_SOURCE"):
    spec = importlib.util.spec_from_file_location("audit_cash_under_test", os.environ["TFB_EXPORT_AUDIT_SOURCE"])
    audit = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = audit
    spec.loader.exec_module(audit)
else:
    from scripts import tfb_export_audit as audit

NOW = dt.datetime(2026, 10, 24, 9, tzinfo=dt.timezone.utc)


def _book(tmp_path, *, badge="HELD", output="HELD", count=0, kpi="9,500 SAR",
          narrative="deployable 9,000 SAR stays Tranche 0", reserve="500", floor="1,000",
          portfolio=True, top10=True):
    wb = openpyxl.Workbook()
    wb.active.title = "Portfolio_Decision"
    wb.active.append(["UNCERTIFIED"])
    wb.active.append(["Status:", "research only"])
    if portfolio:
        ws = wb.create_sheet("Portfolio_Decision_v2")
        ws.append(["PORTFOLIO — DECISION v2", "UNCERTIFIED"])
        ws.append([])
        ws.append(["NAV", "CASH", "DEPLOYABLE"])
        ws.append(["10,000 SAR", "10,000 SAR", kpi])
        ws.append([f"engine reserve {reserve} SAR (enforce) · PF 10% policy floor {floor}"])
        ws.append([narrative])
    if top10:
        ws = wb.create_sheet("Top_10_Investments_v2")
        ws.append(["TOP 10 INVESTMENTS — DECISION v2", badge])
        ws.append(["Blocker: none"])
        ws.append([f"Status: status: ok | output: {output}"])
        ws.append([])
        ws.append(["SCANNED", "ENGINE INVESTABLE", "EXECUTABLE"])
        ws.append([100, 10, count])
    path = tmp_path / "decision-view.xlsx"
    wb.save(path)
    return audit.Book(path)


def _portfolio(book):
    findings, metrics = audit.check_portfolio(book, 3, 7, NOW)
    return {finding["check"]: finding for finding in findings}, metrics


def _cockpit(book):
    findings, metrics = audit.check_cockpit(book, 3, (2, 30))
    return {finding["check"]: finding for finding in findings}, metrics


def test_distinct_engine_and_pf_reserve_bases_are_both_valid(tmp_path):
    findings, metrics = _portfolio(_book(tmp_path))
    assert findings["portfolio_v2_research_cash_arithmetic"]["status"] == "PASS"
    view = metrics["portfolio_v2_research_cash"]
    assert view["engine_post_reserve_sar"] == 9500
    assert view["pf_post_floor_sar"] == 9000
    assert view["narrative_deployable_sar"] == [9000]
    assert view["scope"] == "display_arithmetic_only"
    assert view["execution_clearance"] is False
    assert metrics["cash_certified"] is False
    assert metrics["cash_sar"] is None


@pytest.mark.parametrize("narrative", ["deployable 9,500 SAR research only", "deployable 9,000 SAR stays Tranche 0"])
def test_either_explicit_reserve_basis_is_mathematically_valid(tmp_path, narrative):
    findings, _ = _portfolio(_book(tmp_path, narrative=narrative))
    assert findings["portfolio_v2_research_cash_arithmetic"]["status"] == "PASS"


@pytest.mark.parametrize("changes", [
    {"narrative": "deployable 9,300 SAR stays Tranche 0"},
    {"kpi": "9,300 SAR"},
    {"floor": "1,300"},
    {"reserve": "-500"},
    {"kpi": "NaN SAR"},
    {"kpi": "-1 SAR"},
])
def test_invalid_rendered_arithmetic_fails_without_certifying_cash(tmp_path, changes):
    findings, metrics = _portfolio(_book(tmp_path, **changes))
    assert findings["portfolio_v2_research_cash_arithmetic"]["status"] == "FAIL"
    assert metrics["cash_certified"] is False
    assert metrics["cash_sar"] is None


def test_unavailable_or_conflicting_basis_is_not_passed(tmp_path):
    book = _book(tmp_path)
    rows = book.sheets["Portfolio_Decision_v2"]
    rows[4] = ("DP2_ENGINE_ENV is unavailable",)
    findings, _ = _portfolio(book)
    assert findings["portfolio_v2_research_cash_arithmetic"]["status"] == "NOT_CHECKED"
    rows[4] = ("engine reserve 500 SAR · engine reserve 600 SAR · PF 10% policy floor 1,000",)
    findings, _ = _portfolio(book)
    assert findings["portfolio_v2_research_cash_arithmetic"]["status"] == "NOT_CHECKED"


def test_rendered_rounding_is_allowed_without_funding_tolerance(tmp_path):
    findings, _ = _portfolio(_book(tmp_path, kpi="9,500.50 SAR", narrative="deployable 9,000.25 SAR research"))
    assert findings["portfolio_v2_research_cash_arithmetic"]["status"] == "PASS"


@pytest.mark.parametrize("output,count", [("HELD", 0), ("WITHHELD", 0), ("EXECUTABLE", 0), ("HELD", 2)])
def test_executable_badge_on_zero_or_held_board_fails_presentation_only(tmp_path, output, count):
    findings, metrics = _cockpit(_book(tmp_path, badge="EXECUTABLE", output=output, count=count))
    finding = findings["top10_v2_execution_presentation"]
    assert finding["status"] == "FAIL"
    assert "presentation only" in finding["detail"]
    assert metrics["top10_v2_presentation"]["scope"] == "presentation_only"
    assert "execution_clearance" not in metrics["top10_v2_presentation"]


@pytest.mark.parametrize("badge,output,count", [
    ("BOARD: HELD", "HELD", 2),
    ("HELD", "EXECUTABLE", 2),
    ("FEED VERIFIED", "HELD", 2),
    ("FEED VERIFIED", "EXECUTABLE", 0),
    ("BOARD: NO_TRADE", "EMPTY", 0),
])
def test_nonexecutable_badges_cannot_hide_output_or_ticket_count_contradictions(tmp_path, badge, output, count):
    findings, _ = _cockpit(_book(tmp_path, badge=badge, output=output, count=count))
    assert findings["top10_v2_execution_presentation"]["status"] == "FAIL"


@pytest.mark.parametrize("badge,output,count", [("HELD", "HELD", 0), ("EXECUTABLE", "EXECUTABLE", 2),
                                                ("FEED VERIFIED", "HELD", 0), ("BOARD: HELD", "HELD", 0),
                                                ("BOARD:EXECUTABLE", "EXECUTABLE", 2)])
def test_coherent_badges_and_separate_feed_readiness_pass(tmp_path, badge, output, count):
    findings, _ = _cockpit(_book(tmp_path, badge=badge, output=output, count=count))
    assert findings["top10_v2_execution_presentation"]["status"] == "PASS"


@pytest.mark.parametrize("count", [None, "unknown", -1, 0.5])
def test_missing_or_invalid_ticket_count_is_unknown(tmp_path, count):
    findings, _ = _cockpit(_book(tmp_path, count=count))
    assert findings["top10_v2_execution_presentation"]["status"] == "NOT_CHECKED"


def test_native_uncertified_portfolio_is_not_an_executable_badge_failure(tmp_path):
    book = _book(tmp_path, portfolio=False, top10=False)
    findings, _ = _portfolio(book)
    cockpit, _ = _cockpit(book)
    assert "portfolio_v2_research_cash_arithmetic" not in findings
    assert "top10_v2_execution_presentation" not in cockpit


def test_duplicate_v2_kpi_bands_do_not_pick_a_convenient_amount(tmp_path):
    book = _book(tmp_path)
    rows = book.sheets["Portfolio_Decision_v2"]
    rows.extend([rows[2], rows[3]])
    findings, _ = _portfolio(book)
    assert findings["portfolio_v2_research_cash_arithmetic"]["status"] == "NOT_CHECKED"


def test_explicit_engine_budget_header_accepts_new_basis_label(tmp_path):
    book = _book(tmp_path, narrative="cash after the PF 10% NAV floor: 9,000 SAR stays Tranche 0")
    rows = book.sheets["Portfolio_Decision_v2"]
    rows[2] = ("NAV", "CASH", "ENGINE BUDGET")
    findings, metrics = _portfolio(book)
    assert findings["portfolio_v2_research_cash_arithmetic"]["status"] == "PASS"
    assert metrics["portfolio_v2_research_cash"]["execution_clearance"] is False


@pytest.mark.parametrize("separator", [":", " is"])
def test_explicit_pf_floor_narrative_cannot_silently_use_engine_basis(tmp_path, separator):
    book = _book(tmp_path, narrative=f"cash after the PF 10% NAV floor{separator} 9,500 SAR stays Tranche 0")
    findings, _ = _portfolio(book)
    assert findings["portfolio_v2_research_cash_arithmetic"]["status"] == "FAIL"


@pytest.mark.parametrize("badge", ["arbitrary clearance", "BOARD: UNKNOWN", "BOARD: VALID"])
def test_unknown_badges_do_not_pass_the_presentation_check(tmp_path, badge):
    findings, _ = _cockpit(_book(tmp_path, badge=badge))
    assert findings["top10_v2_execution_presentation"]["status"] == "NOT_CHECKED"
