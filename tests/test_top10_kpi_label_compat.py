"""Both read-only consumers retain displayed cash titles and unknown KPI states."""
import copy

import pytest

from scripts import send_digest as digest
from scripts import tfb_acceptance as acceptance


class Source(acceptance.Source):
    def __init__(self, rows):
        self.rows = rows

    def tab(self, page):
        assert page == "Top_10_Investments"
        return self.rows


def rows(label="Entered cash (SAR)", *, passed="7", cash="1234.50"):
    return [["Status:", "Last run fixture | status: ok | output: WITHHELD | SEAT-CHECK kpi 0 funded vs board 0 exec"],
            [], [label, "Selected", "Passed", "Fundable Now", "Scanned"],
            [cash, "0 / 10 (withheld)", passed, "0", "100"], [],
            ["FEED NOT ACTIONABLE — 0 EXECUTABLE TICKETS"]]


def summaries(grid):
    before = copy.deepcopy(grid)
    accepted = acceptance._top10(Source(grid))

    def read_range(sid, a1):
        assert sid == "synthetic"
        assert a1 == "'Top_10_Investments'!A1:L60"
        return grid

    summarized = digest._read_top10_summary(read_range, "synthetic")
    assert grid == before
    return accepted, summarized


@pytest.mark.parametrize("label", ["Deployable (SAR)", "Entered cash (SAR)"])
@pytest.mark.parametrize("passed,cash,verdict", [("7", "1234.50", "PASS"), ("0", "0", "FAIL")])
def test_old_and_current_titles_preserve_observed_nonzero_and_zero_kpis(label, passed, cash, verdict):
    grid = rows(label, passed=passed, cash=cash)
    for summary in summaries(grid):
        assert summary["kpis"] == {label: cash, "Selected": "0 / 10 (withheld)",
                                   "Passed": passed, "Fundable Now": "0", "Scanned": "100"}
        assert ({"Deployable (SAR)", "Entered cash (SAR)"} - {label}).isdisjoint(summary["kpis"])
    checks = {check.cid: check for check in acceptance.check_board(Source(grid), 5)}
    assert checks["D10-1"].verdict == verdict
    assert checks["D10-1"].measured == float(passed)
    assert checks["D10-1b"].verdict == "WARN" and checks["D10-1b"].measured == 0
    assert checks["D10-1c"].verdict == "PASS" and checks["D10-1c"].measured == 0


@pytest.mark.parametrize("missing", ["Selected", "Passed"])
def test_missing_required_header_is_unknown_instead_of_a_kpi_strip(missing):
    grid = rows()
    column = grid[2].index(missing)
    grid[2].pop(column)
    grid[3].pop(column)
    assert all(not result.get("kpis") for result in summaries(grid))
    assert acceptance.check_board(Source(grid), 5)[0].verdict == "NA"


@pytest.mark.parametrize("duplicate", ["Selected", "Passed", "Scanned", "Entered cash (SAR)", "Deployable (SAR)"])
def test_duplicated_or_two_cash_headers_do_not_choose_or_merge_values(duplicate):
    grid = rows()
    grid[2].append(duplicate)
    grid[3].append("999")
    assert all(not result.get("kpis") for result in summaries(grid))


@pytest.mark.parametrize("second_label", ["Entered cash (SAR)", "Deployable (SAR)"])
def test_repeated_old_or_current_blocks_are_unknown_even_with_identical_values(second_label):
    grid = rows()
    grid += rows(second_label)[2:4]
    assert all(not result.get("kpis") for result in summaries(grid))


def test_portfolio_cash_strip_does_not_replace_top10_kpis():
    grid = [["Deployable (SAR)", "Portfolio Value (SAR)", "Positions"], ["999", "888", "4"], []] + rows()
    for result in summaries(grid):
        assert result["kpis"]["Entered cash (SAR)"] == "1234.50"
        assert "Portfolio Value (SAR)" not in result["kpis"]


def test_portfolio_strip_alone_and_absent_value_row_are_unknown():
    for grid in ([["Deployable (SAR)", "Portfolio Value (SAR)"], ["999", "888"]],
                 [["Entered cash (SAR)", "Selected", "Passed"]]):
        assert all(not result.get("kpis") for result in summaries(grid))


@pytest.mark.parametrize("passed", [None, "", "unknown", "—", "1e999", "7%", "▲7", "▼7", "7.5", "-1", "7,5", False])
def test_missing_or_invalid_passed_is_na_while_execution_and_parity_evidence_survive(passed):
    grid = rows(passed=passed)
    checks = {check.cid: check for check in acceptance.check_board(Source(grid), 5)}
    assert checks["D10-1"].verdict == "NA" and checks["D10-1"].measured is None
    assert checks["D10-1"].na_kind == "parse"
    assert checks["D10-1b"].verdict == "WARN" and checks["D10-1b"].measured == 0
    assert checks["D10-1c"].verdict == "PASS" and checks["D10-1c"].measured == 0


def test_unreadable_kpi_block_keeps_independent_explicit_execution_funding_mismatch():
    grid = rows(passed="")
    grid[0][1] = grid[0][1].replace("kpi 0 funded vs board 0 exec", "kpi 2 funded vs board 0 exec")
    checks = {check.cid: check for check in acceptance.check_board(Source(grid), 5)}
    assert checks["D10-1"].verdict == "NA"
    assert checks["D10-1c"].verdict == "FAIL" and checks["D10-1c"].measured == 2


@pytest.mark.parametrize("passed,expected", [("7.0", 7), ("1,000", 1000), ("1,000.00", 1000), ("+7", 7)])
def test_legacy_formatted_whole_qualified_counts_remain_valid(passed, expected):
    checks = {check.cid: check for check in acceptance.check_board(Source(rows(passed=passed)), 5)}
    assert checks["D10-1"].verdict == "PASS" and checks["D10-1"].measured == expected


@pytest.mark.parametrize("funded", [None, "unknown", "—", "1%", "1.9", "-1", "1e999", "1,5", True])
def test_malformed_funded_kpi_without_seat_check_stays_unknown_instead_of_truncating_or_raising(funded):
    grid = rows()
    grid[0][1] = "Last run fixture | status: ok | output: EXECUTABLE"
    grid[3][3] = funded
    grid[-1] = ["SELECTED — 1 EXECUTABLE TICKET"]
    checks = {check.cid: check for check in acceptance.check_board(Source(grid), 5)}
    assert checks["D10-1"].verdict == "PASS"
    assert checks["D10-1b"].verdict == "PASS" and checks["D10-1b"].measured == 1
    assert checks["D10-1c"].verdict == "NA" and checks["D10-1c"].measured is None


def test_reordered_real_kpi_headers_and_short_value_row_retain_unknown_fields():
    grid = rows()
    grid[2] = ["Selected", "Scanned", "Entered cash (SAR)", "Passed", "Fundable Now"]
    grid[3] = ["0 / 10 (withheld)", "100", "1234.50"]
    for result in summaries(grid):
        assert result["kpis"]["Selected"] == "0 / 10 (withheld)"
        assert result["kpis"]["Entered cash (SAR)"] == "1234.50"
        assert result["kpis"]["Passed"] == "" and result["kpis"]["Fundable Now"] == ""
    assert acceptance.check_board(Source(grid), 5)[0].verdict == "NA"


def test_research_only_brief_preserves_entered_cash_label_without_funding_alias_or_picks(monkeypatch):
    def forbidden(*args, **kwargs):
        pytest.fail("No email transport is authorized in a state-brief test")

    monkeypatch.setattr(digest, "_send_email", forbidden)
    summary = summaries(rows())[1]
    subject, text, html = digest._compose_state_brief("fixture", "NOT_ACTIONABLE", "source", True,
        "fixture manifest", [], {"tfb decision feed": "NOT_ACTIONABLE(source)"}, summary)
    assert "NOT ACTIONABLE (source)" in subject
    assert "Entered cash (SAR)=1234.50" in text and "Selected=0 / 10 (withheld)" in text
    assert "Deployable (SAR)" not in text
    assert "Research only:" in text and "Picks follow below." not in text
    assert not any(token in text for token in ("Best BUY", "Best SELL", "Best SWAP", "SYNTH.US"))
    assert "Entered cash (SAR)=1234.50" in html
