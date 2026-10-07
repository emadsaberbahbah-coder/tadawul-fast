"""Optional operator labels repair display gaps after provider identity guards.

Uses synthetic ledger values and the existing real producer/122-column runner
fixtures. A ledger label never changes acquisition or executable eligibility.
"""
import copy
import logging

import pytest

from scripts import critical_symbol_identity as identity
from scripts import run_dashboard_sync as sync
from tests.test_portfolio_active_targets import (
    HEADERS, HOLDINGS, NOW, SYMBOLS, Writer, acquisition_census, assert_money,
    cell, full_audit, ledger, quote_row, rows_by_symbol, run_portfolio,
)


LABELS = {
    "SYNA.US": "Synthetic Alpha Operator Label",
    "SYNB.US": "Synthetic Beta Operator Label",
    "7010.SR": "Synthetic Riyal Operator Label",
}
SOURCE = "name_source:portfolio_ledger"
NAME_I = HEADERS.index("Name")
WARN_I = HEADERS.index("Warnings")


def labeled_ledger():
    grid = ledger()
    for row in grid[4:]:
        symbol = str(row[0]).strip().upper()
        if row[3] == "Active" and symbol in LABELS:
            row[1] = "  " + LABELS[symbol] + "  "
    return grid


def nameless_quote(symbol, *, blank="", **kwargs):
    row = quote_row(symbol, **kwargs)
    row[NAME_I] = blank
    return row


def money_basis(basis):
    return {symbol: {key: hold[key] for key in ("qty", "cost", "currency")}
        for symbol, hold in basis.items()}


def tokens(row):
    return [token.strip() for token in cell(row, "Warnings").split(";") if token.strip()]


def test_optional_active_labels_follow_row_four_header_and_ignore_inactive_duplicate():
    grid = labeled_ledger()
    grid[0] = ["Name", "Symbol"]  # Metadata does not establish the ledger header.
    inactive = copy.deepcopy(grid[4])
    inactive[1], inactive[3] = "Misleading inactive operator label", "Inactive"
    grid.insert(4, inactive)
    grid.append(copy.deepcopy(inactive))
    writer = Writer(grid)
    basis = sync._read_cost_basis(writer, "offline")
    assert money_basis(basis) == HOLDINGS
    assert {symbol: hold["name"] for symbol, hold in basis.items()} == LABELS
    assert writer.ledger_reads == [("A1:EZ20050",)]


@pytest.mark.parametrize("name_header", ["Name", "Company Name", "Company",
    "Long Name", "Short Name", "Security Name"])
def test_single_existing_name_alias_carries_optional_label(name_header):
    grid = labeled_ledger()
    grid[3][1] = name_header
    basis = sync._read_cost_basis(Writer(grid), "offline")
    assert money_basis(basis) == HOLDINGS
    assert {symbol: hold["name"] for symbol, hold in basis.items()} == LABELS


@pytest.mark.parametrize("bad_name", [None, "", "   ", 123, 123.45, True, False,
    [], ["Human label"], {"label": "Human label"}, "Unknown", "N/A", "null", "--",
    "SYNA.US", "syna", "My_Portfolio SYNA.US", "Global_Markets SYNA.US",
    "my_portfolio SYNA.US", "123.45", "1e6"])
def test_invalid_optional_label_keeps_money_basis_but_name_coverage_fails(bad_name, monkeypatch):
    monkeypatch.setenv("TFB_SYNC_FETCHFAIL_TRUTH", "observe")
    grid = labeled_ledger()
    grid[4][1] = bad_name
    basis = sync._read_cost_basis(Writer(grid), "offline")
    assert money_basis(basis) == HOLDINGS and "name" not in basis["SYNA.US"]
    result, writer, payloads = run_portfolio(monkeypatch,
        [nameless_quote(symbol) for symbol in SYMBOLS], grid=grid)
    assert payloads[0]["symbols"] == SYMBOLS and result.rows_written == 3
    output = rows_by_symbol(writer.published)
    assert cell(output["SYNA.US"], "Name") in (None, "")
    assert SOURCE not in tokens(output["SYNA.US"])
    assert_money(output["SYNA.US"], "SYNA.US")
    assert sync._page_fresh_fetch_metrics(result) == (3, 3, 100.0)
    audit = full_audit(writer.published)
    assert audit.fresh == 3 and audit.name_pct == pytest.approx(200 / 3)
    assert audit.status == "FAIL"
    assert any("name coverage" in failure for failure in audit.failures)


@pytest.mark.parametrize("optional_header", ["Name", "Company Name"])
@pytest.mark.parametrize("reverse_alias_order", [False, True])
def test_ambiguous_optional_name_columns_omit_labels_without_blocking_money(
        optional_header, reverse_alias_order, monkeypatch):
    monkeypatch.setenv("TFB_SYNC_FETCHFAIL_TRUTH", "observe")
    grid = labeled_ledger()
    grid[3].append(optional_header)
    for row in grid[4:]:
        row.append("Conflicting second operator label")
    if reverse_alias_order:
        grid[3][1], grid[3][-1] = grid[3][-1], grid[3][1]
        for row in grid[4:]:
            row[1], row[-1] = row[-1], row[1]
    basis = sync._read_cost_basis(Writer(grid), "offline")
    assert money_basis(basis) == HOLDINGS
    assert all("name" not in hold for hold in basis.values())
    result, writer, payloads = run_portfolio(monkeypatch,
        [nameless_quote(symbol) for symbol in SYMBOLS], grid=grid)
    assert payloads and result.rows_written == 3
    assert all(cell(row, "Name") in (None, "") and SOURCE not in tokens(row)
        for row in writer.published)
    audit = full_audit(writer.published)
    assert audit.name_pct == 0 and audit.fresh == 3 and audit.status == "FAIL"


@pytest.mark.parametrize("missing_kind", ["column", "cell"])
def test_missing_optional_name_never_invents_or_blocks_valid_holdings(missing_kind, monkeypatch):
    monkeypatch.setenv("TFB_SYNC_FETCHFAIL_TRUTH", "observe")
    grid = labeled_ledger()
    if missing_kind == "column":
        grid[3][1] = "Operator memo"
    else:
        grid[3].append(grid[3].pop(1))
        for row in grid[4:]:
            value = row.pop(1)
            if str(row[0]).strip().upper() != "SYNA.US":
                row.append(value)
    basis = sync._read_cost_basis(Writer(grid), "offline")
    assert money_basis(basis) == HOLDINGS and "name" not in basis["SYNA.US"]
    result, writer, payloads = run_portfolio(monkeypatch,
        [nameless_quote(symbol) for symbol in SYMBOLS], grid=grid)
    assert payloads and result.rows_written == 3
    assert cell(rows_by_symbol(writer.published)["SYNA.US"], "Name") in (None, "")
    assert full_audit(writer.published).status == "FAIL"


@pytest.mark.parametrize("blank", ["", None, "   "])
def test_initial_injection_cannot_supply_provider_identity_final_fill_is_idempotent(blank):
    basis = sync._read_cost_basis(Writer(labeled_ledger()), "offline")
    incoming = [nameless_quote(symbol, blank=blank) for symbol in SYMBOLS]
    original = copy.deepcopy(incoming)
    initial, count = sync._inject_portfolio_holdings(HEADERS, incoming, basis)
    assert count == 3 and incoming == original
    assert all(cell(row, "Name") == blank and SOURCE not in tokens(row) for row in initial)
    final, count = sync._inject_portfolio_holdings(HEADERS, initial, basis, include_ledger_name=True)
    assert count == 3
    for before, after in zip(initial, final):
        symbol = cell(after, "Symbol")
        assert cell(after, "Name") == LABELS[symbol]
        assert tokens(after) == tokens(before) + [SOURCE]
        for i, header in enumerate(HEADERS):
            if header not in {"Name", "Warnings"}:
                assert after[i] == before[i]
    repeated, _ = sync._inject_portfolio_holdings(HEADERS, final, basis, include_ledger_name=True)
    assert repeated == final
    assert all(tokens(row).count(SOURCE) == 1 for row in repeated)


@pytest.mark.parametrize("mode", ["off", "observe", "enforce"])
def test_real_122_runner_repairs_display_name_gap_after_guards_and_full_audit_passes(
        mode, monkeypatch, caplog):
    monkeypatch.setenv("TFB_SYNC_FETCHFAIL_TRUTH", mode)
    caplog.set_level(logging.INFO, logger=sync.logger.name)
    rows = [nameless_quote(symbol) for symbol in SYMBOLS]
    original = copy.deepcopy(rows)
    identity_names = []
    originating_names = []
    validate = sync.validate_fresh_critical_rows
    record = sync._record_acquisition_census

    def identity_probe(headers, matrix, requested):
        identity_names.append([row[headers.index("Name")] for row in matrix])
        return validate(headers, matrix, requested)

    def acquisition_probe(result, headers, matrix, requested, **kwargs):
        if kwargs.get("origins") is None:
            originating_names.append([row[headers.index("Name")] for row in matrix])
        return record(result, headers, matrix, requested, **kwargs)

    monkeypatch.setattr(sync, "validate_fresh_critical_rows", identity_probe)
    monkeypatch.setattr(sync, "_record_acquisition_census", acquisition_probe)
    result, writer, payloads = run_portfolio(monkeypatch, rows, grid=labeled_ledger())
    assert payloads[0]["symbols"] == SYMBOLS and result.rows_written == 3
    assert writer.ledger_reads == [("A1:EZ20050",)] and rows == original
    assert identity_names and originating_names
    assert all(not name for batch in identity_names + originating_names for name in batch)
    for row in writer.published:
        symbol = cell(row, "Symbol")
        assert cell(row, "Name") == LABELS[symbol]
        assert SOURCE in tokens(row) and tokens(row).count(SOURCE) == 1
        assert_money(row, symbol)
        before = rows_by_symbol(original)[symbol]
        for name in ("Current Price", "Currency", "Data Provider", "Last Updated (UTC)",
                "Investor Decision", "User Notes", "Recommendation", "Buy Date", "Target Weight %"):
            assert cell(row, name) == cell(before, name)
        assert tokens(row) == tokens(before) + [SOURCE]
    assert sync._page_fresh_fetch_metrics(result) == (3, 3, 100.0)
    assert "acquired=3/3 acquisition=COMPLETE" in writer.status_rows[-1][3]
    sync._apply_stale_skip_escalation([result], writer, "offline")
    assert "fresh_rows=3 requested_rows=3 fresh_pct=100.0000" in caplog.text
    audit = full_audit(writer.published)
    assert audit.status == "PASS" and audit.name_pct == 100 and audit.fresh == 3


def test_real_provider_names_are_never_replaced_by_operator_labels(monkeypatch):
    monkeypatch.setenv("TFB_SYNC_FETCHFAIL_TRUTH", "observe")
    rows = [quote_row(symbol) for symbol in SYMBOLS]
    result, writer, _ = run_portfolio(monkeypatch, rows, grid=labeled_ledger())
    assert result.rows_written == 3
    for output, original in zip(writer.published, rows):
        assert cell(output, "Name") == cell(original, "Name")
        assert tokens(output) == tokens(original) and SOURCE not in tokens(output)
    assert full_audit(writer.published).status == "PASS"


def test_label_uses_same_frozen_ledger_snapshot_when_backend_observes_later_edit(monkeypatch):
    monkeypatch.setenv("TFB_SYNC_FETCHFAIL_TRUTH", "observe")
    rows = [nameless_quote(symbol) for symbol in SYMBOLS]

    def response(_payload, writer):
        writer.ledger[4][1] = "Different later operator label"
        return rows

    result, writer, payloads = run_portfolio(monkeypatch,
        response_factory=response, grid=labeled_ledger())
    assert result.rows_written == 3 and payloads[0]["symbols"] == SYMBOLS
    assert writer.ledger_reads == [("A1:EZ20050",)]
    assert cell(rows_by_symbol(writer.published)["SYNA.US"], "Name") == LABELS["SYNA.US"]
    assert full_audit(writer.published).status == "PASS"


def test_actual_restored_blank_name_gets_label_without_promoting_acquisition(monkeypatch):
    monkeypatch.setenv("TFB_SYNC_FETCHFAIL_TRUTH", "observe")
    restored_symbol = "SYNA.US"
    prior = nameless_quote(restored_symbol, stamp="2026-10-07T11:00:00Z",
        qty=1, cost=1, note="Synthetic preserved operator note")
    original = copy.deepcopy(prior)
    result, writer, payloads = run_portfolio(monkeypatch,
        [nameless_quote(symbol) for symbol in SYMBOLS if symbol != restored_symbol],
        grid=labeled_ledger(), prior=[prior], persistence=True)
    assert payloads and result.rows_written == 3 and result._stamp_meta["persist_restored"] == 1
    restored = rows_by_symbol(writer.published)[restored_symbol]
    assert cell(restored, "Name") == LABELS[restored_symbol] and SOURCE in tokens(restored)
    assert "acquisition_status:preserved" in tokens(restored)
    assert tokens(restored).count(SOURCE) == 1
    assert cell(restored, "Last Updated (UTC)") == cell(original, "Last Updated (UTC)")
    assert cell(restored, "User Notes") == cell(original, "User Notes") and prior == original
    assert_money(restored, restored_symbol)
    assert sync._page_fresh_fetch_metrics(result) == (2, 3, pytest.approx(200 / 3))
    assert "acquired=2/3 acquisition=PARTIAL" in writer.status_rows[-1][3]
    census = acquisition_census(HEADERS, writer.published, now=NOW, max_age_seconds=8 * 3600,
        requested=SYMBOLS, symbol_key=sync.canonicalize_symbol)
    assert census.successful == set(SYMBOLS) - {restored_symbol}
    audit = full_audit(writer.published)
    assert audit.name_pct == 100 and audit.fresh == 2 and audit.status == "FAIL"


def test_ledger_label_cannot_satisfy_actual_critical_provider_identity_guard(monkeypatch):
    monkeypatch.setenv("TFB_SYNC_FETCHFAIL_TRUTH", "observe")
    rules = dict(identity.CRITICAL_IDENTITIES)
    rules["SYNA.US"] = identity.IdentityRule(accepted_name_tokens=(LABELS["SYNA.US"].lower(),))
    monkeypatch.setattr(identity, "CRITICAL_IDENTITIES", rules)
    monkeypatch.setattr(identity, "CRITICAL_FETCH_SYMBOLS", frozenset(rules))
    result, writer, payloads = run_portfolio(monkeypatch,
        [nameless_quote(symbol) for symbol in SYMBOLS], grid=labeled_ledger())
    assert payloads and not writer.writes and result.rows_written == 0
    assert result.status == "failed"
    assert any("SYNA.US" in warning and "blank instrument name" in warning
        for warning in result.warnings)
    assert sync._page_fresh_fetch_metrics(result)[0] == 2


@pytest.mark.parametrize("failure", ["currency", "foreign", "duplicate"])
def test_valid_operator_labels_cannot_bypass_native_currency_or_membership_contract(failure, monkeypatch):
    monkeypatch.setenv("TFB_SYNC_FETCHFAIL_TRUTH", "observe")
    rows = [nameless_quote(symbol) for symbol in SYMBOLS]
    if failure == "currency":
        rows[0][HEADERS.index("Currency")] = "USD"  # SAR ledger holding
    elif failure == "foreign":
        rows.append(nameless_quote("FOREIGN.US"))
    else:
        rows.append(copy.deepcopy(rows[0]))
    result, writer, payloads = run_portfolio(monkeypatch, rows, grid=labeled_ledger())
    assert payloads and not writer.writes and result.rows_written == 0


@pytest.mark.parametrize("missing_column", ["Name", "Warnings"])
def test_missing_label_or_provenance_output_column_does_not_fabricate_label(missing_column):
    basis = sync._read_cost_basis(Writer(labeled_ledger()), "offline")
    keep = [i for i, header in enumerate(HEADERS) if header != missing_column]
    headers = [HEADERS[i] for i in keep]
    rows = [[row[i] for i in keep] for row in [nameless_quote(symbol) for symbol in SYMBOLS]]
    output, count = sync._inject_portfolio_holdings(headers, rows, basis, include_ledger_name=True)
    assert count == 3
    if "Name" in headers:
        assert all(row[headers.index("Name")] in (None, "") for row in output)
    if "Warnings" in headers:
        assert all(SOURCE not in row[headers.index("Warnings")] for row in output)


def test_final_label_cannot_erase_terminal_failure_or_upgrade_real_eligibility_guard(monkeypatch):
    monkeypatch.setenv("TFB_SYNC_FETCHFAIL_TRUTH", "observe")
    monkeypatch.setattr(sync, "_false_green_screen_enabled", lambda: True)
    rows = [nameless_quote(symbol,
        failure="fetch_failed:synthetic" if symbol == "SYNA.US" else "")
        for symbol in SYMBOLS]
    failing = rows_by_symbol(rows)["SYNA.US"]
    failing[HEADERS.index("Final Action")] = "INVEST"
    failing[HEADERS.index("Investability Status")] = "INVESTABLE"
    observed_guard_names = []
    screen = sync._apply_false_green_screen

    def probe(headers, matrix, page):
        observed_guard_names.extend(row[headers.index("Name")] for row in matrix)
        return screen(headers, matrix, page)

    monkeypatch.setattr(sync, "_apply_false_green_screen", probe)
    result, writer, _ = run_portfolio(monkeypatch, rows, grid=labeled_ledger())
    assert observed_guard_names and all(not name for name in observed_guard_names)
    assert result.rows_written == 3
    output = rows_by_symbol(writer.published)["SYNA.US"]
    assert cell(output, "Name") == LABELS["SYNA.US"]
    assert cell(output, "Final Action") == "DO_NOT_INVEST"
    assert cell(output, "Investability Status") == "BLOCKED"
    assert "fetch_failed:synthetic" in tokens(output)
    assert "acquisition_status:failed" in tokens(output)
    assert "false_green_blocked:v6.54.0" in tokens(output) and SOURCE in tokens(output)
    assert sync._page_fresh_fetch_metrics(result) == (2, 3, pytest.approx(200 / 3))
    assert "acquired=2/3 acquisition=PARTIAL" in writer.status_rows[-1][3]
    audit = full_audit(writer.published)
    assert audit.name_pct == 100 and audit.fresh == 2 and audit.status == "FAIL"


@pytest.mark.parametrize("ambiguous_column", ["Company Name", "Flags"])
def test_ambiguous_label_or_warning_output_columns_do_not_supply_name_proof(ambiguous_column):
    basis = sync._read_cost_basis(Writer(labeled_ledger()), "offline")
    headers = HEADERS + [ambiguous_column]
    rows = [nameless_quote(symbol) + [""] for symbol in SYMBOLS]
    output, count = sync._inject_portfolio_holdings(headers, rows, basis, include_ledger_name=True)
    assert count == 3
    assert all(row[NAME_I] in (None, "") and SOURCE not in row[WARN_I] for row in output)


@pytest.mark.parametrize("raw_warning", [
    ["synthetic_quote_note", "acquisition_status:preserved"],
    {"acquisition_status": "preserved"}, 123, True, False,
])
def test_nonstring_warning_cell_is_preserved_and_never_stringified_for_name_fallback(raw_warning):
    from core.data_validity import row_acquisition

    basis = sync._read_cost_basis(Writer(labeled_ledger()), "offline")
    incoming = nameless_quote("SYNA.US")
    incoming[WARN_I] = copy.deepcopy(raw_warning)
    original = copy.deepcopy(incoming)
    before = row_acquisition(dict(zip(HEADERS, incoming)), NOW, 8 * 3600)
    output, count = sync._inject_portfolio_holdings(
        HEADERS, [incoming], basis, include_ledger_name=True)
    after = row_acquisition(dict(zip(HEADERS, output[0])), NOW, 8 * 3600)
    assert count == 1 and incoming == original
    assert output[0][WARN_I] == raw_warning and type(output[0][WARN_I]) is type(raw_warning)
    assert output[0][NAME_I] in (None, "") and before == after
    if isinstance(raw_warning, list):
        assert before.status == "INVALID"


def test_absent_warning_cell_allows_label_provenance_without_minting_acquisition_proof():
    from core.data_validity import row_acquisition

    basis = sync._read_cost_basis(Writer(labeled_ledger()), "offline")
    incoming = nameless_quote("SYNA.US")
    incoming[WARN_I] = None
    before = row_acquisition(dict(zip(HEADERS, incoming)), NOW, 8 * 3600)
    output, _ = sync._inject_portfolio_holdings(
        HEADERS, [incoming], basis, include_ledger_name=True)
    assert output[0][NAME_I] == LABELS["SYNA.US"] and output[0][WARN_I] == SOURCE
    assert row_acquisition(dict(zip(HEADERS, output[0])), NOW, 8 * 3600) == before
