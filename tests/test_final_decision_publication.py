"""Final audited publication can only downgrade one bounded feed cell pair."""
import asyncio
import copy
import json
from datetime import datetime, timezone, timedelta
from types import SimpleNamespace

import pytest

from scripts import finalize_decision_publication as final
from tests.test_decision_surface_freshness import (
    NOW, FLOORS, portfolio_grid, status_grid, top10_grid, publication_grids,
    approved_grids, _native_pool_case, _attach_pool_receipt,
)
from tests.test_full_refresh_coverage import scoped_grid, scoped_symbols, synthetic_approved_scope, symbol_column
from scripts.approved_universe_scope import ScopePolicyError


@pytest.fixture(autouse=True)
def approved_scope_fixture(tmp_path, monkeypatch):
    """Simulate a validated approval at full current counts, never reduced floors."""
    scope = synthetic_approved_scope(tmp_path)
    monkeypatch.setattr(final, "load_scope", lambda: scope)
    return scope


def current_publication_grids():
    sources, portfolio, top10 = publication_grids()
    for row in sources[1:]:
        if row[0] in scoped_symbols():
            approved = len(scoped_symbols()[row[0]])
            row[3] = row[3].replace(f"acquired={row[6]}/{row[6]}", f"acquired={approved}/{approved}")
            row[6] = approved
    top10[1][1] = top10[1][1].replace("Market_Leaders 1356/1356", "Market_Leaders 255/255").replace(
        "Mutual_Funds 4496/4496", "Mutual_Funds 2474/2474")
    return sources, portfolio, top10


def complete_source_grids():
    # Approval covers two source pages, but individual pool exclusions require
    # all four real-cardinality owner grids, not count-only owner placeholders.
    _surfaces, physical = approved_grids(scoped_symbols())
    return physical


class Request:
    def __init__(self, action):
        self.action = action

    def execute(self):
        return self.action()


class GoogleValues:
    """Model the actual values get/update API and a wider _Status worksheet."""
    def __init__(self, *, feed=True, readback=None, write_error=False):
        self.rows = [[f"A{r}", f"B{r}", *[f"other-{r}-{c}" for c in range(2, 11)], "", "", f"N{r}"]
                     for r in range(1, 81)]
        self.rows[0][11:13] = ["Backend URL", "https://synthetic.invalid"]
        self.rows[1][11:13] = ["Last Global Update", "synthetic-stamp"]
        if feed:
            self.rows[3][11:13] = [final.KEY, "EXECUTABLE | synthetic old feed"]
        self.calls = []
        self.readback = readback
        self.write_error = write_error
        self.updated = False

    def spreadsheets(self):
        return self

    def values(self):
        return self

    def get(self, **kwargs):
        self.calls.append(("get", copy.deepcopy(kwargs)))

        def read():
            if kwargs["range"] == final.KEY_RANGE:
                return {"values": [copy.deepcopy(row[11:13]) for row in self.rows[:60]]}
            if self.updated and self.readback is not None:
                return {"values": copy.deepcopy(self.readback)}
            slot = int(kwargs["range"].split("!L")[1].split(":")[0])
            return {"values": [copy.deepcopy(self.rows[slot - 1][11:13])]}
        return Request(read)

    def update(self, **kwargs):
        self.calls.append(("update", copy.deepcopy(kwargs)))

        def write():
            if self.write_error:
                raise RuntimeError("synthetic transport failure")
            slot = int(kwargs["range"].split("!L")[1].split(":")[0])
            self.rows[slot - 1][11:13] = copy.deepcopy(kwargs["body"]["values"][0])
            self.updated = True
            return {"updatedCells": 2}
        return Request(write)


BLOCKER = "NOT_ACTIONABLE(final_publication) | run=90001 | synthetic audit failure"


@pytest.mark.parametrize("feed,slot", [(True, 4), (False, 3)])
def test_blocker_update_is_one_raw_bounded_pair_with_exact_readback(feed, slot):
    service = GoogleValues(feed=feed)
    before = copy.deepcopy(service.rows)
    final.publish_blocker(service, "synthetic-sheet", BLOCKER)
    expected = copy.deepcopy(before)
    expected[slot - 1][11:13] = [final.KEY, BLOCKER]
    assert service.rows == expected
    assert [kind for kind, _ in service.calls] == ["get", "update", "get"]
    update = service.calls[1][1]
    assert update == {"spreadsheetId": "synthetic-sheet", "range": f"'_Status'!L{slot}:M{slot}",
                      "valueInputOption": "RAW", "body": {"values": [[final.KEY, BLOCKER]]}}
    assert service.calls[2][1]["range"] == update["range"]


@pytest.mark.parametrize("grid", [
    None, [], [["wrong block", "anything"]],
    [["Backend URL", "x"], ["Backend URL", "duplicate"]],
    [["Backend URL", "x"], [final.KEY, "x"], [" tfb decision feed ", "y"]],
    [["Backend URL", "x"], ["", "orphaned financial value"]],
    [["Backend URL", "x"], ["", 0]], [["Backend URL", "x"], ["", False]],
    [["Backend URL", "x", "moved column"]], [["Backend URL", "x"], "bad row"],
    [["Backend URL", "x"]] + [[str(index), "occupied"] for index in range(59)],
    [["Backend URL", "x"]] + [[] for _ in range(60)],
])
def test_ambiguous_missing_or_full_layout_refuses_blind_write(grid):
    with pytest.raises(ValueError):
        final.publication_slot(grid)


@pytest.mark.parametrize("readback", [[], [[final.KEY]], [[final.KEY, "EXECUTABLE"]],
                                       [[final.KEY, BLOCKER, "extra"]], [["wrong key", BLOCKER]]])
def test_publication_is_unconfirmed_when_readback_differs(readback):
    with pytest.raises(RuntimeError, match="unconfirmed"):
        final.publish_blocker(GoogleValues(readback=readback), "synthetic-sheet", BLOCKER)


@pytest.mark.parametrize("value", ["EXECUTABLE", "NOT_ACTIONABLE(other)", None,
                                   "NOT_ACTIONABLE(final_publication)unsafe"])
def test_writer_has_no_promotion_or_other_key_value_operation(value):
    service = GoogleValues()
    with pytest.raises(ValueError):
        final.publish_blocker(service, "synthetic-sheet", value)
    assert service.calls == []


def reports(*, coverage_failure=False, decision_failure=False, warning=False):
    """Use actual auditors to construct the report objects consumed by run()."""
    now = datetime.now(timezone.utc)
    headers = ["Symbol", "Name", "Current Price", "Last Updated (UTC)",
               "Position Qty", "Avg Cost", "Data Provider"]
    full = final.coverage.Report(now.isoformat(), "***", "synthetic", [])
    scope = final.load_scope()
    full.approved_scope = scope.receipt()
    for page in sorted(final.REQUIRED_PAGES):
        symbol = page.upper().replace("_", "")[:10] + ".SR"
        rule = final.coverage.Rule(page, 1, 8, 100, 100, 100,
                                   symbols=page not in {"Insights_Analysis", "Data_Dictionary"},
                                   portfolio=page == "My_Portfolio")
        row = [symbol, "Synthetic name", 0 if coverage_failure and page == "Global_Markets" else 50,
               now.isoformat(), 1, 40, "eodhd"]
        grid = scoped_grid(page, now=now) if page in scope.pages else [headers, row]
        page_headers = headers
        if page == "Insights_Analysis":
            from core.sheets.schema_registry import get_sheet_headers
            page_headers = get_sheet_headers(page)
            grid = [page_headers, ["Overview", "Sources", "count", 0, "", "", 1]]
        full.pages.append(final.coverage.audit_grid(grid, rule, page_headers, now, [symbol], approved_scope=scope,
                                                   source_column=symbol_column(grid) if page in scope.pages else None))
    if warning:
        full.pages[0].warnings.append("synthetic maintenance warning")
        full.pages[0].finish()
    grids = list(current_publication_grids())
    if decision_failure:
        grids[1][1][1] = grids[1][1][1].replace("2026-07-31 20:05:00", "2026-07-31 19:00:00")
    physical = complete_source_grids()
    decision = final.freshness.audit_surfaces(*grids, now_utc=NOW, min_rows=final.MARKET_FLOORS,
        expected_source_run="90001", approved_scope=scope,
        source_grids=physical,
        source_columns={page: symbol_column(grid) for page, grid in physical.items()})
    return full, decision


def install_reports(monkeypatch, full, decision):
    calls = {}

    async def coverage_run(*args, **kwargs):
        calls["coverage"] = (args, kwargs)
        if isinstance(full, Exception):
            raise full
        return full

    async def decision_run(*args, **kwargs):
        calls["decision"] = (args, kwargs)
        if isinstance(decision, Exception):
            raise decision
        return decision

    monkeypatch.setattr(final.coverage, "run", coverage_run)
    monkeypatch.setattr(final.freshness, "run_live", decision_run)
    return calls


def run(service, *, publish=True, **kwargs):
    return asyncio.run(final.run("synthetic-sheet", service=service, publish=publish,
                                 run_id="90001", **kwargs))


@pytest.mark.parametrize("warning", [False, True])
def test_clean_or_warning_only_audits_never_promote_or_change_existing_feed(monkeypatch, warning):
    install_reports(monkeypatch, *reports(warning=warning))
    service = GoogleValues()
    before = copy.deepcopy(service.rows)
    result = run(service)
    assert result["exit_code"] == int(warning)
    assert result["blockers"] == [] and not result["feed_promoted"] and not result["blocker_published"]
    assert service.rows == before and service.calls == []


def test_explicit_decision_warning_is_not_promoted(monkeypatch):
    full, decision = reports()
    decision.findings.append(final.freshness.Finding("WARN", "SYNTH_WARN", "Top_10_Investments", "warning"))
    decision.executable = False
    install_reports(monkeypatch, full, decision)
    service = GoogleValues()
    result = run(service)
    assert result["exit_code"] == 1 and result["blockers"] == [] and service.calls == []
    assert not result["feed_promoted"]


@pytest.mark.parametrize("failed", ["coverage", "decision", "both"])
def test_actual_coverage_and_decision_failure_reports_downgrade_feed(monkeypatch, failed):
    install_reports(monkeypatch, *reports(coverage_failure=failed != "decision", decision_failure=failed != "coverage"))
    service = GoogleValues()
    result = run(service)
    assert result["exit_code"] == 2 and result["blocker_published"] and not result["feed_promoted"]
    assert service.rows[3][12].startswith("NOT_ACTIONABLE(final_publication) | run=90001 | ")
    assert set(result["blockers"]) == ({"coverage_failed", "decision_failed"} if failed == "both" else {failed + "_failed"})


def test_read_only_failure_preserves_sheet_and_retains_failure(monkeypatch):
    install_reports(monkeypatch, *reports(coverage_failure=True))
    service = GoogleValues()
    result = run(service, publish=False)
    assert result["exit_code"] == 2 and not result["blocker_published"] and service.calls == []


@pytest.mark.parametrize("name", ["coverage", "decision"])
@pytest.mark.parametrize("code", [True, "0", -1, 4, 0.0, None])
def test_malformed_audit_exit_code_is_blocking(monkeypatch, name, code):
    full, decision = reports()
    if name == "coverage":
        full = SimpleNamespace(code=code, pages=full.pages, payload=full.payload)
    else:
        decision = SimpleNamespace(exit_code=code, executable=True, payload=decision.payload)
    install_reports(monkeypatch, full, decision)
    result = run(GoogleValues())
    assert result["exit_code"] == 3 and result["blocker_published"] and name + "_failed" in result["blockers"]


@pytest.mark.parametrize("change", ["omit", "duplicate", "payload", "summary", "unaccepted", "missing_warning_verdict"])
def test_missing_coverage_or_unverifiable_report_cannot_certify_publication(monkeypatch, change):
    full, decision = reports()
    if change == "omit":
        full.pages.pop()
    elif change == "duplicate":
        full.pages.append(copy.deepcopy(full.pages[0]))
    elif change in {"payload", "summary"}:
        full = SimpleNamespace(code=0, pages=full.pages, payload=lambda: None if change == "payload" else {"summary": {"exit_code": 1}})
    elif change == "unaccepted":
        decision.executable = False
    else:
        decision = SimpleNamespace(exit_code=1, payload=lambda: {"summary": {"exit_code": 1}})
    install_reports(monkeypatch, full, decision)
    result = run(GoogleValues())
    assert result["exit_code"] == 3 and result["blocker_published"]


@pytest.mark.parametrize("name", ["coverage", "decision"])
def test_fatal_audit_exceptions_downgrade_and_redact_credentials(monkeypatch, name):
    monkeypatch.setenv("APP_TOKEN", "synthetic-final-secret")
    full, decision = reports()
    fault = RuntimeError("upstream failed token=synthetic-final-secret")
    install_reports(monkeypatch, fault if name == "coverage" else full, fault if name == "decision" else decision)
    result = run(GoogleValues())
    assert result["exit_code"] == 3 and result["blocker_published"]
    assert name + "_unavailable" in result["blockers"]
    assert "synthetic-final-secret" not in json.dumps(result)


def test_auditor_payload_diagnostics_are_redacted_before_becoming_evidence(monkeypatch):
    monkeypatch.setenv("APP_TOKEN", "synthetic-final-secret")
    full, decision = reports()
    full.pages[0].failures.append("source HTTP token=synthetic-final-secret failed")
    full.pages[0].finish()
    install_reports(monkeypatch, full, decision)
    result = run(GoogleValues())
    assert result["exit_code"] == 2 and "synthetic-final-secret" not in json.dumps(result)


@pytest.mark.parametrize("fault", ["transport", "readback", "layout"])
def test_unconfirmed_or_refused_publication_is_a_fatal_result(monkeypatch, fault):
    install_reports(monkeypatch, *reports(coverage_failure=True))
    service = GoogleValues(write_error=fault == "transport", readback=[] if fault == "readback" else None)
    if fault == "layout":
        service.rows[2][11:13] = ["", "orphan"]
    result = run(service)
    assert result["exit_code"] == 3 and not result["blocker_published"] and result["publication_error"]


@pytest.mark.parametrize("bound", [0, 100, 19999, 20001, 20000.0, True])
def test_direct_api_cannot_lower_or_change_full_audit_bound(monkeypatch, bound):
    calls = install_reports(monkeypatch, *reports())
    with pytest.raises(ValueError, match="20,000"):
        run(GoogleValues(), max_rows=bound)
    assert calls == {}


@pytest.mark.parametrize("setting", ["1", "0", "-5", "nan", "inf", "garbage"])
def test_environment_cannot_weaken_approved_final_acceptance_policy(monkeypatch, setting):
    calls = install_reports(monkeypatch, *reports())
    for page in final.MARKET_FLOORS:
        suffix = page.upper()
        monkeypatch.setenv("TFB_EXPECTED_MIN_ROWS_" + suffix, setting)
        monkeypatch.setenv("TFB_REFRESH_MIN_FRESH_PCT_" + suffix, setting)
        monkeypatch.setenv("TFB_REFRESH_MAX_AGE_H_" + suffix, "999")
    monkeypatch.setenv("TFB_REFRESH_MAX_AGE_H_MY_PORTFOLIO", "999")
    monkeypatch.setenv("TFB_DECISION_SOURCE_MAX_AGE_H", "999")
    monkeypatch.setenv("TFB_DECISION_SURFACE_MAX_AGE_H", "999")
    run(GoogleValues())
    rules = {rule.page: rule for rule in calls["coverage"][1]["policy_rules"]}
    for page, baseline in final.MARKET_FLOORS.items():
        assert rules[page].min_rows >= baseline
        assert rules[page].min_fresh >= 95 and rules[page].min_name >= 99 and rules[page].min_price >= 95
        assert rules[page].max_age_h <= 30
    assert rules["My_Portfolio"].max_age_h <= 8
    policy = calls["decision"][1]
    assert policy["min_rows"] == final.MARKET_FLOORS and policy["market_max_age_h"] <= 30
    assert policy["decision_max_age_h"] <= 8 and policy["expected_source_run"] == "90001"


def test_stricter_operator_policy_reaches_both_actual_auditor_interfaces(monkeypatch):
    calls = install_reports(monkeypatch, *reports())
    monkeypatch.setenv("TFB_EXPECTED_MIN_ROWS_GLOBAL_MARKETS", "7000")
    monkeypatch.setenv("TFB_REFRESH_MIN_FRESH_PCT_GLOBAL_MARKETS", "99")
    monkeypatch.setenv("TFB_DECISION_SOURCE_MAX_AGE_H", "12")
    monkeypatch.setenv("TFB_DECISION_SURFACE_MAX_AGE_H", "4")
    run(GoogleValues())
    rule = next(rule for rule in calls["coverage"][1]["policy_rules"] if rule.page == "Global_Markets")
    assert (rule.min_rows, rule.min_fresh, rule.max_age_h) == (7000, 99, 12)
    policy = calls["decision"][1]
    assert policy["min_rows"]["Global_Markets"] == 7000 and policy["min_fresh_percent"]["Global_Markets"] == 99
    assert policy["decision_max_age_h"] == 4


@pytest.mark.parametrize("page,configured", [("Market_Leaders", "1025"), ("Mutual_Funds", "4496"),
                                              ("Market_Leaders", "9000"), ("Mutual_Funds", "9000")])
def test_only_current_approved_memberships_supersede_configured_row_floor(monkeypatch, page, configured):
    calls = install_reports(monkeypatch, *reports())
    monkeypatch.setenv("TFB_EXPECTED_MIN_ROWS_" + page.upper(), configured)
    result = run(GoogleValues())
    rule = next(rule for rule in calls["coverage"][1]["policy_rules"] if rule.page == page)
    assert rule.min_rows == len(scoped_symbols()[page])
    assert calls["coverage"][1]["approved_scope"] is calls["decision"][1]["approved_scope"]
    assert result["policy"]["approved_scope"]["pages"][page]["supersedes_min_rows"] == (
        1025 if page == "Market_Leaders" else 4496)


@pytest.mark.parametrize("name", ["coverage", "decision"])
@pytest.mark.parametrize("fault", ["missing", "other_page", "policy_hash", "symbol_hash", "duplicate", "false"])
def test_final_acceptance_requires_complete_page_bound_membership_receipts(monkeypatch, name, fault):
    full, decision = reports()
    receipt = (next(page.approved_scope for page in full.pages if page.page == "Market_Leaders")
               if name == "coverage" else decision.source_status["Market_Leaders"]["approved_scope"])
    if fault == "missing":
        receipt.clear()
    elif fault == "other_page":
        other = (next(page.approved_scope for page in full.pages if page.page == "Mutual_Funds")
                 if name == "coverage" else decision.source_status["Mutual_Funds"]["approved_scope"])
        receipt.clear()
        receipt.update(copy.deepcopy(other))
    else:
        key, value = {"policy_hash": ("policy_sha256", "0" * 64),
                      "symbol_hash": ("observed_symbol_set_sha256", "0" * 64),
                      "duplicate": ("duplicate_count", 1), "false": ("valid", False)}[fault]
        receipt[key] = value
    install_reports(monkeypatch, full, decision)
    result = run(GoogleValues())
    assert result["exit_code"] == 3 and result["blocker_published"]
    assert name + "_failed" in result["blockers"] and not result["feed_promoted"]


@pytest.mark.parametrize("name", ["coverage", "decision"])
@pytest.mark.parametrize("fault", ["missing", "partial_range", "not_crossbound", "oversized", "bool_extent", "wrong_hash", "wrong_column"])
def test_final_acceptance_requires_independent_complete_column_proof(monkeypatch, name, fault):
    full, decision = reports()
    receipt = (next(page.approved_scope for page in full.pages if page.page == "Market_Leaders")
               if name == "coverage" else decision.source_status["Market_Leaders"]["approved_scope"])
    if fault == "missing":
        receipt.pop("complete_column_proof")
    else:
        key, value = {"partial_range": ("range", "Market_Leaders!A1:A20050"),
                      "not_crossbound": ("crossbound", False), "oversized": ("returned_rows", 20051),
                      "bool_extent": ("returned_rows", True), "wrong_hash": ("bounded_symbol_set_sha256", "0" * 64),
                      "wrong_column": ("symbol_column", 2)}[fault]
        receipt["complete_column_proof"][key] = value
    install_reports(monkeypatch, full, decision)
    result = run(GoogleValues())
    assert result["exit_code"] == 3 and result["blocker_published"] and not result["feed_promoted"]
    assert name + "_failed" in result["blockers"]


@pytest.mark.parametrize("publish", [False, True])
def test_missing_or_invalid_approval_blocks_without_audit_or_membership_fabrication(monkeypatch, publish):
    calls = install_reports(monkeypatch, *reports())
    def bad_scope():
        raise ScopePolicyError("Synthetic approval missing or invalid")
    monkeypatch.setattr(final, "load_scope", bad_scope)
    service = GoogleValues()
    result = run(service, publish=publish)
    assert result["exit_code"] == 3 and "approved_scope_unavailable" in result["blockers"]
    assert result["blocker_published"] is publish and not result["feed_promoted"]
    assert calls == {} and "approved_scope_error" in result["policy"]


@pytest.mark.parametrize("receipt", ["", " run=80000", " run=0", " run=oops", " run=90001 run=80000", " run=90001 run=90001"])
def test_actual_decision_auditor_requires_one_current_source_cohort_receipt(receipt):
    grid = status_grid()
    for row in grid[1:]:
        if row[0] in FLOORS:
            row[3] += " run=90001" if row[0] != "Global_Markets" else receipt
    report = final.freshness.audit_surfaces(grid, portfolio_grid(), top10_grid(), now_utc=NOW,
                                            min_rows=FLOORS, expected_source_run="90001")
    assert report.exit_code == 2 and not report.executable
    assert any(f.code == "SOURCE_PUBLICATION_COHORT" for f in report.findings)


def test_actual_decision_auditor_accepts_current_cohort_without_changing_default_api():
    assert final.freshness.audit_surfaces(*publication_grids(), now_utc=NOW,
                                         min_rows=FLOORS, expected_source_run="90001").exit_code == 0
    assert final.freshness.audit_surfaces(status_grid(), portfolio_grid(), top10_grid(), now_utc=NOW,
                                         min_rows=FLOORS).exit_code == 0


@pytest.mark.parametrize("fault", [None, "missing_pf", "stale_pf", "duplicate_top10", "stale_top10", "stale_portfolio_source",
                                   "source_substitution", "source_loss", "source_duplicate", "source_extra",
                                   "complete_column_tail", "complete_column_reorder", "pool_proven",
                                   "pool_missing_receipt", "pool_wrong_owner", "pool_owner_torn", "pool_capped"])
def test_real_finalizer_and_live_freshness_reader_enforce_consumed_source_run(monkeypatch, fault):
    full, _decision = reports()
    now_riyadh = datetime.now(timezone.utc).astimezone(timezone(timedelta(hours=3)))
    source_stamp = (now_riyadh - timedelta(minutes=10)).strftime("%Y-%m-%d %H:%M:%S")
    decision_stamp = (now_riyadh - timedelta(minutes=5)).strftime("%Y-%m-%d %H:%M:%S")
    sources, portfolio, top10 = current_publication_grids()
    physical = complete_source_grids()
    if fault and fault.startswith("pool_"):
        _scope, surfaces, physical, receipt = _native_pool_case((final.load_scope(), scoped_symbols()))
        sources, portfolio, top10 = surfaces
        if fault == "pool_wrong_owner":
            receipt["excluded"][0]["owner"] = "Commodities_FX"
        elif fault == "pool_capped":
            receipt["pages"]["Mutual_Funds"]["included"] -= 1
            top10[1][1] = top10[1][1].replace("2469/2474", "2468/2474")
        if fault != "pool_missing_receipt":
            _attach_pool_receipt(surfaces, receipt)
    for row in sources[1:]:
        row[1] = source_stamp
    for surface in (portfolio, top10):
        surface[1][1] = surface[1][1].replace("2026-07-31 20:05:00", decision_stamp).replace("2026-07-31 20:10:00", decision_stamp)
    if fault == "missing_pf":
        portfolio[1][1] = portfolio[1][1].split(" | source readiness")[0]
    elif fault == "stale_pf":
        portfolio[1][1] = portfolio[1][1].replace("(run 90001)", "(run 80000)")
    elif fault == "duplicate_top10":
        top10[1][1] += " | source readiness VERIFIED (run 90001)"
    elif fault == "stale_top10":
        top10[1][1] = top10[1][1].replace("(run 90001)", "(run 80000)")
    elif fault == "stale_portfolio_source":
        next(row for row in sources[1:] if row[0] == "My_Portfolio")[3] = \
            next(row for row in sources[1:] if row[0] == "My_Portfolio")[3].replace("run=90001", "run=80000")
    grids = {"_Status!A1:J100": sources, "Portfolio_Decision!A1:B3": portfolio,
             "Top_10_Investments!A1:B3": top10,
             **{page + "!A1:EZ20050": grid for page, grid in physical.items()},
             **{page + "!A:A": symbol_column(grid) for page, grid in physical.items()}}
    source = grids["Market_Leaders!A1:EZ20050"]
    if fault == "source_substitution":
        source[1][0] = "UNAPPROVED.SR"
    elif fault == "source_loss":
        source.pop()
    elif fault == "source_duplicate":
        source[-1][0] = source[1][0]
    elif fault == "source_extra":
        source.append(["UNAPPROVED.SR", "Synthetic", 50, source_stamp, "", "", "eodhd"])
    elif fault == "complete_column_tail":
        column = grids["Market_Leaders!A:A"]
        column.extend([[] for _ in range(20050 - len(column))])
        column.append(["UNAPPROVED.SR"])
    elif fault == "complete_column_reorder":
        column = grids["Market_Leaders!A:A"]
        column[1], column[2] = column[2], column[1]
    elif fault == "pool_owner_torn":
        grids["Global_Markets!A:A"][1][0] = "SYNTH_CHANGED_OWNER"
    reads = []

    def reader(sheet_id, a1):
        assert sheet_id == "synthetic-sheet"
        reads.append(a1)
        return copy.deepcopy(grids[a1])

    async def actual_coverage_fixture(*_args, **_kwargs):
        return full

    # Only coverage is stubbed. The finalizer invokes the real run_live reader,
    # strict freshness auditor, report verification and bounded blocker writer.
    monkeypatch.setattr(final.coverage, "run", actual_coverage_fixture)
    service = GoogleValues()
    before = copy.deepcopy(service.rows)
    result = run(service, reader=reader)
    assert set(reads) == set(grids)
    accepted = fault in {None, "pool_proven"}
    assert result["exit_code"] == (0 if accepted else 2)
    if accepted:
        assert not result["blocker_published"] and result["blockers"] == []
        assert service.rows == before and service.calls == []
    else:
        assert result["blocker_published"] and result["blockers"] == ["decision_failed"]
        assert service.rows[3][12].startswith("NOT_ACTIONABLE(final_publication) | run=90001")
        assert not result["feed_promoted"]
