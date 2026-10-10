"""Final audited publication can only downgrade one bounded feed cell pair."""
import asyncio
import copy
import json
from datetime import datetime, timezone
from types import SimpleNamespace

import pytest

from scripts import finalize_decision_publication as final
from tests.test_decision_surface_freshness import (
    NOW, FLOORS, portfolio_grid, status_grid, top10_grid,
)


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
    for page in sorted(final.REQUIRED_PAGES):
        symbol = page.upper().replace("_", "")[:10] + ".SR"
        rule = final.coverage.Rule(page, 1, 8, 100, 100, 100,
                                   symbols=page not in {"Insights_Analysis", "Data_Dictionary"},
                                   portfolio=page == "My_Portfolio")
        row = [symbol, "Synthetic name", 0 if coverage_failure and page == "Global_Markets" else 50,
               now.isoformat(), 1, 40, "eodhd"]
        full.pages.append(final.coverage.audit_grid([headers, row], rule, headers, now, [symbol]))
    if warning:
        full.pages[0].warnings.append("synthetic maintenance warning")
        full.pages[0].finish()
    decision = final.freshness.audit_surfaces(
        status_grid(), portfolio_grid("2026-07-31 19:00:00" if decision_failure else "2026-07-31 20:05:00"),
        top10_grid(), now_utc=NOW, min_rows=FLOORS)
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
    grid = status_grid()
    for row in grid[1:]:
        if row[0] in FLOORS:
            row[3] += " run=90001"
    assert final.freshness.audit_surfaces(grid, portfolio_grid(), top10_grid(), now_utc=NOW,
                                         min_rows=FLOORS, expected_source_run="90001").exit_code == 0
    assert final.freshness.audit_surfaces(status_grid(), portfolio_grid(), top10_grid(), now_utc=NOW,
                                         min_rows=FLOORS).exit_code == 0
