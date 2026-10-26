"""finalize 1.2.0: a sync whose only decision findings are same-cohort consumer
lag ends with a warning, never with a promotion, a skipped blocker or a
changed evidence exit_code."""
import copy
import json
from datetime import datetime, timedelta, timezone

import pytest

from scripts import finalize_decision_publication as final
from tests.test_decision_surface_freshness import _attach_pool_receipt, _native_pool_case
from tests.test_final_decision_publication import (  # noqa: F401 - autouse fixture
    GoogleValues, approved_scope_fixture, reports, run,
)
from tests.test_full_refresh_coverage import scoped_symbols, symbol_column

LAG_POOL = ("Physical source membership and deduplicated opportunity pool cannot be reconciled: "
            "ValueError: native pool reconciliation does not match one consumed/current source cohort")


def finding(code, message="synthetic", severity="FAIL"):
    return {"severity": severity, "code": code, "surface": "Top_10_Investments", "message": message}


def pending_result(**changes):
    """The shape of run 40380186439 (26 Oct 18:42Z): coverage 0, decision 2, blocker published."""
    result = {
        "script_version": final.SCRIPT_VERSION, "run_id": "90001", "exit_code": 2,
        "audit_exit_codes": [0, 2], "blockers": ["decision_failed"],
        "blocker_published": True, "feed_promoted": False,
        "audits": {"coverage": {"summary": {"exit_code": 0}}, "decision": {
            "freshness_exit_code": 2, "summary": {"exit_code": 2}, "executable": False,
            "structural_validation": {"exit_code": 0, "same_job_witness": True, "findings": []},
            "findings": [finding("PF_OLDER_THAN_SOURCE"), finding("PF_SOURCE_CONSUMPTION_UNPROVEN"),
                         finding("T10_SOURCE_CONSUMPTION_UNPROVEN"),
                         *[finding("T10_OLDER_THAN_SOURCE") for _ in range(4)],
                         finding("T10_POOL_RECONCILIATION_UNPROVEN", LAG_POOL),
                         finding("FALSE_FULL_UNIVERSE_CLAIM")]}},
    }
    for path, value in changes.items():
        target = result
        keys = path.split(".")
        for key in keys[:-1]:
            target = target[key]
        target[keys[-1]] = value
    return result


def test_observed_lag_state_is_consumer_pending():
    assert final.classify_consumer_pending(pending_result()) is True


@pytest.mark.parametrize("change", [
    {"audit_exit_codes": [2, 2]}, {"audit_exit_codes": [3, 2]}, {"audit_exit_codes": [0, 3]},
    {"audit_exit_codes": [0, 0]}, {"audit_exit_codes": [True, 2]}, {"audit_exit_codes": [0]},
    {"exit_code": 3}, {"blockers": ["coverage_failed", "decision_failed"]},
    {"blockers": ["approved_scope_unavailable", "decision_failed"]},
    {"blocker_published": False}, {"publication_error": "unconfirmed"}, {"fatal": "boom"},
    {"feed_promoted": True},
    {"audits.decision.structural_validation": None},
    {"audits.decision.structural_validation": {"exit_code": 3, "same_job_witness": False}},
    {"audits.decision.structural_validation": {"exit_code": 2, "same_job_witness": True}},
    {"audits.decision.freshness_exit_code": 3},
    {"audits.decision.findings": []},
    {"audits.decision.findings": [finding("FALSE_FULL_UNIVERSE_CLAIM")]},
    {"audits.decision.findings": [finding("T10_OLDER_THAN_SOURCE"), finding("T10_RUN_STALE")]},
    {"audits.decision.findings": [finding("T10_OLDER_THAN_SOURCE"), finding("PF_RUN_STALE")]},
    {"audits.decision.findings": [finding("T10_OLDER_THAN_SOURCE"), finding("T10_STATUS_NOT_OK")]},
    {"audits.decision.findings": [finding("T10_OLDER_THAN_SOURCE"), finding("SOURCE_STALE")]},
    {"audits.decision.findings": [finding("T10_OLDER_THAN_SOURCE"), finding("T10_POOL_COUNT_MISSING")]},
    {"audits.decision.findings": [finding("T10_OLDER_THAN_SOURCE"), finding("SOURCE_PUBLICATION_COHORT")]},
    {"audits.decision.findings": [finding("T10_OLDER_THAN_SOURCE"),
                                  finding("STRUCTURE_WITNESS_UNAVAILABLE")]},
    {"audits.decision.findings": [finding("T10_POOL_RECONCILIATION_UNPROVEN",
                                          "ValueError: Mutual_Funds explicit pool cap omitted unique rows")]},
    {"audits.decision.findings": [finding("T10_OLDER_THAN_SOURCE", severity="WARN")]},
    {"audits.decision.findings": ["T10_OLDER_THAN_SOURCE"]},
    {"audits.decision.findings": None},
])
def test_any_other_condition_stays_red(change):
    assert final.classify_consumer_pending(pending_result(**change)) is False


def test_malformed_results_never_classify():
    for result in ({}, {"exit_code": 2}, {"exit_code": 3, "fatal": "x", "feed_promoted": False}):
        assert final.classify_consumer_pending(result) is False


@pytest.mark.parametrize("policy,expected", [("", 2), ("off", 2), ("WARN", 0), ("warn", 0), (" warn ", 0), ("1", 2)])
def test_process_exit_follows_policy_and_evidence_is_unchanged(monkeypatch, policy, expected):
    monkeypatch.setenv("TFB_FINAL_CONSUMER_PENDING", policy)
    result = pending_result()
    result["consumer_pending"] = final.classify_consumer_pending(result)
    assert final.process_exit_code(result) == expected
    assert result["exit_code"] == 2 and result["blockers"] == ["decision_failed"]


def test_coverage_warning_survives_pending_classification(monkeypatch):
    monkeypatch.setenv("TFB_FINAL_CONSUMER_PENDING", "warn")
    result = pending_result(audit_exit_codes=[1, 2])
    result["consumer_pending"] = final.classify_consumer_pending(result)
    assert result["consumer_pending"] and final.process_exit_code(result) == 1


def test_not_pending_result_keeps_its_exit_under_warn(monkeypatch):
    monkeypatch.setenv("TFB_FINAL_CONSUMER_PENDING", "warn")
    result = pending_result(blockers=["coverage_failed", "decision_failed"])
    result["consumer_pending"] = final.classify_consumer_pending(result)
    assert final.process_exit_code(result) == 2


def _live_grids(lagging):
    """The real pool-proven fixture; 'lagging' = both cockpits still show run 80000."""
    _scope, surfaces, physical, receipt = _native_pool_case((final.load_scope(), scoped_symbols()))
    sources, portfolio, top10 = surfaces
    if lagging:
        receipt["run"] = "80000"    # the board still carries the previous cohort's signed receipt
    _attach_pool_receipt(surfaces, receipt)
    now = datetime.now(timezone.utc).astimezone(timezone(timedelta(hours=3)))
    source_stamp = (now - timedelta(minutes=10)).strftime("%Y-%m-%d %H:%M:%S")
    surface_stamp = (now - timedelta(minutes=5 if not lagging else 40)).strftime("%Y-%m-%d %H:%M:%S")
    for row in sources[1:]:
        row[1] = source_stamp
    for surface in (portfolio, top10):
        surface[1][1] = (surface[1][1].replace("2026-07-31 20:05:00", surface_stamp)
                         .replace("2026-07-31 20:10:00", surface_stamp))
        if lagging:
            surface[1][1] = surface[1][1].replace("(run 90001)", "(run 80000)")
    grids = {"_Status!A1:J100": sources, "Portfolio_Decision!A1:B3": portfolio,
             "Top_10_Investments!A1:B3": top10,
             **{page + "!A1:EZ20050": grid for page, grid in physical.items()},
             **{page + "!A:A": symbol_column(grid) for page, grid in physical.items()}}
    return grids


@pytest.mark.parametrize("lagging", [False, True])
def test_real_finalizer_and_auditor_classify_only_the_lagging_cohort(monkeypatch, tmp_path, lagging):
    full, _decision = reports()
    grids = _live_grids(lagging)

    async def actual_coverage_fixture(*_args, **_kwargs):
        return full

    monkeypatch.setattr(final.coverage, "run", actual_coverage_fixture)
    # decision_structure() has its own battery; here it supplies a clean witness.
    monkeypatch.setattr(final, "decision_structure", lambda *_a: {
        "exit_code": 0, "validator_exit_code": 0, "same_job_witness": True, "findings": []})
    service = GoogleValues()
    before = copy.deepcopy(service.rows)
    result = run(service, reader=lambda _sheet, a1: copy.deepcopy(grids[a1]),
                 decision_structure_report=str(tmp_path / "witness.json"))
    pending = final.classify_consumer_pending(result)
    if not lagging:
        assert result["exit_code"] == 0 and not pending and service.rows == before
        return
    codes = {f["code"] for f in result["audits"]["decision"]["findings"]}
    assert codes and codes <= final.CONSUMER_PENDING_CODES, codes
    assert pending is True
    # The veto is untouched: evidence exit 2, blocker written, nothing promoted.
    assert result["exit_code"] == 2 and result["blockers"] == ["decision_failed"]
    assert result["blocker_published"] and not result["feed_promoted"]
    assert service.rows[3][12].startswith("NOT_ACTIONABLE(final_publication) | run=90001 | ")


def test_main_writes_classification_and_returns_process_exit(monkeypatch, tmp_path):
    out = tmp_path / "final.json"

    async def fake_run(*_args, **_kwargs):
        return pending_result()

    monkeypatch.setattr(final, "run", fake_run)
    monkeypatch.setenv("TFB_FINAL_CONSUMER_PENDING", "warn")
    assert final.main(["--sheet-id", "s", "--json-out", str(out)]) == 0
    written = json.loads(out.read_text())
    assert written["exit_code"] == 2 and written["consumer_pending"] is True
    assert written["process_exit_code"] == 0 and written["blockers"] == ["decision_failed"]
    monkeypatch.delenv("TFB_FINAL_CONSUMER_PENDING")
    assert final.main(["--sheet-id", "s", "--json-out", str(out)]) == 2
    assert json.loads(out.read_text())["process_exit_code"] == 2


@pytest.mark.parametrize("code,pending,warned", [(0, "1", True), (0, "0", False), (2, "1", False)])
def test_workflow_step_warns_only_for_an_accepted_pending_exit(code, pending, warned):
    """Runs the real 'Audit final coverage and decision clocks' shell block."""
    from tests.test_final_publication_workflow import FinalPublicationShellTests
    case = FinalPublicationShellTests("test_summary_missing_report_never_claims_acceptance")
    case.setUp()
    try:
        stub = case.bin / "python"
        text = stub.read_text(encoding="utf-8")
        marker = "'blocker_published': code >= 2, 'feed_promoted': False}"
        assert text.count(marker) == 1
        stub.write_text(text.replace(marker, marker[:-1] + ", 'consumer_pending': os.environ['STUB_PENDING'] == '1'}"),
                        encoding="utf-8")
        result = case.run_step("Audit final coverage and decision clocks",
                               STUB_AUDIT_EXIT=str(code), STUB_PENDING=pending)
        assert result.returncode == code, result.stderr
        assert ("::warning::Sources accepted;" in result.stdout) is warned
        assert ("::error::" in result.stdout) is (code != 0)
    finally:
        case.doCleanups()


def test_policy_is_step_scoped_with_repository_variable_rollback():
    from tests.test_final_publication_workflow import FINAL_JOB, WORKFLOW, job_source, step_source
    job = job_source(WORKFLOW.read_text(encoding="utf-8"), FINAL_JOB)
    step = step_source(job, "Audit final coverage and decision clocks")
    assert "TFB_FINAL_CONSUMER_PENDING: ${{ vars.TFB_FINAL_CONSUMER_PENDING || 'warn' }}" in step
    # The job env stays byte-identical to release_decision_feed.yml's copy.
    assert "TFB_FINAL_CONSUMER_PENDING" not in job.split("    steps:", 1)[0]
