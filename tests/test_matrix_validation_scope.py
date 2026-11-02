"""Execute scoped workflow commands and retain one final structural downgrade.

TFB_MATRIX_WORKFLOW_REF=561db60 exercises the old workflow as a golden negative.
"""
from __future__ import annotations

import ast
import asyncio
import hashlib
import json
import inspect
import os
from pathlib import Path
import subprocess
import sys
import textwrap
import types
from datetime import datetime, timedelta, timezone

import pytest

from scripts import finalize_decision_publication as final
from scripts import workflow_validation_scope as scope
from scripts import validate_dashboard as validator
from scripts import release_decision_feed as release
from tests.test_final_publication_workflow import job_source, step_source, shell_source
from tests.test_final_decision_publication import (
    approved_scope_fixture, reports, install_reports, GoogleValues,
)


ROOT = Path(__file__).resolve().parents[1]
SHA = "a" * 40
PAGES = ["Market_Leaders", "Global_Markets", "Commodities_FX", "Mutual_Funds", "My_Portfolio"]


@pytest.mark.parametrize("raw", ['{"count":1e500}', '{"count":-1e500}', '{"count":NaN}'])
def test_structural_witness_json_rejects_nonfinite_numbers(raw):
    with pytest.raises(ValueError):
        scope.strict_json(raw)


def workflow():
    ref = os.getenv("TFB_MATRIX_WORKFLOW_REF")
    if ref:
        return subprocess.run(["git", "show", ref + ":.github/workflows/daily_sync.yml"],
                              cwd=ROOT, check=True, text=True, capture_output=True).stdout
    return (ROOT / ".github/workflows/daily_sync.yml").read_text()


def test_key_scope_matches_actual_runner_tasks_without_importing_provider_engine():
    tree = ast.parse((ROOT / "scripts/run_dashboard_sync.py").read_text())
    tasks = next(n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name == "_default_tasks")
    actual = {}
    for call in ast.walk(tasks):
        if isinstance(call, ast.Call) and isinstance(call.func, ast.Name) and call.func.id == "TaskSpec":
            values = {k.arg: k.value.value for k in call.keywords if isinstance(k.value, ast.Constant)}
            actual[values["key"]] = values["sheet_name"]
    assert {**scope.SOURCE_PAGES, scope.DECISION_KEY: "Top_10_Investments"} == actual


@pytest.mark.parametrize("keys,pages", [
    ("GLOBAL_MARKETS", ["Global_Markets"]),
    ("MUTUAL_FUNDS", ["Mutual_Funds"]),
    ("MY_PORTFOLIO MARKET_LEADERS COMMODITIES_FX TOP_10_INVESTMENTS", ["My_Portfolio", "Market_Leaders", "Commodities_FX"]),
    ("DATA_DICTIONARY INSIGHTS_ANALYSIS", ["Data_Dictionary", "Insights_Analysis"]),
])
def test_workflow_leg_checks_only_its_written_sources(tmp_path, keys, pages):
    scripts = tmp_path / "scripts"
    scripts.mkdir()
    (scripts / "validate_dashboard.py").touch()
    binary = tmp_path / "bin"
    binary.mkdir()
    shim = binary / "python"
    shim.write_text(f"#!{sys.executable}\n" + "\n".join([
        "import json,os,sys", "from pathlib import Path",
        "if sys.argv[1] == 'scripts/validate_dashboard.py':",
        "    Path(os.environ['CALL']).write_text(json.dumps(sys.argv[1:]))",
        "    raise SystemExit(int(os.environ.get('VALIDATOR_CODE','0')))",
        "if sys.argv[1] == 'scripts/workflow_validation_scope.py':",
        f"    os.execv(sys.executable,[sys.executable,{str(ROOT / 'scripts/workflow_validation_scope.py')!r},*sys.argv[2:]])",
        "raise SystemExit(99)",
    ]) + "\n")
    shim.chmod(0o755)
    env = {"PATH": str(binary) + os.pathsep + os.defpath, "SYNC_KEYS": keys,
           "CALL": str(tmp_path / "call.json"), "TARGET_SHEET_ID": "test-sheet",
           "GITHUB_OUTPUT": str(tmp_path / "outputs")}
    job = job_source(workflow(), "sync-dashboard")
    script = shell_source(step_source(job, "✅ Validate Dashboard (contract + gate integrity)"))
    result = subprocess.run(["bash", "-eo", "pipefail", "-c", script], cwd=tmp_path,
                            env=env, text=True, capture_output=True, timeout=10)
    assert result.returncode == 0, result.stderr
    args = json.loads((tmp_path / "call.json").read_text())
    assert "--pages" in args, "the matrix leg used the validator's unrelated all-page default"
    start = args.index("--pages") + 1
    end = args.index("--json-out")
    assert args[start:end] == pages
    assert args[args.index("--write-sheet") + 1] == "0"


@pytest.mark.parametrize("keys", ["", "   ", "GLOBAL_MARKETS TYPO", "KSA_TADAWUL", "TOP_10_INVESTMENTS"])
def test_empty_unknown_and_native_only_keys_cannot_fall_back_to_all_pages(keys):
    with pytest.raises(ValueError):
        scope.source_pages(keys)


def structure_report(tmp_path, monkeypatch, *, failure=False):
    monkeypatch.setenv("GITHUB_SHA", SHA)
    checks = []
    for page in PAGES:
        checks.extend({"page": page, "name": name, "status": "PASS"}
                      for name in ("contract.header_match", "scope.coverage"))
    checks.extend({"page": "Top_10_Investments", "name": name,
                   "status": "FAIL" if failure and name == "decision.header_found" else "PASS",
                   "detail": "FEED NOT ACTIONABLE — hold" if name == "decision.feed_banner_present" else ""}
                  for name in ("decision.header_found", "decision.feed_banner_present", "decision.sizing_withheld_when_blocked"))
    path = tmp_path / "structural.json"
    payload = {"meta": {"reader": "read_range", "generated_riyadh": datetime.now(
        timezone(timedelta(hours=3))).strftime("%Y-%m-%d %H:%M:%S")}, "results": checks}
    path.write_text(json.dumps(payload))
    scope.attest_structure(path, 2 if failure else 0, run_id="90001", source_sha=SHA, sheet_id="synthetic-sheet")
    return path


def test_valid_final_native_witness_preserves_two_audits_and_no_promotion(tmp_path, monkeypatch, approved_scope_fixture):
    path = structure_report(tmp_path, monkeypatch)
    install_reports(monkeypatch, *reports())
    service = GoogleValues()
    result = asyncio.run(final.run("synthetic-sheet", service=service, publish=True, run_id="90001",
                                  decision_structure_report=path))
    assert result["audit_exit_codes"] == [0, 0]
    assert result["exit_code"] == 0 and service.calls == []
    assert not result["feed_promoted"]


def test_native_sizing_skip_requires_the_actual_actionable_branch(tmp_path, monkeypatch):
    path = structure_report(tmp_path, monkeypatch)
    report = json.loads(path.read_text())
    sizing = next(c for c in report["results"] if c["name"] == "decision.sizing_withheld_when_blocked")
    sizing.update(status="SKIP", detail="unavailable")
    path.write_text(json.dumps(report))
    assert final.decision_structure(path, "synthetic-sheet", "90001")["exit_code"] == 3
    banner = next(c for c in report["results"] if c["name"] == "decision.feed_banner_present")
    banner["detail"] = "FEED ACTIONABLE — HELD"
    sizing["detail"] = "feed not blocked"
    report["results"].append({"page": "Top_10_Investments", "name": "decision.price_present", "status": "PASS"})
    path.write_text(json.dumps(report))
    assert final.decision_structure(path, "synthetic-sheet", "90001")["exit_code"] == 0


def test_real_advisory_sanity_warning_keeps_prior_nonblocking_policy(tmp_path, monkeypatch, approved_scope_fixture):
    path = structure_report(tmp_path, monkeypatch)
    report = json.loads(path.read_text())
    # Exercise the actual producer, rather than inventing a warning vocabulary.
    warnings = [check.to_dict() for check in validator.check_sanity(
        "Global_Markets", {"symbol": "Symbol", "expected_roi_12m": "Expected ROI 12M"},
        {"Symbol", "Expected ROI 12M"}, [{"Symbol": "SYNTH.US", "Expected ROI 12M": .9}])
        if check.status == "WARN"]
    assert [check["name"] for check in warnings] == ["sanity.roi12_within_softcap"]
    report["results"].extend(warnings)
    report["workflow_witness"]["process_exit_code"] = 1
    path.write_text(json.dumps(report))
    install_reports(monkeypatch, *reports())
    baseline = asyncio.run(final.run("synthetic-sheet", publish=False, run_id="90001"))
    assert release.accepted(baseline, "90001") == (True, [])
    service = GoogleValues()
    result = asyncio.run(final.run("synthetic-sheet", service=service, publish=True, run_id="90001",
                                  decision_structure_report=path))
    assert result["audit_exit_codes"] == [0, 0] and result["exit_code"] == 0
    assert result["blockers"] == [] and service.calls == [] and not result["feed_promoted"]
    witness = result["audits"]["decision"]["structural_validation"]
    assert witness["validator_exit_code"] == 1 and witness["exit_code"] == 0
    assert witness["soft_warnings_nonblocking"] is True
    assert witness["findings"][0]["severity"] == "WARN"
    assert result["audits"]["decision"]["findings"] == baseline["audits"]["decision"]["findings"] == []
    assert release.accepted(result, "90001") == (True, [])


@pytest.mark.parametrize("name", ["contract.header_match", "scope.coverage", "gate.fake_critical", "sanity.future_unknown"])
def test_critical_or_unrecognized_warning_cannot_bypass_proof(tmp_path, monkeypatch, name):
    path = structure_report(tmp_path, monkeypatch)
    report = json.loads(path.read_text())
    matching = next((check for check in report["results"] if check["page"] == "Global_Markets" and check["name"] == name), None)
    if matching is None:
        report["results"].append({"page": "Global_Markets", "name": name, "status": "WARN"})
    else:
        matching["status"] = "WARN"
    report["workflow_witness"]["process_exit_code"] = 1
    path.write_text(json.dumps(report))
    assert final.decision_structure(path, "synthetic-sheet", "90001")["exit_code"] == 3


def test_old_ok_status_cannot_hide_missing_grid_and_exactly_one_downgrade(tmp_path, monkeypatch, approved_scope_fixture):
    path = structure_report(tmp_path, monkeypatch, failure=True)
    install_reports(monkeypatch, *reports())  # The independent B2/freshness evidence is all OK.
    service = GoogleValues()
    target = final
    if os.getenv("TFB_MATRIX_FINALIZER_REF"):
        target = types.ModuleType("baseline_matrix_finalizer")
        target.__file__ = final.__file__
        source = subprocess.run(["git", "show", os.environ["TFB_MATRIX_FINALIZER_REF"]+
                                 ":scripts/finalize_decision_publication.py"], cwd=ROOT,
                                check=True,text=True,capture_output=True).stdout
        exec(compile(source, target.__file__, "exec"), target.__dict__)
        monkeypatch.setattr(target, "load_scope", lambda: approved_scope_fixture)
    extra = ({"decision_structure_report": path} if "decision_structure_report" in
             inspect.signature(target.run).parameters else {})
    result = asyncio.run(target.run("synthetic-sheet", service=service, publish=True, run_id="90001", **extra))
    assert result["audit_exit_codes"] == [0, 2]
    assert result["exit_code"] == 2 and result["blocker_published"]
    assert result["audits"]["decision"]["executable"] is False
    assert release.accepted(result, "90001")[0] is False
    assert [kind for kind, _ in service.calls] == ["get", "update", "get"]
    assert service.rows[3][12].startswith("NOT_ACTIONABLE(final_publication)")


@pytest.mark.parametrize("fault", ["missing", "stale", "future", "wrong_run", "wrong_sha", "wrong_sheet",
                                  "all_skip", "missing_scope", "duplicate_check", "logical_reader", "wrong_exit"])
def test_unavailable_or_unbound_structure_withholds_in_normal_writer(tmp_path, monkeypatch, approved_scope_fixture, fault):
    path = structure_report(tmp_path, monkeypatch)
    payload = json.loads(path.read_text())
    if fault == "stale":
        payload["meta"]["generated_riyadh"] = "2020-01-01 00:00:00"
    elif fault == "future":
        payload["meta"]["generated_riyadh"] = "2099-01-01 00:00:00"
    elif fault.startswith("wrong_"):
        key = {"wrong_run": "run_id", "wrong_sha": "source_sha", "wrong_sheet": "sheet_sha256", "wrong_exit": "process_exit_code"}[fault]
        payload["workflow_witness"][key] = 2 if fault == "wrong_exit" else "wrong"
    elif fault == "all_skip":
        for check in payload["results"]:
            check["status"] = "SKIP"
    elif fault == "missing_scope":
        payload["results"] = [check for check in payload["results"] if check["name"] != "scope.coverage"]
    elif fault == "duplicate_check":
        payload["results"].append(payload["results"][0])
    elif fault == "logical_reader":
        payload["meta"]["reader"] = "get_sheet_rows"
    path.write_text(json.dumps(payload))
    if fault == "missing":
        path.unlink()
    install_reports(monkeypatch, *reports())
    service = GoogleValues()
    result = asyncio.run(final.run("synthetic-sheet", service=service, publish=True, run_id="90001",
                                  decision_structure_report=path))
    assert result["audit_exit_codes"] == [0, 3]
    assert result["exit_code"] == 3 and result["blocker_published"]
    assert len([call for call in service.calls if call[0] == "update"]) == 1


def test_final_workflow_runs_real_native_check_and_never_skips_finalizer():
    job = job_source(workflow(), "verify-final-decision-publication")
    script = shell_source(step_source(job, "Audit final coverage and decision clocks"))
    assert "--pages Market_Leaders Global_Markets Commodities_FX Mutual_Funds My_Portfolio Top_10_Investments" in script
    assert "--decision-structure-report final_dashboard_validation.json" in script
    assert script.index("scripts/validate_dashboard.py") < script.index("scripts/finalize_decision_publication.py")
    assert "|| structure_code=$?" in script and "|| code=$?" in script


@pytest.mark.parametrize("structure_exit,write_report", [(0, True), (2, True), (3, False)])
def test_executed_final_shell_always_reaches_one_finalizer(tmp_path, structure_exit, write_report):
    binary = tmp_path / "bin"
    binary.mkdir()
    shim = binary / "python"
    shim.write_text(f"#!{sys.executable}\n" + f"import sys; sys.path.insert(0,{str(ROOT)!r})\n" + textwrap.dedent('''\
        import json,os
        from pathlib import Path
        from datetime import datetime,timedelta,timezone
        if sys.argv[1] == 'scripts/validate_dashboard.py':
            if os.environ['WRITE_REPORT'] == '1':
                pages = sys.argv[sys.argv.index('--pages')+1:sys.argv.index('--json-out')]
                checks = []
                for page in pages:
                    names = ('decision.header_found','decision.feed_banner_present','decision.sizing_withheld_when_blocked') if page == 'Top_10_Investments' else ('contract.header_match','scope.coverage')
                    checks.extend({'page':page,'name':name,'status':'FAIL' if os.environ['STRUCTURE_EXIT']=='2' and name=='decision.header_found' else 'PASS','detail':'FEED NOT ACTIONABLE — hold' if name=='decision.feed_banner_present' else ''} for name in names)
                report = {'meta':{'reader':'read_range','generated_riyadh':datetime.now(timezone(timedelta(hours=3))).strftime('%Y-%m-%d %H:%M:%S')},'results':checks}
                Path('final_dashboard_validation.json').write_text(json.dumps(report))
            raise SystemExit(int(os.environ['STRUCTURE_EXIT']))
        if sys.argv[1] == 'scripts/workflow_validation_scope.py':
            from scripts.workflow_validation_scope import main
            raise SystemExit(main(sys.argv[2:]))
        if sys.argv[1] == 'scripts/finalize_decision_publication.py':
            from scripts.finalize_decision_publication import decision_structure
            call = Path(os.environ['FINALIZER_CALL'])
            assert not call.exists(), 'finalizer must be invoked exactly once'
            call.write_text(json.dumps(sys.argv[1:]))
            path = sys.argv[sys.argv.index('--decision-structure-report')+1]
            result = decision_structure(path,os.environ['TARGET_SHEET_ID'],os.environ['GITHUB_RUN_ID'])
            Path('final_decision_publication.json').write_text(json.dumps(result))
            raise SystemExit(result['exit_code'])
        raise SystemExit(99)
    '''))
    shim.chmod(0o755)
    # A previous PASS must be removed even when this validation crashes.
    (tmp_path / "final_dashboard_validation.json").write_text('{"previous":"PASS"}')
    env = {"PATH": str(binary)+os.pathsep+os.defpath, "TARGET_SHEET_ID": "synthetic-sheet",
           "GITHUB_RUN_ID": "90001", "GITHUB_SHA": SHA,
           "GITHUB_OUTPUT": str(tmp_path/"outputs"), "FINALIZER_CALL": str(tmp_path/"call.json"),
           "STRUCTURE_EXIT": str(structure_exit), "WRITE_REPORT": "1" if write_report else "0"}
    job = job_source(workflow(), "verify-final-decision-publication")
    script = shell_source(step_source(job, "Audit final coverage and decision clocks"))
    result = subprocess.run(["bash","-c",script], cwd=tmp_path, env=env,
                            text=True,capture_output=True,timeout=10)
    assert result.returncode == structure_exit, result.stderr
    assert (tmp_path/"call.json").is_file()
    assert (tmp_path/"outputs").read_text() == f"audit_exit={structure_exit}\n"

