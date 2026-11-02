"""Action summaries retain the genuine failure and identify its owning evidence."""
import json
import os
from pathlib import Path
import subprocess

import pytest
import yaml

from scripts import render_audit_summary as summary


def test_real_postmerge_failure_families_are_actionable_and_still_fail():
    reports = [
        ("full_refresh", {"summary": {"exit_code": 2}, "pages": [
            {"page": "My_Portfolio", "failures": ["acquired timestamp stale"]}]}, 2),
        ("decision_surface", {"summary": {"exit_code": 2}, "findings": [
            {"severity": "FAIL", "code": "PF_SOURCE_STALE", "surface": "Portfolio_Decision",
             "message": "source exceeds 8 hours"}]}, 2),
        ("provider_target", {"exit_code": 2, "findings": [
            {"severity": "CONTROL", "code": "CH_BASELINE_EMPTY", "page": "*",
             "message": "no accepted reference"}]}, 2),
    ]
    text = summary.render(reports)
    assert text.count("| 2 | FAIL |") == 3
    assert "PF_SOURCE_STALE" in text and "CH_BASELINE_EMPTY" in text
    assert "Reacquire My_Portfolio" in text and "do not auto-accept" in text


@pytest.mark.parametrize("code,process,state", [(0, 0, "PASS"), (1, 1, "WARN"),
                                              (2, 2, "FAIL"), (3, 3, "FAIL"),
                                              (0, 2, "UNKNOWN"), (True, 0, "UNKNOWN"),
                                              (None, 0, "UNKNOWN")])
def test_exit_and_process_evidence_must_agree(code, process, state):
    actual, _ = summary.report_rows("audit", {"exit_code": code}, process)
    assert actual == state


def test_explicit_failure_cannot_be_hidden_by_passing_summary():
    state, _ = summary.report_rows("audit", {"exit_code": 0, "findings": [
        {"severity": "FAIL", "code": "SOURCE_BAD"}]}, 0)
    assert state == "FAIL"


@pytest.mark.parametrize("report", [None, [], {"summary": []}, {"findings": {}}, {"pages": "bad"}])
def test_malformed_report_shapes_remain_unknown(report):
    assert summary.report_rows("audit", report, 0)[0] == "UNKNOWN"
    assert "UNKNOWN" in summary.render([("audit", report, 0)])


def test_valid_full_and_final_reports_round_trip_without_writes_to_evidence(tmp_path):
    root = tmp_path / "reports"
    root.mkdir()
    entries = [{"audit": name, "exit_code": 0, "report_present": True, "passed": True} for name in
               ("full_refresh", "decision_surface", "provider_target")]
    (root / "verification.json").write_text(json.dumps({"results": entries}))
    for entry in entries:
        (root / (entry["audit"] + ".json")).write_text('{"exit_code":0}')
    before = {path.name: path.read_bytes() for path in root.iterdir()}
    assert summary.main(["--evidence-root", str(root)]) == 0
    assert before == {path.name: path.read_bytes() for path in root.iterdir()}
    final = tmp_path / "final.json"
    final.write_text(json.dumps({"audit_exit_codes": [0, 2], "audits": {
        "coverage": {"summary": {"exit_code": 0}}, "decision": {"summary": {"exit_code": 2}}}}))
    assert summary.main(["--final-report", str(final)]) == 0  # renderer, not acceptance


@pytest.mark.parametrize("raw", ['{}', '{"audits":{},"audit_exit_codes":[0,0]}',
                                  '{"value":1e500,"audits":{},"audit_exit_codes":[0,0]}',
                                  '{"audits":{},"audits":{},"audit_exit_codes":[0,0]}'])
def test_missing_duplicate_or_incomplete_evidence_is_unknown(tmp_path, raw, capsys):
    path = tmp_path / "bad.json"
    path.write_text(raw)
    assert summary.main(["--final-report", str(path)]) == 1
    assert "UNKNOWN" in capsys.readouterr().out


def test_missing_report_is_not_success_and_cannot_escape_root(tmp_path):
    entries = [{"audit": name, "exit_code": 0, "report_present": True, "passed": True} for name in
               ("full_refresh", "decision_surface", "provider_target")]
    (tmp_path / "verification.json").write_text(json.dumps({"results": entries}))
    assert "Audit report missing" in summary.render(summary.load_reports(evidence_root=tmp_path))
    entries[0]["audit"] = "../../secret"
    (tmp_path / "verification.json").write_text(json.dumps({"results": entries}))
    with pytest.raises(ValueError):
        summary.load_reports(evidence_root=tmp_path)


@pytest.mark.parametrize("damage", ["missing_exit", "null_exit", "bool_exit", "missing_report_receipt",
                                    "report_not_present", "missing_passed", "contradictory_passed"])
def test_incomplete_process_receipts_cannot_certify_retained_reports(tmp_path, damage):
    entries = [{"audit": name, "exit_code": 0, "report_present": True, "passed": True} for name in
               ("full_refresh", "decision_surface", "provider_target")]
    for entry in entries:
        (tmp_path / (entry["audit"] + ".json")).write_text('{"exit_code":0}')
    target = entries[0]
    if damage == "missing_exit": target.pop("exit_code")
    if damage == "null_exit": target["exit_code"] = None
    if damage == "bool_exit": target["exit_code"] = False
    if damage == "missing_report_receipt": target.pop("report_present")
    if damage == "report_not_present": target["report_present"] = False
    if damage == "missing_passed": target.pop("passed")
    if damage == "contradictory_passed": target["passed"] = False
    (tmp_path / "verification.json").write_text(json.dumps({"results": entries}))
    assert "| full_refresh | 0 | UNKNOWN |" in summary.render(summary.load_reports(evidence_root=tmp_path))


def test_markdown_and_message_bounds():
    text = summary.render([("audit", {"exit_code": 2, "findings": [
        {"severity": "FAIL", "message": "bad|field\n" + "x" * 1000}]}, 2)])
    assert "bad / field " in text
    assert "x" * 501 not in text


def test_actual_postmerge_wrapper_retains_failure_and_explains_the_owner(tmp_path, monkeypatch):
    root = Path(__file__).resolve().parents[1]
    workflow = root / ".github/workflows/workflow_repair_tests.yml"
    if os.environ.get("TFB_AUDIT_SUMMARY_WORKFLOW_REF"):
        source = subprocess.run(["git", "show", os.environ["TFB_AUDIT_SUMMARY_WORKFLOW_REF"] +
                                 ":.github/workflows/workflow_repair_tests.yml"],
                                cwd=root, capture_output=True, text=True, check=True).stdout
    else:
        source = workflow.read_text()
    steps = yaml.safe_load(source)["jobs"]["verify-live"]["steps"]
    inline = next(step["run"] for step in steps if step.get("name") ==
                  "Verify live controls and retain genuine failures")
    code = inline.split("python - <<'PY'\n", 1)[1].rsplit("\nPY", 1)[0]
    actual_run = subprocess.run

    def fake_audit(command, **kwargs):
        if command[1].endswith("render_audit_summary.py"):
            return actual_run([command[0], str(root / command[1]), *command[2:]], **kwargs)
        report = Path(command[command.index("--json-out") + 1])
        report.write_text(json.dumps({"exit_code": 2, "findings": [
            {"severity": "FAIL", "code": "PF_SOURCE_STALE", "surface": "Portfolio_Decision",
             "message": "source exceeds 8 hours"}]}))
        return type("Outcome", (), {"returncode": 2})()

    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("DEFAULT_SPREADSHEET_ID", "synthetic")
    monkeypatch.setenv("GITHUB_SHA", "a" * 40)
    monkeypatch.setenv("GITHUB_STEP_SUMMARY", str(tmp_path / "summary.md"))
    monkeypatch.setattr(subprocess, "run", fake_audit)
    with pytest.raises(SystemExit) as result:
        exec(compile(code, str(workflow), "exec"), {})
    assert result.value.code == 1  # display cannot convert failed audits to green
    rendered = (tmp_path / "summary.md").read_text()
    assert "PF_SOURCE_STALE" in rendered and "Reacquire My_Portfolio" in rendered
