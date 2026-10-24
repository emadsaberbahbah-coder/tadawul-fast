"""Failing operational controls need actionable evidence, not automatic acceptance."""
from __future__ import annotations

import copy
import json
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path
import subprocess
import sys
import types

import pytest
import yaml

from scripts import audit_provider_target_coverage as current_ptc
from scripts import renew_verified_name_labels as current_names

ROOT = Path(__file__).resolve().parents[1]
REF = os.environ.get("TFB_MONITOR_DIAGNOSTIC_REF")


def module(name, current):
    if not REF:
        return current
    loaded = types.ModuleType("monitor_baseline_" + name)
    loaded.__file__ = current.__file__
    sys.modules[loaded.__name__] = loaded
    source = subprocess.run(["git", "show", REF + ":scripts/" + name + ".py"],
                            cwd=ROOT, check=True, capture_output=True, text=True).stdout
    exec(compile(source, loaded.__file__, "exec"), loaded.__dict__)
    return loaded


ptc = module("audit_provider_target_coverage", current_ptc)
names = module("renew_verified_name_labels", current_names)
NOW = datetime(2026, 10, 24, 8, tzinfo=timezone.utc)


def grid():
    return [["Symbol", "Forecast Source"]] + [[f"S{i}", "provider_target"] for i in range(60)]


def baseline(cfg, *, pages=None):
    return {"schema": ptc.STATE_SCHEMA, "sheet": "***", "config_hash": cfg.hash(),
            "generated_at_utc": datetime.now(timezone.utc).isoformat(),
            "pages": {"GM": {"share_pct": 100, "rows": 60, "provider": 60}} if pages is None else pages}


@pytest.mark.parametrize("empty", [False, True])
def test_empty_or_missing_reference_keeps_failure_and_proposes_review_only(tmp_path, empty):
    cfg = ptc.Config()
    state = baseline(cfg, pages={}) if empty else None
    before = copy.deepcopy(state)
    report = ptc.audit_pages({"GM": grid()}, state, cfg)
    recovery = report.payload()["recovery"]
    assert report.exit_code == 2
    assert recovery["action"] == "REVIEW_EXPLICIT_BOOTSTRAP"
    assert recovery["automatic"] is recovery["baseline_changed_by_guidance"] is False
    assert "verified" in recovery["detail"] or "healthy" in recovery["detail"]
    assert "accept_baseline=false" in recovery["detail"]
    target = tmp_path / "reference.json"
    assert ptc.save_last_good(str(target), report, "***") == "kept: control-health failure"
    assert not target.exists() and state == before


def test_unreadable_current_measurement_does_not_offer_bootstrap():
    report = ptc.audit_pages({"GM": []}, None, ptc.Config(), read_errors={"GM": "timeout"})
    assert report.payload()["recovery"]["action"] == "REPAIR_CONTROL_EVIDENCE"
    assert report.exit_code == 2


def test_wrong_reference_requires_restore_even_when_pages_have_no_matching_baseline():
    cfg = ptc.Config()
    state = baseline(cfg)
    state["config_hash"] = "wrong-policy"
    report = ptc.audit_pages({"GM": grid()}, state, cfg)
    assert report.payload()["recovery"]["action"] == "RESTORE_OR_REVIEW_REFERENCE"
    assert report.exit_code == 2


def test_coverage_failure_takes_priority_over_missing_reference():
    cfg = ptc.Config(floor_pct=90)
    rows = grid()
    for row in rows[1:]:
        row[1] = "phase_ii_synthetic"
    report = ptc.audit_pages({"GM": rows}, None, cfg)
    assert report.payload()["recovery"]["action"] == "INVESTIGATE_COVERAGE"
    assert report.exit_code == 2


def test_healthy_report_has_no_recovery_and_freeze_still_prevents_write(monkeypatch, tmp_path):
    cfg = ptc.Config()
    report = ptc.audit_pages({"GM": grid()}, baseline(cfg), cfg)
    assert report.payload()["recovery"]["action"] == "NONE" and report.exit_code == 0
    monkeypatch.setenv("TFB_PTC_FREEZE", "1")
    target = tmp_path / "reference.json"
    assert ptc.save_last_good(str(target), report, "***") == "kept: frozen by TFB_PTC_FREEZE"
    assert not target.exists()


def test_provider_summary_exposes_actual_control_reason_without_changing_payload():
    report = ptc.audit_pages({"GM": grid()}, None, ptc.Config())
    payload = report.payload()
    before = copy.deepcopy(payload)
    summary = ptc.render_step_summary(payload)
    assert "CONTROL FAILURE" in summary and "CH_BASELINE_MISSING" in summary
    assert "REVIEW_EXPLICIT_BOOTSTRAP" in summary and "No automatic baseline acceptance" in summary
    assert payload == before


@pytest.mark.parametrize("code", [None, False, True, "0", -1])
def test_malformed_provider_exit_is_unknown_not_a_pass(code):
    assert "Result: **UNKNOWN**" in ptc.render_step_summary({"exit_code": code})


def registry():
    return {"bindings": [
        {"page": "Global_Markets", "symbol": "EXPIRED.US", "expires_at_utc": NOW.isoformat()},
        {"page": "Global_Markets", "symbol": "SOON.US", "expires_at_utc": (NOW + timedelta(hours=24)).isoformat()},
        {"page": "Mutual_Funds", "symbol": "FRESH.US", "expires_at_utc": (NOW + timedelta(days=7)).isoformat()},
    ]}


def test_expiry_check_distinguishes_past_and_future_warning_without_extending_dates():
    source = registry()
    before = copy.deepcopy(source)
    report = names.expiry_report(source, now=NOW, within=timedelta(hours=48))
    assert report["expired"] == ["Global_Markets|EXPIRED.US"]
    assert report["due_soon"] == ["Global_Markets|SOON.US"]
    assert report["status"] == "EXPIRED"
    assert report["automatic_renewal"] is report["registry_changed"] is False
    assert source == before
    text = names.render_step_summary(report)
    assert "EXPIRED.US | EXPIRED" in text and "SOON.US | DUE SOON" in text
    assert "mode=renew" in text and "never extend" in text


def test_clear_and_due_soon_statuses_preserve_48_hour_policy():
    source = registry()
    source["bindings"] = source["bindings"][1:]
    assert names.expiry_report(source, now=NOW, within=timedelta(hours=48))["status"] == "DUE_SOON"
    assert names.expiry_report(source, now=NOW, within=timedelta(hours=1))["status"] == "CLEAR"


def test_partial_renewal_is_not_described_as_complete_or_accepted():
    report = {"renewed": ["GM|A.US"], "held": {"GM|OLD.US": "fetch_failed:HTTPError"}}
    before = copy.deepcopy(report)
    text = names.render_step_summary(report)
    assert "PARTIAL" in text and "OLD.US" in text and "fetch_failed:HTTPError" in text
    assert "candidate, not an accepted authority review" in text
    assert report == before


@pytest.mark.parametrize("report", [{}, {"renewed": []}, {"expiring": "bad"}])
def test_missing_name_measurement_is_unknown_not_renewed(report):
    text = names.render_step_summary(report)
    assert "UNKNOWN" in text and "Status: **RENEWED**" not in text


def test_check_cli_retains_failure_and_emits_expired_recovery(tmp_path, monkeypatch):
    source = registry()
    path, report = tmp_path / "names.json", tmp_path / "report.json"
    path.write_text(json.dumps(source))
    before = path.read_bytes()
    monkeypatch.setattr(names, "http_fetch", lambda *a, **kw: pytest.fail("check must not fetch sources"))
    assert names.main(["--check", "--registry", str(path), "--report", str(report)]) == 1
    saved = json.loads(report.read_text())
    assert saved["expired"] and saved["status"] == "EXPIRED"
    assert path.read_bytes() == before


def workflow(path):
    if REF:
        return yaml.safe_load(subprocess.run(["git", "show", REF + ":" + path], cwd=ROOT,
                                            capture_output=True, text=True, check=True).stdout)
    return yaml.safe_load((ROOT / path).read_text())


def test_workflow_preserves_red_audits_and_always_explains_both_recovery_paths():
    provider = workflow(".github/workflows/provider_target_coverage.yml")["jobs"]["audit"]["steps"]
    audit = next(step for step in provider if step.get("name") == "Audit provider-target coverage")
    assert audit["env"]["GOOGLE_SHEETS_CREDENTIALS_B64"] == "${{ secrets.GOOGLE_SHEETS_CREDENTIALS_B64 }}"
    assert not audit.get("continue-on-error", False)
    explain = next(step for step in provider if step.get("name") == "Explain audit recovery without accepting a baseline")
    assert explain["if"] == "always()"
    assert "render_step_summary" in explain["run"] and "--bootstrap" not in explain["run"]
    steps = workflow(".github/workflows/name_label_renewal.yml")["jobs"]["name-label-renewal"]["steps"]
    explain = next(step for step in steps if step.get("name") == "Explain expired or held authority evidence")
    assert explain["if"] == "always()" and "render_step_summary" in explain["run"]


@pytest.mark.parametrize("kind,present", [("provider", True), ("provider", False),
                                          ("names", True), ("names", False)])
def test_actual_workflow_summary_programs_run_without_live_credentials(tmp_path, kind, present):
    if kind == "provider":
        steps = workflow(".github/workflows/provider_target_coverage.yml")["jobs"]["audit"]["steps"]
        step = next(s for s in steps if s.get("name") == "Explain audit recovery without accepting a baseline")
        report = ptc.audit_pages({"GM": grid()}, None, ptc.Config()).payload()
        filename = "provider_target_coverage.json"
        expected = "CONTROL FAILURE"
    else:
        steps = workflow(".github/workflows/name_label_renewal.yml")["jobs"]["name-label-renewal"]["steps"]
        step = next(s for s in steps if s.get("name") == "Explain expired or held authority evidence")
        report = names.expiry_report(registry(), now=NOW, within=timedelta(hours=48))
        filename = "renewal_report.json"
        expected = "EXPIRED"
    if present:
        (tmp_path / filename).write_text(json.dumps(report))
    summary = tmp_path / "summary.md"
    env = dict(os.environ, PYTHONPATH=str(ROOT), GITHUB_STEP_SUMMARY=str(summary),
               PATH=str(Path(sys.executable).parent) + os.pathsep + os.environ["PATH"])
    result = subprocess.run(["bash", "-eo", "pipefail", "-c", step["run"]],
                            cwd=tmp_path, env=env, capture_output=True, text=True, timeout=10)
    assert result.returncode == 0, result.stderr
    assert (expected if present else "UNKNOWN: no report was produced") in summary.read_text()
    if not present:
        assert "\n\nUNKNOWN:" in summary.read_text()
    assert not (tmp_path / "provider_target_last_good.json").exists()
