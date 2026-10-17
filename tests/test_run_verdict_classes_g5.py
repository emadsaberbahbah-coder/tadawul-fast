"""G5 (2026-10-17): daily_sync.yml v1.0.4 - the 🔔 Run verdict tells a withheld
decision feed (DATA_OK / GATE_NOT_ACTIONABLE) from a data failure (DATA_FAIL)
and from a broken gate (DATA_OK / GATE_ERROR).

Pins (YAML text + the real shells, offline, no webhook):
* the gate job exports `audit_exit` (from the unchanged audit step) and
  `blockers` (from the summary step, now `id: final_summary`);
* the summary step writes `blockers=<csv|none|audit_missing>` to GITHUB_OUTPUT
  and keeps its step-summary table;
* the verdict step's classification, annotation kind (warning vs error), the
  summary rows and the Slack payload text/colour for every class.
"""
from __future__ import annotations

import json
import os
from pathlib import Path
import subprocess

import pytest
import yaml

ROOT = Path(__file__).resolve().parents[1]
DAILY = ROOT / ".github/workflows/daily_sync.yml"
GATE = "verify-final-decision-publication"
VERDICT = "notify-run-verdict"


def _wf():
    return yaml.load(DAILY.read_text(encoding="utf-8"), Loader=yaml.BaseLoader)


def _step(job, name):
    hits = [s for s in _wf()["jobs"][job]["steps"] if name in s.get("name", "")]
    assert len(hits) == 1, (job, name)
    return hits[0]


def _verdict_shell():
    body = _step(VERDICT, "Announce the run verdict")["run"]
    return (body.replace("${{ github.repository }}", "owner/repo")
                .replace("${{ github.run_id }}", "4242")
                .replace("${{ github.event_name }}", "schedule"))


# ---------------------------------------------------------------------------
# static pins
# ---------------------------------------------------------------------------
def test_gate_job_exports_audit_exit_and_blockers():
    gate = _wf()["jobs"][GATE]
    assert gate["outputs"] == {"audit_exit": "${{ steps.final_audit.outputs.audit_exit }}",
                               "blockers": "${{ steps.final_summary.outputs.blockers }}"}
    assert _step(GATE, "Audit final coverage and decision clocks")["id"] == "final_audit"
    summary = _step(GATE, "Publish final audit summary")
    assert summary["id"] == "final_summary" and summary["if"] == "always()"
    verdict = _step(VERDICT, "Announce the run verdict")
    assert verdict["env"]["GATE_AUDIT_EXIT"] == "${{ needs.%s.outputs.audit_exit }}" % GATE
    assert verdict["env"]["GATE_BLOCKERS"] == "${{ needs.%s.outputs.blockers }}" % GATE
    assert verdict["env"]["FINAL_RESULT"] == "${{ needs.%s.result }}" % GATE
    assert "# VERSION 1.0.4 (2026-10-17)" in DAILY.read_text(encoding="utf-8")


# ---------------------------------------------------------------------------
# summary step: blockers output
# ---------------------------------------------------------------------------
@pytest.mark.parametrize("payload,expected", (
    ({"exit_code": 2, "blockers": ["coverage_failed", "decision_failed"]}, "coverage_failed,decision_failed"),
    ({"exit_code": 1, "blockers": []}, "none"),
    ({"exit_code": 0}, "none"),
    (None, "audit_missing"),
))
def test_summary_step_exports_blockers(tmp_path, payload, expected):
    if payload is not None:
        (tmp_path / "final_decision_publication.json").write_text(json.dumps(payload))
    env = {"PATH": os.environ["PATH"], "GITHUB_STEP_SUMMARY": str(tmp_path / "summary"),
           "GITHUB_OUTPUT": str(tmp_path / "outputs")}
    (tmp_path / "summary").write_text(""); (tmp_path / "outputs").write_text("")
    run = subprocess.run(["bash", "-c", _step(GATE, "Publish final audit summary")["run"]],
                         cwd=tmp_path, env=env, capture_output=True, text=True)
    assert run.returncode == 0, run.stderr
    assert (tmp_path / "outputs").read_text() == "blockers=%s\n" % expected
    summary = (tmp_path / "summary").read_text()
    assert summary.startswith("## Final decision publication")
    if payload is None:
        assert "Final audit did not complete" in summary
    else:
        assert "| blockers | %s |" % json.dumps(payload.get("blockers")) in summary


# ---------------------------------------------------------------------------
# verdict step: classification
# ---------------------------------------------------------------------------
CASES = (
    # sync, recovery, final, audit_exit, blockers -> class, annotation
    ("success", "success", "failure", "2", "coverage_failed,decision_failed", "DATA_OK / GATE_NOT_ACTIONABLE", "warning"),
    ("success", "skipped", "failure", "1", "none", "DATA_OK / GATE_NOT_ACTIONABLE", "warning"),
    ("success", "success", "failure", "3", "approved_scope_unavailable", "DATA_OK / GATE_ERROR", "error"),
    ("success", "success", "failure", "", "audit_missing", "DATA_OK / GATE_ERROR", "error"),
    ("success", "success", "cancelled", "", "", "DATA_OK / GATE_ERROR", "error"),
    ("failure", "success", "failure", "2", "coverage_failed", "DATA_FAIL", "error"),
    ("cancelled", "success", "skipped", "", "", "DATA_FAIL", "error"),
    ("success", "failure", "failure", "1", "none", "DATA_FAIL", "error"),
    ("success", "success", "success", "0", "none", "UNCLASSIFIED", "error"),
)


@pytest.mark.parametrize("sync,recovery,final,audit_exit,blockers,klass,kind", CASES)
def test_verdict_classifies_before_announcing(tmp_path, sync, recovery, final, audit_exit, blockers, klass, kind):
    env = {"PATH": os.environ["PATH"], "GITHUB_STEP_SUMMARY": str(tmp_path / "summary"),
           "SYNC_RESULT": sync, "RECOVERY_RESULT": recovery, "FINAL_RESULT": final,
           "GATE_AUDIT_EXIT": audit_exit, "GATE_BLOCKERS": blockers, "SLACK_WEBHOOK_URL": ""}
    (tmp_path / "summary").write_text("")
    run = subprocess.run(["bash", "-c", _verdict_shell()], cwd=tmp_path, env=env, capture_output=True, text=True)
    assert run.returncode == 0, run.stdout + run.stderr
    out = run.stdout
    assert "verdict_class=%s" % klass in out
    assert ("::warning::" in out) is (kind == "warning")
    assert ("::error::" in out) is (kind == "error")
    if kind == "warning":
        assert "the release gate withheld the decision feed - NOT_ACTIONABLE, audit exit %s, blockers: %s" % (audit_exit, blockers) in out
        assert "data-quality verdict, not a run failure" in out
    else:
        assert "Daily Sync did not complete cleanly [%s]" % klass in out
    summary = (tmp_path / "summary").read_text()
    assert "**Verdict class:** `%s`" % klass in summary
    assert "| gate blockers | `%s` |" % (blockers or "unknown") in summary
    assert "| verify-final-decision-publication | `%s` |" % final in summary
    assert "No Slack webhook configured" in out


def test_slack_payload_is_amber_for_a_withheld_feed_and_red_otherwise(tmp_path):
    """Run the shell with a stub curl that records the payload."""
    bindir = tmp_path / "bin"; bindir.mkdir()
    capture = tmp_path / "payload.json"
    (bindir / "curl").write_text('#!/usr/bin/env bash\nwhile [[ $# -gt 0 ]]; do if [[ "$1" == "--data-raw" ]]; then printf "%s" "$2" > "' + str(capture) + '"; shift; fi; shift; done\nexit 0\n')
    (bindir / "curl").chmod(0o755)
    base = {"PATH": str(bindir) + os.pathsep + os.environ["PATH"], "GITHUB_STEP_SUMMARY": str(tmp_path / "summary"),
            "SLACK_WEBHOOK_URL": "https://hooks.invalid/x"}
    for sync, final, audit_exit, blockers, color, text in (
        ("success", "failure", "2", "coverage_failed", "warning", "⚠️ Tadawul Daily Sync: data OK, decision feed NOT ACTIONABLE (gate withheld: coverage_failed)"),
        ("failure", "failure", "2", "coverage_failed", "danger", "❌ Tadawul Daily Sync run did not complete cleanly [DATA_FAIL]"),
        ("success", "failure", "3", "x_unavailable", "danger", "❌ Tadawul Daily Sync run did not complete cleanly [DATA_OK / GATE_ERROR]"),
    ):
        (tmp_path / "summary").write_text("")
        env = base | {"SYNC_RESULT": sync, "RECOVERY_RESULT": "success", "FINAL_RESULT": final,
                      "GATE_AUDIT_EXIT": audit_exit, "GATE_BLOCKERS": blockers}
        run = subprocess.run(["bash", "-c", _verdict_shell()], cwd=tmp_path, env=env, capture_output=True, text=True)
        assert run.returncode == 0, run.stdout + run.stderr
        payload = json.loads(capture.read_text())
        assert payload["text"] == text
        assert payload["attachments"][0]["color"] == color
        fields = {f["title"]: f["value"] for f in payload["attachments"][0]["fields"]}
        assert fields["Gate blockers"] == blockers
        assert fields["Details"] == "<https://github.com/owner/repo/actions/runs/4242|View Logs>"
        assert "Slack notification sent" in run.stdout
