"""G10 (2026-10-17): acceptance_check.yml surfaces the PASS/WARN/FAIL/NA tally
without opening the artifact, while staying a non-strict instrument.

Pins: the headline step exists, runs `if: always()`, reads
artifacts/acceptance.json, writes one `## 📏 Acceptance: ...` heading to the
step summary, emits notice/warning/error by verdict (error on a missing
result), exports `verdict` and `tally`, and never exits non-zero. The measure
step and the strict input are unchanged; the workflow still writes no cell.
"""
from __future__ import annotations

import json
import os
from pathlib import Path
import subprocess

import pytest
import yaml

ROOT = Path(__file__).resolve().parents[1]
WF = ROOT / ".github/workflows/acceptance_check.yml"


def _wf():
    return yaml.load(WF.read_text(encoding="utf-8"), Loader=yaml.BaseLoader)


def _steps():
    return _wf()["jobs"]["acceptance"]["steps"]


def test_headline_step_sits_after_the_measurement_and_always_runs():
    names = [s.get("name", "") for s in _steps()]
    i_measure = names.index("Measure the live workbook (read-only)")
    i_head = names.index("Acceptance headline (tally without opening the artifact)")
    i_upload = names.index("Upload acceptance artifact")
    assert i_measure < i_head < i_upload
    head = _steps()[i_head]
    assert head["id"] == "headline" and head["if"] == "always()"
    # the instrument stays non-strict: the measure step is untouched
    measure = _steps()[i_measure]
    assert 'if [ "${{ inputs.strict }}" = "true" ]; then ARGS+=(--strict); fi' in measure["run"]
    assert _wf()["on"]["workflow_dispatch"]["inputs"]["strict"]["default"] == "false"
    text = WF.read_text(encoding="utf-8")
    assert "(2026-10-17) G10" in text and "v1.0.0" not in text


def _run_headline(tmp_path, payload):
    if payload is not None:
        (tmp_path / "artifacts").mkdir()
        (tmp_path / "artifacts" / "acceptance.json").write_text(json.dumps(payload), encoding="utf-8")
    env = {"PATH": os.environ["PATH"], "GITHUB_STEP_SUMMARY": str(tmp_path / "summary"),
           "GITHUB_OUTPUT": str(tmp_path / "outputs")}
    (tmp_path / "summary").write_text(""); (tmp_path / "outputs").write_text("")
    head = [s for s in _steps() if s.get("id") == "headline"][0]
    run = subprocess.run(["bash", "-c", head["run"]], cwd=tmp_path, env=env, capture_output=True, text=True)
    assert run.returncode == 0, run.stdout + run.stderr
    outputs = dict(l.split("=", 1) for l in (tmp_path / "outputs").read_text().splitlines())
    return run.stdout, (tmp_path / "summary").read_text(), outputs


REVIEW_TALLY = {"overall_verdict": "FAIL", "tally": {"PASS": 28, "WARN": 6, "FAIL": 1, "NA": 7},
                "na_hard": 0, "na_design": 7, "read_errors": []}


def test_fail_tally_becomes_an_error_annotation_and_a_heading(tmp_path):
    out, summary, outputs = _run_headline(tmp_path, REVIEW_TALLY)
    line = "Acceptance: FAIL - PASS=28 WARN=6 FAIL=1 NA=7 (NA_hard=0 NA_design=7)"
    assert out.startswith("::error::" + line)
    assert "non-strict instrument: the job stays green" in out
    assert summary == "## 📏 %s\n\n" % line
    assert outputs == {"verdict": "FAIL", "tally": "PASS=28 WARN=6 FAIL=1 NA=7 (NA_hard=0 NA_design=7)"}


@pytest.mark.parametrize("verdict,kind", (("PASS", "notice"), ("WARN", "warning"), ("FAIL", "error"), ("weird", "error")))
def test_annotation_kind_follows_the_verdict(tmp_path, verdict, kind):
    out, _summary, outputs = _run_headline(tmp_path, {**REVIEW_TALLY, "overall_verdict": verdict})
    assert out.startswith("::%s::Acceptance: %s - " % (kind, verdict))
    assert outputs["verdict"] == verdict


def test_read_errors_are_counted_in_the_line(tmp_path):
    out, _s, outputs = _run_headline(tmp_path, {**REVIEW_TALLY, "read_errors": ["Top_10: 403", "_Status: 403"]})
    assert "read_errors=2" in out and outputs["tally"].endswith("read_errors=2")


def test_missing_result_is_loud_but_does_not_fail_the_job(tmp_path):
    out, summary, outputs = _run_headline(tmp_path, None)
    assert out.startswith("::error::Acceptance: NO RESULT - artifacts/acceptance.json missing")
    assert summary.startswith("## 📏 Acceptance: NO RESULT")
    assert outputs["verdict"] == "NO RESULT"
