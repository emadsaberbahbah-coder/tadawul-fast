"""Execute CI's actual verdict shell with injected job outcomes (zero network)."""

from __future__ import annotations

import re
import subprocess
import textwrap
from pathlib import Path

import pytest


WORKFLOW = Path(__file__).resolve().parents[1] / ".github/workflows/ci.yml"
REQUIRED_JOBS = ("compile", "lean-unit", "contract", "heavy")


def _job_block(workflow: str, job_id: str) -> str:
    """Read a top-level job without requiring PyYAML in the lean CI image."""
    match = re.search(
        rf"^  {re.escape(job_id)}:\n(?P<body>.*?)(?=^  [\w-]+:\n|\Z)",
        workflow,
        re.MULTILINE | re.DOTALL,
    )
    assert match is not None, f"Missing CI job: {job_id}"
    return match.group("body")


def _verdict_script() -> str:
    summary = _job_block(WORKFLOW.read_text(), "summary")
    run_block = re.search(r"^        run: \|\n(?P<body>(?:^          .*\n?)+)", summary, re.MULTILINE)
    assert run_block is not None, "Required verdict must have an executable run block"
    return textwrap.dedent(run_block.group("body"))


def _run_verdict(results: dict[str, str]) -> subprocess.CompletedProcess[str]:
    script = re.sub(
        r"\$\{\{\s*needs\.([\w-]+)\.result\s*\}\}",
        lambda match: results[match.group(1)],
        _verdict_script(),
    )
    assert "${{" not in script, "Unrendered expression in verdict witness"
    return subprocess.run(
        ["bash", "--noprofile", "--norc", "-eo", "pipefail", "-c", script],
        capture_output=True,
        text=True,
        check=False,
        timeout=5,
    )


def test_required_verdict_waits_for_every_critical_job() -> None:
    workflow = WORKFLOW.read_text()
    summary = _job_block(workflow, "summary")
    needs = re.search(r"^    needs: \[([^\]]+)\]$", summary, re.MULTILINE)
    assert needs is not None
    assert set(REQUIRED_JOBS).issubset({job.strip() for job in needs.group(1).split(",")})
    assert re.search(r"^    if: always\(\)$", summary, re.MULTILINE)
    engine = _job_block(workflow, "heavy")
    assert not re.search(r"^\s+continue-on-error:", engine, re.MULTILINE)
    assert "python -m pytest -q tests/test_data_engine_v2.py" in engine
    assert "tests/test_ci_required_verdict.py" in _job_block(workflow, "lean-unit")


def test_all_critical_jobs_success_produces_clean_verdict() -> None:
    witness = _run_verdict(dict.fromkeys(REQUIRED_JOBS, "success"))
    assert witness.returncode == 0, witness.stderr
    assert "VERDICT: CLEAN" in witness.stdout


@pytest.mark.parametrize("job_id", REQUIRED_JOBS)
@pytest.mark.parametrize("outcome", ("failure", "cancelled", "skipped"))
def test_any_non_success_critical_job_fails_required_verdict(job_id: str, outcome: str) -> None:
    results = dict.fromkeys(REQUIRED_JOBS, "success")
    results[job_id] = outcome
    witness = _run_verdict(results)
    assert witness.returncode == 1, witness.stderr
    assert "VERDICT: FAIL" in witness.stdout
    assert "VERDICT: CLEAN" not in witness.stdout
