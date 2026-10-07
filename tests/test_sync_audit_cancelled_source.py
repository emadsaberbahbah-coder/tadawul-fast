#!/usr/bin/env python3
"""Harness: Sync Outcome Audit source gate for 'cancelled' Daily Sync runs.

WHY (2026-10-07): scheduled Daily Sync 37612073997 refreshed every market page
(all matrix legs and recovery 'success') but concluded 'cancelled' because its
unneeded 'Run verdict' job was cancelled when a pending workflow_dispatch run
took the production-write lease. The audit's "Resolve source run" step refused
'cancelled' outright. The step now audits a cancelled run only when the repo
variable TFB_SYNC_AUDIT_ACCEPT_CANCELLED=1; default OFF must be identical.

This harness extracts the real step script from the workflow file and runs it
under bash, so it tests what GitHub executes. Runs as a script ("PASS k/k")
and under pytest. Golden negative: point TFB_SOA_YAML at a base copy of the
workflow (git show <base>:.github/workflows/sync_outcome_audit.yml); the
gate-ON cases must then fail.
"""
from __future__ import annotations

import os
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from scripts import audit_sync_outcome as soa  # noqa: E402

HARNESS_VERSION = (1, 0, 0)
WORKFLOW = Path(os.getenv("TFB_SOA_YAML") or ROOT / ".github/workflows/sync_outcome_audit.yml")
STEP_NAME = "- name: Resolve source run"
REFUSE_FMT = "::error::Source Daily Sync run concluded '{}'."


def _extract_step_script(path: Path = WORKFLOW) -> str:
    """Return the dedented `run: |` body of the 'Resolve source run' step."""
    lines = path.read_text(encoding="utf-8").splitlines()
    start = next(i for i, line in enumerate(lines) if line.strip() == STEP_NAME)
    run_idx = next(i for i in range(start + 1, len(lines)) if lines[i].strip() == "run: |")
    body: list[str] = []
    indent = None
    for line in lines[run_idx + 1:]:
        if line.strip() == "":
            body.append("")
            continue
        cur = len(line) - len(line.lstrip(" "))
        if indent is None:
            indent = cur
        if cur < indent:
            break
        body.append(line[indent:])
    script = "\n".join(body).rstrip() + "\n"
    assert "${{" not in script, "step script must only read env vars"
    return script


def _run(event: str, conclusion: str, gate: str | None, run_id: str = "37612073997",
         input_run_id: str = "") -> tuple[int, str, str]:
    bash = shutil.which("bash")
    assert bash, "bash is required"
    with tempfile.TemporaryDirectory() as tmp:
        out = Path(tmp) / "github_output"
        out.write_text("", encoding="utf-8")
        env = {
            "PATH": os.environ.get("PATH", "/usr/bin:/bin"),
            "EVENT_NAME": event,
            "WORKFLOW_RUN_ID": run_id,
            "WORKFLOW_CONCLUSION": conclusion,
            "INPUT_RUN_ID": input_run_id,
            "GITHUB_OUTPUT": str(out),
        }
        if gate is not None:
            env["TFB_SYNC_AUDIT_ACCEPT_CANCELLED"] = gate
        proc = subprocess.run([bash, "-c", _extract_step_script()], env=env,
                              capture_output=True, text=True, timeout=30)
        return proc.returncode, proc.stdout, out.read_text(encoding="utf-8")


# --- default OFF: frozen pre-change contract (exit, stdout, GITHUB_OUTPUT) ---

def _assert_base_contract(gate: str | None) -> None:
    for conclusion in ("success", "failure"):
        rc, so, go = _run("workflow_run", conclusion, gate)
        assert rc == 0, (conclusion, gate, rc, so)
        assert so == (f"Upstream Daily Sync concluded '{conclusion}' - auditing.\n"
                      "Auditing Daily Sync run 37612073997\n"), so
        assert go == "run_id=37612073997\n", go
    for conclusion in ("cancelled", "skipped", "timed_out", "action_required", ""):
        rc, so, go = _run("workflow_run", conclusion, gate)
        assert rc == 1, (conclusion, gate, rc)
        assert so == REFUSE_FMT.format(conclusion) + "\n", so
        assert go == "", go


def test_gate_absent_is_pre_change_contract():
    _assert_base_contract(None)


def test_gate_zero_is_pre_change_contract():
    _assert_base_contract("0")


def test_gate_non_one_values_stay_off():
    for gate in ("", "true", "yes", "on", "01", " 1", "2"):
        rc, so, go = _run("workflow_run", "cancelled", gate)
        assert rc == 1 and so == REFUSE_FMT.format("cancelled") + "\n" and go == "", (gate, rc, so)


# --- gate ON -----------------------------------------------------------------

def test_gate_on_audits_cancelled_run():
    rc, so, go = _run("workflow_run", "cancelled", "1")
    assert rc == 0, (rc, so)
    assert "concluded 'cancelled' - auditing uploaded evidence" in so, so
    assert "Auditing Daily Sync run 37612073997" in so, so
    assert go == "run_id=37612073997\n", go


def test_gate_on_still_refuses_skipped_and_timed_out():
    for conclusion in ("skipped", "timed_out", "action_required", ""):
        rc, so, go = _run("workflow_run", conclusion, "1")
        assert rc == 1 and so == REFUSE_FMT.format(conclusion) + "\n" and go == "", (conclusion, rc, so)


def test_gate_on_leaves_success_and_failure_unchanged():
    for conclusion in ("success", "failure"):
        assert _run("workflow_run", conclusion, "1") == _run("workflow_run", conclusion, None)


def test_gate_on_still_validates_run_id():
    rc, so, go = _run("workflow_run", "cancelled", "1", run_id="12x")
    assert rc == 1 and "::error::Invalid workflow run id: '12x'" in so and go == "", (rc, so)


def test_manual_dispatch_path_unaffected_by_gate():
    for gate in (None, "1"):
        rc, so, go = _run("workflow_dispatch", "", gate, run_id="", input_run_id="555")
        assert rc == 0 and go == "run_id=555\n" and so == "Auditing Daily Sync run 555\n", (gate, rc, so)


# --- the auditor behind the gate still fails closed ---------------------------

def test_cancelled_run_without_logs_fails_closed():
    with tempfile.TemporaryDirectory() as tmp:
        assert soa.main(["--root", tmp]) == 3


def test_cancelled_run_with_missing_page_fails_closed():
    with tempfile.TemporaryDirectory() as tmp:
        art = Path(tmp) / "tadawul-sync-logs-37612073997-core-pages"
        art.mkdir()
        lines = [f"[PAGE-VERDICT v6.26.0] page={p} status=success rows_written=10"
                 for p in soa.CRITICAL_MARKET_PAGES[:-1]]
        (art / "sync_execution.log").write_text("\n".join(lines) + "\n", encoding="utf-8")
        assert soa.main(["--root", tmp]) == 2


def test_version_floor():
    assert HARNESS_VERSION >= (1, 0, 0)
    assert tuple(int(x) for x in soa.SCRIPT_VERSION.split(".")) >= (1, 1, 2)


TESTS = [obj for name, obj in sorted(globals().items()) if name.startswith("test_") and callable(obj)]


def main() -> int:
    passed = 0
    for fn in TESTS:
        try:
            fn()
            passed += 1
            print(f"ok   {fn.__name__}")
        except Exception as exc:  # noqa: BLE001
            print(f"FAIL {fn.__name__}: {exc!r}")
    total = len(TESTS)
    print(f"{'PASS' if passed == total else 'FAIL'} {passed}/{total}")
    return 0 if passed == total else 1


if __name__ == "__main__":
    raise SystemExit(main())
