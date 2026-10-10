"""Execute the final publication workflow's real local shell contracts.

Only the Google-facing command is replaced: a Python shim records its argv and
returns a chosen exit code. Credentials, summaries and cleanup use the actual
workflow source, a temporary runner directory and fake service-account data.
"""
from __future__ import annotations

import ast
import base64
import itertools
import json
import os
from pathlib import Path
import re
import stat
import subprocess
import sys
import tempfile
import textwrap
from types import SimpleNamespace
import unittest


ROOT = Path(__file__).resolve().parents[1]
WORKFLOW = ROOT / ".github/workflows/daily_sync.yml"
FINAL_JOB = "verify-final-decision-publication"


def job_source(text: str, name: str) -> str:
    match = re.search(r"^  " + re.escape(name) + r":\s*$", text, re.MULTILINE)
    if not match:
        raise AssertionError(f"Missing workflow job: {name}")
    end = re.search(r"^  [a-zA-Z0-9_-]+:\s*$", text[match.end():], re.MULTILINE)
    return text[match.end():match.end() + end.start()] if end else text[match.end():]


def step_source(job: str, name: str) -> str:
    match = re.search(r"^      - name: " + re.escape(name) + r"\s*$", job, re.MULTILINE)
    if not match:
        raise AssertionError(f"Missing workflow step: {name}")
    end = re.search(r"^      - ", job[match.end():], re.MULTILINE)
    return job[match.end():match.end() + end.start()] if end else job[match.end():]


def shell_source(step: str) -> str:
    match = re.search(r"^        run: \|\s*\n((?:          .*\n|\n)*)", step, re.MULTILINE)
    if not match:
        raise AssertionError("Missing literal shell block")
    return textwrap.dedent(match.group(1))


def expression_field(text: str, field: str, indent: int) -> str:
    match = re.search(r"^" + " " * indent + re.escape(field) + r":\s*(.*)$", text, re.MULTILINE)
    if not match:
        raise AssertionError(f"Missing workflow field: {field}")
    value = match.group(1).strip()
    if value == ">-":
        lines = []
        for line in text[match.end():].splitlines():
            if line.strip() and len(line) - len(line.lstrip()) <= indent:
                break
            lines.append(line.strip())
        value = " ".join(lines)
    return value.strip()


def evaluate_condition(expression: str, *, event: str, mode: str = "", ref: str = "refs/heads/main"):
    """Evaluate the small, validated Actions expression subset used here."""
    if not expression.startswith("${{") or not expression.endswith("}}"):
        raise AssertionError("Expected an explicit GitHub Actions expression")
    source = expression[3:-2].strip().replace("&&", " and ").replace("||", " or ")
    tree = ast.parse(source, mode="eval")
    allowed = (ast.Expression, ast.BoolOp, ast.And, ast.Or, ast.Compare, ast.Eq,
               ast.Constant, ast.Name, ast.Load, ast.Attribute, ast.Call)
    for node in ast.walk(tree):
        if not isinstance(node, allowed):
            raise AssertionError(f"Unexpected expression syntax: {type(node).__name__}")
        if isinstance(node, ast.Call) and (not isinstance(node.func, ast.Name)
                                          or node.func.id not in {"always", "format"}):
            raise AssertionError("Unexpected expression function")
    github = SimpleNamespace(event_name=event, ref=ref, workflow="Advanced Sync",
                             event=SimpleNamespace(inputs=SimpleNamespace(run_mode=mode)))
    return eval(compile(tree, "<workflow condition>", "eval"), {"__builtins__": {}},
                {"github": github, "always": lambda: True,
                 "format": lambda template, *args: template.format(*args)})


class FinalPublicationStructureTests(unittest.TestCase):
    def setUp(self):
        self.workflow = WORKFLOW.read_text(encoding="utf-8")
        self.job = job_source(self.workflow, FINAL_JOB)

    def test_gate_runs_only_for_schedule_or_manual_full_sync(self):
        expression = expression_field(self.job, "if", 4)
        for event, mode in itertools.product(
            ("schedule", "workflow_dispatch", "push", "pull_request", "workflow_run"),
            ("", "full_sync", "health_only", "single_key"),
        ):
            with self.subTest(event=event, mode=mode):
                expected = event == "schedule" or (event == "workflow_dispatch" and mode == "full_sync")
                self.assertEqual(evaluate_condition(expression, event=event, mode=mode), expected)
        self.assertIn("always()", expression, "Final audit must run after unsuccessful source/recovery jobs")

    def test_waits_for_all_source_legs_and_recovery_without_releasing_lease(self):
        needs = expression_field(self.job, "needs", 4)
        self.assertEqual([item.strip() for item in needs.strip("[]").split(",")],
                         ["sync-dashboard", "recover-missing-market-pages"])
        self.assertNotRegex(self.job, r"^    concurrency:", "An independent job lease could race production writes")
        expression = expression_field(self.workflow, "group", 2)
        manual_recovery = (ROOT / ".github/workflows/page_refresh_recovery.yml").read_text(encoding="utf-8")
        recovery_expression = expression_field(manual_recovery, "group", 2)
        for ref in ("refs/heads/main", "refs/heads/master", "refs/heads/review"):
            lease = evaluate_condition(expression, event="schedule", ref=ref)
            self.assertEqual(lease, "tadawul-production-write-" + ref)
            self.assertEqual(evaluate_condition(expression, event="workflow_dispatch", ref=ref), lease)
            self.assertEqual(evaluate_condition(recovery_expression, event="workflow_dispatch", ref=ref), lease)
        for event in ("schedule", "workflow_dispatch"):
            self.assertFalse(evaluate_condition(expression_field(self.workflow, "cancel-in-progress", 2), event=event))
        for event in ("push", "pull_request"):
            self.assertNotIn("production-write", evaluate_condition(expression, event=event))

    def test_checkout_pins_this_runs_commit_and_retains_no_auth(self):
        checkout = step_source(self.job, "Checkout this run's source")
        self.assertEqual(expression_field(checkout, "ref", 10), "${{ github.sha }}")
        self.assertEqual(expression_field(checkout, "fetch-depth", 10), "1")
        self.assertEqual(expression_field(checkout, "persist-credentials", 10), "false")

    def test_failure_evidence_and_cleanup_are_unconditional_and_bounded(self):
        for name in ("Publish final audit summary", "Upload final publication evidence", "Secure credential cleanup"):
            self.assertEqual(expression_field(step_source(self.job, name), "if", 8), "always()")
        artifact = step_source(self.job, "Upload final publication evidence")
        self.assertEqual(expression_field(artifact, "path", 10), "final_decision_publication.json")
        self.assertNotIn("continue-on-error", self.job)
        verdict = job_source(self.workflow, "notify-run-verdict")
        self.assertIn(FINAL_JOB, expression_field(verdict, "needs", 4))
        self.assertIn("${{ needs.verify-final-decision-publication.result }}", verdict)


class FinalPublicationShellTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(prefix="final-publication-contract-")
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        self.bin = self.root / "bin"
        self.bin.mkdir()
        self.runner_temp = self.root / "runner-temp"
        self.runner_temp.mkdir()
        self.credentials = {
            "type": "service_account", "client_email": "fake-account@workflow.test",
            "private_key": "FAKE_PRIVATE_KEY_DO_NOT_LOG_$(touch credential-shell-injection)",
        }
        self.raw = json.dumps(self.credentials)
        self.env = {
            "PATH": str(self.bin) + os.pathsep + os.defpath,
            "RUNNER_TEMP": str(self.runner_temp), "TARGET_SHEET_ID": "fake-workbook",
            "GITHUB_ENV": str(self.root / "github-env"),
            "GITHUB_OUTPUT": str(self.root / "github-output"),
            "GITHUB_STEP_SUMMARY": str(self.root / "summary.md"),
            "SECRET_CREDS": self.raw, "SECRET_CREDS_B64": "",
            "STUB_AUDIT_EXIT": "0", "STUB_CALL_PATH": str(self.root / "audit-call.json"),
        }
        self.job = job_source(WORKFLOW.read_text(encoding="utf-8"), FINAL_JOB)
        python = self.bin / "python"
        python.write_text(f"#!{sys.executable}\n" + textwrap.dedent("""\
            import json, os, sys
            from pathlib import Path
            if len(sys.argv) > 1 and sys.argv[1] == 'scripts/finalize_decision_publication.py':
                Path(os.environ['STUB_CALL_PATH']).write_text(json.dumps(sys.argv[1:]))
                code = int(os.environ['STUB_AUDIT_EXIT'])
                payload = {'exit_code': code, 'audit_exit_codes': [code, 0],
                           'blockers': ['coverage_failed'] if code >= 2 else [],
                           'blocker_published': code >= 2, 'feed_promoted': False}
                output = sys.argv[sys.argv.index('--json-out') + 1]
                Path(output).write_text(json.dumps(payload))
                raise SystemExit(code)
            os.execv(sys.executable, [sys.executable, *sys.argv[1:]])
            """), encoding="utf-8")
        python.chmod(0o755)

    def run_step(self, name: str, **env):
        return subprocess.run(
            ["bash", "-c", shell_source(step_source(self.job, name))],
            cwd=self.root, env={**self.env, **env}, text=True, capture_output=True, timeout=10,
        )

    def assert_no_credential_disclosure(self, result):
        output = result.stdout + result.stderr
        self.assertNotIn(self.credentials["private_key"], output)
        self.assertNotIn(self.raw, output)
        unmasked = output.replace("::add-mask::" + self.credentials["client_email"], "")
        self.assertNotIn(self.credentials["client_email"], unmasked)
        self.assertFalse((self.root / "credential-shell-injection").exists())

    def test_audit_exit_0_and_1_are_accepted_but_2_and_3_fail_job(self):
        for code in (0, 1, 2, 3, 127):
            with self.subTest(code=code):
                output_path = Path(self.env["GITHUB_OUTPUT"])
                output_path.unlink(missing_ok=True)
                result = self.run_step("Audit final coverage and decision clocks", STUB_AUDIT_EXIT=str(code))
                self.assertEqual(result.returncode, code if code >= 2 else 0, result.stderr)
                self.assertEqual(output_path.read_text(), f"audit_exit={code}\n")
                self.assertEqual("::error::" in result.stdout, code >= 2)
                self.assertEqual("::warning::" in result.stdout, code == 1)
                self.assert_no_credential_disclosure(result)
        self.assertEqual(json.loads(Path(self.env["STUB_CALL_PATH"]).read_text()), [
            "scripts/finalize_decision_publication.py", "--sheet-id", "fake-workbook",
            "--max-rows", "20000", "--publish-blockers", "--json-out", "final_decision_publication.json",
        ])

    def test_plain_and_base64_credentials_use_private_temp_file(self):
        for use_b64 in (False, True):
            with self.subTest(base64=use_b64):
                result = self.run_step(
                    "Configure final-audit credentials", SECRET_CREDS="" if use_b64 else self.raw,
                    SECRET_CREDS_B64=base64.b64encode(self.raw.encode()).decode() if use_b64 else "",
                )
                self.assertEqual(result.returncode, 0, result.stderr)
                path = self.runner_temp / "final_publication_credentials.json"
                self.assertEqual(json.loads(path.read_text()), self.credentials)
                self.assertEqual(stat.S_IMODE(path.stat().st_mode), 0o600)
                self.assertIn("GOOGLE_APPLICATION_CREDENTIALS=" + str(path) + "\n",
                              Path(self.env["GITHUB_ENV"]).read_text())
                self.assert_no_credential_disclosure(result)
                cleanup = self.run_step("Secure credential cleanup")
                self.assertEqual(cleanup.returncode, 0, cleanup.stderr)
                self.assertFalse(path.exists())

    def test_plain_secret_takes_priority_over_invalid_base64(self):
        result = self.run_step("Configure final-audit credentials", SECRET_CREDS_B64="not valid base64")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assert_no_credential_disclosure(result)

    def test_credentials_restrict_permissions_on_preexisting_temp_file(self):
        path = self.runner_temp / "final_publication_credentials.json"
        path.write_text("stale credentials")
        path.chmod(0o644)
        result = self.run_step("Configure final-audit credentials")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(stat.S_IMODE(path.stat().st_mode), 0o600)
        self.assertEqual(json.loads(path.read_text()), self.credentials)
        self.assert_no_credential_disclosure(result)

    def test_invalid_credentials_fail_without_creating_or_exporting_auth(self):
        cases = [
            {"TARGET_SHEET_ID": " "},
            {"SECRET_CREDS": "", "SECRET_CREDS_B64": ""},
            {"SECRET_CREDS": "", "SECRET_CREDS_B64": "invalid!"},
            {"SECRET_CREDS": "invalid JSON with " + self.credentials["private_key"]},
            {"SECRET_CREDS": "[]"},
            {"SECRET_CREDS": json.dumps({**self.credentials, "type": "authorized_user"})},
            {"SECRET_CREDS": json.dumps({**self.credentials, "client_email": ""})},
            {"SECRET_CREDS": json.dumps({**self.credentials, "private_key": ""})},
        ]
        for env in cases:
            with self.subTest(env_keys=sorted(env)):
                result = self.run_step("Configure final-audit credentials", **env)
                self.assertNotEqual(result.returncode, 0)
                self.assertIn("::error::", result.stderr)
                self.assertFalse((self.runner_temp / "final_publication_credentials.json").exists())
                self.assertFalse(Path(self.env["GITHUB_ENV"]).exists())
                self.assert_no_credential_disclosure(result)

    def test_cleanup_removes_stale_credential_even_without_exported_env(self):
        path = self.runner_temp / "final_publication_credentials.json"
        path.write_text(self.raw)
        shred = self.bin / "shred"
        shred.write_text("#!/bin/sh\nexit 1\n")
        shred.chmod(0o755)
        result = self.run_step("Secure credential cleanup")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertFalse(path.exists(), "Cleanup must fall back to rm if shred fails")
        self.assert_no_credential_disclosure(result)
        self.assertEqual(self.run_step("Secure credential cleanup").returncode, 0)

    def test_cleanup_is_bounded_to_the_final_audits_credential_file(self):
        path = self.runner_temp / "final_publication_credentials.json"
        path.write_text(self.raw)
        other = self.runner_temp / "other-job-credentials.json"
        other.write_text("other job data")
        result = self.run_step("Secure credential cleanup", GOOGLE_APPLICATION_CREDENTIALS=str(other))
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertFalse(path.exists())
        self.assertEqual(other.read_text(), "other job data")

    def test_summary_missing_report_never_claims_acceptance(self):
        result = self.run_step("Publish final audit summary")
        self.assertEqual(result.returncode, 0, result.stderr)
        summary = Path(self.env["GITHUB_STEP_SUMMARY"]).read_text()
        self.assertIn("No acceptance was issued", summary)
        self.assertNotIn("| exit_code |", summary)
        self.assert_no_credential_disclosure(result)

    def test_summary_surfaces_failed_audit_and_requires_native_refresh(self):
        self.assertEqual(self.run_step("Audit final coverage and decision clocks", STUB_AUDIT_EXIT="2").returncode, 2)
        result = self.run_step("Publish final audit summary")
        self.assertEqual(result.returncode, 0, result.stderr)
        summary = Path(self.env["GITHUB_STEP_SUMMARY"]).read_text()
        for expected in ("| exit_code | 2 |", "coverage_failed", "| blocker_published | true |",
                         "| feed_promoted | false |", "Native installation and refresh are required"):
            self.assertIn(expected, summary)
        self.assertNotIn(self.credentials["private_key"], summary)
        self.assertNotIn(self.credentials["client_email"], summary)
        self.assert_no_credential_disclosure(result)

    def test_evidence_and_credentials_cleanup_complete_for_every_audit_verdict(self):
        for code in (0, 1, 2, 3):
            with self.subTest(code=code):
                self.assertEqual(self.run_step("Configure final-audit credentials").returncode, 0)
                audit = self.run_step("Audit final coverage and decision clocks", STUB_AUDIT_EXIT=str(code))
                self.assertEqual(audit.returncode, code if code >= 2 else 0)
                for name in ("Publish final audit summary", "Secure credential cleanup"):
                    result = self.run_step(name)
                    self.assertEqual(result.returncode, 0, result.stderr)
                    self.assert_no_credential_disclosure(result)
                self.assertFalse((self.runner_temp / "final_publication_credentials.json").exists())
                evidence = (self.root / "final_decision_publication.json").read_text()
                self.assertEqual(json.loads(evidence)["exit_code"], code)
                for path in (self.root / "final_decision_publication.json",
                             Path(self.env["GITHUB_STEP_SUMMARY"]), Path(self.env["GITHUB_OUTPUT"]),
                             Path(self.env["GITHUB_ENV"])):
                    self.assertNotIn(self.credentials["private_key"], path.read_text())
                    self.assertNotIn(self.credentials["client_email"], path.read_text())


if __name__ == "__main__":
    unittest.main()
