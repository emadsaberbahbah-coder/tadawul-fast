from pathlib import Path
import sys
import unittest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from scripts.audit_repository_workflows import audit_workflow_text


class RepositoryWorkflowAuditTests(unittest.TestCase):
    def codes(self, text: str) -> set[str]:
        return {item.code for item in audit_workflow_text("workflow.yml", text)}

    def test_current_action_majors_are_clean(self):
        text = """
steps:
  - uses: actions/checkout@v6
  - uses: actions/setup-python@v6
  - uses: actions/upload-artifact@v7
"""
        self.assertEqual(self.codes(text), set())

    def test_future_checkout_major_is_error(self):
        findings = audit_workflow_text(
            "workflow.yml", "steps:\n  - uses: actions/checkout@v7\n"
        )
        self.assertEqual(findings[0].severity, "ERROR")
        self.assertEqual(findings[0].code, "UNSUPPORTED_ACTION_MAJOR")

    def test_old_action_major_is_warning(self):
        findings = audit_workflow_text(
            "workflow.yml", "steps:\n  - uses: actions/upload-artifact@v4\n"
        )
        self.assertEqual(findings[0].severity, "WARN")
        self.assertEqual(findings[0].code, "OUTDATED_ACTION_MAJOR")

    def test_pull_request_target_and_write_all_are_blocked(self):
        codes = self.codes(
            "on:\n  pull_request_target:\npermissions: write-all\n"
        )
        self.assertIn("PULL_REQUEST_TARGET", codes)
        self.assertIn("WRITE_ALL_PERMISSIONS", codes)

    def test_sensitive_manual_input_is_blocked(self):
        text = """
on:
  workflow_dispatch:
    inputs:
      api_key:
        type: string
"""
        self.assertIn("SENSITIVE_WORKFLOW_INPUT", self.codes(text))

    def test_temporary_force_refetch_is_visible(self):
        findings = audit_workflow_text(
            "workflow.yml",
            'env:\n  TFB_SYNC_FORCE_REFETCH_SYMBOLS: "BK,FI"\n',
        )
        self.assertEqual(findings[0].severity, "WARN")
        self.assertEqual(findings[0].code, "TEMPORARY_OVERRIDE_ACTIVE")

    def test_used_oidc_permission_is_not_flagged(self):
        text = """
permissions:
  id-token: write
steps:
  - uses: google-github-actions/auth@v3
"""
        self.assertNotIn("UNUSED_ID_TOKEN_PERMISSION", self.codes(text))


if __name__ == "__main__":
    unittest.main()
