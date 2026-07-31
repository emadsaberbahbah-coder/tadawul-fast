from __future__ import annotations

import re
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
SOURCE = ROOT / "apps_script" / "11_Manual_Refresh_Coordinator.gs"
DOC = ROOT / "docs" / "MANUAL_REFRESH_PRIORITY_V1.md"


class ManualRefreshPriorityContractTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.source = SOURCE.read_text(encoding="utf-8")
        cls.doc = DOC.read_text(encoding="utf-8")

    def function_body(self, signature: str) -> str:
        body = re.search(
            rf"function {re.escape(signature)} \{{(?P<body>.*?)\n\}}",
            self.source,
            flags=re.S,
        )
        self.assertIsNotNone(body, signature)
        return body.group("body")

    def test_public_and_wrapper_entrypoints_exist(self) -> None:
        required = (
            "function tfbManualRefresh()",
            "function tfbManualRefreshDeferred_()",
            "function tfbRunAutomaticRefresh_(callback, label)",
            "function tfbAutomaticYieldPoint_(label)",
            "function tfbManualRefreshStatus()",
            "function tfbClearStaleManualRefreshPause()",
        )
        for token in required:
            self.assertIn(token, self.source)

    def test_version_includes_cleanup_hardening(self) -> None:
        self.assertIn("TFB_REFRESH_COORDINATOR_VERSION = '1.0.1'", self.source)

    def test_pause_is_ttl_backed_and_self_clearing(self) -> None:
        self.assertIn("TFB_MANUAL_REFRESH_UNTIL_MS", self.source)
        self.assertRegex(self.source, r"MANUAL_PAUSE_TTL_MS:\s*20\s*\*\s*60\s*\*\s*1000")
        self.assertIn("tfbClearManualPause_('expired')", self.source)

    def test_automatic_rechecks_manual_priority_after_lock(self) -> None:
        text = self.function_body("tfbRunAutomaticRefresh_(callback, label)")
        self.assertGreaterEqual(text.count("tfbAutomaticRefreshAllowed_(label)"), 2)
        self.assertIn("LockService.getScriptLock()", text)

    def test_public_manual_entrypoints_validate_before_pause(self) -> None:
        cases = (
            ("tfbManualRefresh()", "tfbRequestManualPause_('menu-request')"),
            (
                "tfbManualRefreshDeferred_()",
                "tfbRequestManualPause_('deferred-request')",
            ),
        )
        for signature, pause_call in cases:
            text = self.function_body(signature)
            resolve_at = text.index("var configured = tfbConfiguredManualHandler_();")
            pause_at = text.index(pause_call)
            execute_at = text.index("tfbExecuteManualHandler_")
            self.assertLess(resolve_at, pause_at, signature)
            self.assertLess(pause_at, execute_at, signature)

    def test_manual_cleanup_is_isolated_and_clears_before_trigger_work(self) -> None:
        text = self.function_body("tfbExecuteManualHandler_(sourceLabel, configured)")
        self.assertIn("finally", text)
        self.assertIn("lock.releaseLock();", text)
        clear_at = text.index("tfbClearManualPause_('manual-finally');")
        remove_at = text.index("tfbRemoveOwnDeferredTrigger_();", clear_at)
        resume_at = text.index("tfbScheduleAutomaticResume_();", remove_at)
        self.assertLess(clear_at, remove_at)
        self.assertLess(remove_at, resume_at)
        self.assertIn("tfbLogCleanupFailure_('release-lock'", text)
        self.assertIn("tfbLogCleanupFailure_('clear-manual-pause'", text)
        self.assertIn("tfbLogCleanupFailure_('remove-deferred-trigger'", text)
        self.assertIn("tfbLogCleanupFailure_('schedule-automatic-resume'", text)

    def test_failed_deferred_schedule_clears_pause(self) -> None:
        text = self.function_body("tfbExecuteManualHandler_(sourceLabel, configured)")
        self.assertIn("tfbClearManualPause_('deferred-schedule-failed')", text)
        self.assertIn("throw scheduleErr;", text)

    def test_no_bulk_trigger_deletion(self) -> None:
        # Only the coordinator-owned deferred trigger may be deleted, by unique ID.
        self.assertEqual(self.source.count("ScriptApp.deleteTrigger("), 1)
        text = self.function_body("tfbRemoveOwnDeferredTrigger_()")
        self.assertIn("PROP_DEFERRED_TRIGGER_ID", text)
        self.assertIn("getUniqueId() === expectedId", text)

    def test_deferred_trigger_is_deduplicated(self) -> None:
        text = self.function_body("tfbEnsureOneShotTrigger_(handlerName, delayMs)")
        self.assertIn("tfbFindTriggerByHandler_(handlerName)", text)
        self.assertIn("created: false", text)

    def test_recursive_manual_handler_is_rejected(self) -> None:
        self.assertIn("Recursive refresh handler is not allowed", self.source)
        self.assertIn("'tfbManualRefresh'", self.source)

    def test_document_states_not_deployed(self) -> None:
        self.assertIn("not deployed to the bound Google Apps Script project", self.doc)
        self.assertIn("cannot forcibly terminate", self.doc)
        self.assertIn("three successful manual-during-auto simulations", self.doc)


if __name__ == "__main__":
    unittest.main()
