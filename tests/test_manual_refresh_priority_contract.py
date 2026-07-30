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

    def test_pause_is_ttl_backed_and_self_clearing(self) -> None:
        self.assertIn("TFB_MANUAL_REFRESH_UNTIL_MS", self.source)
        self.assertRegex(self.source, r"MANUAL_PAUSE_TTL_MS:\s*20\s*\*\s*60\s*\*\s*1000")
        self.assertIn("tfbClearManualPause_('expired')", self.source)

    def test_automatic_rechecks_manual_priority_after_lock(self) -> None:
        body = re.search(
            r"function tfbRunAutomaticRefresh_\(callback, label\) \{(?P<body>.*?)\n\}",
            self.source,
            flags=re.S,
        )
        self.assertIsNotNone(body)
        text = body.group("body")
        self.assertGreaterEqual(text.count("tfbAutomaticRefreshAllowed_(label)"), 2)
        self.assertIn("LockService.getScriptLock()", text)

    def test_manual_always_clears_pause_and_resumes(self) -> None:
        body = re.search(
            r"function tfbExecuteManualHandler_\(sourceLabel\) \{(?P<body>.*?)\n\}",
            self.source,
            flags=re.S,
        )
        self.assertIsNotNone(body)
        text = body.group("body")
        self.assertIn("finally", text)
        self.assertIn("lock.releaseLock();", text)
        self.assertIn("tfbClearManualPause_('manual-finally');", text)
        self.assertIn("tfbScheduleAutomaticResume_();", text)

    def test_no_bulk_trigger_deletion(self) -> None:
        # Only the coordinator-owned deferred trigger may be deleted, by unique ID.
        self.assertEqual(self.source.count("ScriptApp.deleteTrigger("), 1)
        deletion_body = re.search(
            r"function tfbRemoveOwnDeferredTrigger_\(\) \{(?P<body>.*?)\n\}",
            self.source,
            flags=re.S,
        )
        self.assertIsNotNone(deletion_body)
        text = deletion_body.group("body")
        self.assertIn("PROP_DEFERRED_TRIGGER_ID", text)
        self.assertIn("getUniqueId() === expectedId", text)

    def test_deferred_trigger_is_deduplicated(self) -> None:
        body = re.search(
            r"function tfbEnsureOneShotTrigger_\(handlerName, delayMs\) \{(?P<body>.*?)\n\}",
            self.source,
            flags=re.S,
        )
        self.assertIsNotNone(body)
        text = body.group("body")
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
