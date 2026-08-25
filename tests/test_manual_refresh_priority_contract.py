from __future__ import annotations

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
        marker = f"function {signature} {{"
        start = self.source.find(marker)
        self.assertGreaterEqual(start, 0, signature)
        open_brace = self.source.find("{", start)
        self.assertGreaterEqual(open_brace, 0, signature)

        depth = 0
        quote = ""
        escaped = False
        line_comment = False
        block_comment = False
        index = open_brace
        while index < len(self.source):
            char = self.source[index]
            nxt = self.source[index + 1] if index + 1 < len(self.source) else ""

            if line_comment:
                if char == "\n":
                    line_comment = False
                index += 1
                continue
            if block_comment:
                if char == "*" and nxt == "/":
                    block_comment = False
                    index += 2
                    continue
                index += 1
                continue
            if quote:
                if escaped:
                    escaped = False
                elif char == "\\":
                    escaped = True
                elif char == quote:
                    quote = ""
                index += 1
                continue

            if char == "/" and nxt == "/":
                line_comment = True
                index += 2
                continue
            if char == "/" and nxt == "*":
                block_comment = True
                index += 2
                continue
            if char in {"'", '"', "`"}:
                quote = char
                index += 1
                continue
            if char == "{":
                depth += 1
            elif char == "}":
                depth -= 1
                if depth == 0:
                    return self.source[open_brace + 1 : index]
            index += 1

        self.fail(f"Unclosed function body: {signature}")

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

    def test_version_includes_race_hardening(self) -> None:
        # v1.1.0 (2026-08-25): backend write-window hold honored at the
        # Allowed_/YieldPoint_ choke points (counterpart of sync v6.45.0
        # [SYNC-HOLD]); gate TFB_GAS_BACKEND_HOLD default OFF. The pin
        # tracks the reviewed version deliberately - bump it only with a
        # reviewed coordinator change.
        self.assertIn("TFB_REFRESH_COORDINATOR_VERSION = '1.1.0'", self.source)
        self.assertIn("TFB_GAS_BACKEND_HOLD", self.source)
        self.assertIn("automatic-skipped-for-backend-hold", self.source)
        self.assertIn("automatic-yielded-for-backend-hold", self.source)
        self.assertIn("TFB_MANUAL_REFRESH_REQUEST_ID", self.source)

    def test_coordinator_uses_a_separate_document_lock(self) -> None:
        text = self.function_body("tfbWithCoordinatorLock_(label, callback)")
        self.assertIn("LockService.getDocumentLock()", text)
        self.assertIn("COORDINATOR_LOCK_WAIT_MS", text)
        self.assertIn("lock.releaseLock();", text)
        self.assertIn("LockService.getScriptLock()", self.source)

    def test_expired_pause_is_compare_and_deleted(self) -> None:
        text = self.function_body(
            "tfbClearExpiredManualPauseIfMatch_(expectedRaw, expectedRequestId)"
        )
        self.assertIn("tfbWithCoordinatorLock_('clear-expired-pause'", text)
        self.assertIn("currentRaw !== String(expectedRaw || '')", text)
        self.assertIn(
            "currentRequestId !== String(expectedRequestId || '')",
            text,
        )
        self.assertIn("tfbDeleteManualPauseUnlocked_(props)", text)

        reader = self.function_body("tfbReadManualPause_()")
        self.assertIn("for (var attempt = 0; attempt < 3; attempt++)", reader)
        self.assertIn("tfbClearExpiredManualPauseIfMatch_(raw, requestId)", reader)
        self.assertIn("State changed while the stale instance was being checked", reader)

    def test_manual_pause_claim_is_atomic_and_deduplicated(self) -> None:
        text = self.function_body("tfbClaimManualPause_(reason)")
        self.assertIn("tfbWithCoordinatorLock_('claim-manual-pause'", text)
        self.assertIn("claimed: false", text)
        self.assertIn("tfbWriteManualPauseUnlocked_(props, reason, requestId)", text)
        self.assertIn("manual-pause-deduplicated", text)

    def test_pause_extension_and_clear_are_request_scoped(self) -> None:
        extend = self.function_body("tfbExtendManualPause_(reason, requestId)")
        self.assertIn("currentId !== String(requestId || '')", extend)
        self.assertIn("manual-pause-extension-skipped", extend)

        clear = self.function_body(
            "tfbClearManualPause_(reason, expectedRequestId, force)"
        )
        self.assertIn("currentId !== String(expectedRequestId || '')", clear)
        self.assertIn("manual-pause-clear-skipped", clear)
        self.assertIn("tfbWithCoordinatorLock_('clear-manual-pause'", clear)

    def test_automatic_rechecks_manual_priority_after_script_lock(self) -> None:
        text = self.function_body("tfbRunAutomaticRefresh_(callback, label)")
        self.assertGreaterEqual(text.count("tfbAutomaticRefreshAllowed_(label)"), 2)
        self.assertIn("LockService.getScriptLock()", text)

    def test_public_manual_entrypoints_validate_before_pause_work(self) -> None:
        menu = self.function_body("tfbManualRefresh()")
        self.assertLess(
            menu.index("var configured = tfbConfiguredManualHandler_();"),
            menu.index("var claim = tfbClaimManualPause_('menu-request');"),
        )
        self.assertLess(
            menu.index("var claim = tfbClaimManualPause_('menu-request');"),
            menu.index("tfbExecuteManualHandler_('menu'"),
        )

        deferred = self.function_body("tfbManualRefreshDeferred_()")
        self.assertLess(
            deferred.index("var configured = tfbConfiguredManualHandler_();"),
            deferred.index("var pause = tfbReadManualPause_();"),
        )
        self.assertLess(
            deferred.index("var pause = tfbReadManualPause_();"),
            deferred.index("tfbExecuteManualHandler_('deferred'"),
        )

    def test_deferred_trigger_creation_is_one_atomic_transaction(self) -> None:
        text = self.function_body("tfbScheduleDeferredManual_()")
        lock_at = text.index("tfbWithCoordinatorLock_('schedule-deferred-manual'")
        ensure_at = text.index("tfbEnsureOneShotTrigger_(", lock_at)
        property_at = text.index("PROP_DEFERRED_TRIGGER_ID", ensure_at)
        self.assertLess(lock_at, ensure_at)
        self.assertLess(ensure_at, property_at)

        ensure = self.function_body("tfbEnsureOneShotTrigger_(handlerName, delayMs)")
        self.assertIn("tfbFindTriggerByHandler_(handlerName)", ensure)
        self.assertIn("created: false", ensure)
        self.assertIn("Caller must hold the coordinator DocumentLock", self.source)

    def test_manual_cleanup_clears_owned_pause_before_trigger_work(self) -> None:
        text = self.function_body(
            "tfbExecuteManualHandler_(sourceLabel, configured, requestId)"
        )
        self.assertIn("finally", text)
        self.assertIn("lock.releaseLock();", text)
        clear_at = text.index(
            "tfbClearManualPause_('manual-finally', requestId, false)"
        )
        remove_at = text.index("tfbRemoveOwnDeferredTrigger_();", clear_at)
        resume_at = text.index("tfbScheduleAutomaticResume_();", remove_at)
        self.assertLess(clear_at, remove_at)
        self.assertLess(remove_at, resume_at)
        self.assertIn("if (pauseCleared)", text)
        self.assertIn("manual-cleanup-preserved-newer-request", text)
        self.assertIn("tfbLogCleanupFailure_('release-lock'", text)
        self.assertIn("tfbLogCleanupFailure_('clear-manual-pause'", text)
        self.assertIn("tfbLogCleanupFailure_('remove-deferred-trigger'", text)
        self.assertIn("tfbLogCleanupFailure_('schedule-automatic-resume'", text)

    def test_failed_deferred_schedule_clears_only_owned_request(self) -> None:
        text = self.function_body(
            "tfbExecuteManualHandler_(sourceLabel, configured, requestId)"
        )
        self.assertIn(
            "tfbClearManualPause_('deferred-schedule-failed', requestId, false)",
            text,
        )
        self.assertIn("throw scheduleErr;", text)

    def test_no_bulk_trigger_deletion(self) -> None:
        # Only the coordinator-owned deferred trigger may be deleted, by unique ID.
        self.assertEqual(self.source.count("ScriptApp.deleteTrigger("), 1)
        text = self.function_body("tfbRemoveOwnDeferredTrigger_()")
        self.assertIn("PROP_DEFERRED_TRIGGER_ID", text)
        self.assertIn("getUniqueId() === expectedId", text)
        self.assertIn("tfbWithCoordinatorLock_('remove-deferred-manual'", text)

    def test_recursive_manual_handler_is_rejected(self) -> None:
        self.assertIn("Recursive refresh handler is not allowed", self.source)
        self.assertIn("'tfbManualRefresh'", self.source)

    def test_document_states_not_deployed(self) -> None:
        self.assertIn("not deployed to the bound Google Apps Script project", self.doc)
        self.assertIn("cannot forcibly terminate", self.doc)
        self.assertIn("three successful manual-during-auto simulations", self.doc)


if __name__ == "__main__":
    unittest.main()
