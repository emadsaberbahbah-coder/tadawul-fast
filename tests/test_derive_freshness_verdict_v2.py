from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from scripts.derive_freshness_verdict_v2 import (
    _replace_v2_lines,
    derive_artifacts,
    derive_from_text,
)
from scripts.freshness_verdict_v2 import TAG_PREFIX


class DeriveFreshnessVerdictV2Tests(unittest.TestCase):
    @staticmethod
    def _legacy(
        page: str,
        *,
        status: str = "success",
        rows: int = 100,
        age: str = "1.0",
        reason: str = "clean",
    ) -> str:
        return (
            "2026-07-30 01:00:00 | INFO | DashboardSync | "
            f"[PAGE-VERDICT v6.26.0] page={page} status={status} "
            f"rows_written={rows} newest_stamp_age_h={age} reason={reason}\n"
        )

    def test_full_success_derives_core_counts_without_inventing_unknowns(self):
        result = derive_from_text(self._legacy("Market_Leaders", rows=100))
        self.assertEqual(len(result), 1)
        evidence = result[0]
        self.assertEqual(evidence.requested, 100)
        self.assertEqual(evidence.fresh, 100)
        self.assertEqual(evidence.preserved, 0)
        self.assertEqual(evidence.stubs, 0)
        self.assertEqual(evidence.identity_failures, 0)
        self.assertIsNone(evidence.stale)
        self.assertIsNone(evidence.oldest_source_age_h)
        self.assertIsNone(evidence.api_units)

    def test_floor_merge_derives_exact_fresh_and_preserved_counts(self):
        text = (
            "[v6.25.0 FLOOR-MERGE] Partial fetch on 'Global_Markets': "
            "80 fresh row(s) for 100 requested (80% coverage, floor 70%)\n"
            "[SYMBOL-PERSISTENCE v6.19.0] preserved 20 last-good row(s) for "
            "fetch-missed symbol(s) on 'Global_Markets': A.US\n"
            + self._legacy("Global_Markets", status="partial", rows=100)
        )
        evidence = derive_from_text(text)[0]
        self.assertEqual(evidence.requested, 100)
        self.assertEqual(evidence.fresh, 80)
        self.assertEqual(evidence.preserved, 20)
        self.assertAlmostEqual(evidence.coverage_pct or 0, 80.0)

    def test_unrestored_quarantine_becomes_stub_and_identity_failure(self):
        text = (
            "[ID-FIREWALL v6.24.0] quarantined 3 identity-broken outgoing row(s) "
            "on 'Market_Leaders': A.US, B.US, C.US\n"
            "[v6.25.1 FW-KEEP] 'Market_Leaders': restored 2/3 quarantined "
            "row(s) from last-good; 1 had no last-good (left as stub).\n"
            + self._legacy("Market_Leaders", status="partial", rows=100)
        )
        evidence = derive_from_text(text)[0]
        self.assertEqual(evidence.preserved, 2)
        self.assertEqual(evidence.stubs, 1)
        self.assertEqual(evidence.identity_failures, 3)

    def test_batch_reason_derives_requested_and_provider_failures(self):
        reason = "[SYMBOL-BATCH] fetched 100 symbol(s) in 3/4 batch(es) of 25 via /v1/analysis"
        evidence = derive_from_text(
            self._legacy("Mutual_Funds", status="partial", rows=75, reason=reason)
        )[0]
        self.assertEqual(evidence.requested, 100)
        self.assertEqual(evidence.fresh, 75)
        self.assertEqual(evidence.provider_failures, 1)
        self.assertAlmostEqual(evidence.coverage_pct or 0, 75.0)

    def test_append_is_idempotent(self):
        text = self._legacy("Commodities_FX", rows=10)
        evidence = derive_from_text(text)
        once = _replace_v2_lines(text, evidence)
        twice = _replace_v2_lines(once, evidence)
        self.assertEqual(once, twice)
        self.assertEqual(once.count(TAG_PREFIX), 1)

    def test_artifact_append_updates_canonical_log(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            artifact = root / "artifact"
            artifact.mkdir()
            log = artifact / "sync_execution.log"
            log.write_text(self._legacy("Global_Markets", rows=10), encoding="utf-8")
            evidence, touched = derive_artifacts(root, append=True)
            self.assertEqual(len(evidence), 1)
            self.assertEqual(touched, [str(log)])
            self.assertIn(TAG_PREFIX, log.read_text(encoding="utf-8"))


if __name__ == "__main__":
    unittest.main()
