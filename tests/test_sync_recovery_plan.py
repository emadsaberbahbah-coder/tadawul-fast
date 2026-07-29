from __future__ import annotations

import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

from scripts.plan_sync_recovery import PAGE_CONFIG, RECOVERY_ORDER, build_recovery_plan


ROOT = Path(__file__).resolve().parents[1]


class SyncRecoveryPlanTests(unittest.TestCase):
    @staticmethod
    def _verdict(page: str, status: str = "success", rows: int = 10) -> str:
        return (
            "2026-07-29 12:00:00 | INFO | DashboardSync | "
            f"[PAGE-VERDICT v6.26.0] page={page} status={status} "
            f"rows_written={rows} newest_stamp_age_h=1 reason=test\n"
        )

    def _plan(self, text: str) -> dict[str, object]:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            artifact = root / "source"
            artifact.mkdir()
            (artifact / "sync_execution.log").write_text(text, encoding="utf-8")
            return build_recovery_plan(root)

    def test_all_pages_refreshed_produces_no_recovery(self):
        text = "".join(self._verdict(page) for page in (
            "Market_Leaders",
            "Global_Markets",
            "Commodities_FX",
            "Mutual_Funds",
        ))
        plan = self._plan(text)
        self.assertFalse(plan["needs_recovery"])
        self.assertEqual(plan["retry_pages"], [])
        self.assertEqual(plan["matrix"], {"include": []})

    def test_skipped_and_zero_row_pages_are_retried(self):
        text = "".join((
            self._verdict("Market_Leaders", "success", 1360),
            self._verdict("Global_Markets", "skipped", 0),
            self._verdict("Commodities_FX", "success", 0),
            self._verdict("Mutual_Funds", "success", 2475),
        ))
        plan = self._plan(text)
        self.assertTrue(plan["needs_recovery"])
        self.assertEqual(
            plan["retry_pages"],
            ["Global_Markets", "Commodities_FX"],
        )
        self.assertEqual(
            [item["key"] for item in plan["matrix"]["include"]],
            ["GLOBAL_MARKETS", "COMMODITIES_FX"],
        )

    def test_missing_page_is_retried(self):
        text = "".join((
            self._verdict("Market_Leaders"),
            self._verdict("Global_Markets"),
            self._verdict("Mutual_Funds"),
        ))
        plan = self._plan(text)
        self.assertEqual(plan["retry_pages"], ["Commodities_FX"])

    def test_recovery_order_pairs_large_and_small_pages(self):
        self.assertEqual(
            RECOVERY_ORDER,
            (
                "Global_Markets",
                "Commodities_FX",
                "Mutual_Funds",
                "Market_Leaders",
            ),
        )
        self.assertEqual(PAGE_CONFIG["Global_Markets"]["stagger"], 0)
        self.assertEqual(PAGE_CONFIG["Commodities_FX"]["stagger"], 120)

    def test_force_refetch_evidence_is_preserved_in_plan(self):
        text = "[FORCE-REFETCH] symbol=BK incoming_name=test\n" + "".join(
            self._verdict(page) for page in (
                "Market_Leaders",
                "Global_Markets",
                "Commodities_FX",
                "Mutual_Funds",
            )
        )
        plan = self._plan(text)
        self.assertEqual(plan["force_refetch_evidence_lines"], 1)

    def test_matrix_is_json_serializable_for_github(self):
        text = "".join(
            self._verdict(page, "skipped", 0)
            for page in (
                "Market_Leaders",
                "Global_Markets",
                "Commodities_FX",
                "Mutual_Funds",
            )
        )
        plan = self._plan(text)
        rendered = json.dumps(plan["matrix"], separators=(",", ":"))
        self.assertIn('"include"', rendered)
        self.assertEqual(len(plan["matrix"]["include"]), 4)

    def test_direct_cli_invocation_matches_github_actions(self):
        """Exercise the exact path used by page_refresh_recovery.yml."""
        text = "".join((
            self._verdict("Market_Leaders", "success", 1360),
            self._verdict("Global_Markets", "skipped", 0),
            self._verdict("Commodities_FX", "success", 0),
            self._verdict("Mutual_Funds", "success", 2475),
        ))
        with tempfile.TemporaryDirectory() as tmp:
            temp = Path(tmp)
            artifact = temp / "source"
            artifact.mkdir()
            (artifact / "sync_execution.log").write_text(text, encoding="utf-8")
            json_out = temp / "sync-recovery-plan.json"
            github_output = temp / "github-output.txt"

            result = subprocess.run(
                [
                    sys.executable,
                    "scripts/plan_sync_recovery.py",
                    "--root",
                    str(temp),
                    "--json-out",
                    str(json_out),
                    "--github-output",
                    str(github_output),
                ],
                cwd=ROOT,
                text=True,
                capture_output=True,
                check=False,
            )

            self.assertEqual(result.returncode, 0, result.stderr or result.stdout)
            plan = json.loads(json_out.read_text(encoding="utf-8"))
            outputs = github_output.read_text(encoding="utf-8")

        self.assertEqual(plan["retry_pages"], ["Global_Markets", "Commodities_FX"])
        self.assertIn("needs_recovery=true", outputs)
        self.assertIn('"include"', outputs)


if __name__ == "__main__":
    unittest.main()
