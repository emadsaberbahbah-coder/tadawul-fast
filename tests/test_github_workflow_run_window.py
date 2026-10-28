"""The read-only Actions inventory must verify complete, bounded run coverage."""
import json
from pathlib import Path
import shutil
import subprocess
import unittest


ROOT = Path(__file__).resolve().parents[1]


class WorkflowRunWindowTests(unittest.TestCase):
    def test_real_node_collector_validates_coverage_and_adverse_boundaries(self):
        node = shutil.which("node")
        if node is None:
            self.skipTest("Node is unavailable in this test environment")
        result = subprocess.run([node, "tests/js/github_workflow_run_window.test.js"],
                                cwd=ROOT, capture_output=True, text=True, timeout=15)
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        report = json.loads(result.stdout.strip().splitlines()[-1])
        self.assertEqual(report, {"cases":16, "passed":16})

    def test_health_workflow_uses_same_scope_bounded_collector_and_no_schedule(self):
        text = (ROOT / ".github/workflows/actions_health_review.yml").read_text()
        self.assertIn("require('./scripts/github_workflow_run_window.js')", text)
        self.assertIn("collectWorkflowRuns(github, {owner, repo, since: start, until: end})", text)
        self.assertIn("INVENTORY_INCOMPLETE", text)
        self.assertIn("persist-credentials: false", text)
        self.assertNotIn("if (runs.length >= 1000)", text)
        self.assertNotIn("  schedule:", text)


if __name__ == "__main__":
    unittest.main()
