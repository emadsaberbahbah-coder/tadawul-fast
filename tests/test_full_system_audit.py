from __future__ import annotations

import importlib.util
import json
from pathlib import Path
import sys
import tempfile
import unittest

MODULE_PATH = Path(__file__).resolve().parents[1] / "scripts" / "full_system_audit.py"
SPEC = importlib.util.spec_from_file_location("full_system_audit", MODULE_PATH)
assert SPEC and SPEC.loader
AUDIT = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = AUDIT
SPEC.loader.exec_module(AUDIT)


class FullSystemAuditTests(unittest.TestCase):
    def fixture(self, root: Path) -> None:
        files = {
            "main.py": "app=object()\n", "requirements.txt": "fastapi==0\n",
            "Procfile": "# render.yaml defines the web service\nweb: ./scripts/start_web.sh\n",
            "scripts/start_web.sh": "#!/bin/sh\n", "scripts/verify_deployment.py": "print('{}')\n",
            "scripts/validate_dashboard.py": "raise SystemExit(0)\n",
            "scripts/run_dashboard_sync.py": "TFB_ENGINE_OHLC_COHERENCE='x'\n",
            "core/data_engine_v2.py": "TFB_ENGINE_OHLC_COHERENCE='x'\nTFB_ENGINE_OHLC_COHERENCE_FINAL='x'\nTFB_ENGINE_BATCH_FPRINT='x'\n",
            "core/surface_action_invariants.py": "# TFB_T10_BLOCKED_INVARIANT default 0\n# TFB_T10_FETCHFAIL_BLOCKED default 0\n# TFB_WARN_INVEST_INVARIANT default 0\n# TFB_ROW_SANITY_QUARANTINE default 0\n",
            "core/sheets/schema_registry.py": "X={}\n", "config.py": "X=1\n", "core/config.py": "X=2\n",
            "symbols_reader.py": "X=1\n", "core/symbols_reader.py": "X=2\n",
            "integrations/symbols_reader.py": "X=3\n", "core/data_engine.py": "X=1\n",
            "core/scoring.py": "X=1\n", "core/scoring_engine.py": "X=2\n",
            "apps_script/11_Manual_Refresh_Coordinator.gs": "function x(){}\n",
        }
        for rel, text in files.items():
            path = root / rel
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(text, encoding="utf-8")

    def test_secret_redaction(self) -> None:
        clean = AUDIT.redact({"api_key": "abc", "nested": {"Authorization": "Bearer xyz"}, "text": "token=123"})
        self.assertEqual(clean["api_key"], "<redacted>")
        self.assertEqual(clean["nested"]["Authorization"], "<redacted>")
        self.assertIn("<redacted>", clean["text"])

    def test_repo_audit_detects_governance_gaps(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp); self.fixture(root)
            by_id = {x["check"]: x for x in AUDIT.repo_audit(root, 30)}
            self.assertEqual(by_id["render.start_command"]["status"], "PASS")
            self.assertEqual(by_id["render.blueprint_truth"]["status"], "WARN")
            self.assertEqual(by_id["gas.source_control"]["status"], "WARN")
            self.assertEqual(by_id["repo.canonical_paths"]["status"], "WARN")
            self.assertEqual(by_id["repo.guard_contract"]["status"], "WARN")

    def test_verdict_is_fail_closed(self) -> None:
        fail = AUDIT.finding("x", "GitHub", "FAIL", "CRITICAL", "broken")
        warn = AUDIT.finding("y", "Render", "WARN", "HIGH", "uncertain")
        passed = AUDIT.finding("z", "GitHub", "PASS", "INFO", "clean")
        self.assertEqual(AUDIT.verdict([fail], "all"), "NO_GO")
        self.assertEqual(AUDIT.verdict([warn], "all"), "CONDITIONAL_NO_GO")
        self.assertEqual(AUDIT.verdict([passed], "repo"), "REPO_CLEAN_PRODUCTION_UNVERIFIED")

    def test_markdown_and_json_are_serializable(self) -> None:
        item = AUDIT.finding("x", "GitHub", "PASS", "INFO", "clean", {"a": 1})
        report = {"generated_at_utc": AUDIT.now(), "release_sha": "abc", "mode": "repo", "technical_verdict": "REPO_CLEAN_PRODUCTION_UNVERIFIED", "findings": [item]}
        text = AUDIT.markdown(report)
        self.assertIn("TFB Full-System Audit", text)
        json.dumps(item)


if __name__ == "__main__":
    unittest.main()
