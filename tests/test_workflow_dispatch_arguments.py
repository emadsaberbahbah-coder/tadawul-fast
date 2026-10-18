"""Execute real manual workflow wrappers with an inert argv-recording boundary."""
from __future__ import annotations

import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import textwrap
import unittest

from tests.test_final_publication_workflow import (
    expression_field, job_source, shell_source, step_source,
)


ROOT = Path(__file__).resolve().parents[1]
CASES = {
    "acceptance_check": {
        "job": "acceptance", "step": "Measure the live workbook (read-only)",
        "inputs": {"BOARD_MIN": "${{ inputs.board_min || '5' }}", "STRICT": "${{ inputs.strict }}"},
        "defaults": {"BOARD_MIN": "5", "STRICT": ""},
        "calls": [["scripts/tfb_acceptance.py", "--live", "--json", "artifacts/acceptance.json", "--board-min", "5"]],
    },
    "backtest_signals": {
        "job": "backtest", "step": "Backtest (all signals, all cohorts)",
        "inputs": {"SINCE": "${{ inputs.since }}", "FILTER": "${{ inputs.filter }}"},
        "defaults": {"SINCE": "", "FILTER": ""},
        "calls": [
            ["scripts/tfb_backtest.py", "--live", "--all-signals", "--edges", "50,70,85", "--json", "artifacts/backtest_all.json"],
            ["scripts/tfb_backtest.py", "--live", "--all-signals", "--edges", "50,70,85", "--filter", "Origin Tab=Top_10_Investments", "--json", "artifacts/backtest_board.json"],
            ["scripts/tfb_backtest.py", "--live", "--all-signals", "--edges", "50,70,85", "--json", "artifacts/backtest_window.json"],
        ],
    },
    "thesis_exit_backtest": {
        "job": "backtest", "step": "Replay on the live workbook (read-only)",
        "inputs": {"SCORE": "${{ inputs.score || '60' }}", "DAYS": "${{ inputs.days || '5' }}"},
        "defaults": {"SCORE": "60", "DAYS": "5"},
        "calls": [["scripts/backtest_pf_thesis_exit.py", "--live", "--score", "60", "--days", "5", "--out-md", "artifacts/thesis_exit_backtest.md", "--out-json", "artifacts/thesis_exit_backtest.json"]],
    },
    "grid_capacity_repair": {
        "job": "grid-repair", "step": "Run repair tool",
        "inputs": {key.upper(): "${{ inputs." + key + " }}" for key in ("apply", "have_backup", "confirm", "targets_json")},
        "defaults": {"APPLY": "false", "HAVE_BACKUP": "no", "CONFIRM": "", "TARGETS_JSON": ""},
        "calls": [["scripts/repair_grid_capacity.py", "--confirm", "", "--artifacts-dir", "artifacts"]],
    },
    "shadow_scorer": {
        "job": "score", "step": "Score shadow vs champion",
        "inputs": {key.upper(): "${{ github.event.inputs." + key + " }}" for key in ("dry_run", "rollback_drill_passed")},
        "defaults": {"DRY_RUN": "false", "ROLLBACK_DRILL_PASSED": "false"},
        "calls": [["scripts/run_shadow_scorer.py"]],
    },
    "weekly_brief": {
        "job": "brief", "step": "Run weekly brief",
        "inputs": {key.upper(): "${{ github.event.inputs." + key + " }}" for key in ("dry_run", "no_mail")},
        "defaults": {"DRY_RUN": "false", "NO_MAIL": "false"},
        "calls": [["scripts/run_weekly_brief.py"]],
    },
}


class WorkflowDispatchArgumentTests(unittest.TestCase):
    def setUp(self):
        temp = tempfile.TemporaryDirectory(prefix="dispatch-arguments-")
        self.addCleanup(temp.cleanup)
        self.root = Path(temp.name)
        self.bin = self.root / "bin"
        self.bin.mkdir()
        self.calls = self.root / "calls.jsonl"
        wrapper = self.bin / "python"
        wrapper.write_text(f"#!{sys.executable}\n" + textwrap.dedent("""\
            import json, os, sys
            from pathlib import Path
            with Path(os.environ['ARGV_CAPTURE']).open('a') as output:
                output.write(json.dumps(sys.argv[1:]) + '\\n')
            print('offline command boundary')
            raise SystemExit(int(os.environ.get('COMMAND_EXIT', '0')))
            """), encoding="utf-8")
        wrapper.chmod(0o755)

    def workflow_step(self, name):
        case = CASES[name]
        source = (ROOT / ".github/workflows" / (name + ".yml")).read_text(encoding="utf-8")
        return step_source(job_source(source, case["job"]), case["step"])

    def run_wrapper(self, name, *, command_exit=0, **inputs):
        self.calls.unlink(missing_ok=True)
        values = {**CASES[name]["defaults"], **inputs}
        env = {
            "PATH": str(self.bin) + os.pathsep + os.defpath,
            "ARGV_CAPTURE": str(self.calls), "COMMAND_EXIT": str(command_exit),
            "GITHUB_STEP_SUMMARY": str(self.root / "summary.md"),
            **{"INPUT_" + key: value for key, value in values.items()},
        }
        result = subprocess.run(
            ["bash", "--noprofile", "--norc", "-eo", "pipefail", "-c", shell_source(self.workflow_step(name))],
            cwd=self.root, env=env, text=True, capture_output=True, timeout=10,
        )
        calls = [json.loads(line) for line in self.calls.read_text().splitlines()] if self.calls.exists() else []
        return result, calls

    def test_inputs_are_bound_as_environment_data(self):
        for name, case in CASES.items():
            with self.subTest(workflow=name):
                step = self.workflow_step(name)
                for key, expression in case["inputs"].items():
                    self.assertEqual(expression_field(step, "INPUT_" + key, 10), expression)
                self.assertNotIn("${{", shell_source(step))

    def test_default_dispatch_and_schedule_arguments_are_unchanged(self):
        for name, case in CASES.items():
            with self.subTest(workflow=name):
                result, calls = self.run_wrapper(name)
                self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
                self.assertEqual(calls, case["calls"])

    def test_quotes_newlines_and_shell_metacharacters_stay_literal(self):
        value = 'O\'Brien "quoted"\n$(touch injected-input) `touch injected-backtick` $HOME'
        targets = json.dumps({"O'Brien": 200, "$(touch injected-targets)": 100})
        cases = {
            "acceptance_check": ({"BOARD_MIN": value, "STRICT": "true"}, {"--board-min": value}, {"--strict"}),
            "backtest_signals": ({"SINCE": value, "FILTER": "Note=" + value}, {"--since": value, "--filter": "Note=" + value}, set()),
            "thesis_exit_backtest": ({"SCORE": value, "DAYS": value}, {"--score": value, "--days": value}, set()),
            "grid_capacity_repair": ({"APPLY": "true", "HAVE_BACKUP": "yes", "CONFIRM": value, "TARGETS_JSON": targets}, {"--confirm": value, "--targets-json": targets}, {"--apply", "--i-have-backup"}),
            "shadow_scorer": ({"DRY_RUN": value, "ROLLBACK_DRILL_PASSED": value}, {}, set()),
            "weekly_brief": ({"DRY_RUN": value, "NO_MAIL": value}, {}, set()),
        }
        for name, (inputs, arguments, switches) in cases.items():
            with self.subTest(workflow=name):
                result, calls = self.run_wrapper(name, **inputs)
                self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
                for option, expected in arguments.items():
                    self.assertEqual(calls[-1][calls[-1].index(option) + 1], expected)
                self.assertTrue(switches.issubset(calls[-1]))
                if not arguments:
                    # String switch selectors only recognize the exact value
                    # "true"; metacharacters stay data and select no flags.
                    self.assertEqual(calls, CASES[name]["calls"])
                for marker in ("injected-input", "injected-backtick", "injected-targets"):
                    self.assertFalse((self.root / marker).exists())

    def test_string_switches_compose_and_preserve_empty_schedule_inputs(self):
        for name, second, flag in (
            ("shadow_scorer", "ROLLBACK_DRILL_PASSED", "--rollback-drill-passed"),
            ("weekly_brief", "NO_MAIL", "--no-mail"),
        ):
            cases = (
                ("", "", []),
                ("false", "false", []),
                ("true", "false", ["--dry-run"]),
                ("false", "true", [flag]),
                ("true", "true", ["--dry-run", flag]),
            )
            for dry_run, second_value, flags in cases:
                with self.subTest(workflow=name, dry_run=dry_run, second=second_value):
                    result, calls = self.run_wrapper(name, DRY_RUN=dry_run, **{second: second_value})
                    self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
                    self.assertEqual(calls, [CASES[name]["calls"][0] + flags])

    def test_command_refusal_propagates_through_console_capture(self):
        for name in CASES:
            for code in (1, 2, 23):
                with self.subTest(workflow=name, exit_code=code):
                    result, calls = self.run_wrapper(name, command_exit=code)
                    self.assertEqual(result.returncode, code, result.stdout + result.stderr)
                    self.assertEqual(len(calls), 1, "Stop after the failing command")
                    self.assertIn("offline command boundary", result.stdout)


if __name__ == "__main__":
    unittest.main()
