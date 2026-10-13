"""Exercise the real grid repair workflow wrapper without workbook access."""
from __future__ import annotations

import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import textwrap
import unittest

from tests.test_final_publication_workflow import job_source, step_source, shell_source, expression_field

ROOT = Path(__file__).resolve().parents[1]
WORKFLOW = ROOT / '.github/workflows/grid_capacity_repair.yml'


class GridCapacityWorkflowTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(prefix='grid-workflow-')
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        self.bin = self.root / 'bin'
        self.bin.mkdir()
        self.call = self.root / 'call.json'
        wrapper = self.bin / 'python'
        wrapper.write_text(f'#!{sys.executable}\n' + textwrap.dedent('''\
            import json, os, sys
            from pathlib import Path
            Path(os.environ['GRID_CALL']).write_text(json.dumps(sys.argv[1:]))
            print('offline repair tool')
            raise SystemExit(int(os.environ.get('GRID_EXIT', '0')))
            '''), encoding='utf-8')
        wrapper.chmod(0o755)
        self.step = step_source(job_source(WORKFLOW.read_text(encoding='utf-8'), 'grid-repair'),
                                'Run repair tool')
        self.body = shell_source(self.step)

    def run_wrapper(self, **values):
        env = {
            'PATH': str(self.bin) + os.pathsep + os.defpath,
            'GRID_CALL': str(self.call), 'INPUT_APPLY': 'false',
            'INPUT_HAVE_BACKUP': 'no', 'INPUT_CONFIRM': '', 'INPUT_TARGETS_JSON': '',
        }
        env.update(values)
        return subprocess.run(['bash', '-c', self.body], cwd=self.root, env=env,
                              text=True, capture_output=True, timeout=20)

    def test_dispatch_inputs_reach_shell_through_environment(self):
        for key in ('apply', 'have_backup', 'confirm', 'targets_json'):
            self.assertEqual(expression_field(self.step, 'INPUT_' + key.upper(), 10),
                             '${{ inputs.' + key + ' }}')
        self.assertNotIn('${{', self.body)

    def test_default_dry_run_retains_tool_safety_arguments(self):
        proc = self.run_wrapper()
        self.assertEqual(proc.returncode, 0, proc.stdout + proc.stderr)
        self.assertEqual(json.loads(self.call.read_text()),
                         ['scripts/repair_grid_capacity.py', '--confirm', '', '--artifacts-dir', 'artifacts'])

    def test_apply_inputs_and_json_are_preserved_without_shell_execution(self):
        confirm = 'RESIZE"; touch injected-confirm; #'
        targets = '{"O\'Brien": 200, "$(touch injected-targets)": 100}'
        proc = self.run_wrapper(INPUT_APPLY='true', INPUT_HAVE_BACKUP='yes',
                                INPUT_CONFIRM=confirm, INPUT_TARGETS_JSON=targets)
        self.assertEqual(proc.returncode, 0, proc.stdout + proc.stderr)
        self.assertEqual(json.loads(self.call.read_text()),
                         ['scripts/repair_grid_capacity.py', '--apply', '--i-have-backup',
                          '--confirm', confirm, '--targets-json', targets, '--artifacts-dir', 'artifacts'])
        self.assertFalse((self.root / 'injected-confirm').exists())
        self.assertFalse((self.root / 'injected-targets').exists())

    def test_tool_safety_refusal_propagates_through_console_capture(self):
        proc = self.run_wrapper(GRID_EXIT='2', INPUT_APPLY='true')
        self.assertEqual(proc.returncode, 2, proc.stdout + proc.stderr)
        self.assertIn('offline repair tool', (self.root / 'artifacts_console.txt').read_text())


if __name__ == '__main__':
    unittest.main()
