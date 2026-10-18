"""Execute the real Actions target resolution with offline dispatch inputs."""
from __future__ import annotations

import json
import os
from pathlib import Path
import re
import shutil
import subprocess
import tempfile
import unittest

from tests.test_final_publication_workflow import job_source, step_source, shell_source, expression_field

ROOT = Path(__file__).resolve().parents[1]
WORKFLOW = ROOT / '.github/workflows/daily_sync.yml'
PRIMARY_STEP = '⚙️ Smart Configuration Resolution (robust for schedule + manual)'
RECOVERY_STEP = 'Resolve recovery target'
FINAL_STEP = 'Resolve final-audit target'
MANUAL_WORKFLOW = ROOT / '.github/workflows/page_refresh_recovery.yml'
MANUAL_STEP = 'Resolve explicit source target'


class SyncTargetWorkflowTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.source = WORKFLOW.read_text(encoding='utf-8')
        if not shutil.which('bash') or not shutil.which('jq'):
            raise unittest.SkipTest('Actions shell tools bash and jq are required')

    def resolve(self, job, step, values, *, source=None):
        with tempfile.TemporaryDirectory() as tmp:
            output = Path(tmp) / 'github_env'
            output.touch()
            env = {
                'PATH': os.environ.get('PATH', os.defpath),
                'GITHUB_ENV': str(output), 'RUNNER_TEMP': tmp,
                'GITHUB_EVENT_NAME': 'workflow_dispatch',
                'INPUT_MODE': 'full_sync', 'VAR_DEFAULT_KEYS': 'MARKET_LEADERS',
                'SECRET_CREDS': json.dumps({'client_email': 'offline@example.invalid'}),
            }
            env.update(values)
            body = shell_source(step_source(job_source(source or self.source, job), step))
            proc = subprocess.run(['bash', '-c', body], env=env, text=True,
                                  capture_output=True, timeout=20)
            resolved = dict(line.split('=', 1) for line in output.read_text().splitlines())
            return proc, resolved

    def test_recovery_uses_same_dispatch_inputs_as_source_writers(self):
        primary = step_source(job_source(self.source, 'sync-dashboard'), PRIMARY_STEP)
        recovery = step_source(job_source(self.source, 'recover-missing-market-pages'), RECOVERY_STEP)
        for name in ('INPUT_URL', 'INPUT_SHEET', 'VAR_URL', 'VAR_SHEET'):
            self.assertEqual(expression_field(recovery, name, 10), expression_field(primary, name, 10))
        recovery_job = job_source(self.source, 'recover-missing-market-pages')
        self.assertLess(recovery_job.index('- name: ' + RECOVERY_STEP),
                        recovery_job.index('- name: 🔐 Configure production credentials'))
        self.assertLess(recovery_job.index('- name: ' + RECOVERY_STEP),
                        recovery_job.index('- name: 🧯 Recover only pages that did not refresh'))
        final = step_source(job_source(self.source, 'verify-final-decision-publication'), FINAL_STEP)
        for name in ('INPUT_SHEET', 'VAR_SHEET'):
            self.assertEqual(expression_field(final, name, 10), expression_field(primary, name, 10))

    def test_real_source_and_recovery_resolve_identical_target(self):
        cases = [
            ({'INPUT_URL': 'https://alternate.invalid/', 'INPUT_SHEET': 'alternate-sheet',
              'VAR_URL': 'https://production.invalid', 'VAR_SHEET': 'production-sheet'},
             'https://alternate.invalid', 'alternate-sheet'),
            ({'INPUT_URL': '', 'INPUT_SHEET': '',
              'VAR_URL': 'https://production.invalid/', 'VAR_SHEET': 'production-sheet'},
             'https://production.invalid', 'production-sheet'),
            ({'INPUT_URL': 'null', 'INPUT_SHEET': 'undefined',
              'VAR_URL': 'https://production.invalid', 'VAR_SHEET': 'production-sheet'},
             'https://production.invalid', 'production-sheet'),
            ({'INPUT_URL': '', 'INPUT_SHEET': 'alternate-sheet', 'VAR_URL': '', 'VAR_SHEET': ''},
             'https://tadawul-fast-bridge.onrender.com', 'alternate-sheet'),
        ]
        for values, url, sheet in cases:
            with self.subTest(values=values):
                for job, step in (('sync-dashboard', PRIMARY_STEP),
                                  ('recover-missing-market-pages', RECOVERY_STEP),
                                  ('verify-final-decision-publication', FINAL_STEP)):
                    proc, resolved = self.resolve(job, step, values)
                    self.assertEqual(proc.returncode, 0, proc.stdout + proc.stderr)
                    self.assertEqual(resolved['TARGET_SHEET_ID'], sheet)
                    if job != 'verify-final-decision-publication':
                        self.assertEqual(resolved['TARGET_URL'], url)

    def test_missing_sheet_or_invalid_backend_blocks_recovery(self):
        for values in ({'INPUT_URL': 'file:///tmp/fake', 'INPUT_SHEET': 'alternate-sheet'},
                       {'INPUT_URL': 'https://alternate.invalid', 'INPUT_SHEET': '', 'VAR_SHEET': ''}):
            with self.subTest(values=values):
                proc, resolved = self.resolve('recover-missing-market-pages', RECOVERY_STEP, values)
                self.assertNotEqual(proc.returncode, 0)
                self.assertIn('::error::', proc.stdout)
                self.assertNotIn('TARGET_SHEET_ID', resolved)

    def test_manual_recovery_requires_an_explicit_source_target_before_writing(self):
        source = MANUAL_WORKFLOW.read_text(encoding='utf-8')
        for name in ('backend_url', 'sheet_id'):
            control = re.search(r'^      ' + name + r':\n((?:        .*\n)+)', source, re.MULTILINE)
            self.assertIsNotNone(control)
            self.assertEqual(expression_field(control.group(1), 'required', 8), 'true')
            self.assertNotIn('default:', control.group(1))
        job = job_source(source, 'recover-page')
        self.assertLess(job.index('- name: ' + MANUAL_STEP),
                        job.index('- name: Configure production credentials'))
        proc, values = self.resolve('recover-page', MANUAL_STEP,
                                    {'INPUT_URL': 'https://source.invalid/', 'INPUT_SHEET': 'source-sheet',
                                     'VAR_URL': 'https://production.invalid', 'VAR_SHEET': 'production-sheet'},
                                    source=source)
        self.assertEqual(proc.returncode, 0, proc.stdout + proc.stderr)
        self.assertEqual(values, {'TARGET_URL': 'https://source.invalid', 'TARGET_SHEET_ID': 'source-sheet'})

    def test_all_daily_targets_reject_environment_file_injection_and_blank_ids(self):
        for sheet in (' ', 'source sheet', 'source\nBAD_ENV=value', 'source\rBAD_ENV=value'):
            for job, step in (('sync-dashboard', PRIMARY_STEP),
                              ('recover-missing-market-pages', RECOVERY_STEP),
                              ('verify-final-decision-publication', FINAL_STEP)):
                with self.subTest(sheet=sheet, job=job):
                    proc, values = self.resolve(job, step, {
                        'INPUT_URL': 'https://source.invalid', 'INPUT_SHEET': sheet,
                    })
                    self.assertNotEqual(proc.returncode, 0)
                    self.assertNotIn('TARGET_SHEET_ID', values)
                    self.assertNotIn('BAD_ENV', values)
                    self.assertNotIn('GOOGLE_APPLICATION_CREDENTIALS', values)
        for url in ('https://', 'https://source.invalid bad',
                    'https://source.invalid\nBAD_ENV=value', 'https://source.invalid\rBAD_ENV=value'):
            for job, step in (('sync-dashboard', PRIMARY_STEP),
                              ('recover-missing-market-pages', RECOVERY_STEP)):
                with self.subTest(url=url, job=job):
                    proc, values = self.resolve(job, step, {
                        'INPUT_URL': url, 'INPUT_SHEET': 'source-sheet',
                    })
                    self.assertNotEqual(proc.returncode, 0)
                    self.assertEqual(values, {})

    def test_manual_recovery_never_falls_back_to_repository_defaults(self):
        source = MANUAL_WORKFLOW.read_text(encoding='utf-8')
        for url, sheet in (('', ''), ('https://source.invalid', ''), ('', 'source-sheet'),
                           ('https://source.invalid', 'null'), ('https://source.invalid', 'undefined'),
                           ('file:///tmp/fake', 'source-sheet'),
                           ('https://source.invalid\nBAD_ENV=value', 'source-sheet'),
                           ('https://source.invalid', 'source-sheet\nBAD_ENV=value')):
            with self.subTest(url=url, sheet=sheet):
                proc, values = self.resolve('recover-page', MANUAL_STEP,
                                            {'INPUT_URL': url, 'INPUT_SHEET': sheet,
                                             'VAR_URL': 'https://production.invalid', 'VAR_SHEET': 'production-sheet'},
                                            source=source)
                self.assertNotEqual(proc.returncode, 0)
                self.assertIn('::error::', proc.stdout)
                self.assertEqual(values, {})


if __name__ == '__main__':
    unittest.main()
