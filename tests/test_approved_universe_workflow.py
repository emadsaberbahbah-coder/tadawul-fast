"""Owner-approved current roster policy reaches every real audit workflow.

Execute the workflow acceptance wrappers offline, including warning/fatal
results. No credentials, provider requests or workbook writes are involved.
"""
from __future__ import annotations

import fnmatch
import os
from pathlib import Path
import subprocess

import pytest
import yaml

ROOT = Path(__file__).resolve().parents[1]
WORKFLOWS = ROOT / '.github' / 'workflows'
SCOPE_PATH = 'config/approved_universe_scope_v1.json'
DEDICATED = ('full_refresh_coverage.yml', 'decision_surface_freshness.yml',
             'workflow_repair_tests.yml')
JOBS = (
    ('full_refresh_coverage.yml', 'audit-live-workbook'),
    ('decision_surface_freshness.yml', 'audit-live-surfaces'),
    ('workflow_repair_tests.yml', 'verify-live'),
    ('daily_sync.yml', 'verify-final-decision-publication'),
)
FLOORS = {'MARKET_LEADERS': '255', 'GLOBAL_MARKETS': '6512',
          'COMMODITIES_FX': '453', 'MUTUAL_FUNDS': '2474'}


class UniqueLoader(yaml.BaseLoader):
    pass


def unique_mapping(loader, node, deep=False):
    result = {}
    for key_node, value_node in node.value:
        key = loader.construct_object(key_node, deep=deep)
        assert key not in result, f'Duplicate workflow key: {key}'
        result[key] = loader.construct_object(value_node, deep=deep)
    return result


UniqueLoader.add_constructor(yaml.resolver.BaseResolver.DEFAULT_MAPPING_TAG, unique_mapping)


def workflow(name):
    return yaml.load((WORKFLOWS / name).read_text(), Loader=UniqueLoader)


def step_with_script(name, job_name, script):
    steps = workflow(name)['jobs'][job_name]['steps']
    matches = [step for step in steps if script in step.get('run', '')]
    assert len(matches) == 1, (name, job_name, script, len(matches))
    return matches[0]


@pytest.mark.parametrize('name,job_name', JOBS)
def test_approved_scope_fallbacks_bound_to_each_live_audit_without_disable_switch(name, job_name):
    job = workflow(name)['jobs'][job_name]
    for page, floor in FLOORS.items():
        env_name = 'TFB_EXPECTED_MIN_ROWS_' + page
        assert job['env'][env_name] == "${{ vars.EXPECTED_MIN_ROWS_" + page + " || '" + floor + "' }}"
    assert 'TFB_APPROVED_UNIVERSE_SCOPE' not in job['env']
    assert job['environment'] == 'production'
    assert '2026-10-12' in (WORKFLOWS / name).read_text()


@pytest.mark.parametrize('name', DEDICATED)
def test_scope_config_helper_and_both_regressions_trigger_each_dedicated_ci(name):
    triggers = workflow(name)['on']
    paths = (SCOPE_PATH, 'scripts/approved_universe_scope.py',
             'tests/test_approved_universe_scope.py', 'tests/test_approved_universe_workflow.py')
    for event in ('push', 'pull_request'):
        patterns = triggers[event]['paths']
        for path in paths:
            assert any(fnmatch.fnmatchcase(path, pattern) for pattern in patterns), (name, event, path)
    regression = workflow(name)['jobs']['regression' if name == 'workflow_repair_tests.yml' else 'regression-tests']
    commands = '\n'.join(step.get('run', '') for step in regression['steps'])
    assert 'tests/test_approved_universe_scope.py' in commands
    assert 'tests/test_approved_universe_workflow.py' in commands
    assert 'pytest==9.1.1' in commands and 'pyyaml==6.0.3' in commands
    assert 'GOOGLE_SHEETS_CREDENTIALS' not in str(regression)


def test_freshness_name_price_and_custody_guards_keep_existing_thresholds():
    full = workflow('full_refresh_coverage.yml')['jobs']['audit-live-workbook']['env']
    for page in ('MARKET_LEADERS', 'GLOBAL_MARKETS', 'COMMODITIES_FX', 'MUTUAL_FUNDS'):
        assert full['TFB_REFRESH_MIN_FRESH_PCT_' + page] == '95'
        assert full['TFB_REFRESH_MAX_AGE_H_' + page] == '30'
    assert full['TFB_REFRESH_MIN_FRESH_PCT_MY_PORTFOLIO'] == '100'
    assert full['TFB_REFRESH_MAX_AGE_H_MY_PORTFOLIO'] == '8'
    for name, job in JOBS[1:]:
        env = workflow(name)['jobs'][job]['env']
        assert env['TFB_DECISION_SOURCE_MAX_AGE_H'] == '30'
        assert env['TFB_DECISION_SURFACE_MAX_AGE_H'] == '8'
    final = workflow('daily_sync.yml')['jobs']['verify-final-decision-publication']
    assert final['needs'] == ['sync-dashboard', 'recover-missing-market-pages']
    assert "github.event.inputs.run_mode == 'full_sync'" in final['if']
    assert 'github.event_name == \'schedule\'' in final['if']
    assert '--publish-blockers' in step_with_script('daily_sync.yml',
        'verify-final-decision-publication', 'scripts/finalize_decision_publication.py')['run']
    verification = workflow('workflow_repair_tests.yml')['jobs']['verify-live']
    assert verification['env']['TFB_PTC_FREEZE'] == '1'
    assert verification['env']['TFB_PTC_BOOTSTRAP'] == '0'
    assert verification['env']['TFB_PTC_ACCEPT_BASELINE'] == '0'
    assert all(step.get('uses') != 'actions/cache/save@v4' for step in verification['steps'])


def test_audit_live_jobs_remain_manual_or_completed_main_sync_only():
    for name, job in JOBS[:2]:
        condition = workflow(name)['jobs'][job]['if']
        assert "github.event_name == 'workflow_dispatch'" in condition
        assert "github.event_name == 'workflow_run'" in condition
        assert "github.event.workflow_run.head_branch == 'main'" in condition
        assert "github.event.workflow_run.event == 'schedule'" in condition
        assert "github.event.workflow_run.event == 'workflow_dispatch'" in condition
    verification = workflow('workflow_repair_tests.yml')['jobs']['verify-live']
    assert "github.ref == 'refs/heads/main'" in verification['if']
    assert verification['needs'] == 'regression'


WRAPPERS = (
    ('full_refresh_coverage.yml', 'audit-live-workbook', 'scripts/audit_full_refresh_coverage.py'),
    ('decision_surface_freshness.yml', 'audit-live-surfaces', 'scripts/audit_decision_surface_freshness.py'),
    ('daily_sync.yml', 'verify-final-decision-publication', 'scripts/finalize_decision_publication.py'),
)


@pytest.mark.parametrize('name,job,script', WRAPPERS)
@pytest.mark.parametrize('code', (0, 1, 2, 3, 124))
def test_actual_live_shell_wrapper_accepts_only_zero(name, job, script, code, tmp_path):
    path = tmp_path / script
    path.parent.mkdir()
    path.write_text('import os, sys\nsys.exit(int(os.environ["OFFLINE_AUDIT_EXIT"]))\n')
    env = dict(os.environ, TARGET_SHEET_ID='offline-scope',
               TFB_REFRESH_AUDIT_MAX_ROWS='20000', GITHUB_OUTPUT=str(tmp_path / 'outputs'),
               OFFLINE_AUDIT_EXIT=str(code))
    result = subprocess.run(['bash', '-c', step_with_script(name, job, script)['run']],
                            cwd=tmp_path, env=env, text=True, capture_output=True)
    assert result.returncode == code, result.stdout + result.stderr
    assert (tmp_path / 'outputs').read_text().strip() == f'audit_exit={code}'


@pytest.mark.parametrize('code,report_present', ((0, True), (1, True), (2, True), (0, False)))
def test_actual_combined_verification_rejects_warnings_failures_or_missing_report(code, report_present, tmp_path):
    scripts = tmp_path / 'scripts'
    scripts.mkdir()
    for script in ('audit_full_refresh_coverage.py', 'audit_decision_surface_freshness.py',
                   'audit_provider_target_coverage.py'):
        (scripts / script).write_text('''import os, sys
from pathlib import Path
if os.environ['OFFLINE_REPORT_PRESENT'] == 'true':
    Path(sys.argv[sys.argv.index('--json-out') + 1]).write_text('{}')
sys.exit(int(os.environ['OFFLINE_AUDIT_EXIT']))
''')
    env = dict(os.environ, DEFAULT_SPREADSHEET_ID='offline-scope', GITHUB_SHA='offline-commit',
               GITHUB_STEP_SUMMARY=str(tmp_path / 'summary'), OFFLINE_AUDIT_EXIT=str(code),
               OFFLINE_REPORT_PRESENT=str(report_present).lower())
    step = step_with_script('workflow_repair_tests.yml', 'verify-live',
                            'scripts/audit_full_refresh_coverage.py')
    result = subprocess.run(['bash', '-c', step['run']], cwd=tmp_path, env=env,
                            text=True, capture_output=True)
    assert result.returncode == (0 if code == 0 and report_present else 1), result.stdout + result.stderr
