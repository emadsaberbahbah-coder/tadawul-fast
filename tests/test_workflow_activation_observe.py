"""Execute per-run observe wrappers and preserve real historical tracker rows."""
from __future__ import annotations

import asyncio
import copy
from datetime import datetime, timedelta, timezone
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest
import yaml

from scripts import workflow_activation_receipt as receipt

ROOT = Path(__file__).resolve().parents[1]


def workflow(name):
    return yaml.load((ROOT / '.github/workflows' / name).read_text(), Loader=yaml.BaseLoader)


def step(name, label):
    jobs = workflow(name)['jobs'].values()
    matches = [item for job in jobs for item in job['steps'] if label in item.get('name', '')]
    assert len(matches) == 1, (name, label)
    return matches[0]


@pytest.mark.parametrize('explicit,repository,expected', (
    ('', '', 'off'), ('inherit', '', 'off'), ('inherit', 'observe', 'observe'),
    ('inherit', 'enforce', 'enforce'), ('', 'invalid', 'off'),
    ('observe', '', 'observe'), ('observe', 'enforce', 'observe'), ('off', 'observe', 'off'),
))
def test_actual_dq_resolver_preserves_scheduled_defaults_and_manual_observe(explicit, repository, expected, tmp_path):
    env = dict(os.environ, INPUT_DQ_MODE=explicit, REPOSITORY_DQ_MODE=repository,
               GITHUB_OUTPUT=str(tmp_path / 'outputs'))
    run = subprocess.run(['bash', '-c', step('workbook_backup.yml', "Resolve this run's DQ mode")['run']],
                         cwd=tmp_path, env=env, capture_output=True, text=True)
    assert run.returncode == 0, run.stderr
    assert (tmp_path / 'outputs').read_text() == f'mode={expected}\n'


@pytest.mark.parametrize('value', ('enforce', 'invalid', '$(touch injected)'))
def test_manual_dq_resolver_refuses_unsupported_or_injected_mode(value, tmp_path):
    env = dict(os.environ, INPUT_DQ_MODE=value, REPOSITORY_DQ_MODE='',
               GITHUB_OUTPUT=str(tmp_path / 'outputs'))
    run = subprocess.run(['bash', '-c', step('workbook_backup.yml', "Resolve this run's DQ mode")['run']],
                         cwd=tmp_path, env=env, capture_output=True, text=True)
    assert run.returncode == 2
    assert not (tmp_path / 'injected').exists()
    assert not (tmp_path / 'outputs').exists()


def test_one_dq_mode_controls_export_execution_report_and_soft_failure():
    backup = step('workbook_backup.yml', 'Run backup')
    audit = step('workbook_backup.yml', 'Workbook DQ audit (WP-0)')
    upload = step('workbook_backup.yml', 'Upload DQ report')
    assert 'steps.dq_mode.outputs.mode' in backup['env']['TFB_BACKUP_LOCAL_COPY']
    assert audit['env']['TFB_DQ_AUDIT_MODE'] == '${{ steps.dq_mode.outputs.mode }}'
    assert audit['continue-on-error'] == "${{ steps.dq_mode.outputs.mode != 'enforce' }}"
    for item in (audit, upload):
        assert 'steps.dq_mode.outputs.mode' in item['if']
        assert "restore_test != 'true'" in item['if']
    assert 'dq_activation_receipt.json' in upload['with']['path']
    assert '.xlsx' not in upload['with']['path']
    for name, input_name in (('workbook_backup.yml', 'dq_audit_mode'),
                             ('track_performance.yml', 'signal_history_mode')):
        parameter = workflow(name)['on']['workflow_dispatch']['inputs'][input_name]
        assert parameter['default'] == 'inherit'
        assert parameter['options'] == ['inherit', 'off', 'observe']


@pytest.mark.parametrize('name,label', (
    ('workbook_backup.yml', 'Workbook DQ audit (WP-0)'),
    ('track_performance.yml', 'Run performance clock'),
))
def test_receipt_identity_comes_from_actual_checkout_output(name, label):
    checkouts = [item for job in workflow(name)['jobs'].values() for item in job['steps']
                 if item.get('uses') == 'actions/checkout@v6']
    assert len(checkouts) == 1
    checkout = checkouts[0]
    assert checkout['id'] == 'source_checkout'
    assert step(name, label)['env']['TFB_CHECKOUT_SHA'] == '${{ steps.source_checkout.outputs.commit }}'
    # Preserve each workflow's source selection, including manual dispatch refs.
    if name == 'workbook_backup.yml':
        assert checkout['with']['ref'] == 'main'
    else:
        assert 'ref' not in checkout['with']


@pytest.fixture
def wrapper_repo(tmp_path):
    scripts = tmp_path / 'scripts'
    scripts.mkdir()
    shutil.copy(ROOT / 'scripts/workflow_activation_receipt.py', scripts / 'workflow_activation_receipt.py')
    (scripts / 'backup_workbook.py').write_text('__version__ = "1.5.0"\n')
    (scripts / 'tfb_workbook_dq_audit.py').write_text('''import hashlib, json, os, sys
from pathlib import Path
SCRIPT_VERSION = "1.0.1"
export = Path(sys.argv[1])
code = int(os.getenv('OFFLINE_AUDIT_EXIT', '0'))
if os.getenv('OFFLINE_SKIP_REPORT') != '1':
    Path('dq_report.json').write_text(json.dumps({'version':SCRIPT_VERSION,
        'mode':os.environ['TFB_DQ_AUDIT_MODE'], 'export_sha256':hashlib.sha256(export.read_bytes()).hexdigest(),
        'cash_balance':999999, 'private_key':'NEVER_EXPORT_SECRET'}))
    Path('dq_scorecard.md').write_text('offline scorecard')
sys.exit(code)
''')
    (scripts / 'track_performance.py').write_text('''import json, os, sys
from pathlib import Path
SCRIPT_VERSION = "6.45.0"
if '--help' in sys.argv:
    print('Tadawul Fast Bridge - Performance Tracker v6.45.0')
    sys.exit(0)
Path('actual_tracker_call.json').write_text(json.dumps({'args':sys.argv[1:],
 'mode':os.getenv('TRACK_DEDUP_SIGNAL_HISTORY','off'),
 'audit_env':os.getenv('TRACK_AUDIT'), 'calibration_publish':os.getenv('TRACK_PUBLISH_CALIBRATION'),
 'trends':os.getenv('TFB_SIGNAL_TRENDS_TAB'),'summary_side':os.getenv('TFB_TRACK_SUMMARY_SIDE')}))
mode=os.getenv('TRACK_DEDUP_SIGNAL_HISTORY','off')
if mode in ('observe','enforce'):
    print('[v6.44.0 DEDUP-SIGNAL] mode='+mode+' would_skip=1/2 new=1 | sheet rows=6 keys=3 backlog=3 max_per_key=3 | sample=ignored')
print('record_signal_snapshots: wrote 2 new daily snapshot(s)')
sys.exit(int(os.getenv('OFFLINE_AUDIT_EXIT','0')))
''')
    subprocess.run(['git', 'init', '-q'], cwd=tmp_path, check=True)
    subprocess.run(['git', 'add', 'scripts'], cwd=tmp_path, check=True)
    subprocess.run(['git', '-c', 'user.name=Offline Fixture', '-c', 'user.email=offline@example.invalid',
                    'commit', '-qm', 'offline source identity'], cwd=tmp_path, check=True)
    source = subprocess.check_output(['git', 'rev-parse', 'HEAD'], cwd=tmp_path, text=True).strip()
    runner = tmp_path / 'runner'
    runner.mkdir()
    (runner / 'tfb_export.xlsx').write_bytes(b'offline current export')
    return tmp_path, dict(os.environ, GITHUB_SHA=source, TFB_CHECKOUT_SHA=source,
        GITHUB_RUN_ID='12345', GITHUB_RUN_ATTEMPT='1',
        RUNNER_TEMP=str(runner), GITHUB_STEP_SUMMARY=str(tmp_path / 'summary'),
        TARGET_SHEET_ID='offline-sheet', GOOGLE_SHEETS_CREDENTIALS='NEVER_EXPORT_SECRET',
        BACKEND_TOKEN='NEVER_EXPORT_TOKEN', TRACK_CANONICAL_TARGETS='0',
        TRACK_SIGNAL_PUBLICATION_MODE='off', TFB_PERF_TARGET_UNIT_SENTRY='observe')


@pytest.mark.parametrize('code', (0, 1))
def test_actual_dq_audit_wrapper_exports_verified_metadata_without_private_report_fields(wrapper_repo, code):
    root, env = wrapper_repo
    run = subprocess.run(['bash', '-c', step('workbook_backup.yml', 'Workbook DQ audit (WP-0)')['run']],
        cwd=root, env=env | {'TFB_DQ_AUDIT_MODE':'observe', 'OFFLINE_AUDIT_EXIT':str(code)},
        capture_output=True, text=True)
    assert run.returncode == code, run.stderr
    value = json.loads((root / 'dq_activation_receipt.json').read_text())
    assert value['reported_mode'] == value['selected_mode'] == 'observe'
    assert value['reported_version'] == '1.0.1'
    assert value['source_matches_event'] is True
    assert value['source_matches_checkout'] is True
    assert value['source_sha'] == value['checkout_sha'] == env['TFB_CHECKOUT_SHA']
    assert value['workflow_run_id'] == 12345 and value['execution_evidence_complete'] is True
    assert value['sheet_readback_verified'] is value['data_quality_accepted'] is None
    rendered = json.dumps(value)
    for forbidden in ('999999', 'NEVER_EXPORT_SECRET', 'cash_balance', 'private_key', 'offline-sheet'):
        assert forbidden not in rendered


def test_zero_exit_without_dq_report_is_not_execution_proof(wrapper_repo):
    root, env = wrapper_repo
    run = subprocess.run(['bash', '-c', step('workbook_backup.yml', 'Workbook DQ audit (WP-0)')['run']],
        cwd=root, env=env | {'TFB_DQ_AUDIT_MODE':'observe', 'OFFLINE_SKIP_REPORT':'1'}, capture_output=True, text=True)
    assert run.returncode == 2
    assert json.loads((root / 'dq_activation_receipt.json').read_text())['execution_evidence_complete'] is False


@pytest.mark.parametrize('change', ('source', 'version', 'mode', 'export'))
def test_dq_receipt_rejects_source_mode_version_or_input_digest_mismatch(wrapper_repo, change):
    root, env = wrapper_repo
    export = Path(env['RUNNER_TEMP']) / 'tfb_export.xlsx'
    report = {'version':'1.0.1','mode':'observe',
              'export_sha256':hashlib.sha256(export.read_bytes()).hexdigest()}
    env['TFB_DQ_AUDIT_MODE'] = 'observe'
    if change == 'source': env['TFB_CHECKOUT_SHA'] = '0' * 40
    elif change == 'version': report['version'] = '1.0.0'
    elif change == 'mode': report['mode'] = 'off'
    else: report['export_sha256'] = '0' * 64
    (root / 'dq_report.json').write_text(json.dumps(report))
    assert receipt.dq_receipt(0, root=root, env=env)['execution_evidence_complete'] is False


@pytest.mark.parametrize('kind', ('dq', 'tracker'))
def test_actual_wrappers_accept_checkout_commit_different_from_event(wrapper_repo, kind):
    root, env = wrapper_repo
    env |= {'GITHUB_SHA':'0' * 40, 'TFB_DQ_AUDIT_MODE':'observe',
            'INPUT_SIGNAL_HISTORY_MODE':'observe', 'INPUT_RUN_AUDIT':'false'}
    name, label = ('workbook_backup.yml', 'Workbook DQ audit (WP-0)') if kind == 'dq' else (
        'track_performance.yml', 'Run performance clock')
    block = step(name, label)['run'].replace('${{ github.event.inputs.enable_debug }}', 'false')
    run = subprocess.run(['bash', '-c', block], cwd=root, env=env, capture_output=True, text=True)
    assert run.returncode == 0, run.stderr
    path = 'dq_activation_receipt.json' if kind == 'dq' else 'tracker_activation_receipt.json'
    value = json.loads((root / path).read_text())
    assert value['source_sha'] == value['checkout_sha'] == env['TFB_CHECKOUT_SHA']
    assert value['event_sha'] == env['GITHUB_SHA']
    assert value['source_matches_event'] is False
    assert value['source_matches_checkout'] is value['execution_evidence_complete'] is True


def current_lane_receipt(kind, root, env):
    """Create valid execution artifacts so identity is the only failing guard."""
    if kind == 'dq':
        export = Path(env['RUNNER_TEMP']) / 'tfb_export.xlsx'
        (root / 'dq_report.json').write_text(json.dumps({'version':'1.0.1','mode':'observe',
            'export_sha256':hashlib.sha256(export.read_bytes()).hexdigest()}))
        return receipt.dq_receipt(0, root=root, env=env | {'TFB_DQ_AUDIT_MODE':'observe'})
    help_path, log = root / 'help.txt', root / 'log.txt'
    help_path.write_text('Performance Tracker v6.45.0')
    log.write_text('record_signal_snapshots: wrote 2 new daily snapshot(s)')
    return receipt.tracker_receipt(0, log, help_path, False, root=root,
                                   env=env | {'TRACK_DEDUP_SIGNAL_HISTORY':'off'})


@pytest.mark.parametrize('kind', ('dq', 'tracker'))
@pytest.mark.parametrize('witness', ('missing', '', 'not-a-commit', 'whitespace', '0' * 40))
def test_receipts_reject_missing_malformed_or_wrong_checkout_even_when_event_matches(wrapper_repo, kind, witness):
    root, env = wrapper_repo
    if witness == 'missing':
        env.pop('TFB_CHECKOUT_SHA')
    else:
        env['TFB_CHECKOUT_SHA'] = ' ' + env['GITHUB_SHA'] if witness == 'whitespace' else witness
    value = current_lane_receipt(kind, root, env)
    assert value['source_matches_event'] is True
    assert value['source_matches_checkout'] is value['execution_evidence_complete'] is False
    assert value['checkout_sha'] == ('0' * 40 if witness == '0' * 40 else None)


@pytest.mark.parametrize('kind', ('dq', 'tracker'))
def test_receipts_reject_head_drift_after_checkout_even_when_event_matches(wrapper_repo, kind):
    root, env = wrapper_repo
    subprocess.run(['git', '-c', 'user.name=Offline Fixture', '-c', 'user.email=offline@example.invalid',
                    'commit', '--allow-empty', '-qm', 'post-checkout drift'], cwd=root, check=True)
    env['GITHUB_SHA'] = subprocess.check_output(['git', 'rev-parse', 'HEAD'], cwd=root, text=True).strip()
    value = current_lane_receipt(kind, root, env)
    assert value['source_matches_event'] is True
    assert value['source_sha'] != value['checkout_sha'] == env['TFB_CHECKOUT_SHA']
    assert value['source_matches_checkout'] is value['execution_evidence_complete'] is False


@pytest.mark.parametrize('explicit,repository,effective', (
    ('', 'off', 'off'), ('inherit', 'observe', 'observe'), ('inherit', 'enforce', 'enforce'),
    ('observe', 'off', 'observe'), ('observe', 'enforce', 'observe'), ('off', 'observe', 'off'),
))
@pytest.mark.parametrize('audit', ('true', 'false'))
def test_actual_tracker_wrapper_observe_is_measurement_and_noaudit_blocks_history_save(wrapper_repo, explicit, repository, effective, audit):
    root, env = wrapper_repo
    run_block = step('track_performance.yml', 'Run performance clock')['run'].replace(
        '${{ github.event.inputs.enable_debug }}', 'false')
    env |= {'INPUT_SIGNAL_HISTORY_MODE':explicit,'INPUT_RUN_AUDIT':audit,
            'TRACK_DEDUP_SIGNAL_HISTORY':repository,'TRACK_AUDIT':'1'}
    run = subprocess.run(['bash', '-c', run_block], cwd=root, env=env, capture_output=True, text=True)
    assert run.returncode == 0, run.stderr
    actual = json.loads((root / 'actual_tracker_call.json').read_text())
    assert actual['mode'] == effective
    assert ('--audit' in actual['args']) is (audit == 'true')
    if audit == 'false':
        assert actual['audit_env'] == actual['calibration_publish'] == actual['trends'] == '0'
        assert actual['summary_side'] == '1'
    value = json.loads((root / 'tracker_activation_receipt.json').read_text())
    assert value['source_matches_checkout'] is value['execution_evidence_complete'] is True
    assert value['cli_version'] == value['source_versions']['track_performance'] == '6.45.0'
    assert value['signal_history_mode'] == effective
    assert value['same_day_deduplication_applied'] is False
    assert value['historical_grid_save_requested'] is (audit == 'true')
    assert value['sheet_readback_verified'] is value['historical_prefix_verified'] is None
    assert value['signal_snapshot_append_acknowledged'] == 2
    rendered = json.dumps(value)
    for forbidden in ('NEVER_EXPORT_SECRET','NEVER_EXPORT_TOKEN','offline-sheet','private_key','cash_balance'):
        assert forbidden not in rendered


def test_tracker_manual_enforce_is_refused_before_execution(wrapper_repo):
    root, env = wrapper_repo
    block = step('track_performance.yml', 'Run performance clock')['run'].replace(
        '${{ github.event.inputs.enable_debug }}', 'false')
    run = subprocess.run(['bash','-c',block],cwd=root,
        env=env|{'INPUT_SIGNAL_HISTORY_MODE':'enforce','INPUT_RUN_AUDIT':'false'},capture_output=True,text=True)
    assert run.returncode == 2
    assert not (root / 'actual_tracker_call.json').exists()


def test_tracker_receipt_rejects_missing_measurement_and_logged_append_failure(wrapper_repo):
    root, env = wrapper_repo
    help_path = root / 'help.txt'
    help_path.write_text('Performance Tracker v6.45.0')
    log = root / 'log.txt'
    log.write_text('record_signal_snapshots: wrote 2 new daily snapshot(s)')
    value = receipt.tracker_receipt(0, log, help_path, False, root=root,
                                    env=env|{'TRACK_DEDUP_SIGNAL_HISTORY':'observe'})
    assert value['execution_evidence_complete'] is False
    log.write_text('[v6.44.0 DEDUP-SIGNAL] mode=observe would_skip=1/2 new=1 | sheet rows=6 keys=3 backlog=3 max_per_key=3\n[EVIDENCE-APPEND v6.34.0] FAILED')
    value = receipt.tracker_receipt(0, log, help_path, False, root=root,
                                    env=env|{'TRACK_DEDUP_SIGNAL_HISTORY':'observe'})
    assert value['append_failure_logged'] is True and value['execution_evidence_complete'] is False


def test_real_record_analyze_path_preserves_history_and_never_calls_audit_or_save(monkeypatch):
    from scripts import track_performance as tracker
    now = datetime(2026,10,12,8,tzinfo=timezone(timedelta(hours=3)))
    monkeypatch.setattr(tracker.RiyadhTime,'now',classmethod(lambda cls:now))
    for name,value in {'TFB_TRACK_FORCE_DECISION_SYMBOLS':'0','TFB_TRACK_SHADOW_COHORTS':'0',
        'TRACK_DEDUP_SIGNAL_HISTORY':'observe','TRACK_SIGNAL_PUBLICATION_MODE':'off',
        'TRACK_CANONICAL_TARGETS':'0','TRACK_PUBLISH_CALIBRATION':'0','TFB_TRACK_SUMMARY_SIDE':'1'}.items():
        monkeypatch.setenv(name,value)
    snapshots = {name:copy.deepcopy(getattr(tracker,name)) for name in
        ('_LAST_DEDUP_SIGNAL','_LAST_LOAD_STATS','_PERF_UNIT_CREATION','_EVIDENCE_APPEND_FAILURES')}
    class Backoff:
        def execute_sync(self,call,*args,**kwargs): return call(*args,**kwargs)
    class WS:
        def __init__(self,rows,signal=False):
            self.rows=copy.deepcopy(rows);self.row_count=100;self.signal=signal;self.writes=[]
        def get(self,range_name):
            return [[row[1]] for row in self.rows] if range_name.startswith('B') else copy.deepcopy(self.rows)
        def append_rows(self,rows,**kwargs):
            self.writes.append(('append',copy.deepcopy(rows)));self.rows.extend(copy.deepcopy(rows))
        def batch_clear(self,ranges):
            assert ranges==['A1:E4','AG1:AK13'],'historical grid clear requested'
            self.writes.append(('summary_clear',ranges))
        def update(self,values,range_name,**kwargs):
            assert range_name=='AG1:AK13','historical grid update requested'
            self.writes.append(('summary_update',range_name))
    def record(symbol,recorded,target):
        return tracker.PerformanceRecord(record_id='unchanged-'+symbol,symbol=symbol,
            horizon=tracker.HorizonType.MONTH_1,date_recorded=recorded,entry_price=10,
            entry_recommendation=tracker.RecommendationType.BUY,entry_score=70,
            entry_risk_bucket='LOW',entry_confidence='HIGH',origin_tab='test',target_price=11,
            target_roi=0.1,target_date=target,status=tracker.PerformanceStatus.ACTIVE,
            current_price=10,notes='historical note must remain byte-identical')
    perf=object.__new__(tracker.PerformanceStore)
    perf.backoff=Backoff();perf.cache={};perf.cache_lock=tracker.Lock();perf.sheet=None
    perf.spreadsheet_id='offline';perf._ws_book=None;perf._ws_sid=None
    old=[record('AAA.US',now,now+timedelta(days=30)),
         record('DDD.US',now-timedelta(days=45),now-timedelta(days=15))]
    duplicate = copy.deepcopy(old[0])
    duplicate.record_id = 'duplicate-active-history-must-not-be-rewritten'
    old.append(duplicate)
    perf.ws=WS([perf._record_to_row(item) for item in old])
    perf.save_records=Mock(wraps=perf.save_records)
    signal=object.__new__(tracker.SignalHistoryStore)
    signal.backoff=Backoff();signal.cache_keys=set();signal.cache_lock=tracker.Lock();signal.sheet=None
    signal.spreadsheet_id='offline';signal._ws_book=None;signal._ws_sid=None
    old_snap=tracker.SignalSnapshot('old-snapshot','AAA.US',now-timedelta(days=1),
        tracker.RecommendationType.BUY,'INVEST','WATCHLIST',70,50,90,20,10,'test')
    signal.ws=WS([signal._snapshot_to_row(old_snap)],signal=True)
    perf_before=copy.deepcopy(perf.ws.rows);signal_before=copy.deepcopy(signal.ws.rows)
    class Backend:
        base_url='https://offline.invalid'
        async def get_top10_rows(self,**kwargs):
            return [{'symbol':symbol,'current_price':10,'forecast_price_1m':11,
                'expected_roi_1m':0.1,'recommendation':'BUY'} for symbol in ('AAA.US','BBB.US')],{}
    app=object.__new__(tracker.PerformanceTrackerApp)
    app.store=perf;app.signal_store=signal;app.backend=Backend();app.signal_history_enabled=True
    app.args=SimpleNamespace(record=True,audit=False,analyze=True,export=False,calibrate=False,
                            simulate=False,max_records=100,horizons=['1M'],verbose=False)
    app.signal_trends_enabled=False;app.signal_trend_store=None;app.trend_window=5
    app.trend_analyzer=tracker.SignalTrendAnalyzer();app.analyzer=tracker.PerformanceAnalyzer()
    app.calibration_enabled=True;app.calibrator=tracker.ReliabilityCalibrator(10,20)
    app.audit_active_records=AsyncMock(wraps=app.audit_active_records)
    app._publish_s1_calibration=Mock(wraps=app._publish_s1_calibration)
    app._append_runlog_verdict=Mock(wraps=app._append_runlog_verdict)
    try:
        assert asyncio.run(app.run_once())==0
        app.audit_active_records.assert_not_called();perf.save_records.assert_not_called()
        app._publish_s1_calibration.assert_not_called();app._append_runlog_verdict.assert_not_called()
        assert perf.ws.rows[:len(perf_before)]==perf_before
        assert signal.ws.rows[:len(signal_before)]==signal_before
        assert len(perf.ws.rows)==len(perf_before)+1
        assert len(signal.ws.rows)==len(signal_before)+2
        assert all(operation[0] in ('append','summary_clear','summary_update') for operation in perf.ws.writes)
    finally:
        for name,value in snapshots.items():
            actual=getattr(tracker,name)
            if isinstance(actual,dict): actual.clear();actual.update(value)
            else: actual[:]=value
