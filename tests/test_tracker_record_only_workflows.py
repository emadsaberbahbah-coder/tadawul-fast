"""Execute the actual tracker workflow wrappers with historical audit opt-in."""
from __future__ import annotations

import json
import subprocess

import pytest

from tests.test_scheduled_portfolio_refresh import accepts
from tests.test_workflow_activation_observe import step, workflow
from tests.test_workflow_activation_observe import wrapper_repo as wrapper_repo  # noqa: PLC0414 (pytest fixture)


@pytest.mark.parametrize('name,input_name', (
    ('track_performance.yml', 'run_audit'), ('daily_sync.yml', 'run_tracker'),
))
def test_historical_audit_requires_false_default_and_plain_explicit_opt_in(name, input_name):
    parameter = workflow(name)['on']['workflow_dispatch']['inputs'][input_name]
    assert parameter['type'] == 'boolean' and parameter['default'] == 'false'
    assert 'historical maturity audit' in parameter['description']
    assert 'explicit opt-in' in parameter['description']


@pytest.mark.parametrize('event,audit,expected', (
    ('schedule', '', False), ('schedule', 'false', False), ('schedule', 'true', False),
    ('workflow_dispatch', '', False), ('workflow_dispatch', 'false', False),
    ('workflow_dispatch', 'true', True), ('workflow_dispatch', 'TRUE', False),
    ('', 'true', False), ('push', 'true', False),
))
@pytest.mark.parametrize('lane', ('standalone', 'daily'))
def test_actual_tracker_shell_requires_manual_true_and_schedules_keep_recording(wrapper_repo, event, audit, expected, lane):
    root, env = wrapper_repo
    if lane == 'standalone':
        item = step('track_performance.yml', 'Run performance clock')
        block = item['run'].replace('${{ github.event.inputs.enable_debug }}', 'false')
        control = 'INPUT_RUN_AUDIT'
        assert item['env'][control] == '${{ github.event.inputs.run_audit }}'
    else:
        item = step('daily_sync.yml', 'Track Performance (record + audit)')
        block = item['run'].replace('/tmp/track_performance.log', str(root / 'daily_tracker.log'))
        control = 'INPUT_RUN_TRACKER'
        assert item['env'][control] == '${{ github.event.inputs.run_tracker }}'
    env |= {control:audit, 'GITHUB_EVENT_NAME':event, 'INPUT_SIGNAL_HISTORY_MODE':'observe',
        'TRACK_DEDUP_SIGNAL_HISTORY':'observe', 'TRACK_AUDIT':'1',
        'TRACK_PUBLISH_CALIBRATION':'1', 'TFB_SIGNAL_TRENDS_TAB':'1',
        'TFB_TRACK_SUMMARY_SIDE':'0', 'GOOGLE_APPLICATION_CREDENTIALS':''}
    run = subprocess.run(['bash', '-c', block], cwd=root, env=env, capture_output=True, text=True, check=False)
    assert run.returncode == 0, run.stdout + run.stderr
    actual = json.loads((root / 'actual_tracker_call.json').read_text())
    assert all(flag in actual['args'] for flag in ('--record', '--analyze', '--export'))
    assert ('--audit' in actual['args']) is expected
    if not expected:
        assert actual['audit_env'] == actual['calibration_publish'] == actual['trends'] == '0'
        assert actual['summary_side'] == '1'
    else:
        assert actual['audit_env'] == actual['calibration_publish'] == actual['trends'] == '1'
    if lane == 'standalone':
        value = json.loads((root / 'tracker_activation_receipt.json').read_text())
        assert value['audit_enabled'] is value['historical_grid_save_requested'] is expected
        assert value['execution_evidence_complete'] is True


@pytest.mark.parametrize('event,choice,should_run', (
    ('schedule', '', True), ('schedule', 'false', True),
    ('workflow_dispatch', '', True), ('workflow_dispatch', 'false', False),
    ('workflow_dispatch', 'true', True),
))
def test_daily_actual_step_gate_skips_manual_false_before_any_tracker_call(wrapper_repo, event, choice, should_run):
    root, env = wrapper_repo
    item = step('daily_sync.yml', 'Track Performance (record + audit)')
    condition = item['if'].removeprefix('${{').removesuffix('}}').strip()
    allowed = accepts(condition, event=event, run_tracker=choice)
    assert allowed is should_run
    if allowed:
        body = item['run'].replace('/tmp/track_performance.log', str(root / 'daily_tracker.log'))
        run = subprocess.run(['bash', '-c', body], cwd=root,
                             env=env | {'GITHUB_EVENT_NAME': event, 'INPUT_RUN_TRACKER': choice},
                             text=True, capture_output=True, check=False)
        assert run.returncode == 0, run.stdout + run.stderr
    assert (root / 'actual_tracker_call.json').exists() is should_run


@pytest.mark.parametrize('event,choice,record,should_run', (
    ('workflow_dispatch', 'false', 'true', True), ('workflow_dispatch', 'false', 'false', False),
    ('workflow_dispatch', 'false', '', False), ('workflow_dispatch', 'true', 'true', True),
    ('schedule', '', '', True),
))
def test_dispatcher_record_tracker_input_records_without_historical_audit(wrapper_repo, event, choice, record, should_run):
    # P-170c: the Apps Script dispatcher sends record_tracker="true" (run_tracker
    # keeps its "false" default), so its full_sync records like a scheduled run
    # but never opts into the historical audit.
    root, env = wrapper_repo
    item = step('daily_sync.yml', 'Track Performance (record + audit)')
    condition = item['if'].removeprefix('${{').removesuffix('}}').strip()
    allowed = accepts(condition, event=event, run_tracker=choice, record_tracker=record)
    assert allowed is should_run
    if allowed:
        body = item['run'].replace('/tmp/track_performance.log', str(root / 'daily_tracker.log'))
        run = subprocess.run(['bash', '-c', body], cwd=root,
                             env=env | {'GITHUB_EVENT_NAME': event, 'INPUT_RUN_TRACKER': choice},
                             text=True, capture_output=True, check=False)
        assert run.returncode == 0, run.stdout + run.stderr
        actual = json.loads((root / 'actual_tracker_call.json').read_text())
        assert '--record' in actual['args']
        assert ('--audit' in actual['args']) is (event == 'workflow_dispatch' and choice == 'true')
    assert (root / 'actual_tracker_call.json').exists() is should_run
