"""The evidence workflow has no workbook mutation or publication path."""
from pathlib import Path

import yaml


def test_manual_evidence_workflow_is_read_only_and_exact_ref():
    root = Path(__file__).resolve().parents[1]
    workflow = yaml.safe_load((root / '.github/workflows/core_repair_readback.yml').read_text())
    trigger = workflow.get('on', workflow.get(True))
    assert set(trigger) == {'workflow_dispatch'}
    assert trigger['workflow_dispatch']['inputs']['name_identity_evidence'] == {
        'description': 'Collect current missing-name evidence without changing the workbook',
        'type': 'boolean', 'required': False, 'default': False}
    assert workflow['permissions'] == {'contents': 'read'}
    assert set(workflow['jobs']) == {'readback', 'collect-name-identity'}
    assert workflow['jobs']['readback']['if'] == "github.ref == 'refs/heads/main'"
    job = workflow['jobs']['collect-name-identity']
    assert job['if'] == '${{ inputs.name_identity_evidence }}'
    assert job['environment'] == 'production'
    steps = job['steps']
    checkout = next(s for s in steps if s.get('uses', '').startswith('actions/checkout@'))
    assert checkout['with'] == {'ref': '${{ github.sha }}', 'persist-credentials': False}
    collect = next(s for s in steps if s.get('name') == 'Collect fresh exact-code listing evidence')
    assert '--sheet-id "$DEFAULT_SPREADSHEET_ID"' in collect['run']
    assert '--output name-identity-evidence/evidence.json' in collect['run']
    assert 'EODHD_API_KEY' in collect['env']
    commands = '\n'.join(s.get('run', '') for s in steps)
    assert 'finalize_decision_publication' not in commands
    assert 'run_dashboard_sync' not in commands
    assert '--publish' not in commands
    assert 'gh ' not in commands
    artifact = next(s for s in steps if s.get('uses', '').startswith('actions/upload-artifact@'))
    assert artifact['with']['path'] == 'name-identity-evidence/'
    assert artifact['with']['retention-days'] == 7
