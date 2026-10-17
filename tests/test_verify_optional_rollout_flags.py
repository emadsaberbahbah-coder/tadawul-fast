"""Optional rollout flags stay visible without making disarmed state drift."""
import ast
from pathlib import Path


def literal(name):
    tree = ast.parse((Path(__file__).resolve().parents[1] / 'scripts/verify_deployment.py').read_text())
    for node in tree.body:
        target = node.target if isinstance(node, ast.AnnAssign) else node.targets[0] if isinstance(node, ast.Assign) else None
        if isinstance(target, ast.Name) and target.id == name:
            return ast.literal_eval(node.value)
    raise AssertionError(f'{name} missing')


def test_render_rollout_defaults_visible_and_disarmed_not_drift():
    flags = {name:(default,meaning,required) for name,default,meaning,required in literal('FLAGS')}
    expected = {'TFB_HORIZON_COHERENT':'', 'TFB_STRICT_INVEST_GATE':'0',
                'TFB_T10_REL_FLOOR_MODE':'gate', 'TFB_MARGIN_PUBLISH':'off',
                'TFB_PRICE_PRECISION':'0', 'TFB_OPP_FX_USX_SUBUNIT':'0',
                'TFB_EODHD_CRYPTO_MAP':'0', 'TFB_CRYPTO_NAME_GATE':'off'}
    for name,default in expected.items():
        assert flags[name][0] == default
        assert flags[name][2] is False
    modes = literal('_VALUE_FLAGS')
    assert {'TFB_HORIZON_COHERENT','TFB_T10_REL_FLOOR_MODE','TFB_MARGIN_PUBLISH','TFB_CRYPTO_NAME_GATE'} <= modes
    assert not expected.keys() & literal('_WORKFLOW_SCOPED')


def test_review_rollout_receipts_distinguish_measurement_from_enforcement(monkeypatch):
    from scripts.verify_deployment import check_flags

    controls = {
        'TFB_ROI12_SOFTCAP_GATE': 'observe',
        'TFB_TUPLE_NOISE_RELEASE': 'enforce',
        'TFB_EQ_ROI_UNIT_SENTRY': 'observe',
        'TRACK_DEDUP_SIGNAL_HISTORY': 'enforce',
        'TRACK_SIGNAL_PUBLICATION_MODE': 'immutable',
        'TRACK_CANONICAL_TARGETS': '1',
        'TFB_DQ_AUDIT_MODE': 'observe',
    }
    for name, value in controls.items():
        monkeypatch.setenv(name, value)
    rows = {row['flag']: row for row in check_flags()}
    for name in controls:
        assert rows[name]['set'] and not rows[name]['kill_switch']
    assert not rows['TFB_ROI12_SOFTCAP_GATE']['armed']
    assert rows['TFB_TUPLE_NOISE_RELEASE']['armed']
    assert not rows['TFB_EQ_ROI_UNIT_SENTRY']['armed']
    assert rows['TRACK_DEDUP_SIGNAL_HISTORY']['armed']   # v6.47.0 [L5]: enforce writes
    assert rows['TRACK_SIGNAL_PUBLICATION_MODE']['armed']
    assert rows['TRACK_CANONICAL_TARGETS']['armed']
    assert rows['TFB_DQ_AUDIT_MODE']['armed']
    for name in ('TRACK_DEDUP_SIGNAL_HISTORY', 'TRACK_SIGNAL_PUBLICATION_MODE',
                 'TRACK_CANONICAL_TARGETS', 'TRACK_PUBLICATION_ID', 'TFB_DQ_AUDIT_MODE'):
        assert rows[name]['scope'] == 'workflow'
    for name in ('TFB_ROI12_SOFTCAP_GATE', 'TFB_TUPLE_NOISE_RELEASE', 'TFB_EQ_ROI_UNIT_SENTRY'):
        assert rows[name]['scope'] == 'render'


def test_existing_enforce_modes_never_claim_off_or_observe_is_armed(monkeypatch):
    from scripts.verify_deployment import check_flags

    controls = ('TFB_CRYPTO_NAME_GATE', 'TFB_MARGIN_PUBLISH', 'TFB_HORIZON_COHERENT')
    for mode in ('off', 'observe', 'enforce', ' ENFORCE '):
        for name in controls:
            monkeypatch.setenv(name, mode)
        rows = {row['flag']: row for row in check_flags()}
        for name in controls:
            assert rows[name]['set']
            assert rows[name]['armed'] is (mode.strip().lower() == 'enforce')
            assert rows[name]['scope'] == 'render'
