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
