"""opportunity_builder v1.27.1 — TFB_OPP_ROTATION_BASIS (12m default | plan3m).

T1  default / invalid value -> 12m: engine-12M holding map, 8 pp hurdle, 'engine 12M' label (v1.27.0 byte-identical).
T2  plan3m: the holding map reads the plan-TP1 ROI (_tp1_plan_roi: (TP1 - price)/price), never the valuation
    roi_pct; a held name without a ladder has no 3M figure; the hurdle is
    TFB_OPP_ROTATION_EDGE_PP_3M (default 5), the label is 'plan 3M'.
T3  the owner's case: opportunity 20% / holding 14% over 3 months rotates on plan3m (edge 4.9 pp < 5? no —
    exactly the hurdle semantics: 20 - 14 - 1.1 = 4.9 < 5 -> no rotation at the default; at EDGE_PP_3M=4 it rotates)
    and never on 12m with those numbers.
T4  the live 19-Oct pair: XIOR.BR plan 17.3% vs CWBC.US plan 6.4% -> edge 9.8 pp on plan3m (rotates); engine 34.6 vs
    15.3 -> 18.2 pp on 12m (rotates). The degraded-forecast guard still excludes a holding on either basis.
T5  _funding_plan_text and the rotation_proposal wording name the basis in use; kpis.rotation_basis is additive;
    the module never reads cand["roi_pct"] for the rotation on either basis.
T6  the manifest pin equals the module constant (verify_deployment).
"""
from __future__ import annotations

import ast
import pathlib
import sys

import pytest

ROOT = pathlib.Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from core.analysis import opportunity_builder as ob  # noqa: E402

HOLDINGS = [
    {"symbol": "CWBC.US", "value_sar": 9664.0, "buy_date": "2026-09-10", "tp1_sar": 111.0, "price_sar": 96.56},
    {"symbol": "DDI.US", "value_sar": 11860.0, "buy_date": "2026-09-08", "tp1_sar": 56.4, "price_sar": 51.75},
    {"symbol": "5023.SR", "value_sar": 9850.0, "buy_date": "2025-11-25"},
]


def _audit(rows):
    return [{"symbol": s, "_cand": c} for s, c in rows]


def _cand(engine12, price, tp1, valuation_roi):
    """A normalized-candidate shape: roi_pct is the VALUATION (TP2) upside, the plan-TP1 figure comes from
    price/tp1 through _tp1_plan_roi — the two differ on purpose so a wrong field choice fails the tests."""
    return {"engine_roi_12m_pct": engine12, "price": price, "tp1": tp1, "roi_pct": valuation_roi,
            "forecast_source": "provider_target", "warnings": ""}


def _pool():
    return _audit([
        ("CWBC.US", _cand(15.3, 25.75, 27.398, 15.3)),    # plan TP1 ROI 6.4 %, valuation upside 15.3 %
        ("DDI.US", _cand(30.7, 13.80, 15.580, 30.7)),     # 12.9 % vs 30.7 %
        ("XIOR.BR", _cand(34.6, 23.75, 27.859, 34.6)),    # 17.3 % vs 34.6 %
    ])


@pytest.fixture(autouse=True)
def _clean_env(monkeypatch):
    for k in ("TFB_OPP_ROTATION_BASIS", "TFB_OPP_ROTATION_EDGE_PP", "TFB_OPP_ROTATION_EDGE_PP_3M",
              "TFB_OPP_ROTATION_FLOOR_GUARD"):
        monkeypatch.delenv(k, raising=False)
    yield


def test_t1_default_is_12m_byte_identical(monkeypatch):
    assert ob._env_rotation_basis() == "12m"
    assert ob._rotation_basis_label() == "engine 12M"
    assert ob._env_rotation_edge_pp() == 8.0
    m = ob._holding_roi_map(_pool(), HOLDINGS)
    assert m == {"CWBC.US": 15.3, "DDI.US": 30.7}
    monkeypatch.setenv("TFB_OPP_ROTATION_BASIS", "quarter")      # unknown word -> 12m
    assert ob._env_rotation_basis() == "12m"
    monkeypatch.setenv("TFB_OPP_ROTATION_EDGE_PP", "6")
    assert ob._env_rotation_edge_pp() == 6.0                      # 12m hurdle still honoured


def test_t2_plan3m_map_hurdle_label(monkeypatch):
    monkeypatch.setenv("TFB_OPP_ROTATION_BASIS", " PLAN3M ")
    assert ob._env_rotation_basis() == "plan3m"
    assert ob._rotation_basis_label() == "plan 3M"
    assert ob._env_rotation_edge_pp() == 5.0
    monkeypatch.setenv("TFB_OPP_ROTATION_EDGE_PP", "8")           # the 12m hurdle is ignored on plan3m
    assert ob._env_rotation_edge_pp() == 5.0
    monkeypatch.setenv("TFB_OPP_ROTATION_EDGE_PP_3M", "4")
    assert ob._env_rotation_edge_pp() == 4.0
    monkeypatch.setenv("TFB_OPP_ROTATION_EDGE_PP_3M", "-1")       # invalid -> default 5
    assert ob._env_rotation_edge_pp() == 5.0
    m = ob._holding_roi_map(_pool(), HOLDINGS)
    assert m == {"CWBC.US": 6.4, "DDI.US": 12.9}          # plan-TP1 ROI, never the valuation roi_pct
    # a held name without a TP1 ladder has no 3M figure -> it never rotates on this basis
    no_ladder = _audit([("CWBC.US", {"engine_roi_12m_pct": 15.3, "roi_pct": 15.3, "price": 25.75, "tp1": None,
                                     "forecast_source": "provider_target", "warnings": ""})])
    assert "CWBC.US" not in ob._holding_roi_map(no_ladder, HOLDINGS)
    assert ob._tp1_plan_roi(_cand(0, 100.0, 120.0, 40.0)) == 20.0   # the opportunity side uses the same helper


def test_t3_owner_case_20_vs_14(monkeypatch):
    hold = [{"symbol": "H1.US", "value_sar": 10000.0, "buy_date": "2026-09-01"}]
    # 12m basis, engine numbers: 20 - 14 - 1.1 = 4.9 pp < 8 -> no rotation
    assert ob._rotation_pick(20.0, hold, {"H1.US": 14.0}, set(), 8.0, 1.1) is None
    # plan3m default hurdle 5: 4.9 pp -> still no rotation (the hurdle is honest, not permissive)
    monkeypatch.setenv("TFB_OPP_ROTATION_BASIS", "plan3m")
    assert ob._rotation_pick(20.0, hold, {"H1.US": 14.0}, set(), ob._env_rotation_edge_pp(), 1.1) is None
    # the operator lowers the 3M hurdle to 4 -> rotates
    monkeypatch.setenv("TFB_OPP_ROTATION_EDGE_PP_3M", "4")
    pick = ob._rotation_pick(20.0, hold, {"H1.US": 14.0}, set(), ob._env_rotation_edge_pp(), 1.1)
    assert pick == {"symbol": "H1.US", "value_sar": 10000.0, "roi_pct": 14.0, "edge_pp": 4.9}


def test_t4_live_pair_both_bases_and_guard(monkeypatch):
    excl = ob._env_rotation_exclude()
    # 12m: XIOR 34.6 vs CWBC 15.3 -> 18.2 pp
    m12 = ob._holding_roi_map(_pool(), HOLDINGS)
    p12 = ob._rotation_pick(34.6, HOLDINGS, m12, excl, ob._env_rotation_edge_pp(), ob._env_rotation_cost_pct())
    assert p12 and p12["symbol"] == "CWBC.US" and p12["edge_pp"] == 18.2
    # plan3m: XIOR 17.3 vs CWBC 6.4 -> 9.8 pp (>= 5)
    monkeypatch.setenv("TFB_OPP_ROTATION_BASIS", "plan3m")
    m3 = ob._holding_roi_map(_pool(), HOLDINGS)
    p3 = ob._rotation_pick(17.3, HOLDINGS, m3, excl, ob._env_rotation_edge_pp(), ob._env_rotation_cost_pct())
    assert p3 and p3["symbol"] == "CWBC.US" and p3["edge_pp"] == 9.8
    # the sukuk never rotates (exclusion), DDI is not the lowest
    assert "5023.SR" not in m3
    # degraded forecast row is skipped on plan3m too (floor guard default ON)
    degraded = _audit([("CWBC.US", dict(_cand(15.3, 25.75, 27.398, 15.3), forecast_source="phase_ii_synthetic"))])
    assert "CWBC.US" not in ob._holding_roi_map(degraded, HOLDINGS)
    # the valuation field alone would have shown 18.2 pp on the 3M basis too — the TP1 figure must not
    assert ob._rotation_pick(34.6, HOLDINGS, m3, excl, ob._env_rotation_edge_pp(), ob._env_rotation_cost_pct())["edge_pp"] == 27.1
    assert p3["edge_pp"] == 9.8


def test_t5_text_and_alert_name_the_basis(monkeypatch):
    plan = {"symbol": "XIOR.BR", "need_sar": 5901.0, "shortfall_sar": 0.0, "state": "FUNDABLE_BY_ROTATION",
            "rotation": {"symbol": "CWBC.US", "value_sar": 9664.0, "roi_pct": 15.3, "edge_pp": 18.2,
                         "proceeds_sar": 5901.0, "action": "TRIM"}}
    t12 = ob._funding_plan_text(plan, 0.0)
    assert "engine 12M edge +18.2pp" in t12
    monkeypatch.setenv("TFB_OPP_ROTATION_BASIS", "plan3m")
    t3 = ob._funding_plan_text(dict(plan, rotation=dict(plan["rotation"], roi_pct=6.4, edge_pp=9.8)), 0.0)
    assert "plan 3M edge +9.8pp" in t3 and "engine 12M" not in t3
    cc = {"symbol": "RDN.US", "need_sar": 5901.0, "shortfall_sar": 5901.0, "state": "CAPITAL_CALL",
          "rotation": None, "rotation_insufficient": {"symbol": "KRP.US", "covers_sar": 3000.0,
                                                      "short_sar": 5901.0, "edge_pp": 7.0}}
    assert "plan 3M edge +7pp" in ob._funding_plan_text(cc, 0.0)
    # the module source carries the label in the rotation_proposal alert and the additive kpi
    src = (ROOT / "core" / "analysis" / "opportunity_builder.py").read_text(encoding="utf-8")
    assert 'kpis["rotation_basis"] = _rotation_basis_label()' in src
    assert src.count('" (" + _rotation_basis_label() + " edge +"') >= 2   # text + alert
    assert '" (engine 12M edge +"' not in src                               # no hard-coded label left
    assert 'pool[s] = _tp1_plan_roi(c)' in src and '_opp_roi = _tp1_plan_roi(_a["_cand"])' in src
    assert '_to_float(c.get("roi_pct"))' not in src                          # the valuation field is not the 3M basis


def test_t6_version_and_manifest_pin():
    assert ob.OPPORTUNITY_BUILDER_VERSION == "1.27.4"
    tree = ast.parse((ROOT / "scripts" / "verify_deployment.py").read_text(encoding="utf-8"))
    pins = [n for n in ast.walk(tree) if isinstance(n, ast.Tuple) and len(n.elts) == 4
            and isinstance(n.elts[0], ast.Constant) and n.elts[0].value == "core.analysis.opportunity_builder"]
    assert pins and pins[0].elts[2].value == "1.27.4"
