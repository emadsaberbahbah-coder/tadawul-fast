#!/usr/bin/env python3
"""opportunity_builder v1.27.2 — conviction sizing: an exhausted budget is not a release.

Z1  conviction + zero cash: every cohort seat is a 0-SAR pick (capital exhausted) with a funding need at the
    minimum ticket — kpis.capital_call and the near-miss text equal cap mode's; no seat carries the
    "budget released to higher-conviction seats" deferral.
Z2  conviction + a budget that funds the stronger seats: the weakest seat released by the floor rule is still the
    v1.27.0 deferral (no need, no capital call) — the budget really was re-spread.
Z3  conviction + a budget below one minimum ticket: whole-cohort release = capital exhaustion (needs recorded).
Z4  cap mode is byte-identical to v1.27.1 on the zero-cash case (3 capital calls).
Z5  the manifest pin equals the module constant.
"""
from __future__ import annotations

import ast
import copy
import os
import sys
from pathlib import Path

import pytest

_REPO = Path(__file__).resolve().parents[1]
if str(_REPO) not in sys.path:
    sys.path.insert(0, str(_REPO))
sys.path.insert(0, str(_REPO / "tests"))

from core.analysis import opportunity_builder as ob  # noqa: E402
from test_ob_conviction_sizing import CRITERIA, FX, ROWS, build_with_observed_inputs  # noqa: E402

RELEASED = "budget released to higher-conviction seats"


@pytest.fixture(autouse=True)
def _isolated(monkeypatch):
    for key in list(os.environ):
        if key.startswith(("TFB_OPP_", "TFB_T10_")):
            monkeypatch.delenv(key, raising=False)
    monkeypatch.delenv("RENDER_GIT_COMMIT", raising=False)
    monkeypatch.setenv("TFB_OPP_ENABLED", "1")
    monkeypatch.setenv("APP_TOKEN", "synthetic-sizing-test")


def _build(cash, **criteria):
    return build_with_observed_inputs(
        ob, copy.deepcopy(ROWS), criteria={**CRITERIA, **criteria},
        portfolio={"cash_available_sar": cash}, fx_rates=dict(FX))


def _funding_rows(payload):
    return {n["symbol"]: str(n.get("current") or "") for n in payload["near_miss"] if n.get("failed_gate") == "Funding"}


def test_z1_zero_cash_is_capital_exhaustion_not_a_release(monkeypatch):
    monkeypatch.setenv("TFB_OPP_SIZING_MODE", "conviction")
    p = _build(0.0)
    assert p["status"] == "ok" and p["selected"] == []
    rows = _funding_rows(p)
    assert set(rows) == {"ALPHA.SR", "BRAVO.SR", "CHARLIE.SR"}
    for txt in rows.values():
        assert "capital exhausted" in txt and "CAPITAL_CALL" in txt, txt
        assert RELEASED not in txt
    assert p["kpis"]["capital_call"] == 3
    assert p["kpis"]["capital_call_topn_sar"] == 3 * CRITERIA["min_ticket_sar"]
    # the candidates grid carries no release deferral either
    assert not any(RELEASED in str(r) for r in p.get("candidates_rows") or [])


def test_z2_partial_release_is_still_a_deferral(monkeypatch):
    monkeypatch.setenv("TFB_OPP_SIZING_MODE", "conviction")
    # 2,500 SAR across three seats with a 1,000 floor: the weakest share falls below the floor and is released,
    # the budget is re-spread — a genuine release, no funding need
    p = _build(2_500.0)
    sizes = {t["symbol"]: float(t["suggested_sar"]) for t in p["selected"]}
    assert sizes and "CHARLIE.SR" not in sizes and all(v >= CRITERIA["min_ticket_sar"] for v in sizes.values()), sizes
    rows = _funding_rows(p)
    assert "CHARLIE.SR" in rows and RELEASED in rows["CHARLIE.SR"], rows
    assert "capital exhausted" not in rows["CHARLIE.SR"]
    assert p["kpis"]["capital_call"] == 0


def test_z3_budget_below_one_ticket_is_exhaustion(monkeypatch):
    monkeypatch.setenv("TFB_OPP_SIZING_MODE", "conviction")
    p = _build(600.0)   # below the 1,000 minimum ticket: nothing can be funded, the whole cohort comes back
    assert p["selected"] == []
    rows = _funding_rows(p)
    assert set(rows) == {"ALPHA.SR", "BRAVO.SR", "CHARLIE.SR"}
    assert all("capital exhausted" in t and RELEASED not in t for t in rows.values()), rows
    assert p["kpis"]["capital_call"] == 3


def test_z3b_cap_below_the_floor_with_cash_is_infeasible_not_exhaustion(monkeypatch):
    """v1.27.3: 5,000 SAR cash, 15% cap (750 SAR), 2,000 SAR floor -> every target disappears, but cash is plentiful:
    the seats are deferred as infeasible under the cap and NO need is recorded (no capital call, no rotation)."""
    monkeypatch.setenv("TFB_OPP_SIZING_MODE", "conviction")
    p = _build(5_000.0, max_weight_pct=15.0, min_ticket_sar=2_000.0)
    assert p["selected"] == []
    rows = _funding_rows(p)
    assert set(rows) == {"ALPHA.SR", "BRAVO.SR", "CHARLIE.SR"}
    assert all("infeasible under the cap" in t and "capital exhausted" not in t and RELEASED not in t for t in rows.values()), rows
    assert p["kpis"]["capital_call"] == 0
    # the same cash with a feasible cap funds normally
    ok = _build(5_000.0, max_weight_pct=100.0, min_ticket_sar=2_000.0)
    assert {t["symbol"] for t in ok["selected"]} and ok["kpis"]["capital_call"] == 0


def test_z4_cap_mode_unchanged_on_zero_cash(monkeypatch):
    monkeypatch.setenv("TFB_OPP_SIZING_MODE", "cap")
    p = _build(0.0)
    rows = _funding_rows(p)
    assert set(rows) == {"ALPHA.SR", "BRAVO.SR", "CHARLIE.SR"}
    assert all("capital exhausted" in t for t in rows.values())
    assert p["kpis"]["capital_call"] == 3
    assert p["meta"]["sizing"] == {"mode": "cap"}


def test_z5_version_and_manifest_pin():
    assert ob.OPPORTUNITY_BUILDER_VERSION == "1.27.3"
    tree = ast.parse((_REPO / "scripts" / "verify_deployment.py").read_text(encoding="utf-8"))
    pins = [n for n in ast.walk(tree) if isinstance(n, ast.Tuple) and len(n.elts) == 4
            and isinstance(n.elts[0], ast.Constant) and n.elts[0].value == "core.analysis.opportunity_builder"]
    assert pins and pins[0].elts[2].value == "1.27.3"
