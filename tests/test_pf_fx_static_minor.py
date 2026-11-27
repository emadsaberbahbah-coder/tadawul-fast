"""portfolio_actions v1.20.1 [FX-STATIC-MINOR]: a minor-unit holding (GBp, ZAc, ILA)
whose SAR value comes from the built-in FX table ("static/100") is listed in
meta fx.static_fallback_used, like a major-currency holding priced from it ("static")."""
import os
import sys

import pytest

_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _ROOT)

from core.analysis import portfolio_actions as pa  # noqa: E402


def test_static_source_classifier():
    assert pa._static_fx_source("static") is True
    assert pa._static_fx_source("static/100") is True
    for s in ("provided", "provided/100", "row", "", None, 3.75, "staticx", "x/static"):
        assert pa._static_fx_source(s) is False


@pytest.fixture(autouse=True)
def _env(monkeypatch):
    monkeypatch.setenv("TFB_PF_ENABLED", "1")


def _meta_fx(rows, fx_rates):
    ctl = pa.make_controls({"cash_available_sar": 10_000})
    return pa._build_verified([dict(r) for r in rows], ctl, fx_rates, {})["meta"]["fx"]


LSE = {"Symbol": "VOD.L", "Name": "Vodafone", "Current Price": 70.0, "Currency": "GBp",
       "Quantity": 100, "Buy Price": 72.0, "Buy Date": "2026-10-01"}
US = {"Symbol": "AAA.US", "Name": "Aaa", "Current Price": 50.0, "Currency": "USD",
      "Quantity": 10, "Buy Price": 48.0, "Buy Date": "2026-10-01"}


def test_minor_unit_static_fallback_is_disclosed():
    fx = _meta_fx([LSE, US], {"USD": 3.75})            # no GBP rate supplied -> built-in table
    assert fx["static_fallback_used"] == ["GBp"]       # 1.20.0 listed [] here


def test_no_fallback_when_the_rate_is_supplied():
    fx = _meta_fx([LSE, US], {"USD": 3.75, "GBP": 4.75})
    assert fx["static_fallback_used"] == []
    assert fx["provided"] == ["GBP", "USD"]


def test_version_and_manifest_pin():
    assert pa.PORTFOLIO_ACTIONS_VERSION == "1.20.1"
    manifest = open(os.path.join(_ROOT, "scripts", "verify_deployment.py"), encoding="utf-8").read()
    assert '"1.20.1", "portfolio actions")' in manifest
    ci = open(os.path.join(_ROOT, ".github", "workflows", "ci.yml"), encoding="utf-8").read()
    assert "tests/test_pf_fx_static_minor.py" in ci
