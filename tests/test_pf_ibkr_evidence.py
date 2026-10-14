"""10_My_Portfolio v1.5.12 IBKR capture evidence, checked against the real backend.

Runs tests/js/pf_ibkr_evidence.test.js (the REAL Apps Script source in a vm,
synthetic holdings, contract ids and amounts) and feeds what it sends to:
- core.portfolio_reconciliation.certify_portfolio_inputs at a fixed clock:
  the enforce packet certifies holdings and cash, and the request cash equals
  the certified cash quantized half-even (the backend's own comparison);
- Decimal arithmetic for every cash case the harness produced;
- the REAL portfolio_actions build around the current clock: the enforce
  request is certified and funding-eligible, and each fix is necessary
  (one more cent, the lookup FX, no quote provenance, the out-of-scope row
  or a 16-minute-old capture each withhold with the backend's reason).
The whole-suite CI job collects this file, so the native harness runs there too.
"""
from __future__ import annotations

import copy
import datetime as dt
import json
import os
import shutil
import subprocess
from decimal import ROUND_HALF_EVEN, Decimal
from pathlib import Path

import pytest

from core.analysis import portfolio_actions as pa
from core.portfolio_reconciliation import certify_portfolio_inputs
from routes.advanced_analysis import _pf_controls_from_body

ROOT = Path(__file__).resolve().parents[1]
HARNESS = ROOT / "tests" / "js" / "pf_ibkr_evidence.test.js"
GAS = ROOT / "apps_script" / "10_My_Portfolio.gs"
UTC = dt.timezone.utc
CASH = "PF: Cash Available SAR"


def node_binary():
    for candidate in (os.environ.get("TFB_TEST_NODE", ""), shutil.which("node") or "",
                      "/opt/node22/bin/node"):
        if candidate and os.path.exists(candidate):
            return candidate
    return None


def _node(env_extra, tmp):
    node = node_binary()
    if node is None:
        pytest.skip("node is not installed")
    env = dict(os.environ, PF_GAS=str(GAS), **env_extra)
    for key in ("PF_WRITE_GOLDEN", "PF_BASE_GAS"):
        env.pop(key, None)
    proc = subprocess.run([node, str(HARNESS)], env=env, capture_output=True, text=True,
                          timeout=180, cwd=str(tmp))
    assert proc.returncode == 0, (proc.stdout[-2000:], proc.stderr[-2000:])
    return proc.stdout


@pytest.fixture(scope="module")
def dump(tmp_path_factory):
    tmp = tmp_path_factory.mktemp("pf_ibkr")
    out = tmp / "dump.json"
    stdout = _node({"PF_DUMP": str(out)}, tmp)
    assert "PASS 14/14 digest" in stdout, stdout[-2000:]
    return json.loads(out.read_text(encoding="utf-8"))


def _cents(value) -> Decimal:
    return Decimal(str(value)).quantize(Decimal("0.01"), rounding=ROUND_HALF_EVEN)


def test_enforce_packet_certifies_holdings_and_cash_at_the_capture_clock(dump):
    body = dump["enforce_body"]
    now = dt.datetime(2026, 10, 15, 5, 45, tzinfo=UTC)
    cert = certify_portfolio_inputs(body["rows"], body["reconciliation_evidence"],
                                    body["fx_rates"], now=now)
    assert cert["status"] == "certified", cert["reason_counts"]
    assert cert["holdings_certified"] and cert["cash_certified"] and cert["funding_eligible"]
    assert [row["status"] for row in cert["row_results"]] == ["matched"] * 3
    assert _cents(cert["certified_cash_available_sar_exact"]) == _cents(body["controls"][CASH])
    late = certify_portfolio_inputs(body["rows"], body["reconciliation_evidence"],
                                    body["fx_rates"], now=now + dt.timedelta(minutes=12))
    assert late["status"] == "withheld" and late["reason_counts"] == {"invalid_evidence": 3}


def test_every_cash_case_matches_decimal_and_the_real_reconciliation(dump):
    assert len(dump["cash_cases"]) >= 15
    for case in dump["cash_cases"]:
        cash = case["cash"]
        expected = ((Decimal(cash["settled_cash"]) - Decimal(cash["reserved_cash"]))
                    * Decimal("3.75")).quantize(Decimal("0.01"), rounding=ROUND_HALF_EVEN)
        assert Decimal(case["cents"]) / 100 == expected, case
        packet = copy.deepcopy(case["packet"])
        packet["accounts"][0]["positions"] = []
        packet["holding_links"] = []
        captured = dt.datetime.fromisoformat(packet["captured_at"].replace("Z", "+00:00"))
        cert = certify_portfolio_inputs([], packet, {"USD": 3.75},
                                        now=captured + dt.timedelta(seconds=60))
        assert cert["status"] == "certified", (case, cert["reason_counts"])
        assert _cents(cert["certified_cash_available_sar_exact"]) == expected, case


@pytest.fixture()
def live(tmp_path, monkeypatch):
    """The enforce request built by the real GAS source around the current clock."""
    for name in ("TFB_PF_RESEARCH_VIEW", "TFB_OPP_FX_USX_SUBUNIT"):
        monkeypatch.delenv(name, raising=False)
    now = dt.datetime.now(UTC).replace(microsecond=0)
    out = tmp_path / "e2e.json"
    _node({"PF_E2E_NOW": now.isoformat().replace("+00:00", "Z"), "PF_E2E_OUT": str(out)},
          tmp_path)
    return json.loads(out.read_text(encoding="utf-8"))["body"]


def _build(body):
    return pa.build_portfolio_actions(
        copy.deepcopy(body["rows"]), controls=_pf_controls_from_body(body["controls"]),
        fx_rates=dict(body["fx_rates"]),
        upstream_meta={"reconciliation_evidence": copy.deepcopy(body.get("reconciliation_evidence")),
                       "holdings_input_incomplete": False})


def test_real_build_certifies_the_enforce_request(live):
    out = _build(live)
    cert = out["meta"]["input_certification"]
    assert cert["status"] == "certified", cert
    assert cert["funding_eligible"] is True and cert["trusted_rows"] == 3
    assert out["meta"]["execution_ready"] is True
    assert len(out["actions"]) == 3
    for action in out["actions"]:
        assert action["detail"]["reconciliation_status"] == "matched", action
    # KPIs are whole SAR; the funding identity uses the certified cash itself.
    assert abs(Decimal(str(out["kpis"]["cash_sar"])) - _cents(live["controls"][CASH])) < 1


def _without_provenance(body):
    for row in body["rows"]:
        for key in ("Data Provider", "Warnings", "Last Updated (UTC)"):
            row.pop(key, None)


def _with_sukuk(body):
    body["rows"].append({"Symbol": "9999.SR", "Name": "Synthetic Sukuk", "Currency": "SAR",
                         "Quantity": 10, "Buy Price": 1000, "Avg Cost": 1000,
                         "Current Price": 998.5})


def _aged(body):
    old = (dt.datetime.now(UTC) - dt.timedelta(minutes=16)).isoformat().replace("+00:00", "Z")
    packet = body["reconciliation_evidence"]
    packet["captured_at"] = old
    for account in packet["accounts"]:
        account["positions_asof"] = account["cash_asof"] = old
    for proof in packet["fx_rates"]:
        proof["asof"] = old


@pytest.mark.parametrize("label, mutate, reason", [
    ("one more cent than certified", lambda b: b["controls"].update(
        {CASH: float(_cents(b["controls"][CASH]) + Decimal("0.01"))}), "cash_request_mismatch"),
    ("lookup FX instead of the peg", lambda b: b["fx_rates"].update({"USD": 3.75547}),
     "invalid_evidence"),
    ("no quote provenance", _without_provenance, "holding_quote_unverified"),
    ("out-of-scope row sent", _with_sukuk, "custody_unknown"),
    ("capture 16 minutes old", _aged, "invalid_evidence"),
])
def test_each_fix_is_necessary(live, label, mutate, reason):
    body = copy.deepcopy(live)
    mutate(body)
    cert = _build(body)["meta"]["input_certification"]
    assert cert["status"] == "withheld", label
    assert cert["funding_eligible"] is False, label
    assert reason in cert["reason_counts"], (label, cert["reason_counts"])
