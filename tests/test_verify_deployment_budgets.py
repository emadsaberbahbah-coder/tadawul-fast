"""Bad numeric deployment controls must block a strict verification gate."""
import json

import pytest

from scripts import verify_deployment as verifier


@pytest.mark.parametrize("value", ["nan", "NaN", "inf", "-Infinity"])
def test_nonfinite_budget_is_drift(monkeypatch, capsys, value):
    monkeypatch.setattr(verifier, "MODULES", [])
    monkeypatch.setattr(verifier, "SCRIPTS", [])
    monkeypatch.setattr(verifier, "FLAGS", [
        ("TFB_OPP_BUILD_TIMEOUT_S", "70", "build budget", False),
    ])
    monkeypatch.setenv("TFB_OPP_BUILD_TIMEOUT_S", value)
    assert verifier.main(["--strict", "--json"]) == 2
    assert '"verdict": "DRIFT"' in capsys.readouterr().out


@pytest.mark.parametrize("value", [None, "", "60", "70", "80"])
def test_valid_or_default_budget_has_no_violation(value):
    assert verifier._range_violation("TFB_OPP_BUILD_TIMEOUT_S", value) == ""


def test_zero_candidate_sentinel_remains_legal():
    assert verifier._range_violation("TFB_OPP_MAX_CANDIDATES", "0") == ""


def test_json_report_keeps_import_diagnostics_on_stderr(monkeypatch, capsys):
    def noisy_import():
        print("module startup diagnostic")
        return []

    monkeypatch.setattr(verifier, "check_modules", noisy_import)
    monkeypatch.setattr(verifier, "check_scripts", lambda: [])
    monkeypatch.setattr(verifier, "check_flags", lambda: [])
    monkeypatch.setattr(verifier, "check_wiring", lambda: [])
    assert verifier.main(["--json", "--strict"]) == 0
    streams = capsys.readouterr()
    assert json.loads(streams.out)["verdict"] == "CLEAN"
    assert "module startup diagnostic" in streams.err


def test_json_report_explains_unwired_drift(monkeypatch, capsys):
    monkeypatch.setattr(verifier, "check_modules", lambda: [])
    monkeypatch.setattr(verifier, "check_scripts", lambda: [])
    monkeypatch.setattr(verifier, "check_flags", lambda: [])
    wiring = [{"module": "core.unused", "label": "unused", "declared": False,
               "reason": ""}]
    monkeypatch.setattr(verifier, "check_wiring", lambda: wiring)
    assert verifier.main(["--json", "--strict"]) == 2
    report = json.loads(capsys.readouterr().out)
    assert report["verdict"] == "DRIFT"
    assert report["wiring"] == wiring
