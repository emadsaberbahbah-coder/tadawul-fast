"""The actual offline CLI produces review reports without financial stdout."""
import json
import subprocess
import sys

from tests.test_portfolio_reconciliation import evidence, ROWS, FX, NOW
from scripts import tfb_reconcile_portfolio as cli


def run(tmp_path, packet=None, extra=()):
    source = tmp_path / "synthetic-capture.json"
    source.write_text(json.dumps({"holdings": ROWS, "reconciliation_evidence": evidence() if packet is None else packet, "fx_rates": FX}))
    result = subprocess.run([sys.executable, cli.__file__, str(source), "--now", NOW.isoformat(), *extra],
                            text=True, capture_output=True, timeout=10)
    return result, source


def test_real_cli_valid_capture_stdout_only_safe_counts(tmp_path):
    report = tmp_path / "private-report.json"
    result, source = run(tmp_path, extra=("--private-report", str(report)))
    assert result.returncode == 0 and json.loads(result.stdout)["funding_eligible"]
    assert "synthetic-account" not in result.stdout and "675" not in result.stdout
    private = json.loads(report.read_text())
    assert private["certified_cash_available_sar_exact"] == "675"
    assert len(private["input_sha256"]) == 64
    assert report.stat().st_mode & 0o777 == 0o600
    assert source.is_file()


def test_real_cli_zero_position_proposal_is_private_and_no_ledger_is_changed(tmp_path):
    packet = evidence()
    packet["accounts"][0]["positions"][0]["quantity"] = 0
    report = tmp_path / "private-report.json"
    result, source = run(tmp_path, packet, extra=("--private-report", str(report)))
    assert result.returncode == 2
    assert "synthetic-account" not in result.stdout and "observed_quantity" not in result.stdout
    proposal = json.loads(report.read_text())["proposals"][0]
    assert proposal["application"] == "review_only" and proposal["kind"] == "close_review"
    assert json.loads(source.read_text())["holdings"][0]["quantity"] == "12"


def test_real_cli_path_alias_refused_without_overwriting_input(tmp_path):
    source = tmp_path / "synthetic-capture.json"
    result, source = run(tmp_path, extra=("--private-report", str(source)))
    assert result.returncode == 2
    assert json.loads(source.read_text())["holdings"] == ROWS
    assert "synthetic-account" not in result.stdout


def test_real_cli_invalid_time_does_not_replace_accepted_report(tmp_path):
    source = tmp_path / "synthetic-capture.json"
    source.write_text("{}")
    report = tmp_path / "private-report.json"
    report.write_text("accepted")
    result = subprocess.run([sys.executable, cli.__file__, str(source), "--now", "2026-10-09", "--private-report", str(report)],
                            text=True, capture_output=True, timeout=10)
    assert result.returncode == 2 and report.read_text() == "accepted"
