"""An actual later recovery attempt cannot inherit an older refresh verdict."""
from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

import pytest

from scripts.audit_sync_outcome import CRITICAL_MARKET_PAGES, audit_artifacts
from scripts.plan_sync_recovery import build_recovery_plan

ROOT = Path(__file__).resolve().parents[1]


def verdict(page="Global_Markets", *, status="success", fresh=100):
    return (f"[PAGE-VERDICT v6.26.0] page={page} status={status} rows_written=100 "
            f"fresh_rows={fresh} requested_rows=100 fresh_pct={fresh:.4f} reason=synthetic\n")


def write(path, text):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text)
    return path


def primary(root):
    write(root / "tadawul-sync-logs-123-core-pages/sync_execution.log",
          "".join(verdict(page) for page in CRITICAL_MARKET_PAGES))


def recovery(root, *, shape="named", group="global-markets", run_id="123"):
    family = root / "zz-recovery"
    if shape == "named":
        family /= f"page-refresh-{run_id}-{group}"
    return family


@pytest.fixture(autouse=True)
def audit_policy(monkeypatch):
    monkeypatch.setenv("TFB_AUDIT_REQUIRE_FULL_FETCH", "1")
    monkeypatch.setenv("TFB_AUDIT_MIN_FRESH_PCT", "95")


@pytest.mark.parametrize("shape", ["named", "flattened"])
@pytest.mark.parametrize("gate", ["0", "1"])
@pytest.mark.parametrize("latest_text", ["", "ERROR: backend unavailable\n", "[PAGE-VERDICT v6.26.0] page=Global_Markets status=success rows_written=",
                                        "[PAGE-VERDICT v6.26.0] page=Global_Markets\nstatus=success\nrows_written=100\n"])
def test_empty_or_truncated_later_attempt_invalidates_only_its_page(tmp_path, monkeypatch, shape, gate, latest_text):
    monkeypatch.setenv("TFB_AUDIT_REQUIRE_FULL_FETCH", gate)
    primary(tmp_path)
    family = recovery(tmp_path, shape=shape)
    write(family / "sync_execution.log", verdict())
    latest = write(family / "cycle2/sync_execution.log", latest_text)
    result = audit_artifacts(tmp_path)
    assert result.status == "blocked" and result.failed_pages == ("Global_Markets",)
    item = next(item for item in result.verdicts if item.page == "Global_Markets")
    assert item.status == "unverified_attempt" and item.rows_written == 0 and item.source == str(latest)
    assert result.to_dict()["verdicts"][1]["passed"] is False
    evidence = next(item for item in result.fetch_evidence if item["page"] == "Global_Markets")
    assert evidence["attempt_verdict_missing"] and evidence["coverage_unknown"]
    assert "fresh_rows" not in evidence and "fresh_pct" not in evidence
    assert evidence["source"] == str(latest)
    assert build_recovery_plan(tmp_path)["retry_pages"] == ["Global_Markets"]


def test_empty_canonical_cannot_fall_back_to_older_timestamped_log(tmp_path):
    primary(tmp_path)
    family = recovery(tmp_path)
    write(family / "sync_execution.log", verdict())
    write(family / "cycle2/sync_20261016_010000.log", verdict())
    empty = write(family / "cycle2/sync_execution.log", "")
    result = audit_artifacts(tmp_path)
    assert result.failed_pages == ("Global_Markets",)
    assert result.verdicts[1].source == str(empty)


@pytest.mark.parametrize("latest", ["empty", "success", "failure", "partial", "incomplete"])
def test_numeric_cycle10_supersedes_cycle2_and_clears_only_actual_earlier_evidence(tmp_path, latest):
    primary(tmp_path)
    family = recovery(tmp_path)
    write(family / "sync_execution.log", verdict())
    write(family / "cycle2/sync_execution.log", "")
    text = {"empty": "", "success": verdict(), "failure": verdict(status="failed"),
            "partial": verdict(status="partial"), "incomplete": verdict(fresh=93)}[latest]
    final = write(family / "cycle10/sync_execution.log", text)
    result = audit_artifacts(tmp_path)
    assert result.verdicts[1].source == str(final)
    assert result.status == ("ok" if latest == "success" else "blocked")
    evidence = next(item for item in result.fetch_evidence if item["page"] == "Global_Markets")
    assert evidence["source"] == str(final)
    assert bool(evidence.get("attempt_verdict_missing")) is (latest == "empty")
    if latest == "success":
        assert evidence["fresh_rows"] == 100 and not result.failed_pages


@pytest.mark.parametrize("shape", ["named", "flattened"])
@pytest.mark.parametrize("filename", ["sync_runner_receipt.json", "page-audit.json"])
def test_receipt_without_log_proves_attempt_but_never_certifies_success(tmp_path, shape, filename):
    primary(tmp_path)
    family = recovery(tmp_path, shape=shape)
    write(family / "sync_execution.log", verdict())
    payload = ({"results": [{"sheet_name": "Global_Markets", "status": "success", "rows_written": 100}]}
               if filename == "sync_runner_receipt.json" else {"required_pages": ["Global_Markets"], "status": "ok"})
    receipt = write(family / "cycle2" / filename, json.dumps(payload))
    result = audit_artifacts(tmp_path)
    assert result.failed_pages == ("Global_Markets",)
    assert result.verdicts[1].status == "unverified_attempt" and result.verdicts[1].source == str(receipt)
    evidence = next(item for item in result.fetch_evidence if item["page"] == "Global_Markets")
    assert evidence["attempt_log_missing"] and evidence["source"] == str(receipt)
    assert not any("cycle2/sync_execution.log" in path for path in result.log_files)


@pytest.mark.parametrize("filename", ["sync_runner_receipt.json", "page-audit.json"])
def test_flattened_first_empty_log_can_scope_only_from_its_actual_receipt(tmp_path, filename):
    primary(tmp_path)
    family = recovery(tmp_path, shape="flattened")
    write(family / "sync_execution.log", "")
    payload = ({"results": [{"sheet_name": "Global_Markets"}]} if filename == "sync_runner_receipt.json"
               else {"required_pages": ["Global_Markets"]})
    write(family / filename, json.dumps(payload))
    assert audit_artifacts(tmp_path).failed_pages == ("Global_Markets",)


def test_flattened_unscoped_attempt_cannot_borrow_scope_from_primary_or_another_artifact(tmp_path):
    primary(tmp_path)
    other = recovery(tmp_path, group="commodities-fx")
    write(other / "sync_execution.log", verdict("Commodities_FX"))
    # No same-family base verdict, path group or receipt identifies this log.
    write(tmp_path / "zz-recovery/cycle2/sync_execution.log", "")
    with pytest.raises(OSError, match="scope"):
        audit_artifacts(tmp_path)
    with pytest.raises(OSError, match="scope"):
        build_recovery_plan(tmp_path)


@pytest.mark.parametrize("wrong_evidence", ["terminal_verdict", "runner_receipt", "audit_receipt"])
def test_wrong_page_evidence_in_scoped_attempt_is_an_error_not_a_page_guess(tmp_path, wrong_evidence):
    primary(tmp_path)
    family = recovery(tmp_path)
    write(family / "sync_execution.log", verdict())
    write(family / "cycle2/sync_execution.log", verdict("Commodities_FX") if wrong_evidence == "terminal_verdict" else "")
    if wrong_evidence == "runner_receipt":
        write(family / "cycle2/sync_runner_receipt.json", '{"results":[{"sheet_name":"Commodities_FX"}]}')
    elif wrong_evidence == "audit_receipt":
        write(family / "cycle2/page-audit.json", '{"required_pages":["Commodities_FX"]}')
    with pytest.raises(OSError, match="scope"):
        audit_artifacts(tmp_path)


def test_conflicting_known_source_run_ids_cannot_override_the_source_primary(tmp_path):
    primary(tmp_path)
    write(recovery(tmp_path, run_id="999") / "sync_execution.log", verdict())
    with pytest.raises(OSError, match="source run IDs conflict"):
        audit_artifacts(tmp_path)


@pytest.mark.parametrize("receipt", ['not-json', '{"results":[]}',
                                      '{"results":[{"sheet_name":"Global_Markets","sheet_name":"Commodities_FX"}]}'])
def test_malformed_optional_receipts_cannot_certify_missing_terminal_or_poison_known_scope(tmp_path, receipt):
    primary(tmp_path)
    family = recovery(tmp_path)
    write(family / "sync_execution.log", verdict())
    write(family / "cycle2/sync_execution.log", "")
    write(family / "cycle2/sync_runner_receipt.json", receipt)
    result = audit_artifacts(tmp_path)
    assert result.failed_pages == ("Global_Markets",)
    assert result.verdicts[1].status == "unverified_attempt"


def test_absent_attempt_and_quota_summary_do_not_invalidate_primary_or_cycle_one(tmp_path):
    primary(tmp_path)
    assert audit_artifacts(tmp_path).status == "ok"
    family = recovery(tmp_path)
    write(family / "sync_execution.log", verdict())
    (family / "cycle2").mkdir()
    write(family / "cycle2/inline-recovery-summary.json", '{"skipped":"quota","passed":false}')
    assert audit_artifacts(tmp_path).status == "ok"


def test_legacy_primary_layout_with_unrelated_empty_log_is_unchanged(tmp_path):
    write(tmp_path / "legacy/sync_execution.log", "".join(verdict(page) for page in CRITICAL_MARKET_PAGES))
    write(tmp_path / "unrelated-primary/sync_execution.log", "")
    assert audit_artifacts(tmp_path).status == "ok"


def test_direct_inline_single_page_root_identifies_its_attempt_without_other_artifacts(tmp_path):
    latest = write(tmp_path / "inline-recovery-evidence/global-markets/cycle2/sync_execution.log", "")
    write(latest.parent / "sync_runner_receipt.json", '{"results":[{"sheet_name":"Global_Markets"}]}')
    result = audit_artifacts(latest.parent, required_pages=("Global_Markets",))
    assert result.failed_pages == ("Global_Markets",) and result.verdicts[0].status == "unverified_attempt"


@pytest.mark.parametrize("filename,payload", [
    ("sync_runner_receipt.json", "truncated-json"),
    ("page-audit.json", '{"status":"read_error","error":"old failure"}'),
])
def test_later_clean_terminal_and_own_receipt_clear_older_malformed_diagnostics(tmp_path, filename, payload):
    primary(tmp_path)
    family = recovery(tmp_path)
    write(family / "sync_execution.log", "")
    write(family / filename, payload)
    later = write(family / "cycle2/sync_execution.log", verdict())
    write(later.parent / "sync_runner_receipt.json", '{"results":[{"sheet_name":"Global_Markets"}]}')
    assert audit_artifacts(tmp_path).status == "ok"
    assert audit_artifacts(later.parent, required_pages=("Global_Markets",)).status == "ok"


def test_cycle_root_never_reads_parent_receipts_or_ancestor_source_run_identity(tmp_path, monkeypatch):
    parent = tmp_path / "page-refresh-999-global-markets"
    write(parent / "sync_runner_receipt.json", '{"results":[{"sheet_name":"Commodities_FX"}]}')
    cycle = parent / "cycle2"
    write(cycle / "sync_execution.log", verdict())
    write(cycle / "sync_runner_receipt.json", '{"results":[{"sheet_name":"Global_Markets"}]}')
    original = Path.read_bytes
    def bounded_read(path):
        assert path.is_relative_to(cycle)
        return original(path)
    monkeypatch.setattr(Path, "read_bytes", bounded_read)
    assert audit_artifacts(cycle, required_pages=("Global_Markets",)).status == "ok"


def test_source_run_ids_outside_audited_root_do_not_poison_current_source(tmp_path):
    root = tmp_path / "page-refresh-999-global-markets/unrelated-workspace/artifacts"
    primary(root)
    assert audit_artifacts(root).status == "ok"


@pytest.mark.parametrize("group", ["global-markets", "Global_Markets"])
def test_daily_slug_and_manual_canonical_page_artifact_names_bind_empty_attempt(tmp_path, group):
    primary(tmp_path)
    family = recovery(tmp_path, group=group)
    write(family / "sync_execution.log", verdict())
    write(family / "cycle2/sync_execution.log", "")
    assert audit_artifacts(tmp_path).failed_pages == ("Global_Markets",)


def test_unknown_recovery_group_with_no_page_proof_is_explicit_read_error(tmp_path):
    primary(tmp_path)
    write(recovery(tmp_path, group="unknown-group") / "sync_execution.log", "")
    with pytest.raises(OSError, match="scope"):
        audit_artifacts(tmp_path)


def test_scope_receipt_cannot_follow_symlink_outside_artifact_root(tmp_path):
    outside = write(tmp_path / "outside.json", '{"results":[{"sheet_name":"Global_Markets"}]}')
    root = tmp_path / "artifacts"
    primary(root)
    family = recovery(root)
    write(family / "sync_execution.log", verdict())
    (family / "sync_runner_receipt.json").symlink_to(outside)
    with pytest.raises(OSError, match="escapes the artifact root"):
        audit_artifacts(root)


def test_unknown_recovery_scope_cli_and_planner_exit_read_error_without_traceback(tmp_path):
    primary(tmp_path)
    write(tmp_path / "zz-recovery/cycle2/sync_execution.log", "")
    for script in ("audit_sync_outcome.py", "plan_sync_recovery.py"):
        result = subprocess.run([sys.executable, str(ROOT / "scripts" / script), "--root", str(tmp_path)],
                                text=True, capture_output=True, check=False, timeout=20)
        assert result.returncode == 3 and "READ_ERROR" in result.stdout
        assert "Traceback" not in result.stdout + result.stderr
