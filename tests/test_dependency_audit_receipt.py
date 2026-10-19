"""Complete installed-distribution audit coverage; synthetic JSON and no network."""
from __future__ import annotations

import copy
import json
from pathlib import Path
import subprocess
import sys

import pytest

from scripts import verify_dependency_audit as verifier


INSTALLED = [{"name": "httpcore", "version": "1.0.9"},
             {"name": "Synthetic.Package", "version": "2.1.0"}]


def clean_audit(installed=INSTALLED):
    return {"dependencies": [{**entry, "vulns": []} for entry in installed], "fixes": []}


def test_complete_exact_inventory_succeeds_without_a_hardcoded_package_count():
    for size in (1, 2, 7):
        installed = [{"name": f"synthetic-{index}", "version": "1.0"} for index in range(size)]
        receipt = verifier.verify(installed, clean_audit(installed))
        assert receipt == {"schema_version": 1, "status": "pass", "code": "complete",
                           "installed_count": size, "audited_count": size,
                           "missing_count": 0, "unexpected_count": 0,
                           "version_mismatch_count": 0, "vulnerability_count": 0}


def test_case_and_separator_aliases_match_as_python_distribution_names():
    report = clean_audit()
    report["dependencies"][0]["name"] = "HTTPCORE"
    report["dependencies"][1]["name"] = "synthetic___package"
    assert verifier.verify(INSTALLED, report)["status"] == "pass"


@pytest.mark.parametrize("omitted", ("httpcore", "Synthetic.Package"))
def test_any_installed_package_omission_including_url_pinned_httpcore_fails(omitted):
    report = clean_audit()
    report["dependencies"] = [item for item in report["dependencies"] if item["name"] != omitted]
    with pytest.raises(verifier.VerificationError, match="coverage_mismatch") as error:
        verifier.verify(INSTALLED, report)
    assert error.value.details["missing_count"] == 1


def test_skipped_entry_is_not_clean_audit_coverage():
    report = clean_audit()
    report["dependencies"][0] = {"name": "httpcore", "skip_reason": "SYNTHETIC_PRIVATE_URL"}
    with pytest.raises(verifier.VerificationError, match="skipped_dependency"):
        verifier.verify(INSTALLED, report)


def test_skip_marker_cannot_be_hidden_alongside_an_empty_vulnerability_list():
    report = clean_audit()
    report["dependencies"][0]["skip_reason"] = ""
    with pytest.raises(verifier.VerificationError, match="skipped_dependency"):
        verifier.verify(INSTALLED, report)


@pytest.mark.parametrize("source", ("installed", "audit"))
def test_duplicate_normalized_names_fail_even_when_versions_are_identical(source):
    installed, report = copy.deepcopy(INSTALLED), clean_audit()
    entries = installed if source == "installed" else report["dependencies"]
    entries.append({**entries[1], "name": "synthetic-package"})
    with pytest.raises(verifier.VerificationError, match=source + "_duplicate_dependency"):
        verifier.verify(installed, report)


def test_extra_distribution_is_not_a_substitute_for_missing_inventory_member():
    report = clean_audit()
    report["dependencies"][0]["name"] = "different-package"
    with pytest.raises(verifier.VerificationError, match="coverage_mismatch") as error:
        verifier.verify(INSTALLED, report)
    assert error.value.details["missing_count"] == 1
    assert error.value.details["unexpected_count"] == 1


def test_versions_must_match_exactly():
    report = clean_audit()
    report["dependencies"][0]["version"] = "1.0.8"
    with pytest.raises(verifier.VerificationError, match="coverage_mismatch") as error:
        verifier.verify(INSTALLED, report)
    assert error.value.details["version_mismatch_count"] == 1


def test_known_vulnerability_prevents_passing_receipt():
    report = clean_audit()
    report["dependencies"][0]["vulns"] = [{"id": "SYNTHETIC-ADVISORY", "fix_versions": ["1.0.10"]}]
    with pytest.raises(verifier.VerificationError, match="vulnerabilities_present") as error:
        verifier.verify(INSTALLED, report)
    assert error.value.details["vulnerability_count"] == 1


@pytest.mark.parametrize("report", (
    None, [], {}, {"dependencies": []}, {"dependencies": [], "fixes": None},
    {"dependencies": {}, "fixes": []}, {"dependencies": [None], "fixes": []},
    {"dependencies": [], "fixes": [], "error": "SYNTHETIC_PRIVATE_RESPONSE"},
))
def test_malformed_or_partial_report_is_rejected(report):
    with pytest.raises(verifier.VerificationError, match="audit_schema_error"):
        verifier.verify(INSTALLED, report)


@pytest.mark.parametrize("field, value", (("name", None), ("name", ""), ("name", "bad name"),
                                         ("version", None), ("version", 1), ("version", ""),
                                         ("vulns", None), ("vulns", {})))
def test_dependency_entry_requires_real_name_version_and_vulnerability_list(field, value):
    report = clean_audit()
    report["dependencies"][0][field] = value
    with pytest.raises(verifier.VerificationError, match="audit_schema_error"):
        verifier.verify(INSTALLED, report)


@pytest.mark.parametrize("field", ("name", "version", "vulns"))
def test_missing_dependency_field_cannot_default_to_success(field):
    report = clean_audit()
    del report["dependencies"][0][field]
    with pytest.raises(verifier.VerificationError, match="audit_schema_error"):
        verifier.verify(INSTALLED, report)


@pytest.mark.parametrize("field", ("name", "version"))
def test_unknown_identity_rejected_even_if_both_inputs_agree(field):
    installed = copy.deepcopy(INSTALLED)
    installed[0][field] = "UNKNOWN"
    with pytest.raises(verifier.VerificationError, match="unknown_dependency"):
        verifier.verify(installed, clean_audit(installed))


@pytest.mark.parametrize("installed", ([], None, {}, [None], [{"name": "httpcore"}],
                                        [{"name": "httpcore", "version": "1.0.9", "unknown": True}]))
def test_invalid_inventory_is_not_a_complete_audit(installed):
    with pytest.raises(verifier.VerificationError, match="installed_schema_error"):
        verifier.verify(installed, clean_audit())


def test_empty_dependency_result_fails_complete_coverage():
    with pytest.raises(verifier.VerificationError, match="coverage_mismatch"):
        verifier.verify(INSTALLED, {"dependencies": [], "fixes": []})


def test_fix_output_cannot_be_relabelled_as_a_clean_read_only_audit():
    report = clean_audit()
    report["fixes"] = [{"name": "httpcore", "old_version": "1.0.8", "new_version": "1.0.9"}]
    with pytest.raises(verifier.VerificationError, match="unexpected_fixes"):
        verifier.verify(INSTALLED, report)


def invoke(tmp_path, capsys, *, installed=None, audit=None):
    inventory_path, report_path, output_path = [tmp_path / name for name in ("installed.json", "audit.json", "receipt.json")]
    inventory_path.write_text(json.dumps(INSTALLED) if installed is None else installed, encoding="utf-8")
    report_path.write_text(json.dumps(clean_audit()) if audit is None else audit, encoding="utf-8")
    code = verifier.main(["--installed", str(inventory_path), "--audit", str(report_path), "--output", str(output_path)])
    result = capsys.readouterr()
    receipt = json.loads(result.out)
    assert json.loads(output_path.read_text()) == receipt
    assert result.err == ""
    return code, receipt


def test_cli_writes_same_compact_receipt_as_stdout(tmp_path, capsys):
    code, receipt = invoke(tmp_path, capsys)
    assert code == 0
    assert receipt["status"] == "pass"
    assert receipt["installed_count"] == 2


@pytest.mark.parametrize("invalid", ("", "{", "SYNTHETIC_PRIVATE_CONTENT", "NaN",
                                     "[" * 2000 + "]" * 2000,
                                     '{"dependencies": [], "fixes": [], "dependencies": []}'))
def test_cli_malformed_json_has_sanitized_nonzero_receipt(tmp_path, capsys, invalid):
    code, receipt = invoke(tmp_path, capsys, audit=invalid)
    assert code == 1
    assert receipt["status"] == "fail"
    assert "SYNTHETIC_PRIVATE" not in json.dumps(receipt)


def test_duplicate_vulns_key_cannot_hide_a_reported_vulnerability(tmp_path, capsys):
    code, receipt = invoke(tmp_path, capsys, audit='{"dependencies": [{"name": "httpcore", "version": "1.0.9", "vulns": [{"id": "SYNTHETIC"}], "vulns": []}], "fixes": []}')
    assert code == 1
    assert receipt["code"] == "duplicate_json_key"


def test_missing_file_failure_does_not_print_paths(tmp_path, capsys):
    private_path = tmp_path / "SYNTHETIC_PRIVATE_PATH"
    assert verifier.main(["--installed", str(private_path), "--audit", str(private_path)]) == 1
    captured = capsys.readouterr()
    receipt = json.loads(captured.out)
    assert receipt["code"] == "file_read_error"
    assert receipt["error_type"] == "FileNotFoundError"
    assert "SYNTHETIC_PRIVATE_PATH" not in captured.out + captured.err


def test_output_write_failure_cannot_exit_success(tmp_path, capsys):
    inventory, report = tmp_path / "installed.json", tmp_path / "audit.json"
    inventory.write_text(json.dumps(INSTALLED))
    report.write_text(json.dumps(clean_audit()))
    assert verifier.main(["--installed", str(inventory), "--audit", str(report), "--output", str(tmp_path)]) == 1
    assert json.loads(capsys.readouterr().out)["code"] == "output_write_error"


def test_argument_errors_have_no_raw_argument_text(capsys):
    assert verifier.main(["--SYNTHETIC_PRIVATE_ARGUMENT"]) == 1
    captured = capsys.readouterr()
    assert json.loads(captured.out)["code"] == "argument_error"
    assert "SYNTHETIC_PRIVATE" not in captured.out + captured.err


def test_real_cli_uses_stdlib_only_without_importing_inventory_package(tmp_path):
    inventory, report = tmp_path / "installed.json", tmp_path / "audit.json"
    inventory.write_text(json.dumps([{"name": "must-not-import", "version": "1.0"}]))
    report.write_text(json.dumps(clean_audit([{"name": "must-not-import", "version": "1.0"}])))
    (tmp_path / "must_not_import.py").write_text("raise RuntimeError('PACKAGE CODE MUST NOT EXECUTE')\n")
    script = Path(verifier.__file__).resolve()
    run = subprocess.run([sys.executable, "-I", "-S", str(script), "--installed", str(inventory),
                          "--audit", str(report)], capture_output=True, text=True, cwd=tmp_path)
    assert run.returncode == 0, run.stderr
    assert json.loads(run.stdout)["status"] == "pass"
