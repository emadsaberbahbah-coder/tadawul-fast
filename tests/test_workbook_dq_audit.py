#!/usr/bin/env python3
"""tests/test_workbook_dq_audit.py - harness for scripts/tfb_workbook_dq_audit.py v1.0.0 (WP-0).

Pattern: tests/test_export_audit_v1.py (the REAL script runs as a subprocess; no
stand-ins) plus the namespace import used by tests/test_cash_snapshot_certification.py.
Offline: no network, no credentials, no live Sheet. The golden-fixture comparison
against the real 2026-10-10 export runs only when TFB_TEST_EXPORT_XLSX points at it.

Contract:
  P1  --selftest exits 0 and prints "[DQ-SELFTEST v1.0.0] N/N PASS  cases-digest=<frozen>"
  P2  CLI: no args -> 2; missing export -> 2; missing --baseline -> 2; --help -> 0
  P3  determinism: two --selftest runs print the same cases-digest
  P4  D7 guard: the script is not this file; SCRIPT_VERSION = "1.0.0"; no def test_
  P5  golden fixture structure and spot values (tests/fixtures/dq_baseline_2026-10-10.json)
  P6  fixture self-comparison through compare_to_baseline (regressions / critical)
  P7  redaction guard accepts the fixture and rejects injected money
  P8  synthetic negatives through the real audit path (build_synthetic + audit_workbook)
  P9  import safety: importing the module performs no openpyxl / tfb_export_audit import
  P10 workflow wiring (regex, no PyYAML) in .github/workflows/workbook_backup.yml
  P11 backup_workbook._write_local_copy: unset env -> nothing written; set -> 0600 file
  P12 golden run on the real export (skipped without TFB_TEST_EXPORT_XLSX)
Runs under pytest or directly (python tests/test_workbook_dq_audit.py).
"""
from __future__ import annotations

import copy
import hashlib
import json
import os
import re
import stat
import subprocess
import sys
import tempfile

try:
    import pytest
except ImportError:  # pragma: no cover - direct run without pytest
    pytest = None

if pytest is not None:
    openpyxl = pytest.importorskip("openpyxl")  # heavy-lane dependency; lean environments skip this module
else:  # pragma: no cover
    import openpyxl  # noqa: F401

HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.abspath(os.path.join(HERE, ".."))
if REPO not in sys.path:
    sys.path.insert(0, REPO)
SCRIPT = os.path.join(REPO, "scripts", "tfb_workbook_dq_audit.py")
FIXTURE = os.path.join(HERE, "fixtures", "dq_baseline_2026-10-10.json")
WORKFLOW = os.path.join(REPO, ".github", "workflows", "workbook_backup.yml")
EXPECT = "[DQ-SELFTEST v1.0.0] 97/97 PASS  cases-digest=c1bdd1a2cc1c3927"
EXPORT_SHA16 = "f5d327afc9fb409c"
DECISION_CRITICAL = {"DQ-07", "DQ-08", "DQ-09", "DQ-10", "DQ-16", "DQ-22", "DQ-24", "DQ-25", "DQ-30", "DQ-33", "DQ-34",
                     "DQ-38", "DQ-46", "DQ-53", "DQ-62"}

from scripts import tfb_workbook_dq_audit as dq  # noqa: E402  (namespace import, as the cash-certification test does)


def _run(*args, timeout=600):
    p = subprocess.run([sys.executable, SCRIPT] + list(args), capture_output=True, text=True, timeout=timeout, cwd=REPO)
    return p.returncode, (p.stdout or "") + (p.stderr or "")


def _fixture():
    with open(FIXTURE, encoding="utf-8") as fh:
        return json.load(fh)


def _as_report(fixture):
    """A fixture as a minimal report (checks list) for compare_to_baseline."""
    return {"checks": [dict(id=k, **v) for k, v in fixture["checks"].items()]}


def test_p1_selftest_passes():
    rc, out = _run("--selftest")
    assert rc == 0, out[-1200:]
    assert EXPECT in out, out[-400:]


def test_p2_cli_contract():
    rc, _ = _run()
    assert rc == 2
    rc, _ = _run(os.path.join(HERE, "no_such_export.xlsx"))
    assert rc == 2
    rc, out = _run("--help")
    assert rc == 0 and "--selftest" in out and "--baseline" in out
    with tempfile.TemporaryDirectory() as tmp:
        path = dq.build_synthetic(tmp, clean=True)
        rc, out = _run(path, "--baseline", os.path.join(tmp, "no_such.json"), "--quiet")
        assert rc == 2 and "baseline not found" in out


def test_p3_selftest_deterministic():
    _, a = _run("--selftest")
    _, b = _run("--selftest")
    da = re.search(r"cases-digest=([0-9a-f]{16})", a)
    db = re.search(r"cases-digest=([0-9a-f]{16})", b)
    assert da and db and da.group(1) == db.group(1) == EXPECT.rsplit("=", 1)[1]


def test_p4_not_a_copy_of_the_script():
    with open(SCRIPT, "rb") as f:
        script = f.read()
    with open(os.path.abspath(__file__), "rb") as f:
        me = f.read()
    assert hashlib.sha256(script).hexdigest() != hashlib.sha256(me).hexdigest()
    assert b'SCRIPT_VERSION = "1.0.0"' in script
    assert b"def test_" not in script


def test_p5_golden_fixture_structure():
    fx = _fixture()
    assert fx["version"] == "1.0.0" and fx["script"] == "tfb_workbook_dq_audit"
    assert fx["export_sha256"].startswith(EXPORT_SHA16)
    assert fx["asof"] == "2026-10-10"
    assert fx["expect"] == {"Market_Leaders": 255, "Global_Markets": 6609, "Commodities_FX": 453, "Mutual_Funds": 2474}
    ids = sorted(fx["checks"])
    assert ids == [f"DQ-{i:02d}" for i in range(1, 76)]
    statuses = {c["status"] for c in fx["checks"].values()}
    assert statuses <= {"PASS", "WARN", "FAIL", "INFO", "NOT_CHECKED"}
    assert DECISION_CRITICAL <= set(fx["checks"])
    C = fx["checks"]
    assert C["DQ-01"]["status"] == "PASS" and C["DQ-01"]["values"] == fx["expect"]
    assert C["DQ-09"]["count"] == 5 and C["DQ-10"]["count"] == 14
    assert C["DQ-16"]["count"] == 10
    assert C["DQ-24"]["count"] == 795 and C["DQ-25"]["count"] == 80 and C["DQ-30"]["count"] == 360
    assert C["DQ-33"]["count"] == 153 and C["DQ-34"]["count"] == 9
    assert C["DQ-37"]["values"]["flips_total"] == 2618 and C["DQ-37"]["values"]["non_converged_total"] == 46
    assert C["DQ-38"]["status"] == "PASS" and C["DQ-38"]["values"]["Global_Markets:investable_without_roi12"] == 4
    assert C["DQ-46"]["status"] == "FAIL" and C["DQ-46"]["count"] == 3
    assert C["DQ-48"]["values"]["rows"] == 500 and C["DQ-48"]["values"]["news_unknown"] == 440
    assert C["DQ-61"]["count"] == 21670 and C["DQ-63"]["values"]["duplicate_matured_keys"] == 27
    assert C["DQ-64"]["values"]["target_hit"] == 4576
    counts = {s: sum(1 for c in C.values() if c["status"] == s) for s in ("PASS", "WARN", "FAIL", "INFO")}
    assert counts == {"PASS": 10, "WARN": 19, "FAIL": 44, "INFO": 2}, counts


def test_p6_fixture_self_comparison():
    fx = _fixture()
    same = dq.compare_to_baseline(_as_report(fx), fx, "dq_baseline_2026-10-10.json")
    assert same["regressions"] == [] and same["decision_critical_regressions"] == [] and same["compared_n"] == 75
    worse = copy.deepcopy(fx)
    worse["checks"]["DQ-33"]["count"] = 154
    worse["checks"]["DQ-46"]["count"] = 5
    cmp_ = dq.compare_to_baseline(_as_report(worse), fx)
    assert {"DQ-33", "DQ-46"} <= set(cmp_["decision_critical_regressions"])
    noncrit = copy.deepcopy(fx)
    noncrit["checks"]["DQ-43"]["count"] = fx["checks"]["DQ-43"]["count"] + 1
    cmp2 = dq.compare_to_baseline(_as_report(noncrit), fx)
    assert [e["id"] for e in cmp2["regressions"]] == ["DQ-43"] and cmp2["decision_critical_regressions"] == []
    better = copy.deepcopy(fx)
    better["checks"]["DQ-33"]["count"] = 0
    better["checks"]["DQ-33"]["status"] = "PASS"
    cmp3 = dq.compare_to_baseline(_as_report(better), fx)
    assert [e["id"] for e in cmp3["improvements"]] == ["DQ-33"] and cmp3["regressions"] == []


def test_p7_redaction_guard():
    fx = _fixture()
    dq._assert_redacted(fx)
    for bad in ({"checks": {"DQ-60": {"cash_sar": 1234.5}}}, {"examples": ["12,345 SAR"]}, {"d": "12345.6789 SAR"},
                {"x": "equity=99,999 SAR"}, {"t": "Cash floor 10% of NAV 123456"}):
        try:
            dq._assert_redacted(bad)
        except dq.RedactionError:
            continue
        raise AssertionError(f"redaction guard accepted {bad!r}")
    text = open(FIXTURE, encoding="utf-8").read()
    assert not re.search(r"\d{1,3}(,\d{3})+(\.\d+)?\s?(SAR|USD)", text)
    assert not re.search(r"(?i)\d[\d,]*(\.\d+)?\s*(SAR|USD)\b", text)


def test_p8_synthetic_negatives_through_the_real_path():
    with tempfile.TemporaryDirectory() as tmp:
        defect = dq.audit_workbook(dq.build_synthetic(tmp, clean=False), expect=dq.SYN_EXPECT)
        clean = dq.audit_workbook(dq.build_synthetic(tmp, clean=True), expect=dq.SYN_EXPECT)
    D = {c["id"]: c for c in defect["checks"]}
    C = {c["id"]: c for c in clean["checks"]}
    for cid, count in (("DQ-24", 1), ("DQ-61", 1), ("DQ-10", 1), ("DQ-09", 1), ("DQ-33", 1), ("DQ-38", 1), ("DQ-30", 1)):
        assert D[cid]["status"] == "FAIL" and D[cid]["count"] == count, (cid, D[cid]["status"], D[cid]["count"])
    assert D["DQ-46"]["status"] == "FAIL" and D["DQ-57"]["status"] == "FAIL" and D["DQ-66"]["status"] == "FAIL"
    assert D["DQ-70"]["status"] == "WARN"
    assert not [c["id"] for c in clean["checks"] if c["status"] == "FAIL"]
    assert all(c["status"] != "PASS" for c in clean["checks"] if c["id"].startswith("NC-"))
    assert dq.exit_code(clean) == 0 and dq.exit_code(defect) == 1
    assert C["DQ-57"]["status"] == "PASS" and C["DQ-60"]["status"] == "PASS"


def test_p9_import_is_side_effect_free():
    code = ("import sys; sys.path.insert(0, 'scripts'); import tfb_workbook_dq_audit as m; "
            "assert 'openpyxl' not in sys.modules; assert 'tfb_export_audit' not in sys.modules; "
            "assert 'scripts.tfb_export_audit' not in sys.modules; print(m.SCRIPT_VERSION)")
    p = subprocess.run([sys.executable, "-I", "-c", code], capture_output=True, text=True, cwd=REPO, timeout=120)
    assert p.returncode == 0, p.stderr
    assert p.stdout.strip() == "1.0.0"


def test_p10_workflow_wiring():
    text = open(WORKFLOW, encoding="utf-8").read()
    run_backup = text.split("- name: Run backup", 1)[1].split("- name:", 1)[0]
    assert "TFB_BACKUP_LOCAL_COPY:" in run_backup
    assert "vars.TFB_DQ_AUDIT_MODE == 'observe'" in run_backup and "format('{0}/tfb_export.xlsx', runner.temp)" in run_backup
    audit = text.split("- name: Workbook DQ audit (WP-0)", 1)[1].split("- name:", 1)[0]
    assert re.search(r"if:\s*\$\{\{.*vars\.TFB_DQ_AUDIT_MODE", audit)
    assert "continue-on-error: ${{ vars.TFB_DQ_AUDIT_MODE != 'enforce' }}" in audit
    assert "scripts/tfb_workbook_dq_audit.py" in audit and "--baseline tests/fixtures/dq_baseline_2026-10-10.json" in audit
    upload = text.split("- name: Upload DQ report", 1)[1].split("- name:", 1)[0]
    assert "uses: actions/upload-artifact@v7" in upload
    path_block = upload.split("path:", 1)[1].split("retention-days", 1)[0]
    assert "dq_report.json" in path_block and "dq_scorecard.md" in path_block and ".xlsx" not in path_block
    assert "if-no-files-found: warn" in upload and "restore_test != 'true'" in upload
    cleanup = text.split("- name: Remove Google credentials", 1)[1]
    assert re.search(r"rm -f .*gcreds\.json.*tfb_export\.xlsx", cleanup)
    assert "v1.2.0" in text


def test_p11_backup_local_copy_flag_default():
    from scripts import backup_workbook as bw
    assert bw.__version__ == "1.5.0"
    with tempfile.TemporaryDirectory() as tmp:
        old = os.environ.pop("TFB_BACKUP_LOCAL_COPY", None)
        try:
            bw._write_local_copy(b"x", "abc")
            assert os.listdir(tmp) == []
            target = os.path.join(tmp, "e.xlsx")
            os.environ["TFB_BACKUP_LOCAL_COPY"] = target
            assert bw._write_local_copy(b"xyz", "abc") is None
            assert open(target, "rb").read() == b"xyz"
            assert stat.S_IMODE(os.stat(target).st_mode) == 0o600
            os.environ["TFB_BACKUP_LOCAL_COPY"] = os.path.join(tmp, "e.xlsx", "nested", "no.xlsx")   # parent is a file
            assert bw._write_local_copy(b"x", "abc") is None                                        # logged, never raised
        finally:
            if old is None:
                os.environ.pop("TFB_BACKUP_LOCAL_COPY", None)
            else:
                os.environ["TFB_BACKUP_LOCAL_COPY"] = old


def test_p12_golden_run_on_the_real_export():
    export = os.getenv("TFB_TEST_EXPORT_XLSX")
    if not export or not os.path.exists(export):
        if pytest is not None:
            pytest.skip("TFB_TEST_EXPORT_XLSX not set - golden run against the 2026-10-10 export skipped")
        return
    with tempfile.TemporaryDirectory() as tmp:
        out = os.path.join(tmp, "dq_report.json")
        rc, text = _run(export, "--json", out, "--baseline", FIXTURE, "--quiet")
        assert rc == 0, text[-600:]
        line = [ln for ln in text.splitlines() if ln.startswith("[DQ-AUDIT v1.0.0]")][-1]
        assert "regressions=0 critical_regressions=0" in line, line
        report = json.load(open(out, encoding="utf-8"))
    fx = _fixture()
    assert report["export_sha256"] == fx["export_sha256"]
    assert dq.reduce_baseline(report)["checks"] == fx["checks"]
    assert report["baseline"]["decision_critical_regressions"] == [] and report["baseline"]["regressions"] == []
    dq._assert_redacted(report)


if __name__ == "__main__":
    tests = [test_p1_selftest_passes, test_p2_cli_contract, test_p3_selftest_deterministic, test_p4_not_a_copy_of_the_script,
             test_p5_golden_fixture_structure, test_p6_fixture_self_comparison, test_p7_redaction_guard,
             test_p8_synthetic_negatives_through_the_real_path, test_p9_import_is_side_effect_free, test_p10_workflow_wiring,
             test_p11_backup_local_copy_flag_default, test_p12_golden_run_on_the_real_export]
    fails = skipped = 0
    for t in tests:
        try:
            t()
            print("PASS", t.__name__)
        except AssertionError as exc:
            fails += 1
            print("FAIL", t.__name__, "::", str(exc)[:300])
        except BaseException as exc:  # pytest.skip raises Skipped (an OutcomeException, not an Exception)
            if type(exc).__name__ != "Skipped":
                raise
            skipped += 1
            print("SKIP", t.__name__, "::", str(exc)[:160])
    print("[DQ-AUDIT HARNESS v1.0.0] %d/%d PASS (%d skipped)" % (len(tests) - fails - skipped, len(tests), skipped))
    sys.exit(1 if fails else 0)
