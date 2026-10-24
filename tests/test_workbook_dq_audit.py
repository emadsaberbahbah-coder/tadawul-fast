#!/usr/bin/env python3
"""tests/test_workbook_dq_audit.py - harness for scripts/tfb_workbook_dq_audit.py v1.0.3 (WP-0; v1.0.3 = truthful Top-10 KPI observations).

Pattern: tests/test_export_audit_v1.py (the REAL script runs as a subprocess; no
stand-ins) plus the namespace import used by tests/test_cash_snapshot_certification.py.
Offline: no network, no credentials, no live Sheet. The golden-fixture comparison
against the real 2026-10-10 export runs only when TFB_TEST_EXPORT_XLSX points at it.

Contract:
  P1  --selftest exits 0 and prints "[DQ-SELFTEST v1.0.3] N/N PASS  cases-digest=<frozen>"
  P2  CLI: no args -> 2; missing export -> 2; missing --baseline -> 2; --help -> 0
  P3  determinism: two --selftest runs print the same cases-digest
  P4  D7 guard: the script is not this file; SCRIPT_VERSION = "1.0.3"; no def test_
  P5  golden fixture structure and spot values (tests/fixtures/dq_baseline_2026-10-10.json)
  P6  fixture self-comparison through compare_to_baseline (regressions / critical)
  P7  redaction guard accepts the fixture and rejects injected money
  P8  synthetic negatives through the real audit path (build_synthetic + audit_workbook)
  P9  import safety: importing the module performs no openpyxl / tfb_export_audit import
  P10 workflow wiring (regex, no PyYAML) in .github/workflows/workbook_backup.yml
  P11 backup_workbook._write_local_copy: unset env -> nothing written; set -> 0600 file
  P12 golden run on the real export (skipped without TFB_TEST_EXPORT_XLSX)
  P13 v1.0.3 DQ-37 non-converged predicate: f7_settle_failed or the settle_unresolved: prefix,
      never the bare substring "unresolved"; an INVESTABLE one is listed in the examples
  P14 DQ-45 accepts current/legacy KPI titles by unique headers; missing, ambiguous
      or malformed observations stay unknown; execution text or zero-withheld
      proves exec counts, while plain selected/funded counts do not
  P15 cohort evidence references use physical worksheet rows after blank filtering
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
EXPECT = "[DQ-SELFTEST v1.0.4] 97/97 PASS  cases-digest=c1bdd1a2cc1c3927"
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
    assert b'SCRIPT_VERSION = "1.0.4"' in script
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
    # review fix: DQ-23 count = bad rows (lower is better) - 5 -> 10 must regress, 5 -> 2 must improve (non-critical)
    base23 = copy.deepcopy(fx)
    base23["checks"]["DQ-23"]["count"] = 5
    base23["checks"]["DQ-23"]["status"] = "FAIL"
    for cur_count, regressed, improved in ((10, ["DQ-23"], []), (2, [], ["DQ-23"])):
        cur23 = copy.deepcopy(base23)
        cur23["checks"]["DQ-23"]["count"] = cur_count
        cmp23 = dq.compare_to_baseline(_as_report(cur23), base23)
        assert [e["id"] for e in cmp23["regressions"]] == regressed, (cur_count, cmp23["regressions"])
        assert [e["id"] for e in cmp23["improvements"]] == improved, (cur_count, cmp23["improvements"])
        assert cmp23["decision_critical_regressions"] == []
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
    assert p.stdout.strip() == "1.0.4"


def test_p10_workflow_wiring():
    text = open(WORKFLOW, encoding="utf-8").read()
    run_backup = text.split("- name: Run backup", 1)[1].split("- name:", 1)[0]
    assert "TFB_BACKUP_LOCAL_COPY:" in run_backup
    assert "steps.dq_mode.outputs.mode == 'observe'" in run_backup and "format('{0}/tfb_export.xlsx', runner.temp)" in run_backup
    audit = text.split("- name: Workbook DQ audit (WP-0)", 1)[1].split("- name:", 1)[0]
    assert re.search(r"if:\s*\$\{\{.*steps\.dq_mode\.outputs\.mode", audit)
    assert "continue-on-error: ${{ steps.dq_mode.outputs.mode != 'enforce' }}" in audit
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
    assert bw.__version__ == "1.5.1"   # v1.5.1: resumable upload resumes (tests/test_backup_upload_resume.py)
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
        line = [ln for ln in text.splitlines() if ln.startswith("[DQ-AUDIT v1.0.4]")][-1]
        assert "regressions=0 critical_regressions=0" in line, line
        report = json.load(open(out, encoding="utf-8"))
    fx = _fixture()
    assert report["export_sha256"] == fx["export_sha256"]
    # v1.0.4 adds the full-source denominators to DQ-72. Preserve the frozen
    # historical fixture and every old value; derive only these new witnesses.
    for page, count in fx["expect"].items():
        fx["checks"]["DQ-72"]["values"][f"{page}:source_rows"] = count
    assert dq.reduce_baseline(report)["checks"] == fx["checks"]
    assert report["baseline"]["decision_critical_regressions"] == [] and report["baseline"]["regressions"] == []
    dq._assert_redacted(report)


def test_p13_dq37_counts_both_receipts_by_prefix():
    """v1.0.3 (WP-2bc integration): engine 5.153.0 under TFB_SETTLE_UNRESOLVED=enforce publishes a
    non-converged row with settle_unresolved:enforce:... and WITHOUT f7_settle_failed:non_converged.
    DQ-37 must still count it; a margin receipt ending in ':unresolved' must not be counted."""
    hdr = ["Symbol", "Investability Status", "Warnings"]
    # six INVESTABLE rc-flip rows first: on the real export such flips fill every
    # example slot before the later non-converged rows are reached
    flips = [["F%d.US" % i, "INVESTABLE", "f7_settle:observe:st3:ov=60.0>55.0:rc=buy>hold"] for i in range(6)]
    rows = [hdr] + flips + [
            ["AAA.US", "WATCHLIST", "f7_settle_failed:non_converged;rel_path:sheet"],
            ["BBB.US", "WATCHLIST", "settle_unresolved:enforce:pass_limit:p3:ov=55.02:rc=hold;rank_skipped_settle_unresolved"],
            ["CCC.US", "WATCHLIST", "margin_publish:gross_margin:unresolved;rel_path:sheet"],
            ["DDD.US", "INVESTABLE", "settle_unresolved:observe:pass_limit:p3:ov=61.2:rc=buy;f7_settle_failed:non_converged"],
            ["EEE.US", "WATCHLIST", "unresolved_settle_unresolved:x;f7_settle:observe:st3;name_unresolved"]]
    page = dq.Page("Global_Markets", rows, len(rows))
    rec = dq.dq_37_f7({"pages": {"Global_Markets": page}})
    assert rec["values"]["Global_Markets:non_converged"] == 3, rec["values"]
    assert rec["values"]["non_converged_total"] == 3 and rec["status"] == "FAIL", rec
    assert rec["count"] == 6          # count is the rc-flip count (the six F*.US rows)
    assert rec["examples"][0] == "DDD.US INVESTABLE non-converged (GM)", rec["examples"]
    assert dq.F7_NONCONV_PREFIXES == ("f7_settle_failed", "settle_unresolved:")
    # v1.0.0 predicate on the same rows would have missed BBB.US (the enforce receipt)
    v100 = sum(1 for tg in page.tags if any(t.startswith("f7_settle_failed") for t in tg))
    assert v100 == 2


def test_dq37_reads_structured_failure_and_deduplicates_warning():
    hdr = ["Symbol", "Investability Status", "Warnings", "Scoring Errors"]
    record = 'scoring_settlement:{"status":"non_converged","reason":"pass_limit"}'
    rows = [hdr,
            ["PDN.AX", "WATCHLIST", "", record],
            ["BOTH.US", "INVESTABLE", "settle_unresolved:enforce:pass_limit", record],
            ["ERROR.US", "WATCHLIST", "", 'scoring_settlement:{"status":"error"};other:ok'],
            ["OK.US", "WATCHLIST", "", 'scoring_settlement:{"status":"converged","reason":"non_converged"}'],
            ["OTHER.US", "WATCHLIST", "", 'other:{"status":"non_converged"}'],
            ["BAD.US", "WATCHLIST", "", 'scoring_settlement:{"status":'],
            ["NEST.US", "WATCHLIST", "", 'scoring_settlement:{"status":"converged","nested":{"status":"error"}}']]
    page = dq.Page("Global_Markets", rows, len(rows))
    rec = dq.dq_37_f7({"pages": {"Global_Markets": page}})
    assert rec["values"]["non_converged_total"] == 3
    assert rec["values"]["Global_Markets:non_converged"] == 3
    assert rec["count"] == 0 and rec["status"] == "FAIL"
    assert rec["examples"][0].startswith("BOTH.US INVESTABLE")
    assert rec["values"]["invalid_settlement_diagnostics_total"] == 1
    assert [dq._settlement_failed(page, row, tags) for row, tags in zip(page.body, page.tags)] == [True, True, True, False, False, True, False]


def test_dq37_malformed_status_cannot_crash_or_silently_pass():
    statuses = ['{"status":[]}', '{"status":{}}', '{"status":true}',
                '{"status":"error","status":"converged"}', '{"other":"converged"}', '[]']
    rows = [["Symbol", "Warnings", "Scoring Errors"]] + [
        ["SYNTH%d.US" % i, "", "scoring_settlement:" + raw] for i, raw in enumerate(statuses)]
    page = dq.Page("Global_Markets", rows, len(rows))
    rec = dq.dq_37_f7({"pages": {"Global_Markets": page}})
    assert rec["status"] == "FAIL"
    assert rec["values"]["non_converged_total"] == 0
    assert rec["values"]["invalid_settlement_diagnostics_total"] == len(statuses)


def test_dq37_structured_error_also_marks_real_row_flags():
    with tempfile.TemporaryDirectory() as tmp:
        path = dq.build_synthetic(tmp, clean=True)
        wb = openpyxl.load_workbook(path)
        ws = wb["Global_Markets"]
        headers = [c.value for c in ws[1]]
        ws.cell(2, headers.index("Warnings") + 1).value = ""
        ws.cell(2, headers.index("Scoring Errors") + 1).value = 'scoring_settlement:{"status":"non_converged"}'
        wb.save(path)
        wb.close()
        report = dq.audit_workbook(path, include_rows=True)
        check = next(c for c in report["checks"] if c["id"] == "DQ-37")
        assert check["values"]["non_converged_total"] == 1
        assert any("DQ-37" in r.get("checks_failed", []) for r in report["rows"])


def _dq45_context(header, values):
    return {"t10": [header, values],
            "status_globals": {"TFB Decision Feed": "NOT_ACTIONABLE(synthetic)"},
            "board": {"AAA.US", "BBB.US"}, "qualified": {"AAA.US", "BBB.US"}}


def test_dq45_reads_both_cash_titles_and_selected_header():
    for cash in ("Deployable (SAR)", "Entered cash (SAR)"):
        for selected, expected in (("2 exec + 1 pend / 10", 2),
                                   ("2 exec + 1 fast-track suspended + 1 research + 1 grace / 10", 2),
                                   ("0 / 10 (withheld)", 0), ("0 exec", 0),
                                   ("0 exec + 1 grace / 10", 0)):
            for header, values in (([cash, "Selected", "Passed"], [None, selected, 4]),
                                   (["Passed", "Selected", cash], [4, selected, None]),
                                   ([cash, "Scanned", "Passed", "Selected"], [None, 10, 4, selected])):
                result = dq.dq_45_feed_state(_dq45_context(header, values))
                assert result["status"] == "INFO"
                assert result["count"] == result["values"]["exec_seats"] == expected, (header, selected, result)
                dq._assert_redacted(result)


def test_dq45_unknown_for_missing_or_ambiguous_kpi_strip():
    valid = ["Entered cash (SAR)", "Selected", "Passed"]
    cases = [([], []), (["Selected", "Passed"], ["2 exec", 2]),
             (["Entered cash (SAR)", "Selected"], [None, "2 exec"]),
             (["Entered cash (SAR)", "Passed"], [None, 2]),
             (["Entered cash (SAR)", "Selected", "Selected", "Passed"], [None, "2 exec", "2 exec", 2]),
             (["Entered cash (SAR)", "Selected", "Passed", "Passed"], [None, "2 exec", 2, 2]),
             (["Entered cash (SAR)", "Deployable (SAR)", "Selected", "Passed"], [None, None, "2 exec", 2]),
             (["Entered cash (SAR)", "Selected", "Passed", "Scanned", "Scanned"], [None, "2 exec", 2, 10, 10]),
             (valid, [None])]
    for header, values in cases:
        result = dq.dq_45_feed_state(_dq45_context(header, values))
        assert result["status"] == "INFO"
        assert result["count"] is None and result["values"]["exec_seats"] is None, (header, values, result)
    ctx = _dq45_context(valid, [None, "2 exec", 2])
    ctx["t10"] += [["Deployable (SAR)", "Selected", "Passed"], [None, "2 exec", 2]]
    assert dq.dq_45_feed_state(ctx)["values"]["exec_seats"] is None
    ctx["t10"] = [valid]
    assert dq.dq_45_feed_state(ctx)["values"]["exec_seats"] is None
    # A separate cash-only portfolio strip is not a Top-10 KPI block.
    ctx["t10"] = [["Deployable (SAR)"], [None], valid, [None, "2 exec", 2]]
    assert dq.dq_45_feed_state(ctx)["values"]["exec_seats"] == 2


def test_dq45_malformed_selected_is_unknown_not_zero():
    for selected in (None, "", True, False, float("inf"), float("nan"), -1, 1.5,
                     0, 0.0, "0", 4, "3 / 10", "0 / 10",
                     "-1 exec", "-2 exec", "1.5 exec", "2e3 exec", "2 / 1", "two",
                     "2 exec + 3 exec", "2 exec + 3 exec / 10", "withheld", "unknown (withheld)",
                     "2 exec / 10 garbage", "0 / 10 (withheld) extra", "2 / 10 (withheld)", "10%",
                     "2 exec + 1 pend + 1 pending / 10", "9 exec + 2 grace / 10", "9" * 5000):
        result = dq.dq_45_feed_state(_dq45_context(
            ["Entered cash (SAR)", "Selected", "Passed"], [None, selected, 2]))
        assert result["status"] == "INFO"
        assert result["count"] is None and result["values"]["exec_seats"] is None, (selected, result)
        dq._assert_redacted(result)


def test_row_evidence_refs_preserve_physical_worksheet_rows():
    with tempfile.TemporaryDirectory() as tmp:
        path = dq.build_synthetic(tmp, clean=True)
        wb = openpyxl.load_workbook(path)
        mp = wb["My_Portfolio"]
        another_holding = [cell.value for cell in mp[2]]
        another_holding[0] = "G2.US"
        mp.append(another_holding)
        wb.save(path)
        before = dq.audit_workbook(path, include_rows=True)
        # Both symbol tables have real records at rows 2 and 4, separated by
        # a completely blank row. Filtering must not renumber the evidence.
        for page in ("Market_Leaders", "My_Portfolio"):
            wb[page].insert_rows(3)
        wb.save(path)
        wb.close()
        after = dq.audit_workbook(path, include_rows=True)
    refs = {(r["page"], r["symbol"]): int(r["evidence_ref"].rsplit("!", 1)[1]) for r in after["rows"]}
    assert refs[("Market_Leaders", "S1.SR")] == 2
    assert refs[("Market_Leaders", "S2.SR")] == 4
    assert refs[("My_Portfolio", "G1.US")] == 2
    assert refs[("My_Portfolio", "G2.US")] == 4
    assert before["rows_summary"] == after["rows_summary"]
    B, A = ({c["id"]: c for c in report["checks"]} for report in (before, after))
    # Adding blank storage legitimately changes padding/size; symbol business
    # measurements, contracts, arithmetic and permission checks are unchanged.
    for cid in ("DQ-01", "DQ-07", "DQ-08", "DQ-30", "DQ-31", "DQ-38", "DQ-45", "DQ-53", "DQ-60"):
        assert B[cid] == A[cid], cid
    dq._assert_redacted(after)


if __name__ == "__main__":
    tests = [test_p1_selftest_passes, test_p2_cli_contract, test_p3_selftest_deterministic, test_p4_not_a_copy_of_the_script,
             test_p5_golden_fixture_structure, test_p6_fixture_self_comparison, test_p7_redaction_guard,
             test_p8_synthetic_negatives_through_the_real_path, test_p9_import_is_side_effect_free, test_p10_workflow_wiring,
             test_p11_backup_local_copy_flag_default, test_p12_golden_run_on_the_real_export,
             test_p13_dq37_counts_both_receipts_by_prefix, test_dq37_reads_structured_failure_and_deduplicates_warning,
             test_dq37_structured_error_also_marks_real_row_flags, test_dq45_reads_both_cash_titles_and_selected_header,
             test_dq45_unknown_for_missing_or_ambiguous_kpi_strip, test_dq45_malformed_selected_is_unknown_not_zero,
             test_row_evidence_refs_preserve_physical_worksheet_rows]
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
    print("[DQ-AUDIT HARNESS v1.0.3] %d/%d PASS (%d skipped)" % (len(tests) - fails - skipped, len(tests), skipped))
    sys.exit(1 if fails else 0)
