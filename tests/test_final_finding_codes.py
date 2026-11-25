"""finalize_decision_publication 1.2.0 [FINDING-CODES]: a red verdict names every failing decision finding as
CODE@surface in the log (full audit of 25 Nov 2026: 7 of 42 scheduled syncs of 12-25 Nov ended red on the bare
blocker "decision_failed"). run() is replaced by a scripted fake; no sheet, no network."""
from __future__ import annotations

import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from scripts import finalize_decision_publication as fin  # noqa: E402


def verdict(codes):
    f = [{"severity": "FAIL", "code": c, "surface": "Top_10_Investments", "message": "secret 123,456 SAR"}
         for c in codes] + [{"severity": "WARN", "code": "SOFT", "surface": "X", "message": "w"}]
    return {"script_version": fin.SCRIPT_VERSION, "exit_code": 2 if codes else 0, "audit_exit_codes": [0, 2],
            "blockers": ["decision_failed"] if codes else [], "blocker_published": bool(codes),
            "feed_promoted": False, "audits": {"decision": {"findings": f}}}


def run_with(monkeypatch, tmp_path, v):
    async def fake(*a, **k):
        return v
    monkeypatch.setattr(fin, "run", fake)
    return fin.main(["--sheet-id", "sid", "--publish-blockers", "--json-out", str(tmp_path / "f.json")])


def test_c1_a_red_verdict_names_its_findings_and_no_value(monkeypatch, tmp_path, capsys):
    assert run_with(monkeypatch, tmp_path, verdict(["T10_SOURCE_CONSUMPTION_UNPROVEN", "T10_RUN_STALE"])) == 2
    out = capsys.readouterr().out
    assert ("[FINAL-DIAG] failing decision findings: T10_SOURCE_CONSUMPTION_UNPROVEN@Top_10_Investments, "
            "T10_RUN_STALE@Top_10_Investments") in out
    assert "123,456" not in out and "SOFT" not in out


def test_c2_a_clean_verdict_prints_nothing_new(monkeypatch, tmp_path, capsys):
    assert run_with(monkeypatch, tmp_path, verdict([])) == 0
    assert "[FINAL-DIAG]" not in capsys.readouterr().out
    assert json.loads((tmp_path / "f.json").read_text())["exit_code"] == 0


def test_c3_pure_helper_and_versions():
    from scripts import await_cockpit_receipts as w
    assert fin.SCRIPT_VERSION == "1.2.0" and w.SCRIPT_VERSION == "1.4.0" and w.WAIT_ONLY_MAX_FAULTS == 30
    assert fin.failing_codes({}) == [] and fin.failing_codes({"audits": {"decision": {"findings": "x"}}}) == []
    assert fin.failing_codes(verdict(["A" * 80])) == ["A" * 60 + "@Top_10_Investments"]
