"""Render the existing live-audit evidence without changing acceptance.

Use the same JSON artifacts operators download today. Missing or inconsistent
evidence is shown as UNKNOWN, never a passing audit. This command performs no
network calls, workbook writes, baseline acceptance or decision publication.
"""
from __future__ import annotations

import argparse
import json
import math
from pathlib import Path
import sys
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from core.secret_redaction import redact_text


NEXT_ACTION = {
    "PF_SOURCE_STALE": "Reacquire My_Portfolio, then refresh the native Portfolio decision.",
    "PF_RUN_STALE": "Refresh the native Portfolio decision after its sources.",
    "PF_OLDER_THAN_SOURCE": "Refresh the native Portfolio decision after its sources.",
    "T10_OLDER_THAN_SOURCE": "Refresh native Top10 after all required market sources complete.",
    "T10_POOL_RECONCILIATION_UNPROVEN": "Verify the native source receipt against this acquisition cohort.",
    "CH_BASELINE_EMPTY": "Prepare and review a healthy provider reference; do not auto-accept this observation.",
    "CH_BASELINE_MISSING": "Prepare and review a healthy provider reference; do not auto-accept this observation.",
    "CH_BASELINE_STALE": "Review the reference age and current provider evidence before renewal.",
}
_NO_PROCESS = object()


def _cell(value: Any) -> str:
    return redact_text(str(value)).replace("|", " / ").replace("\n", " ").replace("\r", " ")[:500]


def _read(path: Path) -> dict:
    def finite_float(value):
        number = float(value)
        if not math.isfinite(number):
            raise ValueError("nonfinite JSON number")
        return number
    def unique(pairs):
        result = {}
        for key, value in pairs:
            if key in result:
                raise ValueError("duplicate JSON key")
            result[key] = value
        return result
    result = json.loads(path.read_text(encoding="utf-8"), object_pairs_hook=unique, parse_float=finite_float,
                        parse_constant=lambda _: (_ for _ in ()).throw(ValueError("nonfinite JSON")))
    if not isinstance(result, dict):
        raise ValueError("audit report is not an object")
    return result


def report_rows(name: str, report: dict, process_exit=_NO_PROCESS) -> tuple[str, list[list[Any]]]:
    if (not isinstance(report, dict) or
            not isinstance(report.get("summary", {}), dict) or
            not isinstance(report.get("findings", []), list) or
            not isinstance(report.get("pages", []), (list, dict))):
        return "UNKNOWN", [[name, "UNKNOWN", "REPORT_UNVERIFIED", "*",
                            "Audit report structure is invalid.", "Inspect the retained report."]]
    summary = report.get("summary") or {}
    code = summary.get("exit_code", report.get("exit_code"))
    valid_code = type(code) is int and code in (0, 1, 2, 3)
    process_matches = process_exit is _NO_PROCESS or (type(process_exit) is int and process_exit == code)
    state = ("UNKNOWN" if not valid_code or not process_matches else
             "PASS" if code == 0 else "WARN" if code == 1 else "FAIL")
    rows = []
    if state == "UNKNOWN":
        rows.append([name, "UNKNOWN", "REPORT_UNVERIFIED", "*",
                     "Missing/invalid report exit or disagreement with process exit.",
                     "Inspect the retained report and process log."])
    fatal = report.get("fatal") or summary.get("fatal")
    if fatal:
        rows.append([name, "FAIL", "AUDIT_UNAVAILABLE", "*", fatal,
                     "Restore the reader/configuration; do not treat unavailable evidence as PASS."])
        state = "FAIL"
    findings = report.get("findings") or []
    for finding in findings:
        if not isinstance(finding, dict):
            continue
        severity = str(finding.get("severity", "UNKNOWN")).upper()
        if severity not in {"FAIL", "CONTROL", "ALERT", "WARN", "WARNING", "ERROR"}:
            continue
        code_name = str(finding.get("code") or "UNCLASSIFIED")
        rows.append([name, severity, code_name,
                     finding.get("surface") or finding.get("page") or "*",
                     finding.get("message") or finding.get("detail") or "See report.",
                     NEXT_ACTION.get(code_name, "Correct the reported evidence and rerun the relevant audit.")])
    for page in report.get("pages", []):
        if not isinstance(page, dict):
            continue
        for field, severity in (("failures", "FAIL"), ("warnings", "WARN")):
            for message in page.get(field, []):
                rows.append([name, severity, "COVERAGE_GATE", page.get("page", "*"), message,
                             "Verify fresh acquisition, required names/prices and the exact source scope."])
    # Contradictory passing metadata must never hide an explicit failed check.
    if any(row[1] in {"FAIL", "CONTROL", "ALERT", "ERROR"} for row in rows):
        state = "FAIL"
    return state, rows


def render(reports: list[tuple[str, dict, Any]]) -> str:
    lines = ["## Live audit triage", "", "| Audit | Exit | Evidence result |",
             "|---|---:|---|"]
    findings = []
    for name, report, process_exit in reports:
        state, rows = report_rows(name, report, process_exit)
        code = ((report.get("summary") or {}).get("exit_code", report.get("exit_code"))
                if isinstance(report, dict) and isinstance(report.get("summary", {}), dict) else None)
        lines.append(f"| {_cell(name)} | {_cell(code if code is not None else 'unknown')} | {state} |")
        findings.extend(rows)
    lines += ["", "Passing code tests or a successful source write is not final decision acceptance.",
              "This summary does not refresh data, accept a reference, or release a decision feed.", ""]
    if findings:
        lines += ["| Audit | Severity | Code | Page / surface | Reason | Next action |",
                  "|---|---|---|---|---|---|"]
        for row in findings[:60]:
            lines.append("| " + " | ".join(_cell(item) for item in row) + " |")
        if len(findings) > 60:
            lines += ["", f"Showing 60/{len(findings)} findings; the full evidence remains in the artifact."]
    else:
        lines.append("No blocking findings were reported by the supplied audits.")
    return "\n".join(lines) + "\n"


def load_reports(*, evidence_root: Path | None = None, final_report: Path | None = None):
    reports = []
    if evidence_root is not None:
        verification = _read(evidence_root / "verification.json")
        entries = verification.get("results")
        expected = {"full_refresh", "decision_surface", "provider_target"}
        if (not isinstance(entries, list) or len(entries) != 3 or
                any(not isinstance(item, dict) for item in entries) or
                {item.get("audit") for item in entries} != expected):
            raise ValueError("verification audit inventory is incomplete or ambiguous")
        for item in entries:
            name = item["audit"]
            observed_exit = item.get("exit_code")
            receipt_valid = (type(observed_exit) is int and observed_exit in (0, 1, 2, 3) and
                             item.get("report_present") is True and type(item.get("passed")) is bool and
                             item["passed"] == (observed_exit == 0))
            try:
                report = _read(evidence_root / (name + ".json"))
            except (OSError, ValueError):
                report = {"fatal": "Audit report missing or unreadable.", "exit_code": 3}
            reports.append((name, report, observed_exit if receipt_valid else None))
    else:
        report = _read(final_report)
        audits = report.get("audits")
        codes = report.get("audit_exit_codes")
        if (not isinstance(audits, dict) or not isinstance(codes, list) or
                len(codes) != 2 or any(type(code) is not int or code not in (0, 1, 2, 3) for code in codes) or
                set(audits) != {"coverage", "decision"}):
            raise ValueError("final audit inventory is incomplete or ambiguous")
        reports = [(name, audits[name], codes[index]) for index, name in enumerate(("coverage", "decision"))]
    return reports


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    source = parser.add_mutually_exclusive_group(required=True)
    source.add_argument("--evidence-root", type=Path)
    source.add_argument("--final-report", type=Path)
    args = parser.parse_args(argv)
    try:
        print(render(load_reports(evidence_root=args.evidence_root, final_report=args.final_report)), end="")
        return 0
    except (OSError, ValueError, TypeError) as exc:
        print("## Live audit triage\n\nUNKNOWN — summary evidence unavailable: " + _cell(exc))
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
