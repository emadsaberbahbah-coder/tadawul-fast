"""Check final workbook publication and downgrade the decision feed on failure.

Run after all market matrix legs and inline recovery, under their write lease.
This command never refreshes a cockpit, restores a universe, or promotes the
feed to EXECUTABLE. Only a bounded existing _Status L:M key may be changed.
Native installation and a subsequent native refresh/readback remain required.

v1.2.0 (2026-10-26) -- CONSUMER-PENDING PROCESS EXIT. 26_Decision_Chain
renders Top_10 for run N only after this job has written 'TFB Decision Feed'
run=N, so this audit always meets decision pages that have not consumed the
cohort yet. Every scheduled sync therefore ended red (5/6 scheduled runs,
25-26 Oct; 20/21 in the 17-24 Oct census), which hid genuine failures.
classify_consumer_pending() recognises exactly that state: coverage <= 1,
structure witness < 2, the blocker published, and every decision finding a
same-cohort consumer-lag code. Under TFB_FINAL_CONSUMER_PENDING=warn the
PROCESS exits with the coverage code instead of 2. run() is unchanged: the
evidence keeps exit_code 2, the blockers and the NOT_ACTIONABLE write, and
release_decision_feed.py still re-runs the real audits. Default (unset/off)
keeps the v1.1.2 exit codes and writes; the evidence only gains the
consumer_pending and process_exit_code keys. Rollback: unset the variable.
"""
from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import math
import os
import re
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from core.secret_redaction import redact_text, safe_error_text
from scripts import audit_decision_surface_freshness as freshness
from scripts import audit_full_refresh_coverage as coverage
from scripts.approved_universe_scope import (
    approved_floors, load_scope, receipt_matches_policy,
)
from scripts.workflow_validation_scope import SOURCE_PAGES, strict_json

SCRIPT_VERSION = "1.2.0"
KEY = "TFB Decision Feed"
KEY_RANGE = "'_Status'!L1:M60"
SELF_CHECK_KEYS = {"backend url", "last global update", "token loaded"}
REQUIRED_PAGES = {
    "Market_Leaders", "Global_Markets", "Commodities_FX", "Mutual_Funds",
    "My_Portfolio", "Insights_Analysis", "Data_Dictionary",
}
MARKET_FLOORS = {"Market_Leaders": 255, "Global_Markets": 6512,
                 "Commodities_FX": 453, "Mutual_Funds": 2474}
# The per-leg validator has always allowed these advisory WARNs (exit 1).
# Moving validation stages must not turn them into a new release policy.
SOFT_VALIDATION_WARNINGS = frozenset({
    "sanity.no_duplicate_symbols", "sanity.price_in_day_range", "sanity.open_present",
    "sanity.open_in_day_range", "sanity.price_in_52w_range", "sanity.roi12_within_softcap",
    "decision.price_present",
})


# Same-cohort consumer lag only: the decision pages have not rendered the run
# this job audits yet. Stale (>8 h), missing, NOT_OK, pool-count and source
# findings are deliberately absent and keep the job red.
CONSUMER_PENDING_CODES = frozenset({
    "PF_OLDER_THAN_SOURCE", "PF_SOURCE_CONSUMPTION_UNPROVEN",
    "T10_OLDER_THAN_SOURCE", "T10_SOURCE_CONSUMPTION_UNPROVEN",
    "T10_POOL_RECONCILIATION_UNPROVEN", "FALSE_FULL_UNIVERSE_CLAIM",
})
# A pool reconciliation finding is lag only when its receipt names another run.
POOL_LAG_MESSAGES = (
    "native pool reconciliation does not match one consumed/current source cohort",
    "native pool reconciliation does not match expected source run",
)


def classify_consumer_pending(result: Any) -> bool:
    """PURE. True only when the sole blocker is decision pages awaiting this cohort."""
    try:
        codes = result["audit_exit_codes"]
        if (result.get("fatal") or result.get("publication_error") or result.get("feed_promoted")
                or result.get("exit_code") != 2 or result.get("blockers") != ["decision_failed"]
                or result.get("blocker_published") is not True
                or not isinstance(codes, list) or len(codes) != 2
                or any(type(code) is not int for code in codes)
                or codes[0] not in (0, 1) or codes[1] != 2):
            return False
        decision = result["audits"]["decision"]
        structure = decision.get("structural_validation")
        if (not isinstance(structure, dict) or structure.get("same_job_witness") is not True
                or structure.get("exit_code") not in (0, 1)
                or decision.get("freshness_exit_code") != 2):
            return False
        findings = decision.get("findings")
        if not isinstance(findings, list) or not findings:
            return False
        lag = False
        for finding in findings:
            code = finding.get("code") if isinstance(finding, dict) else None
            if finding.get("severity") != "FAIL" or code not in CONSUMER_PENDING_CODES:
                return False
            if code == "T10_POOL_RECONCILIATION_UNPROVEN" and not any(
                    message in str(finding.get("message", "")) for message in POOL_LAG_MESSAGES):
                return False
            lag |= code != "FALSE_FULL_UNIVERSE_CLAIM"
        return lag
    except (KeyError, TypeError, AttributeError):
        return False


def process_exit_code(result: Any) -> int:
    """The job's exit. Evidence exit_code is never changed here."""
    policy = os.getenv("TFB_FINAL_CONSUMER_PENDING", "").strip().lower()
    if policy == "warn" and result.get("consumer_pending") is True:
        return result["audit_exit_codes"][0]
    return result["exit_code"]


def _bounded_env(name: str, default: float, *, minimum=None, maximum=None) -> float:
    """Operator settings can tighten the final gate, never weaken it."""
    try:
        value = float(os.getenv(name, "") or default)
        if not math.isfinite(value) or value <= 0:
            raise ValueError("invalid final audit policy")
    except (ValueError, TypeError, OverflowError):
        value = default
    if minimum is not None:
        value = max(minimum, value)
    if maximum is not None:
        value = min(maximum, value)
    return value


def final_policy(*, approved_scope=None) -> tuple[list[coverage.Rule], dict[str, Any]]:
    """Pass an explicit policy to both auditors without mutating process env."""
    scope = approved_scope if approved_scope is not None else load_scope()
    current = approved_floors(policy=scope)
    floors, minimum_fresh, rules = {}, {}, []
    market_age = _bounded_env("TFB_DECISION_SOURCE_MAX_AGE_H", 30, maximum=30)
    decision_age = _bounded_env("TFB_DECISION_SURFACE_MAX_AGE_H", 8, maximum=8)
    for page, baseline in MARKET_FLOORS.items():
        suffix = page.upper()
        floors[page] = (current[page] if page in current else
                        math.ceil(_bounded_env("TFB_EXPECTED_MIN_ROWS_" + suffix,
                                               baseline, minimum=baseline)))
        minimum_fresh[page] = _bounded_env("TFB_REFRESH_MIN_FRESH_PCT_" + suffix,
                                          95, minimum=95, maximum=100)
        age = _bounded_env("TFB_REFRESH_MAX_AGE_H_" + suffix, market_age, maximum=market_age)
        rules.append(coverage.Rule(page, floors[page], age, minimum_fresh[page], 99, 95))
    portfolio_age = _bounded_env("TFB_REFRESH_MAX_AGE_H_MY_PORTFOLIO", decision_age,
                                 maximum=decision_age)
    rules.extend([
        coverage.Rule("My_Portfolio", 1, portfolio_age, 100, 100, 100, True, True),
        coverage.Rule("Insights_Analysis", 1, None, 0, 0, 0, False),
        coverage.Rule("Data_Dictionary", 1, None, 0, 0, 0, False),
    ])
    return rules, {"min_rows": floors, "market_max_age_h": market_age,
                   "decision_max_age_h": decision_age, "min_fresh_percent": minimum_fresh}


def complete_membership_proven(receipt: dict, page: str, scope) -> bool:
    """A matching bounded snapshot cannot attest the unexamined sparse tail."""
    proof = receipt.get("complete_column_proof")
    if not isinstance(proof, dict) or proof.get("valid") is not True or proof.get("crossbound") is not True:
        return False
    if proof.get("range") != page + "!A:A":
        return False
    if type(proof.get("bound")) is not int or proof["bound"] != 20050:
        return False
    if type(proof.get("symbol_column")) is not int or proof["symbol_column"] != 1:
        return False
    header, extent = proof.get("header_row"), proof.get("returned_rows")
    if type(header) is not int or not 1 <= header <= 45 or type(extent) is not int:
        return False
    if not header + scope.pages[page].approved_count <= extent <= 20050:
        return False
    return all(proof.get(key) == receipt.get(outer) for key, outer in (
        ("observed_symbol_set_sha256", "observed_symbol_set_sha256"),
        ("bounded_symbol_set_sha256", "observed_symbol_set_sha256"),
        ("actual_membership_sha256", "actual_membership_sha256"),
        ("bounded_membership_sha256", "actual_membership_sha256"),
    ))


def publication_slot(grid: Any) -> int:
    """Refuse ambiguous/moved layouts and never insert a worksheet row."""
    if not isinstance(grid, list) or len(grid) > 60:
        raise ValueError("decision feed layout unavailable")
    matches, blanks, known = [], [], False
    seen = set()
    for index in range(60):
        row = grid[index] if index < len(grid) else []
        if not isinstance(row, list) or len(row) > 2:
            raise ValueError("decision feed layout invalid")
        key = ("" if not row or row[0] is None else str(row[0]).strip()).casefold()
        if not key:
            if len(row) > 1 and row[1] is not None and str(row[1]).strip():
                raise ValueError("decision feed layout has an orphaned value")
            blanks.append(index + 1)
            continue
        if key in seen:
            raise ValueError("duplicate decision feed key")
        seen.add(key)
        known |= key in SELF_CHECK_KEYS
        if key == KEY.casefold():
            matches.append(index + 1)
    if not known:
        raise ValueError("decision feed layout self-check failed")
    if matches:
        return matches[0]
    if not blanks:
        raise ValueError("decision feed layout has no free slot")
    return blanks[0]


def publish_blocker(service: Any, spreadsheet_id: str, value: str) -> None:
    """Downgrade in one bounded RAW request and require exact readback."""
    if not isinstance(value, str) or not value.startswith("NOT_ACTIONABLE(final_publication) | "):
        raise ValueError("final publication guard may only downgrade")
    values = service.spreadsheets().values()
    grid = values.get(spreadsheetId=spreadsheet_id, range=KEY_RANGE,
                      valueRenderOption="UNFORMATTED_VALUE").execute().get("values")
    slot = publication_slot(grid)
    target = f"'_Status'!L{slot}:M{slot}"
    expected = [[KEY, value]]
    values.update(spreadsheetId=spreadsheet_id, range=target,
                  valueInputOption="RAW", body={"values": expected}).execute()
    actual = values.get(spreadsheetId=spreadsheet_id, range=target,
                        valueRenderOption="UNFORMATTED_VALUE").execute().get("values")
    if actual != expected:
        raise RuntimeError("decision blocker publication unconfirmed")


def decision_structure(path: str | Path, spreadsheet_id: str, run_id: str) -> dict:
    """Verify a same-job, bounded literal-source/native validation witness.

    This supplements the freshness receipt checks: old OK status text cannot
    certify a missing/half-rendered cockpit grid. An unavailable witness stays
    a blocking decision result and still reaches the normal downgrade writer.
    """
    try:
        source = Path(path)
        if source.stat().st_size > 262144:
            raise ValueError("structural report exceeds witness size")
        report = strict_json(source.read_text(encoding="utf-8"))
        meta, witness, checks = report["meta"], report["workflow_witness"], report["results"]
        expected_sha = os.getenv("GITHUB_SHA", "")
        if (not re.fullmatch(r"[0-9a-f]{40}", expected_sha)
                or witness.get("source_sha") != expected_sha or witness.get("run_id") != str(run_id)
                or witness.get("sheet_sha256") != hashlib.sha256(spreadsheet_id.encode()).hexdigest()):
            raise ValueError("structural witness workflow/source/sheet identity differs")
        stamp = datetime.strptime(meta["generated_riyadh"], "%Y-%m-%d %H:%M:%S").replace(
            tzinfo=timezone(timedelta(hours=3)))
        age = (datetime.now(timezone.utc) - stamp).total_seconds()
        if not -5 <= age <= 600 or meta.get("reader") != "read_range" or meta.get("fatal"):
            raise ValueError("structural witness clock/reader is unverified")
        if not isinstance(checks, list) or not checks or len(checks) > 1000:
            raise ValueError("structural results are unavailable or unbounded")
        expected_pages = set(SOURCE_PAGES.values()) - {"Insights_Analysis", "Data_Dictionary"}
        expected_pages.add("Top_10_Investments")
        names, details, findings, statuses = {}, {}, [], []
        for check in checks:
            page, name, status = check["page"], check["name"], check["status"]
            if page not in expected_pages or status not in {"PASS", "FAIL", "WARN", "SKIP"}:
                raise ValueError("structural result vocabulary differs")
            key = (page, name)
            if key in names:
                raise ValueError("duplicate structural check")
            names[key] = status
            details[key] = str(check.get("detail", ""))
            statuses.append(status)
            if status in {"FAIL", "WARN"}:
                findings.append({"severity": status, "code": "STRUCTURE_" + str(name),
                                 "surface": page, "message": str(check.get("detail", ""))})
        code = 2 if "FAIL" in statuses else 1 if "WARN" in statuses else 0
        if type(witness.get("process_exit_code")) is not int or witness["process_exit_code"] != code:
            raise ValueError("structural process/report exit differs")
        if code < 2:
            for page in expected_pages - {"Top_10_Investments"}:
                for name in ("contract.header_match", "scope.coverage"):
                    if names.get((page, name)) != "PASS":
                        raise ValueError("full source contract/scope proof is missing")
            for name in ("decision.header_found", "decision.feed_banner_present"):
                if names.get(("Top_10_Investments", name)) != "PASS":
                    raise ValueError("native cockpit structure proof is missing")
            banner = details[("Top_10_Investments", "decision.feed_banner_present")].upper()
            if "FEED" not in banner or "ACTIONABLE" not in banner:
                raise ValueError("native feed banner witness is unavailable")
            sizing_key = ("Top_10_Investments", "decision.sizing_withheld_when_blocked")
            if "NOT ACTIONABLE" in banner:
                if names.get(sizing_key) != "PASS":
                    raise ValueError("blocked native withholding proof is missing")
            elif (names.get(sizing_key) != "SKIP" or details.get(sizing_key) != "feed not blocked"
                  or names.get(("Top_10_Investments", "decision.price_present")) not in {"PASS", "WARN"}):
                raise ValueError("actionable native sizing/price check is unverified")
            if any(status == "WARN" and name not in SOFT_VALIDATION_WARNINGS
                   for (_page, name), status in names.items()):
                raise ValueError("critical or unrecognized warning cannot supply a structural certificate")
        return {"exit_code": 0 if code == 1 else code, "validator_exit_code": code,
                "soft_warnings_nonblocking": code == 1,
                "same_job_witness": True, "findings": findings,
                "generated_riyadh": meta["generated_riyadh"]}
    except Exception as exc:
        return {"exit_code": 3, "same_job_witness": False, "findings": [{
            "severity": "FAIL", "code": "STRUCTURE_WITNESS_UNAVAILABLE",
            "surface": "Top_10_Investments", "message": safe_error_text(exc)}]}


async def run(spreadsheet_id: str, *, reader=None, registry=None,
              service=None, publish=False, max_rows=20000, run_id="local",
              decision_structure_report=None) -> dict:
    """Reuse the actual audits; warning-only results never upgrade the feed."""
    if not spreadsheet_id:
        raise ValueError("spreadsheet ID is missing")
    if type(max_rows) is not int or max_rows != 20000:
        raise ValueError("final publication requires the full 20,000-row audit bound")
    if not re.fullmatch(r"[A-Za-z0-9._-]+", str(run_id)):
        raise ValueError("run identity is invalid")
    scope, policy_fault = None, None
    try:
        scope = load_scope()
        rules, policy = final_policy(approved_scope=scope)
        if publish or str(run_id) != "local":
            policy["expected_source_run"] = str(run_id)
        reports = await asyncio.gather(
            coverage.run(spreadsheet_id, max_rows, reader=reader, registry=registry,
                         policy_rules=rules, approved_scope=scope),
            freshness.run_live(spreadsheet_id, reader=reader, approved_scope=scope, **policy),
            return_exceptions=True,
        )
    except Exception as exc:
        policy_fault = exc
        policy = {"approved_scope_error": safe_error_text(exc)}
        reports = (exc, exc)
    full, decision = reports
    reasons, audit_payloads, codes = (["approved_scope_unavailable"] if policy_fault else []), {}, []
    for name, report, attr in (("coverage", full, "code"),
                               ("decision", decision, "exit_code")):
        if isinstance(report, BaseException):
            codes.append(3)
            reasons.append(name + "_unavailable")
            audit_payloads[name] = {"fatal": safe_error_text(report), "exit_code": 3}
            continue
        try:
            code = getattr(report, attr)
            if type(code) is not int or code not in (0, 1, 2, 3):
                raise ValueError("invalid audit exit code")
            payload = report.payload()
            if not isinstance(payload, dict) or not isinstance(payload.get("summary"), dict):
                raise ValueError("audit payload is invalid")
            if type(payload["summary"].get("exit_code")) is not int or payload["summary"]["exit_code"] != code:
                raise ValueError("audit payload exit code is inconsistent")
            if name == "coverage" and (len(report.pages) != len(REQUIRED_PAGES)
                                       or {page.page for page in report.pages} != REQUIRED_PAGES):
                raise ValueError("coverage audit omitted or duplicated a required page")
            if name == "decision" and code <= 1:
                if type(report.executable) is not bool or (code == 0 and report.executable is not True):
                    raise ValueError("decision audit has no accepted verdict")
            if code <= 1:
                for page in scope.pages:
                    receipt = (next(item.approved_scope for item in report.pages if item.page == page)
                               if name == "coverage" else
                               report.source_status.get(page, {}).get("approved_scope"))
                    if (not isinstance(receipt, dict) or receipt.get("page") != page
                            or not receipt_matches_policy(receipt, policy=scope)
                            or not complete_membership_proven(receipt, page, scope)):
                        raise ValueError(name + " audit has no verified approved membership for " + page)
            payload = json.loads(redact_text(json.dumps(payload, default=str, allow_nan=False)))
        except Exception as exc:
            code, payload = 3, {"fatal": safe_error_text(exc), "exit_code": 3}
        codes.append(code)
        audit_payloads[name] = payload
        if code >= 2:
            reasons.append(name + "_failed")
    if decision_structure_report is not None:
        structure = decision_structure(decision_structure_report, spreadsheet_id, str(run_id))
        decision_payload = audit_payloads["decision"]
        decision_payload["freshness_exit_code"] = codes[1]
        decision_payload["structural_validation"] = structure
        codes[1] = max(codes[1], structure["exit_code"])
        decision_payload.setdefault("summary", {})["exit_code"] = codes[1]
        if not isinstance(decision_payload.get("findings", []), list):
            codes[1] = 3
            decision_payload["summary"]["exit_code"] = 3
            decision_payload["findings"] = [{"severity": "FAIL", "code": "STRUCTURE_DECISION_PAYLOAD_INVALID",
                                               "surface": "Top_10_Investments", "message": "decision findings are unavailable"}]
        # Release consumers require decision.findings == []; observational
        # sanity WARNs must remain nested evidence, as they were before moving
        # this check. Only blocking structural findings join the decision gate.
        if structure["exit_code"] >= 2:
            decision_payload.setdefault("findings", []).extend(structure["findings"])
        if codes[1] > 0:
            decision_payload["executable"] = False
        if codes[1] >= 2 and "decision_failed" not in reasons:
            reasons.append("decision_failed")
        audit_payloads["decision"] = json.loads(redact_text(json.dumps(decision_payload, default=str)))
    result = {"script_version": SCRIPT_VERSION, "run_id": run_id,
              "policy": {**policy, **({"approved_scope": scope.receipt()} if scope else {})},
              "audits": audit_payloads, "audit_exit_codes": codes,
              "blockers": reasons, "feed_promoted": False,
              "blocker_published": False, "exit_code": max(codes)}
    if reasons and publish:
        try:
            if service is None:
                from integrations.google_sheets_service import get_sheets_service
                service = get_sheets_service()
            stamp = datetime.now(timezone(timedelta(hours=3))).strftime("%Y-%m-%d %H:%M:%S%z")
            stamp = stamp[:-2] + ":" + stamp[-2:]
            value = ("NOT_ACTIONABLE(final_publication) | run=" + run_id +
                     " | " + stamp + " | " + ",".join(reasons))
            publish_blocker(service, spreadsheet_id, value)
            result["blocker_published"] = True
        except Exception as exc:
            result["publication_error"] = safe_error_text(exc)
            result["exit_code"] = 3
    return result


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--sheet-id", default=os.getenv("DEFAULT_SPREADSHEET_ID", ""))
    parser.add_argument("--max-rows", type=int, default=20000)
    parser.add_argument("--publish-blockers", action="store_true")
    parser.add_argument("--decision-structure-report", help="Required same-job native/source structure witness when supplied")
    parser.add_argument("--json-out", default="final_decision_publication.json")
    args = parser.parse_args(argv)
    try:
        if args.max_rows != 20000:
            raise ValueError("final publication requires the full 20,000-row audit bound")
        result = asyncio.run(run(args.sheet_id, publish=args.publish_blockers,
                                 max_rows=args.max_rows,
                                 run_id=os.getenv("GITHUB_RUN_ID", "local"),
                                 decision_structure_report=args.decision_structure_report))
    except Exception as exc:
        result = {"script_version": SCRIPT_VERSION, "exit_code": 3,
                  "fatal": safe_error_text(exc), "feed_promoted": False}
    result["consumer_pending"] = classify_consumer_pending(result)
    result["process_exit_code"] = process_exit_code(result)
    text = json.dumps(result, ensure_ascii=False, default=str, indent=2)
    Path(args.json_out).write_text(text + "\n", encoding="utf-8")
    print(json.dumps({key: result.get(key) for key in (
        "script_version", "exit_code", "audit_exit_codes", "blockers",
        "blocker_published", "feed_promoted", "consumer_pending", "process_exit_code",
        "fatal", "publication_error")}, ensure_ascii=False))
    return result["process_exit_code"]


if __name__ == "__main__":
    raise SystemExit(main())
