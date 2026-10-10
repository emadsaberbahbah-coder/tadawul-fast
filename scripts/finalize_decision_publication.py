"""Check final workbook publication and downgrade the decision feed on failure.

Run after all market matrix legs and inline recovery, under their write lease.
This command never refreshes a cockpit, restores a universe, or promotes the
feed to EXECUTABLE. Only a bounded existing _Status L:M key may be changed.
Native installation and a subsequent native refresh/readback remain required.
"""
from __future__ import annotations

import argparse
import asyncio
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

SCRIPT_VERSION = "1.0.0"
KEY = "TFB Decision Feed"
KEY_RANGE = "'_Status'!L1:M60"
SELF_CHECK_KEYS = {"backend url", "last global update", "token loaded"}
REQUIRED_PAGES = {
    "Market_Leaders", "Global_Markets", "Commodities_FX", "Mutual_Funds",
    "My_Portfolio", "Insights_Analysis", "Data_Dictionary",
}
MARKET_FLOORS = {"Market_Leaders": 1025, "Global_Markets": 6512,
                 "Commodities_FX": 453, "Mutual_Funds": 4496}


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


def final_policy() -> tuple[list[coverage.Rule], dict[str, Any]]:
    """Pass an explicit policy to both auditors without mutating process env."""
    floors, minimum_fresh, rules = {}, {}, []
    market_age = _bounded_env("TFB_DECISION_SOURCE_MAX_AGE_H", 30, maximum=30)
    decision_age = _bounded_env("TFB_DECISION_SURFACE_MAX_AGE_H", 8, maximum=8)
    for page, baseline in MARKET_FLOORS.items():
        suffix = page.upper()
        floors[page] = math.ceil(_bounded_env("TFB_EXPECTED_MIN_ROWS_" + suffix,
                                             baseline, minimum=baseline))
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


async def run(spreadsheet_id: str, *, reader=None, registry=None,
              service=None, publish=False, max_rows=20000, run_id="local") -> dict:
    """Reuse the actual audits; warning-only results never upgrade the feed."""
    if not spreadsheet_id:
        raise ValueError("spreadsheet ID is missing")
    if type(max_rows) is not int or max_rows != 20000:
        raise ValueError("final publication requires the full 20,000-row audit bound")
    if not re.fullmatch(r"[A-Za-z0-9._-]+", str(run_id)):
        raise ValueError("run identity is invalid")
    rules, policy = final_policy()
    if publish or str(run_id) != "local":
        policy["expected_source_run"] = str(run_id)
    reports = await asyncio.gather(
        coverage.run(spreadsheet_id, max_rows, reader=reader, registry=registry, policy_rules=rules),
        freshness.run_live(spreadsheet_id, reader=reader, **policy),
        return_exceptions=True,
    )
    full, decision = reports
    reasons, audit_payloads, codes = [], {}, []
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
            payload = json.loads(redact_text(json.dumps(payload, default=str, allow_nan=False)))
        except Exception as exc:
            code, payload = 3, {"fatal": safe_error_text(exc), "exit_code": 3}
        codes.append(code)
        audit_payloads[name] = payload
        if code >= 2:
            reasons.append(name + "_failed")
    result = {"script_version": SCRIPT_VERSION, "run_id": run_id,
              "policy": policy,
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
    parser.add_argument("--json-out", default="final_decision_publication.json")
    args = parser.parse_args(argv)
    try:
        if args.max_rows != 20000:
            raise ValueError("final publication requires the full 20,000-row audit bound")
        result = asyncio.run(run(args.sheet_id, publish=args.publish_blockers,
                                 max_rows=args.max_rows,
                                 run_id=os.getenv("GITHUB_RUN_ID", "local")))
    except Exception as exc:
        result = {"script_version": SCRIPT_VERSION, "exit_code": 3,
                  "fatal": safe_error_text(exc), "feed_promoted": False}
    text = json.dumps(result, ensure_ascii=False, default=str, indent=2)
    Path(args.json_out).write_text(text + "\n", encoding="utf-8")
    print(json.dumps({key: result.get(key) for key in (
        "script_version", "exit_code", "audit_exit_codes", "blockers",
        "blocker_published", "feed_promoted", "fatal", "publication_error")}, ensure_ascii=False))
    return result["exit_code"]


if __name__ == "__main__":
    raise SystemExit(main())
