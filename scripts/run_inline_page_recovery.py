#!/usr/bin/env python3
"""Recover only market pages that failed to refresh in the current sync run.

This runner is intended to execute inside the same GitHub Actions workflow as the
primary dashboard sync. Keeping recovery in the same workflow holds the production
write lease until all required pages either refresh successfully or fail closed.

Each page retry launches a fresh ``run_dashboard_sync.py`` process, so every page
receives an independent ``TFB_SYNC_TIME_BUDGET_SEC`` budget. Existing persistence,
last-good-row, and page-verdict safeguards remain authoritative.
"""
from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
from pathlib import Path
from typing import Mapping, Sequence

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from scripts.audit_sync_outcome import audit_artifacts
from scripts.plan_sync_recovery import build_recovery_plan

SCRIPT_VERSION = "1.0.0"


def _stream_process(
    command: Sequence[str],
    *,
    env: Mapping[str, str],
    log_path: Path,
) -> int:
    """Run one page refresh while teeing combined output to console and a log."""
    log_path.parent.mkdir(parents=True, exist_ok=True)
    with log_path.open("w", encoding="utf-8") as handle:
        process = subprocess.Popen(
            list(command),
            cwd=REPO_ROOT,
            env=dict(env),
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1,
        )
        assert process.stdout is not None
        for line in process.stdout:
            print(line, end="")
            handle.write(line)
        return process.wait()


def run_inline_recovery(
    *,
    source_root: Path,
    backend: str,
    sheet_id: str,
    evidence_root: Path,
    plan_out: Path,
    summary_out: Path,
) -> int:
    """Plan and execute independent retries for failed or missing market pages."""
    plan = build_recovery_plan(source_root)
    plan_payload = dict(plan) | {"inline_runner_version": SCRIPT_VERSION}
    plan_out.parent.mkdir(parents=True, exist_ok=True)
    plan_out.write_text(
        json.dumps(plan_payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )

    summary: dict[str, object] = {
        "schema_version": "1.0",
        "script_version": SCRIPT_VERSION,
        "needs_recovery": bool(plan["needs_recovery"]),
        "retry_pages": list(plan["retry_pages"]),
        "results": [],
    }

    if not plan["needs_recovery"]:
        print("::notice::All required market pages refreshed; inline recovery not needed.")
        summary["status"] = "ok"
        summary_out.write_text(
            json.dumps(summary, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        return 0

    evidence_root.mkdir(parents=True, exist_ok=True)
    failed_pages: list[str] = []

    for item in plan["matrix"]["include"]:
        page = str(item["page"])
        key = str(item["key"])
        group = str(item["group"])
        page_root = evidence_root / group
        log_path = page_root / "sync_execution.log"

        print(f"::group::Recover {page}")
        env = os.environ.copy()
        env["TFB_SYNC_PAGE_ORDER"] = page

        command = (
            sys.executable,
            str(REPO_ROOT / "scripts" / "run_dashboard_sync.py"),
            "--backend",
            backend,
            "--sheet-id",
            sheet_id,
            "--keys",
            key,
            "--start-cell",
            "A1",
        )
        runner_exit = _stream_process(command, env=env, log_path=log_path)

        audit_status = "blocked"
        audit_payload: dict[str, object]
        try:
            audit = audit_artifacts(page_root, required_pages=(page,))
            audit_status = audit.status
            audit_payload = audit.to_dict()
        except OSError as exc:
            audit_payload = {"status": "read_error", "error": str(exc)}

        (page_root / "page-audit.json").write_text(
            json.dumps(audit_payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )

        passed = runner_exit == 0 and audit_status == "ok"
        if not passed:
            failed_pages.append(page)
            print(
                f"::error::Inline recovery failed for {page}: "
                f"runner_exit={runner_exit}, audit_status={audit_status}"
            )
        else:
            print(f"::notice::Inline recovery passed for {page}")

        summary["results"].append(
            {
                "page": page,
                "key": key,
                "group": group,
                "runner_exit": runner_exit,
                "audit_status": audit_status,
                "passed": passed,
                "evidence_root": str(page_root),
            }
        )
        print("::endgroup::")

    summary["status"] = "ok" if not failed_pages else "blocked"
    summary["failed_pages"] = failed_pages
    summary_out.parent.mkdir(parents=True, exist_ok=True)
    summary_out.write_text(
        json.dumps(summary, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )

    if failed_pages:
        print(f"::error::Inline page recovery blocked: {', '.join(failed_pages)}")
        return 2

    print("::notice::All targeted market pages recovered successfully.")
    return 0


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source-root", required=True)
    parser.add_argument("--backend", required=True)
    parser.add_argument("--sheet-id", required=True)
    parser.add_argument("--evidence-root", default="inline-recovery-evidence")
    parser.add_argument("--plan-out", default="inline-recovery-plan.json")
    parser.add_argument("--summary-out", default="inline-recovery-summary.json")
    args = parser.parse_args(argv)

    try:
        return run_inline_recovery(
            source_root=Path(args.source_root),
            backend=args.backend.rstrip("/"),
            sheet_id=args.sheet_id,
            evidence_root=Path(args.evidence_root),
            plan_out=Path(args.plan_out),
            summary_out=Path(args.summary_out),
        )
    except OSError as exc:
        print(f"::error::INLINE_RECOVERY_IO_ERROR: {exc}")
        return 3


if __name__ == "__main__":
    raise SystemExit(main())
