#!/usr/bin/env python3
"""Recover only market pages that failed to refresh in the current sync run.

This runner is intended to execute inside the same GitHub Actions workflow as the
primary dashboard sync. Keeping recovery in the same workflow holds the production
write lease until all required pages either refresh successfully or fail closed.

Each page retry launches a fresh ``run_dashboard_sync.py`` process, so every page
receives an independent ``TFB_SYNC_TIME_BUDGET_SEC`` budget. Existing persistence,
last-good-row, and page-verdict safeguards remain authoritative.

v1.2.0 (P-154b, 2026-09-22) QUOTA-AWARE RECOVERY. Every page replay is a FULL
page re-fetch (``--keys <page>``): measured on 2026-09-21/22, one Global_Markets
replay costs ~53-60k EODHD calls (13-15% of the 400k/day allowance) and the
replay fired in 5 of the last 6 Global_Markets windows. On a quota-exhausted
day the loop is a death spiral: exhaustion -> failed batches / low fresh %% ->
full replay (up to TFB_INLINE_RECOVERY_MAX_CYCLES) -> more exhaustion, and the
replay CANNOT produce fresh rows while the counter is exhausted. This version
reads the sync's own ``[EODHD-QUOTA v...]`` line (run_dashboard_sync v6.60.0,
observe arming) for the page from the same artifact logs the audit reads and,
behind ``TFB_INLINE_RECOVERY_QUOTA_GUARD`` (off | observe | enforce, explicit
words, default off = v1.1.0 byte-identical), refuses a replay whose quota state
is EXHAUSTED / ON_EXTRA or whose used%% is at/above
``TFB_INLINE_RECOVERY_QUOTA_SKIP_PCT`` (default 90, the sentinel's CRIT line).
observe only annotates (``would SKIP`` / ``would allow``); enforce skips the
replay, records it as ``skipped:quota`` (last-good rows stay on the sheet, the
next scheduled window retries) and does NOT count it as a failed page. Pages
with no parsable quota line are replayed exactly as v1.1.0. Between cycles the
guard re-reads the replay's own log, so a replay that pushed the counter over
the line stops the next cycle.
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

SCRIPT_VERSION = "1.2.0"


def _max_cycles() -> int:
    """v1.1.0 FULL-FILL loop cap. Default "1" => v1.0.0 single-pass behavior
    byte-identical. TFB_INLINE_RECOVERY_MAX_CYCLES=N (1..6) lets recovery
    re-plan from its own evidence and retry still-incomplete pages, each
    attempt with an independent TFB_SYNC_TIME_BUDGET_SEC budget, until the
    audit criterion passes or cycles are exhausted."""
    try:
        return max(1, min(6, int((os.getenv("TFB_INLINE_RECOVERY_MAX_CYCLES") or "1").strip())))
    except ValueError:
        return 1


# --- v1.2.0 (P-154b) QUOTA-AWARE RECOVERY ---------------------------------------
import re as _re

_QUOTA_GUARD_ENV = "TFB_INLINE_RECOVERY_QUOTA_GUARD"
_QUOTA_SKIP_PCT_ENV = "TFB_INLINE_RECOVERY_QUOTA_SKIP_PCT"
_QUOTA_SKIP_PCT_DEFAULT = 90.0
_QUOTA_SKIP_STATES = ("EXHAUSTED", "ON_EXTRA")
# run_dashboard_sync v6.60.0 line, e.g.
#   [EODHD-QUOTA v6.60.0] Global_Markets | used=244362/400000 (61.1%) date=2026-09-21 extra=0 | delta=... | rows402 new=0 carried=1 | f429=0 f404=43 fetch_failed=44 | state=OK | selftest=PASS 5/5
#   [EODHD-QUOTA v6.60.0] Global_Markets | used=unknown (no_key) | ... | state=UNKNOWN | ...
_QUOTA_LINE_RE = _re.compile(
    r"\[EODHD-QUOTA v[\d.]+\]\s+(?P<page>\S+)\s+\|\s+used=(?P<used>[^|]+?)\s*\|"
    r".*?rows402 new=(?P<new>\d+) carried=(?P<carried>\d+)"
    r".*?state=(?P<state>[A-Z_]+)"
)
_QUOTA_USED_RE = _re.compile(r"(?P<used>\d+)/(?P<limit>\d+)\s+\((?P<pct>[\d.]+)%\)")


def _quota_guard_mode() -> str:
    """off (default) | observe | enforce -- explicit words only; "1"/"true"/"on"
    read as off so an accidental boolean can never skip a replay."""
    raw = (os.getenv(_QUOTA_GUARD_ENV) or "").strip().lower()
    return raw if raw in ("observe", "enforce") else "off"


def _quota_skip_pct() -> float:
    try:
        v = float((os.getenv(_QUOTA_SKIP_PCT_ENV) or "").strip() or _QUOTA_SKIP_PCT_DEFAULT)
    except ValueError:
        v = _QUOTA_SKIP_PCT_DEFAULT
    return max(50.0, min(100.0, v))


def _quota_logs(root: Path) -> list[Path]:
    """Same log discovery as scripts.audit_sync_outcome._candidate_logs."""
    if not root.exists():
        return []
    canonical = sorted(p for p in root.rglob("sync_execution.log") if p.is_file())
    if canonical:
        return canonical
    return sorted(p for p in root.rglob("sync_*.log") if p.is_file())


def _latest_quota_for_page(root: Path, page: str) -> dict | None:
    """The LAST [EODHD-QUOTA] line for ``page`` below ``root`` (by file order,
    then line order), parsed. None when no line exists or nothing parses."""
    found: dict | None = None
    for path in _quota_logs(root):
        try:
            text = path.read_text(encoding="utf-8", errors="replace")
        except OSError:
            continue
        for line in text.splitlines():
            m = _QUOTA_LINE_RE.search(line)
            if not m or m.group("page") != page:
                continue
            q: dict = {"page": page, "state": m.group("state"), "rows402_new": int(m.group("new")),
                       "rows402_carried": int(m.group("carried")), "used": None, "limit": None,
                       "pct": None, "source": str(path)}
            u = _QUOTA_USED_RE.search(m.group("used"))
            if u:
                q["used"] = int(u.group("used")); q["limit"] = int(u.group("limit")); q["pct"] = float(u.group("pct"))
            found = q
    return found


def _quota_decision(q: dict | None, skip_pct: float) -> tuple[str, str]:
    """('allow'|'skip', reason). No evidence -> allow (v1.1.0 behaviour)."""
    if not q:
        return "allow", "no_quota_line"
    state = str(q.get("state") or "")
    if state in _QUOTA_SKIP_STATES or int(q.get("rows402_new") or 0) > 0:
        return "skip", "state=%s rows402_new=%s" % (state, q.get("rows402_new"))
    pct = q.get("pct")
    if pct is not None and float(pct) >= skip_pct:
        return "skip", "used=%.1f%% >= %.0f%% (state=%s)" % (float(pct), skip_pct, state)
    return "allow", "used=%s%% state=%s" % ("?" if pct is None else ("%.1f" % float(pct)), state or "?")


def _quota_guard(mode: str, page: str, cycle: int, roots: Sequence[Path], skip_pct: float,
                 summary: dict) -> bool:
    """Evaluate the guard for one replay. Returns True when the replay must be
    SKIPPED (enforce only). Records one entry per evaluation in
    summary["quota_guard"]. Never raises."""
    try:
        q: dict | None = None
        for root in roots:            # most recent evidence first
            q = _latest_quota_for_page(root, page)
            if q:
                break
        decision, reason = _quota_decision(q, skip_pct)
        entry = {"page": page, "cycle": cycle, "mode": mode, "decision": decision, "reason": reason,
                 "quota": q}
        summary.setdefault("quota_guard", []).append(entry)
        tag = "[RECOVERY-QUOTA v%s %s]" % (SCRIPT_VERSION, mode)
        if decision == "skip":
            if mode == "enforce":
                print("::warning::%s %s (cycle %d): replay SKIPPED - %s; last-good rows stay, next window retries"
                      % (tag, page, cycle, reason))
                return True
            print("::warning::%s %s (cycle %d): would SKIP replay - %s" % (tag, page, cycle, reason))
            return False
        print("::notice::%s %s (cycle %d): %s replay - %s"
              % (tag, page, cycle, "allow" if mode == "enforce" else "would allow", reason))
        return False
    except Exception as exc:  # fail-open: never block a replay on guard trouble
        print("::warning::[RECOVERY-QUOTA v%s] guard error for %s (%s) - replay allowed"
              % (SCRIPT_VERSION, page, type(exc).__name__))
        return False


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
    skipped_pages: list[str] = []          # v1.2.0 (P-154b)
    quota_mode = _quota_guard_mode()       # v1.2.0 (P-154b)
    quota_skip_pct = _quota_skip_pct()     # v1.2.0 (P-154b)
    max_cycles = _max_cycles()
    summary["max_cycles"] = max_cycles
    if quota_mode != "off":                # v1.2.0: off keeps the v1.1.0 summary byte-identical
        summary["quota_guard_mode"] = quota_mode
        summary["quota_skip_pct"] = quota_skip_pct
    pending: list[dict] = [dict(item) for item in plan["matrix"]["include"]]
    cycle = 0
    while pending and cycle < max_cycles:
        cycle += 1
        if cycle > 1:
            print(
                f"::notice::FULL-FILL cycle {cycle}/{max_cycles} — retrying "
                f"{', '.join(str(i['page']) for i in pending)}"
            )
        current, pending = pending, []
        for item in current:
            page = str(item["page"])
            key = str(item["key"])
            group = str(item["group"])
            page_root = evidence_root / group if cycle == 1 else evidence_root / group / f"cycle{cycle}"
            log_path = page_root / "sync_execution.log"

            print(f"::group::Recover {page} (cycle {cycle})")
            # v1.2.0 (P-154b): quota-aware guard. Cycle 1 reads the matrix leg's
            # own log (source artifacts); later cycles read the previous replay's
            # log first, so a replay that crossed the line stops the next one.
            if quota_mode != "off":
                prev_root = (evidence_root / group) if cycle == 2 else (evidence_root / group / f"cycle{cycle - 1}")
                roots = ([prev_root] if cycle > 1 else []) + [source_root]
                if _quota_guard(quota_mode, page, cycle, roots, quota_skip_pct, summary):
                    skipped_pages.append(page)
                    summary["results"].append(
                        {
                            "page": page,
                            "key": key,
                            "group": group,
                            "cycle": cycle,
                            "runner_exit": None,
                            "audit_status": "skipped",
                            "passed": False,
                            "skipped": "quota",
                            "evidence_root": str(page_root),
                        }
                    )
                    print("::endgroup::")
                    continue
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
                if cycle < max_cycles:
                    pending.append(item)
                    print(
                        f"::warning::Inline recovery incomplete for {page} "
                        f"(cycle {cycle}): runner_exit={runner_exit}, "
                        f"audit_status={audit_status} — will retry."
                    )
                else:
                    failed_pages.append(page)
                    print(
                        f"::error::Inline recovery failed for {page}: "
                        f"runner_exit={runner_exit}, audit_status={audit_status}"
                    )
            else:
                print(f"::notice::Inline recovery passed for {page} (cycle {cycle})")

            summary["results"].append(
                {
                    "page": page,
                    "key": key,
                    "group": group,
                    "cycle": cycle,
                    "runner_exit": runner_exit,
                    "audit_status": audit_status,
                    "passed": passed,
                    "evidence_root": str(page_root),
                }
            )
            print("::endgroup::")

    summary["cycles_used"] = cycle
    summary["status"] = "ok" if not failed_pages else "blocked"
    summary["failed_pages"] = failed_pages
    if quota_mode != "off":                # v1.2.0 (P-154b)
        summary["skipped_pages"] = sorted(dict.fromkeys(skipped_pages))
    summary_out.parent.mkdir(parents=True, exist_ok=True)
    summary_out.write_text(
        json.dumps(summary, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )

    if failed_pages:
        print(f"::error::Inline page recovery blocked: {', '.join(failed_pages)}")
        return 2
    if skipped_pages:
        print("::warning::Inline recovery deferred on EODHD quota for: %s (last-good rows kept; next scheduled window retries)"
              % ", ".join(sorted(dict.fromkeys(skipped_pages))))
        return 0

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
