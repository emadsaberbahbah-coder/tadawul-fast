#!/usr/bin/env python3
"""Build one read-only TFB evidence bundle across GitHub, Render and Sheets.

Modes:
  repo     static repository audit; no credentials required
  runtime  existing deployment verifier + optional Render endpoint probe
  sheets   existing live dashboard validator
  all      all of the above

The command never changes production data and redacts secret-bearing fields.
A clean technical result is not an investment recommendation.
"""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import re
import subprocess
import sys
import time
from typing import Any, Mapping, Sequence
from urllib.error import HTTPError, URLError
from urllib.parse import urljoin
from urllib.request import Request, urlopen

VERSION = "1.0.0"
SECRET_RE = re.compile(r"(?i)(secret|token|password|private[_-]?key|api[_-]?key|authorization|cookie|credential)")
TRUTHY = {"1", "true", "yes", "y", "on", "enabled", "enable"}
CRITICAL_FILES = (
    "main.py", "requirements.txt", "Procfile", "scripts/start_web.sh",
    "scripts/verify_deployment.py", "scripts/validate_dashboard.py",
    "scripts/run_dashboard_sync.py", "core/data_engine_v2.py",
    "core/surface_action_invariants.py", "core/sheets/schema_registry.py",
)
GUARDS = (
    "TFB_T10_BLOCKED_INVARIANT", "TFB_T10_FETCHFAIL_BLOCKED",
    "TFB_WARN_INVEST_INVARIANT", "TFB_ROW_SANITY_QUARANTINE",
    "TFB_ENGINE_OHLC_COHERENCE", "TFB_ENGINE_OHLC_COHERENCE_FINAL",
    "TFB_ENGINE_BATCH_FPRINT",
)
DUPLICATE_GROUPS = {
    "config": ("config.py", "core/config.py", "routes/config.py"),
    "symbols_reader": ("symbols_reader.py", "core/symbols_reader.py", "integrations/symbols_reader.py"),
    "data_engine": ("core/data_engine.py", "core/data_engine_v2.py"),
    "scoring": ("core/scoring.py", "core/scoring_engine.py"),
}


def now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def redact(value: Any) -> Any:
    if isinstance(value, Mapping):
        return {str(k): ("<redacted>" if SECRET_RE.search(str(k)) else redact(v)) for k, v in value.items()}
    if isinstance(value, list):
        return [redact(v) for v in value]
    if isinstance(value, str):
        value = re.sub(r"(?i)(bearer\s+)[A-Za-z0-9._~+\-/=]+", r"\1<redacted>", value)
        value = re.sub(r"(?i)((?:api[_-]?key|token|password)\s*[=:]\s*)[^\s,;]+", r"\1<redacted>", value)
        return value[:12000] + ("\n...<truncated>" if len(value) > 12000 else "")
    return value


def finding(check: str, area: str, status: str, severity: str, summary: str, evidence: Any = None, action: str = "") -> dict[str, Any]:
    return {
        "check": check, "area": area, "status": status, "severity": severity,
        "summary": summary, "evidence": redact(evidence or {}), "next_action": action,
    }


def sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for block in iter(lambda: f.read(1024 * 1024), b""):
            h.update(block)
    return h.hexdigest()


def git_sha(root: Path) -> str:
    try:
        p = subprocess.run(["git", "rev-parse", "HEAD"], cwd=root, text=True, capture_output=True, timeout=10)
        if p.returncode == 0:
            return p.stdout.strip()
    except Exception:
        pass
    return os.getenv("GITHUB_SHA") or os.getenv("RENDER_GIT_COMMIT") or "unknown"


def run(command: Sequence[str], root: Path, timeout: int, env: Mapping[str, str] | None = None) -> dict[str, Any]:
    merged = os.environ.copy()
    if env:
        merged.update({str(k): str(v) for k, v in env.items()})
    started = time.monotonic()
    try:
        p = subprocess.run(list(command), cwd=root, env=merged, text=True, capture_output=True, timeout=timeout)
        return redact({
            "command": list(command), "exit_code": p.returncode,
            "seconds": round(time.monotonic() - started, 3),
            "stdout": p.stdout, "stderr": p.stderr,
        })
    except subprocess.TimeoutExpired as exc:
        return redact({"command": list(command), "exit_code": 124, "stdout": exc.stdout or "", "stderr": "timeout"})
    except OSError as exc:
        return {"command": list(command), "exit_code": 127, "stdout": "", "stderr": str(exc)}


def repo_audit(root: Path, timeout: int) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    missing = [p for p in CRITICAL_FILES if not (root / p).exists()]
    if missing:
        out.append(finding("repo.critical_files", "GitHub", "FAIL", "CRITICAL", "Critical files are missing.", {"missing": missing}, "Restore or formally retire each path."))
    else:
        out.append(finding("repo.critical_files", "GitHub", "PASS", "INFO", "Critical files are present and fingerprinted.", {p: sha256(root / p) for p in CRITICAL_FILES}))

    proc = root / "Procfile"
    if proc.exists():
        text = proc.read_text(encoding="utf-8", errors="replace")
        web = next((x.strip() for x in text.splitlines() if x.strip().startswith("web:")), "")
        aligned = "scripts/start_web.sh" in web
        out.append(finding("render.start_command", "Render", "PASS" if aligned else "FAIL", "INFO" if aligned else "CRITICAL", "Procfile web command is aligned." if aligned else "Procfile web command is not aligned.", {"web": web}, "Use scripts/start_web.sh as the canonical start path."))
        blueprint = (root / "render.yaml").exists() or (root / "render.yml").exists()
        stale = (not blueprint) and any(x in text.lower() for x in ("render.yaml defines", "render reads render.yaml", "ensured by render.yaml", "match render.yaml's"))
        out.append(finding("render.blueprint_truth", "Render", "WARN" if stale else "PASS", "HIGH" if stale else "INFO", "Stale Render Blueprint claims remain." if stale else "Render documentation matches Blueprint presence/absence.", {"blueprint_present": blueprint}, "State that the service is dashboard-managed and capture the observed settings."))

    gas_files = sorted(str(p.relative_to(root)) for p in (root / "apps_script").glob("*.gs")) if (root / "apps_script").exists() else []
    out.append(finding("gas.source_control", "Google Apps Script", "WARN" if len(gas_files) < 3 else "PASS", "CRITICAL" if len(gas_files) < 3 else "INFO", "Bound-project parity cannot be certified from the current mirror." if len(gas_files) < 3 else "Apps Script mirror has non-trivial coverage.", {"count": len(gas_files), "files": gas_files}, "Export the complete bound project and compare hashes."))

    dup = {name: [p for p in paths if (root / p).exists()] for name, paths in DUPLICATE_GROUPS.items()}
    dup = {k: v for k, v in dup.items() if len(v) > 1}
    out.append(finding("repo.canonical_paths", "GitHub", "WARN" if dup else "PASS", "HIGH" if dup else "INFO", "Multiple critical implementation paths require a canonical owner." if dup else "No duplicate critical implementation group detected.", dup, "Designate one canonical implementation and test production imports."))

    text = ""
    for rel in ("core/surface_action_invariants.py", "core/data_engine_v2.py", "scripts/run_dashboard_sync.py"):
        if (root / rel).exists():
            text += "\n" + (root / rel).read_text(encoding="utf-8", errors="replace")
    absent = [g for g in GUARDS if g not in text]
    default_off = bool(re.search(r"TFB_[A-Z0-9_]+[^\n]{0,100}default(?:s)?\s+(?:to\s+)?(?:['\"]?0|OFF)", text, re.I))
    if absent:
        out.append(finding("repo.guard_contract", "GitHub", "FAIL", "CRITICAL", "Critical guard names are absent.", {"missing": absent}, "Restore the guard or update the safety contract with evidence."))
    else:
        out.append(finding("repo.guard_contract", "GitHub", "WARN" if default_off else "PASS", "CRITICAL" if default_off else "INFO", "Critical guards exist but default-OFF behavior is documented." if default_off else "Critical guard names are present.", {"guards": list(GUARDS)}, "Use observe-to-enforce arming and expose effective state in health/_Status."))

    protected = (os.getenv("GITHUB_REF_PROTECTED") or "").lower()
    ref_name = (os.getenv("GITHUB_REF_NAME") or "").strip()
    base_ref = (os.getenv("GITHUB_BASE_REF") or "").strip()
    # github.ref_protected describes the current ref. On pull_request this is
    # commonly a merge/head ref, not the default branch, so never misreport an
    # unprotected PR branch as proof that main is unprotected.
    if protected and (not base_ref or ref_name == base_ref):
        is_protected = protected in TRUTHY
        out.append(finding("github.branch_protection", "GitHub", "PASS" if is_protected else "WARN", "INFO" if is_protected else "HIGH", "Audited branch-protection state was reported.", {"protected": is_protected, "ref_name": ref_name or "unknown"}, "Require review and relevant status checks on the default branch."))
    else:
        out.append(finding("github.branch_protection", "GitHub", "SKIP", "HIGH", "Default-branch protection is not provable from this workflow context.", {"ref_name": ref_name or "unknown", "base_ref": base_ref or ""}, "Capture the default branch settings through the GitHub API/settings page."))

    auditor = root / "scripts/audit_repository_workflows.py"
    if auditor.exists():
        r = run([sys.executable, str(auditor), "--root", str(root)], root, timeout)
        status = "PASS" if r["exit_code"] == 0 else "FAIL"
        out.append(finding("github.workflow_audit", "GitHub", status, "INFO" if status == "PASS" else "CRITICAL", "Existing workflow safety audit completed.", r))
    else:
        out.append(finding("github.workflow_audit", "GitHub", "SKIP", "HIGH", "Workflow safety auditor is missing."))

    targets = [root / x for x in ("scripts/full_system_audit.py", "scripts/verify_deployment.py", "scripts/validate_dashboard.py", "core/surface_action_invariants.py") if (root / x).exists()]
    if targets:
        r = run([sys.executable, "-m", "py_compile", *map(str, targets)], root, timeout)
        status = "PASS" if r["exit_code"] == 0 else "FAIL"
        out.append(finding("repo.python_compile", "GitHub", status, "INFO" if status == "PASS" else "CRITICAL", "Critical Python files compile." if status == "PASS" else "Critical Python compilation failed.", r))
    return out


def probe(base: str, endpoint: str, timeout: int) -> dict[str, Any]:
    url = urljoin(base.rstrip("/") + "/", endpoint.lstrip("/"))
    headers = {"Accept": "application/json,text/plain,*/*"}
    if os.getenv("TFB_AUDIT_TOKEN"):
        headers["Authorization"] = "Bearer " + os.environ["TFB_AUDIT_TOKEN"]
    started = time.monotonic()
    try:
        with urlopen(Request(url, headers=headers), timeout=min(timeout, 20)) as response:
            body = response.read(131072).decode("utf-8", errors="replace")
            try:
                body = json.loads(body)
            except Exception:
                pass
            return redact({"url": url, "status": response.status, "seconds": round(time.monotonic() - started, 3), "body": body})
    except HTTPError as exc:
        return redact({"url": url, "status": exc.code, "error": exc.read(32768).decode("utf-8", errors="replace")})
    except (URLError, OSError, TimeoutError) as exc:
        return {"url": url, "status": 0, "error": str(exc)}


def runtime_audit(root: Path, timeout: int, render_url: str) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    out: list[dict[str, Any]] = []
    safe_names = ("RENDER_SERVICE_NAME", "RENDER_SERVICE_ID", "RENDER_GIT_COMMIT", "RENDER_INSTANCE_ID", "RENDER_REGION", "PORT", "WEB_CONCURRENCY", "WORKERS_MAX", "LOG_LEVEL") + GUARDS
    snapshot = {name: os.environ[name] for name in safe_names if name in os.environ}
    snapshot["critical_hashes"] = {p: sha256(root / p) for p in CRITICAL_FILES if (root / p).exists()}

    verifier = root / "scripts/verify_deployment.py"
    if verifier.exists():
        r = run([sys.executable, str(verifier), "--json", "--strict"], root, timeout)
        mapping = {0: ("PASS", "INFO"), 2: ("WARN", "HIGH"), 1: ("FAIL", "CRITICAL")}
        status, sev = mapping.get(r["exit_code"], ("FAIL", "CRITICAL"))
        out.append(finding("render.verify_deployment", "Render", status, sev, "Existing deployment verifier inspected effective versions and flags.", r))
    else:
        out.append(finding("render.verify_deployment", "Render", "FAIL", "CRITICAL", "Deployment verifier is missing."))

    if render_url:
        probes = {e: probe(render_url, e, timeout) for e in ("/readyz", "/health", "/healthz", "/version")}
        snapshot["endpoint_probes"] = probes
        ready = int(probes["/readyz"].get("status") or 0)
        out.append(finding("render.readiness", "Render", "PASS" if 200 <= ready < 300 else "FAIL", "INFO" if 200 <= ready < 300 else "CRITICAL", "Render readiness endpoint responded successfully." if 200 <= ready < 300 else "Render readiness endpoint failed.", probes, "Correct the service URL, health path, deployment or authorization."))
    else:
        out.append(finding("render.readiness", "Render", "SKIP", "CRITICAL", "Render URL was not supplied.", {}, "Set TFB_RENDER_BASE_URL or pass --render-url."))
    return out, redact(snapshot)


def sheets_audit(root: Path, timeout: int, sheet_id: str, output: Path) -> list[dict[str, Any]]:
    validator = root / "scripts/validate_dashboard.py"
    if not validator.exists():
        return [finding("sheets.validate_dashboard", "Google Sheets", "FAIL", "CRITICAL", "Dashboard validator is missing.")]
    creds = any((os.getenv(x) or "").strip() for x in ("GOOGLE_SHEETS_CREDENTIALS", "GOOGLE_CREDENTIALS", "GOOGLE_APPLICATION_CREDENTIALS"))
    if not sheet_id or not creds:
        return [finding("sheets.validate_dashboard", "Google Sheets", "SKIP", "CRITICAL", "Spreadsheet ID or service-account credentials are unavailable.", {"sheet_id_present": bool(sheet_id), "credentials_present": bool(creds)}, "Configure repository secrets and rerun the manual all-mode workflow.")]
    artifact = output / "dashboard_validation.json"
    r = run([sys.executable, str(validator)], root, max(timeout, 900), {
        "VALIDATE_SHEET_ID": sheet_id, "VALIDATE_JSON_OUT": str(artifact), "VALIDATE_WRITE_SHEET": "0",
    })
    mapping = {0: ("PASS", "INFO"), 1: ("WARN", "HIGH"), 2: ("FAIL", "CRITICAL"), 3: ("FAIL", "CRITICAL")}
    status, sev = mapping.get(r["exit_code"], ("FAIL", "CRITICAL"))
    if artifact.exists():
        r["artifact"] = str(artifact)
        r["artifact_sha256"] = sha256(artifact)
    return [finding("sheets.validate_dashboard", "Google Sheets", status, sev, "Live persisted-sheet validation completed.", r)]


def verdict(findings: Sequence[dict[str, Any]], mode: str) -> str:
    if any(x["status"] == "FAIL" for x in findings):
        return "NO_GO"
    if any(x["status"] in {"WARN", "SKIP"} and x["severity"] in {"CRITICAL", "HIGH"} for x in findings):
        return "CONDITIONAL_NO_GO"
    return "REPO_CLEAN_PRODUCTION_UNVERIFIED" if mode == "repo" else "TECHNICAL_GO_FOR_SHADOW_ONLY"


def markdown(report: dict[str, Any]) -> str:
    lines = [
        "# TFB Full-System Audit", "",
        f"- Generated: `{report['generated_at_utc']}`",
        f"- Release SHA: `{report['release_sha']}`",
        f"- Mode: `{report['mode']}`",
        f"- Technical verdict: **{report['technical_verdict']}**", "",
        "> Technical certification only; not an investment recommendation.", "",
        "| Area | Check | Status | Severity | Summary |", "|---|---|---|---|---|",
    ]
    for x in report["findings"]:
        lines.append(f"| {x['area']} | `{x['check']}` | **{x['status']}** | {x['severity']} | {x['summary'].replace('|', '/')} |")
    lines.extend(["", "## Evidence details", ""])
    for x in report["findings"]:
        lines.extend([f"### {x['check']} - {x['status']}", "", x["summary"]])
        if x.get("next_action"):
            lines.extend(["", f"**Next action:** {x['next_action']}"])
        if x.get("evidence"):
            lines.extend(["", "```json", json.dumps(x["evidence"], indent=2, ensure_ascii=False), "```"])
        lines.append("")
    return "\n".join(lines)


def main(argv: Sequence[str] | None = None) -> int:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--root", default=".")
    p.add_argument("--mode", choices=("repo", "runtime", "sheets", "all"), default="repo")
    p.add_argument("--output-dir", default="audit-artifacts/full-system")
    p.add_argument("--render-url", default=os.getenv("TFB_RENDER_BASE_URL", ""))
    p.add_argument("--sheet-id", default=os.getenv("VALIDATE_SHEET_ID") or os.getenv("DEFAULT_SPREADSHEET_ID") or "")
    p.add_argument("--timeout", type=int, default=180)
    p.add_argument("--strict", action="store_true")
    args = p.parse_args(argv)

    root = Path(args.root).resolve()
    output = Path(args.output_dir)
    if not output.is_absolute():
        output = root / output
    output.mkdir(parents=True, exist_ok=True)

    findings: list[dict[str, Any]] = []
    runtime: dict[str, Any] = {}
    if args.mode in {"repo", "all"}:
        findings += repo_audit(root, max(10, args.timeout))
    if args.mode in {"runtime", "all"}:
        f, runtime = runtime_audit(root, max(10, args.timeout), args.render_url.strip())
        findings += f
    if args.mode in {"sheets", "all"}:
        findings += sheets_audit(root, max(10, args.timeout), args.sheet_id.strip(), output)

    counts = {s: sum(x["status"] == s for x in findings) for s in ("PASS", "WARN", "FAIL", "SKIP")}
    report = {
        "generated_at_utc": now(), "script_version": VERSION, "mode": args.mode,
        "release_sha": git_sha(root), "technical_verdict": verdict(findings, args.mode),
        "counts": counts, "findings": findings, "runtime_snapshot": runtime,
        "limitations": [
            "The audit never writes production Sheets or Render settings.",
            "Bound Apps Script parity requires a complete export or clasp/API evidence.",
            "A technical GO is shadow-only and is not an investment instruction.",
        ],
    }
    (output / "full_system_audit.json").write_text(json.dumps(report, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    md = markdown(report)
    (output / "full_system_audit.md").write_text(md, encoding="utf-8")
    if os.getenv("GITHUB_STEP_SUMMARY"):
        with open(os.environ["GITHUB_STEP_SUMMARY"], "a", encoding="utf-8") as f:
            f.write(md)
    print(f"TFB full-system audit v{VERSION}: {report['technical_verdict']} {counts}")
    if counts["FAIL"]:
        return 1
    if args.strict and (counts["WARN"] or counts["SKIP"]):
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
