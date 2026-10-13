"""tests/test_runner_pin_ubuntu2404.py - GitHub Actions runner image pin harness.

HARNESS_VERSION 1.0.0 (2026-10-13)

WHY (2026-10-13): GitHub is moving the ubuntu-latest runner label to a newer
Ubuntu image (26.04) from 19 Oct 2026. Every workflow in this repository ran
on ubuntu-latest (55 labels in 34 files at dda1211), and the scheduled lanes
depend on what the image ships (system curl/jq/python3/pip3 in the daily_sync
preflight, node for the Apps Script harnesses, a setup-python 3.11 build for
the Ubuntu release). ci.yml v1.1.13 pins every label to ubuntu-24.04 - the
image ubuntu-latest resolves to today - and adds a manual-only canary
workflow for the next image. This harness keeps that pin from drifting back
and proves the pin moved nothing but labels.

Cases (offline, read-only, YAML text only; no network, no credentials):
  P1 no ubuntu-latest in any workflow's YAML content (every parsed key and
     value, plus every line that is not a full-line YAML comment)
  P2 every job's runs-on is exactly ubuntu-24.04, except the canary job, whose
     runs-on is exactly the dispatch-input expression with the 24.04 fallback
  P3 the canary workflow: dispatch-only trigger, a runner_image choice of
     ubuntu-26.04 (default) / ubuntu-24.04, its own concurrency group (not
     ci-*, not the production write lease, shared with no other workflow),
     permissions contents: read, a bounded timeout, checkout without
     persisted credentials, no ${{ ... }} expression inside any run block,
     the daily_sync preflight tool check copied verbatim, node --version,
     setup-python@v6 3.11, pip install -r requirements.txt pytest, compileall
     and only tests that the blocking lean lane also runs
  P4 scripts/audit_repository_workflows.py reports errors=0 on the tree and
     no finding at all for the canary file
  P5 [needs TFB_RUNNER_PIN_BASE_DIR] label-only proof: for every base file,
     yaml.safe_load(current) == yaml.safe_load(base text with
     'runs-on: ubuntu-latest' replaced by 'runs-on: ubuntu-24.04'), and the
     only new workflow file is the canary. The single declared exception is
     the enrolment line of this harness in ci.yml's lean pytest list, which
     must appear exactly once and is removed before ci.yml is compared
  P6 [needs TFB_RUNNER_PIN_BASE_DIR] golden negative: the pin check (the
     function behind P2) reports exactly 55 unpinned labels in 34 files in
     the base copies, every one of them ubuntu-latest
  P7 [needs TFB_RUNNER_PIN_BASE_DIR] golden negative: the gate cases P1, P2
     and P3 each FAIL when pointed at the base copies

Env (all optional):
  TFB_RUNNER_PIN_BASE_DIR=<dir>       directory of base workflow copies, e.g.
      for f in $(git ls-tree --name-only dda1211 .github/workflows/); do
        git show dda1211:$f > <dir>/$(basename $f); done
    Unset: P5-P7 SKIP cleanly.
  TFB_RUNNER_PIN_WORKFLOWS_DIR=<dir>  check another workflows directory
    instead of this repository's .github/workflows. Pointing it at the base
    copies turns P1-P3 (and P5) red: the golden negative run by hand.

Run:  python tests/test_runner_pin_ubuntu2404.py        -> "PASS k/k", exit 0/1
      python -m pytest -q tests/test_runner_pin_ubuntu2404.py
"""
from __future__ import annotations

import importlib.util
import os
import sys
from pathlib import Path

import yaml

try:  # pytest is optional in script mode
    import pytest
except ImportError:  # pragma: no cover - script mode without pytest
    pytest = None


HARNESS_VERSION = "1.0.0"
ROOT = Path(__file__).resolve().parents[1]
REPO_WORKFLOWS = ROOT / ".github" / "workflows"

PINNED_LABEL = "ubuntu-24.04"
FLOATING_LABEL = "ubuntu-latest"
CANARY_FILE = "runner_image_canary.yml"
CANARY_RUNS_ON = "${{ inputs.runner_image || 'ubuntu-24.04' }}"
CANARY_CHOICES = ("ubuntu-26.04", "ubuntu-24.04")
CANARY_DEFAULT = "ubuntu-26.04"
CANARY_MAX_TIMEOUT = 30
PREFLIGHT_SOURCE = ("daily_sync.yml", "preflight-validation", "validate")
LEAN_SOURCE = ("ci.yml", "lean-unit")
PRODUCTION_LEASE_MARKERS = ("tadawul-production-write", "tadawul-ci-")

BASE_COMMIT = "dda1211"
BASE_UNPINNED_LABELS = 55
BASE_UNPINNED_FILES = 34
BASE_LABEL_LINE = "runs-on: " + FLOATING_LABEL
PINNED_LABEL_LINE = "runs-on: " + PINNED_LABEL
# The one non-label change this item makes to an existing workflow: enrolling
# this harness in ci.yml's blocking lean pytest list. P5 removes exactly this
# line (it must be present exactly once) before comparing with the base.
DECLARED_EXTRA_LINES = {"ci.yml": ("tests/test_runner_pin_ubuntu2404.py \\",)}

ENV_BASE_DIR = "TFB_RUNNER_PIN_BASE_DIR"
ENV_WORKFLOWS_DIR = "TFB_RUNNER_PIN_WORKFLOWS_DIR"


# ---------------------------------------------------------------- helpers
def _workflows_dir() -> Path:
    override = os.environ.get(ENV_WORKFLOWS_DIR, "").strip()
    return Path(override) if override else REPO_WORKFLOWS


def _base_dir():
    value = os.environ.get(ENV_BASE_DIR, "").strip()
    return Path(value) if value else None


def _files(directory: Path):
    return sorted(list(directory.glob("*.yml")) + list(directory.glob("*.yaml")))


def _load(path: Path):
    return yaml.safe_load(path.read_text(encoding="utf-8"))


def _triggers(doc):
    # PyYAML (YAML 1.1) reads the bare key `on` as boolean True.
    if not isinstance(doc, dict):
        return None
    return doc.get("on", doc.get(True))


def _strings(node):
    if isinstance(node, dict):
        for key, value in node.items():
            yield from _strings(key)
            yield from _strings(value)
    elif isinstance(node, list):
        for item in node:
            yield from _strings(item)
    elif isinstance(node, str):
        yield node


def _jobs(doc):
    jobs = doc.get("jobs") if isinstance(doc, dict) else None
    return jobs if isinstance(jobs, dict) else {}


def _steps(job):
    steps = job.get("steps") if isinstance(job, dict) else None
    return [step for step in (steps or []) if isinstance(step, dict)]


def _concurrency_group(doc):
    value = doc.get("concurrency") if isinstance(doc, dict) else None
    if isinstance(value, dict):
        return value.get("group")
    return value


# ------------------------------------------------------------ the checks
def floating_hits(directory: Path):
    """Every place ubuntu-latest still appears as workflow content."""
    hits = []
    for path in _files(directory):
        text = path.read_text(encoding="utf-8")
        for number, line in enumerate(text.splitlines(), 1):
            if FLOATING_LABEL in line and not line.lstrip().startswith("#"):
                hits.append(f"{path.name}:{number}: {line.strip()}")
        if any(FLOATING_LABEL in value for value in _strings(_load(path))):
            hits.append(f"{path.name}: parsed YAML content names {FLOATING_LABEL}")
    return hits


def runs_on_labels(directory: Path):
    """(file, job, runs-on) for every job; reusable-workflow jobs are skipped."""
    labels = []
    for path in _files(directory):
        for job_name, job in _jobs(_load(path)).items():
            if isinstance(job, dict) and "uses" in job and "runs-on" not in job:
                continue
            value = job.get("runs-on") if isinstance(job, dict) else None
            labels.append((path.name, job_name, value))
    return labels


def unpinned_labels(directory: Path):
    """The pin check: every runs-on that is not the pinned label.

    The canary's own dispatch-input expression is the single allowed exception.
    """
    return [
        (name, job, value) for name, job, value in runs_on_labels(directory)
        if value != PINNED_LABEL
        and not (name == CANARY_FILE and value == CANARY_RUNS_ON)
    ]


def _preflight_run(directory: Path):
    name, job_name, step_id = PREFLIGHT_SOURCE
    path = directory / name
    if not path.exists():
        return None
    job = _jobs(_load(path)).get(job_name) or {}
    for step in _steps(job):
        if step.get("id") == step_id:
            return step.get("run")
    return None


def _lean_text(directory: Path):
    name, job_name = LEAN_SOURCE
    path = directory / name
    if not path.exists():
        return ""
    job = _jobs(_load(path)).get(job_name) or {}
    return "\n".join(str(step.get("run", "")) for step in _steps(job))


def canary_problems(directory: Path):
    problems = []
    path = directory / CANARY_FILE
    if not path.exists():
        return [f"{CANARY_FILE} is missing"]
    doc = _load(path)
    if not isinstance(doc, dict):
        return [f"{CANARY_FILE} is not a mapping"]

    triggers = _triggers(doc)
    if not isinstance(triggers, dict) or set(triggers) != {"workflow_dispatch"}:
        problems.append(f"trigger must be workflow_dispatch only, got {triggers!r}")
        dispatch = {}
    else:
        dispatch = triggers.get("workflow_dispatch") or {}
    inputs = dispatch.get("inputs") if isinstance(dispatch, dict) else None
    inputs = inputs if isinstance(inputs, dict) else {}
    if set(inputs) != {"runner_image"}:
        problems.append(f"inputs must be exactly runner_image, got {sorted(inputs)}")
    image = inputs.get("runner_image") or {}
    if image.get("type") != "choice":
        problems.append("runner_image must be a choice input")
    if sorted(image.get("options") or []) != sorted(CANARY_CHOICES):
        problems.append(f"runner_image options must be {list(CANARY_CHOICES)}, got {image.get('options')}")
    if image.get("default") != CANARY_DEFAULT:
        problems.append(f"runner_image default must be {CANARY_DEFAULT}, got {image.get('default')!r}")

    group = _concurrency_group(doc)
    if not isinstance(group, str) or not group.strip():
        problems.append("canary needs its own concurrency group")
    else:
        if group.startswith("ci-") or "${{ github.workflow }}" in group:
            problems.append(f"concurrency group must not be ci-* or workflow-derived: {group}")
        if any(marker in group for marker in PRODUCTION_LEASE_MARKERS):
            problems.append(f"concurrency group must not be the production lease: {group}")
        if "runner-image-canary" not in group:
            problems.append(f"concurrency group must name the canary: {group}")
        for other in _files(directory):
            if other.name != CANARY_FILE and _concurrency_group(_load(other)) == group:
                problems.append(f"concurrency group shared with {other.name}")

    if doc.get("permissions") != {"contents": "read"}:
        problems.append(f"permissions must be exactly contents: read, got {doc.get('permissions')!r}")

    jobs = _jobs(doc)
    if not jobs:
        problems.append("canary has no jobs")
    runs = []
    uses = []
    for job_name, job in jobs.items():
        if job.get("runs-on") != CANARY_RUNS_ON:
            problems.append(f"job {job_name} runs-on must be {CANARY_RUNS_ON}, got {job.get('runs-on')!r}")
        if "permissions" in job and job["permissions"] != {"contents": "read"}:
            problems.append(f"job {job_name} must not widen permissions")
        timeout = job.get("timeout-minutes")
        if not isinstance(timeout, int) or not 1 <= timeout <= CANARY_MAX_TIMEOUT:
            problems.append(f"job {job_name} timeout-minutes must be 1..{CANARY_MAX_TIMEOUT}, got {timeout!r}")
        for step in _steps(job):
            if "run" in step:
                runs.append(str(step["run"]))
                if "${{" in str(step["run"]):
                    problems.append(f"step {step.get('name')!r} interpolates an expression inside run")
            if "uses" in step:
                uses.append(step)

    checkouts = [s for s in uses if str(s["uses"]).startswith("actions/checkout@")]
    if not checkouts:
        problems.append("canary must check out the repository")
    for step in checkouts:
        with_block = step.get("with") or {}
        if str(with_block.get("persist-credentials")).lower() != "false":
            problems.append("checkout must set persist-credentials: false")
    pythons = [s for s in uses if s["uses"] == "actions/setup-python@v6"]
    if not any(str((s.get("with") or {}).get("python-version")) == "3.11" for s in pythons):
        problems.append("canary must run actions/setup-python@v6 with 3.11")

    preflight = _preflight_run(directory)
    if not preflight:
        problems.append("daily_sync preflight tool check not found to compare against")
    elif preflight not in runs:
        problems.append("daily_sync preflight tool check is not copied verbatim")
    joined = "\n".join(runs)
    for needle in ("node --version", "pip install -r requirements.txt pytest",
                   "compileall -q main.py core routes scripts", "pytest"):
        if needle not in joined:
            problems.append(f"canary run blocks lack {needle!r}")

    lean = set(_lean_text(directory).replace("\\", " ").split())
    tests = sorted({token for run in runs for token in run.replace("\\", " ").split()
                    if token.startswith("tests/") and token.endswith(".py")})
    if not tests:
        problems.append("canary runs no tests from the lean list")
    for test in tests:
        if test not in lean:
            problems.append(f"{test} is not in the blocking lean lane")
        if not (ROOT / test).exists():
            problems.append(f"{test} does not exist")
    return problems


def _audit_module():
    spec = importlib.util.spec_from_file_location(
        "_tfb_audit_repository_workflows", ROOT / "scripts" / "audit_repository_workflows.py")
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def audit_problems(directory: Path):
    findings = _audit_module().audit_paths(_files(directory))
    problems = [f"{f.severity} {f.code} {Path(f.path).name}:{f.line}" for f in findings
                if f.severity == "ERROR"]
    problems += [f"canary finding {f.severity} {f.code}:{f.line}" for f in findings
                 if Path(f.path).name == CANARY_FILE and f.severity != "ERROR"]
    warnings = sum(f.severity == "WARN" for f in findings)
    return problems, warnings


def _strip_declared_lines(node, lines, removed):
    """Drop the declared enrolment line(s) from every run string, counting them."""
    if isinstance(node, dict):
        return {key: _strip_declared_lines(value, lines, removed) for key, value in node.items()}
    if isinstance(node, list):
        return [_strip_declared_lines(item, lines, removed) for item in node]
    if isinstance(node, str) and any(line in node for line in lines):
        kept = []
        for part in node.split("\n"):
            if part.strip() in lines:
                removed.append(part.strip())
            else:
                kept.append(part)
        return "\n".join(kept)
    return node


def label_only_problems(current: Path, base: Path):
    problems = []
    base_files = {path.name: path for path in _files(base)}
    current_files = {path.name: path for path in _files(current)}
    if not base_files:
        return [f"no workflow files in base dir {base}"], 0, 0
    substitutions = 0
    for name, base_path in base_files.items():
        if name not in current_files:
            problems.append(f"{name} was removed")
            continue
        base_text = base_path.read_text(encoding="utf-8")
        substitutions += base_text.count(BASE_LABEL_LINE)
        expected = yaml.safe_load(base_text.replace(BASE_LABEL_LINE, PINNED_LABEL_LINE))
        actual = _load(current_files[name])
        declared = DECLARED_EXTRA_LINES.get(name, ())
        if declared:
            removed = []
            actual = _strip_declared_lines(actual, declared, removed)
            if sorted(removed) != sorted(declared):
                problems.append(f"{name}: declared extra line(s) {declared} found {removed}")
        if actual != expected:
            problems.append(f"{name}: parsed YAML differs beyond the label substitution")
    extra = sorted(set(current_files) - set(base_files) - {CANARY_FILE})
    if extra:
        problems.append(f"unexpected new workflow files: {extra}")
    return problems, substitutions, len(base_files)


# -------------------------------------------------------- case functions
def case_p1():
    hits = floating_hits(_workflows_dir())
    if hits:
        return "FAIL", f"{len(hits)} hit(s): {hits[:3]}"
    return "PASS", f"0 {FLOATING_LABEL} in {len(_files(_workflows_dir()))} workflow files"


def case_p2():
    directory = _workflows_dir()
    labels = runs_on_labels(directory)
    bad = unpinned_labels(directory)
    if not labels:
        return "FAIL", "no jobs found"
    if bad:
        return "FAIL", f"{len(bad)} unpinned: {bad[:3]}"
    pinned = sum(value == PINNED_LABEL for _, _, value in labels)
    return "PASS", f"{pinned} jobs on {PINNED_LABEL} + {len(labels) - pinned} canary expression"


def case_p3():
    problems = canary_problems(_workflows_dir())
    if problems:
        return "FAIL", "; ".join(problems[:4])
    return "PASS", f"{CANARY_FILE}: dispatch-only, own group, contents: read, no expression in run"


def case_p4():
    problems, warnings = audit_problems(_workflows_dir())
    if problems:
        return "FAIL", "; ".join(problems[:4])
    return "PASS", f"workflow audit errors=0 warnings={warnings}; canary findings=0"


def case_p5():
    base = _base_dir()
    if base is None:
        return "SKIP", f"{ENV_BASE_DIR} unset"
    problems, substitutions, count = label_only_problems(_workflows_dir(), base)
    if problems:
        return "FAIL", "; ".join(problems[:4])
    return "PASS", f"{count} base files equal after {substitutions} label substitutions; only new file {CANARY_FILE}"


def case_p6():
    base = _base_dir()
    if base is None:
        return "SKIP", f"{ENV_BASE_DIR} unset"
    bad = unpinned_labels(base)
    files = {name for name, _, _ in bad}
    values = {value for _, _, value in bad}
    ok = (len(bad) == BASE_UNPINNED_LABELS and len(files) == BASE_UNPINNED_FILES
          and values == {FLOATING_LABEL})
    detail = f"pin check on base: {len(bad)} unpinned labels in {len(files)} files, values {sorted(map(str, values))}"
    return ("PASS" if ok else "FAIL"), detail


def case_p7():
    base = _base_dir()
    if base is None:
        return "SKIP", f"{ENV_BASE_DIR} unset"
    red = {
        "P1": bool(floating_hits(base)),
        "P2": bool(unpinned_labels(base)),
        "P3": bool(canary_problems(base)),
    }
    ok = all(red.values())
    return ("PASS" if ok else "FAIL"), "gate cases red on base: " + ", ".join(
        f"{name}={'FAIL' if failed else 'pass'}" for name, failed in red.items())


CASES = (
    ("P1 no ubuntu-latest in workflow content", case_p1),
    ("P2 every runs-on pinned (canary expression excepted)", case_p2),
    ("P3 canary workflow is manual, isolated and read-only", case_p3),
    ("P4 repository workflow audit errors=0", case_p4),
    ("P5 base: label-only yaml.safe_load equality", case_p5),
    ("P6 base golden negative: 55 unpinned labels", case_p6),
    ("P7 base golden negative: gate cases fail on base", case_p7),
)


# ----------------------------------------------------------- pytest glue
if pytest is not None:
    @pytest.mark.parametrize("name,case", CASES, ids=[name.split()[0] for name, _ in CASES])
    def test_runner_pin_case(name, case):
        status, detail = case()
        if status == "SKIP":
            pytest.skip(f"{name}: {detail}")
        assert status == "PASS", f"{name}: {detail}"


def main() -> int:
    passed = failed = skipped = 0
    for name, case in CASES:
        try:
            status, detail = case()
        except Exception as exc:  # noqa: BLE001 - a crashing case is a failing case
            status, detail = "FAIL", f"{type(exc).__name__}: {exc}"
        print(f"{status} {name} | {detail}")
        passed += status == "PASS"
        failed += status == "FAIL"
        skipped += status == "SKIP"
    ran = passed + failed
    note = f" ({skipped} skipped: {ENV_BASE_DIR} unset)" if skipped else ""
    if failed:
        print(f"FAIL {failed}/{ran} (PASS {passed}/{ran}){note}")
        return 1
    print(f"PASS {passed}/{ran}{note}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
