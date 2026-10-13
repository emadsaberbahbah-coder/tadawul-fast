"""tests/test_render_env_contract.py - Render ENV contract harness (Build Spec H-8).

HARNESS_VERSION 1.0.0 (2026-10-13)

WHY (2026-10-13): Build Spec item H-8 asks for a Render ENV contract and the
repository had none. On 13 Oct 2026 the owner's /health showed app_version
5.111.0 while entry_version was 8.14.3: config.py reads TFB_APP_VERSION, then
APP_VERSION, and main.py falls back to its entry version only when both are
unset, so one of them is still set in the Render service ENV or in a linked
env group. scripts/render_env_contract.py 1.0.0 now generates
docs/render_env_contract.md from the code (stdlib + ast, never importing what
it scans). This harness keeps that document complete, safe to publish and in
step with its generator.

REVIEW FIXES (2026-10-13, same unreleased 1.0.0, nothing removed): R8 now
looks for a unique sentinel instead of the bare word "probe", which a future
FLAGS meaning could legitimately contain. R10 also covers the reviewer's
leak probes (a *_URL value, "user:pass", a key block, a spaced opaque string,
a long letters-and-digits run). New R13 (env_combo names its four ENV inputs
and its cross-key rule), R14 (section C says which kill switches read
disarmed when unset) and R15 (the generator refuses to emit a document that
fails its own secret check). R13 and R14 join the golden-negative set.

Cases (offline; read-only except a temporary directory; no network, no
credentials):
  R1 the generator exists, imports only the standard library, and builds the
     document with sockets blocked and without importing any module it scans
     (main, config, env, core.config, core.data_engine_v2,
     core.price_precision, the two decision modules, verify_deployment)
  R2 every main.py _PF_GATE_ENVS and _OB_GATE_ENVS key (read here from
     main.py by AST, independently of the generator) has a row in section B
     of the committed document
  R3 the TFB_APP_VERSION and APP_VERSION rows of section A say MUST be unset
     in both the contract and the expected-value columns
  R4 every surface_gate_states() key has a row in B.3, every Render-scoped
     verify_deployment FLAGS entry has a row in C, and no workflow-scoped
     entry does
  R5 no secret-looking value, no URL and no auth/token/secret key name
     (owner decision D7) anywhere in the committed document
  R6 the committed document is ASCII
  R7 the committed document equals the generator output; the failure says
     how to regenerate
  R8 the output does not depend on the environment: APP_VERSION,
     TFB_APP_VERSION, a gate and REDIS_URL set to probe values change nothing,
     and the probe sentinel never appears in it
  R9 CLI: --check exits 0 on the committed document and 1 on a stale or
     missing copy; the default mode writes, prints WARN on a change, exits 0
  R10 expected values: with no file every row says 'not recorded' and the
     preamble says 'expected production value: not recorded'; with an owner
     file a plain value is shown, a secret-looking value and a D7 name are
     withheld, a *_URL value is never shown, and APP_VERSION stays MUST be
     unset
  R11 [needs TFB_RENDER_ENV_CONTRACT_BASE] golden negative: R1, R2, R3, R7,
     R13 and R14 each FAIL against the base copy (no generator, no document)
  R12 guard self-test: the R5 check reports each seeded secret (credentials
     in a URL, a D7 key name, a long opaque token, a private key block)
     appended to an in-memory copy of the document; nothing is written
  R13 engine_gates env_combo lists every ENV name that
     core/surface_action_invariants.env_combo_violations() reads (found here
     by AST, independently), shows 'ok' when unset, prints one rule line per
     violation code in that function, and no B.3 row claims "no ENV read"
  R14 section C explains "Kill switch", and every Render-scoped kill switch
     whose manifest default is not in the verifier's _ARMED (and is not a
     declared disarm) says "unset reads DISARMED"
  R15 the generator's own guard reports every R12 seed, and its CLI exits 2
     without writing or printing when the finished text would carry one

Env (optional):
  TFB_RENDER_ENV_CONTRACT_BASE=<dir>  a full source copy of the base commit,
      e.g.  mkdir -p <dir> && git archive dda1211 | tar -x -C <dir>
    Unset: R11 SKIPs cleanly.

Run:  python tests/test_render_env_contract.py        -> "PASS k/k", exit 0/1
      python -m pytest -q tests/test_render_env_contract.py

Regenerate the document after changing any input:
      python scripts/render_env_contract.py
"""
from __future__ import annotations

import ast
import hashlib
import importlib.util
import json
import os
import re
import shutil
import socket
import subprocess
import sys
import tempfile
from pathlib import Path
from unittest import mock

try:  # pytest is optional in script mode
    import pytest
except ImportError:  # pragma: no cover - script mode without pytest
    pytest = None


HARNESS_VERSION = "1.0.0"
ROOT = Path(__file__).resolve().parents[1]
GENERATOR = "scripts/render_env_contract.py"
DOC = "docs/render_env_contract.md"
REGENERATE = "python scripts/render_env_contract.py"
ENV_BASE = "TFB_RENDER_ENV_CONTRACT_BASE"
BASE_COMMIT = "dda1211"
MUST_UNSET = "MUST be unset"
NOT_RECORDED_LINE = "expected production value: not recorded"

SCANNED_MODULES = (
    "main", "config", "env", "core.config", "core.data_engine_v2",
    "core.price_precision", "core.analysis.portfolio_actions",
    "core.analysis.opportunity_builder", "verify_deployment",
    "scripts.verify_deployment",
)
# Kept independent of the generator's own pattern on purpose (D7).
D7_NAME_RE = re.compile(
    r"AUTH|TOKEN|SECRET|PASSWORD|PASSWD|PASSPHRASE|API_?KEY|PRIVATE|CREDENTIAL|"
    r"BEARER|OPEN_MODE|SIGNING|COOKIE|SESSION_KEY|_KEYS?$|^KEY_")
KEY_NAME_RE = re.compile(r"\b[A-Z][A-Z0-9]*(?:_[A-Z0-9]+)+\b")
SECRET_PATTERNS = (
    ("private key block", re.compile(r"-----BEGIN")),
    ("URL", re.compile(r"[A-Za-z][A-Za-z0-9+.\-]*://")),
    ("credentials in a URL", re.compile(r"//[^/\s]*:[^/\s]*@")),
    ("provider key prefix", re.compile(r"\b(?:sk|pk|rk)[-_][A-Za-z0-9]{16,}")),
    ("cloud access key", re.compile(r"\bAKIA[0-9A-Z]{16}\b")),
    ("GitHub token", re.compile(r"\bgh[pousr]_[A-Za-z0-9]{20,}")),
    ("Slack token", re.compile(r"\bxox[baprs]-")),
    ("Google API key", re.compile(r"\bAIza[0-9A-Za-z_\-]{30,}")),
    ("JWT", re.compile(r"\beyJ[A-Za-z0-9_\-]{10,}\.")),
)
LONG_TOKEN_RE = re.compile(r"`([^`]{24,})`")
SNAKE_RE = re.compile(r"_*[a-z0-9]+(?:_[a-z0-9]+)*(?:\(\))?")


# ---------------------------------------------------------------- helpers
def _read(root: Path, rel: str):
    path = Path(root) / rel
    return path.read_text(encoding="utf-8") if path.is_file() else None


def _module_assign(root: Path, rel: str, name: str):
    """literal_eval of the last top-level assignment of name (AST, no import)."""
    text = _read(root, rel)
    if text is None:
        raise AssertionError(f"{rel} is missing")
    found = None
    for node in ast.parse(text).body:
        targets = node.targets if isinstance(node, ast.Assign) else (
            [node.target] if isinstance(node, ast.AnnAssign) else [])
        for target in targets:
            if isinstance(target, ast.Name) and target.id == name and node.value is not None:
                try:
                    found = ast.literal_eval(node.value)
                except ValueError:
                    found = node.value
    if found is None:
        raise AssertionError(f"{rel}: {name} not found")
    return found


def _engine_keys(root: Path):
    text = _read(root, "core/data_engine_v2.py")
    tree = ast.parse(text)
    for node in tree.body:
        if isinstance(node, ast.FunctionDef) and node.name == "surface_gate_states":
            for inner in ast.walk(node):
                if isinstance(inner, ast.Return) and isinstance(inner.value, ast.Dict):
                    return [k.value for k in inner.value.keys if isinstance(k, ast.Constant)]
    raise AssertionError("surface_gate_states() dict not found")


def _section(doc: str, start: str, end: str | None = None) -> str:
    i = doc.find("\n" + start)
    if i < 0:
        return ""
    j = doc.find("\n" + end, i + 1) if end else -1
    return doc[i:j] if j >= 0 else doc[i:]


def _rows(section: str):
    """{first-cell key: [cells]} for every table row in section."""
    out = {}
    for line in section.splitlines():
        if not line.startswith("| `"):
            continue
        cells = [c.strip() for c in line.strip().strip("|").split("|")]
        out[cells[0].strip("`")] = cells
    return out


def _load_generator(root: Path):
    path = Path(root) / GENERATOR
    if not path.is_file():
        raise AssertionError(f"{GENERATOR} is missing under {root}")
    tag = hashlib.sha1(str(path).encode("utf-8")).hexdigest()[:12]
    spec = importlib.util.spec_from_file_location(f"_render_env_contract_{tag}", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


_GENERATED = {}


def _generated(root: Path) -> str:
    key = str(Path(root).resolve())
    if key not in _GENERATED:
        _GENERATED[key] = _load_generator(root).generate(Path(root))
    return _GENERATED[key]


def _committed(root: Path) -> str:
    doc = _read(root, DOC)
    if doc is None:
        raise AssertionError(f"{DOC} is missing under {root}")
    return doc


def _blocked(*_args, **_kwargs):
    raise OSError("network blocked by tests/test_render_env_contract.py")


def _run_cli(*args, cwd=None):
    proc = subprocess.run([sys.executable, "-I", str(ROOT / GENERATOR), *args],
                          capture_output=True, text=True, cwd=cwd or tempfile.gettempdir(),
                          timeout=180, check=False)
    return proc.returncode, proc.stdout + proc.stderr


# ------------------------------------------------------------------ cases
def case_r1(root=ROOT):
    path = Path(root) / GENERATOR
    if not path.is_file():
        return "FAIL", f"{GENERATOR} is missing"
    tree = ast.parse(path.read_text(encoding="utf-8"))
    imported = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            imported.update(a.name.split(".")[0] for a in node.names)
        elif isinstance(node, ast.ImportFrom) and not node.level:
            imported.add((node.module or "").split(".")[0])
    stdlib = set(getattr(sys, "stdlib_module_names", ())) | {"__future__"}
    foreign = sorted(m for m in imported if m not in stdlib)
    if foreign:
        return "FAIL", f"non-stdlib imports: {foreign}"
    before = {m for m in SCANNED_MODULES if m in sys.modules}
    with mock.patch.object(socket.socket, "connect", _blocked), \
            mock.patch.object(socket, "create_connection", _blocked), \
            mock.patch.object(socket, "getaddrinfo", _blocked):
        text = _load_generator(root).generate(Path(root))
    newly = sorted(m for m in SCANNED_MODULES if m in sys.modules and m not in before)
    if newly:
        return "FAIL", f"generator imported scanned modules: {newly}"
    if "## A." not in text or "## B." not in text or "## C." not in text or "## D." not in text:
        return "FAIL", "generated document lacks a section A-D heading"
    _GENERATED[str(Path(root).resolve())] = text
    return "PASS", f"stdlib only ({len(imported)} modules); offline; {len(text.splitlines())} lines"


def case_r2(root=ROOT):
    doc = _committed(root)
    pf = _module_assign(root, "main.py", "_PF_GATE_ENVS")
    ob = _module_assign(root, "main.py", "_OB_GATE_ENVS")
    b1 = _rows(_section(doc, "### B.1", "### B.2"))
    b2 = _rows(_section(doc, "### B.2", "### B.3"))
    missing = [k for k in pf if k not in b1] + [k for k in ob if k not in b2]
    if missing:
        return "FAIL", f"gate keys without a section B row: {missing}"
    return "PASS", f"{len(pf)} _PF_GATE_ENVS + {len(ob)} _OB_GATE_ENVS keys present"


def case_r3(root=ROOT):
    doc = _committed(root)
    rows = _rows(_section(doc, "## A.", "## B."))
    problems = []
    for key in ("TFB_APP_VERSION", "APP_VERSION"):
        cells = rows.get(key)
        if not cells:
            problems.append(f"{key}: no row")
            continue
        if cells[1] != MUST_UNSET or cells[-1] != MUST_UNSET:
            problems.append(f"{key}: contract={cells[1]!r} expected={cells[-1]!r}")
    if "5.111.0" not in _section(doc, "## A.", "## B."):
        problems.append("section A does not cite the 13 Oct /health app_version 5.111.0")
    if problems:
        return "FAIL", "; ".join(problems)
    return "PASS", "TFB_APP_VERSION and APP_VERSION rows say MUST be unset"


def case_r4(root=ROOT):
    doc = _committed(root)
    b3 = _rows(_section(doc, "### B.3", "## C."))
    c = _rows(_section(doc, "## C.", "## D."))
    flags = _module_assign(root, "scripts/verify_deployment.py", "FLAGS")
    workflow = set(_module_assign(root, "scripts/verify_deployment.py", "_WORKFLOW_SCOPED"))
    problems = [f"engine_gates.{k} has no B.3 row" for k in _engine_keys(root) if k not in b3]
    render_scoped = [f[0] for f in flags if f[0] not in workflow and not D7_NAME_RE.search(f[0])]
    problems += [f"{k} has no section C row" for k in render_scoped if k not in c]
    problems += [f"workflow-scoped {k} is in section C" for k in sorted(workflow) if k in c]
    if problems:
        return "FAIL", "; ".join(problems[:5])
    return "PASS", (f"{len(b3)} engine_gates rows; {len(render_scoped)} Render-scoped FLAGS; "
                    f"{len(workflow)} workflow-scoped kept out")


def secret_problems(doc: str):
    """What R5 objects to in a document (empty list when clean)."""
    problems = [label for label, pattern in SECRET_PATTERNS if pattern.search(doc)]
    names = sorted({n for n in KEY_NAME_RE.findall(doc) if D7_NAME_RE.search(n)})
    if names:
        problems.append(f"D7 key names present: {names[:5]}")
    for token in LONG_TOKEN_RE.findall(doc):
        if " " in token or KEY_NAME_RE.fullmatch(token) or SNAKE_RE.fullmatch(token):
            continue  # ENV names and code identifiers
        if (re.fullmatch(r"[A-Za-z0-9+/=_\-]{24,}", token) and re.search(r"[0-9]", token)
                and re.search(r"[a-z]", token) and re.search(r"[A-Z]", token)):
            problems.append(f"long opaque token ({len(token)} chars)")
    return problems


def case_r5(root=ROOT):
    problems = secret_problems(_committed(root))
    if problems:
        return "FAIL", "; ".join(problems)
    return "PASS", "no secret-looking value, no URL, no D7 key name"


def case_r6(root=ROOT):
    raw = (Path(root) / DOC).read_bytes() if (Path(root) / DOC).is_file() else None
    if raw is None:
        return "FAIL", f"{DOC} is missing"
    bad = [i for i, b in enumerate(raw) if b > 127]
    if bad:
        return "FAIL", f"{len(bad)} non-ASCII bytes, first at offset {bad[0]}"
    return "PASS", f"{len(raw)} bytes, all ASCII"


def case_r7(root=ROOT):
    committed = _committed(root)
    generated = _generated(root)
    if committed == generated:
        return "PASS", f"{DOC} equals the generator output"
    a, b = committed.splitlines(), generated.splitlines()
    first = next((i for i, (x, y) in enumerate(zip(a, b)) if x != y), min(len(a), len(b)))
    return "FAIL", (f"{DOC} is stale: regenerate with `{REGENERATE}` and commit the result "
                    f"(first difference at line {first + 1})")


PROBE_SENTINEL = "tfbprobe7f3c"


def case_r8(root=ROOT):
    baseline = _generated(root)
    tag = PROBE_SENTINEL
    probe = {"APP_VERSION": f"9.9.9-{tag}", "TFB_APP_VERSION": f"9.9.9-{tag}",
             "TFB_PF_ENABLED": "0", "TFB_PF_RESEARCH_VIEW": "enforce",
             "REDIS_URL": f"rediss://{tag}-user:{tag}-pass@{tag}.invalid:6379/0",
             "PYTHONHASHSEED": "7"}
    with mock.patch.dict(os.environ, probe):
        text = _load_generator(root).generate(Path(root))
    if text != baseline:
        return "FAIL", "the document changes with the environment"
    if tag in text:
        return "FAIL", "an environment value leaked into the document"
    return "PASS", "identical output with APP_VERSION, gates and REDIS_URL set"


def case_r9(root=ROOT):
    rc, out = _run_cli("--check", "--output", str(Path(root) / DOC))
    if rc != 0 or "OK:" not in out:
        return "FAIL", f"--check on the committed doc: rc={rc} {out.strip()[:160]}"
    tmp = Path(tempfile.mkdtemp(prefix="tfb_render_env_"))
    try:
        stale = tmp / "render_env_contract.md"
        stale.write_text(_committed(root).replace("MUST be unset", "may be set", 1), encoding="utf-8")
        rc, out = _run_cli("--check", "--output", str(stale))
        if rc != 1 or "DRIFT" not in out or REGENERATE not in out:
            return "FAIL", f"--check on a stale copy: rc={rc} {out.strip()[:160]}"
        rc, out = _run_cli("--check", "--output", str(tmp / "missing.md"))
        if rc != 1 or "is missing" not in out:
            return "FAIL", f"--check on a missing file: rc={rc} {out.strip()[:160]}"
        rc, out = _run_cli("--output", str(stale))
        if rc != 0 or "WARN" not in out:
            return "FAIL", f"default mode on a stale copy: rc={rc} {out.strip()[:160]}"
        if stale.read_text(encoding="utf-8") != _committed(root):
            return "FAIL", "default mode did not rewrite the stale copy"
        rc, out = _run_cli("--check", "--output", str(stale))
        if rc != 0:
            return "FAIL", f"--check after regeneration: rc={rc}"
    finally:
        shutil.rmtree(tmp, ignore_errors=True)
    return "PASS", "--check 0/1/1, default mode rewrites with WARN and exits 0"


def case_r10(root=ROOT):
    gen = _load_generator(root)
    tmp = Path(tempfile.mkdtemp(prefix="tfb_render_env_"))
    try:
        absent = gen.generate(Path(root), tmp / "absent.json")
        if NOT_RECORDED_LINE not in absent:
            return "FAIL", f"no file: preamble lacks {NOT_RECORDED_LINE!r}"
        b1 = _rows(_section(absent, "### B.1", "### B.2"))
        if any(cells[-1] != "not recorded" for cells in b1.values()):
            return "FAIL", "no file: a B.1 row is not 'not recorded'"
        owner = tmp / "render_env_expected.json"
        secretish = "Zq8" * 12
        # The reviewer's leak probes: none of these may reach the document.
        leaks = {
            "REDIS_URL": "redis-host.internal:6379",
            "TFB_PF_TRUST_GATE": "hunter2:pw",
            "TFB_T10_SHOCK_PCT": "-----" + "BEGIN",
            "TFB_PF_SWITCH_SCAN": "Zq8Zq8Zq8 Zq8Zq8Zq8 Zq8Zq8Zq8 Zq8Zq8",
            "TFB_PF_NULL_LEVELS": "abcd1234efgh5678ijkl",
            "TFB_PF_IDENTITY_GATE": "k=v",
        }
        owner.write_text(json.dumps({"recorded": "2026-10-13", "expected": dict(leaks, **{
            "TFB_PF_DD_EXIT": "observe",
            "TFB_PF_ENABLED": secretish,
            "TFB_PROBE_" + "AUTH_" + "TOKEN": "x",
            "APP_VERSION": "5.111.0",
        })}), encoding="utf-8")
        text = gen.generate(Path(root), owner)
        b1 = _rows(_section(text, "### B.1", "### B.2"))
        b2 = _rows(_section(text, "### B.2", "### B.3"))
        a = _rows(_section(text, "## A.", "## B."))
        problems = []
        if b1.get("TFB_PF_DD_EXIT", [""])[-1] != "`observe`":
            problems.append("plain value not shown")
        if secretish in text or "withheld" not in b1.get("TFB_PF_ENABLED", [""])[-1]:
            problems.append("secret-looking value not withheld")
        for name, value in leaks.items():
            row = a.get(name) or b1.get(name) or b2.get(name) or [""]
            if value in text or "withheld" not in row[-1]:
                problems.append(f"{name}: owner value not withheld ({row[-1]!r})")
        if a.get("REDIS_URL", [""])[-1] != "recorded (value withheld)":
            problems.append("REDIS_URL expected cell is not 'recorded (value withheld)'")
        if secret_problems(text):
            problems.append("R5 objects to the owner-file output: "
                            + "; ".join(secret_problems(text)))
        if "PROBE_AUTH" in text:
            problems.append("D7 name printed")
        if a.get("APP_VERSION", [""])[-1] != MUST_UNSET:
            problems.append("APP_VERSION expected cell overridden")
        if NOT_RECORDED_LINE in text or "recorded 2026-10-13" not in text:
            problems.append("preamble does not reflect the owner file")
        if problems:
            return "FAIL", "; ".join(problems)
    finally:
        shutil.rmtree(tmp, ignore_errors=True)
    return "PASS", ("absent -> not recorded; plain shown; secret, *_URL, leak probes and D7 "
                    "withheld; MUST be unset kept")


R12_SEEDS = {
    "credentials in a URL": "| `REDIS_URL` | x | `rediss://u:p@host.invalid:6379` |",
    "D7 key name": "| `TFB_" + "APP_" + "TOKEN` | x |",
    "long opaque token": "| `X` | `" + "Ab3" * 10 + "` |",
    "private key block": "-----BEGIN " + "PRIVATE KEY-----",
}


def case_r12():
    clean = _committed(ROOT)
    if secret_problems(clean):
        return "FAIL", "the committed document is not clean (see R5)"
    seeds = R12_SEEDS
    missed = [label for label, line in seeds.items()
              if not secret_problems(clean + "\n" + line + "\n")]
    if missed:
        return "FAIL", f"R5 check missed seeded: {missed}"
    return "PASS", f"{len(seeds)}/{len(seeds)} seeded secrets reported; committed doc clean"


SAI_FILE = "core/surface_action_invariants.py"
COMBO_FUNC = "env_combo_violations"
RULE_CODE_RE = re.compile(r"^([A-Z][A-Z0-9_]{3,}):")


def _function_env_names(tree, fn_name, seen=None):
    """ENV names a module function reads through string-literal calls to
    os.getenv or to module helpers, following plain calls in the same file."""
    seen = set() if seen is None else seen
    funcs = {n.name: n for n in tree.body if isinstance(n, ast.FunctionDef)}
    fn = funcs.get(fn_name)
    if fn is None or fn_name in seen:
        return []
    seen.add(fn_name)
    names = []
    for node in ast.walk(fn):
        if not isinstance(node, ast.Call):
            continue
        if node.args and isinstance(node.args[0], ast.Constant) and isinstance(node.args[0].value, str):
            value = node.args[0].value
            if KEY_NAME_RE.fullmatch(value) and value not in names:
                names.append(value)
        if isinstance(node.func, ast.Name) and node.func.id in funcs:
            for name in _function_env_names(tree, node.func.id, seen):
                if name not in names:
                    names.append(name)
    return names


def case_r13(root=ROOT):
    doc = _committed(root)
    sai_text = _read(root, SAI_FILE)
    if sai_text is None:
        return "FAIL", f"{SAI_FILE} is missing"
    sai_tree = ast.parse(sai_text)
    expected = _function_env_names(sai_tree, COMBO_FUNC)
    combo_fn = next((n for n in sai_tree.body
                     if isinstance(n, ast.FunctionDef) and n.name == COMBO_FUNC), None)
    codes = []
    for node in ast.walk(combo_fn) if combo_fn is not None else ():
        if isinstance(node, ast.Constant) and isinstance(node.value, str):
            match = RULE_CODE_RE.match(node.value)
            if match and match.group(1) not in codes:
                codes.append(match.group(1))
    if not expected or not codes:
        return "FAIL", f"{SAI_FILE} {COMBO_FUNC}() reads {expected}, codes {codes}; re-check this case"
    b3_text = _section(doc, "### B.3", "## C.")
    b3 = _rows(b3_text)
    row = b3.get("env_combo")
    problems = []
    if not row:
        problems.append("no env_combo row")
    else:
        missing = [n for n in expected if f"`{n}`" not in row[1]]
        if missing:
            problems.append(f"env_combo row lacks {missing}")
        if row[3] != "'ok'":
            problems.append(f"env_combo /health value when unset is {row[3]!r}, not 'ok'")
    claims = [k for k, cells in b3.items() if any("no ENV read" in c for c in cells)]
    if claims:
        problems.append(f"B.3 rows still claim 'no ENV read': {claims}")
    lines = b3_text.splitlines()
    for rule_code in codes:
        rule = [ln for ln in lines if ln.startswith(f"- `{rule_code}` holds when ")]
        if not rule or not any(n in rule[0] for n in expected):
            problems.append(f"cross-key rule {rule_code} is not printed with its ENV names")
    if problems:
        return "FAIL", "; ".join(problems)
    return "PASS", (f"env_combo names {len(expected)} ENV inputs, 'ok' when unset; "
                    f"rule(s) printed: {codes}")


def case_r14(root=ROOT):
    doc = _committed(root)
    c_text = _section(doc, "## C.", "## D.")
    c = _rows(c_text)
    flags = _module_assign(root, "scripts/verify_deployment.py", "FLAGS")
    workflow = set(_module_assign(root, "scripts/verify_deployment.py", "_WORKFLOW_SCOPED"))
    disarms = set(_module_assign(root, "scripts/verify_deployment.py", "DECLARED_DISARMS"))
    armed = {str(w) for w in _module_assign(root, "scripts/verify_deployment.py", "_ARMED")}
    problems = []
    if "`Kill switch` yes:" not in c_text or "DRIFT" not in c_text:
        problems.append("section C does not explain the Kill switch column")
    must_set = []
    for name, default, _meaning, kill in flags:
        if not kill or name in workflow or name in disarms or D7_NAME_RE.search(name):
            continue
        cells = c.get(name)
        if cells is None:
            problems.append(f"{name}: no section C row")
            continue
        if str(default).strip().lower() not in armed:
            must_set.append(name)
            if "unset reads DISARMED" not in cells[3]:
                problems.append(f"{name}: kill cell {cells[3]!r} hides the disarmed default")
        elif "armed when unset" not in cells[3]:
            problems.append(f"{name}: kill cell {cells[3]!r}")
    if problems:
        return "FAIL", "; ".join(problems[:5])
    return "PASS", f"{len(must_set)} kill switch(es) flagged as must-set in Render: {must_set}"


def case_r15(root=ROOT):
    gen = _load_generator(root)
    if not hasattr(gen, "secret_findings") or not hasattr(gen, "check_publishable"):
        return "FAIL", "the generator has no secret guard"
    clean = _committed(root)
    if gen.secret_findings(clean):
        return "FAIL", f"the guard objects to the committed doc: {gen.secret_findings(clean)}"
    missed = [label for label, line in R12_SEEDS.items()
              if not gen.secret_findings(clean + "\n" + line + "\n")]
    if missed:
        return "FAIL", f"the generator guard missed seeded: {missed}"
    seeded = clean + "\n" + R12_SEEDS["credentials in a URL"] + "\n"
    tmp = Path(tempfile.mkdtemp(prefix="tfb_render_env_"))
    try:
        target = tmp / "out.md"
        with mock.patch.object(gen, "render_markdown", lambda _data: (seeded, 0)), \
                mock.patch.object(sys, "stderr", new=_Capture()):
            rc_file = gen.main(["--root", str(root), "--output", str(target)])
            with mock.patch.object(sys, "stdout", new=_Capture()) as out, \
                    mock.patch.object(sys, "stderr", new=_Capture()) as err:
                rc_stdout = gen.main(["--root", str(root), "--output", "-"])
        if "refusing to emit" not in err.getvalue():
            return "FAIL", "the guard did not explain why it refused"
        if rc_file != 2 or target.exists():
            return "FAIL", f"file mode: rc={rc_file}, written={target.exists()}"
        if rc_stdout != 2 or "rediss://" in out.getvalue():
            return "FAIL", f"stdout mode: rc={rc_stdout}, leaked={'rediss://' in out.getvalue()}"
    finally:
        shutil.rmtree(tmp, ignore_errors=True)
    return "PASS", f"{len(R12_SEEDS)}/{len(R12_SEEDS)} seeds caught; exit 2 with nothing written or printed"


class _Capture:
    """A minimal stdout stand-in (keeps this harness stdlib-only)."""

    def __init__(self):
        self.parts = []

    def write(self, text):
        self.parts.append(text)
        return len(text)

    def flush(self):
        return None

    def getvalue(self):
        return "".join(self.parts)


GATE_CASES = (("R1", case_r1), ("R2", case_r2), ("R3", case_r3), ("R7", case_r7),
              ("R13", case_r13), ("R14", case_r14))


def case_r11():
    base = os.environ.get(ENV_BASE, "").strip()
    if not base:
        return "SKIP", f"{ENV_BASE} unset"
    base_root = Path(base)
    if not (base_root / "main.py").is_file():
        return "FAIL", f"{ENV_BASE}={base} is not a source copy (no main.py)"
    passed = []
    for label, case in GATE_CASES:
        try:
            status, _detail = case(base_root)
        except Exception:  # noqa: BLE001 - a crash on the base is a failure there
            status = "FAIL"
        if status != "FAIL":
            passed.append(label)
    if passed:
        return "FAIL", f"gate cases did not fail on the base copy: {passed}"
    return "PASS", f"{len(GATE_CASES)}/{len(GATE_CASES)} gate cases FAIL on the {BASE_COMMIT} copy"


CASES = (
    ("R1 generator is stdlib-only, offline and import-free", case_r1),
    ("R2 every _PF/_OB gate key has a section B row", case_r2),
    ("R3 TFB_APP_VERSION and APP_VERSION say MUST be unset", case_r3),
    ("R4 engine_gates and Render-scoped FLAGS complete", case_r4),
    ("R5 no secret-looking value or D7 key name", case_r5),
    ("R6 committed document is ASCII", case_r6),
    ("R7 committed document equals generator output", case_r7),
    ("R8 output independent of the environment", case_r8),
    ("R9 CLI --check and WARN-only default", case_r9),
    ("R10 expected values file handling", case_r10),
    ("R11 base golden negative", case_r11),
    ("R12 R5 check reports seeded secrets", case_r12),
    ("R13 env_combo names its ENV inputs and rule", case_r13),
    ("R14 section C flags kill switches that read disarmed", case_r14),
    ("R15 generator refuses a secret-bearing document", case_r15),
)


# ----------------------------------------------------------- pytest glue
if pytest is not None:
    @pytest.mark.parametrize("name,case", CASES, ids=[name.split()[0] for name, _ in CASES])
    def test_render_env_contract_case(name, case):
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
    note = f" ({skipped} skipped: {ENV_BASE} unset)" if skipped else ""
    if failed:
        print(f"FAIL {failed}/{ran} (PASS {passed}/{ran}){note}")
        return 1
    print(f"PASS {passed}/{ran}{note}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
