#!/usr/bin/env python3
"""Generate a static TFB_/TRACK_ flag registry without importing production code.

VERSION 1.0.0 (2026-10-11, WP-12). Defaults are source observations, not live
environment state. Static consumers, deployment FLAGS and Actions env mappings
are reconciled independently. Dynamic names/defaults are disclosed as unresolved
instead of guessed. This tool never reads credentials or contacts a service.
"""
from __future__ import annotations

import argparse
import ast
from collections import defaultdict
import json
from pathlib import Path
import re


SCRIPT_VERSION = "1.0.0"
FLAG_RE = re.compile(r"^(?:TFB_|TRACK_)[A-Z0-9_]+$")
MAPPING_RE = re.compile(r"^\s+((?:TFB_|TRACK_)[A-Z0-9_]+):\s*(.*?)\s*$")
SOURCE_DIRS = ("core", "routes", "integrations", "scripts", "utils")


def _env_call(node, targets=None):
    if not isinstance(node, ast.Call):
        return False
    target = ast.unparse(node.func)
    return target in (targets or {"os.getenv", "os.environ.get", "environ.get", "getenv"})


def _literal(node):
    try:
        value = ast.literal_eval(node)
    except (ValueError, TypeError, SyntaxError):
        return "computed"
    if isinstance(value, (str, int, float, bool, type(None))):
        return repr(value)
    return "computed"


def scan_python(source, path):
    """Return literal flag consumers and unresolved dynamic env reads."""
    tree = ast.parse(source)
    parents = {child: parent for parent in ast.walk(tree) for child in ast.iter_child_nodes(parent)}
    targets = {"os.getenv", "os.environ.get", "environ.get", "getenv"}
    constants = {}
    for node in tree.body:
        if isinstance(node, ast.Import):
            for alias in node.names:
                if alias.name == "os":
                    module = alias.asname or "os"
                    targets.update({module + ".getenv", module + ".environ.get"})
        elif isinstance(node, ast.ImportFrom) and node.module == "os":
            for alias in node.names:
                name = alias.asname or alias.name
                if alias.name == "getenv":
                    targets.add(name)
                elif alias.name == "environ":
                    targets.add(name + ".get")
        if isinstance(node, (ast.Assign, ast.AnnAssign)):
            assignments = node.targets if isinstance(node, ast.Assign) else [node.target]
            if isinstance(node.value, ast.Constant) and isinstance(node.value.value, str):
                for target in assignments:
                    if isinstance(target, ast.Name):
                        constants[target.id] = node.value.value
    helpers = set()
    for function in ast.walk(tree):
        if isinstance(function, (ast.FunctionDef, ast.AsyncFunctionDef)):
            if any(_env_call(node, targets) for node in ast.walk(function)):
                helpers.add(function.name)
    # Call wrappers such as _env_flag around a dynamically named _env_bool.
    changed = True
    while changed:
        before = len(helpers)
        for function in ast.walk(tree):
            if isinstance(function, (ast.FunctionDef, ast.AsyncFunctionDef)):
                if any(isinstance(node, ast.Call) and isinstance(node.func, ast.Name)
                       and node.func.id in helpers for node in ast.walk(function)):
                    helpers.add(function.name)
        changed = len(helpers) != before
    consumers, dynamic = [], []
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call) or not node.args:
            continue
        direct = _env_call(node, targets)
        helper = isinstance(node.func, ast.Name) and (
            node.func.id in helpers or node.func.id.startswith(("_env_", "env_")))
        if not (direct or helper):
            continue
        name = node.args[0]
        resolved_name = name.value if isinstance(name, ast.Constant) else constants.get(name.id) if isinstance(name, ast.Name) else None
        if not isinstance(resolved_name, str):
            if direct:
                dynamic.append({"source": path, "line": node.lineno})
            continue
        if not FLAG_RE.fullmatch(resolved_name):
            continue
        default = _literal(node.args[1]) if len(node.args) > 1 else "None" if direct else "computed"
        for keyword in node.keywords:
            if keyword.arg == "default":
                default = _literal(keyword.value)
        parent = parents.get(node)
        if isinstance(parent, ast.BoolOp) and isinstance(parent.op, ast.Or):
            if parent.values[0] is node:
                default = _literal(parent.values[-1])
        consumers.append({"name": resolved_name, "default": default,
                          "source": path, "line": node.lineno})
    return consumers, dynamic


def manifest_flags(source):
    tree = ast.parse(source)
    for node in tree.body:
        if isinstance(node, (ast.Assign, ast.AnnAssign)):
            targets = node.targets if isinstance(node, ast.Assign) else [node.target]
            if any(isinstance(target, ast.Name) and target.id == "FLAGS" for target in targets):
                return {item[0]: {"default": item[1], "kill_switch": item[3]}
                        for item in ast.literal_eval(node.value)}
    return {}


def build_registry(root):
    root = Path(root)
    observations = defaultdict(list)
    dynamic = []
    paths = list(root.glob("*.py"))
    for directory in SOURCE_DIRS:
        paths.extend((root / directory).rglob("*.py"))
    for path in sorted(set(paths)):
        relative = path.relative_to(root).as_posix()
        if "harness_archive" in path.parts or path.name.startswith(("test_", "harness_")):
            continue
        consumers, unresolved = scan_python(path.read_text(encoding="utf-8"), relative)
        dynamic.extend(unresolved)
        for item in consumers:
            observations[item["name"]].append(item)
    manifest_path = root / "scripts" / "verify_deployment.py"
    manifest = manifest_flags(manifest_path.read_text(encoding="utf-8")) if manifest_path.exists() else {}
    mappings = defaultdict(list)
    launches = defaultdict(set)
    for path in sorted((root / ".github" / "workflows").glob("*.y*ml")):
        source = path.read_text(encoding="utf-8")
        for number, line in enumerate(source.splitlines(), 1):
            match = MAPPING_RE.match(line)
            if match:
                mappings[match[1]].append({"workflow": path.name, "line": number, "value": match[2]})
        for script in re.findall(r"\bscripts/([\w]+\.py)\b", source):
            launches["scripts/" + script].add(path.name)
    rows = []
    for name in sorted(observations.keys() | manifest.keys() | mappings.keys()):
        uses = observations[name]
        lanes = set()
        for item in uses:
            source = item["source"]
            if source.startswith("scripts/"):
                workflows = launches[source]
                lanes.add("Actions: " + ", ".join(sorted(workflows)) if workflows else "script/operator (no direct workflow launch found)")
            else:
                lanes.add("Render/library")
        rows.append({"name": name, "defaults": sorted({item["default"] for item in uses}),
                     "lanes": sorted(lanes), "consumers": uses,
                     "manifest": manifest.get(name), "mappings": mappings[name]})
    return {"schema_version": 1, "script_version": SCRIPT_VERSION,
            "flags": rows, "dynamic_reads": dynamic}


def render_markdown(registry):
    def cell(value):
        return str(value).replace("|", "&#124;").replace("\n", " ")
    lines = ["# Flag registry", "",
             "Generated from source by `python scripts/flag_registry.py --output docs/flags.md`.", "",
             "This is a static inventory, not a readback of Render or GitHub Variables. It lists literal "
             "TFB_/TRACK_ consumers, observed read/helper defaults, deployment-manifest declarations and "
             "workflow env mappings. Multiple defaults are preserved; computed defaults and dynamic "
             "names are not guessed. Read/helper defaults can be transformed by their parser; consult "
             "the linked source for the final runtime default. Render/library flags require dashboard "
             "configuration. A mapping "
             "anywhere in a workflow does not prove every launch lane maps it; lane-specific tests "
             "remain authoritative. Script imports and indirect launches can require manual lane review.", "",
             f"Discovered {len(registry['flags'])} flag names; "
             f"{len(registry['dynamic_reads'])} env reads use dynamic names (listed below).", "",
             "| Flag | Observed read/helper defaults | Consumer lane | Verifier | Workflow mappings | Source |",
             "| --- | --- | --- | --- | --- | --- |"]
    for row in registry["flags"]:
        sources = sorted({f"{item['source']}:{item['line']}" for item in row["consumers"]})
        mappings = [f"{item['workflow']}:{item['line']} `{item['value']}`" for item in row["mappings"]]
        verifier = "declared" if row["manifest"] is not None else "absent"
        if row["manifest"] is not None:
            verifier += f" (default {row['manifest']['default']!r})"
        values = (f"`{row['name']}`", ", ".join(row["defaults"]) or "no static consumer",
                  "; ".join(row["lanes"]) or "declaration/mapping only", verifier,
                  "; ".join(mappings) or "none", "; ".join(sources))
        lines.append("| " + " | ".join(cell(value) for value in values) + " |")
    lines.extend(("", "Dynamic env reads (helper definitions and constructed names require manual review):", ""))
    lines.extend(f"- `{item['source']}:{item['line']}`" for item in registry["dynamic_reads"])
    return "\n".join(lines) + "\n"


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=Path(__file__).resolve().parents[1])
    parser.add_argument("--output", type=Path)
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args(argv)
    registry = build_registry(args.root)
    text = json.dumps(registry, indent=2, sort_keys=True) + "\n" if args.json else render_markdown(registry)
    if args.output:
        args.output.write_text(text, encoding="utf-8")
        print(f"Wrote {args.output}: {len(registry['flags'])} static flag names")
    else:
        print(text, end="")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
