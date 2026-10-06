#!/usr/bin/env python3
"""tests/test_verify_manifest_pins.py - the deployment manifest can no longer
go stale silently.

WHY (2026-10-06)
----------------
scripts/verify_deployment.py carries a MODULES / SCRIPTS manifest of expected
version constants and reports DRIFT when the live tree disagrees. Its own
v1.0.10 note states the rule it lives by:

    "a stale manifest makes every drift report fiction"

and v1.0.17 through v1.0.28 are a chain of nine separate re-sync commits, each
one apologising for the same omission ("MY OMISSION, AND THE THIRD TIME: I
bumped three scripts in this session and pinned none of them").  On 2026-10-06
a full sweep found the manifest stale on 14 of its 27 entries, among them
opportunity_builder 1.15.1 vs 1.23.0, portfolio_actions 1.9.0 vs 1.14.0,
data_engine_v2 5.133.0 vs 5.151.0 and track_performance 6.34.0 vs 6.42.0 - so
the verifier had been reporting fiction on the decision layer for weeks.

Re-pinning by hand is what keeps failing. This test closes the loop
MECHANICALLY: it reads every pinned version straight out of the source file by
AST (no import, so no provider, credential or network side effect) and asserts
it equals the manifest's expectation. The next person who bumps a module and
forgets the pin gets a red test on the push, instead of a wrong VERDICT on the
pod weeks later.

It deliberately does NOT import verify_deployment as a module - that file
performs live checks at import in some code paths - the manifest tuples are
read out of its source by AST too.

Runs standalone (python tests/test_verify_manifest_pins.py) and under pytest.
"""
from __future__ import annotations

import ast
import sys
from pathlib import Path

_REPO = Path(__file__).resolve().parent.parent
_VERIFIER = _REPO / "scripts" / "verify_deployment.py"

# module dotted path -> file, for the MODULES manifest
_MODULE_FILES = {
    "core.compliance_gate": "core/compliance_gate.py",
    "core.shariah_authority": "core/shariah_authority.py",
    "core.corporate_actions": "core/corporate_actions.py",
    "core.quality_gates": "core/quality_gates.py",
    "core.regime": "core/regime.py",
    "core.risk_limits": "core/risk_limits.py",
    "core.validation": "core/validation.py",
    "core.regret": "core/regret.py",
    "core.scoring": "core/scoring.py",
    "core.enriched_quote": "core/enriched_quote.py",
    "core.analysis.opportunity_builder": "core/analysis/opportunity_builder.py",
    "core.analysis.portfolio_actions": "core/analysis/portfolio_actions.py",
    "routes.advanced_analysis": "routes/advanced_analysis.py",
    "routes.enriched_quote": "routes/enriched_quote.py",
    "core.analysis.top10_selector": "core/analysis/top10_selector.py",
    "core.providers.yahoo_chart_provider": "core/providers/yahoo_chart_provider.py",
    "core.providers.yahoo_fundamentals_provider": "core/providers/yahoo_fundamentals_provider.py",
    "core.providers.finnhub_provider": "core/providers/finnhub_provider.py",
    "core.providers.tadawul_provider": "core/providers/tadawul_provider.py",
    "core.data_engine_v2": "core/data_engine_v2.py",
}

# the SCRIPTS manifest checks SCRIPT_VERSION, falling back to __version__
# (the calendar_sync convention the verifier itself documents).
_SCRIPT_ATTRS = ("SCRIPT_VERSION", "__version__")


def _string_const(path: Path, name: str) -> str | None:
    """Last module-level `name = "<str>"` assignment in the file, by AST.

    Last-wins mirrors Python's own execution order, which matters here: a few
    of these files mention an older version in a docstring or a comment and
    assign the real constant further down.
    """
    tree = ast.parse(path.read_text(encoding="utf-8", errors="replace"))
    found: str | None = None
    for node in ast.walk(tree):
        if not isinstance(node, ast.Assign):
            continue
        if not (isinstance(node.value, ast.Constant) and isinstance(node.value.value, str)):
            continue
        for target in node.targets:
            if isinstance(target, ast.Name) and target.id == name:
                found = node.value.value
    return found


def _manifest() -> tuple[list[tuple], list[tuple]]:
    """The MODULES and SCRIPTS tuples, read out of the verifier's source."""
    tree = ast.parse(_VERIFIER.read_text(encoding="utf-8", errors="replace"))
    out: dict[str, list[tuple]] = {"MODULES": [], "SCRIPTS": []}
    for node in ast.walk(tree):
        targets = []
        if isinstance(node, ast.Assign):
            targets = node.targets
        elif isinstance(node, ast.AnnAssign):
            targets = [node.target]
        else:
            continue
        for target in targets:
            if isinstance(target, ast.Name) and target.id in out and node.value is not None:
                try:
                    out[target.id] = [tuple(x) for x in ast.literal_eval(node.value)]
                except Exception:  # noqa: BLE001 - a non-literal manifest is a real failure
                    out[target.id] = []
    return out["MODULES"], out["SCRIPTS"]


def test_manifest_is_parseable():
    modules, scripts = _manifest()
    assert len(modules) >= 15, f"MODULES manifest did not parse: {len(modules)} entries"
    assert len(scripts) >= 10, f"SCRIPTS manifest did not parse: {len(scripts)} entries"


def test_module_pins_match_head_constants():
    modules, _ = _manifest()
    drift = []
    for dotted, attr, expected, label in modules:
        rel = _MODULE_FILES.get(dotted)
        assert rel, f"{dotted} is pinned but this test has no file mapping for it"
        actual = _string_const(_REPO / rel, attr)
        if actual is None:
            # the verifier documents an alternate-attribute fallback
            for alt in ("__version__", "MODULE_VERSION", "PROVIDER_VERSION"):
                actual = _string_const(_REPO / rel, alt)
                if actual:
                    break
        if actual != expected:
            drift.append(f"{label} ({dotted}.{attr}): manifest {expected} != source {actual}")
    assert not drift, (
        "verify_deployment.py manifest is stale - a stale manifest makes every "
        "drift report fiction (its own v1.0.10 rule). Re-pin these:\n  "
        + "\n  ".join(drift)
    )


def test_script_pins_match_head_constants():
    _, scripts = _manifest()
    drift = []
    for name, expected, label in scripts:
        path = _REPO / "scripts" / f"{name}.py"
        assert path.is_file(), f"{name} is pinned but scripts/{name}.py does not exist"
        actual = None
        for attr in _SCRIPT_ATTRS:
            actual = _string_const(path, attr)
            if actual:
                break
        if actual != expected:
            drift.append(f"{label} (scripts/{name}.py): manifest {expected} != source {actual}")
    assert not drift, (
        "verify_deployment.py SCRIPTS manifest is stale. Re-pin these:\n  "
        + "\n  ".join(drift)
    )


if __name__ == "__main__":
    tests = [
        test_manifest_is_parseable,
        test_module_pins_match_head_constants,
        test_script_pins_match_head_constants,
    ]
    failed = 0
    for fn in tests:
        try:
            fn()
            print("PASS", fn.__name__)
        except AssertionError as exc:
            failed += 1
            print("FAIL", fn.__name__, "::", str(exc)[:1500])
    print("[MANIFEST PIN GUARD] %d/%d PASS" % (len(tests) - failed, len(tests)))
    sys.exit(1 if failed else 0)
