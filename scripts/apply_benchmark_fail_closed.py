#!/usr/bin/env python3
"""Validate the live no-write benchmark's fail-closed operating contract.

This file used to be a one-time workflow transformer. Reapplying a transformer
on every pull-request commit is unsafe and conflicts with intentional workflow
evolution. It is now a read-only validator: it makes no repository changes and
fails when the reviewed live-test boundaries drift.
"""
from __future__ import annotations

from pathlib import Path

WORKFLOW = Path(".github/workflows/python_refresh_benchmark.yml")
text = WORKFLOW.read_text(encoding="utf-8")

required_once = {
    "sequential capability command": (
        "python scripts/verify_backend_symbol_capabilities_sequential.py",
        1,
    ),
    "benchmark command": ("python scripts/benchmark_dashboard_fetch.py", 1),
    "concurrency input forwarding": ('--concurrency "$BENCH_CONCURRENCY"', 1),
    "explicit benchmark label": ("run-live-benchmark", 1),
    "serialized live-test group": ("group: live-read-only-refresh-", 1),
    "fail-closed runner rejection": (
        "production runner rejected the page. Concurrency escalation is blocked.",
        1,
    ),
    "fail-closed incomplete evidence": (
        "full fresh-fetch gate did not pass. Concurrency escalation is blocked.",
        1,
    ),
    "successful gate marker": (
        "Benchmark deployment gate passed with complete evidence.",
        1,
    ),
}

errors: list[str] = []
for label, (marker, expected_count) in required_once.items():
    actual = text.count(marker)
    if actual != expected_count:
        errors.append(
            f"{label}: expected marker count {expected_count}, found {actual}"
        )

required_markers = (
    "pull_request:\n    types: [labeled]",
    "github.event.label.name == 'run-live-benchmark'",
    "cancel-in-progress: false",
    "BENCH_CONCURRENCY: ${{ inputs.concurrency || '1' }}",
    "--timeout 120",
    "exit 3",
    "exit 2",
    "exit 1",
    "no_workbook_writes",
    "Transport HTTP 429",
    "Provider-level HTTP 402/404 markers are classified",
)
for marker in required_markers:
    if marker not in text:
        errors.append(f"missing required marker: {marker!r}")

for forbidden in (
    "verify_backend_symbol_capabilities.py \\",
    "cancel-in-progress: true",
    "::warning::Benchmark completed",
    "exit 0\n          fi\n          exit 0",
):
    if forbidden in text:
        errors.append(f"forbidden legacy marker remains: {forbidden!r}")

# Scope the per-probe timeout check to the capability step. The benchmark itself
# also legitimately uses 120 seconds, so a global exact count would be brittle.
heading = "      - name: Require deployed provider-symbol capabilities\n"
start = text.find(heading)
if start < 0:
    errors.append("provider capability step not found")
else:
    end = text.find("\n      - name:", start + len(heading))
    if end < 0:
        errors.append("provider capability step boundary not found")
    else:
        block = text[start:end]
        if block.count("--timeout 120") != 1:
            errors.append(
                "provider capability step must contain exactly one "
                "--timeout 120"
            )

# The live workflow must be label/dispatch initiated. Ordinary synchronize
# commits should run unit/contract CI only; otherwise the benchmark and
# diagnostics can compete for the same Render/provider capacity.
if "types: [synchronize" in text or "types: [opened" in text:
    errors.append("live benchmark may not auto-run on ordinary PR commits")

if errors:
    raise RuntimeError(
        "benchmark workflow contract invalid:\n- " + "\n- ".join(errors)
    )

print(
    "Benchmark workflow satisfies the explicit, serialized, no-write, "
    "fail-closed sequential gate."
)
