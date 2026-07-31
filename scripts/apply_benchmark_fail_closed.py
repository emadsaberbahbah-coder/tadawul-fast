#!/usr/bin/env python3
"""Idempotently harden the live, no-write refresh deployment gate."""
from __future__ import annotations

from pathlib import Path

path = Path('.github/workflows/python_refresh_benchmark.yml')
text = path.read_text(encoding='utf-8')
original = text

old_exit_block = '''          if [[ "$code" -eq 2 ]]; then
            echo '::warning::Benchmark completed, but the production runner rejected the page; inspect identity/data-quality evidence.'
          elif [[ "$code" -eq 1 ]]; then
            echo '::warning::Benchmark completed with partial/skipped status; evidence retained.'
          fi
          exit 0
'''
new_exit_block = '''          if [[ "$code" -eq 2 ]]; then
            echo '::error::Benchmark completed, but the production runner rejected the page. Concurrency escalation is blocked.'
            exit 2
          fi

          if [[ "$code" -eq 1 ]]; then
            echo '::error::Benchmark evidence is incomplete or the full fresh-fetch gate did not pass. Concurrency escalation is blocked.'
            exit 1
          fi

          echo 'Benchmark deployment gate passed with complete evidence.'
          exit 0
'''

if old_exit_block in text:
    if text.count(old_exit_block) != 1:
        raise RuntimeError('legacy benchmark exit block is not unique')
    text = text.replace(old_exit_block, new_exit_block, 1)
else:
    required_markers = (
        'Concurrency escalation is blocked.',
        'exit 2',
        'exit 1',
        'Benchmark deployment gate passed with complete evidence.',
    )
    missing = [marker for marker in required_markers if marker not in text]
    if missing:
        raise RuntimeError(
            'benchmark workflow is neither legacy nor safely fail-closed; '
            f'missing markers: {missing}'
        )

legacy_probe = '          python scripts/verify_backend_symbol_capabilities.py \\\n'
sequential_probe = (
    '          python scripts/verify_backend_symbol_capabilities_sequential.py \\\n'
)
if legacy_probe in text:
    if text.count(legacy_probe) != 1:
        raise RuntimeError('legacy capability command is not unique')
    text = text.replace(legacy_probe, sequential_probe, 1)
elif text.count(sequential_probe) != 1:
    raise RuntimeError('sequential capability command not found exactly once')

# Scope timeout validation to the provider-capability step. The benchmark command
# also legitimately uses --timeout 120, so a global count is not a valid gate.
capability_heading = '      - name: Require deployed provider-symbol capabilities\n'
start = text.find(capability_heading)
if start < 0:
    raise RuntimeError('provider capability step not found')
end = text.find('\n      - name:', start + len(capability_heading))
if end < 0:
    raise RuntimeError('provider capability step boundary not found')
capability_block = text[start:end]
legacy_timeout = '            --timeout 60 \\\n'
per_probe_timeout = '            --timeout 120 \\\n'
if legacy_timeout in capability_block:
    if capability_block.count(legacy_timeout) != 1:
        raise RuntimeError('legacy capability timeout is not unique in its step')
    capability_block = capability_block.replace(
        legacy_timeout,
        per_probe_timeout,
        1,
    )
    text = text[:start] + capability_block + text[end:]
elif capability_block.count(per_probe_timeout) != 1:
    raise RuntimeError(
        '120-second per-probe timeout not found exactly once in capability step'
    )

path_anchor = "      - 'scripts/verify_backend_symbol_capabilities.py'\n"
new_path = "      - 'scripts/verify_backend_symbol_capabilities_sequential.py'\n"
if new_path not in text:
    if text.count(path_anchor) != 1:
        raise RuntimeError('capability workflow path anchor not found exactly once')
    text = text.replace(path_anchor, path_anchor + new_path, 1)

if "BENCH_CONCURRENCY: ${{ inputs.concurrency || '1' }}" not in text:
    raise RuntimeError('production benchmark default concurrency is no longer 1')
if '--concurrency "$BENCH_CONCURRENCY"' not in text:
    raise RuntimeError('benchmark no longer passes the bounded concurrency input')

if text != original:
    path.write_text(text, encoding='utf-8')
    print('Benchmark workflow hardened and sequential capability probe enabled.')
else:
    print('Benchmark workflow already satisfies the fail-closed sequential gate.')
