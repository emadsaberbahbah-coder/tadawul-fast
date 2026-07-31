#!/usr/bin/env python3
from pathlib import Path

path = Path('.github/workflows/python_refresh_benchmark.yml')
text = path.read_text(encoding='utf-8')
old = '''          if [[ "$code" -eq 2 ]]; then
            echo '::warning::Benchmark completed, but the production runner rejected the page; inspect identity/data-quality evidence.'
          elif [[ "$code" -eq 1 ]]; then
            echo '::warning::Benchmark completed with partial/skipped status; evidence retained.'
          fi
          exit 0
'''
new = '''          if [[ "$code" -eq 2 ]]; then
            echo '::error::Benchmark completed, but the production runner rejected the page; inspect identity/data-quality evidence.'
            exit 2
          elif [[ "$code" -eq 1 ]]; then
            echo '::error::Benchmark evidence is incomplete or the full fresh-fetch gate did not pass. Concurrency escalation is blocked.'
            exit 1
          fi
          exit 0
'''
if text.count(old) != 1:
    raise RuntimeError(f'Expected exactly one benchmark exit block; found {text.count(old)}')
text = text.replace(old, new, 1)
path.write_text(text, encoding='utf-8')
print('Benchmark workflow now fails closed on partial or incomplete evidence.')
