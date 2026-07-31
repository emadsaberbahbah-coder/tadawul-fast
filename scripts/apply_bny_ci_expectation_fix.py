#!/usr/bin/env python3
"""Update stale BK.US regression expectations to current BNY.US lifecycle identity."""
from pathlib import Path

PATH = Path("tests/test_recent_fixes.py")
text = PATH.read_text(encoding="utf-8")
replacements = {
    'assert clean == ["BK.US", "BRK-B.US", "FISV.US"]':
        'assert clean == ["BNY.US", "BRK-B.US", "FISV.US"]',
    '["AAPL", "BK.US", "MSFT", "BRK-B.US", "FISV.US"], 2':
        '["AAPL", "BNY.US", "MSFT", "BRK-B.US", "FISV.US"], 2',
    '== [["BK.US"], ["BRK-B.US"], ["FISV.US"], ["AAPL", "MSFT"]]':
        '== [["BNY.US"], ["BRK-B.US"], ["FISV.US"], ["AAPL", "MSFT"]]',
    'rows = [["BK.US", "Hanwha Aerospace Co., Ltd.", "NYSE", "USD", "USA", ""]]':
        'rows = [["BNY.US", "Hanwha Aerospace Co., Ltd.", "NYSE", "USD", "USA", ""]]',
}
for old, new in replacements.items():
    count = text.count(old)
    if count != 1:
        raise RuntimeError(f"Expected exactly one occurrence of {old!r}; found {count}")
    text = text.replace(old, new)
PATH.write_text(text, encoding="utf-8")
print("Updated stale BK.US regression expectations to BNY.US")
