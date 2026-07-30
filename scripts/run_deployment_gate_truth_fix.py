#!/usr/bin/env python3
"""Repair the temporary transformer's workflow-edit function, then execute it."""
from __future__ import annotations

import re
import runpy
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
TARGET = ROOT / "scripts/apply_deployment_gate_truth_fix.py"
text = TARGET.read_text(encoding="utf-8")

replacement = r'''def patch_workflow() -> None:
    path = ".github/workflows/python_batch_concurrency.yml"
    text = read(path)

    provider_path = "      - 'core/providers/eodhd_provider.py'\n"
    route_path = "      - 'routes/advanced_analysis.py'\n"
    count = text.count(provider_path)
    if count != 2:
        raise RuntimeError(f"workflow provider path: expected 2 matches, found {count}")
    text = text.replace(provider_path, provider_path + route_path)

    test_marker = (
        "      - 'tests/test_eodhd_http402_guard.py'\n"
        "      - '.github/workflows/python_batch_concurrency.yml'"
    )
    test_replacement = (
        "      - 'tests/test_eodhd_http402_guard.py'\n"
        "      - 'tests/test_truthful_failsoft_contract.py'\n"
        "      - '.github/workflows/python_batch_concurrency.yml'"
    )
    count = text.count(test_marker)
    if count != 2:
        raise RuntimeError(f"workflow truthful-test path: expected 2 matches, found {count}")
    text = text.replace(test_marker, test_replacement)

    text = replace_once(
        text,
        "            core/providers/eodhd_provider.py \\\n",
        "            core/providers/eodhd_provider.py \\\n            routes/advanced_analysis.py \\\n",
        label="workflow compile route",
    )
    text = replace_once(
        text,
        "            tests/test_eodhd_http402_guard.py\n",
        "            tests/test_eodhd_http402_guard.py \\\n            tests/test_truthful_failsoft_contract.py\n",
        label="workflow compile truthful test",
    )
    text = replace_once(
        text,
        "          python -m pytest -q tests/test_eodhd_http402_guard.py\n",
        "          python -m pytest -q tests/test_eodhd_http402_guard.py tests/test_truthful_failsoft_contract.py\n",
        label="workflow run truthful test",
    )
    text = text.replace(
        'to_eodhd_symbol("ADNOCDIST.AB") == "ADNOCDIST.ADX"',
        'to_eodhd_symbol("ADNOCDIST.AD") == "ADNOCDIST.ADX"',
    )
    text = text.replace(
        'to_yahoo_symbol("ADNOCDIST.ADX") == "ADNOCDIST.AB"',
        'to_yahoo_symbol("ADNOCDIST.ADX") == "ADNOCDIST.AD"',
    )
    text = text.replace(
        'provider_recovery_variants("BK.US") == ["BK.US", "BK"]',
        'provider_recovery_variants("BNY.US") == ["BNY.US", "BNY", "BK.US", "BK"]',
    )
    text = text.replace(
        'rule.requested_symbol == "BK.US"',
        'rule.requested_symbol == "BNY.US"',
    )
    text = text.replace(
        '[["BK.US", "The Bank of New York Mellon Corporation"',
        '[["BNY.US", "The Bank of New York Mellon Corporation"',
    )
    text = replace_once(
        text,
        '          assert eodhd_provider._err_indicates_provider_unhealthy("HTTP 402") is True\n',
        '          assert eodhd_provider._err_indicates_provider_unhealthy("HTTP 402") is True\n'
        '          os.environ.pop("TFB_EODHD_ENGINE_PATCH_BIND", None)\n'
        '          assert eodhd_provider._engine_patch_bind_enabled() is True\n',
        label="workflow EODHD patch-bind assertion",
    )
    text = replace_once(
        text,
        '          assert canonicalize_symbol("NZYM-B.CO") == "NSIS-B.CO"\n',
        '          assert canonicalize_symbol("NZYM-B.CO") == "NSIS-B.CO"\n'
        '          assert canonicalize_symbol("BK.US") == "BNY.US"\n',
        label="workflow BNY lifecycle assertion",
    )
    write(path, text)


def main() -> None:
'''

pattern = r"def patch_workflow\(\) -> None:\n.*?\n\ndef main\(\) -> None:\n"
updated, count = re.subn(
    pattern,
    lambda _match: replacement,
    text,
    count=1,
    flags=re.S | re.M,
)
if count != 1:
    raise RuntimeError(f"temporary transformer patch_workflow replacement failed: {count}")
TARGET.write_text(updated, encoding="utf-8")
runpy.run_path(str(TARGET), run_name="__main__")
