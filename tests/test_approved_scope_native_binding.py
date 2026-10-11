"""Python and native source readers must enforce the same approved membership."""
import re
from pathlib import Path

from scripts.approved_universe_scope import load_scope


def test_native_scope_matches_the_authoritative_repository_policy():
    root = Path(__file__).resolve().parents[1]
    source = (root / 'apps_script/16_Decision_Top10.gs').read_text()
    declarations = re.findall(r'var DT10_SOURCE_APPROVED_SCOPE = (\{.*?\n\});', source, re.S)
    assert len(declarations) == 1
    declaration = declarations[0]
    policy = load_scope()
    assert re.findall(r"policy_revision:\s*'([^']+)'", declaration) == [policy.policy_revision]
    assert re.findall(r"approved_on:\s*'([^']+)'", declaration) == [policy.approved_on]
    records = re.findall(r"(Market_Leaders|Mutual_Funds):\s*\{approved_count:\s*(\d+),\s*"
                         r"supersedes_min_rows:\s*(\d+),\s*symbol_set_sha256:\s*'([0-9a-f]{64})'\}",
                         declaration)
    assert len(records) == 2
    assert len({page for page, *_ in records}) == 2
    for page, count, old_floor, digest in records:
        expected = policy.pages[page]
        assert (int(count), int(old_floor), digest) == (
            expected.approved_count, expected.supersedes_min_rows, expected.symbol_set_sha256)
