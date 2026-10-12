"""Exact approval changes scope floors without approving data or alias guesses."""
from __future__ import annotations

import copy
from dataclasses import replace
import hashlib
import json
from pathlib import Path
import subprocess
import sys

import pytest

from scripts import approved_universe_scope as scope


def scoped_symbols():
    """Synthetic full-size memberships, never copies of private workbook rows."""
    return {"Market_Leaders": [f"{1000 + i}.SR" for i in range(255)],
            "Mutual_Funds": [f"F{i:04d}.US" for i in range(2474)]}


def manifest_payload(symbols=None):
    """Shared strict test manifest; its two counts cannot be reduced by fixtures."""
    symbols = scoped_symbols() if symbols is None else symbols
    data = {"schema": "tfb.approved-universe-scope/v1",
            "policy_revision": "2026-10-12.synthetic-fixture.v1", "approved_on": "2026-10-12",
            "approval_basis": "Synthetic offline approval fixture; no financial authenticity claim",
            "pages": {}}
    for page, kind, count, old_floor in (
        ("Market_Leaders", "saudi_only", 255, 1025),
        ("Mutual_Funds", "current_fund_tab", 2474, 4496),
    ):
        fingerprints = sorted(scope.symbol_fingerprint(s) for s in symbols[page])
        data["pages"][page] = {"scope": kind, "approved_count": count,
                               "supersedes_min_rows": old_floor,
                               "symbol_fingerprints_sha256": fingerprints,
                               "membership_sha256": scope.membership_sha256(fingerprints),
                               "symbol_set_sha256": scope.symbol_set_sha256(symbols[page])}
    return data


def write_policy(tmp_path, payload=None):
    path = tmp_path / "scope.json"
    path.write_text(json.dumps(manifest_payload() if payload is None else payload, indent=2) + "\n")
    return path


@pytest.fixture
def approved(tmp_path):
    symbols = scoped_symbols()
    path = write_policy(tmp_path, manifest_payload(symbols))
    return scope.load_scope(path), symbols, path


def test_hash_protocol_has_explicit_domain_and_sorted_newline_sets():
    expected = hashlib.sha256(b"tfb.approved-symbol/v1\0AAPL.US").hexdigest()
    assert scope.symbol_fingerprint("  aapl.us\t") == expected
    assert scope.symbol_fingerprint("AAPL") != expected
    hashes = [scope.symbol_fingerprint("ZZZ.US"), expected]
    assert scope.membership_sha256(hashes) == hashlib.sha256(
        ("\n".join(sorted(hashes)) + "\n").encode("ascii")).hexdigest()
    assert scope.symbol_set_sha256([" zzz.us ", "aapl.us"]) == hashlib.sha256(
        b"AAPL.US\nZZZ.US\n").hexdigest()


def test_import_does_not_read_policy_or_connect():
    command = """
from pathlib import Path
import socket
def forbidden(*args, **kwargs):
    raise AssertionError('import attempted external I/O')
Path.read_bytes = forbidden
socket.socket.connect = forbidden
import scripts.approved_universe_scope
"""
    result = subprocess.run([sys.executable, "-c", command], cwd=Path(__file__).resolve().parents[1],
                            capture_output=True, text=True, timeout=10)
    assert result.returncode == 0, result.stderr


def test_valid_policy_is_immutable_and_receipt_binds_whole_bytes(approved):
    policy, _, path = approved
    assert policy.policy_sha256 == hashlib.sha256(path.read_bytes()).hexdigest()
    assert scope.approved_floors(policy=policy) == {"Market_Leaders": 255, "Mutual_Funds": 2474}
    assert scope.approved_count("Market_Leaders", policy=policy) == 255
    assert scope.approved_count("Global_Markets", policy=policy) is None
    assert policy.receipt()["pages"]["Market_Leaders"]["supersedes_min_rows"] == 1025
    with pytest.raises(TypeError):
        policy.pages["Market_Leaders"] = policy.pages["Mutual_Funds"]


def test_default_missing_policy_fails_closed_despite_env_selector_or_disable(monkeypatch, approved, tmp_path):
    policy, _, path = approved
    monkeypatch.setattr(scope, "DEFAULT_SCOPE_PATH", tmp_path / "missing.json")
    monkeypatch.setenv("TFB_APPROVED_UNIVERSE_SCOPE", str(path))
    monkeypatch.setenv("TFB_APPROVED_UNIVERSE_SCOPE_MODE", "off")
    monkeypatch.setenv("TFB_EXPECTED_MIN_ROWS_MARKET_LEADERS", "1")
    with pytest.raises(scope.ScopePolicyError, match="missing or unreadable"):
        scope.load_scope()
    with pytest.raises(scope.ScopePolicyError):
        scope.approved_floors()
    assert scope.approved_floors(policy=policy)["Market_Leaders"] == 255


@pytest.mark.parametrize("page", ["Market_Leaders", "Mutual_Funds"])
def test_order_case_and_outer_whitespace_preserve_exact_membership(approved, page):
    policy, symbols, _ = approved
    rows = [f"\t{s.lower()} " for s in reversed(symbols[page])]
    result = scope.validate_symbols(page, rows, policy=policy)
    receipt = result.to_dict()
    assert result.valid and scope.receipt_matches_policy(receipt, policy=policy)
    assert receipt["actual_count"] == receipt["actual_unique_count"] == receipt["approved_count"]
    assert receipt["observed_symbol_set_sha256"] == receipt["expected_symbol_set_sha256"]
    receipt["unexpected_symbols"].append("changed copy")
    assert not result.receipt["unexpected_symbols"]


@pytest.mark.parametrize("change,missing,unexpected,duplicates", [
    ("missing", 1, 0, 0), ("extra", 0, 1, 0), ("substitution", 1, 1, 0),
    ("duplicate_extra", 0, 0, 1), ("duplicate_substitution", 1, 0, 1),
    ("bare_alias", 1, 1, 0),
])
def test_membership_loss_addition_substitution_duplicates_and_aliases_fail(
        approved, change, missing, unexpected, duplicates):
    policy, symbols, _ = approved
    rows = list(symbols["Mutual_Funds"])
    if change == "missing":
        rows.pop()
    elif change == "extra":
        rows.append("NEW.US")
    elif change == "substitution":
        rows[0] = "NEW.US"
    elif change == "duplicate_extra":
        rows.append(rows[0].lower())
    elif change == "duplicate_substitution":
        rows[-1] = rows[0]
    else:
        rows[0] = rows[0].removesuffix(".US")
    receipt = scope.validate_symbols("Mutual_Funds", rows, policy=policy).to_dict()
    assert not receipt["valid"] and not scope.receipt_matches_policy(receipt, policy=policy)
    assert (receipt["missing_count"], receipt["unexpected_count"], receipt["duplicate_count"]) == (
        missing, unexpected, duplicates)


@pytest.mark.parametrize("cell", [None, True, 255, 255.0, float("nan"), float("inf"),
                                  {}, [], "", "A A.US", "A\0.US", "Z" * 257])
def test_invalid_physical_symbol_cells_fail_without_alias_or_type_guess(approved, cell):
    policy, symbols, _ = approved
    rows = list(symbols["Market_Leaders"])
    rows[0] = cell
    receipt = scope.validate_symbols("Market_Leaders", rows, policy=policy).to_dict()
    assert not receipt["valid"] and receipt["bad_symbol_count"] == receipt["missing_count"] == 1
    assert receipt["unexpected_symbols"] == []


def test_mismatch_details_are_bounded_without_hiding_counts(approved):
    policy, symbols, _ = approved
    rows = list(symbols["Market_Leaders"])
    rows[:20] = [f"EXTRA{i}.US" for i in range(20)]
    receipt = scope.validate_symbols("Market_Leaders", rows, policy=policy, max_examples=3).to_dict()
    assert receipt["missing_count"] == receipt["unexpected_count"] == 20
    for key in ("missing_fingerprints", "unexpected_fingerprints", "unexpected_symbols"):
        assert len(receipt[key]) == 3
    assert not scope.validate_symbols("Market_Leaders", rows, policy=policy, max_examples=0).valid


@pytest.mark.parametrize("field,value", [
    ("scope", "all_markets"), ("approved_count", True), ("approved_count", 255.0),
    ("approved_count", 254), ("approved_count", float("nan")),
    ("supersedes_min_rows", 1), ("supersedes_min_rows", 1025.0),
    ("membership_sha256", "0" * 64), ("membership_sha256", None),
    ("symbol_set_sha256", "bad"), ("symbol_set_sha256", "A" * 64),
])
def test_manifest_page_contract_is_strict(tmp_path, field, value):
    data = manifest_payload()
    data["pages"]["Market_Leaders"][field] = value
    with pytest.raises(scope.ScopePolicyError):
        scope.load_scope(write_policy(tmp_path, data))


@pytest.mark.parametrize("change", ["duplicate", "missing", "unsorted", "bad_hex", "uppercase"])
def test_manifest_fingerprint_set_cannot_be_ambiguous(tmp_path, change):
    data = manifest_payload()
    values = data["pages"]["Market_Leaders"]["symbol_fingerprints_sha256"]
    if change == "duplicate":
        values[0] = values[1]
    elif change == "missing":
        values.pop()
    elif change == "unsorted":
        values.reverse()
    elif change == "bad_hex":
        values[0] = "g" * 64
    else:
        values[0] = values[0].upper()
    with pytest.raises(scope.ScopePolicyError):
        scope.load_scope(write_policy(tmp_path, data))


@pytest.mark.parametrize("change", ["schema", "revision", "date", "basis", "extra_key",
                                     "extra_page", "missing_page", "missing_symbol_digest"])
def test_manifest_has_only_the_defined_versioned_two_page_policy(tmp_path, change):
    data = manifest_payload()
    if change == "schema":
        data["schema"] = "tfb.approved-universe-scope/v2"
    elif change == "revision":
        data["policy_revision"] = ""
    elif change == "date":
        data["approved_on"] = "2026-02-30"
    elif change == "basis":
        data["approval_basis"] = None
    elif change == "extra_key":
        data["symbols"] = ["plaintext identifiers must not be in the policy"]
    elif change == "extra_page":
        data["pages"]["Global_Markets"] = copy.deepcopy(data["pages"]["Mutual_Funds"])
    elif change == "missing_page":
        del data["pages"]["Mutual_Funds"]
    else:
        del data["pages"]["Market_Leaders"]["symbol_set_sha256"]
    with pytest.raises(scope.ScopePolicyError):
        scope.load_scope(write_policy(tmp_path, data))


def test_duplicate_json_keys_are_rejected(tmp_path):
    path = tmp_path / "scope.json"
    text = json.dumps(manifest_payload())
    path.write_text(text.replace('"schema":', '"schema": "other", "schema":', 1))
    with pytest.raises(scope.ScopePolicyError):
        scope.load_scope(path)


def test_second_symbol_digest_must_match_actual_rows_even_with_matching_fingerprints(tmp_path):
    data = manifest_payload()
    data["pages"]["Market_Leaders"]["symbol_set_sha256"] = "0" * 64
    policy = scope.load_scope(write_policy(tmp_path, data))
    receipt = scope.validate_symbols("Market_Leaders", scoped_symbols()["Market_Leaders"],
                                     policy=policy).to_dict()
    assert receipt["missing_count"] == receipt["unexpected_count"] == 0
    assert not receipt["valid"]


@pytest.mark.parametrize("field,value", [
    ("valid", 1), ("policy_sha256", "0" * 64), ("policy_revision", "old"),
    ("page", "Global_Markets"), ("approved_count", 254), ("actual_count", 255.0),
    ("actual_unique_count", 254), ("missing_count", 1), ("unexpected_count", 1),
    ("duplicate_count", 1), ("bad_symbol_count", 1),
    ("actual_membership_sha256", "0" * 64), ("observed_symbol_set_sha256", "0" * 64),
    ("expected_symbol_set_sha256", "0" * 64), ("unexpected_symbols", ["EXTRA.US"]),
])
def test_finalizer_receipt_cannot_hide_failures_or_change_policy(approved, field, value):
    policy, symbols, _ = approved
    receipt = scope.validate_symbols("Market_Leaders", symbols["Market_Leaders"], policy=policy).to_dict()
    assert scope.receipt_matches_policy(receipt, policy=policy)
    receipt[field] = value
    assert not scope.receipt_matches_policy(receipt, policy=policy)


def test_forged_typed_policy_fails_closed(approved):
    policy, _, _ = approved
    with pytest.raises(scope.ScopePolicyError):
        scope.approved_floors(policy=replace(policy, policy_sha256=True))


def test_typed_policy_cannot_approve_different_membership_under_original_manifest_hash(approved):
    policy, symbols, _ = approved
    altered = list(symbols["Market_Leaders"])
    altered[0] = "9999.SR"
    fingerprints = frozenset(scope.symbol_fingerprint(value) for value in altered)
    page = replace(policy.pages["Market_Leaders"], symbol_fingerprints_sha256=fingerprints,
                   membership_sha256=scope.membership_sha256(fingerprints),
                   symbol_set_sha256=scope.symbol_set_sha256(altered))
    pages = dict(policy.pages)
    pages["Market_Leaders"] = page
    changed = replace(policy, pages=pages)
    with pytest.raises(scope.ScopePolicyError, match="do not match loaded manifest bytes"):
        scope.validate_symbols("Market_Leaders", altered, policy=changed)


def test_typed_policy_metadata_and_numeric_types_are_bound_to_original_bytes(approved):
    policy, _, _ = approved
    with pytest.raises(scope.ScopePolicyError, match="do not match loaded manifest bytes"):
        scope.approved_floors(policy=replace(policy, policy_revision="2026-10-12.other.v1"))
    decoded = json.loads(policy._manifest_bytes)
    decoded["pages"]["Market_Leaders"]["approved_count"] = 255.0
    raw = json.dumps(decoded).encode()
    changed = replace(policy, _manifest_bytes=raw, policy_sha256=hashlib.sha256(raw).hexdigest())
    with pytest.raises(scope.ScopePolicyError, match="do not match loaded manifest bytes"):
        scope.approved_floors(policy=changed)
