"""Strict approval of current physical ML/MF memberships, without alias inference.

The default policy is a versioned repository file, never an environment or CLI
selection. Approval establishes membership only; it does not certify instrument
identity, acquisition, prices, freshness, names or custody. Importing this module
performs no policy read or provider/Sheet operation.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date
import hashlib
import json
from pathlib import Path
import re
from types import MappingProxyType
from typing import Any, Iterable, Mapping

SCRIPT_VERSION = "1.1.0"
SCHEMA = "tfb.approved-universe-scope/v1"
DEFAULT_SCOPE_PATH = Path(__file__).resolve().parents[1] / "config/approved_universe_scope_v1.json"
# v1.1.0 (2026-10-17, Codex brief G1 prepare): the market-page row floors the
# three auditors used to hard-code (Global_Markets 6512, Commodities_FX 453)
# live in ONE versioned file. market_floor() never raises: an unreadable or
# malformed file returns the caller's literal default, so every gate is
# byte-identical until the file changes (the D3 retirement edits the file).
DEFAULT_MARKET_FLOORS_PATH = Path(__file__).resolve().parents[1] / "config/market_floors_v1.json"
MARKET_FLOORS_SCHEMA = "tfb.market-floors/v1"


def market_floor(page: str, default: int, path: "str | Path | None" = None) -> int:
    """The configured row floor for a market page, else the literal default."""
    try:
        raw = json.loads(Path(path or DEFAULT_MARKET_FLOORS_PATH).read_text(encoding="utf-8"))
        if raw.get("schema") != MARKET_FLOORS_SCHEMA:
            return int(default)
        value = (raw.get("floors") or {}).get(page)
        return value if _integer(value) and value > 0 else int(default)
    except Exception:
        return int(default)
SYMBOL_HASH_DOMAIN = b"tfb.approved-symbol/v1\0"
_PAGE_CONTRACT = {
    "Market_Leaders": ("saudi_only", 255, 1025),
    "Mutual_Funds": ("current_fund_tab", 2474, 4496),
}
_HEX = re.compile(r"[0-9a-f]{64}\Z")
_REVISION = re.compile(r"[A-Za-z0-9][A-Za-z0-9._-]{0,95}\Z")
_TOP_KEYS = {"schema", "policy_revision", "approved_on", "approval_basis", "pages"}
_PAGE_KEYS = {"scope", "approved_count", "supersedes_min_rows",
              "symbol_fingerprints_sha256", "membership_sha256", "symbol_set_sha256"}
_MISMATCH_FIELDS = ("missing_fingerprints", "unexpected_fingerprints", "unexpected_symbols",
                    "duplicate_symbols", "bad_symbol_examples")


class ScopePolicyError(ValueError):
    """Missing, unreadable or invalid approval cannot establish a scope."""


def _integer(value: Any) -> bool:
    return type(value) is int


def _normalized_symbol(value: Any) -> str:
    if not isinstance(value, str):
        raise ScopePolicyError("Symbol cell must be a string")
    symbol = value.strip().upper()
    if not symbol or len(symbol) > 256:
        raise ScopePolicyError("Symbol cell is blank or exceeds the identifier bound")
    if any(not c.isprintable() or c.isspace() for c in symbol):
        raise ScopePolicyError("Symbol cell contains internal whitespace or control characters")
    return symbol


def symbol_fingerprint(raw_symbol: Any) -> str:
    """Hash strip+upper spelling exactly; never map bare or provider aliases."""
    symbol = _normalized_symbol(raw_symbol)
    return hashlib.sha256(SYMBOL_HASH_DOMAIN + symbol.encode("utf-8")).hexdigest()


def membership_sha256(fingerprints: Iterable[str]) -> str:
    """Digest a unique fingerprint set using the manifest's canonical format."""
    values = list(fingerprints)
    if any(not isinstance(v, str) or not _HEX.fullmatch(v) for v in values):
        raise ScopePolicyError("Membership fingerprints must be lowercase SHA256 strings")
    if len(set(values)) != len(values):
        raise ScopePolicyError("Membership fingerprints must be unique")
    return hashlib.sha256(("\n".join(sorted(values)) + "\n").encode("ascii")).hexdigest()


def symbol_set_sha256(raw_symbols: Iterable[Any]) -> str:
    """One digest of exact normalized spellings, also usable by native readers."""
    if isinstance(raw_symbols, (str, bytes, Mapping)):
        raise ScopePolicyError("Symbol set must be an iterable of physical row values")
    symbols = [_normalized_symbol(value) for value in raw_symbols]
    if len(set(symbols)) != len(symbols):
        raise ScopePolicyError("Symbol set must be unique after strip+upper normalization")
    return hashlib.sha256(("\n".join(sorted(symbols)) + "\n").encode("utf-8")).hexdigest()


@dataclass(frozen=True)
class PageScope:
    scope: str
    approved_count: int
    supersedes_min_rows: int
    symbol_fingerprints_sha256: frozenset[str]
    membership_sha256: str
    symbol_set_sha256: str

    def receipt(self) -> dict[str, Any]:
        return {"scope": self.scope, "approved_count": self.approved_count,
                "supersedes_min_rows": self.supersedes_min_rows,
                "expected_membership_sha256": self.membership_sha256,
                "expected_symbol_set_sha256": self.symbol_set_sha256}


@dataclass(frozen=True)
class ScopePolicy:
    schema: str
    policy_revision: str
    approved_on: str
    approval_basis: str
    policy_sha256: str
    pages: Mapping[str, PageScope]
    _manifest_bytes: bytes = field(default=b"", repr=False, compare=False)

    @property
    def manifest_sha256(self) -> str:
        return self.policy_sha256

    def receipt(self) -> dict[str, Any]:
        _check_policy(self)
        return {"schema": self.schema, "policy_revision": self.policy_revision,
                "policy_sha256": self.policy_sha256, "approved_on": self.approved_on,
                "pages": {p: self.pages[p].receipt() for p in _PAGE_CONTRACT}}


def _check_metadata(schema: Any, revision: Any, approved_on: Any, basis: Any) -> None:
    if schema != SCHEMA:
        raise ScopePolicyError("Unsupported approved scope schema")
    if not isinstance(revision, str) or not _REVISION.fullmatch(revision):
        raise ScopePolicyError("Approved scope needs a bounded policy revision")
    if not isinstance(approved_on, str) or not re.fullmatch(r"\d{4}-\d{2}-\d{2}", approved_on):
        raise ScopePolicyError("Approved scope needs a YYYY-MM-DD approval date")
    try:
        date.fromisoformat(approved_on)
    except ValueError as exc:
        raise ScopePolicyError("Approved scope approval date is invalid") from exc
    if (not isinstance(basis, str) or not basis.strip() or basis != basis.strip()
            or len(basis) > 1000 or not basis.isprintable()):
        raise ScopePolicyError("Approved scope needs a bounded approval basis")


def _check_page(page: str, value: PageScope) -> None:
    scope, count, old_floor = _PAGE_CONTRACT[page]
    if not isinstance(value, PageScope) or value.scope != scope:
        raise ScopePolicyError(f"{page}: invalid approved scope kind")
    if not _integer(value.approved_count) or value.approved_count != count:
        raise ScopePolicyError(f"{page}: approved count must be {count}")
    if not _integer(value.supersedes_min_rows) or value.supersedes_min_rows != old_floor:
        raise ScopePolicyError(f"{page}: superseded minimum must be {old_floor}")
    if not isinstance(value.symbol_fingerprints_sha256, frozenset):
        raise ScopePolicyError(f"{page}: fingerprint set is invalid")
    if len(value.symbol_fingerprints_sha256) != count:
        raise ScopePolicyError(f"{page}: fingerprint count does not match approval")
    digest = membership_sha256(value.symbol_fingerprints_sha256)
    if not isinstance(value.membership_sha256, str) or value.membership_sha256 != digest:
        raise ScopePolicyError(f"{page}: membership digest does not match fingerprints")
    if not isinstance(value.symbol_set_sha256, str) or not _HEX.fullmatch(value.symbol_set_sha256):
        raise ScopePolicyError(f"{page}: symbol set digest must be lowercase SHA256")


def _check_policy(policy: ScopePolicy) -> ScopePolicy:
    if not isinstance(policy, ScopePolicy):
        raise ScopePolicyError("Explicit approved scope must be a validated ScopePolicy")
    _check_metadata(policy.schema, policy.policy_revision, policy.approved_on, policy.approval_basis)
    if not isinstance(policy.policy_sha256, str) or not _HEX.fullmatch(policy.policy_sha256):
        raise ScopePolicyError("Approved scope manifest hash is invalid")
    if not isinstance(policy.pages, Mapping) or set(policy.pages) != set(_PAGE_CONTRACT):
        raise ScopePolicyError("Approved scope must contain exactly Market_Leaders and Mutual_Funds")
    for page in _PAGE_CONTRACT:
        _check_page(page, policy.pages[page])
    if (not isinstance(policy._manifest_bytes, bytes) or not policy._manifest_bytes
            or len(policy._manifest_bytes) > 1024 * 1024
            or hashlib.sha256(policy._manifest_bytes).hexdigest() != policy.policy_sha256):
        raise ScopePolicyError("Typed approved scope is not bound to loaded manifest bytes")
    original = _decode_manifest(policy._manifest_bytes)
    expected = {"schema": policy.schema, "policy_revision": policy.policy_revision,
                "approved_on": policy.approved_on, "approval_basis": policy.approval_basis,
                "pages": {page: {"scope": value.scope, "approved_count": value.approved_count,
                                  "supersedes_min_rows": value.supersedes_min_rows,
                                  "symbol_fingerprints_sha256": sorted(value.symbol_fingerprints_sha256),
                                  "membership_sha256": value.membership_sha256,
                                  "symbol_set_sha256": value.symbol_set_sha256}
                          for page, value in policy.pages.items()}}
    # Canonical JSON preserves the int/float/bool distinction unlike dict equality.
    if _canonical_json(original) != _canonical_json(expected):
        raise ScopePolicyError("Typed approved scope fields do not match loaded manifest bytes")
    return policy


def _json_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    out: dict[str, Any] = {}
    for key, value in pairs:
        if key in out:
            raise ScopePolicyError("Approved scope JSON contains a duplicate key")
        out[key] = value
    return out


def _reject_nonfinite(_: str) -> None:
    raise ScopePolicyError("Non-finite JSON values are not allowed")


def _decode_manifest(raw: bytes) -> Any:
    try:
        return json.loads(raw, object_pairs_hook=_json_object, parse_constant=_reject_nonfinite)
    except (ValueError, UnicodeDecodeError, RecursionError) as exc:
        raise ScopePolicyError("Approved universe scope JSON is invalid") from exc


def _canonical_json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False)


def load_scope(path: str | Path | None = None) -> ScopePolicy:
    """Load the repository approval; an explicit path is for programmatic fixtures."""
    source = DEFAULT_SCOPE_PATH if path is None else Path(path)
    try:
        raw = source.read_bytes()
    except OSError as exc:
        raise ScopePolicyError("Approved universe scope is missing or unreadable") from exc
    if not raw or len(raw) > 1024 * 1024:
        raise ScopePolicyError("Approved universe scope is empty or exceeds the file bound")
    data = _decode_manifest(raw)
    if not isinstance(data, dict) or set(data) != _TOP_KEYS:
        raise ScopePolicyError("Approved universe scope fields do not match the schema")
    _check_metadata(data["schema"], data["policy_revision"], data["approved_on"], data["approval_basis"])
    page_data = data["pages"]
    if not isinstance(page_data, dict) or set(page_data) != set(_PAGE_CONTRACT):
        raise ScopePolicyError("Approved scope must contain exactly Market_Leaders and Mutual_Funds")
    pages = {}
    for page, value in page_data.items():
        if not isinstance(value, dict) or set(value) != _PAGE_KEYS:
            raise ScopePolicyError(f"{page}: fields do not match the approved page schema")
        fingerprints = value["symbol_fingerprints_sha256"]
        if (not isinstance(fingerprints, list)
                or any(not isinstance(v, str) or not _HEX.fullmatch(v) for v in fingerprints)
                or fingerprints != sorted(fingerprints)
                or len(set(fingerprints)) != len(fingerprints)):
            raise ScopePolicyError(f"{page}: fingerprints must be sorted, unique lowercase SHA256 strings")
        pages[page] = PageScope(value["scope"], value["approved_count"],
                                value["supersedes_min_rows"], frozenset(fingerprints),
                                value["membership_sha256"], value["symbol_set_sha256"])
    policy = ScopePolicy(data["schema"], data["policy_revision"], data["approved_on"],
                         data["approval_basis"], hashlib.sha256(raw).hexdigest(),
                         MappingProxyType(pages), raw)
    return _check_policy(policy)


def _policy(policy: ScopePolicy | None) -> ScopePolicy:
    return load_scope() if policy is None else _check_policy(policy)


def approved_floors(*, policy: ScopePolicy | None = None) -> dict[str, int]:
    selected = _policy(policy)
    return {page: selected.pages[page].approved_count for page in _PAGE_CONTRACT}


def approved_count(page: str, *, policy: ScopePolicy | None = None) -> int | None:
    selected = _policy(policy)
    return selected.pages[page].approved_count if page in selected.pages else None


@dataclass(frozen=True)
class ScopeValidation:
    _receipt: Mapping[str, Any]

    @property
    def valid(self) -> bool:
        return self._receipt["valid"] is True

    @property
    def receipt(self) -> dict[str, Any]:
        return self.to_dict()

    def to_dict(self) -> dict[str, Any]:
        return {key: list(value) if isinstance(value, tuple) else value
                for key, value in self._receipt.items()}


def validate_symbols(page: str, raw_symbols: Iterable[Any], *,
                     policy: ScopePolicy | None = None, max_examples: int = 10) -> ScopeValidation:
    selected = _policy(policy)
    if page not in selected.pages:
        raise ScopePolicyError("Page is outside the approved ML/MF scope")
    if not _integer(max_examples) or not 0 <= max_examples <= 100:
        raise ScopePolicyError("Mismatch example bound must be an integer from 0 to 100")
    if raw_symbols is None or isinstance(raw_symbols, (str, bytes, Mapping)):
        raise ScopePolicyError("Actual Symbol cells must be an iterable of physical row values")
    expected = selected.pages[page]
    actual: dict[str, str] = {}
    duplicates: list[str] = []
    bad_examples: list[str] = []
    actual_count = bad_count = duplicate_count = 0
    try:
        iterator = iter(raw_symbols)
    except TypeError as exc:
        raise ScopePolicyError("Actual Symbol cells are not readable") from exc
    for position, raw in enumerate(iterator, 1):
        actual_count += 1
        try:
            symbol = _normalized_symbol(raw)
        except ScopePolicyError:
            bad_count += 1
            if len(bad_examples) < max_examples:
                bad_examples.append(f"Symbol row {position}: invalid {type(raw).__name__} cell")
            continue
        fingerprint = symbol_fingerprint(symbol)
        if fingerprint in actual:
            duplicate_count += 1
            if len(duplicates) < max_examples:
                duplicates.append(symbol)
        else:
            actual[fingerprint] = symbol
    actual_set = set(actual)
    missing = sorted(expected.symbol_fingerprints_sha256 - actual_set)
    unexpected = sorted(actual_set - expected.symbol_fingerprints_sha256)
    actual_digest = membership_sha256(actual_set)
    observed_symbols_digest = symbol_set_sha256(actual.values())
    valid = (actual_count == expected.approved_count == len(actual)
             and not (bad_count or duplicate_count or missing or unexpected)
             and actual_digest == expected.membership_sha256
             and observed_symbols_digest == expected.symbol_set_sha256)
    receipt = {"schema": selected.schema, "policy_revision": selected.policy_revision,
               "policy_sha256": selected.policy_sha256, "approved_on": selected.approved_on,
               "page": page, **expected.receipt(), "actual_count": actual_count,
               "actual_unique_count": len(actual), "actual_membership_sha256": actual_digest,
               "observed_symbol_set_sha256": observed_symbols_digest,
               "valid": valid, "missing_count": len(missing), "unexpected_count": len(unexpected),
               "duplicate_count": duplicate_count, "bad_symbol_count": bad_count,
               "missing_fingerprints": tuple(missing[:max_examples]),
               "unexpected_fingerprints": tuple(unexpected[:max_examples]),
               "unexpected_symbols": tuple(actual[v] for v in unexpected[:max_examples]),
               "duplicate_symbols": tuple(duplicates), "bad_symbol_examples": tuple(bad_examples)}
    return ScopeValidation(MappingProxyType(receipt))


def receipt_matches_policy(receipt: Any, *, policy: ScopePolicy | None = None) -> bool:
    """Require a complete successful membership receipt bound to this exact policy."""
    selected = _policy(policy)
    if not isinstance(receipt, Mapping) or receipt.get("valid") is not True:
        return False
    page = receipt.get("page")
    if not isinstance(page, str) or page not in selected.pages:
        return False
    expected = selected.pages[page]
    exact = {"schema": selected.schema, "policy_revision": selected.policy_revision,
             "policy_sha256": selected.policy_sha256, "approved_on": selected.approved_on,
             "page": page, **expected.receipt(),
             "actual_membership_sha256": expected.membership_sha256,
             "observed_symbol_set_sha256": expected.symbol_set_sha256}
    if any(receipt.get(key) != value for key, value in exact.items()):
        return False
    for key, value in {"approved_count": expected.approved_count,
                       "supersedes_min_rows": expected.supersedes_min_rows,
                       "actual_count": expected.approved_count,
                       "actual_unique_count": expected.approved_count,
                       "missing_count": 0, "unexpected_count": 0,
                       "duplicate_count": 0, "bad_symbol_count": 0}.items():
        if not _integer(receipt.get(key)) or receipt[key] != value:
            return False
    return all(isinstance(receipt.get(key), (list, tuple)) and len(receipt[key]) == 0
               for key in _MISMATCH_FIELDS)
