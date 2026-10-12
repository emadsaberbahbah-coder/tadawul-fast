"""Retain reviewed issuer captions on unpriced, non-actionable market rows.

This leaf changes only Name. It supplies no quote, instrument alias, listing,
custody, financial eligibility or current trading-status certification.
"""
from __future__ import annotations

import hashlib
import json
import re
from collections import Counter
from datetime import UTC, datetime, timedelta
from decimal import Decimal, InvalidOperation
from pathlib import Path
from urllib.parse import urlsplit

__version__ = "1.0.0"
REGISTRY_PATH = Path(__file__).resolve().parents[1] / "data" / "verified_name_labels.json"
_PAGES = {"Global_Markets", "Mutual_Funds"}
_METADATA = ("Asset Class", "Exchange", "Currency", "Country")
_REQUIRED = ("Symbol", "Name", *_METADATA, "Current Price", "Warnings",
             "Investability Status", "Final Action")
_BINDING_KEYS = {"page", "symbol", "name", "native_metadata", "requires_quarantine",
                 "state", "identifiers", "reviewed_at_utc", "expires_at_utc",
                 "sources", "review_receipt_sha256"}
_SHA = re.compile(r"[0-9a-f]{64}\Z")


def _text(value, *, blank=False, limit=256):
    return (isinstance(value, str) and len(value) <= limit
            and (blank or bool(value.strip())) and all(c.isprintable() for c in value))


def _utc(value):
    if not _text(value, limit=64):
        raise ValueError("invalid timestamp")
    result = datetime.fromisoformat(value)
    if result.tzinfo is None:
        raise ValueError("timestamp lacks timezone")
    return result.astimezone(UTC)


def _unique_object(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("duplicate registry key")
        result[key] = value
    return result


def _binding_digest(item):
    public = {key: value for key, value in item.items() if key != "review_receipt_sha256"}
    raw = json.dumps(public, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
    return hashlib.sha256(raw).hexdigest()


def _load_registry(path):
    raw = Path(path).read_bytes()
    if not raw or len(raw) > 256_000:
        raise ValueError("invalid registry size")
    data = json.loads(raw, object_pairs_hook=_unique_object)
    if (not isinstance(data, dict)
            or set(data) != {"schema", "label_only", "current_authority_only", "bindings"}
            or data["schema"] != "tfb.verified-name-labels/v1"
            or data["label_only"] is not True or data["current_authority_only"] is not True
            or not isinstance(data["bindings"], list) or not 1 <= len(data["bindings"]) <= 100):
        raise ValueError("invalid registry schema")
    bindings = {}
    for item in data["bindings"]:
        if not isinstance(item, dict) or set(item) != _BINDING_KEYS:
            raise ValueError("invalid binding shape")
        if (item["page"] not in _PAGES or not _text(item["symbol"], limit=64)
                or any(c.isspace() for c in item["symbol"])
                or not _text(item["name"]) or not _text(item["state"], limit=1024)
                or not isinstance(item["requires_quarantine"], bool)
                or not isinstance(item["native_metadata"], dict)
                or set(item["native_metadata"]) != set(_METADATA)
                or any(not _text(v, blank=True) for v in item["native_metadata"].values())
                or not isinstance(item["identifiers"], dict) or not item["identifiers"]
                or any(not _text(k, limit=64) or not _text(v, limit=256)
                       for k, v in item["identifiers"].items())
                or not isinstance(item["review_receipt_sha256"], str)
                or not _SHA.fullmatch(item["review_receipt_sha256"])):
            raise ValueError("invalid identity binding")
        metadata_blank = not any(item["native_metadata"].values())
        if not metadata_blank and any(not value for value in item["native_metadata"].values()):
            raise ValueError("partial unknown metadata is not an identity fence")
        if metadata_blank != item["requires_quarantine"]:
            raise ValueError("quarantine fence required for empty metadata")
        reviewed, expires = _utc(item["reviewed_at_utc"]), _utc(item["expires_at_utc"])
        if not timedelta(0) < expires - reviewed <= timedelta(days=7):
            raise ValueError("invalid review expiry")
        if not isinstance(item["sources"], list) or not 1 <= len(item["sources"]) <= 20:
            raise ValueError("invalid authority evidence")
        for source in item["sources"]:
            if (not isinstance(source, dict)
                    or set(source) != {"url", "sha256", "captured_at_utc"}
                    or not _text(source["url"], limit=2048)
                    or not isinstance(source["sha256"], str) or not _SHA.fullmatch(source["sha256"])):
                raise ValueError("invalid source receipt")
            url = urlsplit(source["url"])
            if (url.scheme != "https" or not url.hostname or url.username or url.password
                    or url.fragment or url.port not in {None, 443}
                    or any(s in url.query.lower() for s in ("token", "api_key", "apikey", "secret"))
                    or _utc(source["captured_at_utc"]) > reviewed):
                raise ValueError("invalid public authority receipt")
        key = (item["page"], item["symbol"])
        if item["review_receipt_sha256"] != _binding_digest(item):
            raise ValueError("public reviewed binding digest mismatch")
        if key in bindings:
            raise ValueError("duplicate identity binding")
        bindings[key] = (item, reviewed, expires)
    return bindings, hashlib.sha256(raw).hexdigest()


def _unpriced(value):
    if value is None or value == "":
        return True
    if isinstance(value, bool) or not isinstance(value, (int, float, str, Decimal)):
        return False
    if isinstance(value, str) and (not _text(value, limit=80) or value != value.strip()):
        return False
    try:
        number = Decimal(str(value))
        return number.is_finite() and number <= 0
    except (InvalidOperation, ValueError):
        return False


def apply_verified_name_labels(sheet_name, headers, rows, *, registry_path=None, now=None):
    """Return a Name-only copy plus producer counts; unsafe evidence is held.

    Invalid registry/schema/time never changes rows. The supplied clock is for
    deterministic tests; production uses the actual UTC publication clock.
    Other pages, especially manual portfolio fields, are outside this contract.
    """
    if not isinstance(sheet_name, str) or sheet_name not in _PAGES:
        return rows, None
    stats = {"version": __version__, "registry_status": "unavailable", "map_sha256": None,
             "rows": len(rows) if isinstance(rows, (list, tuple)) else 0,
             "matched": 0, "applied": 0, "held": 0, "skipped": 0,
             "reasons": {}, "applied_symbols": [], "evidence": "producer_name_only", "live_readback": False}
    try:
        bindings, digest = _load_registry(REGISTRY_PATH if registry_path is None else registry_path)
        stats.update(registry_status="valid", map_sha256=digest)
    except (OSError, ValueError, TypeError, OverflowError, RecursionError):
        stats["reasons"] = {"invalid_registry": stats["rows"]}
        stats["held"] = stats["rows"]
        return rows, stats
    now = datetime.now(UTC) if now is None else now
    if (not isinstance(now, datetime) or now.tzinfo is None
            or not isinstance(headers, (list, tuple)) or not isinstance(rows, (list, tuple))
            or any(headers.count(field) != 1 for field in _REQUIRED)
            or any(not isinstance(row, (list, tuple)) or len(row) != len(headers) for row in rows)):
        stats["reasons"] = {"invalid_matrix_or_clock": stats["rows"]}
        stats["held"] = stats["rows"]
        return rows, stats
    now = now.astimezone(UTC)
    columns = {field: headers.index(field) for field in _REQUIRED}
    counts = Counter(row[columns["Symbol"]] for row in rows
                     if isinstance(row[columns["Symbol"]], str))
    reasons, output = Counter(), list(rows)
    for i, row in enumerate(rows):
        symbol = row[columns["Symbol"]]
        binding = bindings.get((sheet_name, symbol)) if isinstance(symbol, str) else None
        if binding is None:
            stats["skipped"] += 1
            reasons["unregistered_symbol"] += 1
            continue
        item, reviewed, expires = binding
        stats["matched"] += 1
        reason = None
        if counts[symbol] != 1:
            reason = "duplicate_symbol"
        elif row[columns["Name"]] not in (None, ""):
            stats["skipped"] += 1
            reasons["existing_name"] += 1
            continue
        elif now < reviewed or now >= expires:
            reason = "outside_review_window"
        elif any((row[columns[field]] not in (None, "") if item["requires_quarantine"]
                  else not isinstance(row[columns[field]], str)
                  or row[columns[field]] != item["native_metadata"][field]) for field in _METADATA):
            reason = "metadata_conflict"
        elif not _unpriced(row[columns["Current Price"]]):
            reason = "priced_or_invalid_price"
        else:
            warnings, status, action = (row[columns[field]] for field in
                                        ("Warnings", "Investability Status", "Final Action"))
            tokens = warnings.split(";") if isinstance(warnings, str) else []
            tokens = [token.strip() for token in tokens]
            quarantined = any(token.startswith("identity_quarantined:") for token in tokens)
            blocked = status == "BLOCKED" and action == "DO_NOT_INVEST"
            low_trust = "low_data_trust" in tokens
            if (not isinstance(warnings, str) or not isinstance(status, (str, type(None)))
                    or not isinstance(action, (str, type(None)))
                    or status not in {None, "", "BLOCKED"}
                    or action not in {None, "", "DO_NOT_INVEST"}):
                reason = "actionable_or_invalid_status"
            elif item["requires_quarantine"] and not quarantined:
                reason = "quarantine_fence_missing"
            elif not (blocked or low_trust or quarantined):
                reason = "nonactionable_fence_missing"
        if reason:
            stats["held"] += 1
            reasons[reason] += 1
            continue
        named = list(row)
        named[columns["Name"]] = item["name"]
        output[i] = named
        stats["applied"] += 1
        stats["applied_symbols"].append(symbol)
    stats["reasons"] = dict(sorted(reasons.items()))
    return output if stats["applied"] else rows, stats
