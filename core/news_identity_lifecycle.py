"""Reviewed legacy identifiers are evidence, never automatic ticker aliases.

Reuse the bounded, digested authority-caption registry. This module does not
delete source rows, fetch quotes, rewrite symbols or certify a successor.
"""
from __future__ import annotations

import re
from datetime import datetime, timezone

from core import verified_name_labels as labels


def load_news_lifecycle(*, now, registry_path=None):
    """Return exact venue-qualified legacy IDs and independent review status.

    An expired/future/invalid lifecycle declaration stays UNKNOWN. A malformed
    registry never establishes an empty exclusion set as a verified result.
    """
    try:
        bindings, digest = labels._load_registry(
            labels.REGISTRY_PATH if registry_path is None else registry_path)
        if not isinstance(now, datetime) or now.tzinfo is None:
            raise ValueError("unverified clock")
        now = now.astimezone(timezone.utc)
        entries = {}
        for (_, symbol), (item, reviewed, expires) in bindings.items():
            ids = item["identifiers"]
            if "news_lifecycle" not in ids:
                continue
            # No bare or fuzzy ticker match, even when a caption exists.
            if not re.fullmatch(r"[A-Z0-9][A-Z0-9_-]*\.[A-Z]{1,5}", symbol):
                raise ValueError("ambiguous lifecycle namespace")
            effective = labels._utc(ids["lifecycle_effective_utc"])
            successor = ids["successor_symbol"]
            if (ids["news_lifecycle"] != "LEGACY_SYMBOL"
                    or not re.fullmatch(r"[A-Z0-9][A-Z0-9_-]*\.[A-Z]{1,5}", successor)
                    or successor == symbol or effective > reviewed):
                raise ValueError("invalid lifecycle declaration")
            record = {"symbol": symbol, "historical_name": item["name"],
                      "status": "VERIFIED_LEGACY" if reviewed <= now < expires else "UNKNOWN",
                      "effective_utc": effective.isoformat(), "successor_symbol": successor,
                      "effective_time_precision": ids.get("lifecycle_time_precision", "instant"),
                      "reviewed_at_utc": reviewed.isoformat(), "expires_at_utc": expires.isoformat(),
                      "sources": item["sources"], "review_receipt_sha256": item["review_receipt_sha256"]}
            if symbol in entries and entries[symbol] != record:
                raise ValueError("conflicting lifecycle declarations")
            entries[symbol] = record
        return entries, {"status": "VALID", "map_sha256": digest}
    except (OSError, ValueError, TypeError, KeyError, OverflowError, RecursionError):
        return {}, {"status": "UNKNOWN", "map_sha256": None}
