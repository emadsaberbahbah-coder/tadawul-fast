"""Quarantine exact reviewed debt IDs at final market publication boundaries.

Authority establishes security class, not the supplier quote's identity or a
fixed-income model. All quantitative and equity decision output is withheld.
Expired, invalid or conflicting review evidence cannot restore that output.
Manual portfolio inputs and every unlisted identifier remain unchanged.
"""
from __future__ import annotations

import hashlib
import json
from collections import Counter
from datetime import datetime, timedelta, timezone
from pathlib import Path
from urllib.parse import urlsplit

__version__ = "1.0.0"
REGISTRY_PATH = Path(__file__).resolve().parents[1] / "data/reviewed_instrument_classes.json"
PROTECTED_SYMBOLS = frozenset({"DCBG.US"})
PAGES = frozenset({"Global_Markets", "Mutual_Funds", "Market_Leaders", "Commodities_FX",
                   "Top_10_Investments", "My_Portfolio"})
TAG = "identity_quarantined:instrument_class; unsupported_instrument_model"
REASON = "Instrument model unsupported; supplier quote mapping unverified; equity analysis withheld."
_KEEP = frozenset({"symbol", "exchange", "currency", "country", "data_provider",
                   "provider_secondary", "last_updated_utc", "last_updated_riyadh", "row_source"})
_MANUAL = frozenset({"position_qty", "avg_cost", "position_cost", "buy_date", "target_weight", "user_notes"})


def _now_utc():
    return datetime.now(timezone.utc)


def _utc(value):
    result = datetime.fromisoformat(value)
    if result.tzinfo is None:
        raise ValueError("unverified review clock")
    return result.astimezone(timezone.utc)


def _digest(binding):
    payload = {key: value for key, value in binding.items() if key != "binding_sha256"}
    return hashlib.sha256(json.dumps(payload, sort_keys=True, separators=(",", ":"),
                                     ensure_ascii=False).encode()).hexdigest()


def _unique(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("duplicate review key")
        result[key] = value
    return result


def _load(path):
    raw = Path(path).read_bytes()
    if not raw or len(raw) > 16000:
        raise ValueError("invalid review size")
    data = json.loads(raw, object_pairs_hook=_unique)
    if (set(data) != {"schema", "bindings"} or data["schema"] != "tfb.reviewed-instrument-classes/v1"
            or not isinstance(data["bindings"], list) or len(data["bindings"]) != 1):
        raise ValueError("invalid class review")
    binding = data["bindings"][0]
    if (set(binding) != {"symbol", "name", "asset_class", "currency", "country", "exchange",
                          "reviewed_at_utc", "expires_at_utc", "sources", "quote_identity_verified",
                          "model_supported", "binding_sha256"}
            or binding["symbol"] != "DCBG.US" or binding["asset_class"] != "Bond"
            or not isinstance(binding["name"], str) or not 1 <= len(binding["name"]) <= 180
            or not binding["name"].isprintable()
            or binding["currency"] != "USD" or binding["country"] != ["USA", "United States"]
            or binding["exchange"] != ["NYSE", "NYSE/NASDAQ", "NASDAQ/NYSE"]
            or binding["quote_identity_verified"] is not False or binding["model_supported"] is not False
            or binding["binding_sha256"] != _digest(binding)):
        raise ValueError("invalid instrument binding")
    reviewed, expires = _utc(binding["reviewed_at_utc"]), _utc(binding["expires_at_utc"])
    if not timedelta(0) < expires - reviewed <= timedelta(days=7):
        raise ValueError("invalid review window")
    if not isinstance(binding["sources"], list) or len(binding["sources"]) != 2:
        raise ValueError("unverified authority sources")
    authority_urls = {"https://investors.dime.com/node/25311/html",
                      "https://investors.dime.com/news-releases/news-release-details/dime-transfer-new-york-stock-exchange/"}
    for source in binding["sources"]:
        url = urlsplit(source["url"])
        if (set(source) != {"url", "sha256", "captured_at_utc"}
                or url.scheme != "https" or url.hostname != "investors.dime.com"
                or source["url"] not in authority_urls or url.username or url.password
                or url.port not in (None, 443) or url.query or url.fragment
                or len(source["sha256"]) != 64 or any(c not in "0123456789abcdef" for c in source["sha256"])
                or _utc(source["captured_at_utc"]) > reviewed):
            raise ValueError("unverified authority source")
    if {source["url"] for source in binding["sources"]} != authority_urls:
        raise ValueError("duplicate authority source")
    return binding, reviewed, expires, hashlib.sha256(raw).hexdigest()


def apply_reviewed_instrument_rows(page, rows, *, registry_path=None, now=None):
    """Copy exact target rows; registry failure still quarantines the known ID.

    Only caption/class require current matched authority. No price survives,
    including metadata-matched rows: the listing review does not certify the
    supplier mapping. Unknown internal extras are removed on protected rows;
    canonical market keys and safe acquisition provenance are retained.
    """
    if page not in PAGES:
        return rows, None
    if not isinstance(rows, (list, tuple)) or any(not isinstance(row, dict) for row in rows):
        raise ValueError("instrument class boundary requires dictionary rows")
    targeted = [row for row in rows if isinstance(row.get("symbol"), str)
                and row["symbol"] in PROTECTED_SYMBOLS]
    if not targeted:
        return rows, None
    from core.sheets.schema_registry import get_sheet_keys
    keys = get_sheet_keys("My_Portfolio" if page == "My_Portfolio" else "Global_Markets")
    stats = {"version": __version__, "quarantined": len(targeted), "captioned": 0,
             "quarantined_symbols": sorted({row["symbol"] for row in targeted}),
             "registry_status": "UNKNOWN", "map_sha256": None, "reasons": {},
             "evidence": "producer_instrument_quarantine", "live_readback": False}
    binding = None
    try:
        binding, reviewed, expires, digest = _load(REGISTRY_PATH if registry_path is None else registry_path)
        clock = _now_utc() if now is None else now
        if not isinstance(clock, datetime) or clock.tzinfo is None:
            raise ValueError("invalid clock")
        if not reviewed <= clock.astimezone(timezone.utc) < expires:
            raise ValueError("review outside valid window")
        stats.update(registry_status="VALID", map_sha256=digest)
    except (OSError, ValueError, TypeError, KeyError, AttributeError, OverflowError, RecursionError):
        binding = None
    counts = Counter(row.get("symbol") for row in targeted)
    output, reasons = list(rows), Counter()
    for index, row in enumerate(rows):
        if not isinstance(row.get("symbol"), str) or row["symbol"] not in PROTECTED_SYMBOLS:
            continue
        matched = bool(binding and counts[row["symbol"]] == 1
                       and row.get("currency") == binding["currency"]
                       and row.get("country") in binding["country"]
                       and row.get("exchange") in binding["exchange"]
                       and (row.get("name") in (None, "", binding["name"])))
        # Default is symbol/provenance only; conflicting identity metadata does
        # not become a certified current listing or a newly guessed caption.
        keep = _KEEP if matched else _KEEP - {"exchange", "currency", "country"}
        if page == "My_Portfolio":
            keep = keep | _MANUAL
        out = {key: row.get(key) if key in keep else None for key in keys}
        out.update(symbol=row["symbol"], investability_status="BLOCKED", final_action="DO_NOT_INVEST",
                   block_reason=REASON, recommendation_reason=REASON,
                   final_decision_basis="Reviewed instrument-class quarantine")
        if matched:
            out.update(name=binding["name"], asset_class=binding["asset_class"])
            stats["captioned"] += 1
        warning = row.get("warnings")
        warning = warning if isinstance(warning, str) else ""
        out["warnings"] = warning if TAG in warning else "; ".join(filter(None, (warning, TAG)))
        output[index] = out
        reasons["unsupported_model" if matched else "unverified_or_conflicting_identity"] += 1
    stats["reasons"] = dict(reasons)
    return output, stats


def apply_reviewed_instrument_matrix(page, headers, rows, **kwargs):
    """Use exactly the same protection for the native market writer."""
    if page not in PAGES - {"Top_10_Investments"}:
        return rows, None
    # Preserve unrelated writer behavior and avoid a full matrix copy when the
    # exact protected identifier is absent. Portfolio historically permits
    # other header shapes; this guard imposes no new contract on those writes.
    symbol_columns = [index for index, header in enumerate(headers or ()) if header == "Symbol"]
    if not any(isinstance(row, (list, tuple)) and any(
            index < len(row) and isinstance(row[index], str) and row[index] in PROTECTED_SYMBOLS
            for index in symbol_columns) for row in rows or ()):
        return rows, None
    if page == "My_Portfolio":
        from core.sheets.schema_registry import get_sheet_headers, get_sheet_keys
        keys = get_sheet_keys(page)
        if list(headers) != list(get_sheet_headers(page)) or any(len(row) != len(keys) for row in rows or ()):
            raise ValueError("Portfolio instrument guard requires the exact canonical matrix")
    else:
        from core.sheet_presentation import validate_market_sheet_rows
        keys = validate_market_sheet_rows(page, headers, rows)
    output, stats = apply_reviewed_instrument_rows(page, [dict(zip(keys, row)) for row in rows or ()], **kwargs)
    if stats is None:
        return rows, None
    # RAW values.update skips JSON null cells; explicit empty strings remove the
    # old unsupported quote/score from an existing row. Other symbols are exact.
    return [[("" if row.get(key) is None else row.get(key))
             if isinstance(row.get("symbol"), str) and row["symbol"] in PROTECTED_SYMBOLS
             else row.get(key) for key in keys] for row in output], stats


def reviewed_quote_rows(page, rows):
    """Guard cached public quote results, preserving portfolio manual inputs."""
    return apply_reviewed_instrument_rows("My_Portfolio" if page == "My_Portfolio" else "Global_Markets", rows)[0]
