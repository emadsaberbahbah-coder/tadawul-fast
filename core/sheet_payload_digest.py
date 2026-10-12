"""Typed full-cell RAW intent fingerprints, without changing published cells."""
from __future__ import annotations

import hashlib
import json
import math

__version__ = "1.0.0"
CANONICAL_METHOD = "tfb.sheets-raw-typed-binary64"
CANONICAL_VERSION = 1
_CONTRACT = f"{CANONICAL_METHOD}/v{CANONICAL_VERSION}"
_MARKET_PAGES = {"Market_Leaders", "Global_Markets", "Mutual_Funds", "Commodities_FX"}


class PayloadCanonicalizationError(ValueError):
    """A bounded evidence error code, never a cell value or private payload."""


def _cell(value):
    if value is None or (isinstance(value, str) and value == ""):
        return ["blank"]
    if isinstance(value, bool):
        return ["bool", value]
    if isinstance(value, str):
        return ["string", value]
    if isinstance(value, (int, float)):
        try:
            number = float(value)
        except (OverflowError, ValueError):
            raise PayloadCanonicalizationError("UNSAFE_INTEGER") from None
        if not math.isfinite(number):
            raise PayloadCanonicalizationError("NONFINITE_NUMBER")
        if isinstance(value, int) and int(number) != value:
            raise PayloadCanonicalizationError("UNSAFE_INTEGER")
        # RAW/native JSON can discard the zero sign. Both are numeric zero;
        # the token remains distinct from blank, False and the string "0".
        return ["number", (0.0 if number == 0 else number).hex()]
    raise PayloadCanonicalizationError("UNSUPPORTED_CELL_TYPE")


def typed_payload_sha256(headers, rows):
    """Hash exact ordered structure with only declared blank/number equivalences.

    Sheets documents double values, not universal 15-digit rounding. Finite
    nonzero binary64 values remain exact; integers must convert losslessly.
    Unicode strings are exact UTF-8, without trimming or normalization.
    """
    if (not isinstance(headers, (list, tuple)) or not headers
            or any(not isinstance(header, str) for header in headers)):
        raise PayloadCanonicalizationError("INVALID_HEADER")
    width = len(headers)
    if (not isinstance(rows, (list, tuple))
            or any(not isinstance(row, (list, tuple)) or len(row) != width for row in rows)):
        raise PayloadCanonicalizationError("INVALID_GEOMETRY")
    framed = {"contract": _CONTRACT,
              "headers": [["string", header] for header in headers],
              "rows": [[_cell(value) for value in row] for row in rows], "width": width}
    try:
        raw = json.dumps(framed, sort_keys=True, separators=(",", ":"),
                         ensure_ascii=False, allow_nan=False).encode("utf-8")
    except UnicodeError:
        raise PayloadCanonicalizationError("INVALID_UNICODE") from None
    return hashlib.sha256(raw).hexdigest()


def market_payload_evidence(sheet_name, headers, rows):
    """Prepare intent evidence; attach it only after a successful RAW update.

    Unsupported evidence never changes the actual payload or claims equality.
    None/empty-string equivalence describes blank state; a skipped RAW null can
    leave a stale native value, which only independent readback can detect.
    """
    if not isinstance(sheet_name, str) or sheet_name not in _MARKET_PAGES:
        return None
    evidence = {"canonical_method": CANONICAL_METHOD, "canonical_version": CANONICAL_VERSION,
                "canonical_width": len(headers) if isinstance(headers, (list, tuple)) else None,
                "canonical_rows": len(rows) if isinstance(rows, (list, tuple)) else None,
                "canonical_status": "UNVERIFIABLE", "canonical_verified": False}
    try:
        evidence["written_payload_sha256"] = typed_payload_sha256(headers, rows)
    except PayloadCanonicalizationError as error:
        evidence["canonical_error"] = str(error)
    except Exception:  # noqa: BLE001 -- bounded evidence failure cannot abort a RAW write
        # Evidence failure must not add a new failure to the actual RAW writer.
        # A missing full digest blocks independent acceptance, without leaking
        # an exception's potentially private cell content into the receipt.
        evidence["canonical_error"] = "CANONICALIZATION_ERROR"
    else:
        evidence.update(canonical_status="CANONICALIZED", canonical_verified=True)
    return evidence
