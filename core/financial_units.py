"""Value-bound margin unit reads shared by ingestion and scoring."""
from __future__ import annotations

import math
from typing import Any, Callable, Dict, Mapping, Optional

__version__ = "1.0.0"
MARGIN_UNIT_KEY = "_margin_unit_basis"
MARGIN_FIELDS = ("gross_margin", "operating_margin", "profit_margin")


def _finite_number(value: Any) -> Optional[float]:
    if value is None or isinstance(value, bool):
        return None
    try:
        number = float(value)
    except (TypeError, ValueError, OverflowError):
        return None
    return number if math.isfinite(number) else None


def margin_unit_valid(row: Mapping[str, Any], field: str) -> Optional[str]:
    """A receipt proves its unit only for the exact current finite value."""
    basis = row.get(MARGIN_UNIT_KEY) if isinstance(row, Mapping) else None
    record = basis.get(field) if isinstance(basis, Mapping) else None
    if not isinstance(record, Mapping) or not isinstance(record.get("unit"), str) \
            or record.get("unit") not in {"fraction", "percent_points"}:
        return None
    value, witnessed = _finite_number(row.get(field)), _finite_number(record.get("value"))
    if value is None or witnessed is None or value != witnessed:
        return None
    return str(record["unit"])


def margin_value(
    row: Mapping[str, Any], field: str, value: Any, *, output_unit: str,
    legacy_parser: Callable[[Any], Optional[float]],
) -> Optional[float]:
    """Read the economic quantity without changing the source observation.

    Absent per-field receipts retain the established legacy parser. Explicit
    malformed, stale or unknown receipts cannot fall back to magnitude guesses.
    """
    if output_unit not in {"fraction", "percent_points"}:
        raise ValueError("unsupported margin output unit")
    basis = row.get(MARGIN_UNIT_KEY)
    if MARGIN_UNIT_KEY not in row or (isinstance(basis, Mapping) and field not in basis):
        return legacy_parser(value)
    unit = margin_unit_valid(row, field)
    number = _finite_number(value)
    if unit is None or number is None or number != _finite_number(row.get(field)):
        return None
    if unit == output_unit:
        return number
    return number / 100.0 if output_unit == "fraction" else number * 100.0


def margin_record_lineage(record: Mapping[str, Any]) -> Dict[str, Any]:
    """Whitelist raw supplier facts; callers first validate the landed value."""
    lineage: Dict[str, Any] = {}
    for key in ("provider", "raw_unit", "source_field", "transform_version"):
        value = record.get(key)
        if isinstance(value, str) and value:
            lineage[key] = value
    basis = record.get("unit_basis")
    if isinstance(basis, str) and basis in {
        "supplier_field_contract", "computed_ratio", "explicit_percent", "configured_adapter_contract",
    }:
        lineage["unit_basis"] = basis
    raw = _finite_number(record.get("raw_value"))
    if raw is not None:
        lineage["raw_value"] = raw
    return lineage
