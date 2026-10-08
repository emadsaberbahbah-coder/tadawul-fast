"""Copy-only instrument presentation for the canonical Sheets fraction schema.

This boundary certifies display units, not trading eligibility. Supplier facts,
scores, policies, prices and the caller's row remain unchanged. Unknown margins
and conflicting derived returns are withheld instead of guessed or recalculated.
"""
from __future__ import annotations

import math
import re
from typing import Any, Mapping

from core.financial_units import MARGIN_FIELDS, MARGIN_UNIT_KEY, margin_unit_valid

__version__ = "1.0.1"

_MARKET_SHEET_PAGES = frozenset({
    "Market_Leaders", "Global_Markets", "Mutual_Funds", "Commodities_FX",
})
_TRANSPORT_CLEAR_FIELDS = frozenset({
    *MARGIN_FIELDS, "upside_downside_pct", "upside_pct", "expected_roi_1m",
    "expected_roi_3m", "expected_roi_12m", "invest_period_label", "horizon_label",
})


def validate_market_sheet_rows(sheet_name, headers, rows):
    """Require the four market pages' exact current matrix before publication.

    Other pages retain their existing writer contract. Imports stay lazy so
    this presentation leaf remains safe in standalone and lean environments.
    """
    if sheet_name not in _MARKET_SHEET_PAGES:
        return None
    from core.sheets.schema_registry import get_sheet_headers, get_sheet_keys
    expected = list(get_sheet_headers(sheet_name))
    keys = list(get_sheet_keys(sheet_name))
    if (len(expected) != 115 or len(keys) != 115 or len(set(expected)) != 115
            or not isinstance(headers, (list, tuple)) or list(headers) != expected):
        raise ValueError("Market presentation requires the exact canonical 115-column header")
    if rows is not None and not isinstance(rows, (list, tuple)):
        raise ValueError("Market presentation requires a row matrix")
    if any(not isinstance(row, (list, tuple)) or len(row) != 115 for row in rows or ()):
        raise ValueError("Market presentation requires exact 115-column rows")
    return keys


def present_market_sheet_rows(sheet_name, headers, rows):
    """Copy market display values for Sheets' null-skip/empty-clear transport.

    The API serializer still uses None for unknowns. At the actual Sheets
    writer, only unknown presentation columns become explicit empty strings;
    prices, source observations, scoring and acquisition timestamps stay raw.
    """
    keys = validate_market_sheet_rows(sheet_name, headers, rows)
    if keys is None:
        return rows
    published = []
    for row in rows or ():
        displayed = present_instrument_row(dict(zip(headers, row)))
        published.append(["" if displayed.get(header) is None and key in _TRANSPORT_CLEAR_FIELDS
                          else displayed.get(header) for header, key in zip(headers, keys)])
    return published

_FIELDS = (
    *MARGIN_FIELDS, "current_price", "target_price", "upside_downside_pct",
    "intrinsic_value", "upside_pct", "forecast_price_1m", "forecast_price_3m",
    "forecast_price_12m", "expected_roi_1m", "expected_roi_3m", "expected_roi_12m",
    "horizon_days", "invest_period_label", "horizon_days_effective", "horizon_label",
    "warnings",
)


def _key(value: Any) -> str:
    return re.sub(r"[^a-z0-9]", "", str(value).lower())


_ALIASES = {_key(field): field for field in _FIELDS}
_ALIASES.update({"analysttargetprice": "target_price", "upsidedownside": "upside_downside_pct",
                 "upside": "upside_pct", "periodlabel": "invest_period_label",
                 "flags": "warnings", "warning": "warnings", "rowwarnings": "warnings"})


def _number(value: Any) -> float | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        number = float(value)
    except (TypeError, ValueError, OverflowError):
        return None
    return number if math.isfinite(number) else None


def _warning_parts(value: Any) -> list[str]:
    if isinstance(value, (list, tuple)):
        return [part.strip() for item in value for part in str(item).split(";") if part.strip()]
    return [part.strip() for part in str(value).split(";") if part.strip()] if value is not None else []


def _carry_unit(parts: list[str], field: str, value: Any) -> str | None:
    """Only an exact current value-bound display receipt can survive projection."""
    prefix = "sheet_margin_unit:" + field + ":"
    records = []
    for part in parts:
        if part.startswith(prefix):
            unit, separator, witness = part[len(prefix):].partition(":")
            witnessed = _number(witness)
            if not separator or unit not in {"fraction", "percent_points"} or witnessed is None:
                return None
            records.append((unit, witnessed))
    number = _number(value)
    if not records or number is None or len(set(records)) != 1 or records[0][1] != number:
        return None
    return records[0][0]


def _horizon_label(value: Any) -> str | None:
    days = _number(value)
    if days is None or days <= 0 or not days.is_integer():
        return None
    exact = {1: "1D", 7: "1W", 30: "1M", 90: "3M", 180: "6M", 365: "1Y"}
    return exact.get(int(days), str(int(days)) + "D")


def present_instrument_row(row: Mapping[str, Any]) -> dict[str, Any]:
    """Serialize display facts without mutating or re-evaluating the engine row.

    Margins require current supplier receipts or exact projected display carry.
    The canonical percentage schema uses fractions. No magnitude inference or
    legacy diagnostic tag establishes a unit. Return conflicts are withdrawn;
    this function never creates a modeled price, ROI, score or recommendation.
    """
    out = dict(row)
    columns: dict[str, list[str]] = {}
    for name in row:
        field = _ALIASES.get(_key(name))
        if field:
            columns.setdefault(field, []).append(name)
    if not columns.keys() & set(_FIELDS[:-1]):
        return out
    parts = list(dict.fromkeys(part for name in columns.get("warnings", [])
                               for part in _warning_parts(row[name])))
    added: list[str] = []

    def read(field: str) -> Any:
        values = [row[name] for name in columns.get(field, [])]
        if not values:
            return None
        first = values[0]
        if field not in {"invest_period_label", "horizon_label", "warnings"}:
            # Python considers True == 1 and False == 0. Validate every typed
            # alias before equivalence, so an invalid alias cannot certify a
            # numeric quantity or survive into the writer's last-key mapping.
            numbers = [_number(value) for value in values]
            if any(number is None for number in numbers) or len(set(numbers)) != 1:
                return None
        elif any(value != first for value in values[1:]):
            return None
        return first

    def write(field: str, value: Any) -> None:
        for name in columns.get(field, []):
            out[name] = value

    def populated(field: str) -> bool:
        return any(row[name] not in (None, "") for name in columns.get(field, []))

    price_columns = columns.get("current_price", [])
    duplicate_price_conflict = len(price_columns) > 1 and (
        read("current_price") is None
        or len({"" if row[name] is None else str(row[name]).strip() for name in price_columns}) > 1
    )
    acquisition_prices = [value for name, value in row.items()
                          if _key(name) in {"currentprice", "price", "lastprice"}
                          and value is not None and str(value).strip()]
    # Match the acquisition classifier's comma-tolerant numeric consensus
    # across differently named price aliases; duplicate current-price names
    # above retain its stricter exact-text consensus.
    price_numbers = [_number(str(value).strip().replace(",", ""))
                     if not isinstance(value, bool) else None for value in acquisition_prices]
    cross_price_conflict = len(acquisition_prices) > 1 and (
        any(number is None or number <= 0 for number in price_numbers)
        or len(set(price_numbers)) != 1
    )
    if duplicate_price_conflict or cross_price_conflict:
        # The acquisition classifier already rejects conflicting current-price
        # aliases. Strict projection must not erase that failure and certify a
        # canonical price just because its disagreeing alias was discarded.
        # Retain the raw prices and models; carry only the failed proof.
        added.extend(("sheet_quote_conflict:current_price", "acquisition_status:conflict"))

    witnessed = dict(row)
    for field in MARGIN_FIELDS:
        if field not in columns:
            continue
        value = read(field)
        witnessed[field] = value
        basis = row.get(MARGIN_UNIT_KEY)
        if MARGIN_UNIT_KEY in row:
            unit = margin_unit_valid(witnessed, field)
        else:
            unit = _carry_unit(parts, field, value)
        number = _number(value)
        prefix = "sheet_margin_unit:" + field + ":"
        parts = [part for part in parts if not part.startswith(prefix)]
        if number is None or unit is None:
            write(field, None)
            if populated(field):
                added.append("sheet_margin_unknown:" + field)
            continue
        fraction = number / 100.0 if unit == "percent_points" else number
        write(field, fraction)
        added.append(prefix + "fraction:" + repr(fraction))
        # The copy's private receipt remains valid if a caller reuses the copy
        # before strict projection. Raw lineage is preserved but never emitted.
        if isinstance(basis, Mapping):
            copied_basis = dict(out.get(MARGIN_UNIT_KEY) or {})
            copied_record = dict(basis.get(field) or {})
            copied_record.update(unit="fraction", value=fraction, published=True)
            copied_basis[field] = copied_record
            out[MARGIN_UNIT_KEY] = copied_basis

    for days_field, label_field in (("horizon_days", "invest_period_label"),
                                    ("horizon_days_effective", "horizon_label")):
        if label_field in columns:
            label = _horizon_label(read(days_field))
            write(label_field, label)
            if read(label_field) != label:
                added.append("sheet_horizon_label:" + label_field + ":" + (label or "unknown"))

    cp = _number(read("current_price"))
    for price_field, return_field in (
        ("target_price", "upside_downside_pct"), ("intrinsic_value", "upside_pct"),
        ("forecast_price_1m", "expected_roi_1m"), ("forecast_price_3m", "expected_roi_3m"),
        ("forecast_price_12m", "expected_roi_12m"),
    ):
        if return_field not in columns or not populated(return_field):
            continue
        price, actual = _number(read(price_field)), _number(read(return_field))
        if cp is None or cp <= 0 or price is None or price <= 0 or actual is None:
            write(return_field, None)
            added.append("sheet_tuple_unknown:" + return_field)
            continue
        implied = (price - cp) / cp
        # Compare the stored raw values, not their two-decimal display. A fixed
        # one-cent price allowance would accept arbitrary returns on tiny
        # crypto/FX prices. This permits existing six-decimal ROI rounding.
        tolerance = max(0.000005, abs(implied) * 0.000001)
        if not math.isfinite(implied) or not math.isfinite(tolerance):
            write(return_field, None)
            added.append("sheet_tuple_unknown:" + return_field)
            continue
        if abs(actual - implied) > tolerance:
            write(return_field, None)
            added.append("sheet_tuple_conflict:" + return_field)
        else:
            # RAW Sheets writes preserve strings as text. Publish the witnessed
            # existing numeric return after validation; never derive a new ROI.
            write(return_field, actual)

    if added:
        # Keep display receipts last on every pass; otherwise replacing their
        # value witness would reorder warnings on the second serialization.
        diagnostics = [part for part in added if not part.startswith("sheet_margin_unit:")]
        receipts = [part for part in added if part.startswith("sheet_margin_unit:")]
        warnings = "; ".join(dict.fromkeys(parts + diagnostics + receipts))
        if columns.get("warnings"):
            write("warnings", warnings)
        else:
            out["warnings"] = warnings
    return out
