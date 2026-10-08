"""Pure account-scoped position and settled-cash checks; never writes or trades.

Evidence is a declared capture, not independently authenticated broker truth.
Exact instrument linkage and complete account coverage are supplied explicitly;
symbols, missing positions, fees, settlement and custody are never inferred.
Reuse execution_accounting's strict Decimal/time validation and fingerprints.
"""
from __future__ import annotations

from collections import Counter
import datetime as dt
from decimal import Decimal, localcontext
import math
import re
from typing import Any

from core.execution_accounting import (
    AccountingError, UTC, _amount, _currency, _number, _text, _timestamp,
    payload_fingerprint,
)

__version__ = "1.0.0"
SCHEMA_VERSION = 1
MAX_ACCOUNTS = 100
MAX_HOLDINGS = 500
MAX_POSITIONS = 10_000
MAX_EVIDENCE_BYTES = 1_000_000
DEFAULT_MAX_AGE_SECONDS = 900


def _token(value: Any) -> str:
    return re.sub(r"[^a-z0-9]", "", str(value).lower())


def _native_currency(value: Any) -> str:
    text = _text(value, "currency")
    # Minor quote units cannot be silently capitalized into a cash currency.
    if text != text.upper() or text in {"GBX", "ZAC", "ILA"}:
        raise AccountingError("ambiguous_currency")
    return _currency(text)


def _field(row: dict, names: set[str], parser, label: str):
    values = [parser(value, label) for key, value in row.items() if _token(key) in names]
    if not values or len(set(values)) != 1:
        raise AccountingError("missing or conflicting holding field")
    return values[0]


def _holding(row: Any) -> dict:
    if not isinstance(row, dict):
        raise AccountingError("invalid holding")
    return {
        "symbol": _field(row, {"symbol", "ticker"}, lambda value, label: _text(value, label).upper(), "symbol"),
        "currency": _field(row, {"currency", "nativecurrency", "currencycode"}, lambda value, _: _native_currency(value), "currency"),
        "quantity": _field(row, {"quantity", "qty", "shares", "units", "holdingqty", "positionqty"},
                           lambda value, label: _number(value, label, allow_zero=True), "quantity"),
    }


def _list(value: Any, cap: int) -> list:
    if not isinstance(value, list) or len(value) > cap:
        raise AccountingError("invalid or oversized evidence list")
    return value


def _fresh(value: Any, now: dt.datetime, captured: dt.datetime, max_age: int, code: str) -> dt.datetime:
    stamp = _timestamp(value, "evidence time")
    if stamp > captured or stamp > now:
        raise AccountingError("future_" + code)
    if (now - stamp).total_seconds() > max_age:
        raise AccountingError("stale_" + code)
    return stamp


def _clock(now: dt.datetime | None) -> dt.datetime:
    current = dt.datetime.now(UTC) if now is None else now
    if not isinstance(current, dt.datetime) or current.utcoffset() is None:
        raise AccountingError("invalid_clock")
    return current.astimezone(UTC)


def _empty(count: int, code: str) -> dict:
    return {
        "schema_version": SCHEMA_VERSION, "reconciliation_version": __version__,
        "status": "withheld", "evidence_origin": "declared_not_authenticated",
        "holdings_certified": False, "cash_certified": False,
        "funding_eligible": False, "certified_cash_available_sar": 0.0,
        "certified_cash_available_sar_exact": "0",
        "row_results": [{"row_index": index, "trusted": False, "status": code} for index in range(count)],
        "reason_counts": {code: max(1, count)}, "proposals": [],
    }


def certification_summary(certification: dict) -> dict:
    """Nonidentifying output metadata; excludes evidence, amounts and proposals."""
    return {key: certification.get(key) for key in (
        "schema_version", "reconciliation_version", "status", "evidence_origin",
        "holdings_certified", "cash_certified", "funding_eligible", "reason_counts",
    )} | {
        "rows": len(certification.get("row_results") or []),
        "trusted_rows": sum(row.get("trusted") is True for row in certification.get("row_results") or []),
    }


def certify_portfolio_inputs(holdings: list, evidence: Any, fx_rates: dict | None, *,
                             now: dt.datetime | None = None,
                             max_age_seconds: int = DEFAULT_MAX_AGE_SECONDS,
                             include_proposals: bool = False) -> dict:
    """Validate exact quantity/custody and funds available after reservations.

    Each account must declare a complete long-only positions snapshot. Every
    positive position in supplied accounts must link to exactly one holding;
    omitted instruments remain unknown, even in a complete snapshot. A link
    binds row_index, exact native symbol/currency, account_id, instrument_id
    and supplier position_symbol. Explicit zero positions can propose closure.

    Funding_accounts are explicit and nonempty. Each requires complete cash
    coverage, explicit settled_cash, reserved_cash, reservations_complete and
    exact FX supplied by the caller (SAR is exactly one). Proposed sales and
    unsettled balances never supplement settled cash. Any holding uncertainty
    withholds funding for the entire portfolio. Proposals are opt-in private
    reports only, not commands; neither fees nor realized P&L are guessed.
    """
    count = len(holdings) if isinstance(holdings, list) and len(holdings) <= MAX_HOLDINGS else 0
    try:
        current = _clock(now)
        if (isinstance(max_age_seconds, bool) or not isinstance(max_age_seconds, int)
                or not 1 <= max_age_seconds <= 86_400):
            raise AccountingError("invalid_age_limit")
        rows = _list(holdings, MAX_HOLDINGS)
        if (not isinstance(evidence, dict) or isinstance(evidence.get("schema_version"), bool)
                or evidence.get("schema_version") != SCHEMA_VERSION):
            raise AccountingError("missing_evidence")
        # A bounded canonical serialization validates nesting, NaN and types.
        from core.execution_accounting import _canonical
        if len(_canonical(evidence).encode()) > MAX_EVIDENCE_BYTES:
            raise AccountingError("oversized_evidence")
        _text(evidence.get("source_ref"), "source_ref")
        captured = _timestamp(evidence.get("captured_at"), "captured_at")
        _fresh(evidence["captured_at"], current, captured, max_age_seconds, "capture")
        normalized = [_holding(row) for row in rows]
        if len({(row["symbol"], row["currency"]) for row in normalized}) != len(normalized):
            raise AccountingError("duplicate_holdings")
        accounts = {}
        positions = {}
        position_times = {}
        position_errors = {}
        raw_accounts = _list(evidence.get("accounts"), MAX_ACCOUNTS)
        if not raw_accounts:
            raise AccountingError("missing_accounts")
        for account in raw_accounts:
            if not isinstance(account, dict):
                raise AccountingError("invalid_account")
            aid = _text(account.get("account_id"), "account_id")
            if aid in accounts:
                raise AccountingError("duplicate_accounts")
            accounts[aid] = account
            position_errors[aid] = None
            try:
                if account.get("positions_complete") is not True:
                    raise AccountingError("positions_incomplete")
                position_times[aid] = _fresh(account.get("positions_asof"), current, captured, max_age_seconds, "positions")
            except AccountingError as error:
                position_errors[aid] = str(error) if str(error) in {
                    "positions_incomplete", "future_positions", "stale_positions"} else "invalid_positions_time"
            for position in _list(account.get("positions"), MAX_POSITIONS):
                if not isinstance(position, dict):
                    raise AccountingError("invalid_position")
                if "account_id" in position and _text(position["account_id"], "position account_id") != aid:
                    raise AccountingError("conflicting_position_account")
                key = (aid, _text(position.get("instrument_id"), "instrument_id"))
                if key in positions:
                    raise AccountingError("duplicate_positions")
                positions[key] = {
                    "symbol": _text(position.get("symbol"), "position symbol").upper(),
                    "currency": _native_currency(position.get("currency")),
                    "quantity": _number(position.get("quantity"), "position quantity", allow_zero=True),
                }
        links = {}
        used_positions = set()
        for link in _list(evidence.get("holding_links"), MAX_HOLDINGS):
            if not isinstance(link, dict):
                raise AccountingError("invalid_link")
            index = link.get("row_index")
            if isinstance(index, bool) or not isinstance(index, int) or not 0 <= index < len(rows) or index in links:
                raise AccountingError("invalid_link_index")
            aid = _text(link.get("account_id"), "link account_id")
            key = (aid, _text(link.get("instrument_id"), "link instrument_id"))
            if key in used_positions:
                raise AccountingError("duplicate_linked_position")
            used_positions.add(key)
            if (_text(link.get("symbol"), "link symbol").upper() != normalized[index]["symbol"]
                    or _native_currency(link.get("currency")) != normalized[index]["currency"]):
                raise AccountingError("link_holding_mismatch")
            links[index] = (key, _text(link.get("position_symbol"), "position_symbol").upper())
        result = _empty(len(rows), "custody_unknown")
        reasons = Counter()
        proposals = []
        for index, row in enumerate(normalized):
            status = "custody_unknown"
            if index in links:
                key, supplier_symbol = links[index]
                position = positions.get(key)
                if key[0] not in accounts:
                    status = "custody_unknown"
                elif position_errors[key[0]]:
                    status = position_errors[key[0]]
                elif position is None:
                    status = "position_missing"
                elif position["symbol"] != supplier_symbol or position["currency"] != row["currency"]:
                    status = "position_identity_mismatch"
                elif position["quantity"] != row["quantity"]:
                    status = "position_closed" if position["quantity"] == 0 else "quantity_mismatch"
                    if include_proposals:
                        proposals.append({
                            "kind": "close_review" if position["quantity"] == 0 else "quantity_review",
                            "row_index": index, "account_id": key[0], "instrument_id": key[1],
                            "symbol": row["symbol"], "currency": row["currency"],
                            "original_row_sha256": payload_fingerprint(rows[index]),
                            "expected_quantity": _amount(row["quantity"]),
                            "observed_quantity": _amount(position["quantity"]),
                            "positions_asof": position_times[key[0]].isoformat().replace("+00:00", "Z"),
                            "application": "review_only", "fee_reconciliation": "unverified",
                        })
                else:
                    status = "matched" if row["quantity"] > 0 else "inactive_zero"
            trusted = status == "matched"
            result["row_results"][index] = {"row_index": index, "trusted": trusted, "status": status}
            if not trusted:
                reasons[status] += 1
        if any(position["quantity"] > 0 and key not in used_positions for key, position in positions.items()):
            reasons["unrepresented_positions"] += 1
        if any(position_errors.values()):
            for error in position_errors.values():
                if error and not reasons[error]:
                    reasons[error] += 1
        holdings_ok = not reasons
        cash_ok = True
        cash_total = Decimal(0)
        funding = _list(evidence.get("funding_accounts"), MAX_ACCOUNTS)
        if not funding or len(set(_text(item, "funding account") for item in funding)) != len(funding):
            raise AccountingError("invalid_funding_accounts")
        rates = {} if fx_rates is None else fx_rates
        if not isinstance(rates, dict):
            raise AccountingError("invalid_fx")
        proven_rates = {"SAR": Decimal(1)}
        for proof in _list(evidence.get("fx_rates", []), MAX_ACCOUNTS):
            if not isinstance(proof, dict):
                raise AccountingError("invalid_fx")
            ccy = _native_currency(proof.get("currency"))
            if ccy in proven_rates:
                raise AccountingError("duplicate_fx")
            _text(proof.get("source_ref"), "FX source_ref")
            _fresh(proof.get("asof"), current, captured, max_age_seconds, "fx")
            rate = _number(proof.get("rate_to_sar"), "FX")
            if _number(rates.get(ccy), "caller FX") != rate:
                raise AccountingError("fx_basis_mismatch")
            proven_rates[ccy] = rate
        if "SAR" in rates and _number(rates["SAR"], "SAR FX") != 1:
            raise AccountingError("invalid_fx")
        if any(row["currency"] not in proven_rates for row in normalized):
            cash_ok = False
            reasons["fx_unverified"] += 1
        for aid in funding:
            try:
                account = accounts.get(aid)
                if account is None:
                    raise AccountingError("cash_account_unknown")
                if account.get("cash_complete") is not True:
                    raise AccountingError("cash_incomplete")
                _fresh(account.get("cash_asof"), current, captured, max_age_seconds, "cash")
                cash_rows = _list(account.get("cash"), MAX_ACCOUNTS)
                if not cash_rows:
                    raise AccountingError("cash_missing")
                seen = set()
                for cash in cash_rows:
                    if not isinstance(cash, dict):
                        raise AccountingError("invalid_cash")
                    if "account_id" in cash and _text(cash["account_id"], "cash account_id") != aid:
                        raise AccountingError("conflicting_cash_account")
                    ccy = _native_currency(cash.get("currency"))
                    if ccy in seen:
                        raise AccountingError("duplicate_cash_currency")
                    seen.add(ccy)
                    settled = _number(cash.get("settled_cash"), "settled cash", allow_zero=True)
                    reserved = _number(cash.get("reserved_cash"), "reserved cash", allow_zero=True)
                    if cash.get("reservations_complete") is not True:
                        raise AccountingError("reservations_incomplete")
                    if reserved > settled:
                        raise AccountingError("cash_overreserved")
                    if ccy not in proven_rates:
                        raise AccountingError("fx_unverified")
                    rate = proven_rates[ccy]
                    with localcontext() as context:
                        context.prec = 512
                        cash_total += (settled - reserved) * rate
            except AccountingError as error:
                cash_ok = False
                code = str(error)
                reasons[code if code in {
                    "cash_account_unknown", "cash_incomplete", "stale_cash", "future_cash",
                    "cash_missing", "duplicate_cash_currency", "reservations_incomplete",
                    "cash_overreserved", "invalid_fx", "fx_unverified"} else "invalid_cash"] += 1
        available = float(cash_total) if holdings_ok and cash_ok else 0.0
        if not math.isfinite(available):
            raise AccountingError("invalid_cash_total")
        result.update({
            "status": "certified" if holdings_ok and cash_ok else "withheld",
            "holdings_certified": holdings_ok, "cash_certified": cash_ok,
            "funding_eligible": holdings_ok and cash_ok,
            "certified_cash_available_sar": available,
            "certified_cash_available_sar_exact": _amount(cash_total) if holdings_ok and cash_ok else "0",
            "reason_counts": dict(sorted(reasons.items())), "proposals": proposals,
        })
        return result
    except (AccountingError, TypeError, ValueError, OverflowError):
        # Never include source refs, account IDs, financial values or raw errors.
        return _empty(count, "invalid_evidence" if evidence is not None else "missing_evidence")
