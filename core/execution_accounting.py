"""Offline, append-only execution replay. This module never applies ledger changes.

Reported commissions are broker fields, not a reconciled fee statement. A
net_amount field is retained raw and never supplies a fee-inclusive total.
The supported captured-feed contract treats an omitted commission/fee currency
as execution currency. Explicit commission_currency or fee_currency must be a
valid matching three-letter code; no cross-currency fee conversion is inferred.

The snapshot has a trades list (optionally inside response). Each trade needs
trade_id, symbol, currency, BUY/SELL side, size or quantity, price, trade_time
with seconds and timezone, and an explicit account context. IDs are supplied by
the capture, never generated. Monetary inputs should be strings or Decimal;
the CLI also parses JSON number literals exactly. No instrument alias mapping
is inferred: supplier and original-row symbols must agree.

Optional original_rows bind account_id and row_ref to a complete raw_row
object. cost_basis_references bind a witnessed total and source_ref to exact
account-scoped execution_keys; absent keys pin the matched BUY cohort on first
capture. amendment_requests require those keys, an original_row_ref and its
expected_original_sha256. Proposals freeze a reference source and raw hash (or
its absence). All amendments remain conditional review proposals.

Fingerprints establish internal consistency, not broker/source authenticity.
Callers must supply an authoritative complete original row and approve its
instrument linkage before any separate native application can be considered.
"""
from __future__ import annotations

from collections import defaultdict
from decimal import Decimal, InvalidOperation, localcontext
import datetime as dt
import hashlib
import json
import re
from typing import Any

__version__ = "1.0.0"
SCHEMA_VERSION = 1
MAX_RECORDS = 10_000
MAX_RAW_BYTES = 65_536
UTC = dt.timezone.utc


class AccountingError(ValueError):
    """Unproven or conflicting input; callers must preserve their previous state."""


def _text(value: Any, label: str) -> str:
    if not isinstance(value, str) or not value.strip() or len(value) > 512:
        raise AccountingError(f"{label} requires an explicit nonblank string")
    return value.strip()


def _number(value: Any, label: str, *, allow_zero: bool = False) -> Decimal:
    if isinstance(value, bool) or value is None:
        raise AccountingError(f"invalid {label}")
    try:
        number = Decimal(str(value).strip())
    except (InvalidOperation, ValueError):
        raise AccountingError(f"invalid {label}") from None
    if (not number.is_finite() or number.is_signed()
            or len(number.as_tuple().digits) > 80 or abs(number.adjusted()) > 80
            or number < 0 or (number == 0 and not allow_zero)):
        raise AccountingError(f"invalid {label}")
    return number


def _amount(number: Decimal) -> str:
    value = format(number, "f")
    return value.rstrip("0").rstrip(".") if "." in value else value


def _timestamp(value: Any, label: str) -> dt.datetime:
    text = _text(value, label)
    if not re.fullmatch(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d{1,6})?"
                        r"(?:Z|[+-]\d{2}:\d{2})", text):
        raise AccountingError(f"{label} requires a complete timestamp with timezone")
    try:
        return dt.datetime.fromisoformat(text.replace("Z", "+00:00")).astimezone(UTC)
    except ValueError:
        raise AccountingError(f"invalid {label}") from None


def _canonical(value: Any, depth: int = 0) -> str:
    """Canonical JSON that retains exact JSON numbers rather than float-rounding."""
    if depth > 20:
        raise AccountingError("raw payload nesting exceeds limit")
    if isinstance(value, dict):
        if any(not isinstance(key, str) for key in value):
            raise AccountingError("raw payload keys must be strings")
        return "{" + ",".join(json.dumps(key) + ":" + _canonical(value[key], depth + 1)
                               for key in sorted(value)) + "}"
    if isinstance(value, list):
        return "[" + ",".join(_canonical(item, depth + 1) for item in value) + "]"
    if isinstance(value, Decimal):
        if not value.is_finite() or abs(value.adjusted()) > 80 or len(value.as_tuple().digits) > 80:
            raise AccountingError("invalid number in raw payload")
        return str(value)
    try:
        return json.dumps(value, allow_nan=False, ensure_ascii=True)
    except (TypeError, ValueError, OverflowError):
        raise AccountingError("unsupported value in raw payload") from None


def payload_fingerprint(value: Any) -> str:
    return hashlib.sha256(_canonical(value).encode("utf-8")).hexdigest()


def _currency(value: Any) -> str:
    currency = _text(value, "currency").upper()
    if not re.fullmatch(r"[A-Z]{3}", currency):
        raise AccountingError("currency requires an explicit three-letter code")
    return currency


def _bounded_list(value: Any, label: str) -> list:
    if not isinstance(value, list) or len(value) > MAX_RECORDS:
        raise AccountingError(f"invalid or oversized {label}")
    return value


def _execution(raw: dict, account_id: str | None, mapping: dict, source: dict,
               now: dt.datetime) -> dict:
    if not isinstance(raw, dict):
        raise AccountingError("execution must be an object")
    trade_id = _text(raw.get("trade_id"), "trade_id")
    supplied = raw.get("account_id")
    mapped = mapping.get(trade_id)
    contexts = [_text(item, "account_id") for item in (supplied, account_id, mapped)
                if item is not None]
    if not contexts or len(set(contexts)) != 1:
        raise AccountingError("missing or conflicting explicit account context")
    account = contexts[0]
    symbol = _text(raw.get("symbol"), "symbol").upper()
    currency = _currency(raw.get("currency"))
    for field in ("commission_currency", "fee_currency"):
        if field in raw:
            try:
                fee_currency = _currency(raw[field])
            except AccountingError:
                raise AccountingError(f"invalid explicit {field}") from None
            if fee_currency != currency:
                raise AccountingError(f"explicit {field} disagrees with execution currency")
    side = _text(raw.get("side"), "side").upper()
    if side not in {"BUY", "SELL"}:
        raise AccountingError("side must be BUY or SELL")
    quantities = [_number(raw[key], "quantity") for key in ("size", "quantity") if key in raw]
    if not quantities or len(set(quantities)) != 1:
        raise AccountingError("missing or conflicting quantity")
    quantity = quantities[0]
    price = _number(raw.get("price"), "price")
    commission = None if raw.get("commission") is None else _number(
        raw["commission"], "commission", allow_zero=True)
    stamp = _timestamp(raw.get("trade_time"), "trade_time")
    if stamp > now:
        raise AccountingError("future execution timestamp")
    raw_json = _canonical(raw)
    if len(raw_json.encode("utf-8")) > MAX_RAW_BYTES:
        raise AccountingError("execution raw payload exceeds limit")
    order_id = raw.get("order_id")
    if order_id is not None:
        if isinstance(order_id, bool) or not isinstance(order_id, (str, int, Decimal)):
            raise AccountingError("invalid order_id")
        order_id = _text(str(order_id), "order_id")
    with localcontext() as context:
        context.prec = 512
        gross = quantity * price
    return {
        "account_id": account, "trade_id": trade_id, "order_id": order_id,
        "symbol": symbol, "currency": currency, "side": side,
        "trade_time_utc": stamp.isoformat().replace("+00:00", "Z"),
        "quantity": _amount(quantity), "price": _amount(price), "gross_native": _amount(gross),
        "reported_commission_native": None if commission is None else _amount(commission),
        "commission_status": "unreported" if commission is None else "broker_reported_unreconciled",
        "raw_payload_json": raw_json,
        "raw_payload_sha256": hashlib.sha256(raw_json.encode("utf-8")).hexdigest(),
        "source": source,
    }


def _merge(records: list, key, candidate: dict, label: str, *, ignore_source: bool = False) -> None:
    existing = next((item for item in records if key(item) == key(candidate)), None)
    if existing is None:
        if len(records) >= MAX_RECORDS:
            raise AccountingError(f"{label} state exceeds limit")
        records.append(candidate)
    else:
        a, b = dict(existing), dict(candidate)
        if ignore_source:
            a.pop("source", None)
            b.pop("source", None)
        if a != b:
            raise AccountingError(f"conflicting {label}; existing immutable record retained")


def _state_digest(state: dict) -> str:
    return payload_fingerprint({key: value for key, value in state.items() if key != "state_digest"})


def _source(source: Any) -> dict:
    if (not isinstance(source, dict) or set(source) != {"source_ref", "snapshot_sha256"}
            or not isinstance(source.get("snapshot_sha256"), str)
            or not re.fullmatch(r"[0-9a-f]{64}", source["snapshot_sha256"])):
        raise AccountingError("invalid captured source metadata")
    return {"source_ref": _text(source["source_ref"], "source_ref"),
            "snapshot_sha256": source["snapshot_sha256"]}


def _original_record(original: dict) -> dict:
    if not isinstance(original, dict) or not isinstance(original.get("raw_row"), dict):
        raise AccountingError("original row requires its complete raw object")
    raw_json = _canonical(original["raw_row"])
    if len(raw_json.encode()) > MAX_RAW_BYTES:
        raise AccountingError("original raw row exceeds limit")
    return {"row_ref": _text(original.get("row_ref"), "row_ref"),
            "account_id": _text(original.get("account_id"), "original account_id"),
            "raw_row_json": raw_json, "sha256": payload_fingerprint(original["raw_row"])}


def _execution_keys(keys: Any) -> list[dict]:
    keys = _bounded_list(keys, "execution_keys")
    if not keys or any(not isinstance(key, dict) or set(key) != {"account_id", "trade_id"} for key in keys):
        raise AccountingError("identified execution keys are required")
    normalized = [{"account_id": _text(key["account_id"], "execution account_id"),
                   "trade_id": _text(key["trade_id"], "execution trade_id")} for key in keys]
    if len({(key["account_id"], key["trade_id"]) for key in normalized}) != len(normalized):
        raise AccountingError("duplicate execution key")
    return sorted(normalized, key=lambda key: (key["account_id"], key["trade_id"]))


def _linked_events(keys: list[dict], executions: list[dict]) -> list[dict]:
    identities = {(key["account_id"], key["trade_id"]) for key in keys}
    events = [event for event in executions if (event["account_id"], event["trade_id"]) in identities]
    if len(events) != len(keys):
        raise AccountingError("execution linkage is unproven")
    return events


def _reference_key(reference: dict) -> tuple:
    return reference["account_id"], reference["symbol"], reference["currency"], reference["source_ref"]


def _reference_record(reference: dict, executions: list[dict], default_keys=None) -> dict:
    if not isinstance(reference, dict):
        raise AccountingError("invalid cost-basis reference")
    raw_json = _canonical(reference)
    if len(raw_json.encode()) > MAX_RAW_BYTES:
        raise AccountingError("cost-basis reference exceeds limit")
    result = {"account_id": _text(reference.get("account_id"), "reference account_id"),
            "symbol": _text(reference.get("symbol"), "reference symbol").upper(),
            "currency": _currency(reference.get("currency")),
            "quantity": _amount(_number(reference.get("quantity"), "reference quantity")),
            "total_cost_native": _amount(_number(reference.get("total_cost_native"), "total_cost_native")),
            "source_ref": _text(reference.get("source_ref"), "reference source_ref"),
            "raw_payload_json": raw_json, "raw_payload_sha256": payload_fingerprint(reference)}
    keys = reference.get("execution_keys", default_keys)
    if keys is None:
        keys = [{"account_id": event["account_id"], "trade_id": event["trade_id"]} for event in executions
                if (event["account_id"], event["symbol"], event["currency"], event["side"])
                == (result["account_id"], result["symbol"], result["currency"], "BUY")]
    result["execution_keys"] = _execution_keys(keys)
    events = _linked_events(result["execution_keys"], executions)
    if any((event["account_id"], event["symbol"], event["currency"], event["side"])
           != (result["account_id"], result["symbol"], result["currency"], "BUY") for event in events):
        raise AccountingError("cost reference execution linkage disagrees")
    with localcontext() as context:
        context.prec = 512
        quantity = sum((Decimal(event["quantity"]) for event in events), Decimal(0))
    if quantity != Decimal(result["quantity"]):
        raise AccountingError("total-cost reference quantity does not match identified buys")
    return result


def _validate_previous(previous: dict | None, now: dt.datetime) -> dict:
    if previous is None:
        return {"schema_version": SCHEMA_VERSION, "executions": [], "original_rows": [],
                "cost_basis_references": [], "amendment_proposals": []}
    if (not isinstance(previous, dict) or type(previous.get("schema_version")) is not int
            or previous.get("schema_version") != SCHEMA_VERSION
            or set(previous) != {"schema_version", "executions", "original_rows",
                                 "cost_basis_references", "amendment_proposals", "state_digest"}
            or previous.get("state_digest") != _state_digest(previous)):
        raise AccountingError("invalid replay state schema or digest")
    # Round-trip copies prevent a failed replay from mutating the caller's state.
    state = json.loads(json.dumps(previous))
    for label in ("executions", "original_rows", "cost_basis_references", "amendment_proposals"):
        _bounded_list(state[label], label)
    keys = set()
    for event in state["executions"]:
        if not isinstance(event, dict):
            raise AccountingError("invalid stored execution")
        try:
            raw = json.loads(event["raw_payload_json"], parse_float=Decimal)
            rebuilt = _execution(raw, event["account_id"], {}, _source(event["source"]), now)
        except (KeyError, TypeError, json.JSONDecodeError):
            raise AccountingError("invalid stored execution") from None
        key = (event["account_id"], event["trade_id"])
        if rebuilt != event or key in keys:
            raise AccountingError("invalid or duplicate stored execution")
        keys.add(key)
    keys = set()
    for original in state["original_rows"]:
        try:
            raw = json.loads(original["raw_row_json"], parse_float=Decimal)
            rebuilt = _original_record({"row_ref": original["row_ref"],
                                        "account_id": original["account_id"], "raw_row": raw})
        except (KeyError, TypeError, json.JSONDecodeError):
            raise AccountingError("invalid stored original row") from None
        if rebuilt != original or original["row_ref"] in keys:
            raise AccountingError("invalid or duplicate stored original row")
        keys.add(original["row_ref"])
    keys = set()
    for reference in state["cost_basis_references"]:
        try:
            rebuilt = _reference_record(json.loads(reference["raw_payload_json"], parse_float=Decimal),
                                        state["executions"], reference["execution_keys"])
            key = _reference_key(reference)
        except (KeyError, TypeError, json.JSONDecodeError):
            raise AccountingError("invalid stored cost-basis reference") from None
        if rebuilt != reference or key in keys:
            raise AccountingError("invalid or duplicate stored cost-basis reference")
        keys.add(key)
    _summaries(state)
    keys = set()
    for proposal in state["amendment_proposals"]:
        try:
            rebuilt = _proposal({"kind": proposal["kind"], "original_row_ref": proposal["original_row_ref"],
                                 "expected_original_sha256": proposal["original_sha256"],
                                 "execution_keys": proposal["execution_keys"],
                                 "cost_reference_source_ref": proposal["cost_reference_source_ref"]}, state)
        except (KeyError, TypeError):
            raise AccountingError("invalid stored amendment proposal") from None
        if rebuilt != proposal or proposal["proposal_id"] in keys:
            raise AccountingError("invalid or duplicate stored amendment proposal")
        keys.add(proposal["proposal_id"])
    return state


def _summaries(state: dict) -> list[dict]:
    groups = defaultdict(list)
    currencies = defaultdict(set)
    for event in state["executions"]:
        currencies[(event["account_id"], event["symbol"])].add(event["currency"])
        groups[(event["account_id"], event["symbol"], event["currency"], event["side"])].append(event)
    if any(len(values) != 1 for values in currencies.values()):
        raise AccountingError("same account/symbol has conflicting execution currencies")
    return [_summary(events, state["cost_basis_references"]) for _, events in sorted(groups.items())]


def _summary(events: list[dict], references: list[dict]) -> dict:
    first = events[0]
    keys = _execution_keys([{"account_id": event["account_id"], "trade_id": event["trade_id"]} for event in events])
    refs = [ref for ref in references if ref["execution_keys"] == keys]
    # Multiple witnessed totals remain separate reconciliations, never an
    # arbitrarily selected current total. Historic subsets cannot block appends.
    reference = refs[0] if len(refs) == 1 else None
    with localcontext() as context:
        context.prec = 512
        gross = sum((Decimal(event["gross_native"]) for event in events), Decimal(0))
        quantity = sum((Decimal(event["quantity"]) for event in events), Decimal(0))
        reported = [Decimal(event["reported_commission_native"]) for event in events
                    if event["reported_commission_native"] is not None]
        commission = sum(reported, Decimal(0))
        residual = None if reference is None else Decimal(reference["total_cost_native"]) - gross - commission
        return {
            "account_id": first["account_id"], "symbol": first["symbol"], "currency": first["currency"], "side": first["side"],
            "execution_count": len(events), "quantity": _amount(quantity), "gross_native": _amount(gross),
            "reported_commission_native": _amount(commission), "commission_reported_count": len(reported),
            "commission_unreported_count": len(events) - len(reported),
            "gross_plus_reported_commission_native": _amount(gross + commission) if first["side"] == "BUY" else None,
            "referenced_total_cost_native": None if reference is None else reference["total_cost_native"],
            "unclassified_residual_native": None if residual is None else _amount(residual),
            "fee_reconciliation_status": "unverified", "net_proceeds_native": None,
        }


def replay_executions(snapshot: dict, *, source_ref: str, previous_state: dict | None = None,
                      account_id: str | None = None, account_mapping: dict | None = None,
                      now_utc: dt.datetime | None = None, source_sha256: str | None = None) -> tuple[dict, dict]:
    """Validate a snapshot and return new immutable state plus its derived report.

    Inputs may include original_rows, cost_basis_references and amendment_requests.
    No correction applies to a workbook. Missing commissions remain unreported;
    a total-cost reference does not classify the residual as tax or another fee.
    """
    if not isinstance(snapshot, dict):
        raise AccountingError("snapshot must be an object")
    now = dt.datetime.now(UTC) if now_utc is None else now_utc
    if not isinstance(now, dt.datetime) or now.utcoffset() is None:
        raise AccountingError("now_utc must include timezone")
    now = now.astimezone(UTC)
    source = _source({"source_ref": source_ref,
                      "snapshot_sha256": payload_fingerprint(snapshot) if source_sha256 is None else source_sha256})
    mapping = {} if account_mapping is None else account_mapping
    if not isinstance(mapping, dict):
        raise AccountingError("account mapping must be an object")
    if account_id is not None and mapping:
        raise AccountingError("choose one global account context or per-trade account mapping")
    state = _validate_previous(previous_state, now)
    body = snapshot.get("response", snapshot)
    if not isinstance(body, dict):
        raise AccountingError("invalid broker response")
    trades = _bounded_list(body.get("trades"), "trades")
    if not trades:
        raise AccountingError("empty execution snapshot cannot establish accounting")
    for raw in trades:
        event = _execution(raw, account_id, mapping, source, now)
        _merge(state["executions"], lambda item: (item["account_id"], item["trade_id"]),
               event, "execution", ignore_source=True)
    for original in _bounded_list(snapshot.get("original_rows", []), "original_rows"):
        candidate = _original_record(original)
        _merge(state["original_rows"], lambda item: item["row_ref"], candidate, "original row")
    for reference in _bounded_list(snapshot.get("cost_basis_references", []), "cost_basis_references"):
        if not isinstance(reference, dict):
            raise AccountingError("invalid cost-basis reference")
        identity = (_text(reference.get("account_id"), "reference account_id"),
                    _text(reference.get("symbol"), "reference symbol").upper(), _currency(reference.get("currency")),
                    _text(reference.get("source_ref"), "reference source_ref"))
        existing = next((item for item in state["cost_basis_references"] if _reference_key(item) == identity), None)
        candidate = _reference_record(reference, state["executions"], existing["execution_keys"] if existing else None)
        _merge(state["cost_basis_references"], _reference_key,
               candidate, "cost-basis reference")
    summaries = _summaries(state)
    for request in _bounded_list(snapshot.get("amendment_requests", []), "amendment_requests"):
        proposal = _proposal(request, state)
        _merge(state["amendment_proposals"], lambda item: item["proposal_id"], proposal, "amendment proposal")
    for label, key in (("executions", lambda item: (item["account_id"], item["trade_id"])),
                       ("original_rows", lambda item: item["row_ref"]),
                       ("cost_basis_references", _reference_key),
                       ("amendment_proposals", lambda item: item["proposal_id"])):
        state[label].sort(key=key)
    state["state_digest"] = _state_digest(state)
    report = {"schema_version": SCHEMA_VERSION, "accounting_version": __version__, "status": "partial",
              "state_digest": state["state_digest"], "execution_count": len(state["executions"]),
              "summaries": summaries, "amendment_proposals": state["amendment_proposals"],
              "cost_reconciliations": [{**_summary(_linked_events(ref["execution_keys"], state["executions"]), [ref]),
                                        "source_ref": ref["source_ref"], "execution_keys": ref["execution_keys"],
                                        "reference_raw_sha256": ref["raw_payload_sha256"]}
                                       for ref in state["cost_basis_references"]],
              "limitations": ["Broker-reported commissions and residual charges are not statement-reconciled.",
                              "net_amount is preserved raw; it cannot establish net proceeds or total fees.",
                              "Amendments are conditional proposals; no native ledger or order is changed."]}
    return state, report


def _proposal(request: dict, state: dict) -> dict:
    if not isinstance(request, dict):
        raise AccountingError("invalid amendment request")
    original = next((row for row in state["original_rows"] if row["row_ref"] == request.get("original_row_ref")), None)
    if original is None or request.get("expected_original_sha256") != original["sha256"]:
        raise AccountingError("amendment original row or fingerprint is unproven")
    keys = _execution_keys(request.get("execution_keys"))
    events = _linked_events(keys, state["executions"])
    raw = json.loads(original["raw_row_json"], parse_float=Decimal)
    if any(event["account_id"] != original["account_id"] or event["symbol"] != _text(raw.get("Symbol"), "original Symbol").upper()
           or event["currency"] != _currency(raw.get("Ccy")) for event in events):
        raise AccountingError("amendment account/symbol/currency linkage disagrees")
    with localcontext() as context:
        context.prec = 512
        quantity = sum((Decimal(event["quantity"]) for event in events), Decimal(0))
    if quantity != _number(raw.get("Shares"), "original Shares"):
        raise AccountingError("amendment quantity does not match original row")
    kind = request.get("kind")
    reference_source = None
    if kind == "sale_date":
        if len(events) != 1 or events[0]["side"] != "SELL" or "Sell Date" not in raw:
            raise AccountingError("sale-date proposal requires one linked sell and original Sell Date")
        details = {"field": "Sell Date", "original_value": raw["Sell Date"],
                   "proposed_execution_time_utc": events[0]["trade_time_utc"],
                   "proposed_execution_date_utc": events[0]["trade_time_utc"][:10]}
    elif kind == "purchase_cost":
        if any(event["side"] != "BUY" for event in events) or "Cost Basis" not in raw:
            raise AccountingError("purchase-cost proposal requires linked buys and original Cost Basis")
        reference_source = request.get("cost_reference_source_ref")
        if "cost_reference_source_ref" not in request:
            historic = [proposal for proposal in state["amendment_proposals"]
                        if proposal["kind"] == kind and proposal["original_sha256"] == original["sha256"]
                        and proposal["original_row_ref"] == original["row_ref"] and proposal["execution_keys"] == keys]
            if len(historic) > 1:
                raise AccountingError("choose an explicit cost reference for this amendment")
            if historic:
                reference_source = historic[0]["cost_reference_source_ref"]
            else:
                matching = [ref for ref in state["cost_basis_references"] if ref["execution_keys"] == keys]
                reference_source = matching[0]["source_ref"] if len(matching) == 1 else None
        references = [ref for ref in state["cost_basis_references"]
                      if ref["source_ref"] == reference_source and ref["execution_keys"] == keys]
        if reference_source is not None and len(references) != 1:
            raise AccountingError("amendment total-cost reference linkage is unproven")
        summary = _summary(events, references)
        details = {"field": "Cost Basis", "original_value": raw["Cost Basis"],
                   "gross_native": summary["gross_native"],
                   "reported_commission_native": summary["reported_commission_native"],
                   "referenced_total_cost_native": summary["referenced_total_cost_native"],
                   "unclassified_residual_native": summary["unclassified_residual_native"],
                   "cost_reference_raw_sha256": references[0]["raw_payload_sha256"] if references else None,
                   "fee_reconciliation_status": "unverified"}
    else:
        raise AccountingError("unsupported amendment kind")
    proposal = {"kind": kind, "original_row_ref": original["row_ref"], "original_sha256": original["sha256"],
                "cost_reference_source_ref": reference_source,
                "execution_keys": sorted(keys, key=lambda key: (key["account_id"], key["trade_id"])),
                "execution_raw_hashes": sorted(event["raw_payload_sha256"] for event in events),
                "application_status": "conditional_proposal_only", "details": details}
    # Original values remain exact JSON text even when they were numeric.
    proposal["details"] = json.loads(_canonical(proposal["details"]), parse_float=str)
    proposal["proposal_id"] = payload_fingerprint(proposal)
    return proposal
