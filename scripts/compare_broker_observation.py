#!/usr/bin/env python3
"""Compare saved IBKR observations with an XLSX export, offline and read-only.

Stdout contains counts, booleans and fixed research explanations. --private
writes an atomic mode-0600 report with exact amounts, identifiers and input
hashes. Explicit --contract-map JSON is {"contract_id": "CANONICAL.SYMBOL"};
otherwise unique ticker matches are labeled provisional. --usd-sar-rate is an
explicit comparison scenario, never fresh FX evidence. This command never
emits reconciliation_evidence, certifies funding, contacts a broker or edits
the workbook. Retrieval time never becomes broker/quote as-of time.
"""
from __future__ import annotations

import argparse
from collections import Counter
from decimal import Decimal, InvalidOperation, localcontext
import hashlib
import json
import os
from pathlib import Path
import posixpath
import re
import tempfile
import xml.etree.ElementTree as ET
import zipfile

SCRIPT_VERSION = "1.0.0"
MAX_JSON_BYTES = 4_000_000
MAX_XLSX_BYTES = 64_000_000
MAX_MEMBER_BYTES = 128_000_000
MAX_ROWS = 20_000
MAX_POSITIONS = 10_000
NS = "{http://schemas.openxmlformats.org/spreadsheetml/2006/main}"
REL_NS = "{http://schemas.openxmlformats.org/officeDocument/2006/relationships}"
SYMBOL = re.compile(r"[A-Z0-9^][A-Z0-9._^=\-]{0,39}\Z")


class ObservationError(ValueError):
    pass


def _decimal(value, *, nonnegative=False):
    if isinstance(value, bool) or value is None or not isinstance(value, (str, int, float, Decimal)):
        raise ObservationError("invalid decimal")
    try:
        number = Decimal(str(value).strip())
    except (InvalidOperation, ValueError):
        raise ObservationError("invalid decimal") from None
    if (not number.is_finite() or len(number.as_tuple().digits) > 80
            or abs(number.adjusted()) > 80 or (nonnegative and number < 0)):
        raise ObservationError("invalid decimal")
    return number


def _amount(value):
    if value is None:
        return None
    result = format(value, "f")
    return result.rstrip("0").rstrip(".") if "." in result else result


def _optional_decimal(row, key, *, nonnegative=False):
    value = row.get(key)
    return None if value in (None, "") else _decimal(value, nonnegative=nonnegative)


def _symbol(value):
    if not isinstance(value, str) or not SYMBOL.fullmatch(value):
        raise ObservationError("invalid canonical symbol")
    return value


def _contract_id(value):
    if isinstance(value, bool) or not re.fullmatch(r"[1-9][0-9]{0,19}", str(value)):
        raise ObservationError("invalid contract identifier")
    return str(value)


def _unique_object(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise ObservationError("duplicate JSON field")
        result[key] = value
    return result


def _json_file(path):
    if not path.is_file() or path.stat().st_size > MAX_JSON_BYTES:
        raise ObservationError("invalid JSON input")
    raw = path.read_bytes()
    if len(raw) > MAX_JSON_BYTES:
        raise ObservationError("oversized JSON input")
    data = json.loads(raw, parse_float=Decimal, parse_constant=lambda _: (_ for _ in ()).throw(
        ObservationError("invalid JSON number")), object_pairs_hook=_unique_object)
    if not isinstance(data, dict):
        raise ObservationError("JSON object required")
    return data, hashlib.sha256(raw).hexdigest()


def _member(archive, name):
    info = archive.getinfo(name)
    if info.file_size > MAX_MEMBER_BYTES:
        raise ObservationError("oversized XLSX member")
    return archive.open(info)


def _sheet_rows(archive, name, strings):
    with _member(archive, name) as handle:
        count = 0
        for _, node in ET.iterparse(handle, events=("end",)):
            if node.tag != NS + "row":
                continue
            count += 1
            if count > MAX_ROWS:
                raise ObservationError("oversized sheet")
            row = {}
            for cell in node.findall(NS + "c"):
                address = cell.get("r", "")
                match = re.fullmatch(r"([A-Z]+)[1-9][0-9]*", address)
                if not match or match.group(1) in row:
                    raise ObservationError("ambiguous cell address")
                kind = cell.get("t")
                value = cell.findtext(NS + "v")
                if kind == "s":
                    if value is None or not re.fullmatch(r"[0-9]+", value):
                        raise ObservationError("invalid shared string")
                    decoded = strings[int(value)]
                elif kind == "inlineStr":
                    decoded = "".join(t.text or "" for t in cell.iter(NS + "t"))
                elif kind in ("str", "e", "d"):
                    decoded = value
                elif kind == "b":
                    if value not in ("0", "1"):
                        raise ObservationError("invalid boolean cell")
                    decoded = value == "1"
                else:
                    decoded = None if value is None else _decimal(value)
                row[match.group(1)] = decoded
            yield row
            node.clear()


def read_workbook(path):
    """Read only portfolio and cash XML; retain serialized numeric precision."""
    if not path.is_file() or path.stat().st_size > MAX_XLSX_BYTES:
        raise ObservationError("invalid workbook")
    with zipfile.ZipFile(path) as archive:
        if len(archive.namelist()) != len(set(archive.namelist())):
            raise ObservationError("duplicate workbook member")
        strings = []
        if "xl/sharedStrings.xml" in archive.namelist():
            with _member(archive, "xl/sharedStrings.xml") as handle:
                for _, node in ET.iterparse(handle, events=("end",)):
                    if node.tag == NS + "si":
                        strings.append("".join(t.text or "" for t in node.iter(NS + "t")))
                        if len(strings) > 1_000_000:
                            raise ObservationError("oversized string table")
                        node.clear()
        with _member(archive, "xl/_rels/workbook.xml.rels") as handle:
            relationships = ET.parse(handle).getroot()
        targets = {}
        for item in relationships:
            if item.get("TargetMode") == "External":
                continue
            target = item.get("Target", "")
            name = posixpath.normpath(target.lstrip("/") if target.startswith("/")
                                      else "xl/" + target)
            if not name.startswith("xl/"):
                raise ObservationError("invalid workbook relationship")
            targets[item.get("Id")] = name
        with _member(archive, "xl/workbook.xml") as handle:
            workbook = ET.parse(handle).getroot()
        sheets = {item.get("name"): targets.get(item.get(REL_NS + "id"))
                  for item in workbook.iter(NS + "sheet")}
        if not sheets.get("My_Portfolio"):
            raise ObservationError("portfolio sheet missing")
        portfolio = list(_sheet_rows(archive, sheets["My_Portfolio"], strings))
        aliases = {"symbol": {"symbol", "ticker"}, "currency": {"currency", "ccy"},
                   "quantity": {"positionqty", "quantity", "qty", "shares", "units"},
                   "price": {"currentprice", "price"}, "value": {"positionvalue"},
                   "average_cost": {"avgcost", "buyprice"}}
        headers = None
        header_index = None
        for index, row in enumerate(portfolio[:6]):
            columns = {}
            for col, value in row.items():
                token = re.sub(r"[^a-z0-9]", "", str(value or "").lower())
                for key, choices in aliases.items():
                    if token in choices:
                        if key in columns:
                            raise ObservationError("ambiguous portfolio header")
                        columns[key] = col
            if {"symbol", "currency", "quantity"} <= columns.keys():
                headers, header_index = columns, index
                break
        if headers is None:
            raise ObservationError("portfolio header incomplete")
        holdings, seen = [], set()
        for row in portfolio[header_index + 1:]:
            raw_symbol = row.get(headers["symbol"])
            if raw_symbol in (None, ""):
                continue
            symbol = _symbol(str(raw_symbol).strip().upper())
            currency = row.get(headers["currency"])
            if not isinstance(currency, str) or not re.fullmatch(r"[A-Z]{3}", currency):
                raise ObservationError("invalid native currency")
            identity = (symbol, currency)
            if identity in seen:
                raise ObservationError("duplicate sheet holding")
            seen.add(identity)
            holding = {"symbol": symbol, "currency": currency,
                       "quantity": _decimal(row.get(headers["quantity"]), nonnegative=True)}
            for key in ("price", "value", "average_cost"):
                if key in headers:
                    holding[key] = _optional_decimal(row, headers[key], nonnegative=True)
            holdings.append(holding)
        entered_cash = None
        if sheets.get("_Cash_Snapshot"):
            cash_rows = iter(_sheet_rows(archive, sheets["_Cash_Snapshot"], strings))
            cash_header = next(cash_rows, {})
            cash_cols = [col for col, value in cash_header.items()
                         if re.sub(r"[^a-z0-9]", "", str(value or "").lower()) == "balancesar"]
            if len(cash_cols) > 1:
                raise ObservationError("ambiguous cash header")
            if cash_cols:
                records = [row for row in cash_rows if any(v not in (None, "") for v in row.values())]
                if records:
                    entered_cash = _optional_decimal(records[-1], cash_cols[0], nonnegative=True)
    return holdings, entered_cash


def _tool_payload(observation, name):
    result = observation.get(name)
    if not isinstance(result, dict) or result.get("isError") is True:
        return {}
    result = result.get("structuredContent", result)
    return result if isinstance(result, dict) else {}


def compare_observation(observation, holdings, entered_cash_sar, contract_map=None, usd_sar_rate=None):
    """Produce research comparisons; no input can make execution ready."""
    mapping = {}
    for key, value in (contract_map or {}).items():
        mapping[_contract_id(key)] = _symbol(value)
    sheet = {row["symbol"]: row for row in holdings}
    if len(sheet) != len(holdings):
        raise ObservationError("ambiguous sheet symbols")
    positions = _tool_payload(observation, "positions").get("positions")
    if not isinstance(positions, list) or len(positions) > MAX_POSITIONS:
        raise ObservationError("positions unavailable")
    comparisons, linked, contracts = [], set(), set()
    with localcontext() as context:
        context.prec = 512
        for position in positions:
            if not isinstance(position, dict):
                raise ObservationError("invalid position")
            contract = _contract_id(position.get("contract_id"))
            if contract in contracts:
                raise ObservationError("duplicate broker contract")
            contracts.add(contract)
            quantity = _decimal(position.get("position"))
            currency = position.get("currency")
            if not isinstance(currency, str) or not re.fullmatch(r"[A-Z]{3}", currency):
                raise ObservationError("invalid broker currency")
            canonical = mapping.get(contract)
            basis = "explicit_contract_mapping" if canonical else "unlinked"
            if canonical is None:
                ticker = position.get("contract_description")
                matches = [symbol for symbol in sheet if isinstance(ticker, str)
                           and symbol.split(".")[0] == ticker.strip().upper()
                           and sheet[symbol]["currency"] == currency]
                if len(matches) == 1:
                    canonical, basis = matches[0], "provisional_unique_ticker"
            if canonical is not None:
                if canonical not in sheet or canonical in linked:
                    raise ObservationError("invalid or duplicate research link")
                linked.add(canonical)
            holding = sheet.get(canonical)
            status = ("broker_position_unlinked" if holding is None else
                      "currency_mismatch" if currency != holding["currency"] else
                      "quantity_matched_research" if quantity == holding["quantity"] else "quantity_mismatch")
            price = _optional_decimal(position, "market_price", nonnegative=True)
            value = _optional_decimal(position, "market_value")
            average = _optional_decimal(position, "average_price", nonnegative=True)
            comparisons.append({"contract_id": contract, "sheet_symbol": canonical, "mapping_basis": basis,
                "comparison": status, "currency": currency,
                "sheet_currency": holding["currency"] if holding else None,
                "broker_quantity": _amount(quantity),
                "sheet_quantity": _amount(holding["quantity"]) if holding else None,
                "broker_price": _amount(price), "sheet_price": _amount(holding.get("price")) if holding else None,
                "broker_average_price": _amount(average),
                "sheet_average_cost": _amount(holding.get("average_cost")) if holding else None,
                "broker_minus_sheet_average_cost": _amount(average - holding["average_cost"])
                    if average is not None and holding and holding.get("average_cost") is not None
                    and currency == holding["currency"] else None,
                "broker_market_value": _amount(value),
                "implied_broker_market_value": _amount(price * quantity) if price is not None else None,
                "reported_vs_implied_value_difference": _amount(value - price * quantity)
                    if price is not None and value is not None else None})
        outside = [{"sheet_symbol": row["symbol"], "currency": row["currency"],
                    "sheet_quantity": _amount(row["quantity"]), "comparison": "outside_observed_broker_cohort"}
                   for row in holdings if row["symbol"] not in linked]
        balances = _tool_payload(observation, "balances").get("balances", [])
        if not isinstance(balances, list):
            raise ObservationError("invalid balances")
        native_usd = [row for row in balances if isinstance(row, dict) and row.get("currency") == "USD"]
        if len(native_usd) > 1:
            raise ObservationError("ambiguous USD cash")
        settled = _optional_decimal(native_usd[0], "settled_cash") if native_usd else None
        rate = None if usd_sar_rate is None else _decimal(usd_sar_rate, nonnegative=True)
        if rate == 0:
            raise ObservationError("invalid FX scenario")
        equivalent = settled * rate if settled is not None and rate is not None else None
        difference = equivalent - entered_cash_sar if equivalent is not None and entered_cash_sar is not None else None
        summary = _tool_payload(observation, "summary")
        native_summary = summary if summary.get("currency") == "USD" else {}
        gross = _optional_decimal(native_summary, "gross_position_value")
        nav = _optional_decimal(native_summary, "net_liquidation")
        usd_position_values = [_decimal(row["broker_market_value"]) for row in comparisons
                               if row["currency"] == "USD" and row["broker_market_value"] is not None]
        usd_position_rows = [row for row in comparisons if row["currency"] == "USD"]
        # A USD-base summary can include converted foreign positions. Compare
        # to native row values only when the entire observed cohort is USD.
        broker_row_total = sum(usd_position_values, Decimal(0)) if usd_position_rows and len(
            usd_position_values) == len(usd_position_rows) == len(comparisons) else None
        aggregate_delta = gross - broker_row_total if gross is not None and broker_row_total is not None else None
        usd_sheet_values = [row.get("value") for row in holdings if row["currency"] == "USD"]
        sheet_gross = sum(usd_sheet_values, Decimal(0)) if usd_sheet_values and all(
            value is not None for value in usd_sheet_values) else None
        order_rows = _tool_payload(observation, "orders").get("orders")
        orders_available = isinstance(order_rows, list)
        order_rows = order_rows if orders_available else []
        if len(order_rows) > MAX_POSITIONS or any(not isinstance(row, dict) for row in order_rows):
            raise ObservationError("invalid orders")
        # Descriptions can suggest protective pairs, but do not prove OCA links.
        grouped = {}
        for order in order_rows:
            parsed_quantity = _optional_decimal(order, "remaining_shares_qty", nonnegative=True)
            for field in ("total_shares_qty", "cum_shares_qty", "limit_price"):
                _optional_decimal(order, field, nonnegative=True)
            match = re.fullmatch(r"Sell [0-9]+(?:\.[0-9]+)? ([A-Z0-9.\-]+)",
                                 str(order.get("primary_description", "")))
            if order.get("side") == "SELL" and match:
                grouped.setdefault(match.group(1), []).append((order.get("order_type"), parsed_quantity))
        pair_hypotheses = sum(len(rows) == 2 and {row[0] for row in rows} == {"LIMIT", "STOP"}
                              and rows[0][1] is not None and rows[0][1] == rows[1][1]
                              for rows in grouped.values())
        counts = Counter(row["comparison"] for row in comparisons)
        public = {"version": SCRIPT_VERSION, "status": "research_comparison",
            "execution_ready": False, "funding_eligible": False,
            "counts": {"sheet_holdings": len(holdings), "broker_positions": len(positions),
                       "quantity_matches": counts["quantity_matched_research"],
                       "quantity_mismatches": counts["quantity_mismatch"],
                       "currency_mismatches": counts["currency_mismatch"],
                       "broker_unlinked": counts["broker_position_unlinked"],
                       "sheet_outside_observed_cohort": len(outside),
                       "explicit_contract_links": sum(row["mapping_basis"] == "explicit_contract_mapping" for row in comparisons),
                       "provisional_ticker_links": sum(row["mapping_basis"] == "provisional_unique_ticker" for row in comparisons),
                       "observed_orders": len(order_rows), "possible_protective_pairs": pair_hypotheses},
            "checks": {"orders_observed": orders_available, "entered_cash_comparison_available": difference is not None,
                       "entered_cash_matches_explicit_fx_scenario": difference == 0 if difference is not None else None,
                       "position_row_values_match_summary_gross_to_cent": gross.quantize(Decimal("0.01")) ==
                           broker_row_total.quantize(Decimal("0.01")) if aggregate_delta is not None else None},
            "hypotheses": ["LIMIT/STOP pairs may be protective alternatives; OCA linkage is unproven",
                           "Different observation vintages can explain market-value differences"],
            "limitations": ["Offline input origin is not reauthenticated", "Observation does not certify account scope or source as-of",
                            "Reserved cash and commitment completeness are unknown", "Other custody remains unobserved",
                            "An explicit FX scenario is not fresh executable FX proof"]}
        private = dict(public, retrieved_at_utc=observation.get("retrieved_at_utc"),
            source_asof_utc=observation.get("source_asof_utc"), account_id=observation.get("account_id"),
            positions=comparisons, outside_observed_cohort=outside,
            cash_comparison={"currency": "USD", "settled_cash": _amount(settled), "reserved_cash": None,
                "reservations_complete": None, "explicit_usd_sar_rate": _amount(rate), "fx_certified": False,
                "research_equivalent_sar": _amount(equivalent), "sheet_entered_cash_sar": _amount(entered_cash_sar),
                "difference_sar": _amount(difference)},
            native_value_comparison={"currency": "USD", "reported_gross": _amount(gross), "reported_nav": _amount(nav),
                "cash_plus_gross": _amount(settled + gross) if settled is not None and gross is not None else None,
                "nav_minus_cash_plus_gross": _amount(nav - settled - gross)
                    if nav is not None and settled is not None and gross is not None else None,
                "sheet_gross": _amount(sheet_gross),
                "broker_position_rows_total": _amount(broker_row_total),
                "summary_gross_minus_position_rows_total": _amount(aggregate_delta),
                "reported_gross_minus_sheet_gross": _amount(gross - sheet_gross)
                    if gross is not None and sheet_gross is not None else None},
            orders=order_rows, input_observation=observation)
    return public, private


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("observation", type=Path)
    parser.add_argument("workbook", type=Path)
    parser.add_argument("--contract-map", type=Path)
    parser.add_argument("--usd-sar-rate", help="Explicit research FX scenario; no default rate")
    parser.add_argument("--private", type=Path, help="Explicit private report destination (0600)")
    args = parser.parse_args(argv)
    staged = None
    try:
        inputs = [args.observation, args.workbook] + ([args.contract_map] if args.contract_map else [])
        if args.private is not None:
            output = args.private
            if (output.is_symlink() or not output.parent.is_dir()
                    or (output.exists() and not output.is_file())
                    or any(output.resolve() == item.resolve() or
                           (output.exists() and os.path.samefile(output, item)) for item in inputs)):
                raise ObservationError("invalid output path")
        observation, observation_hash = _json_file(args.observation)
        mapping, mapping_hash = _json_file(args.contract_map) if args.contract_map else ({}, None)
        holdings, entered_cash = read_workbook(args.workbook)
        public, private = compare_observation(observation, holdings, entered_cash, mapping, args.usd_sar_rate)
        private["input_sha256"] = {"observation": observation_hash, "contract_map": mapping_hash,
            "workbook": hashlib.sha256(args.workbook.read_bytes()).hexdigest()}
        if args.private is not None:
            descriptor, name = tempfile.mkstemp(prefix=".broker-observation-", dir=args.private.parent)
            staged = Path(name)
            with os.fdopen(descriptor, "w", encoding="utf-8") as handle:
                os.fchmod(handle.fileno(), 0o600)
                json.dump(private, handle, sort_keys=True, indent=2, default=str, allow_nan=False)
                handle.write("\n")
                handle.flush()
                os.fsync(handle.fileno())
            os.replace(staged, args.private)
            staged = None
        print(json.dumps(public, sort_keys=True, allow_nan=False))
        return 0
    except (ObservationError, OSError, ValueError, TypeError, KeyError, IndexError,
            zipfile.BadZipFile, ET.ParseError, OverflowError, RecursionError):
        print(json.dumps({"status": "withheld", "error": "broker_observation_input_or_output_invalid",
                          "execution_ready": False, "funding_eligible": False}, sort_keys=True))
        return 2
    finally:
        if staged is not None:
            try:
                staged.unlink()
            except OSError:
                pass


if __name__ == "__main__":
    raise SystemExit(main())
