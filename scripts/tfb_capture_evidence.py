#!/usr/bin/env python3
"""Build a declared custodian packet offline; no broker or workbook access.

WHY v1.0.0 (2026-10-11): explicit account coverage, position linkage, settled
cash and reservations must travel together. Sheet balances and export notes
cannot supply missing custody evidence. Operator-mapped JSON/CSV is validated
by the same pure certification function as tfb_reconcile_portfolio before a
packet is atomically written with mode 0600. Origin remains unauthenticated.

JSON accepts {holdings, reconciliation_evidence, fx_rates}, as used by the
offline reconciliation CLI. CSV uses explicit typed records documented in
docs/portfolio_reconciliation_contract.md. --now is a complete review clock;
--max-age-check additionally checks *all* declared timestamps against 900s.
Stdout contains only nonidentifying status/counts. No invalid packet is saved.
"""
from __future__ import annotations

import argparse
import csv
import io
import json
import os
import sys
import tempfile
from decimal import Decimal
from pathlib import Path

if __package__ in (None, ""):
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from core.execution_accounting import AccountingError, _timestamp
from core.portfolio_reconciliation import (
    DEFAULT_MAX_AGE_SECONDS,
    SCHEMA_VERSION,
    certification_summary,
    certify_portfolio_inputs,
)

SCRIPT_VERSION = "1.0.0"
MAX_INPUT_BYTES = 2_000_000
MAX_CSV_RECORDS = 25_000
CSV_FIELDS = (
    "record_type", "source_ref", "captured_at", "account_id",
    "positions_asof", "positions_complete", "cash_asof", "cash_complete",
    "instrument_id", "position_symbol", "symbol", "currency", "quantity",
    "settled_cash", "reserved_cash", "reservations_complete", "row_index",
    "rate_to_sar", "asof",
)
_RECORD_FIELDS = {
    "capture": {"source_ref", "captured_at"},
    "account": {"account_id", "positions_asof", "positions_complete", "cash_asof", "cash_complete"},
    "position": {"account_id", "instrument_id", "symbol", "currency", "quantity"},
    "cash": {"account_id", "currency", "settled_cash", "reserved_cash", "reservations_complete"},
    "holding": {"row_index", "symbol", "currency", "quantity"},
    "link": {"row_index", "account_id", "instrument_id", "position_symbol", "symbol", "currency"},
    "funding": {"account_id"},
    "fx": {"currency", "rate_to_sar", "asof", "source_ref"},
    "caller_fx": {"currency", "rate_to_sar"},
}


def _boolean(value):
    if value not in {"true", "false"}:
        raise AccountingError("invalid capture boolean")
    return value == "true"


def _index(value):
    if not isinstance(value, str) or not value.isascii() or not value.isdigit():
        raise AccountingError("invalid capture row index")
    return int(value)


def _unique_object(pairs):
    body = {}
    for key, value in pairs:
        if key in body:
            raise AccountingError("duplicate capture JSON field")
        body[key] = value
    return body


def csv_capture(raw: str) -> dict:
    """Map explicit records; never infer custody, coverage, cash or FX basis."""
    reader = csv.DictReader(io.StringIO(raw))
    headers = reader.fieldnames
    if (not headers or "record_type" not in headers or len(set(headers)) != len(headers)
            or not set(headers) <= set(CSV_FIELDS)):
        raise AccountingError("invalid capture CSV header")
    records = []
    for index, row in enumerate(reader):
        if index >= MAX_CSV_RECORDS or None in row or any(value is None for value in row.values()):
            raise AccountingError("invalid capture CSV size or width")
        row = {key: value.strip() for key, value in row.items()}
        kind = row.pop("record_type")
        allowed = _RECORD_FIELDS.get(kind)
        fields = {key: value for key, value in row.items() if value != ""}
        if allowed is None or set(fields) != allowed:
            raise AccountingError("invalid capture CSV record")
        records.append((kind, fields))
    captures = [row for kind, row in records if kind == "capture"]
    if len(captures) != 1:
        raise AccountingError("one explicit capture required")
    packet = {"schema_version": SCHEMA_VERSION, **captures[0], "accounts": [],
              "holding_links": [], "funding_accounts": [], "fx_rates": []}
    accounts = {}
    for kind, row in records:
        if kind == "account":
            aid = row["account_id"]
            if aid in accounts:
                raise AccountingError("duplicate capture account")
            account = dict(row, positions=[], cash=[])
            for field in ("positions_complete", "cash_complete"):
                account[field] = _boolean(row[field])
            accounts[aid] = account
            packet["accounts"].append(account)
    holdings = {}
    caller_fx = {}
    for kind, row in records:
        if kind in {"capture", "account"}:
            continue
        item = dict(row)
        if kind in {"position", "cash"}:
            aid = item.pop("account_id")
            if aid not in accounts:
                raise AccountingError("undeclared capture account")
            if kind == "cash":
                item["reservations_complete"] = _boolean(item["reservations_complete"])
            accounts[aid]["positions" if kind == "position" else "cash"].append(item)
        elif kind == "holding":
            index = _index(item.pop("row_index"))
            if index in holdings:
                raise AccountingError("duplicate capture holding row")
            holdings[index] = item
        elif kind == "link":
            item["row_index"] = _index(item["row_index"])
            packet["holding_links"].append(item)
        elif kind == "funding":
            packet["funding_accounts"].append(item["account_id"])
        elif kind == "fx":
            packet["fx_rates"].append(item)
        elif kind == "caller_fx":
            ccy = item["currency"]
            if ccy in caller_fx:
                raise AccountingError("duplicate caller FX")
            caller_fx[ccy] = item["rate_to_sar"]
    if sorted(holdings) != list(range(len(holdings))):
        raise AccountingError("holding rows must be contiguous")
    return {"holdings": [holdings[index] for index in range(len(holdings))],
            "reconciliation_evidence": packet, "fx_rates": caller_fx}


def _all_timestamps_fresh(packet: dict, now) -> None:
    """Check even nonfunding account cash, which certification need not use."""
    captured = _timestamp(packet.get("captured_at"), "captured_at")
    timestamps = [captured]
    for account in packet.get("accounts", []):
        timestamps.extend(_timestamp(account.get(field), field)
                          for field in ("positions_asof", "cash_asof"))
    timestamps.extend(_timestamp(proof.get("asof"), "FX asof")
                      for proof in packet.get("fx_rates", []))
    if any(stamp > now or stamp > captured
           or (now - stamp).total_seconds() > DEFAULT_MAX_AGE_SECONDS
           for stamp in timestamps):
        raise AccountingError("capture age check failed")


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("capture", type=Path)
    parser.add_argument("--format", choices=("json", "csv"), help="Default: infer from .json/.csv extension")
    parser.add_argument("--now", required=True, help="Complete timestamp with seconds and timezone")
    parser.add_argument("--output", type=Path, required=True, help="Private evidence packet destination")
    parser.add_argument("--max-age-check", action="store_true", help="Also enforce 900s on all declared timestamps")
    args = parser.parse_args(argv)
    staged = None
    try:
        if not args.capture.is_file() or args.capture.stat().st_size > MAX_INPUT_BYTES:
            raise AccountingError("invalid capture input path or size")
        output = args.output
        if (output.is_symlink() or output.resolve() == args.capture.resolve()
                or not output.parent.is_dir() or (output.exists() and not output.is_file())
                or (output.exists() and os.path.samefile(output, args.capture))):
            raise AccountingError("invalid private output path")
        raw = args.capture.read_bytes()
        if len(raw) > MAX_INPUT_BYTES:
            raise AccountingError("invalid capture input size")
        kind = args.format or args.capture.suffix.lower().lstrip(".")
        if kind == "csv":
            body = csv_capture(raw.decode("utf-8-sig"))
        elif kind == "json":
            body = json.loads(raw, parse_float=Decimal, object_pairs_hook=_unique_object)
        else:
            raise AccountingError("capture format required")
        if not isinstance(body, dict):
            raise AccountingError("invalid capture object")
        packet = body.get("reconciliation_evidence")
        now = _timestamp(args.now, "now")
        report = certify_portfolio_inputs(body.get("holdings"), packet, body.get("fx_rates"),
                                         now=now, max_age_seconds=DEFAULT_MAX_AGE_SECONDS)
        if not report["funding_eligible"]:
            print(json.dumps(certification_summary(report), sort_keys=True))
            return 2
        if args.max_age_check:
            _all_timestamps_fresh(packet, now)
        encoded = json.dumps(packet, sort_keys=True, separators=(",", ":"),
                             default=str, allow_nan=False) + "\n"
        descriptor, name = tempfile.mkstemp(prefix=".custodian-evidence-", dir=output.parent)
        staged = Path(name)
        with os.fdopen(descriptor, "w", encoding="utf-8") as handle:
            os.fchmod(handle.fileno(), 0o600)
            handle.write(encoded)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(staged, output)
        staged = None
        summary = certification_summary(report)
        summary.update(accounts=len(packet["accounts"]),
                       positions=sum(len(account["positions"]) for account in packet["accounts"]),
                       funding_accounts=len(packet["funding_accounts"]),
                       packet_bytes=len(encoded.encode("utf-8")))
        print(json.dumps(summary, sort_keys=True))
        return 0
    except (AccountingError, OSError, ValueError, TypeError, KeyError, csv.Error, OverflowError, RecursionError):
        print(json.dumps({"status": "withheld", "error": "custodian_capture_input_or_output_invalid"}, sort_keys=True))
        return 2
    finally:
        if staged is not None:
            try:
                staged.unlink()
            except OSError:
                pass


if __name__ == "__main__":
    raise SystemExit(main())
