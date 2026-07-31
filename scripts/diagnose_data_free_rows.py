#!/usr/bin/env python3
"""Read-only diagnostic for every data-free row in the dashboard refresh.

This command executes the same sequential production fetch path used by
``scripts/benchmark_dashboard_fetch.py`` with the same in-memory Sheet sink.
It does not change request ordering, retries, provider selection, scoring,
ranking, portfolio logic, or Google Sheets data.

The output explains *why* each returned row is not decision-eligible. Missing
facts remain unknown; this tool never substitutes zero, stale data, or a
synthetic recommendation.
"""
from __future__ import annotations

import argparse
import asyncio
import csv
import json
from collections import Counter
from pathlib import Path
from typing import Any, Mapping, Sequence

from scripts import benchmark_dashboard_fetch as benchmark
from scripts import run_dashboard_sync as sync

DIAGNOSTIC_VERSION = "1.0.0"


def _text(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip()


def _header_index(headers: Sequence[Any], candidates: Sequence[str]) -> int:
    wanted = {sync._guard_norm(value) for value in candidates if value}
    for index, header in enumerate(headers or []):
        if sync._guard_norm(header) in wanted:
            return index
    return -1


def _cell(row: Sequence[Any] | None, index: int) -> Any:
    if row is None or index < 0 or index >= len(row):
        return None
    return row[index]


def _is_blank(value: Any) -> bool:
    try:
        return bool(sync._guard_is_blank(value))
    except Exception:
        return not _text(value)


def _positive(value: Any) -> bool:
    try:
        return bool(benchmark._positive(value))
    except Exception:
        try:
            parsed = float(_text(value).replace(",", ""))
        except Exception:
            return False
        return 0.0 < parsed < 1e15


def _provider_error(value: Any) -> bool:
    try:
        return bool(benchmark._provider_error(value))
    except Exception:
        normalized = "".join(
            character
            for character in _text(value).casefold()
            if character.isalnum()
        )
        return (not normalized) or normalized in {
            "fallbackerror",
            "error",
            "unavailable",
            "none",
            "placeholder",
            "synthetic",
            "stub",
        }


def _symbol_bucket(symbol: str) -> str:
    value = _text(symbol).upper()
    if not value:
        return "UNKNOWN"
    if value.startswith("^"):
        return "INDEX"
    if value.endswith("=X"):
        return "FX"
    if value.endswith("=F"):
        return "FUTURE"
    if value.endswith("-USD") or value.endswith("-USDT"):
        return "CRYPTO"
    if "." in value:
        return "." + value.rsplit(".", 1)[-1]
    return "BARE"


def _column_map(headers: Sequence[Any]) -> dict[str, int]:
    return {
        "symbol": _header_index(headers, ("Symbol", "Ticker")),
        "name": _header_index(headers, ("Name", "Company Name", "Instrument Name")),
        "price": _header_index(headers, ("Current Price", "Price", "Last Price")),
        "provider": _header_index(headers, ("Data Provider", "Provider", "Data Source")),
        "exchange": _header_index(headers, ("Exchange", "Market")),
        "currency": _header_index(headers, ("Currency",)),
        "asset_class": _header_index(headers, ("Asset Class", "Asset Type")),
        "warnings": _header_index(headers, ("Warnings", "Warning")),
        "block_reason": _header_index(headers, ("Block Reason", "Blocked Reason")),
        "row_source": _header_index(headers, ("Row Source", "Source")),
        "last_updated_utc": _header_index(headers, ("Last Updated (UTC)", "Last Updated UTC")),
        "last_updated_riyadh": _header_index(
            headers,
            ("Last Updated (Riyadh)", "Last Updated Riyadh"),
        ),
    }


def _reason_codes(
    row: Sequence[Any] | None,
    columns: Mapping[str, int],
) -> list[str]:
    if row is None:
        return ["missing_response_row"]

    reasons: list[str] = []
    symbol = _cell(row, columns["symbol"])
    name = _cell(row, columns["name"])
    price = _cell(row, columns["price"])
    provider = _cell(row, columns["provider"])

    if _is_blank(symbol):
        reasons.append("missing_symbol")
    if columns["name"] >= 0 and _is_blank(name):
        reasons.append("missing_name")
    if columns["price"] >= 0:
        if _is_blank(price):
            reasons.append("missing_price")
        elif not _positive(price):
            reasons.append("nonpositive_or_invalid_price")
    if columns["provider"] >= 0:
        if _is_blank(provider):
            reasons.append("missing_provider")
        elif _provider_error(provider):
            reasons.append("provider_error_or_placeholder")

    diagnostic_text = " | ".join(
        _text(_cell(row, columns[key])).casefold()
        for key in ("warnings", "block_reason", "row_source")
        if columns[key] >= 0
    )
    if any(
        marker in diagnostic_text
        for marker in (
            "identity_quarantined",
            "identity blocked",
            "identity_blocked",
            "identity-broken",
            "identity_patch_refused",
            "identity_echo_refused",
        )
    ):
        reasons.append("identity_blocked_or_quarantined")
    if any(
        marker in diagnostic_text
        for marker in (
            "provider_unhealthy",
            "provider unavailable",
            "provider_unavailable",
            "payment_required",
            "plan_restricted",
        )
    ):
        reasons.append("provider_reported_unavailable")

    return list(dict.fromkeys(reasons))


def _availability_class(data_free: bool, reasons: Sequence[str]) -> str:
    reason_set = set(reasons)
    if not data_free:
        return "VERIFIED_FRESH"
    if "missing_response_row" in reason_set:
        return "MISSING_RESPONSE_ROW"
    if "identity_blocked_or_quarantined" in reason_set:
        return "IDENTITY_BLOCKED"
    if reason_set.intersection(
        {
            "missing_provider",
            "provider_error_or_placeholder",
            "provider_reported_unavailable",
        }
    ):
        return "PROVIDER_UNAVAILABLE_OR_ERROR"
    return "MISSING_VERIFIED_FACTS"


def build_diagnostic_payload(
    *,
    headers: Sequence[Any],
    requested_symbols: Sequence[str],
    rows_by_symbol: Mapping[str, Sequence[Any]],
    collector_metrics: Mapping[str, Any],
    result_payload: Mapping[str, Any],
    planned_writes: Sequence[Mapping[str, Any]],
    clear_requests: Sequence[Mapping[str, Any]],
    page: str,
    backend_url: str,
) -> dict[str, Any]:
    columns = _column_map(headers)
    records: list[dict[str, Any]] = []
    reason_counts: Counter[str] = Counter()
    class_counts: Counter[str] = Counter()
    suffix_counts: Counter[str] = Counter()
    reason_suffix_counts: Counter[str] = Counter()

    fresh_count = 0
    for symbol in requested_symbols:
        row = rows_by_symbol.get(symbol)
        data_free = row is None or not benchmark._row_good(headers, row)
        reasons = _reason_codes(row, columns)
        if data_free and not reasons:
            reasons = ["unclassified_data_free"]
        availability = _availability_class(data_free, reasons)
        suffix = _symbol_bucket(symbol)

        if not data_free:
            fresh_count += 1
            continue

        for reason in reasons:
            reason_counts[reason] += 1
            reason_suffix_counts[f"{reason}|{suffix}"] += 1
        class_counts[availability] += 1
        suffix_counts[suffix] += 1

        records.append(
            {
                "symbol": symbol,
                "symbol_bucket": suffix,
                "availability_class": availability,
                "decision_eligible": False,
                "reason_codes": reasons,
                "name": _text(_cell(row, columns["name"])),
                "current_price": _cell(row, columns["price"]),
                "data_provider": _text(_cell(row, columns["provider"])),
                "exchange": _text(_cell(row, columns["exchange"])),
                "currency": _text(_cell(row, columns["currency"])),
                "asset_class": _text(_cell(row, columns["asset_class"])),
                "warnings": _text(_cell(row, columns["warnings"])),
                "block_reason": _text(_cell(row, columns["block_reason"])),
                "row_source": _text(_cell(row, columns["row_source"])),
                "last_updated_utc": _text(_cell(row, columns["last_updated_utc"])),
                "last_updated_riyadh": _text(
                    _cell(row, columns["last_updated_riyadh"])
                ),
            }
        )

    expected_data_free = int(collector_metrics.get("symbols_data_free") or 0)
    evidence_consistent = (
        len(records) == expected_data_free
        and fresh_count == int(collector_metrics.get("symbols_fresh") or 0)
        and len(requested_symbols)
        == int(collector_metrics.get("symbols_requested") or 0)
    )

    return {
        "schema_version": "1.0",
        "diagnostic_version": DIAGNOSTIC_VERSION,
        "mode": "read_live_fetch_no_write_data_free_diagnostic",
        "page": page,
        "backend": backend_url,
        "no_workbook_writes": True,
        "evidence_consistent": evidence_consistent,
        "summary": {
            "requested_symbols": len(requested_symbols),
            "fresh_symbols": fresh_count,
            "data_free_symbols": len(records),
            "decision_eligible_symbols": fresh_count,
            "decision_blocked_symbols": len(records),
            "unclassified_data_free": reason_counts.get("unclassified_data_free", 0),
            "reason_counts": dict(reason_counts.most_common()),
            "availability_class_counts": dict(class_counts.most_common()),
            "symbol_bucket_counts": dict(suffix_counts.most_common()),
            "reason_symbol_bucket_counts": dict(reason_suffix_counts.most_common()),
        },
        "collector_metrics": dict(collector_metrics),
        "runner_result": dict(result_payload),
        "planned_writes": [dict(item) for item in planned_writes],
        "clear_requests": [dict(item) for item in clear_requests],
        "data_free_rows": records,
    }


def _write_csv(path: str, records: Sequence[Mapping[str, Any]]) -> None:
    if not path:
        return
    columns = [
        "symbol",
        "symbol_bucket",
        "availability_class",
        "decision_eligible",
        "reason_codes",
        "name",
        "current_price",
        "data_provider",
        "exchange",
        "currency",
        "asset_class",
        "warnings",
        "block_reason",
        "row_source",
        "last_updated_utc",
        "last_updated_riyadh",
    ]
    output = Path(path)
    output.parent.mkdir(parents=True, exist_ok=True)
    with output.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=columns)
        writer.writeheader()
        for record in records:
            row = dict(record)
            row["reason_codes"] = ";".join(record.get("reason_codes") or [])
            writer.writerow({key: row.get(key, "") for key in columns})


async def run_diagnostic(args: argparse.Namespace) -> tuple[int, dict[str, Any]]:
    if int(args.concurrency) != 1:
        raise ValueError(
            "data-free diagnostics must run on the accepted sequential baseline "
            "with --concurrency 1"
        )

    benchmark._set_runtime_env(args)
    task = benchmark._task_for(args.page)
    sheet_id = sync._default_spreadsheet_id(args.sheet_id)
    if not sheet_id:
        raise RuntimeError("Spreadsheet ID is required")

    backend_url = (args.backend or sync._default_backend_url()).rstrip("/")
    backend = sync.BackendClient(
        backend_url,
        timeout_sec=float(args.timeout),
        token=sync._env_token(),
    )
    sheets = benchmark.NoWriteSheets()
    collector = benchmark.SequentialEvidenceCollector()
    collector.install(backend)
    result = None
    try:
        sync._idfw_selftest_()
        result = await sync._run_one_task(
            task=task,
            spreadsheet_id=sheet_id,
            start_cell="A1",
            max_symbols_override=int(args.max_symbols),
            clear_before_write=False,
            dry_run=False,
            backend=backend,
            sheets=sheets,
        )
    finally:
        collector.restore()
        await backend.close()

    if result is None:
        raise RuntimeError("runner returned no TaskResult")

    collector_metrics = collector.metrics()
    rows_by_symbol = collector._rows_by_symbol()
    requested_symbols = list(collector.requested_symbols)
    payload = build_diagnostic_payload(
        headers=collector.headers,
        requested_symbols=requested_symbols,
        rows_by_symbol=rows_by_symbol,
        collector_metrics=collector_metrics,
        result_payload=result.to_dict(),
        planned_writes=sheets.planned_writes,
        clear_requests=sheets.clear_requests,
        page=task.sheet_name,
        backend_url=backend_url,
    )

    if result.status == "failed":
        return 2, payload
    if not payload["evidence_consistent"]:
        return 1, payload
    return 0, payload


def create_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--page", default="Market_Leaders")
    parser.add_argument("--sheet-id", default="")
    parser.add_argument("--backend", default="")
    parser.add_argument("--max-symbols", type=int, default=1000)
    parser.add_argument("--batch-size", type=int, default=25)
    parser.add_argument("--concurrency", type=int, default=1)
    parser.add_argument("--outer-retries", type=int, default=1)
    parser.add_argument("--timeout", type=float, default=120.0)
    parser.add_argument("--time-budget", type=int, default=2100)
    parser.add_argument("--json-out", default="data_free_diagnostics.json")
    parser.add_argument("--csv-out", default="data_free_rows.csv")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = create_parser().parse_args(argv)
    try:
        code, payload = asyncio.run(run_diagnostic(args))
    except Exception as exc:
        print(f"::error::DATA_FREE_DIAGNOSTIC_FAILED: {type(exc).__name__}: {exc}")
        return 3

    rendered = json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True)
    print(rendered)
    if args.json_out:
        output = Path(args.json_out)
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_text(rendered + "\n", encoding="utf-8")
    _write_csv(args.csv_out, payload.get("data_free_rows") or [])
    return code


if __name__ == "__main__":
    raise SystemExit(main())
