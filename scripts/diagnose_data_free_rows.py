#!/usr/bin/env python3
"""Read-only decision-eligibility diagnostic for dashboard refresh rows.

This command executes the same sequential production fetch path used by
``scripts/benchmark_dashboard_fetch.py`` with the same in-memory Sheet sink.
It does not change request ordering, retries, provider selection, scoring,
ranking, portfolio logic, or Google Sheets data.

The report distinguishes three layers that were previously conflated:

* transport health — GitHub's HTTP request to Render;
* provider health — e.g. ``fetch_failed:HTTP 402`` embedded in a returned row;
* provider circuit state — rows intentionally short-circuited after the first
  plan/entitlement failure, without repeating the network call;
* decision eligibility — verified identity, venue metadata, name and price.

Missing facts remain unknown. This tool never substitutes zero, stale data or a
synthetic recommendation. All requested symbols are emitted in the eligibility
manifest; data-free details are not capped at 100 rows.
"""
from __future__ import annotations

import argparse
import asyncio
import csv
import json
import re
from collections import Counter
from pathlib import Path
from typing import Any, Mapping, Sequence

from scripts import benchmark_dashboard_fetch as benchmark
from scripts import run_dashboard_sync as sync

DIAGNOSTIC_VERSION = "1.2.0"


_MARKET_TRUTH: dict[str, tuple[str, str, str]] = {
    ".AB": ("ADX", "AED", "United Arab Emirates"),
    ".AD": ("ADX", "AED", "United Arab Emirates"),
    ".ADX": ("ADX", "AED", "United Arab Emirates"),
    ".PS": ("PSE", "PHP", "Philippines"),
    ".PSE": ("PSE", "PHP", "Philippines"),
    ".OM": ("MSX", "OMR", "Oman"),
}
_VALID_SR = re.compile(r"^\d{3,6}\.SR$", re.IGNORECASE)

# Provider-level signals live inside a successful Render response. Do not merge
# them with the outer GitHub->Render HTTP status. In particular, an open local
# circuit is not another HTTP 402 network event.
_PROVIDER_PATTERNS: dict[str, tuple[str, ...]] = {
    "provider_http_402": (
        "http 402",
        "http_402",
        "status 402",
        "payment_required",
    ),
    "provider_circuit_open": ("provider_circuit_open:eodhd",),
    "provider_http_404": ("http 404", "http_404", "status 404"),
    "provider_unhealthy_eodhd": ("provider_unhealthy:eodhd",),
    "provider_timeout": ("provider_timeout", "fetch_timeout", "timed out", "timeout"),
}

_BLOCKING_REASONS = {
    "missing_response_row",
    "missing_symbol",
    "missing_name",
    "missing_price",
    "nonpositive_or_invalid_price",
    "missing_provider",
    "provider_error_or_placeholder",
    "identity_blocked_or_quarantined",
    "provider_reported_unavailable",
    "provider_http_402",
    "provider_circuit_open",
    "provider_http_404",
    "provider_unhealthy_eodhd",
    "provider_timeout",
    "metadata_exchange_conflict",
    "metadata_currency_conflict",
    "metadata_country_conflict",
    "invalid_symbol_shape",
    "unclassified_data_free",
}


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
        "country": _header_index(headers, ("Country",)),
        "asset_class": _header_index(headers, ("Asset Class", "Asset Type")),
        "warnings": _header_index(headers, ("Warnings", "Warning")),
        "block_reason": _header_index(headers, ("Block Reason", "Blocked Reason")),
        "row_source": _header_index(headers, ("Row Source", "Source")),
        "investability_status": _header_index(
            headers,
            ("Investability Status", "Investability"),
        ),
        "final_action": _header_index(headers, ("Final Action",)),
        "last_updated_utc": _header_index(headers, ("Last Updated (UTC)", "Last Updated UTC")),
        "last_updated_riyadh": _header_index(
            headers,
            ("Last Updated (Riyadh)", "Last Updated Riyadh"),
        ),
    }


def _diagnostic_text(row: Sequence[Any] | None, columns: Mapping[str, int]) -> str:
    if row is None:
        return ""
    return " | ".join(
        _text(_cell(row, columns[key])).casefold()
        for key in ("warnings", "block_reason", "row_source")
        if columns[key] >= 0
    )


def _provider_warning_codes(text: str) -> list[str]:
    codes: list[str] = []
    lowered = text.casefold()
    for code, patterns in _PROVIDER_PATTERNS.items():
        if any(pattern in lowered for pattern in patterns):
            codes.append(code)
    return codes


def _expected_market(symbol: str) -> tuple[str, str, str] | None:
    upper = _text(symbol).upper()
    for suffix in sorted(_MARKET_TRUTH, key=len, reverse=True):
        if upper.endswith(suffix):
            return _MARKET_TRUTH[suffix]
    if upper.endswith(".SR") and _VALID_SR.fullmatch(upper):
        return ("Tadawul", "SAR", "Saudi Arabia")
    return None


def _exchange_matches(actual: str, expected: str) -> bool:
    a = sync._guard_norm(actual)
    e = sync._guard_norm(expected)
    if not a:
        return True
    aliases = {
        "adx": {"adx", "abudhabisecuritiesexchange", "dfmadx"},
        "pse": {"pse", "philippinestockexchange"},
        "msx": {"msx", "muscatstockexchange", "muscatsecuritiesmarket"},
        "tadawul": {"tadawul", "saudiexchange"},
    }
    return a == e or a in aliases.get(e, set())


def _country_matches(actual: str, expected: str) -> bool:
    a = sync._guard_norm(actual)
    e = sync._guard_norm(expected)
    if not a:
        return True
    aliases = {
        "unitedarabemirates": {"unitedarabemirates", "uae"},
        "philippines": {"philippines", "philippine"},
        "saudiarabia": {"saudiarabia", "ksa"},
        "oman": {"oman"},
    }
    return a == e or a in aliases.get(e, set())


def _metadata_reason_codes(
    symbol: str,
    row: Sequence[Any] | None,
    columns: Mapping[str, int],
) -> list[str]:
    if row is None:
        return []
    upper = _text(symbol).upper()
    if upper.endswith(".SR") and not _VALID_SR.fullmatch(upper):
        return ["invalid_symbol_shape"]

    expected = _expected_market(upper)
    if expected is None:
        return []
    expected_exchange, expected_currency, expected_country = expected

    actual_exchange = _text(_cell(row, columns["exchange"]))
    actual_currency = _text(_cell(row, columns["currency"])).upper()
    actual_country = _text(_cell(row, columns["country"]))

    reasons: list[str] = []
    if actual_exchange and not _exchange_matches(actual_exchange, expected_exchange):
        reasons.append("metadata_exchange_conflict")
    if actual_currency and actual_currency != expected_currency:
        reasons.append("metadata_currency_conflict")
    if actual_country and not _country_matches(actual_country, expected_country):
        reasons.append("metadata_country_conflict")
    if upper.endswith(".AB"):
        reasons.append("legacy_symbol_alias")
    return reasons


def _reason_codes(
    symbol: str,
    row: Sequence[Any] | None,
    columns: Mapping[str, int],
) -> list[str]:
    if row is None:
        return ["missing_response_row"]

    reasons: list[str] = []
    row_symbol = _cell(row, columns["symbol"])
    name = _cell(row, columns["name"])
    price = _cell(row, columns["price"])
    provider = _cell(row, columns["provider"])

    if _is_blank(row_symbol):
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

    diagnostic_text = _diagnostic_text(row, columns)
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
            "provider_circuit_open",
        )
    ):
        reasons.append("provider_reported_unavailable")

    reasons.extend(_provider_warning_codes(diagnostic_text))
    reasons.extend(_metadata_reason_codes(symbol, row, columns))
    return list(dict.fromkeys(reasons))


def _availability_class(
    *,
    collector_data_free: bool,
    decision_blocked: bool,
    reasons: Sequence[str],
) -> str:
    reason_set = set(reasons)
    if not decision_blocked:
        return "VERIFIED_FRESH"
    if "invalid_symbol_shape" in reason_set:
        return "INVALID_SYMBOL_SHAPE"
    if "missing_response_row" in reason_set:
        return "MISSING_RESPONSE_ROW"
    if "identity_blocked_or_quarantined" in reason_set:
        return "IDENTITY_BLOCKED"
    if reason_set.intersection(
        {
            "provider_http_402",
            "provider_circuit_open",
            "provider_http_404",
            "provider_unhealthy_eodhd",
            "provider_timeout",
            "missing_provider",
            "provider_error_or_placeholder",
            "provider_reported_unavailable",
        }
    ):
        return "PROVIDER_UNAVAILABLE_OR_ERROR"
    if reason_set.intersection(
        {
            "metadata_exchange_conflict",
            "metadata_currency_conflict",
            "metadata_country_conflict",
        }
    ):
        return "MARKET_METADATA_CONFLICT"
    if collector_data_free:
        return "MISSING_VERIFIED_FACTS"
    return "DECISION_BLOCKED"


def _build_record(
    *,
    symbol: str,
    row: Sequence[Any] | None,
    columns: Mapping[str, int],
    collector_data_free: bool,
    reasons: Sequence[str],
) -> dict[str, Any]:
    blocking = any(reason in _BLOCKING_REASONS for reason in reasons)
    decision_blocked = collector_data_free or blocking
    availability = _availability_class(
        collector_data_free=collector_data_free,
        decision_blocked=decision_blocked,
        reasons=reasons,
    )
    expected = _expected_market(symbol)
    return {
        "symbol": symbol,
        "symbol_bucket": _symbol_bucket(symbol),
        "availability_class": availability,
        "collector_data_free": collector_data_free,
        "decision_eligible": not decision_blocked,
        "reason_codes": list(reasons),
        "expected_exchange": expected[0] if expected else "",
        "expected_currency": expected[1] if expected else "",
        "expected_country": expected[2] if expected else "",
        "name": _text(_cell(row, columns["name"])),
        "current_price": _cell(row, columns["price"]),
        "data_provider": _text(_cell(row, columns["provider"])),
        "exchange": _text(_cell(row, columns["exchange"])),
        "currency": _text(_cell(row, columns["currency"])),
        "country": _text(_cell(row, columns["country"])),
        "asset_class": _text(_cell(row, columns["asset_class"])),
        "investability_status": _text(
            _cell(row, columns["investability_status"])
        ),
        "final_action": _text(_cell(row, columns["final_action"])),
        "warnings": _text(_cell(row, columns["warnings"])),
        "block_reason": _text(_cell(row, columns["block_reason"])),
        "row_source": _text(_cell(row, columns["row_source"])),
        "last_updated_utc": _text(_cell(row, columns["last_updated_utc"])),
        "last_updated_riyadh": _text(
            _cell(row, columns["last_updated_riyadh"])
        ),
    }


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
    eligibility: list[dict[str, Any]] = []
    data_free_records: list[dict[str, Any]] = []
    reason_counts: Counter[str] = Counter()
    class_counts: Counter[str] = Counter()
    suffix_counts: Counter[str] = Counter()
    reason_suffix_counts: Counter[str] = Counter()
    provider_warning_counts: Counter[str] = Counter()

    collector_fresh_count = 0
    metadata_conflict_rows = 0
    invalid_symbol_shape_rows = 0

    for symbol in requested_symbols:
        row = rows_by_symbol.get(symbol)
        collector_data_free = row is None or not benchmark._row_good(headers, row)
        reasons = _reason_codes(symbol, row, columns)
        if collector_data_free and not reasons:
            reasons = ["unclassified_data_free"]

        record = _build_record(
            symbol=symbol,
            row=row,
            columns=columns,
            collector_data_free=collector_data_free,
            reasons=reasons,
        )
        eligibility.append(record)

        if collector_data_free:
            data_free_records.append(record)
        else:
            collector_fresh_count += 1

        if not record["decision_eligible"]:
            class_counts[record["availability_class"]] += 1
            suffix_counts[record["symbol_bucket"]] += 1
            for reason in reasons:
                reason_counts[reason] += 1
                reason_suffix_counts[f"{reason}|{record['symbol_bucket']}"] += 1

        provider_codes = set(_provider_warning_codes(_diagnostic_text(row, columns)))
        for code in provider_codes:
            provider_warning_counts[code] += 1

        reason_set = set(reasons)
        if reason_set.intersection(
            {
                "metadata_exchange_conflict",
                "metadata_currency_conflict",
                "metadata_country_conflict",
            }
        ):
            metadata_conflict_rows += 1
        if "invalid_symbol_shape" in reason_set:
            invalid_symbol_shape_rows += 1

    expected_data_free = int(collector_metrics.get("symbols_data_free") or 0)
    evidence_consistent = (
        len(data_free_records) == expected_data_free
        and collector_fresh_count == int(collector_metrics.get("symbols_fresh") or 0)
        and len(requested_symbols)
        == int(collector_metrics.get("symbols_requested") or 0)
        and len(eligibility) == len(requested_symbols)
    )

    decision_eligible_count = sum(
        1 for record in eligibility if record["decision_eligible"]
    )
    provider_warning_summary = {
        "http_402_rows": provider_warning_counts.get("provider_http_402", 0),
        "circuit_open_rows": provider_warning_counts.get("provider_circuit_open", 0),
        "http_404_rows": provider_warning_counts.get("provider_http_404", 0),
        "provider_unhealthy_eodhd_rows": provider_warning_counts.get(
            "provider_unhealthy_eodhd",
            0,
        ),
        "timeout_rows": provider_warning_counts.get("provider_timeout", 0),
    }

    return {
        "schema_version": "1.2",
        "diagnostic_version": DIAGNOSTIC_VERSION,
        "mode": "read_live_fetch_no_write_decision_eligibility",
        "page": page,
        "backend": backend_url,
        "no_workbook_writes": True,
        "evidence_consistent": evidence_consistent,
        "summary": {
            "requested_symbols": len(requested_symbols),
            "fresh_symbols": collector_fresh_count,
            "data_free_symbols": len(data_free_records),
            "decision_eligible_symbols": decision_eligible_count,
            "decision_blocked_symbols": len(requested_symbols)
            - decision_eligible_count,
            "unclassified_data_free": reason_counts.get(
                "unclassified_data_free",
                0,
            ),
            "metadata_conflict_rows": metadata_conflict_rows,
            "invalid_symbol_shape_rows": invalid_symbol_shape_rows,
            "provider_warning_counts": provider_warning_summary,
            "reason_counts": dict(reason_counts.most_common()),
            "availability_class_counts": dict(class_counts.most_common()),
            "symbol_bucket_counts": dict(suffix_counts.most_common()),
            "reason_symbol_bucket_counts": dict(reason_suffix_counts.most_common()),
        },
        "collector_metrics": dict(collector_metrics),
        "runner_result": dict(result_payload),
        "planned_writes": [dict(item) for item in planned_writes],
        "clear_requests": [dict(item) for item in clear_requests],
        "data_free_rows": data_free_records,
        "decision_eligibility": eligibility,
    }


_CSV_COLUMNS = [
    "symbol",
    "symbol_bucket",
    "availability_class",
    "collector_data_free",
    "decision_eligible",
    "reason_codes",
    "expected_exchange",
    "expected_currency",
    "expected_country",
    "name",
    "current_price",
    "data_provider",
    "exchange",
    "currency",
    "country",
    "asset_class",
    "investability_status",
    "final_action",
    "warnings",
    "block_reason",
    "row_source",
    "last_updated_utc",
    "last_updated_riyadh",
]


def _write_csv(path: str, records: Sequence[Mapping[str, Any]]) -> None:
    if not path:
        return
    output = Path(path)
    output.parent.mkdir(parents=True, exist_ok=True)
    with output.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=_CSV_COLUMNS)
        writer.writeheader()
        for record in records:
            row = dict(record)
            row["reason_codes"] = ";".join(record.get("reason_codes") or [])
            writer.writerow({key: row.get(key, "") for key in _CSV_COLUMNS})


async def run_diagnostic(args: argparse.Namespace) -> tuple[int, dict[str, Any]]:
    if int(args.concurrency) != 1:
        raise ValueError(
            "decision-eligibility diagnostics must run on the accepted "
            "sequential baseline with --concurrency 1"
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
    payload = build_diagnostic_payload(
        headers=collector.headers,
        requested_symbols=list(collector.requested_symbols),
        rows_by_symbol=collector._rows_by_symbol(),
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
    parser.add_argument(
        "--eligibility-csv-out",
        default="decision_eligibility.csv",
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = create_parser().parse_args(argv)
    try:
        code, payload = asyncio.run(run_diagnostic(args))
    except Exception as exc:
        print(
            f"::error::DATA_FREE_DIAGNOSTIC_FAILED: "
            f"{type(exc).__name__}: {exc}"
        )
        return 3

    rendered = json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True)
    print(rendered)
    if args.json_out:
        output = Path(args.json_out)
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_text(rendered + "\n", encoding="utf-8")
    _write_csv(args.csv_out, payload.get("data_free_rows") or [])
    _write_csv(
        args.eligibility_csv_out,
        payload.get("decision_eligibility") or [],
    )
    return code


if __name__ == "__main__":
    raise SystemExit(main())
