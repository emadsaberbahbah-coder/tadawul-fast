#!/usr/bin/env python3
"""Read-only benchmark for the production Python dashboard refresh path.

The benchmark reads the live page universe and calls the same Render/backend
routes and guards used by ``run_dashboard_sync.py``. It replaces the final
Sheet writer with an in-memory sink and disables the optional _Run_Log append,
so no workbook cell is changed.

Concurrency defaults to the exact sequential production path (1). A higher
value must be supplied explicitly after the sequential deployment gate passes.
"""
from __future__ import annotations

import argparse
import asyncio
import json
import os
import time
from pathlib import Path
from typing import Any, Sequence

from scripts import run_dashboard_sync as sync

BENCHMARK_VERSION = "1.2.1"


class NoWriteSheets(sync.SheetsWriter):
    """Production reader plus a write sink that never calls Google write APIs."""

    def __init__(self) -> None:
        super().__init__()
        self.planned_writes: list[dict[str, Any]] = []
        self.clear_requests: list[dict[str, Any]] = []

    def clear_from(self, spreadsheet_id: str, sheet_name: str, start_a1: str) -> None:
        self.clear_requests.append({"sheet_name": sheet_name, "start_a1": start_a1})

    def write_table(
        self,
        spreadsheet_id: str,
        sheet_name: str,
        start_a1: str,
        headers: list[Any],
        rows: list[list[Any]],
    ) -> int:
        self.planned_writes.append(
            {
                "sheet_name": sheet_name,
                "start_a1": start_a1,
                "columns": len(headers or []),
                "rows": len(rows or []),
            }
        )
        return len(rows or [])


def _task_for(value: str) -> sync.TaskSpec:
    wanted = sync._guard_norm(value)
    for task in sync._default_tasks():
        if wanted in {sync._guard_norm(task.key), sync._guard_norm(task.sheet_name)}:
            return task
    raise ValueError(f"Unknown benchmark page/key: {value}")


def _set_runtime_env(args: argparse.Namespace) -> None:
    os.environ["TFB_SYNC_BATCH_CONCURRENCY"] = str(args.concurrency)
    os.environ["TFB_SYNC_SYMBOL_BATCH_SIZE"] = str(args.batch_size)
    os.environ["TFB_SYNC_BATCH_OUTER_RETRIES"] = str(args.outer_retries)
    os.environ["TFB_SYNC_TIME_BUDGET_SEC"] = str(args.time_budget)
    os.environ["TFB_SYNC_IDFW_RUNLOG"] = "0"
    os.environ["TFB_XPAGE_PRICE_CHECK"] = "0"
    os.environ.setdefault("TFB_SYNC_TARGET_RECOVERY", "1")
    os.environ.setdefault("TFB_SYNC_TARGET_RECOVERY_MAX", "120")
    os.environ.setdefault("TFB_SYNC_TARGET_RECOVERY_BATCH_SIZE", "10")
    os.environ.setdefault("TFB_SYNC_TARGET_RECOVERY_ROUNDS", "1")
    sync._TIME_BUDGET_START = time.monotonic()


def _metric_int(metrics: dict[str, Any], key: str) -> int | None:
    value = metrics.get(key)
    if value is None or isinstance(value, bool):
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


async def run_benchmark(args: argparse.Namespace) -> tuple[int, dict[str, Any]]:
    _set_runtime_env(args)
    task = _task_for(args.page)
    sheet_id = sync._default_spreadsheet_id(args.sheet_id)
    if not sheet_id:
        raise RuntimeError("Spreadsheet ID is required")

    backend_url = (args.backend or sync._default_backend_url()).rstrip("/")
    backend = sync.BackendClient(
        backend_url,
        timeout_sec=float(args.timeout),
        token=sync._env_token(),
    )
    sheets = NoWriteSheets()
    started = time.perf_counter()
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
        await backend.close()

    elapsed_ms = round((time.perf_counter() - started) * 1000.0)
    result_payload = result.to_dict()
    metrics = dict(result_payload.get("batch_metrics") or {})
    requested = int(metrics.get("symbols_requested") or result.symbols_requested or 0)
    returned = _metric_int(metrics, "symbols_returned")
    fresh = _metric_int(metrics, "symbols_fresh")
    data_free = _metric_int(metrics, "symbols_data_free")
    missing = _metric_int(metrics, "symbols_missing")
    unattempted = _metric_int(metrics, "symbols_unattempted")
    http_429 = _metric_int(metrics, "http_429")
    http_5xx = _metric_int(metrics, "http_5xx")
    recovery_requested = _metric_int(metrics, "targeted_recovery_requested")
    recovery_healed = _metric_int(metrics, "targeted_recovery_healed")

    warnings = [str(item) for item in (result_payload.get("warnings") or [])]
    identity_or_coherence_failure = any(
        ("quarantined " in warning)
        or ("identity-broken" in warning)
        or ("mismatched=" in warning and "mismatched=0" not in warning)
        or ("incoherent=" in warning and "incoherent=0" not in warning)
        for warning in warnings
    )

    required_metrics = {
        "symbols_returned": returned,
        "symbols_fresh": fresh,
        "symbols_data_free": data_free,
        "symbols_missing": missing,
        "symbols_unattempted": unattempted,
        "http_429": http_429,
        "http_5xx": http_5xx,
        "targeted_recovery_requested": recovery_requested,
        "targeted_recovery_healed": recovery_healed,
    }
    missing_acceptance_metrics = sorted(
        key for key, value in required_metrics.items() if value is None
    )
    metrics_complete = not missing_acceptance_metrics
    planned_rows = sum(int(item.get("rows") or 0) for item in sheets.planned_writes)
    universe_preserved = requested > 0 and planned_rows == requested
    complete_fresh_fetch = bool(
        metrics_complete
        and requested
        and returned == requested
        and fresh == requested
        and data_free == 0
        and missing == 0
        and unattempted == 0
        and http_429 == 0
        and http_5xx == 0
        and recovery_healed == recovery_requested
        and universe_preserved
        and not identity_or_coherence_failure
    )

    payload: dict[str, Any] = {
        "schema_version": "1.2",
        "benchmark_version": BENCHMARK_VERSION,
        "runner_version": sync.SCRIPT_VERSION,
        "mode": "read_live_fetch_no_write",
        "no_workbook_writes": True,
        "page": task.sheet_name,
        "backend": backend_url,
        "config": {
            "max_symbols": int(args.max_symbols),
            "batch_size": int(args.batch_size),
            "concurrency": int(args.concurrency),
            "outer_retries": int(args.outer_retries),
            "timeout_sec": float(args.timeout),
            "time_budget_sec": int(args.time_budget),
        },
        "elapsed_ms": elapsed_ms,
        "elapsed_minutes": round(elapsed_ms / 60000.0, 3),
        "result": result_payload,
        "batch_metrics": metrics,
        "planned_writes": sheets.planned_writes,
        "clear_requests": sheets.clear_requests,
        "acceptance": {
            "requested_symbols": requested,
            "returned_symbols": returned,
            "fresh_symbols": fresh,
            "data_free_symbols": data_free,
            "missing_symbols": missing,
            "returned_coverage_pct": metrics.get("returned_coverage_pct"),
            "fresh_coverage_pct": metrics.get("fresh_coverage_pct"),
            "targeted_recovery_requested": recovery_requested,
            "targeted_recovery_healed": recovery_healed,
            "unattempted_symbols": unattempted,
            "http_429": http_429,
            "http_5xx": http_5xx,
            "within_25_minutes": elapsed_ms <= 25 * 60 * 1000,
            "within_35_minutes": elapsed_ms <= 35 * 60 * 1000,
            "universe_preserved": universe_preserved,
            "identity_or_coherence_failure": identity_or_coherence_failure,
            "acceptance_metrics_complete": metrics_complete,
            "missing_acceptance_metrics": missing_acceptance_metrics,
            "complete_fresh_fetch": complete_fresh_fetch,
            "runner_status": result.status,
            "runner_error": result.error,
        },
    }

    if result.status == "failed":
        exit_code = 2
    elif result.status in {"partial", "skipped"} or not complete_fresh_fetch:
        exit_code = 1
    else:
        exit_code = 0
    return exit_code, payload


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
    parser.add_argument("--json-out", default="python_refresh_benchmark.json")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = create_parser().parse_args(argv)
    try:
        code, payload = asyncio.run(run_benchmark(args))
    except Exception as exc:
        print(f"::error::PYTHON_REFRESH_BENCHMARK_FAILED: {type(exc).__name__}: {exc}")
        return 3

    rendered = json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True)
    print(rendered)
    if args.json_out:
        Path(args.json_out).write_text(rendered + "\n", encoding="utf-8")
    return code


if __name__ == "__main__":
    raise SystemExit(main())
