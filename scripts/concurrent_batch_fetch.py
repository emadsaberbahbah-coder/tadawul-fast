#!/usr/bin/env python3
"""Bounded-concurrency market batch fetcher for ``run_dashboard_sync``.

The module changes provider-fetch scheduling and targeted recovery only. The
production runner keeps ownership of symbol read-back, persistence,
keep-last-good, identity firewalls, Sheet publication, and exit-code policy.
"""
from __future__ import annotations

import asyncio
import math
import os
import statistics
import time
from typing import Any, Awaitable, Callable, Iterable, Sequence

VERSION = "1.3.1"
_METRICS: dict[str, dict[str, Any]] = {}

_RECOVERY_SUFFIX_VARIANTS: tuple[tuple[str, str], ...] = (
    (".AD", ".ADX"),
    (".AB", ".ADX"),  # legacy project spelling
    (".PS", ".PSE"),
)


def provider_recovery_variants(symbol: str) -> list[str]:
    """Return a small, deterministic provider-safe variant set for recovery.

    The original canonical symbol is always first. Variants are used only after
    the normal fetch failed or returned a data-free stub; they never broaden the
    primary page request or bypass the final identity firewall.
    """
    canonical = str(symbol or "").strip().upper()
    variants = [canonical] if canonical else []
    for source_suffix, provider_suffix in _RECOVERY_SUFFIX_VARIANTS:
        if canonical.endswith(source_suffix):
            variants.append(canonical[: -len(source_suffix)] + provider_suffix)
    if canonical == "BNY.US":
        # Current ticker first; stale BK spellings are last-resort lifecycle aliases.
        variants.extend(["BNY", "BK.US", "BK"])
    result: list[str] = []
    seen: set[str] = set()
    for value in variants:
        if value and value not in seen:
            seen.add(value)
            result.append(value)
    return result


def _int(name: str, default: int, lo: int, hi: int) -> int:
    try:
        value = int(float(os.getenv(name, str(default))))
    except Exception:
        value = default
    return max(lo, min(hi, value))


def _bool(name: str, default: bool) -> bool:
    raw = (os.getenv(name) or ("1" if default else "0")).strip().lower()
    if raw in {"1", "true", "yes", "on"}:
        return True
    if raw in {"0", "false", "no", "off"}:
        return False
    return default


def concurrency() -> int:
    """Configured provider requests; default 1 until the staged gate passes."""
    return _int("TFB_SYNC_BATCH_CONCURRENCY", 1, 1, 6)


def outer_retries() -> int:
    """Extra batch-level passes after BackendClient exhausts its own retries."""
    return _int("TFB_SYNC_BATCH_OUTER_RETRIES", 1, 0, 2)


def targeted_recovery_enabled() -> bool:
    return _bool("TFB_SYNC_TARGET_RECOVERY", True)


def targeted_recovery_max() -> int:
    return _int("TFB_SYNC_TARGET_RECOVERY_MAX", 120, 1, 1000)


def targeted_recovery_batch_size() -> int:
    return _int("TFB_SYNC_TARGET_RECOVERY_BATCH_SIZE", 10, 1, 50)


def targeted_recovery_rounds() -> int:
    return _int("TFB_SYNC_TARGET_RECOVERY_ROUNDS", 1, 0, 2)


def get_metrics(request_id: str) -> dict[str, Any] | None:
    value = _METRICS.get(str(request_id or ""))
    return dict(value) if value is not None else None


def _percentile(values: Sequence[float], percentile: float) -> float:
    clean = sorted(float(value) for value in values if value is not None)
    if not clean:
        return 0.0
    if len(clean) == 1:
        return clean[0]
    position = (len(clean) - 1) * percentile
    lower = math.floor(position)
    upper = math.ceil(position)
    if lower == upper:
        return clean[lower]
    weight = position - lower
    return clean[lower] * (1.0 - weight) + clean[upper] * weight


def _same_headers(left: Sequence[Any], right: Sequence[Any]) -> bool:
    return [str(value or "").strip() for value in left] == [
        str(value or "").strip() for value in right
    ]


def build(
    sync: Any,
) -> Callable[
    ...,
    Awaitable[tuple[list[Any], list[list[Any]], str | None, str | None]],
]:
    """Build a concurrent fetch function against the current runner module."""

    def _column(headers: Sequence[Any], attr: str, fallback: Sequence[str]) -> int:
        aliases = getattr(sync, attr, frozenset(fallback))
        return sync._guard_find_col(list(headers), aliases)

    def _positive(value: Any) -> bool:
        try:
            parsed = float(str(value).replace(",", "").strip())
            return 0.0 < parsed < 1e15
        except Exception:
            return False

    def _provider_error(value: Any) -> bool:
        checker = getattr(sync, "_klg_provider_is_error", None)
        if callable(checker):
            try:
                return bool(checker(value))
            except Exception:
                pass
        normalized = "".join(
            char for char in str(value or "").strip().casefold() if char.isalnum()
        )
        return normalized in {"fallbackerror", "error", "unavailable", "none"}

    def _row_good(headers: Sequence[Any], row: Sequence[Any]) -> bool:
        symbol_index = _column(headers, "_GUARD_SYMBOL_ALIASES", ("symbol", "ticker"))
        name_index = _column(headers, "_GUARD_NAME_ALIASES", ("name", "companyname"))
        price_index = _column(headers, "_XPAGE_PRICE_ALIASES", ("currentprice", "price", "lastprice"))
        provider_index = _column(headers, "_KLG_PROVIDER_ALIASES", ("dataprovider", "provider", "datasource"))
        if symbol_index < 0 or symbol_index >= len(row):
            return False
        if sync._guard_is_blank(row[symbol_index]):
            return False
        if name_index >= 0:
            if name_index >= len(row) or sync._guard_is_blank(row[name_index]):
                return False
        if price_index >= 0:
            if price_index >= len(row) or not _positive(row[price_index]):
                return False
        if provider_index >= 0 and provider_index < len(row):
            if _provider_error(row[provider_index]):
                return False
        return True

    def _requested_order(symbols: Sequence[str]) -> list[str]:
        ordered: list[str] = []
        seen: set[str] = set()
        for raw in symbols:
            symbol = sync.canonicalize_symbol(raw)
            if symbol and symbol not in seen:
                seen.add(symbol)
                ordered.append(symbol)
        return ordered

    def _rows_by_symbol(
        headers: Sequence[Any],
        rows: Sequence[Sequence[Any]],
    ) -> dict[str, list[Any]]:
        symbol_index = _column(headers, "_GUARD_SYMBOL_ALIASES", ("symbol", "ticker"))
        if symbol_index < 0:
            return {}
        result: dict[str, list[Any]] = {}
        for raw in rows:
            if (
                not isinstance(raw, (list, tuple))
                or symbol_index >= len(raw)
                or sync._guard_is_blank(raw[symbol_index])
            ):
                continue
            row = list(raw)
            symbol = sync.canonicalize_symbol(row[symbol_index])
            if not symbol:
                continue
            row[symbol_index] = symbol
            result.setdefault(symbol, row)
        return result

    def _classify(
        headers: Sequence[Any],
        rows: Sequence[Sequence[Any]],
        symbols: Sequence[str],
    ) -> tuple[dict[str, list[Any]], list[str], list[str], list[str]]:
        ordered = _requested_order(symbols)
        mapping = _rows_by_symbol(headers, rows)
        good = [symbol for symbol in ordered if symbol in mapping and _row_good(headers, mapping[symbol])]
        data_free = [symbol for symbol in ordered if symbol in mapping and symbol not in set(good)]
        missing = [symbol for symbol in ordered if symbol not in mapping]
        return mapping, good, data_free, missing

    async def one(
        backend: Any,
        idx: int,
        batch: Sequence[str],
        payload: dict[str, Any],
        endpoints: Sequence[str],
        request_id: str,
        *,
        delay_ms: int = 0,
        require_good: bool = False,
    ) -> dict[str, Any]:
        if delay_ms > 0:
            await asyncio.sleep(delay_ms / 1000.0)
        if sync._time_budget_exceeded():
            return {
                "i": idx,
                "b": list(batch),
                "h": [],
                "r": [],
                "e": "time budget exhausted",
                "ep": None,
                "attempted": False,
                "ms": 0.0,
                "code": 0,
            }

        body = dict(payload)
        body.update(
            tickers=list(batch),
            symbols=list(batch),
            limit=min(sync._request_limit_ceiling(), max(1, len(batch))),
            request_id=request_id,
        )
        started = time.perf_counter()
        last_error: str | None = None
        last_code = 0
        for endpoint in endpoints:
            data, error, code = await backend.post_json(endpoint, body)
            last_code = int(code or 0)
            if error:
                last_error = f"{endpoint} -> {error}"
                continue
            if not isinstance(data, dict):
                last_error = f"{endpoint} -> Non-dict response"
                continue
            headers, raw_rows = sync._extract_table_payload(data)
            if not headers:
                last_error = f"{endpoint} -> Missing headers"
                continue
            rows = list(sync._rectify_matrix(headers, raw_rows) or [])
            if require_good:
                _, good, _, _ = _classify(headers, rows, batch)
                if not good:
                    last_error = f"{endpoint} -> no usable rows"
                    continue
            return {
                "i": idx,
                "b": list(batch),
                "h": list(headers),
                "r": rows,
                "e": None,
                "ep": endpoint,
                "attempted": True,
                "ms": (time.perf_counter() - started) * 1000.0,
                "code": last_code,
            }
        return {
            "i": idx,
            "b": list(batch),
            "h": [],
            "r": [],
            "e": last_error or "all endpoints failed",
            "ep": None,
            "attempted": True,
            "ms": (time.perf_counter() - started) * 1000.0,
            "code": last_code,
        }

    async def fan(
        backend: Any,
        indexed_batches: Sequence[tuple[int, Sequence[str]]],
        payload: dict[str, Any],
        endpoints: Sequence[str],
        request_id: str,
        *,
        phase: str,
        max_concurrency: int,
        delay_ms: int,
        require_good: bool = False,
    ) -> list[dict[str, Any]]:
        semaphore = asyncio.Semaphore(max_concurrency)

        async def guarded(
            position: int,
            idx: int,
            batch: Sequence[str],
        ) -> dict[str, Any]:
            async with semaphore:
                stagger = (position % max_concurrency) * delay_ms
                return await one(
                    backend,
                    idx,
                    batch,
                    payload,
                    endpoints,
                    f"{request_id}-{phase}-{idx + 1}",
                    delay_ms=stagger,
                    require_good=require_good,
                )

        tasks = [
            asyncio.create_task(guarded(position, idx, batch))
            for position, (idx, batch) in enumerate(indexed_batches)
        ]
        return list(await asyncio.gather(*tasks)) if tasks else []

    async def provider_variant_fan(
        backend: Any,
        indexed_batches: Sequence[tuple[int, Sequence[str]]],
        payload: dict[str, Any],
        endpoints: Sequence[str],
        request_id: str,
        *,
        phase: str,
        max_concurrency: int,
        delay_ms: int,
    ) -> list[dict[str, Any]]:
        """Fetch recovery aliases and map accepted rows to requested symbols."""
        semaphore = asyncio.Semaphore(max_concurrency)

        async def guarded(
            position: int,
            idx: int,
            originals: Sequence[str],
        ) -> dict[str, Any]:
            async with semaphore:
                alias_to_original: dict[str, str] = {}
                alias_rank: dict[str, int] = {}
                expanded: list[str] = []
                for original in originals:
                    canonical_original = sync.canonicalize_symbol(original)
                    for rank, alias in enumerate(provider_recovery_variants(canonical_original)):
                        alias_key = str(alias or "").strip().upper()
                        canonical_alias = sync.canonicalize_symbol(alias)
                        for key in (alias_key, canonical_alias):
                            if key and key not in alias_to_original:
                                alias_to_original[key] = canonical_original
                                alias_rank[key] = rank
                        if alias_key and alias_key not in expanded:
                            expanded.append(alias_key)

                stagger = (position % max_concurrency) * delay_ms
                outcome = await one(
                    backend,
                    idx,
                    expanded,
                    payload,
                    endpoints,
                    f"{request_id}-{phase}-{idx + 1}",
                    delay_ms=stagger,
                    require_good=False,
                )
                item = dict(outcome)
                item["b"] = list(originals)
                item["provider_variants"] = list(expanded)
                if not item.get("h"):
                    return item

                headers = list(item["h"])
                symbol_index = _column(
                    headers, "_GUARD_SYMBOL_ALIASES", ("symbol", "ticker")
                )
                selected: dict[str, tuple[int, list[Any]]] = {}
                for raw in item.get("r", []):
                    if (
                        not isinstance(raw, (list, tuple))
                        or symbol_index < 0
                        or symbol_index >= len(raw)
                        or sync._guard_is_blank(raw[symbol_index])
                    ):
                        continue
                    row = list(raw)
                    raw_key = str(row[symbol_index] or "").strip().upper()
                    canonical_key = sync.canonicalize_symbol(raw_key)
                    original = alias_to_original.get(raw_key) or alias_to_original.get(
                        canonical_key
                    )
                    if not original:
                        continue
                    row[symbol_index] = original
                    if not _row_good(headers, row):
                        continue
                    rank = alias_rank.get(raw_key, alias_rank.get(canonical_key, 999))
                    previous = selected.get(original)
                    if previous is None or rank < previous[0]:
                        selected[original] = (rank, row)
                item["r"] = [
                    selected[sync.canonicalize_symbol(original)][1]
                    for original in originals
                    if sync.canonicalize_symbol(original) in selected
                ]
                return item

        tasks = [
            asyncio.create_task(guarded(position, idx, batch))
            for position, (idx, batch) in enumerate(indexed_batches)
        ]
        return list(await asyncio.gather(*tasks)) if tasks else []

    def normalize_outcomes(
        outcomes: Iterable[dict[str, Any]],
        canonical_headers: Sequence[Any],
    ) -> list[dict[str, Any]]:
        normalized: list[dict[str, Any]] = []
        for outcome in outcomes:
            item = dict(outcome)
            if item.get("h") and not _same_headers(canonical_headers, item["h"]):
                item["e"] = "header mismatch against endpoint-resolving batch"
                item["h"] = []
                item["r"] = []
            normalized.append(item)
        return normalized

    def merge(
        headers: Sequence[Any],
        outcomes: Sequence[dict[str, Any]],
        symbols: Sequence[str],
        result: Any,
    ) -> list[list[Any]]:
        symbol_index = _column(headers, "_GUARD_SYMBOL_ALIASES", ("symbol", "ticker"))
        if not sync._batch_identity_enabled() or symbol_index < 0:
            return [
                list(row)
                for outcome in sorted(outcomes, key=lambda item: item["i"])
                for row in outcome.get("r", [])
            ]

        rows_by_symbol: dict[str, list[Any]] = {}
        bleed = duplicate = blank = 0
        for outcome in sorted(outcomes, key=lambda item: item["i"]):
            requested = {sync.canonicalize_symbol(value) for value in outcome["b"]}
            requested.discard("")
            for raw in outcome.get("r", []):
                if (
                    not isinstance(raw, (list, tuple))
                    or symbol_index >= len(raw)
                    or sync._guard_is_blank(raw[symbol_index])
                ):
                    blank += 1
                    continue
                row = list(raw)
                symbol = sync.canonicalize_symbol(row[symbol_index])
                row[symbol_index] = symbol
                if symbol not in requested:
                    bleed += 1
                    continue
                if symbol in rows_by_symbol:
                    duplicate += 1
                    continue
                rows_by_symbol[symbol] = row

        if bleed or duplicate or blank:
            message = (
                f"{sync._BATCH_IDENTITY_TAG} concurrent fold dropped "
                f"cross_batch={bleed} duplicate={duplicate} blank={blank}"
            )
            result.warnings.append(message)
            sync.logger.warning(message)

        return [
            rows_by_symbol[symbol]
            for symbol in _requested_order(symbols)
            if symbol in rows_by_symbol
        ]

    async def concurrent(
        backend: Any,
        task: Any,
        symbols: list[str],
        payload: dict[str, Any],
        gateway: str,
        result: Any,
    ) -> tuple[list[Any], list[list[Any]], str | None, str | None]:
        started = time.perf_counter()
        size = sync._symbol_batch_size()
        batches = sync.build_isolated_batches(symbols, size)
        max_concurrency = concurrency()
        candidates = sync._endpoint_candidates_for_gateway(gateway)
        request_id = str(result.request_id)
        metrics: dict[str, Any] = {
            "version": VERSION,
            "mode": "concurrent_targeted_recovery",
            "page": task.sheet_name,
            "concurrency": max_concurrency,
            "batch_size": size,
            "batches_total": len(batches),
            "symbols_requested": len(_requested_order(symbols)),
        }
        _METRICS[request_id] = metrics
        if not batches:
            metrics.update(
                batches_attempted=0,
                batches_succeeded=0,
                batches_failed=0,
                batches_unattempted=0,
                symbols_attempted=0,
                symbols_returned=0,
                symbols_fresh=0,
                symbols_data_free=0,
                symbols_missing=0,
                symbols_failed=0,
                symbols_unattempted=0,
                returned_coverage_pct=0.0,
                fresh_coverage_pct=0.0,
                elapsed_ms=0,
                endpoint_resolve_batches=0,
                retry_rounds=0,
                targeted_recovery_requested=0,
                targeted_recovery_healed=0,
            )
            if hasattr(result, "batch_metrics"):
                result.batch_metrics = dict(metrics)
            return [], [], None, "no batches"

        outcomes: list[dict[str, Any]] = []
        recovery_outcomes: list[dict[str, Any]] = []
        endpoint: str | None = None
        headers: list[Any] = []
        last_error: str | None = None
        resolve_count = 0

        for idx, batch in enumerate(batches):
            if sync._time_budget_exceeded():
                break
            outcome = await one(
                backend,
                idx,
                batch,
                payload,
                candidates,
                f"{request_id}-resolve-{idx + 1}",
            )
            outcomes.append(outcome)
            resolve_count += 1
            if outcome.get("e"):
                last_error = outcome["e"]
            if outcome.get("h"):
                endpoint = outcome["ep"]
                headers = list(outcome["h"])
                break

        attempted_indices = {int(item["i"]) for item in outcomes}
        unresolved_rest = [
            (idx, batch)
            for idx, batch in enumerate(batches)
            if idx not in attempted_indices
        ]
        if endpoint:
            fetched = await fan(
                backend,
                unresolved_rest,
                payload,
                (endpoint,),
                request_id,
                phase="batch",
                max_concurrency=max_concurrency,
                delay_ms=sync._batch_delay_ms(),
            )
            outcomes.extend(normalize_outcomes(fetched, headers))
        else:
            outcomes.extend(
                {
                    "i": idx,
                    "b": list(batch),
                    "h": [],
                    "r": [],
                    "e": "endpoint unresolved",
                    "ep": None,
                    "attempted": False,
                    "ms": 0.0,
                    "code": 0,
                }
                for idx, batch in unresolved_rest
            )

        retry_rounds_done = 0
        for retry_round in range(1, outer_retries() + 1):
            failed_batches = [
                (int(item["i"]), list(item["b"]))
                for item in outcomes
                if item.get("attempted") and not item.get("h")
            ]
            if not failed_batches or not endpoint or sync._time_budget_exceeded():
                break
            retried = await fan(
                backend,
                failed_batches,
                payload,
                (endpoint,),
                request_id,
                phase=f"retry{retry_round}",
                max_concurrency=max_concurrency,
                delay_ms=sync._batch_delay_ms(),
            )
            retried = normalize_outcomes(retried, headers)
            by_index = {int(item["i"]): item for item in outcomes}
            for item in retried:
                index = int(item["i"])
                if item.get("h"):
                    by_index[index] = item
                elif item.get("e"):
                    last_error = item["e"]
            outcomes = [by_index[index] for index in sorted(by_index)]
            retry_rounds_done += 1

        outcomes = sorted(outcomes, key=lambda item: int(item["i"]))
        if not headers:
            headers = list(next((item["h"] for item in outcomes if item.get("h")), []))
        rows = merge(headers, outcomes, symbols, result) if headers else []
        initial_mapping, initial_good, initial_data_free, initial_missing = _classify(
            headers, rows, symbols
        ) if headers else ({}, [], [], _requested_order(symbols))
        final_mapping = dict(initial_mapping)
        recovery_requested: set[str] = set()
        recovery_healed: set[str] = set()

        if (
            headers
            and endpoint
            and targeted_recovery_enabled()
            and targeted_recovery_rounds() > 0
            and not sync._time_budget_exceeded()
        ):
            endpoint_chain = [endpoint] + [candidate for candidate in candidates if candidate != endpoint]
            for recovery_round in range(1, targeted_recovery_rounds() + 1):
                _, good_now, data_free_now, missing_now = _classify(
                    headers,
                    [final_mapping[symbol] for symbol in _requested_order(symbols) if symbol in final_mapping],
                    symbols,
                )
                targets = (missing_now + data_free_now)[: targeted_recovery_max()]
                if not targets or sync._time_budget_exceeded():
                    break
                recovery_requested.update(targets)
                recovery_batches = sync.build_isolated_batches(
                    targets, targeted_recovery_batch_size()
                )
                indexed_recovery = list(enumerate(recovery_batches))
                recovered = await provider_variant_fan(
                    backend,
                    indexed_recovery,
                    payload,
                    tuple(endpoint_chain),
                    request_id,
                    phase=f"target{recovery_round}",
                    max_concurrency=max_concurrency,
                    delay_ms=sync._batch_delay_ms(),
                )
                recovered = normalize_outcomes(recovered, headers)
                recovery_outcomes.extend(recovered)
                recovered_rows = merge(headers, recovered, targets, result)
                recovered_mapping, recovered_good, _, _ = _classify(
                    headers, recovered_rows, targets
                )
                for symbol in recovered_good:
                    final_mapping[symbol] = recovered_mapping[symbol]
                    recovery_healed.add(symbol)

        ordered_symbols = _requested_order(symbols)
        final_rows = [final_mapping[symbol] for symbol in ordered_symbols if symbol in final_mapping]
        _, final_good, final_data_free, final_missing = _classify(
            headers, final_rows, symbols
        ) if headers else ({}, [], [], ordered_symbols)

        attempted = [item for item in outcomes if item.get("attempted")]
        succeeded = [item for item in outcomes if item.get("h")]
        failed_batches = [item for item in attempted if not item.get("h")]
        unattempted = [item for item in outcomes if not item.get("attempted")]
        all_api_outcomes = attempted + [
            item for item in recovery_outcomes if item.get("attempted")
        ]
        durations = [float(item.get("ms") or 0.0) for item in all_api_outcomes]
        symbols_attempted = len(
            {
                sync.canonicalize_symbol(symbol)
                for item in attempted
                for symbol in item["b"]
                if sync.canonicalize_symbol(symbol)
            }
        )
        symbols_unattempted = sum(len(item["b"]) for item in unattempted)
        returned_coverage = 100.0 * len(final_rows) / len(ordered_symbols) if ordered_symbols else 100.0
        fresh_coverage = 100.0 * len(final_good) / len(ordered_symbols) if ordered_symbols else 100.0
        metrics.update(
            endpoint=endpoint,
            endpoint_resolve_batches=resolve_count,
            retry_rounds=retry_rounds_done,
            batches_attempted=len(attempted),
            batches_succeeded=len(succeeded),
            batches_failed=len(failed_batches),
            batches_unattempted=len(unattempted),
            symbols_attempted=symbols_attempted,
            symbols_returned_initial=len(initial_mapping),
            symbols_fresh_initial=len(initial_good),
            symbols_data_free_initial=len(initial_data_free),
            symbols_missing_initial=len(initial_missing),
            data_free_symbols_initial=initial_data_free[:100],
            missing_symbols_initial=initial_missing[:100],
            targeted_recovery_requested=len(recovery_requested),
            targeted_recovery_healed=len(recovery_healed),
            targeted_recovery_healed_symbols=sorted(recovery_healed)[:100],
            targeted_recovery_batches=len(recovery_outcomes),
            symbols_returned=len(final_rows),
            symbols_fresh=len(final_good),
            symbols_data_free=len(final_data_free),
            symbols_missing=len(final_missing),
            symbols_failed=len(final_data_free),
            data_free_symbols=final_data_free[:100],
            missing_symbols=final_missing[:100],
            symbols_unattempted=symbols_unattempted,
            returned_coverage_pct=round(returned_coverage, 3),
            fresh_coverage_pct=round(fresh_coverage, 3),
            api_symbol_attempts=sum(len(item["b"]) for item in all_api_outcomes),
            http_429=sum(1 for item in all_api_outcomes if item.get("code") == 429),
            http_5xx=sum(
                1
                for item in all_api_outcomes
                if 500 <= int(item.get("code") or 0) < 600
            ),
            elapsed_ms=round((time.perf_counter() - started) * 1000.0),
            mean_batch_ms=round(statistics.fmean(durations)) if durations else 0,
            p50_batch_ms=round(_percentile(durations, 0.50)),
            p95_batch_ms=round(_percentile(durations, 0.95)),
            max_batch_ms=round(max(durations, default=0.0)),
        )
        _METRICS[request_id] = dict(metrics)
        if hasattr(result, "batch_metrics"):
            result.batch_metrics = dict(metrics)

        message = (
            f"[BATCH-CONCURRENCY v{VERSION}] page={task.sheet_name} "
            f"concurrency={max_concurrency} batch_size={size} "
            f"batches={len(succeeded)}/{len(batches)} "
            f"attempted={symbols_attempted} returned={len(final_rows)} "
            f"good_fresh={len(final_good)} data_free={len(final_data_free)} "
            f"missing={len(final_missing)} recovered={len(recovery_healed)}/"
            f"{len(recovery_requested)} coverage={fresh_coverage:.1f}% "
            f"elapsed_ms={metrics['elapsed_ms']} p95_batch_ms={metrics['p95_batch_ms']}"
        )
        result.warnings.append(message)
        sync.logger.info(message)
        return headers, final_rows, endpoint, last_error

    return concurrent


def install(sync: Any) -> None:
    """Install a reversible dispatcher on an imported production runner."""
    if getattr(sync, "_TFB_CONCURRENT_BATCH_FETCH_INSTALLED", False):
        return

    original = sync._fetch_market_rows_batched
    concurrent_fetch = build(sync)

    async def dispatch(*args: Any, **kwargs: Any):
        if concurrency() <= 1:
            return await original(*args, **kwargs)
        try:
            return await concurrent_fetch(*args, **kwargs)
        except Exception as exc:
            result = args[5] if len(args) > 5 else kwargs.get("res")
            message = (
                f"[BATCH-CONCURRENCY v{VERSION}] adapter failure; "
                f"falling back to sequential fetch: {exc}"
            )
            if result is not None and hasattr(result, "warnings"):
                result.warnings.append(message)
            sync.logger.exception(message)
            return await original(*args, **kwargs)

    sync._fetch_market_rows_batched = dispatch
    sync._TFB_CONCURRENT_BATCH_FETCH_ORIGINAL = original
    sync._TFB_CONCURRENT_BATCH_FETCH_INSTALLED = True
