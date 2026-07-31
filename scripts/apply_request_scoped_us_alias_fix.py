#!/usr/bin/env python3
"""Apply the request-scoped bare-US/.US response-alias repair.

This transformer is assertion-heavy by design. It changes only response
membership/folding in the dashboard sync client and its focused tests. It does
not change global symbol canonicalization, provider payloads, scoring, Sheet
writers, or the production concurrency default.
"""
from __future__ import annotations

import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def read(path: str) -> str:
    return (ROOT / path).read_text(encoding="utf-8")


def write(path: str, text: str) -> None:
    (ROOT / path).write_text(text, encoding="utf-8")


def replace_once(text: str, old: str, new: str, *, label: str) -> str:
    count = text.count(old)
    if count != 1:
        raise RuntimeError(f"{label}: expected exactly one match, found {count}")
    return text.replace(old, new, 1)


def replace_count(text: str, old: str, new: str, expected: int, *, label: str) -> str:
    count = text.count(old)
    if count != expected:
        raise RuntimeError(f"{label}: expected {expected} matches, found {count}")
    return text.replace(old, new)


def regex_once(text: str, pattern: str, replacement: str, *, label: str) -> str:
    updated, count = re.subn(pattern, replacement, text, count=1, flags=re.S | re.M)
    if count != 1:
        raise RuntimeError(f"{label}: expected exactly one regex match, found {count}")
    return updated


def patch_runner() -> None:
    path = "scripts/run_dashboard_sync.py"
    text = read(path)

    text = replace_once(
        text,
        'SCRIPT_VERSION = "6.31.0"',
        'SCRIPT_VERSION = "6.32.0"',
        label="runner version constant",
    )
    text = text.replace(
        "TADAWUL FAST BRIDGE — DASHBOARD SYNC RUNNER (v6.31.0)",
        "TADAWUL FAST BRIDGE — DASHBOARD SYNC RUNNER (v6.32.0)",
        1,
    )

    helper = r'''
_US_REQUEST_ALIAS_RE = re.compile(r"^[A-Z][A-Z0-9-]{0,11}$")


def _request_symbol_alternates(symbol: Any) -> List[str]:
    """Return only the bare-US/.US spelling alternate for one request token.

    This is deliberately request-scoped. It does not change the global symbol
    canonicalizer and it never treats non-US exchange suffixes as equivalent.
    """
    canonical = canonicalize_symbol(symbol)
    if not canonical:
        return []
    if canonical.endswith(".US"):
        base = canonical[:-3]
        return [base] if _US_REQUEST_ALIAS_RE.fullmatch(base) else []
    if "." not in canonical and _US_REQUEST_ALIAS_RE.fullmatch(canonical):
        return [f"{canonical}.US"]
    return []


def _build_request_symbol_index(
    requested_symbols: Iterable[Any],
) -> Tuple[Dict[str, str], Dict[str, str]]:
    """Build exact and unambiguous response-alias maps for one request only.

    Exact requested spellings always win. An alternate spelling is accepted
    only when it is not itself requested and maps to exactly one requested
    symbol. Therefore requesting both AAPL and AAPL.US preserves both exact
    identities and creates no cross-mapping.
    """
    exact: Dict[str, str] = {}
    for raw in requested_symbols or []:
        requested = canonicalize_symbol(raw)
        if requested and requested not in exact:
            exact[requested] = requested

    aliases: Dict[str, str] = {}
    collisions: set[str] = set()
    for requested in exact:
        for alternate in _request_symbol_alternates(requested):
            if alternate in exact:
                continue
            previous = aliases.get(alternate)
            if previous is not None and previous != requested:
                collisions.add(alternate)
            else:
                aliases[alternate] = requested
    for alternate in collisions:
        aliases.pop(alternate, None)
    return exact, aliases


def _resolve_requested_symbol(
    value: Any,
    requested_symbols: Optional[Iterable[Any]] = None,
    *,
    request_index: Optional[Tuple[Dict[str, str], Dict[str, str]]] = None,
) -> str:
    """Resolve a returned symbol to the exact spelling requested in this call."""
    exact, aliases = request_index or _build_request_symbol_index(
        requested_symbols or []
    )
    returned = canonicalize_symbol(value)
    if not returned:
        return ""
    if returned in exact:
        return exact[returned]
    return aliases.get(returned, "")


'''
    anchor = "def _filter_rows_to_requested(\n"
    count = text.count(anchor)
    if count != 1:
        raise RuntimeError(f"runner filter anchor: expected 1, found {count}")
    text = text.replace(anchor, helper + anchor, 1)

    filter_function = r'''def _filter_rows_to_requested(
    headers: List[Any],
    rows_matrix: List[List[Any]],
    requested_symbols: List[str],
) -> Tuple[List[List[Any]], List[str]]:
    """Drop response rows outside the current request, preserving request spelling.

    Exact membership remains authoritative. The only accepted response alias is
    the unambiguous bare-US/.US alternate for a symbol in this same request.
    Accepted rows are rewritten to the requested spelling before persistence.
    """
    if not headers or not rows_matrix:
        return rows_matrix, []
    symbol_index = _guard_find_col(headers, _GUARD_SYMBOL_ALIASES)
    if symbol_index < 0:
        return rows_matrix, []

    request_index = _build_request_symbol_index(requested_symbols)
    kept: List[List[Any]] = []
    dropped: List[str] = []
    for raw in rows_matrix:
        row = list(raw)
        if symbol_index >= len(row) or _guard_is_blank(row[symbol_index]):
            kept.append(row)
            continue
        raw_symbol = canonicalize_symbol(row[symbol_index])
        requested = _resolve_requested_symbol(
            row[symbol_index], request_index=request_index
        )
        if not requested:
            dropped.append(raw_symbol or str(row[symbol_index] or ""))
            continue
        row[symbol_index] = requested
        kept.append(row)
    return kept, dropped


'''
    text = regex_once(
        text,
        r"^def _filter_rows_to_requested\(.*?(?=^def [A-Za-z_]\w*\()",
        filter_function,
        label="runner request membership function",
    )

    text = replace_count(
        text,
        "_batch_set = {canonicalize_symbol(t) for t in batch}",
        "_batch_index = _build_request_symbol_index(batch)",
        2,
        label="sequential batch request indexes",
    )

    pattern = (
        r"(?P<indent>[ \t]+)_t = canonicalize_symbol\(_row\[_idb_sym_i\]\)\n"
        r"(?P=indent)_row\[_idb_sym_i\] = _t\n"
        r"(?P=indent)if _t not in _batch_set:\n"
        r"(?P=indent)    _idb_bleed \+= 1\n"
        r"(?P=indent)    continue"
    )

    def sequential_replacement(match: re.Match[str]) -> str:
        indent = match.group("indent")
        return (
            f"{indent}_t = _resolve_requested_symbol(\n"
            f"{indent}    _row[_idb_sym_i], request_index=_batch_index\n"
            f"{indent})\n"
            f"{indent}if not _t:\n"
            f"{indent}    _idb_bleed += 1\n"
            f"{indent}    continue\n"
            f"{indent}_row[_idb_sym_i] = _t"
        )

    text, count = re.subn(pattern, sequential_replacement, text, flags=re.M)
    if count != 2:
        raise RuntimeError(
            f"sequential response resolver: expected 2 matches, found {count}"
        )

    write(path, text)


def patch_concurrent_fetch() -> None:
    path = "scripts/concurrent_batch_fetch.py"
    text = read(path)
    text = replace_once(
        text,
        'VERSION = "1.3.1"',
        'VERSION = "1.3.2"',
        label="concurrent adapter version",
    )

    helper = r'''
    def _request_index(
        symbols: Sequence[str],
    ) -> tuple[dict[str, str], dict[str, str]]:
        builder = getattr(sync, "_build_request_symbol_index", None)
        if callable(builder):
            return builder(symbols)
        exact: dict[str, str] = {}
        for raw in symbols:
            requested = sync.canonicalize_symbol(raw)
            if requested and requested not in exact:
                exact[requested] = requested
        return exact, {}

    def _resolve(
        value: Any,
        symbols: Sequence[str],
        *,
        request_index: tuple[dict[str, str], dict[str, str]] | None = None,
    ) -> str:
        index = request_index or _request_index(symbols)
        resolver = getattr(sync, "_resolve_requested_symbol", None)
        if callable(resolver):
            return resolver(value, request_index=index)
        returned = sync.canonicalize_symbol(value)
        return index[0].get(returned, "")

'''
    pattern = (
        r"(    def _requested_order\(symbols: Sequence\[str\]\) -> list\[str\]:"
        r".*?        return ordered\n)\n"
        r"(?=    def _rows_by_symbol)"
    )
    text = regex_once(
        text,
        pattern,
        r"\1\n" + helper,
        label="concurrent request resolver insertion",
    )

    rows_function = r'''    def _rows_by_symbol(
        headers: Sequence[Any],
        rows: Sequence[Sequence[Any]],
        symbols: Sequence[str],
    ) -> dict[str, list[Any]]:
        symbol_index = _column(headers, "_GUARD_SYMBOL_ALIASES", ("symbol", "ticker"))
        if symbol_index < 0:
            return {}
        request_index = _request_index(symbols)
        result: dict[str, list[Any]] = {}
        for raw in rows:
            if (
                not isinstance(raw, (list, tuple))
                or symbol_index >= len(raw)
                or sync._guard_is_blank(raw[symbol_index])
            ):
                continue
            row = list(raw)
            requested = _resolve(
                row[symbol_index], symbols, request_index=request_index
            )
            if not requested:
                continue
            row[symbol_index] = requested
            result.setdefault(requested, row)
        return result

'''
    text = regex_once(
        text,
        r"^    def _rows_by_symbol\(.*?(?=^    def _classify\()",
        rows_function,
        label="concurrent rows-by-symbol",
    )

    classify_function = r'''    def _classify(
        headers: Sequence[Any],
        rows: Sequence[Sequence[Any]],
        symbols: Sequence[str],
    ) -> tuple[dict[str, list[Any]], list[str], list[str], list[str]]:
        ordered = _requested_order(symbols)
        mapping = _rows_by_symbol(headers, rows, symbols)
        good_set = {
            symbol
            for symbol in ordered
            if symbol in mapping and _row_good(headers, mapping[symbol])
        }
        good = [symbol for symbol in ordered if symbol in good_set]
        data_free = [
            symbol for symbol in ordered if symbol in mapping and symbol not in good_set
        ]
        missing = [symbol for symbol in ordered if symbol not in mapping]
        return mapping, good, data_free, missing

'''
    text = regex_once(
        text,
        r"^    def _classify\(.*?(?=^    async def one\()",
        classify_function,
        label="concurrent classify",
    )

    text = replace_once(
        text,
        '            requested = {sync.canonicalize_symbol(value) for value in outcome["b"]}\n            requested.discard("")',
        '            request_index = _request_index(outcome["b"])',
        label="concurrent merge request index",
    )

    merge_pattern = (
        r"(?P<indent>[ \t]+)symbol = sync\.canonicalize_symbol\(row\[symbol_index\]\)\n"
        r"(?P=indent)row\[symbol_index\] = symbol\n"
        r"(?P=indent)if symbol not in requested:\n"
        r"(?P=indent)    bleed \+= 1\n"
        r"(?P=indent)    continue"
    )

    def merge_replacement(match: re.Match[str]) -> str:
        indent = match.group("indent")
        return (
            f"{indent}symbol = _resolve(\n"
            f"{indent}    row[symbol_index], outcome[\"b\"], request_index=request_index\n"
            f"{indent})\n"
            f"{indent}if not symbol:\n"
            f"{indent}    bleed += 1\n"
            f"{indent}    continue\n"
            f"{indent}row[symbol_index] = symbol"
        )

    text, count = re.subn(merge_pattern, merge_replacement, text, count=1, flags=re.M)
    if count != 1:
        raise RuntimeError(f"concurrent merge resolver: expected 1, found {count}")

    write(path, text)


def patch_tests() -> None:
    path = "tests/test_concurrent_batch_fetch.py"
    text = read(path)
    text = replace_once(
        text,
        "        stub_once=(),\n    ):",
        "        stub_once=(),\n        response_aliases=None,\n    ):",
        label="test backend response alias argument",
    )
    text = replace_once(
        text,
        "        self.stub_once = set(stub_once)\n",
        "        self.stub_once = set(stub_once)\n        self.response_aliases = dict(response_aliases or {})\n",
        label="test backend response alias state",
    )
    text = replace_once(
        text,
        '            rows = [[symbol, symbol.lower(), 100.0, "mock"] for symbol in reversed(symbols)]',
        '            rows = [\n                [self.response_aliases.get(symbol, symbol), symbol.lower(), 100.0, "mock"]\n                for symbol in reversed(symbols)\n            ]',
        label="test backend alias response rows",
    )
    text = replace_once(
        text,
        "        canonicalize_symbol=lambda value: str(value).strip().upper(),\n",
        "        canonicalize_symbol=lambda value: str(value).strip().upper(),\n"
        "        _build_request_symbol_index=production_sync._build_request_symbol_index,\n"
        "        _resolve_requested_symbol=production_sync._resolve_requested_symbol,\n",
        label="fake sync request resolver bindings",
    )

    new_test = r'''
    async def test_request_scoped_us_suffix_echoes_map_back_to_requested_order(self):
        fn = build(fake(size=2))
        requested = ["HNGE", "TT.US", "ADP", "ITW.US"]
        aliases = {
            "HNGE": "HNGE.US",
            "TT.US": "TT",
            "ADP": "ADP.US",
            "ITW.US": "ITW",
        }
        result = Result("us-suffix-echo")
        with patch.dict(
            os.environ,
            {
                "TFB_SYNC_BATCH_CONCURRENCY": "3",
                "TFB_SYNC_BATCH_OUTER_RETRIES": "0",
                "TFB_SYNC_TARGET_RECOVERY": "0",
            },
        ):
            _, rows, _, _ = await fn(
                Backend(response_aliases=aliases),
                SimpleNamespace(sheet_name="Market_Leaders"),
                requested,
                {},
                "analysis",
                result,
            )
        self.assertEqual([row[0] for row in rows], requested)
        self.assertEqual(result.batch_metrics["symbols_fresh"], 4)
        self.assertEqual(result.batch_metrics["symbols_missing"], 0)
        self.assertFalse(any("cross_batch=" in warning for warning in result.warnings))

'''
    anchor = "    async def test_header_mismatch_is_not_merged(self):\n"
    count = text.count(anchor)
    if count != 1:
        raise RuntimeError(f"concurrent test insertion anchor: expected 1, found {count}")
    text = text.replace(anchor, new_test + anchor, 1)
    write(path, text)

    path = "tests/test_critical_symbol_identity.py"
    text = read(path)
    new_membership_test = r'''
    def test_request_scoped_us_suffix_echoes_are_rewritten_to_requested_spelling(self):
        requested = ["HNGE", "TT.US", "ADP", "ITW.US"]
        rows = [
            ["HNGE.US", "Hinge Health", "NYSE", "USD", "USA", 50.0, "", "eodhd"],
            ["TT", "Trane Technologies", "NYSE", "USD", "USA", 400.0, "", "eodhd"],
            ["ADP.US", "Automatic Data Processing", "NASDAQ", "USD", "USA", 300.0, "", "eodhd"],
            ["ITW", "Illinois Tool Works", "NYSE", "USD", "USA", 250.0, "", "eodhd"],
        ]
        kept, dropped = rds._filter_rows_to_requested(
            PRODUCTION_HEADERS, rows, requested
        )
        self.assertEqual(dropped, [])
        self.assertEqual([row[0] for row in kept], requested)

    def test_request_scoped_alias_does_not_merge_two_exact_requested_spellings(self):
        index = rds._build_request_symbol_index(["AAPL", "AAPL.US"])
        self.assertEqual(rds._resolve_requested_symbol("AAPL", request_index=index), "AAPL")
        self.assertEqual(rds._resolve_requested_symbol("AAPL.US", request_index=index), "AAPL.US")

'''
    anchor = "class CriticalIdentityProductionPathTests(unittest.IsolatedAsyncioTestCase):\n"
    count = text.count(anchor)
    if count != 1:
        raise RuntimeError(f"critical test class anchor: expected 1, found {count}")
    text = text.replace(anchor, anchor + new_membership_test, 1)
    write(path, text)


def main() -> None:
    patch_runner()
    patch_concurrent_fetch()
    patch_tests()
    print("request-scoped US suffix alias fix applied")


if __name__ == "__main__":
    main()
