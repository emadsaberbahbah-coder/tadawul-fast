#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
integrations/symbols_reader.py
===============================================================================
Schema-safe, Render-safe symbol reader for Tadawul Fast Bridge -- v2.2.0
===============================================================================

Purpose
-------
Centralize symbol extraction / normalization from:
- raw strings
- query/body payloads
- rows / rows_matrix API envelopes
- JSON / CSV / TXT files
- page-specific fallback defaults

Design goals
------------
- No network I/O at import time
- No Google / provider dependency
- Safe for Render / FastAPI / Gunicorn / Uvicorn
- Works with Tadawul Fast Bridge page aliases and schema-style envelopes
- Preserves symbol order while de-duplicating
- Supports KSA suffix inference only when explicitly enabled

v2.2.0 changes (what moved from v2.1.0)
---------------------------------------
- FIX: ``DEFAULT_MAX_SYMBOLS`` now parses ``SYMBOLS_READER_MAX_SYMBOLS`` via
  a safe helper. v2.1.0 called ``int(os.getenv(...))`` directly at
  import time, which raised ``ValueError`` on any malformed value (e.g.
  ``"abc"``) and broke the module import chain.

- FIX: ``extract_symbols_from_any`` no longer short-circuits
  ``list[Mapping]`` into the shallow ``extract_symbols_from_rows``.
  v2.1.0 did, which meant envelopes passed by
  ``read_symbols_from_request`` -- e.g. ``body={"rows": [...]}`` or
  ``body={"data": {"symbols": [...]}}`` -- returned zero symbols
  because ``extract_symbols_from_rows`` only scans TOP-level keys of
  each dict for symbol-field aliases and ``"rows"`` / ``"data"`` are
  not symbol fields. v2.2.0 recurses via ``_extract_symbols_from_mapping``
  on each Mapping item, which does the full 4-pass scan (symbol fields,
  rows_matrix+headers, row containers, nested payloads). Behavior is
  strictly a superset for row-shaped data (same symbols found) and
  correct for envelope shapes (symbols now found).

- CLEANUP: library best practice -- attach ``NullHandler`` to ``logger``
  so importing this module from a host without log config does not
  emit "No handlers could be found" warnings.

- PERF: ``normalize_symbols`` now tracks seen items via a set instead
  of calling ``_dedupe_preserve_order`` on every append. O(n) instead of
  O(n^2) on large inputs. No behavior change.

- Bumped ``MODULE_VERSION`` to ``"2.2.0"``.

Preserved
---------
- All public functions and their signatures.
- Page alias table (``CANONICAL_PAGE_ALIASES``).
- Symbol-field alias tuples (``SYMBOL_FIELD_ALIASES``,
  ``DIRECT_SYMBOL_CONTAINER_KEYS``, ``NESTED_PAYLOAD_KEYS``,
  ``ROW_CONTAINER_KEYS``, ``QUERY_PAGE_KEYS``).
- Regex/normalization helpers (``TOKEN_SPLIT_RE``, ``VALID_SYMBOL_RE``,
  ``DIGITS_ONLY_RE``, ``HEADER_NORMALIZE_RE``).
- KSA suffix inference rules (off by default; enable via env or
  per-call ``infer_sr_suffix=True``).
- Built-in per-page fallback defaults.
- ``SymbolReadResult`` dataclass shape (.symbols, .count, .source,
  .used_fallback, .warnings, .diagnostics, .to_dict()).
- File readers (.json, .csv, .txt, generic).

Typical use
-----------
from integrations.symbols_reader import read_symbols_from_request

result = read_symbols_from_request(
    page="Market_Leaders",
    query_params={"symbols": "2222.SR,1120.SR"},
    body={"direct_symbols": ["2010.SR", "7010.SR"]},
)
symbols = list(result.symbols)

Environment variables
---------------------
SYMBOLS_READER_MAX_SYMBOLS=500
SYMBOLS_READER_INFER_SR_SUFFIX=false

Optional per-page defaults:
MARKET_LEADERS_DEFAULT_SYMBOLS=2222.SR,1120.SR,2010.SR,7010.SR
GLOBAL_MARKETS_DEFAULT_SYMBOLS=AAPL,MSFT,NVDA,AMZN,META
COMMODITIES_FX_DEFAULT_SYMBOLS=GC=F,BZ=F,SI=F,EURUSD=X,SAR=X
MUTUAL_FUNDS_DEFAULT_SYMBOLS=SPY,QQQ,VTI,VOO,IWM
MY_PORTFOLIO_DEFAULT_SYMBOLS=...
TOP_10_INVESTMENTS_DEFAULT_SYMBOLS=...
INSIGHTS_ANALYSIS_DEFAULT_SYMBOLS=...
"""

from __future__ import annotations

import csv
import json
import logging
import os
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence, Set, Tuple

logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())  # v2.2.0: library best practice

MODULE_VERSION = "2.2.0"


# -----------------------------------------------------------------------------
# Safe env parsers (v2.2.0 fix #1)
# -----------------------------------------------------------------------------

def _safe_env_int(name: str, default: int, *, lo: Optional[int] = None, hi: Optional[int] = None) -> int:
    """Parse an integer env var with a safe fallback.

    v2.2.0: introduced to avoid the ``ValueError`` at import time when
    users ship a malformed env value (e.g. ``"abc"``, empty string, or
    accidentally quoted input).
    """
    raw = os.getenv(name)
    if raw is None:
        return default
    try:
        value = int(float(str(raw).strip()))
    except (ValueError, TypeError):
        return default
    if lo is not None and value < lo:
        return lo
    if hi is not None and value > hi:
        return hi
    return value


def _safe_env_bool(name: str, default: bool) -> bool:
    """Parse a boolean env var with a safe fallback."""
    raw = os.getenv(name)
    if raw is None:
        return default
    value = str(raw).strip().lower()
    if not value:
        return default
    if value in {"1", "true", "yes", "y", "on", "t"}:
        return True
    if value in {"0", "false", "no", "n", "off", "f"}:
        return False
    return default


DEFAULT_MAX_SYMBOLS = _safe_env_int("SYMBOLS_READER_MAX_SYMBOLS", 500, lo=1, hi=100000)
DEFAULT_INFER_SR_SUFFIX = _safe_env_bool("SYMBOLS_READER_INFER_SR_SUFFIX", False)

# -----------------------------------------------------------------------------
# Page aliases
# -----------------------------------------------------------------------------

CANONICAL_PAGE_ALIASES: Dict[str, str] = {
    "marketleaders": "Market_Leaders",
    "market_leaders": "Market_Leaders",
    "leaders": "Market_Leaders",
    "myportfolio": "My_Portfolio",
    "my_portfolio": "My_Portfolio",
    "myinvestments": "My_Portfolio",
    "my_investments": "My_Portfolio",
    "portfolio": "My_Portfolio",
    "globalmarkets": "Global_Markets",
    "global_markets": "Global_Markets",
    "global": "Global_Markets",
    "commoditiesfx": "Commodities_FX",
    "commodities_fx": "Commodities_FX",
    "commodities": "Commodities_FX",
    "fx": "Commodities_FX",
    "mutualfunds": "Mutual_Funds",
    "mutual_funds": "Mutual_Funds",
    "funds": "Mutual_Funds",
    "insightsanalysis": "Insights_Analysis",
    "insights_analysis": "Insights_Analysis",
    "insights": "Insights_Analysis",
    "analysis": "Insights_Analysis",
    "top10investments": "Top_10_Investments",
    "top_10_investments": "Top_10_Investments",
    "top10": "Top_10_Investments",
    "top_10": "Top_10_Investments",
    "datadictionary": "Data_Dictionary",
    "data_dictionary": "Data_Dictionary",
    "dictionary": "Data_Dictionary",
}

# -----------------------------------------------------------------------------
# Field aliases / payload keys
# -----------------------------------------------------------------------------

SYMBOL_FIELD_ALIASES: Tuple[str, ...] = (
    "symbol",
    "symbols",
    "ticker",
    "tickers",
    "ticker_symbol",
    "ticker_symbols",
    "symbol_code",
    "symbol_name",
    "code",
    "instrument",
    "instrument_code",
    "instrument_symbol",
    "security",
    "security_code",
    "ric",
    "isin_symbol",
    "yahoo_symbol",
    "trading_symbol",
    "direct_symbols",
    "watchlist",
    "watch_list",
    "holdings",
    "positions",
)

DIRECT_SYMBOL_CONTAINER_KEYS: Tuple[str, ...] = (
    "symbols",
    "tickers",
    "direct_symbols",
    "watchlist",
    "watch_list",
    "symbol_list",
    "ticker_list",
    "symbols_csv",
    "tickers_csv",
    "raw_symbols",
    "raw_tickers",
)

NESTED_PAYLOAD_KEYS: Tuple[str, ...] = (
    "data",
    "payload",
    "result",
    "results",
    "request",
    "body",
    "params",
    "filters",
    "input",
)

ROW_CONTAINER_KEYS: Tuple[str, ...] = (
    "rows",
    "items",
    "quotes",
    "results",
    "instruments",
    "holdings",
    "positions",
    "portfolio",
    "watchlist",
    "data",
)

QUERY_PAGE_KEYS: Tuple[str, ...] = ("page", "sheet", "sheet_name", "name", "tab")

# -----------------------------------------------------------------------------
# Regex / normalization helpers
# -----------------------------------------------------------------------------

TOKEN_SPLIT_RE = re.compile(r"[\s,;|\u060C\u061B]+")
VALID_SYMBOL_RE = re.compile(r"^[A-Za-z0-9^][A-Za-z0-9._=\-^:/]{0,31}$")
DIGITS_ONLY_RE = re.compile(r"^\d{3,6}$")
HEADER_NORMALIZE_RE = re.compile(r"[^a-z0-9]+")

KSA_PAGE_HINTS = {"Market_Leaders", "My_Portfolio", "Top_10_Investments"}

_SYMBOL_STOPWORDS = {
    "",
    "NONE",
    "NULL",
    "NAN",
    "NA",
    "N/A",
    "TRUE",
    "FALSE",
    "PAGE",
    "SHEET",
    "LIMIT",
    "MODE",
    "LIVE",
    "FULL",
    "LIGHT",
    "TEST",
    "HEALTH",
    "QUOTE",
    "QUOTES",
    "ROW",
    "ROWS",
    "ITEM",
    "ITEMS",
    "DATA",
    "RESULT",
    "RESULTS",
    "PAYLOAD",
    "REQUEST",
    "BODY",
    "PARAMS",
    "FILTER",
    "FILTERS",
    "INPUT",
    "OUTPUT",
    "JSON",
    "CSV",
    "TXT",
    "FILE",
    "PATH",
    "ALL",
    "DEFAULT",
    "SCHEMA",
    "HEADER",
    "HEADERS",
    "KEY",
    "KEYS",
    "DISPLAY",
}

NORMALIZED_SYMBOL_FIELD_ALIASES = {
    HEADER_NORMALIZE_RE.sub("", value.strip().lower()) for value in SYMBOL_FIELD_ALIASES
}
NORMALIZED_DIRECT_SYMBOL_CONTAINER_KEYS = {
    HEADER_NORMALIZE_RE.sub("", value.strip().lower()) for value in DIRECT_SYMBOL_CONTAINER_KEYS
}
NORMALIZED_NESTED_PAYLOAD_KEYS = {
    HEADER_NORMALIZE_RE.sub("", value.strip().lower()) for value in NESTED_PAYLOAD_KEYS
}
NORMALIZED_ROW_CONTAINER_KEYS = {
    HEADER_NORMALIZE_RE.sub("", value.strip().lower()) for value in ROW_CONTAINER_KEYS
}

# -----------------------------------------------------------------------------
# Built-in page defaults
# -----------------------------------------------------------------------------

BUILTIN_PAGE_DEFAULTS: Dict[str, Tuple[str, ...]] = {
    "Market_Leaders": ("2222.SR", "1120.SR", "2010.SR", "7010.SR"),
    "My_Portfolio": (),
    "Global_Markets": ("AAPL", "MSFT", "NVDA", "AMZN", "META"),
    "Commodities_FX": ("GC=F", "BZ=F", "SI=F", "EURUSD=X", "SAR=X"),
    "Mutual_Funds": ("SPY", "QQQ", "VTI", "VOO", "IWM"),
}

# -----------------------------------------------------------------------------
# Data classes / errors
# -----------------------------------------------------------------------------


@dataclass(frozen=True)
class SymbolReadResult:
    symbols: Tuple[str, ...]
    page: Optional[str] = None
    source: str = "unknown"
    used_fallback: bool = False
    warnings: Tuple[str, ...] = ()
    diagnostics: Dict[str, Any] = field(default_factory=dict)

    @property
    def count(self) -> int:
        return len(self.symbols)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "symbols": list(self.symbols),
            "count": self.count,
            "page": self.page,
            "source": self.source,
            "used_fallback": self.used_fallback,
            "warnings": list(self.warnings),
            "diagnostics": self.diagnostics,
        }


class SymbolsReaderError(RuntimeError):
    """Raised when a symbol source cannot be parsed or loaded."""


# -----------------------------------------------------------------------------
# Core utilities
# -----------------------------------------------------------------------------


def _normalize_header_name(value: Any) -> str:
    return HEADER_NORMALIZE_RE.sub("", str(value or "").strip().lower())


def _dedupe_preserve_order(values: Iterable[str]) -> List[str]:
    seen: Set[str] = set()
    out: List[str] = []
    for item in values:
        if item and item not in seen:
            seen.add(item)
            out.append(item)
    return out


def _clean_token(token: Any) -> str:
    text = str(token or "").strip()
    text = text.strip("[](){}<>\"'`")
    text = text.replace("\u200f", "").replace("\u200e", "").replace("\ufeff", "")
    return text.strip()


def canonical_page_name(page: Optional[str]) -> Optional[str]:
    if page is None:
        return None
    raw = str(page).strip()
    if not raw:
        return None

    normalized = _normalize_header_name(raw)
    return CANONICAL_PAGE_ALIASES.get(normalized, raw)


def is_symbol_like(token: Any) -> bool:
    text = _clean_token(token)
    if not text:
        return False

    upper = text.upper()
    if upper in _SYMBOL_STOPWORDS:
        return False

    if len(text) > 32:
        return False

    if not VALID_SYMBOL_RE.fullmatch(text):
        return False

    if any(ch.isdigit() for ch in text):
        return True

    if any(ch in text for ch in (".", "=", "-", "^", ":")):
        return True

    if text.isalpha() and 1 <= len(text) <= 5:
        return True

    return False


def normalize_symbol(
    symbol: Any,
    *,
    page: Optional[str] = None,
    infer_sr_suffix: Optional[bool] = None,
) -> Optional[str]:
    text = _clean_token(symbol)
    if not text:
        return None

    text = text.replace(" ", "")
    text = text.upper()

    if infer_sr_suffix is None:
        infer_sr_suffix = DEFAULT_INFER_SR_SUFFIX

    page = canonical_page_name(page)

    if infer_sr_suffix and page in KSA_PAGE_HINTS and DIGITS_ONLY_RE.fullmatch(text):
        text = f"{text}.SR"

    if not is_symbol_like(text):
        return None

    return text


def normalize_symbols(
    values: Iterable[Any],
    *,
    page: Optional[str] = None,
    infer_sr_suffix: Optional[bool] = None,
    limit: Optional[int] = None,
) -> Tuple[str, ...]:
    """Normalize an iterable of raw values into de-duplicated symbols.

    v2.2.0: uses a set to track seen items for O(n) dedup; v2.1.0 called
    ``_dedupe_preserve_order`` inside the loop on every append (O(n^2)
    in the worst case).
    """
    out: List[str] = []
    seen: Set[str] = set()

    for value in values:
        symbol = normalize_symbol(value, page=page, infer_sr_suffix=infer_sr_suffix)
        if not symbol or symbol in seen:
            continue
        seen.add(symbol)
        out.append(symbol)
        if limit and len(out) >= limit:
            break

    return tuple(out)


def parse_symbol_text(
    text: Any,
    *,
    page: Optional[str] = None,
    infer_sr_suffix: Optional[bool] = None,
    limit: Optional[int] = None,
) -> Tuple[str, ...]:
    if text is None:
        return ()

    if isinstance(text, (list, tuple, set)):
        return normalize_symbols(text, page=page, infer_sr_suffix=infer_sr_suffix, limit=limit)

    raw = str(text).strip()
    if not raw:
        return ()

    if raw.startswith("{") or raw.startswith("["):
        try:
            parsed = json.loads(raw)
            return extract_symbols_from_any(
                parsed,
                page=page,
                infer_sr_suffix=infer_sr_suffix,
                limit=limit,
            )
        except Exception:
            pass

    flattened = raw.replace("\n", " ").replace("\t", " ")
    tokens = [tok for tok in TOKEN_SPLIT_RE.split(flattened) if tok]
    return normalize_symbols(tokens, page=page, infer_sr_suffix=infer_sr_suffix, limit=limit)


# -----------------------------------------------------------------------------
# Default symbols
# -----------------------------------------------------------------------------


def _env_default_symbols_for_page(page: Optional[str]) -> Tuple[str, ...]:
    if not page:
        return ()

    page_upper = page.upper().replace("-", "_")
    env_keys = [
        f"{page_upper}_DEFAULT_SYMBOLS",
    ]

    for key in env_keys:
        raw = os.getenv(key)
        if raw and raw.strip():
            return parse_symbol_text(raw, page=page)

    return ()


def get_default_symbols_for_page(page: Optional[str]) -> Tuple[str, ...]:
    page = canonical_page_name(page)

    env_symbols = _env_default_symbols_for_page(page)
    if env_symbols:
        return env_symbols

    if page in BUILTIN_PAGE_DEFAULTS:
        return BUILTIN_PAGE_DEFAULTS[page]

    if page in {"Insights_Analysis", "Top_10_Investments"}:
        merged: List[str] = []
        for seed_page in ("Market_Leaders", "Global_Markets", "Commodities_FX", "Mutual_Funds"):
            merged.extend(BUILTIN_PAGE_DEFAULTS.get(seed_page, ()))
        return tuple(_dedupe_preserve_order(merged))

    return ()


# -----------------------------------------------------------------------------
# Extraction from mappings / rows / matrices
# -----------------------------------------------------------------------------


def _extract_symbols_from_mapping(
    data: Mapping[str, Any],
    *,
    page: Optional[str] = None,
    infer_sr_suffix: Optional[bool] = None,
    limit: Optional[int] = None,
    _depth: int = 0,
) -> Tuple[str, ...]:
    if _depth > 6:
        return ()

    collected: List[str] = []
    normalized_key_map = {_normalize_header_name(k): k for k in data.keys()}

    # Direct symbol fields / containers
    for normalized_key, original_key in normalized_key_map.items():
        if (
            normalized_key in NORMALIZED_SYMBOL_FIELD_ALIASES
            or normalized_key in NORMALIZED_DIRECT_SYMBOL_CONTAINER_KEYS
        ):
            value = data.get(original_key)
            if isinstance(value, (str, list, tuple, set)):
                collected.extend(
                    parse_symbol_text(
                        value,
                        page=page,
                        infer_sr_suffix=infer_sr_suffix,
                        limit=limit,
                    )
                )
            elif isinstance(value, Mapping):
                collected.extend(
                    _extract_symbols_from_mapping(
                        value,
                        page=page,
                        infer_sr_suffix=infer_sr_suffix,
                        limit=limit,
                        _depth=_depth + 1,
                    )
                )

            if limit and len(_dedupe_preserve_order(collected)) >= limit:
                uniq = _dedupe_preserve_order(collected)
                return tuple(uniq[:limit])

    # rows_matrix + headers envelope
    headers = data.get("headers") or data.get("keys") or data.get("display_headers")
    rows_matrix = data.get("rows_matrix")
    if headers and rows_matrix:
        collected.extend(
            extract_symbols_from_rows_matrix(
                headers=headers,
                rows_matrix=rows_matrix,
                page=page,
                infer_sr_suffix=infer_sr_suffix,
                limit=limit,
            )
        )

    # row-style containers
    for normalized_key, original_key in normalized_key_map.items():
        if normalized_key in NORMALIZED_ROW_CONTAINER_KEYS:
            collected.extend(
                extract_symbols_from_any(
                    data.get(original_key),
                    page=page,
                    infer_sr_suffix=infer_sr_suffix,
                    limit=limit,
                    _depth=_depth + 1,
                )
            )
            if limit and len(_dedupe_preserve_order(collected)) >= limit:
                uniq = _dedupe_preserve_order(collected)
                return tuple(uniq[:limit])

    # nested containers
    for normalized_key, original_key in normalized_key_map.items():
        if normalized_key in NORMALIZED_NESTED_PAYLOAD_KEYS:
            collected.extend(
                extract_symbols_from_any(
                    data.get(original_key),
                    page=page,
                    infer_sr_suffix=infer_sr_suffix,
                    limit=limit,
                    _depth=_depth + 1,
                )
            )
            if limit and len(_dedupe_preserve_order(collected)) >= limit:
                uniq = _dedupe_preserve_order(collected)
                return tuple(uniq[:limit])

    uniq = _dedupe_preserve_order(collected)
    return tuple(uniq[:limit] if limit else uniq)


def extract_symbols_from_rows(
    rows: Iterable[Any],
    *,
    page: Optional[str] = None,
    infer_sr_suffix: Optional[bool] = None,
    limit: Optional[int] = None,
) -> Tuple[str, ...]:
    """Extract symbols from an iterable of row-like values.

    Rows can be:
      - Mapping: looks for top-level keys in ``SYMBOL_FIELD_ALIASES``.
      - Single-element list/tuple: treats the element as a symbol token.
      - String: parsed as delimited token text.

    Used directly by ``read_symbols(rows=...)``. For recursive extraction
    from nested envelopes (``body={"rows": [...]}``), callers should go
    through ``extract_symbols_from_any`` instead, which recurses via
    ``_extract_symbols_from_mapping``.
    """
    collected: List[str] = []

    for row in rows or ():
        if isinstance(row, Mapping):
            for key, value in row.items():
                if _normalize_header_name(key) in NORMALIZED_SYMBOL_FIELD_ALIASES:
                    collected.extend(
                        parse_symbol_text(
                            value,
                            page=page,
                            infer_sr_suffix=infer_sr_suffix,
                            limit=limit,
                        )
                    )
        elif isinstance(row, (list, tuple)) and len(row) == 1:
            collected.extend(
                parse_symbol_text(
                    row[0],
                    page=page,
                    infer_sr_suffix=infer_sr_suffix,
                    limit=limit,
                )
            )
        elif isinstance(row, str):
            collected.extend(
                parse_symbol_text(
                    row,
                    page=page,
                    infer_sr_suffix=infer_sr_suffix,
                    limit=limit,
                )
            )

        if limit and len(_dedupe_preserve_order(collected)) >= limit:
            break

    uniq = _dedupe_preserve_order(collected)
    return tuple(uniq[:limit] if limit else uniq)


def extract_symbols_from_rows_matrix(
    *,
    headers: Sequence[Any],
    rows_matrix: Sequence[Sequence[Any]],
    page: Optional[str] = None,
    infer_sr_suffix: Optional[bool] = None,
    limit: Optional[int] = None,
) -> Tuple[str, ...]:
    if not headers or not rows_matrix:
        return ()

    normalized_headers = [_normalize_header_name(h) for h in headers]
    symbol_indexes = [
        idx for idx, h in enumerate(normalized_headers) if h in NORMALIZED_SYMBOL_FIELD_ALIASES
    ]

    if not symbol_indexes:
        return ()

    collected: List[str] = []

    for row in rows_matrix:
        if not isinstance(row, (list, tuple)):
            continue

        for idx in symbol_indexes:
            if idx < len(row):
                collected.extend(
                    parse_symbol_text(
                        row[idx],
                        page=page,
                        infer_sr_suffix=infer_sr_suffix,
                        limit=limit,
                    )
                )

        if limit and len(_dedupe_preserve_order(collected)) >= limit:
            break

    uniq = _dedupe_preserve_order(collected)
    return tuple(uniq[:limit] if limit else uniq)


def extract_symbols_from_any(
    value: Any,
    *,
    page: Optional[str] = None,
    infer_sr_suffix: Optional[bool] = None,
    limit: Optional[int] = None,
    _depth: int = 0,
) -> Tuple[str, ...]:
    """Recursively extract symbols from any value (Mapping, list, str, ...).

    v2.2.0 fix: v2.1.0 had a fast path for ``list[Mapping]`` that called
    the shallow ``extract_symbols_from_rows`` (which only inspects
    top-level symbol-field keys of each dict). This silently missed
    symbols inside envelope shapes such as ``[{"rows": [...]}]`` or
    ``[{"data": {"symbols": [...]}}]`` that ``read_symbols_from_request``
    produces when it wraps the body/query_params into a list of
    payload candidates.

    v2.2.0 removes that fast path and iterates each item recursively.
    For Mapping items the recursion dispatches to
    ``_extract_symbols_from_mapping``, which does the full 4-pass
    scan (symbol fields, rows_matrix+headers, row containers, nested
    payloads). For row-shaped data this returns the same symbols; for
    envelope shapes it now finds them too.
    """
    if value is None or _depth > 6:
        return ()

    if isinstance(value, Mapping):
        return _extract_symbols_from_mapping(
            value,
            page=page,
            infer_sr_suffix=infer_sr_suffix,
            limit=limit,
            _depth=_depth,
        )

    if isinstance(value, str):
        return parse_symbol_text(
            value,
            page=page,
            infer_sr_suffix=infer_sr_suffix,
            limit=limit,
        )

    if isinstance(value, (list, tuple, set)):
        # v2.2.0: no more fast-path for all-Mapping lists. Always recurse
        # so that envelope shapes (list of dicts with nested row/data
        # containers) are fully processed.
        collected: List[str] = []
        for item in value:
            collected.extend(
                extract_symbols_from_any(
                    item,
                    page=page,
                    infer_sr_suffix=infer_sr_suffix,
                    limit=limit,
                    _depth=_depth + 1,
                )
            )
            if limit and len(_dedupe_preserve_order(collected)) >= limit:
                break

        uniq = _dedupe_preserve_order(collected)
        return tuple(uniq[:limit] if limit else uniq)

    return ()


# -----------------------------------------------------------------------------
# File readers
# -----------------------------------------------------------------------------


def read_symbols_from_csv_file(
    path: "str | Path",
    *,
    page: Optional[str] = None,
    infer_sr_suffix: Optional[bool] = None,
    limit: Optional[int] = None,
    encoding: str = "utf-8-sig",
) -> Tuple[str, ...]:
    file_path = Path(path)
    if not file_path.exists():
        raise SymbolsReaderError(f"CSV file not found: {file_path}")

    with file_path.open("r", encoding=encoding, newline="") as handle:
        sample = handle.read(4096)
        handle.seek(0)

        try:
            dialect = csv.Sniffer().sniff(sample)
        except Exception:
            dialect = csv.excel

        reader = csv.DictReader(handle, dialect=dialect)
        rows = list(reader)

    return extract_symbols_from_rows(rows, page=page, infer_sr_suffix=infer_sr_suffix, limit=limit)


def read_symbols_from_json_file(
    path: "str | Path",
    *,
    page: Optional[str] = None,
    infer_sr_suffix: Optional[bool] = None,
    limit: Optional[int] = None,
    encoding: str = "utf-8",
) -> Tuple[str, ...]:
    file_path = Path(path)
    if not file_path.exists():
        raise SymbolsReaderError(f"JSON file not found: {file_path}")

    with file_path.open("r", encoding=encoding) as handle:
        payload = json.load(handle)

    return extract_symbols_from_any(payload, page=page, infer_sr_suffix=infer_sr_suffix, limit=limit)


def read_symbols_from_text_file(
    path: "str | Path",
    *,
    page: Optional[str] = None,
    infer_sr_suffix: Optional[bool] = None,
    limit: Optional[int] = None,
    encoding: str = "utf-8",
) -> Tuple[str, ...]:
    file_path = Path(path)
    if not file_path.exists():
        raise SymbolsReaderError(f"Text file not found: {file_path}")

    text = file_path.read_text(encoding=encoding)
    return parse_symbol_text(text, page=page, infer_sr_suffix=infer_sr_suffix, limit=limit)


def read_symbols_from_file(
    path: "str | Path",
    *,
    page: Optional[str] = None,
    infer_sr_suffix: Optional[bool] = None,
    limit: Optional[int] = None,
) -> Tuple[str, ...]:
    file_path = Path(path)
    if not file_path.exists():
        raise SymbolsReaderError(f"File not found: {file_path}")

    suffix = file_path.suffix.lower()

    if suffix == ".json":
        return read_symbols_from_json_file(
            file_path, page=page, infer_sr_suffix=infer_sr_suffix, limit=limit
        )

    if suffix == ".csv":
        return read_symbols_from_csv_file(
            file_path, page=page, infer_sr_suffix=infer_sr_suffix, limit=limit
        )

    if suffix in {".txt", ".list", ".symbols"}:
        return read_symbols_from_text_file(
            file_path, page=page, infer_sr_suffix=infer_sr_suffix, limit=limit
        )

    text = file_path.read_text(encoding="utf-8")
    try:
        payload = json.loads(text)
        return extract_symbols_from_any(
            payload, page=page, infer_sr_suffix=infer_sr_suffix, limit=limit
        )
    except Exception:
        return parse_symbol_text(text, page=page, infer_sr_suffix=infer_sr_suffix, limit=limit)


# -----------------------------------------------------------------------------
# High-level readers / mergers
# -----------------------------------------------------------------------------


def merge_symbol_sources(
    *sources: Iterable[Any],
    page: Optional[str] = None,
    infer_sr_suffix: Optional[bool] = None,
    limit: Optional[int] = None,
) -> Tuple[str, ...]:
    collected: List[str] = []

    for source in sources:
        if source is None:
            continue

        if isinstance(source, (str, list, tuple, set, Mapping)):
            symbols = extract_symbols_from_any(
                source,
                page=page,
                infer_sr_suffix=infer_sr_suffix,
                limit=limit,
            )
        else:
            try:
                symbols = normalize_symbols(
                    source,
                    page=page,
                    infer_sr_suffix=infer_sr_suffix,
                    limit=limit,
                )
            except TypeError:
                symbols = ()

        collected.extend(symbols)

        if limit and len(_dedupe_preserve_order(collected)) >= limit:
            break

    uniq = _dedupe_preserve_order(collected)
    return tuple(uniq[:limit] if limit else uniq)


def read_symbols(
    *,
    page: Optional[str] = None,
    raw: Any = None,
    payload: Any = None,
    rows: Optional[Iterable[Any]] = None,
    headers: Optional[Sequence[Any]] = None,
    rows_matrix: Optional[Sequence[Sequence[Any]]] = None,
    file_path: Optional[str] = None,
    allow_fallback: bool = True,
    infer_sr_suffix: Optional[bool] = None,
    limit: Optional[int] = None,
) -> SymbolReadResult:
    page = canonical_page_name(page)
    limit = limit or DEFAULT_MAX_SYMBOLS

    warnings: List[str] = []
    diagnostics: Dict[str, Any] = {
        "module_version": MODULE_VERSION,
        "input_page": page,
        "limit": limit,
        "infer_sr_suffix": (
            DEFAULT_INFER_SR_SUFFIX if infer_sr_suffix is None else bool(infer_sr_suffix)
        ),
    }

    source_hits: List[Tuple[str, Tuple[str, ...]]] = []

    if raw is not None:
        source_hits.append(
            (
                "raw",
                extract_symbols_from_any(
                    raw,
                    page=page,
                    infer_sr_suffix=infer_sr_suffix,
                    limit=limit,
                ),
            )
        )

    if payload is not None:
        source_hits.append(
            (
                "payload",
                extract_symbols_from_any(
                    payload,
                    page=page,
                    infer_sr_suffix=infer_sr_suffix,
                    limit=limit,
                ),
            )
        )

    if rows is not None:
        source_hits.append(
            (
                "rows",
                extract_symbols_from_rows(
                    rows,
                    page=page,
                    infer_sr_suffix=infer_sr_suffix,
                    limit=limit,
                ),
            )
        )

    if headers is not None and rows_matrix is not None:
        source_hits.append(
            (
                "rows_matrix",
                extract_symbols_from_rows_matrix(
                    headers=headers,
                    rows_matrix=rows_matrix,
                    page=page,
                    infer_sr_suffix=infer_sr_suffix,
                    limit=limit,
                ),
            )
        )

    if file_path:
        try:
            source_hits.append(
                (
                    "file",
                    read_symbols_from_file(
                        file_path,
                        page=page,
                        infer_sr_suffix=infer_sr_suffix,
                        limit=limit,
                    ),
                )
            )
        except Exception as exc:
            warnings.append(f"file_read_failed: {exc}")

    non_empty_hits = [(name, symbols) for name, symbols in source_hits if symbols]

    if len(non_empty_hits) == 1:
        selected_source = non_empty_hits[0][0]
        merged = non_empty_hits[0][1]
    else:
        selected_source = "merged" if non_empty_hits else "none"
        merged = merge_symbol_sources(
            *[symbols for _, symbols in source_hits],
            page=page,
            infer_sr_suffix=infer_sr_suffix,
            limit=limit,
        )

    used_fallback = False
    if not merged and allow_fallback:
        fallback = get_default_symbols_for_page(page)
        if fallback:
            merged = normalize_symbols(
                fallback,
                page=page,
                infer_sr_suffix=infer_sr_suffix,
                limit=limit,
            )
            selected_source = "fallback_defaults"
            used_fallback = True
            warnings.append("no_input_symbols_found_using_page_defaults")

    if not merged:
        warnings.append("no_symbols_found")

    diagnostics["source_hits"] = {name: len(symbols) for name, symbols in source_hits}

    return SymbolReadResult(
        symbols=merged,
        page=page,
        source=selected_source,
        used_fallback=used_fallback,
        warnings=tuple(_dedupe_preserve_order(warnings)),
        diagnostics=diagnostics,
    )


def read_symbols_from_request(
    *,
    page: Optional[str] = None,
    query_params: Optional[Mapping[str, Any]] = None,
    body: Optional[Any] = None,
    allow_fallback: bool = True,
    infer_sr_suffix: Optional[bool] = None,
    limit: Optional[int] = None,
) -> SymbolReadResult:
    page_candidates: List[Any] = [page]
    payload_candidates: List[Any] = []

    if query_params:
        payload_candidates.append(dict(query_params))
        for key in QUERY_PAGE_KEYS:
            if key in query_params and query_params.get(key):
                page_candidates.append(query_params.get(key))

    if body is not None:
        payload_candidates.append(body)
        if isinstance(body, Mapping):
            for key in QUERY_PAGE_KEYS:
                if body.get(key):
                    page_candidates.append(body.get(key))

    resolved_page = None
    for candidate in page_candidates:
        resolved_page = canonical_page_name(candidate)
        if resolved_page:
            break

    return read_symbols(
        page=resolved_page,
        payload=payload_candidates,
        allow_fallback=allow_fallback,
        infer_sr_suffix=infer_sr_suffix,
        limit=limit,
    )


# -----------------------------------------------------------------------------
# Diagnostics / metadata / compatibility aliases
# -----------------------------------------------------------------------------


def get_symbols_reader_meta(page: Optional[str] = None) -> Dict[str, Any]:
    canonical_page = canonical_page_name(page)
    return {
        "module": "integrations.symbols_reader",
        "version": MODULE_VERSION,
        "page": canonical_page,
        "default_max_symbols": DEFAULT_MAX_SYMBOLS,
        "infer_sr_suffix_default": DEFAULT_INFER_SR_SUFFIX,
        "default_symbols": list(get_default_symbols_for_page(canonical_page)),
        "supported_symbol_fields": list(SYMBOL_FIELD_ALIASES),
    }


def get_symbols(
    *,
    page: Optional[str] = None,
    raw: Any = None,
    payload: Any = None,
    rows: Optional[Iterable[Any]] = None,
    headers: Optional[Sequence[Any]] = None,
    rows_matrix: Optional[Sequence[Sequence[Any]]] = None,
    file_path: Optional[str] = None,
    allow_fallback: bool = True,
    infer_sr_suffix: Optional[bool] = None,
    limit: Optional[int] = None,
) -> Tuple[str, ...]:
    return read_symbols(
        page=page,
        raw=raw,
        payload=payload,
        rows=rows,
        headers=headers,
        rows_matrix=rows_matrix,
        file_path=file_path,
        allow_fallback=allow_fallback,
        infer_sr_suffix=infer_sr_suffix,
        limit=limit,
    ).symbols


def parse_symbols(
    value: Any,
    *,
    page: Optional[str] = None,
    infer_sr_suffix: Optional[bool] = None,
    limit: Optional[int] = None,
) -> Tuple[str, ...]:
    return extract_symbols_from_any(
        value,
        page=page,
        infer_sr_suffix=infer_sr_suffix,
        limit=limit,
    )


# -----------------------------------------------------------------------------
# CLI self-test
# -----------------------------------------------------------------------------


if __name__ == "__main__":
    demo = read_symbols(
        page="Global_Markets",
        raw="AAPL, MSFT NVDA; AMZN | META",
        allow_fallback=True,
    )
    print(json.dumps(demo.to_dict(), ensure_ascii=False, indent=2))
