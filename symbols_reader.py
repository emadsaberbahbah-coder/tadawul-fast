from __future__ import annotations

"""
symbols_reader.py
================================================================================
Safe symbol reader / normalizer for Tadawul Fast Bridge
================================================================================

Design goals
------------
- No network I/O or heavy work at import time
- Schema-safe and route-safe
- Accepts symbols from many request styles:
    - symbols
    - tickers
    - direct_symbols
    - symbol / ticker
    - CSV / text / JSON array
    - nested payloads
    - rows / items lists that contain symbol-like fields
- Page-aware defaults
- Strong normalization and de-duplication
- KSA-aware normalization:
    - "2222"     -> "2222.SR"
    - "2222.sr"  -> "2222.SR"
- Helpful provider hint for downstream readers/builders
"""

import csv
import io
import json
import re
from dataclasses import asdict, dataclass, field
from typing import Any, Iterable, Mapping, Sequence

# ------------------------------------------------------------------------------
# Canonical page aliases
# ------------------------------------------------------------------------------

_PAGE_ALIAS_MAP: dict[str, str] = {
    # Market Leaders
    "market_leaders": "Market_Leaders",
    "marketleaders": "Market_Leaders",
    "leaders": "Market_Leaders",

    # My Portfolio / Investments
    "my_portfolio": "My_Portfolio",
    "portfolio": "My_Portfolio",
    "myportfolio": "My_Portfolio",
    "my_investments": "My_Portfolio",
    "myinvestments": "My_Portfolio",
    "investments": "My_Portfolio",

    # Global Markets
    "global_markets": "Global_Markets",
    "globalmarkets": "Global_Markets",
    "global": "Global_Markets",

    # Commodities & FX
    "commodities_fx": "Commodities_FX",
    "commoditiesfx": "Commodities_FX",
    "commodities": "Commodities_FX",
    "fx": "Commodities_FX",
    "commodities_and_fx": "Commodities_FX",

    # Mutual Funds
    "mutual_funds": "Mutual_Funds",
    "mutualfunds": "Mutual_Funds",
    "funds": "Mutual_Funds",

    # Insights / Analysis
    "insights_analysis": "Insights_Analysis",
    "insights": "Insights_Analysis",
    "analysis": "Insights_Analysis",

    # Top 10
    "top_10_investments": "Top_10_Investments",
    "top10investments": "Top_10_Investments",
    "top10": "Top_10_Investments",

    # Data Dictionary
    "data_dictionary": "Data_Dictionary",
    "datadictionary": "Data_Dictionary",
    "dictionary": "Data_Dictionary",
}

CANONICAL_PAGES: tuple[str, ...] = (
    "Market_Leaders",
    "My_Portfolio",
    "Global_Markets",
    "Commodities_FX",
    "Mutual_Funds",
    "Insights_Analysis",
    "Top_10_Investments",
    "Data_Dictionary",
)

# ------------------------------------------------------------------------------
# Page defaults
# ------------------------------------------------------------------------------

_DEFAULT_PAGE_SYMBOLS: dict[str, list[str]] = {
    "Market_Leaders": [
        "2222.SR", "1120.SR", "2010.SR", "7010.SR", "7203.SR",
        "1211.SR", "1180.SR", "1150.SR", "2280.SR", "2380.SR",
    ],
    "My_Portfolio": [
        "1120.SR", "4013.SR", "7020.SR",
    ],
    "Global_Markets": [
        "AAPL", "MSFT", "NVDA", "AMZN", "META", "GOOGL", "TSLA", "AVGO",
        "SPY", "QQQ", "VTI", "VOO", "IWM",
    ],
    "Commodities_FX": [
        "GC=F", "SI=F", "BZ=F", "CL=F", "EURUSD=X", "GBPUSD=X", "JPY=X", "SAR=X",
    ],
    "Mutual_Funds": [
        "VFIAX", "SWPPX", "FXAIX", "VTSAX", "VTIAX",
    ],
    "Insights_Analysis": [
        "TASI", "NOMU", "SPY", "QQQ", "GC=F", "BZ=F",
    ],
    "Top_10_Investments": [
        "2222.SR", "1120.SR", "AAPL", "MSFT", "NVDA",
        "SPY", "QQQ", "GC=F", "BZ=F", "EURUSD=X",
    ],
    "Data_Dictionary": [],
}

# ------------------------------------------------------------------------------
# Field aliases commonly seen in requests
# ------------------------------------------------------------------------------

PAGE_KEYS: tuple[str, ...] = (
    "page", "sheet", "sheet_name", "sheetName", "name", "tab", "page_name",
)

LIMIT_KEYS: tuple[str, ...] = (
    "limit", "max", "max_rows", "rows", "top", "n",
)

SYMBOL_KEYS: tuple[str, ...] = (
    "symbols", "tickers", "direct_symbols", "directSymbols",
    "symbol_list", "ticker_list", "symbols_csv", "tickers_csv",
    "symbols_text", "tickers_text", "symbolsText", "tickersText",
    "symbol", "ticker",
)

ROW_CONTAINER_KEYS: tuple[str, ...] = (
    "rows", "items", "data", "quotes", "results", "payload",
)

ROW_SYMBOL_KEYS: tuple[str, ...] = (
    "symbol", "ticker", "code", "instrument", "instrument_code", "Symbol", "Ticker",
)

# ------------------------------------------------------------------------------
# Symbol aliases / friendly names
# ------------------------------------------------------------------------------

_FRIENDLY_SYMBOL_ALIASES: dict[str, str] = {
    # Indices
    "TASI": "TASI",
    "NOMU": "NOMU",
    "SP500": "SPY",
    "S&P500": "SPY",
    "NASDAQ": "QQQ",
    "DOW": "DIA",

    # Commodities
    "GOLD": "GC=F",
    "SILVER": "SI=F",
    "BRENT": "BZ=F",
    "CRUDE": "CL=F",
    "WTI": "CL=F",

    # FX
    "EURUSD": "EURUSD=X",
    "GBPUSD": "GBPUSD=X",
    "USDJPY": "JPY=X",
    "USDSAR": "SAR=X",
    "SARUSD": "SAR=X",
}

_KSA_NUMERIC_RE = re.compile(r"^\d{3,6}$")
_KSA_SUFFIX_RE = re.compile(r"^\d{3,6}\.SR$", re.IGNORECASE)
_MULTI_SPLIT_RE = re.compile(r"[,;\n\r\t|]+")
_EXTRA_SPACE_RE = re.compile(r"\s+")

# ------------------------------------------------------------------------------
# Result object
# ------------------------------------------------------------------------------

@dataclass(slots=True)
class ResolvedSymbols:
    page: str | None
    symbols: list[str]
    requested_symbols: list[str] = field(default_factory=list)
    source: str = "unknown"
    provider_hint: str = "AUTO"
    used_page_defaults: bool = False
    limit_applied: int | None = None
    dropped_tokens: list[str] = field(default_factory=list)
    meta: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

# ------------------------------------------------------------------------------
# Small helpers
# ------------------------------------------------------------------------------

def _is_blank(value: Any) -> bool:
    return value is None or (isinstance(value, str) and not value.strip())

def _clean_text(value: Any) -> str:
    if value is None:
        return ""
    text = str(value)
    text = text.replace("،", ",").replace("؛", ";")
    text = text.replace("\u00A0", " ").replace("\u2007", " ").replace("\u202F", " ")
    return text.strip()

def _to_int(value: Any, default: int | None = None) -> int | None:
    if value is None:
        return default
    try:
        iv = int(str(value).strip())
        return iv if iv >= 0 else default
    except Exception:
        return default

def _ordered_unique(values: Iterable[str]) -> list[str]:
    """Return a unique list of strings while preserving the original order."""
    return list(dict.fromkeys(v for v in values if v))

def _first_present(mapping: Mapping[str, Any] | None, keys: Sequence[str]) -> Any:
    if not isinstance(mapping, Mapping):
        return None
    for key in keys:
        if key in mapping and not _is_blank(mapping.get(key)):
            return mapping.get(key)
    return None

# ------------------------------------------------------------------------------
# Page helpers
# ------------------------------------------------------------------------------

def normalize_page_name(page: Any) -> str | None:
    raw = _clean_text(page)
    if not raw:
        return None

    normalized = raw.replace("-", "_").replace(" ", "_").strip("_").lower()
    if normalized in _PAGE_ALIAS_MAP:
        return _PAGE_ALIAS_MAP[normalized]

    # Direct canonical match
    for canonical in CANONICAL_PAGES:
        if normalized == canonical.lower():
            return canonical

    return raw

def get_provider_hint_for_page(page: str | None) -> str:
    page = normalize_page_name(page)
    if page in {"Global_Markets", "Commodities_FX", "Mutual_Funds"}:
        return "EODHD_PRIMARY"
    if page in {"Market_Leaders", "My_Portfolio"}:
        return "KSA_PRIMARY"
    if page in {"Insights_Analysis", "Top_10_Investments"}:
        return "HYBRID"
    return "AUTO"

def get_default_symbols_for_page(page: Any, limit: int | None = None) -> list[str]:
    canonical = normalize_page_name(page)
    symbols = list(_DEFAULT_PAGE_SYMBOLS.get(canonical or "", []))
    if limit is not None and limit >= 0:
        return symbols[:limit]
    return symbols

# ------------------------------------------------------------------------------
# Symbol normalization
# ------------------------------------------------------------------------------

def normalize_symbol(raw: Any) -> str | None:
    token = _clean_text(raw)
    if not token:
        return None

    token = token.strip("\"'`")
    token = _EXTRA_SPACE_RE.sub("", token)
    token = token.upper()

    if not token:
        return None

    if token in {"N/A", "NA", "NULL", "NONE", "-", "--"}:
        return None

    # Friendly alias mapping
    if token in _FRIENDLY_SYMBOL_ALIASES:
        token = _FRIENDLY_SYMBOL_ALIASES[token]

    # KSA numeric only -> .SR
    if _KSA_NUMERIC_RE.fullmatch(token):
        return f"{token}.SR"

    # Normalize .sr suffix
    if _KSA_SUFFIX_RE.fullmatch(token):
        left = token.split(".", 1)[0]
        return f"{left}.SR"

    return token

def split_symbol_text(value: str) -> list[str]:
    text = _clean_text(value)
    if not text:
        return []

    # JSON array support
    if text.startswith("[") and text.endswith("]"):
        try:
            parsed = json.loads(text)
            if isinstance(parsed, list):
                return [str(x) for x in parsed if not _is_blank(x)]
        except Exception:
            pass

    # CSV reader for comma-heavy inputs
    if "," in text and "\n" in text:
        try:
            reader = csv.reader(io.StringIO(text))
            tokens: list[str] = []
            for row in reader:
                tokens.extend(col for col in row if not _is_blank(col))
            if tokens:
                return tokens
        except Exception:
            pass

    # Generic split
    parts = _MULTI_SPLIT_RE.split(text)
    return [part for part in parts if part and part.strip()]

def _extract_symbols_from_iterable(values: Iterable[Any]) -> list[str]:
    out: list[str] = []
    for item in values:
        if _is_blank(item):
            continue

        if isinstance(item, str):
            out.extend(split_symbol_text(item))
            continue

        if isinstance(item, Mapping):
            extracted = _first_present(item, ROW_SYMBOL_KEYS)
            if extracted is not None:
                out.extend(split_symbol_text(str(extracted)))
            continue

        out.append(str(item))
    return out

def _extract_symbols_from_mapping(payload: Mapping[str, Any]) -> list[str]:
    collected: list[str] = []

    # Direct symbol fields
    for key in SYMBOL_KEYS:
        if key in payload and not _is_blank(payload.get(key)):
            value = payload.get(key)
            if isinstance(value, (list, tuple, set)):
                collected.extend(_extract_symbols_from_iterable(value))
            elif isinstance(value, Mapping):
                collected.extend(_extract_symbols_from_mapping(value))
            else:
                collected.extend(split_symbol_text(str(value)))

    # Rows/items/data containers
    for key in ROW_CONTAINER_KEYS:
        value = payload.get(key)
        if isinstance(value, list):
            collected.extend(_extract_symbols_from_iterable(value))
        elif isinstance(value, Mapping):
            collected.extend(_extract_symbols_from_mapping(value))

    return collected

def extract_symbols_from_rows(rows: Sequence[Any], limit: int | None = None) -> list[str]:
    raw_symbols = _extract_symbols_from_iterable(rows)
    normalized = normalize_symbols(raw_symbols)
    if limit is not None and limit >= 0:
        return normalized[:limit]
    return normalized

def normalize_symbols(values: Iterable[Any]) -> list[str]:
    normalized: list[str] = []
    for raw in values:
        token = normalize_symbol(raw)
        if token:
            normalized.append(token)
    return _ordered_unique(normalized)

# ------------------------------------------------------------------------------
# Public resolution function
# ------------------------------------------------------------------------------

def resolve_symbols(
    payload: Mapping[str, Any] | None = None,
    query_params: Mapping[str, Any] | None = None,
    *,
    page: str | None = None,
    explicit_symbols: Iterable[Any] | None = None,
    allow_page_defaults: bool = True,
    default_limit: int | None = None,
) -> ResolvedSymbols:
    """
    Resolve symbols from mixed request sources.

    Priority
    --------
    1) explicit_symbols
    2) payload symbol fields
    3) query_params symbol fields
    4) page defaults (if allowed)

    Returns
    -------
    ResolvedSymbols
    """

    payload = payload or {}
    query_params = query_params or {}

    canonical_page = normalize_page_name(
        page
        or _first_present(payload, PAGE_KEYS)
        or _first_present(query_params, PAGE_KEYS)
    )

    provider_hint = get_provider_hint_for_page(canonical_page)

    limit = (
        _to_int(_first_present(payload, LIMIT_KEYS))
        or _to_int(_first_present(query_params, LIMIT_KEYS))
        or default_limit
    )

    dropped_tokens: list[str] = []
    requested_raw: list[str] = []

    # 1) Explicit symbols
    if explicit_symbols is not None:
        requested_raw.extend(_extract_symbols_from_iterable(list(explicit_symbols)))
        source = "explicit_symbols"
    else:
        source = "request"

    # 2) Payload
    requested_raw.extend(_extract_symbols_from_mapping(payload))

    # 3) Query params
    requested_raw.extend(_extract_symbols_from_mapping(query_params))

    requested_symbols = _ordered_unique([_clean_text(x) for x in requested_raw if _clean_text(x)])
    normalized_symbols: list[str] = []

    for raw in requested_symbols:
        norm = normalize_symbol(raw)
        if norm:
            normalized_symbols.append(norm)
        else:
            dropped_tokens.append(raw)

    normalized_symbols = _ordered_unique(normalized_symbols)

    used_page_defaults = False
    if not normalized_symbols and allow_page_defaults:
        normalized_symbols = get_default_symbols_for_page(canonical_page, limit=limit)
        used_page_defaults = True
        if source == "request":
            source = "page_defaults"

    if limit is not None and limit >= 0:
        normalized_symbols = normalized_symbols[:limit]

    return ResolvedSymbols(
        page=canonical_page,
        symbols=normalized_symbols,
        requested_symbols=requested_symbols,
        source=source,
        provider_hint=provider_hint,
        used_page_defaults=used_page_defaults,
        limit_applied=limit,
        dropped_tokens=dropped_tokens,
        meta={
            "input_count": len(requested_symbols),
            "output_count": len(normalized_symbols),
            "canonical_page": canonical_page,
        },
    )

# ------------------------------------------------------------------------------
# Convenience helpers for routes/builders
# ------------------------------------------------------------------------------

def resolve_symbols_from_request_parts(
    body: Mapping[str, Any] | None = None,
    query: Mapping[str, Any] | None = None,
    *,
    page: str | None = None,
    symbols: Iterable[Any] | None = None,
    allow_page_defaults: bool = True,
    default_limit: int | None = None,
) -> dict[str, Any]:
    """
    Route-friendly wrapper that returns a plain dict.
    """
    resolved = resolve_symbols(
        payload=body,
        query_params=query,
        page=page,
        explicit_symbols=symbols,
        allow_page_defaults=allow_page_defaults,
        default_limit=default_limit,
    )
    return resolved.to_dict()

def ensure_symbols(
    payload: Mapping[str, Any] | None = None,
    query_params: Mapping[str, Any] | None = None,
    *,
    page: str | None = None,
    explicit_symbols: Iterable[Any] | None = None,
    allow_page_defaults: bool = True,
    default_limit: int | None = None,
) -> list[str]:
    """
    Return only the resolved symbol list.
    """
    return resolve_symbols(
        payload=payload,
        query_params=query_params,
        page=page,
        explicit_symbols=explicit_symbols,
        allow_page_defaults=allow_page_defaults,
        default_limit=default_limit,
    ).symbols

def build_symbol_meta(
    payload: Mapping[str, Any] | None = None,
    query_params: Mapping[str, Any] | None = None,
    *,
    page: str | None = None,
    explicit_symbols: Iterable[Any] | None = None,
    allow_page_defaults: bool = True,
    default_limit: int | None = None,
) -> dict[str, Any]:
    """
    Meta-only helper for diagnostics / health / response envelopes.
    """
    resolved = resolve_symbols(
        payload=payload,
        query_params=query_params,
        page=page,
        explicit_symbols=explicit_symbols,
        allow_page_defaults=allow_page_defaults,
        default_limit=default_limit,
    )
    return {
        "page": resolved.page,
        "provider_hint": resolved.provider_hint,
        "source": resolved.source,
        "used_page_defaults": resolved.used_page_defaults,
        "requested_symbol_count": len(resolved.requested_symbols),
        "resolved_symbol_count": len(resolved.symbols),
        "dropped_tokens": resolved.dropped_tokens,
        "limit_applied": resolved.limit_applied,
    }

# ------------------------------------------------------------------------------
# Module exports
# ------------------------------------------------------------------------------

__all__ = [
    "CANONICAL_PAGES",
    "ResolvedSymbols",
    "PAGE_KEYS",
    "LIMIT_KEYS",
    "SYMBOL_KEYS",
    "ROW_SYMBOL_KEYS",
    "normalize_page_name",
    "normalize_symbol",
    "normalize_symbols",
    "split_symbol_text",
    "extract_symbols_from_rows",
    "get_default_symbols_for_page",
    "get_provider_hint_for_page",
    "resolve_symbols",
    "resolve_symbols_from_request_parts",
    "ensure_symbols",
    "build_symbol_meta",
]

# ------------------------------------------------------------------------------
# Execution / Testing
# ------------------------------------------------------------------------------
if __name__ == "__main__":
    # Example tests to verify functionality
    print("Testing resolve_symbols()...")
    
    # Test 1: Page defaults
    result = resolve_symbols(page="market leaders", limit=3)
    assert result.symbols == ["2222.SR", "1120.SR", "2010.SR"]
    print("✓ Page Defaults Passed")
    
    # Test 2: Extraction and mapping KSA suffixes / aliases
    payload = {"tickers": ["2222", "1120.sr", "GOLD", "N/A", "AAPL"]}
    result = resolve_symbols(payload=payload)
    assert result.symbols == ["2222.SR", "1120.SR", "GC=F", "AAPL"]
    print("✓ Extraction & Normalization Passed")

    # Test 3: Nested Row extractions
    payload_with_rows = {"data": [{"ticker": "TSLA"}, {"code": "MSFT"}]}
    result = resolve_symbols(payload=payload_with_rows)
    assert result.symbols == ["TSLA", "MSFT"]
    print("✓ Nested Extraction Passed")
    
    print("All tests successfully completed!")
