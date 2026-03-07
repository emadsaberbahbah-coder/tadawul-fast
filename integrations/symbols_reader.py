#!/usr/bin/env python3
"""
integrations/symbols_reader.py
================================================================================
TADAWUL FAST BRIDGE — SYMBOLS READER INTEGRATION BRIDGE — v3.0.0
================================================================================
ENGINE-COMPATIBLE • IMPORT-SAFE • SINGLE-SOURCE-OF-TRUTH • NO-DRIFT

Why this revision
-----------------
- ✅ FIX: Ensures integrations.symbols_reader exposes the SAME public API as
  core.symbols_reader.
- ✅ FIX: Prevents logic drift between integration path and core path.
- ✅ FIX: Supports lazy engine discovery patterns:
    - module-level functions
    - SymbolsReader class
    - get_reader()
    - create_reader()
    - build_reader()
- ✅ SAFE: If core.symbols_reader cannot be imported, returns empty results
  instead of crashing the app at import time.
- ✅ SAFE: No network I/O at import time.

Design
------
This module is a thin compatibility bridge. The canonical implementation lives in:
    core.symbols_reader

Any caller importing:
    integrations.symbols_reader
will receive the same runtime behavior and contract.
================================================================================
"""

from __future__ import annotations

import asyncio
import logging
from typing import Any, Dict, List, Optional

logger = logging.getLogger("integrations.symbols_reader")
logger.addHandler(logging.NullHandler())

SCRIPT_VERSION = "3.0.0"


# =============================================================================
# Canonical import bridge
# =============================================================================
try:
    from core.symbols_reader import (  # type: ignore
        PAGE_REGISTRY,
        ConfidenceLevel,
        DiscoveryStrategy,
        PageSpec,
        SymbolMetadata,
        SymbolType,
        SymbolsReader as _CoreSymbolsReader,
        build_reader as _core_build_reader,
        create_reader as _core_create_reader,
        detector,
        extractor,
        get_page_symbols as _core_get_page_symbols,
        get_page_symbols_async as _core_get_page_symbols_async,
        get_reader as _core_get_reader,
        get_sheet_symbols as _core_get_sheet_symbols,
        get_sheet_symbols_async as _core_get_sheet_symbols_async,
        get_symbols as _core_get_symbols,
        get_symbols_async as _core_get_symbols_async,
        get_symbols_for_page as _core_get_symbols_for_page,
        get_symbols_for_page_async as _core_get_symbols_for_page_async,
        get_symbols_for_sheet as _core_get_symbols_for_sheet,
        get_symbols_for_sheet_async as _core_get_symbols_for_sheet_async,
        get_universe as _core_get_universe,
        get_universe_async as _core_get_universe_async,
        list_symbols as _core_list_symbols,
        list_symbols_async as _core_list_symbols_async,
        list_symbols_for_page as _core_list_symbols_for_page,
        list_symbols_for_page_async as _core_list_symbols_for_page_async,
        list_tabs as _core_list_tabs,
        normalizer,
        read_symbols_for_sheet as _core_read_symbols_for_sheet,
        read_symbols_for_sheet_async as _core_read_symbols_for_sheet_async,
        supported_pages as _core_supported_pages,
    )

    _CORE_AVAILABLE = True

except Exception as e:  # pragma: no cover
    _CORE_AVAILABLE = False
    _CORE_IMPORT_ERROR = f"{type(e).__name__}: {e}"
    logger.warning("integrations.symbols_reader could not import core.symbols_reader: %s", _CORE_IMPORT_ERROR)

    class SymbolType(str):
        KSA = "ksa"
        GLOBAL = "global"
        INDEX = "index"
        ETF = "etf"
        MUTUAL_FUND = "mutual_fund"
        CURRENCY = "currency"
        CRYPTO = "crypto"
        COMMODITY = "commodity"
        UNKNOWN = "unknown"

    class DiscoveryStrategy(str):
        HEADER = "header"
        DATA_SCAN = "data_scan"
        ML = "ml"
        EXPLICIT = "explicit"
        DEFAULT = "default"

    class ConfidenceLevel(str):
        HIGH = "high"
        MEDIUM = "medium"
        LOW = "low"
        NONE = "none"

    class SymbolMetadata:  # lightweight fallback
        pass

    class PageSpec:  # lightweight fallback
        pass

    PAGE_REGISTRY: Dict[str, Any] = {}
    normalizer = None
    extractor = None
    detector = None


# =============================================================================
# Empty-result fallback helpers
# =============================================================================
def _empty_result_dict(error: str = "core.symbols_reader unavailable") -> Dict[str, Any]:
    return {
        "all": [],
        "symbols": [],
        "ksa": [],
        "global": [],
        "by_type": {},
        "metadata": [],
        "status": "error",
        "error": error,
        "version": SCRIPT_VERSION,
    }


def _empty_list() -> List[str]:
    return []


async def _empty_result_dict_async(error: str = "core.symbols_reader unavailable") -> Dict[str, Any]:
    return _empty_result_dict(error=error)


async def _empty_list_async() -> List[str]:
    return []


# =============================================================================
# Public sync wrappers
# =============================================================================
def get_page_symbols(key: str, spreadsheet_id: Optional[str] = None) -> Dict[str, Any]:
    if _CORE_AVAILABLE:
        return _core_get_page_symbols(key, spreadsheet_id=spreadsheet_id)
    return _empty_result_dict()


def get_universe(keys: List[str], spreadsheet_id: Optional[str] = None) -> Dict[str, Any]:
    if _CORE_AVAILABLE:
        return _core_get_universe(keys, spreadsheet_id=spreadsheet_id)
    return {
        "symbols": [],
        "by_origin": {},
        "by_type": {},
        "origin_map": {},
        "metadata": {},
        "status": "error",
        "error": "core.symbols_reader unavailable",
        "version": SCRIPT_VERSION,
    }


def get_symbols_for_sheet(
    sheet: Optional[str] = None,
    spreadsheet_id: Optional[str] = None,
    limit: int = 5000,
    **kwargs: Any,
) -> List[str]:
    if _CORE_AVAILABLE:
        return _core_get_symbols_for_sheet(sheet=sheet, spreadsheet_id=spreadsheet_id, limit=limit, **kwargs)
    return _empty_list()


def read_symbols_for_sheet(
    sheet: Optional[str] = None,
    spreadsheet_id: Optional[str] = None,
    limit: int = 5000,
    **kwargs: Any,
) -> List[str]:
    if _CORE_AVAILABLE:
        return _core_read_symbols_for_sheet(sheet=sheet, spreadsheet_id=spreadsheet_id, limit=limit, **kwargs)
    return _empty_list()


def get_sheet_symbols(
    sheet: Optional[str] = None,
    spreadsheet_id: Optional[str] = None,
    limit: int = 5000,
    **kwargs: Any,
) -> List[str]:
    if _CORE_AVAILABLE:
        return _core_get_sheet_symbols(sheet=sheet, spreadsheet_id=spreadsheet_id, limit=limit, **kwargs)
    return _empty_list()


def get_symbols_for_page(
    page: Optional[str] = None,
    spreadsheet_id: Optional[str] = None,
    limit: int = 5000,
    **kwargs: Any,
) -> List[str]:
    if _CORE_AVAILABLE:
        return _core_get_symbols_for_page(page=page, spreadsheet_id=spreadsheet_id, limit=limit, **kwargs)
    return _empty_list()


def list_symbols_for_page(
    page: Optional[str] = None,
    spreadsheet_id: Optional[str] = None,
    limit: int = 5000,
    **kwargs: Any,
) -> List[str]:
    if _CORE_AVAILABLE:
        return _core_list_symbols_for_page(page=page, spreadsheet_id=spreadsheet_id, limit=limit, **kwargs)
    return _empty_list()


def get_symbols(
    sheet: Optional[str] = None,
    page: Optional[str] = None,
    spreadsheet_id: Optional[str] = None,
    limit: int = 5000,
    **kwargs: Any,
) -> List[str]:
    if _CORE_AVAILABLE:
        return _core_get_symbols(sheet=sheet, page=page, spreadsheet_id=spreadsheet_id, limit=limit, **kwargs)
    return _empty_list()


def list_symbols(
    sheet: Optional[str] = None,
    page: Optional[str] = None,
    spreadsheet_id: Optional[str] = None,
    limit: int = 5000,
    **kwargs: Any,
) -> List[str]:
    if _CORE_AVAILABLE:
        return _core_list_symbols(sheet=sheet, page=page, spreadsheet_id=spreadsheet_id, limit=limit, **kwargs)
    return _empty_list()


def list_tabs(spreadsheet_id: Optional[str] = None) -> List[str]:
    if _CORE_AVAILABLE:
        return _core_list_tabs(spreadsheet_id=spreadsheet_id)
    return []


def supported_pages() -> List[str]:
    if _CORE_AVAILABLE:
        return _core_supported_pages()
    return []


# =============================================================================
# Public async wrappers
# =============================================================================
async def get_page_symbols_async(key: str, spreadsheet_id: Optional[str] = None) -> Dict[str, Any]:
    if _CORE_AVAILABLE:
        return await _core_get_page_symbols_async(key, spreadsheet_id=spreadsheet_id)
    return await _empty_result_dict_async()


async def get_universe_async(keys: List[str], spreadsheet_id: Optional[str] = None) -> Dict[str, Any]:
    if _CORE_AVAILABLE:
        return await _core_get_universe_async(keys, spreadsheet_id=spreadsheet_id)
    return {
        "symbols": [],
        "by_origin": {},
        "by_type": {},
        "origin_map": {},
        "metadata": {},
        "status": "error",
        "error": "core.symbols_reader unavailable",
        "version": SCRIPT_VERSION,
    }


async def get_symbols_for_sheet_async(
    sheet: Optional[str] = None,
    spreadsheet_id: Optional[str] = None,
    limit: int = 5000,
    **kwargs: Any,
) -> List[str]:
    if _CORE_AVAILABLE:
        return await _core_get_symbols_for_sheet_async(sheet=sheet, spreadsheet_id=spreadsheet_id, limit=limit, **kwargs)
    return await _empty_list_async()


async def read_symbols_for_sheet_async(
    sheet: Optional[str] = None,
    spreadsheet_id: Optional[str] = None,
    limit: int = 5000,
    **kwargs: Any,
) -> List[str]:
    if _CORE_AVAILABLE:
        return await _core_read_symbols_for_sheet_async(sheet=sheet, spreadsheet_id=spreadsheet_id, limit=limit, **kwargs)
    return await _empty_list_async()


async def get_sheet_symbols_async(
    sheet: Optional[str] = None,
    spreadsheet_id: Optional[str] = None,
    limit: int = 5000,
    **kwargs: Any,
) -> List[str]:
    if _CORE_AVAILABLE:
        return await _core_get_sheet_symbols_async(sheet=sheet, spreadsheet_id=spreadsheet_id, limit=limit, **kwargs)
    return await _empty_list_async()


async def get_symbols_for_page_async(
    page: Optional[str] = None,
    spreadsheet_id: Optional[str] = None,
    limit: int = 5000,
    **kwargs: Any,
) -> List[str]:
    if _CORE_AVAILABLE:
        return await _core_get_symbols_for_page_async(page=page, spreadsheet_id=spreadsheet_id, limit=limit, **kwargs)
    return await _empty_list_async()


async def list_symbols_for_page_async(
    page: Optional[str] = None,
    spreadsheet_id: Optional[str] = None,
    limit: int = 5000,
    **kwargs: Any,
) -> List[str]:
    if _CORE_AVAILABLE:
        return await _core_list_symbols_for_page_async(page=page, spreadsheet_id=spreadsheet_id, limit=limit, **kwargs)
    return await _empty_list_async()


async def get_symbols_async(
    sheet: Optional[str] = None,
    page: Optional[str] = None,
    spreadsheet_id: Optional[str] = None,
    limit: int = 5000,
    **kwargs: Any,
) -> List[str]:
    if _CORE_AVAILABLE:
        return await _core_get_symbols_async(sheet=sheet, page=page, spreadsheet_id=spreadsheet_id, limit=limit, **kwargs)
    return await _empty_list_async()


async def list_symbols_async(
    sheet: Optional[str] = None,
    page: Optional[str] = None,
    spreadsheet_id: Optional[str] = None,
    limit: int = 5000,
    **kwargs: Any,
) -> List[str]:
    if _CORE_AVAILABLE:
        return await _core_list_symbols_async(sheet=sheet, page=page, spreadsheet_id=spreadsheet_id, limit=limit, **kwargs)
    return await _empty_list_async()


# =============================================================================
# Reader object + factories
# =============================================================================
if _CORE_AVAILABLE:
    class SymbolsReader(_CoreSymbolsReader):
        """Integration-path alias of core.symbols_reader.SymbolsReader."""
        pass
else:
    class SymbolsReader:
        async def get_symbols_for_sheet(self, sheet: Optional[str] = None, limit: int = 5000, **kwargs: Any) -> List[str]:
            return []

        async def read_symbols_for_sheet(self, sheet: Optional[str] = None, limit: int = 5000, **kwargs: Any) -> List[str]:
            return []

        async def get_sheet_symbols(self, sheet: Optional[str] = None, limit: int = 5000, **kwargs: Any) -> List[str]:
            return []

        async def get_symbols_for_page(self, page: Optional[str] = None, limit: int = 5000, **kwargs: Any) -> List[str]:
            return []

        async def list_symbols_for_page(self, page: Optional[str] = None, limit: int = 5000, **kwargs: Any) -> List[str]:
            return []

        async def get_symbols(self, sheet: Optional[str] = None, page: Optional[str] = None, limit: int = 5000, **kwargs: Any) -> List[str]:
            return []

        async def list_symbols(self, sheet: Optional[str] = None, page: Optional[str] = None, limit: int = 5000, **kwargs: Any) -> List[str]:
            return []


def get_reader(*args: Any, **kwargs: Any) -> SymbolsReader:
    if _CORE_AVAILABLE:
        try:
            return _core_get_reader(*args, **kwargs)
        except Exception:
            return SymbolsReader()
    return SymbolsReader()


def create_reader(*args: Any, **kwargs: Any) -> SymbolsReader:
    if _CORE_AVAILABLE:
        try:
            return _core_create_reader(*args, **kwargs)
        except Exception:
            return SymbolsReader()
    return SymbolsReader()


def build_reader(*args: Any, **kwargs: Any) -> SymbolsReader:
    if _CORE_AVAILABLE:
        try:
            return _core_build_reader(*args, **kwargs)
        except Exception:
            return SymbolsReader()
    return SymbolsReader()


__all__ = [
    "SCRIPT_VERSION",
    "SymbolType",
    "DiscoveryStrategy",
    "ConfidenceLevel",
    "SymbolMetadata",
    "PageSpec",
    "PAGE_REGISTRY",
    "SymbolsReader",
    "get_reader",
    "create_reader",
    "build_reader",
    "get_page_symbols",
    "get_page_symbols_async",
    "get_universe",
    "get_universe_async",
    "get_symbols_for_sheet",
    "get_symbols_for_sheet_async",
    "read_symbols_for_sheet",
    "read_symbols_for_sheet_async",
    "get_sheet_symbols",
    "get_sheet_symbols_async",
    "get_symbols_for_page",
    "get_symbols_for_page_async",
    "list_symbols_for_page",
    "list_symbols_for_page_async",
    "get_symbols",
    "get_symbols_async",
    "list_symbols",
    "list_symbols_async",
    "list_tabs",
    "supported_pages",
    "normalizer",
    "extractor",
    "detector",
]
