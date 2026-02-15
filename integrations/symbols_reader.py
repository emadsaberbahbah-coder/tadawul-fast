#!/usr/bin/env python3
# integrations/symbols_reader.py
"""
integrations/symbols_reader.py
===========================================================
Compatibility + Repo-Hygiene Shim — v0.5.0 (PROD SAFE)

Why this file exists
- Some older modules may import: `from integrations.symbols_reader import ...`
- The canonical implementation lives in repo root: `symbols_reader.py`
- This shim ensures:
  1) This module is ALWAYS valid Python (no markdown fences / no runtime crashes)
  2) Imports remain backward-compatible
  3) No duplicate business logic (single source of truth)

Behavior
- Preferred: re-export symbols from root `symbols_reader.py`
- Fallback: provide safe outputs with consistent shape + minimal, well-tested normalization.
  Never raises during import; never crashes app startup.

Fallback return shape (always)
{
  "all": [...],
  "ksa": [...],
  "global": [...],
  "tickers": [...],   # alias of "all"
  "symbols": [...],   # alias of "all"
  "meta": {
      "ok": bool,
      "status": "success"|"partial"|"error",
      "error": "...?",
      "shim_version": "...",
      "canonical_import": "ok"|"failed",
      "canonical_version": "...",
      "notes": [...],
  }
}

Aligned conventions (TFB ecosystem)
- KSA tickers normalized to: ####.SR
- Supports: "1120", "1120.SR", "TADAWUL:1120", "1120.TADAWUL"
- Removes Arabic digits: ١٢٣٤٥٦٧٨٩٠ -> 1234567890
- Removes invisible chars (LRM/RLM/ZWS etc.)
"""

from __future__ import annotations

import logging
import os
import re
from typing import Any, Dict, Optional, Sequence, Tuple

logger = logging.getLogger("integrations.symbols_reader_shim")

SHIM_VERSION = "0.5.0"

_ARABIC_DIGITS = str.maketrans("٠١٢٣٤٥٦٧٨٩", "0123456789")
# common invisible bidi marks / zero-width
_INVIS_RE = re.compile(r"[\u200b\u200c\u200d\u200e\u200f\u202a\u202b\u202c\u202d\u202e\u2066\u2067\u2068\u2069]")

# fast KSA numeric detector (3-6 digits)
_KSA_NUM_RE = re.compile(r"^\d{3,6}$")
# allow already-normalized .SR
_KSA_SR_RE = re.compile(r"^(\d{3,6})\.SR$")


def _env_truthy(name: str, default: bool = False) -> bool:
    v = (os.getenv(name, "") or "").strip().lower()
    if not v:
        return default
    return v in {"1", "true", "yes", "y", "on", "t"}


def _meta_base() -> Dict[str, Any]:
    return {
        "ok": False,
        "status": "error",
        "shim_version": SHIM_VERSION,
        "canonical_import": "failed",
        "canonical_version": "unknown",
        "notes": [],
    }


def _bundle(all_list: Sequence[str], ksa: Sequence[str], glob: Sequence[str], meta: Dict[str, Any]) -> Dict[str, Any]:
    a = list(all_list or [])
    return {
        "all": a,
        "ksa": list(ksa or []),
        "global": list(glob or []),
        # aliases (some callers expect these keys)
        "tickers": a,
        "symbols": a,
        "meta": meta or {},
    }


def _clean_text(s: str) -> str:
    """
    Safe normalization:
    - strip
    - uppercase
    - Arabic digits -> Latin digits
    - remove invisible chars
    """
    if s is None:
        return ""
    t = str(s).strip()
    if not t:
        return ""
    t = t.translate(_ARABIC_DIGITS)
    t = _INVIS_RE.sub("", t)
    # normalize common separators
    t = t.replace("，", ",").replace("؛", ";")
    return t.strip().upper()


def _strip_known_prefixes_suffixes(sym: str) -> str:
    """
    Normalize common user/provider wrappers:
    - TADAWUL:1120 -> 1120
    - TDWL:1120 -> 1120
    - SA:1120 -> 1120
    - 1120.TADAWUL -> 1120
    """
    s = _clean_text(sym)
    if not s:
        return ""

    for p in ("TADAWUL:", "TDWL:", "SA:", "KSA:", "TASI:"):
        if s.startswith(p):
            s = s[len(p) :].strip()

    if s.endswith(".TADAWUL"):
        s = s[: -len(".TADAWUL")].strip()

    return s.strip()


def _normalize_ksa_symbol(sym: str) -> str:
    """
    Returns normalized KSA symbol ####.SR or "" if not KSA-like.
    """
    s = _strip_known_prefixes_suffixes(sym)
    if not s:
        return ""

    # Already .SR
    m = _KSA_SR_RE.match(s)
    if m:
        return f"{m.group(1)}.SR"

    # Raw numeric
    if _KSA_NUM_RE.match(s):
        return f"{s}.SR"

    return ""


def _normalize_global_symbol(sym: str) -> str:
    """
    Best-effort global normalization:
    - removes prefixes/suffixes, keeps indices (^), futures (=), FX pairs, etc.
    - doesn't force ".US" (backend/provider should decide)
    """
    s = _strip_known_prefixes_suffixes(sym)
    if not s:
        return ""

    # Keep special tickers as-is
    if any(ch in s for ch in ("^", "=", "/")):
        return s

    # Avoid accidental KSA numeric here (handled earlier)
    if _KSA_NUM_RE.match(s):
        return f"{s}.SR"

    return s


def split_tickers_by_market(tickers: Sequence[str]) -> Tuple[list, list]:
    """
    Market split aligned to TFB contract:
    - KSA: ####.SR (numeric 3-6 digits normalized)
    - Global: everything else (normalized)
    Returns: (ksa_list, global_list)
    """
    seen_ksa, seen_glb = set(), set()
    ksa: list = []
    glob: list = []

    for t in (tickers or []):
        raw = _clean_text(str(t))
        if not raw:
            continue

        k = _normalize_ksa_symbol(raw)
        if k:
            if k not in seen_ksa:
                seen_ksa.add(k)
                ksa.append(k)
            continue

        g = _normalize_global_symbol(raw)
        if g and g not in seen_glb:
            seen_glb.add(g)
            glob.append(g)

    return ksa, glob


# =============================================================================
# Preferred: canonical re-export
# =============================================================================
_CANONICAL_IMPORT_ERROR: Optional[str] = None

try:
    # ✅ Canonical module (repo root)
    from symbols_reader import (  # type: ignore
        PageConfig,
        PAGE_REGISTRY,
        split_tickers_by_market as _canon_split_tickers_by_market,
        get_symbols_from_sheet,
        get_page_symbols,
        get_all_pages_symbols,
    )

    # If canonical exposes a version, pass it through
    try:
        from symbols_reader import SYMBOLS_READER_VERSION as SYMBOLS_READER_VERSION  # type: ignore
    except Exception:
        SYMBOLS_READER_VERSION = "unknown"

    # Optionally prefer canonical split (but keep our fallback available)
    if callable(_canon_split_tickers_by_market):
        split_tickers_by_market = _canon_split_tickers_by_market  # type: ignore

    # Done: canonical path
    __CANON_OK__ = True

except Exception as _import_exc:  # pragma: no cover
    __CANON_OK__ = False
    _CANONICAL_IMPORT_ERROR = f"{_import_exc.__class__.__name__}: {_import_exc}"

    # Quiet import: do NOT crash app startup
    if _env_truthy("TFB_DEBUG_IMPORTS", False):
        logger.warning(
            "[integrations.symbols_reader] Fallback active (root symbols_reader import failed): %s",
            _CANONICAL_IMPORT_ERROR,
        )

    # Minimal placeholders for exported names
    PageConfig = object  # type: ignore
    PAGE_REGISTRY = {}  # type: ignore
    SYMBOLS_READER_VERSION = "fallback"

    def _meta_err(msg: str, *, status: str = "error") -> Dict[str, Any]:
        m = _meta_base()
        m["ok"] = False
        m["status"] = status
        m["error"] = msg
        m["canonical_import"] = "failed"
        m["canonical_version"] = SYMBOLS_READER_VERSION
        m["notes"] = [
            "Fallback shim active: root symbols_reader.py import failed.",
            "Outputs are safe but may be empty because sheet reading requires canonical module.",
        ]
        return m

    def get_symbols_from_sheet(  # type: ignore
        spreadsheet_id: str,
        sheet_name: str,
        header_row: int = 5,
        max_cols: int = 52,
        scan_rows: int = 2000,
    ) -> Dict[str, Any]:
        """
        Fallback: cannot read Google Sheets without canonical implementation.
        Returns empty bundle with error meta (never raises).
        """
        meta = _meta_err(f"symbols_reader import failed: {_CANONICAL_IMPORT_ERROR}")
        return _bundle([], [], [], meta)

    def get_page_symbols(page_key: str, spreadsheet_id: Optional[str] = None) -> Dict[str, Any]:  # type: ignore
        """
        Fallback: cannot resolve pages without PAGE_REGISTRY.
        """
        meta = _meta_err(f"symbols_reader import failed: {_CANONICAL_IMPORT_ERROR}")
        return _bundle([], [], [], meta)

    def get_all_pages_symbols(spreadsheet_id: Optional[str] = None) -> Dict[str, Dict[str, Any]]:  # type: ignore
        """
        Fallback: keep shape consistent: dict(page_key -> bundle).
        No pages available without canonical registry.
        """
        return {}


# =============================================================================
# Public exports
# =============================================================================
__all__ = [
    "PageConfig",
    "PAGE_REGISTRY",
    "SYMBOLS_READER_VERSION",
    "split_tickers_by_market",
    "get_symbols_from_sheet",
    "get_page_symbols",
    "get_all_pages_symbols",
    "SHIM_VERSION",
]
