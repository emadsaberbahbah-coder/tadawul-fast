"""
core/providers/argaam_provider.py
===============================================================
Argaam Provider (v1.0.0) — optional enrichment (PROD SAFE)

Notes
- DataEngineV2 expects this module if "argaam" is in KSA_PROVIDERS.
- If you don't configure ARGAAM_* URLs, it returns a clean "not configured" error,
  NOT an import crash.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import random
from datetime import datetime, timezone
from typing import Any, Dict, Optional, Tuple, Union

import httpx
from cachetools import TTLCache

logger = logging.getLogger("core.providers.argaam_provider")

PROVIDER_VERSION = "1.0.0"
USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/124.0.0.0 Safari/537.36"
)

DEFAULT_TIMEOUT_SEC = 20.0
DEFAULT_RETRY_ATTEMPTS = 2

_CLIENT: Optional[httpx.AsyncClient] = None
_LOCK = asyncio.Lock()

_CACHE: TTLCache = TTLCache(maxsize=5000, ttl=max(5.0, float(os.getenv("ARGAAM_TTL_SEC", "15") or "15")))


def _utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _safe_str(x: Any) -> Optional[str]:
    if x is None:
        return None
    s = str(x).strip()
    return s or None


def _get_headers() -> Dict[str, str]:
    h = {
        "User-Agent": USER_AGENT,
        "Accept": "application/json,text/html;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.8,ar;q=0.6",
    }
    raw = _safe_str(os.getenv("ARGAAM_HEADERS_JSON", ""))
    if raw:
        try:
            obj = json.loads(raw)
            if isinstance(obj, dict):
                for k, v in obj.items():
                    h[str(k)] = str(v)
        except Exception:
            pass
    return h


async def _get_client() -> httpx.AsyncClient:
    global _CLIENT
    if _CLIENT is None:
        async with _LOCK:
            if _CLIENT is None:
                timeout_sec = float(os.getenv("ARGAAM_TIMEOUT_SEC", str(DEFAULT_TIMEOUT_SEC)) or DEFAULT_TIMEOUT_SEC)
                timeout = httpx.Timeout(timeout_sec, connect=min(10.0, timeout_sec))
                _CLIENT = httpx.AsyncClient(timeout=timeout, follow_redirects=True, headers=_get_headers())
                logger.info("Argaam client init v%s | timeout=%.1fs", PROVIDER_VERSION, timeout_sec)
    return _CLIENT


async def aclose_argaam_client() -> None:
    global _CLIENT
    if _CLIENT is None:
        return
    try:
        await _CLIENT.aclose()
    finally:
        _CLIENT = None


async def _get_json(url: str) -> Optional[Union[Dict[str, Any], list]]:
    retries = int(os.getenv("ARGAAM_RETRY_ATTEMPTS", str(DEFAULT_RETRY_ATTEMPTS)) or DEFAULT_RETRY_ATTEMPTS)
    client = await _get_client()

    for attempt in range(max(1, retries)):
        try:
            r = await client.get(url)
            if r.status_code in (429,) or 500 <= r.status_code < 600:
                if attempt < (retries - 1):
                    await asyncio.sleep(0.25 * (2**attempt) + random.random() * 0.25)
                    continue
                return None
            if r.status_code >= 400:
                return None
            try:
                return r.json()
            except Exception:
                return None
        except Exception:
            if attempt < (retries - 1):
                await asyncio.sleep(0.25 * (2**attempt) + random.random() * 0.25)
                continue
            return None
    return None


def _format_url(tpl: str, symbol: str) -> str:
    s = (symbol or "").strip().upper()
    code = s[:-3] if s.endswith(".SR") else s
    return (tpl or "").replace("{symbol}", s).replace("{code}", code)


async def fetch_quote_patch(symbol: str) -> Tuple[Dict[str, Any], Optional[str]]:
    tpl = _safe_str(os.getenv("ARGAAM_QUOTE_URL", ""))
    if not tpl:
        return {}, "argaam: not configured (ARGAAM_QUOTE_URL)"

    sym = (symbol or "").strip().upper()
    if not sym:
        return {}, "argaam: empty symbol"

    ck = f"q::{sym}"
    hit = _CACHE.get(ck)
    if isinstance(hit, dict) and hit:
        return dict(hit), None

    url = _format_url(tpl, sym)
    data = await _get_json(url)
    if not data or not isinstance(data, dict):
        return {}, "argaam: empty response"

    # Best-effort mapping (depends on your endpoint payload)
    patch: Dict[str, Any] = {
        "market": "KSA",
        "currency": "SAR",
        "data_source": "argaam",
        "last_updated_utc": _utc_iso(),
    }

    # If your endpoint returns useful keys, extend mapping here later.
    _CACHE[ck] = dict(patch)
    return patch, None


__all__ = ["fetch_quote_patch", "aclose_argaam_client", "PROVIDER_VERSION"]
