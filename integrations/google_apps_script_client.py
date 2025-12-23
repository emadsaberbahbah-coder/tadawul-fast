"""
google_apps_script_client.py
------------------------------------------------------------
Google Apps Script client for Tadawul Fast Bridge – v2.4.0 (HARDENED)

GOALS
- Robust bridge for backend/tools to call your deployed Google Apps Script WebApp.
- Routes tickers intelligently:
      • KSA (.SR / TADAWUL: / numeric) -> Apps Script KSA logic (Argaam/Tadawul)
      • Global                         -> Apps Script Global logic (EODHD/FMP/other)
- Reads configuration from env.py / core.config / environment variables (in that order).
- NEVER calls market providers directly (only talks to GAS).

NOTES
- Uses urllib only (max portability).
- Defensive networking: retries for transient failures, JSON-safe parsing.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import random
import ssl
import time
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Sequence, Tuple

logger = logging.getLogger("google_apps_script_client")

CLIENT_VERSION = "2.4.0"


# =============================================================================
# Settings resolution (env.py -> core.config -> env vars)
# =============================================================================
def _safe_import(path: str) -> Optional[Any]:
    try:
        __import__(path)
        return __import__(path, fromlist=["*"])
    except Exception:
        return None


_env_mod = _safe_import("env")
_settings_obj = getattr(_env_mod, "settings", None) if _env_mod else None

_core_cfg = _safe_import("core.config")
_get_settings = getattr(_core_cfg, "get_settings", None) if _core_cfg else None


def _get_str(*, attr: str, env_key: str, default: str = "") -> str:
    # 1) env.py settings
    try:
        if _settings_obj is not None:
            v = getattr(_settings_obj, attr, None)
            if isinstance(v, str) and v.strip():
                return v.strip()
    except Exception:
        pass

    # 2) core.config Settings
    try:
        if callable(_get_settings):
            s = _get_settings()
            v = getattr(s, attr, None)
            if isinstance(v, str) and v.strip():
                return v.strip()
    except Exception:
        pass

    # 3) env var
    v2 = os.getenv(env_key, default) or default
    return v2.strip() if isinstance(v2, str) else default


def _get_float(*, attr: str, env_key: str, default: float) -> float:
    # settings/core.config
    for src in ("env", "core"):
        try:
            if src == "env" and _settings_obj is not None:
                v = getattr(_settings_obj, attr, None)
            elif src == "core" and callable(_get_settings):
                v = getattr(_get_settings(), attr, None)
            else:
                v = None
            if v is not None:
                return float(v)
        except Exception:
            pass

    # env var
    try:
        return float(os.getenv(env_key, str(default)))
    except Exception:
        return float(default)


_DEFAULT_URL = _get_str(attr="google_apps_script_backup_url", env_key="GOOGLE_APPS_SCRIPT_BACKUP_URL", default="")
_DEFAULT_TOKEN = _get_str(attr="app_token", env_key="APP_TOKEN", default="")
_BACKEND_URL = _get_str(attr="backend_base_url", env_key="BACKEND_BASE_URL", default="")

_DEFAULT_TIMEOUT = _get_float(attr="apps_script_timeout_sec", env_key="APPS_SCRIPT_TIMEOUT_SEC", default=45.0)


# =============================================================================
# SSL context
# =============================================================================
_SSL_CONTEXT_DEFAULT = ssl.create_default_context()


def _make_ssl_context(verify_ssl: bool = True) -> ssl.SSLContext:
    if verify_ssl:
        return _SSL_CONTEXT_DEFAULT
    # Not recommended, but useful for debugging certain environments
    ctx = ssl.create_default_context()
    ctx.check_hostname = False
    ctx.verify_mode = ssl.CERT_NONE
    return ctx


# =============================================================================
# Data structures
# =============================================================================
@dataclass
class AppsScriptResult:
    ok: bool
    status_code: int
    data: Any
    error: Optional[str] = None
    raw_text: Optional[str] = None
    url: Optional[str] = None
    elapsed_ms: Optional[int] = None

    def as_dict(self) -> Dict[str, Any]:
        return {
            "ok": self.ok,
            "status_code": self.status_code,
            "data": self.data,
            "error": self.error,
            "raw_text": self.raw_text,
            "url": self.url,
            "elapsed_ms": self.elapsed_ms,
        }


# =============================================================================
# Symbol / Market split helpers
# =============================================================================
def _normalize_ksa_symbol(symbol: str) -> str:
    """
    Strict KSA normalizer:
    - '1120' -> '1120.SR'
    - 'TADAWUL:1120' -> '1120.SR'
    - '1120.TADAWUL' -> '1120.SR'
    - '1120.SR' -> '1120.SR'
    Returns "" if invalid.
    """
    s = (symbol or "").strip().upper()
    if not s:
        return ""

    if s.startswith("TADAWUL:"):
        s = s.split(":", 1)[1].strip()

    if s.endswith(".TADAWUL"):
        s = s[: -len(".TADAWUL")].strip()

    if s.endswith(".SR"):
        base = s[:-3]
        return f"{base}.SR" if base.isdigit() else ""

    # allow 3–6 digits to be safe (some systems include 3-digit indices)
    if s.isdigit() and 3 <= len(s) <= 6:
        return f"{s}.SR"

    return ""


def split_tickers_by_market(tickers: Sequence[str]) -> Tuple[List[str], List[str]]:
    """
    Split tickers into KSA vs Global.

    KSA rules:
    - Numeric (3–6 digits) => Tadawul => .SR
    - Ends with .SR
    - TADAWUL:xxxx or xxxx.TADAWUL

    Returns (ksa, global) with de-dup preserving order.
    """
    seen_ksa = set()
    seen_glb = set()
    ksa: List[str] = []
    glb: List[str] = []

    for t in tickers or []:
        raw = (t or "").strip()
        if not raw:
            continue

        ksa_norm = _normalize_ksa_symbol(raw)
        if ksa_norm:
            if ksa_norm not in seen_ksa:
                seen_ksa.add(ksa_norm)
                ksa.append(ksa_norm)
            continue

        up = raw.strip().upper()
        if up not in seen_glb:
            seen_glb.add(up)
            glb.append(up)

    return ksa, glb


# =============================================================================
# Client
# =============================================================================
class GoogleAppsScriptClient:
    """
    Lightweight client to call a deployed Google Apps Script WebApp.
    Uses urllib standard library.
    """

    def __init__(
        self,
        base_url: Optional[str] = None,
        app_token: Optional[str] = None,
        timeout: Optional[float] = None,
        verify_ssl: bool = True,
    ) -> None:
        raw_url = (base_url or _DEFAULT_URL or "").strip()

        # Fix missing scheme
        if raw_url and not raw_url.startswith(("http://", "https://")):
            raw_url = "https://" + raw_url

        self.base_url: str = raw_url.rstrip("/")
        self.app_token: str = (app_token or _DEFAULT_TOKEN or "").strip()
        self.timeout: float = float(timeout if timeout is not None else _DEFAULT_TIMEOUT)
        self._ssl_context: ssl.SSLContext = _make_ssl_context(verify_ssl=verify_ssl)

        if not self.base_url:
            logger.warning("[AppsScriptClient] No URL configured (GOOGLE_APPS_SCRIPT_BACKUP_URL). Calls will fail.")

    # ------------------------------------------------------------------
    # URL + parsing
    # ------------------------------------------------------------------
    def _build_url(self, *, mode: str, extra_query: Optional[Dict[str, str]] = None) -> str:
        mode = (mode or "").strip()
        if not mode:
            mode = "default"

        url_parts = list(urllib.parse.urlparse(self.base_url))
        existing_qs = dict(urllib.parse.parse_qsl(url_parts[4] or ""))

        existing_qs["mode"] = mode
        if extra_query:
            for k, v in extra_query.items():
                if k and v is not None:
                    existing_qs[str(k)] = str(v)

        url_parts[4] = urllib.parse.urlencode(existing_qs)
        return urllib.parse.urlunparse(url_parts)

    @staticmethod
    def _parse_json_or_text(text: str) -> Any:
        if text is None:
            return None
        t = text.strip()
        if not t:
            return None
        try:
            return json.loads(t)
        except Exception:
            return {"raw": text}

    # ------------------------------------------------------------------
    # Retry policy
    # ------------------------------------------------------------------
    @staticmethod
    def _is_retryable_status(code: int) -> bool:
        # Retry common transient statuses
        return code in (408, 425, 429) or 500 <= code <= 599

    @staticmethod
    def _sleep_backoff(attempt: int) -> None:
        # exponential backoff with jitter (max ~10s)
        base = min(10.0, (2.0 ** attempt))
        jitter = random.uniform(0.0, 0.6)
        time.sleep(base + jitter)

    # ------------------------------------------------------------------
    # Core call
    # ------------------------------------------------------------------
    def call_script(
        self,
        mode: str,
        payload: Optional[Dict[str, Any]] = None,
        method: str = "POST",
        extra_query: Optional[Dict[str, str]] = None,
        retries: int = 2,
        headers: Optional[Dict[str, str]] = None,
    ) -> AppsScriptResult:
        if not self.base_url:
            return AppsScriptResult(False, 0, None, "GOOGLE_APPS_SCRIPT_BACKUP_URL not configured")

        method = (method or "POST").strip().upper()
        url = self._build_url(mode=mode, extra_query=extra_query)

        req_headers: Dict[str, str] = {
            "Content-Type": "application/json; charset=utf-8",
            "Accept": "application/json, text/plain;q=0.9, */*;q=0.8",
            "User-Agent": f"TadawulFastBridge/AppsScriptClient/{CLIENT_VERSION}",
        }
        if self.app_token:
            req_headers["X-APP-TOKEN"] = self.app_token
        if headers:
            for k, v in headers.items():
                if k and v is not None:
                    req_headers[str(k)] = str(v)

        data_bytes: Optional[bytes] = None

        # If GET with payload, push payload keys into query (stringified)
        if method == "GET" and payload:
            q = dict(extra_query or {})
            for k, v in payload.items():
                if v is None:
                    continue
                q[str(k)] = str(v)
            url = self._build_url(mode=mode, extra_query=q)
            payload = None

        if method != "GET" and payload is not None:
            try:
                data_bytes = json.dumps(payload, ensure_ascii=False).encode("utf-8")
            except Exception as exc:
                return AppsScriptResult(False, 0, None, f"JSON Encode Error: {exc}", url=url)

        last_err: Optional[str] = None
        started = time.time()

        for attempt in range(max(0, int(retries)) + 1):
            try:
                req = urllib.request.Request(url, data=data_bytes, headers=req_headers, method=method)
                with urllib.request.urlopen(req, timeout=self.timeout, context=self._ssl_context) as resp:
                    status_code = getattr(resp, "status", resp.getcode())
                    raw = resp.read()
                    text = raw.decode("utf-8", errors="replace")
                    parsed = self._parse_json_or_text(text)

                    ok = 200 <= int(status_code) < 300
                    elapsed_ms = int((time.time() - started) * 1000)

                    # retryable even if response is HTTP but transient
                    if not ok and self._is_retryable_status(int(status_code)) and attempt < int(retries):
                        last_err = f"HTTP {status_code}"
                        logger.warning(
                            "[AppsScriptClient] %s (attempt=%d/%d) -> retry",
                            last_err,
                            attempt + 1,
                            int(retries) + 1,
                        )
                        self._sleep_backoff(attempt)
                        continue

                    return AppsScriptResult(
                        ok=ok,
                        status_code=int(status_code),
                        data=parsed,
                        error=None if ok else f"HTTP {status_code}",
                        raw_text=text,
                        url=url,
                        elapsed_ms=elapsed_ms,
                    )

            except urllib.error.HTTPError as exc:
                # We got an HTTP status (4xx/5xx). Decide to retry based on code.
                code = int(getattr(exc, "code", 0) or 0)
                text = ""
                try:
                    if exc.fp:
                        text = exc.read().decode("utf-8", errors="replace")
                except Exception:
                    text = ""
                parsed = self._parse_json_or_text(text)
                elapsed_ms = int((time.time() - started) * 1000)

                if self._is_retryable_status(code) and attempt < int(retries):
                    last_err = f"HTTP {code}"
                    logger.warning(
                        "[AppsScriptClient] %s (attempt=%d/%d) -> retry",
                        last_err,
                        attempt + 1,
                        int(retries) + 1,
                    )
                    self._sleep_backoff(attempt)
                    continue

                return AppsScriptResult(
                    ok=False,
                    status_code=code,
                    data=parsed,
                    error=f"HTTP {code}: {getattr(exc, 'reason', '')}".strip() or f"HTTP {code}",
                    raw_text=text,
                    url=url,
                    elapsed_ms=elapsed_ms,
                )

            except Exception as exc:
                elapsed_ms = int((time.time() - started) * 1000)
                last_err = str(exc)

                if attempt < int(retries):
                    logger.warning(
                        "[AppsScriptClient] Network error '%s' (attempt=%d/%d) -> retry",
                        last_err,
                        attempt + 1,
                        int(retries) + 1,
                    )
                    self._sleep_backoff(attempt)
                    continue

                return AppsScriptResult(
                    ok=False,
                    status_code=0,
                    data=None,
                    error=f"Max retries exceeded. Last error: {last_err}",
                    raw_text=None,
                    url=url,
                    elapsed_ms=elapsed_ms,
                )

        # should not reach
        return AppsScriptResult(False, 0, None, f"Max retries exceeded. Last error: {last_err}", url=url)

    # ------------------------------------------------------------------
    # Async wrapper
    # ------------------------------------------------------------------
    async def call_script_async(
        self,
        mode: str,
        payload: Optional[Dict[str, Any]] = None,
        method: str = "POST",
        extra_query: Optional[Dict[str, str]] = None,
        retries: int = 2,
        headers: Optional[Dict[str, str]] = None,
    ) -> AppsScriptResult:
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(
            None,
            lambda: self.call_script(
                mode=mode,
                payload=payload,
                method=method,
                extra_query=extra_query,
                retries=retries,
                headers=headers,
            ),
        )

    # ------------------------------------------------------------------
    # Payload builders / operations
    # ------------------------------------------------------------------
    @staticmethod
    def _now_utc_iso() -> str:
        return datetime.now(timezone.utc).isoformat()

    def build_quotes_payload(
        self,
        sheet_id: str,
        sheet_name: str,
        tickers: Sequence[str],
        extra_meta: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """
        Construct payload expected by GAS doPost.

        We include:
        - split tickers (ksa/global/all)
        - sheet id/name
        - backend base url (optional)
        - meta: client version + timestamp + any extra
        """
        ksa_tickers, global_tickers = split_tickers_by_market(list(tickers or []))

        meta: Dict[str, Any] = {
            "client": "tadawul_fast_bridge",
            "client_version": CLIENT_VERSION,
            "generated_at_utc": self._now_utc_iso(),
        }
        if extra_meta:
            try:
                meta.update(extra_meta)
            except Exception:
                meta["extra_meta_raw"] = str(extra_meta)

        return {
            "ksa_tickers": ksa_tickers,
            "global_tickers": global_tickers,
            "all_tickers": ksa_tickers + global_tickers,
            "sheet": {"id": (sheet_id or "").strip(), "name": (sheet_name or "").strip()},
            "backend": {"base_url": (_BACKEND_URL or "").strip()},
            "meta": meta,
        }

    def sync_quotes_to_sheet(
        self,
        sheet_id: str,
        sheet_name: str,
        tickers: Sequence[str],
        *,
        mode: str = "refresh_quotes",
        retries: int = 2,
    ) -> AppsScriptResult:
        payload = self.build_quotes_payload(sheet_id, sheet_name, tickers)
        return self.call_script(mode=mode, payload=payload, method="POST", retries=retries)

    # Small convenience ping (if your GAS supports it)
    def ping(self, *, mode: str = "ping") -> AppsScriptResult:
        return self.call_script(mode=mode, payload=None, method="GET", retries=0)


# =============================================================================
# Singleton
# =============================================================================
apps_script_client = GoogleAppsScriptClient()


def get_apps_script_client() -> GoogleAppsScriptClient:
    return apps_script_client


__all__ = [
    "AppsScriptResult",
    "GoogleAppsScriptClient",
    "apps_script_client",
    "get_apps_script_client",
    "split_tickers_by_market",
]
