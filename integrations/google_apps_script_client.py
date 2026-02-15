# integrations/google_apps_script_client.py  (FULL REPLACEMENT) — v3.2.0
"""
integrations/google_apps_script_client.py
------------------------------------------------------------
Google Apps Script client for Tadawul Fast Bridge — v3.2.0 (ADVANCED + RESILIENT + ALIGNED)

GOALS
- Robust bridge for backend/tools to call your deployed Google Apps Script WebApp.
- Smart Routing: Normalize sheet/page keys and split KSA vs Global tickers reliably.
- Resilience: Jittered exponential backoff, retry-after support, backup URL failover,
  and circuit-breaker for repeated failures / auth lockouts.
- Payload Hygiene: Recursive prune of None, safe JSON encoding, optional debug hooks.
- Environment Aware: Adjust retries/timeouts based on APP_ENV, and emit structured logs.

ENV (selected)
- GOOGLE_APPS_SCRIPT_URL / GOOGLE_APPS_SCRIPT_BACKUP_URL
- APP_TOKEN / BACKUP_APP_TOKEN
- APPS_SCRIPT_TIMEOUT_SEC
- APPS_SCRIPT_VERIFY_SSL=1|0
- APPS_SCRIPT_TOKEN_TRANSPORT="query,body,header" (any combo)
- APPS_SCRIPT_TOKEN_PARAM_NAME="token"
- APPS_SCRIPT_MAX_RETRIES=2
- APPS_SCRIPT_MAX_RAW_TEXT_CHARS=12000
- APPS_SCRIPT_CB_FAIL_THRESHOLD=4
- APPS_SCRIPT_CB_OPEN_SEC=45
- APPS_SCRIPT_AUTH_LOCK_SEC=300
- APPS_SCRIPT_HTTP_METHOD="POST" (default)
- BACKEND_BASE_URL (optional metadata)
"""

from __future__ import annotations

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

CLIENT_VERSION = "3.2.0"
_TRUTHY = {"1", "true", "yes", "y", "on", "t"}


# =============================================================================
# Env helpers (import-safe)
# =============================================================================
def _get_env_str(key: str, default: str = "") -> str:
    try:
        return (os.getenv(key) or default).strip()
    except Exception:
        return default


def _get_env_int(key: str, default: int) -> int:
    try:
        return int(float(os.getenv(key, str(default))))
    except Exception:
        return default


def _get_env_float(key: str, default: float) -> float:
    try:
        return float(os.getenv(key, str(default)))
    except Exception:
        return default


def _get_env_bool(key: str, default: bool = False) -> bool:
    try:
        v = (os.getenv(key) or "").strip().lower()
        if not v:
            return bool(default)
        return v in _TRUTHY
    except Exception:
        return bool(default)


def _utc_iso() -> str:
    try:
        return datetime.now(timezone.utc).isoformat()
    except Exception:
        return ""


# =============================================================================
# Configuration
# =============================================================================
_APP_ENV = _get_env_str("APP_ENV", "prod").lower()

_PRIMARY_URL = _get_env_str("GOOGLE_APPS_SCRIPT_URL")
_BACKUP_URL = _get_env_str("GOOGLE_APPS_SCRIPT_BACKUP_URL")
_DEFAULT_URL = _PRIMARY_URL or _BACKUP_URL

_APP_TOKEN = _get_env_str("APP_TOKEN")
_BACKUP_TOKEN = _get_env_str("BACKUP_APP_TOKEN")
_DEFAULT_TOKEN = _APP_TOKEN or _BACKUP_TOKEN

_BACKEND_URL = _get_env_str("BACKEND_BASE_URL")

_TIMEOUT = _get_env_float("APPS_SCRIPT_TIMEOUT_SEC", 45.0)
_VERIFY_SSL = _get_env_bool("APPS_SCRIPT_VERIFY_SSL", True)

_TOKEN_TRANSPORT = _get_env_str("APPS_SCRIPT_TOKEN_TRANSPORT", "query,body").lower()
_TOKEN_PARAM = _get_env_str("APPS_SCRIPT_TOKEN_PARAM_NAME", "token")

_MAX_RETRIES = _get_env_int("APPS_SCRIPT_MAX_RETRIES", 2)
_MAX_LOG_CHARS = _get_env_int("APPS_SCRIPT_MAX_RAW_TEXT_CHARS", 12000)

# Circuit breaker
_CB_FAIL_THRESHOLD = _get_env_int("APPS_SCRIPT_CB_FAIL_THRESHOLD", 4)
_CB_OPEN_SEC = _get_env_int("APPS_SCRIPT_CB_OPEN_SEC", 45)
_AUTH_LOCK_SEC = _get_env_int("APPS_SCRIPT_AUTH_LOCK_SEC", 300)

_DEFAULT_HTTP_METHOD = _get_env_str("APPS_SCRIPT_HTTP_METHOD", "POST").upper().strip() or "POST"


# =============================================================================
# Page Specs (aligned with your GAS modes)
# =============================================================================
PAGE_SPECS: Dict[str, Dict[str, str]] = {
    # Your main pages
    "MARKET_LEADERS": {"market": "mixed", "mode": "refresh_quotes"},
    "MY_PORTFOLIO": {"market": "mixed", "mode": "refresh_quotes"},
    "GLOBAL_MARKETS": {"market": "global", "mode": "refresh_quotes_global"},
    "KSA_TADAWUL": {"market": "ksa", "mode": "refresh_quotes_ksa"},
    "MUTUAL_FUNDS": {"market": "global", "mode": "refresh_quotes_global"},
    "COMMODITIES_FX": {"market": "global", "mode": "refresh_quotes_global"},
    "INSIGHTS_ANALYSIS": {"market": "mixed", "mode": "refresh_quotes"},
}


def _norm_page_key(page_key: str) -> str:
    """
    Normalizes a page key/sheet name to canonical keys used in PAGE_SPECS.
    Tries to align with core.schemas.resolve_sheet_key if available, but stays import-safe.
    """
    raw = (page_key or "").strip()
    if not raw:
        return ""

    # Try core.schemas resolver if available (import-safe)
    try:
        from core.schemas import resolve_sheet_key  # type: ignore

        k = resolve_sheet_key(raw) or ""
        k = k.strip().upper()
    except Exception:
        # fallback normalization
        k = raw.strip().upper()
        for ch in ["-", " ", ".", "/", "\\", "|", ":", ";", ",", "(", ")"]:
            k = k.replace(ch, "_")
        while "__" in k:
            k = k.replace("__", "_")
        k = k.strip("_")

    # Common aliases
    aliases = {
        "MARKET_LEADERS": "MARKET_LEADERS",
        "MARKETLEADERS": "MARKET_LEADERS",
        "GLOBAL_MARKETS": "GLOBAL_MARKETS",
        "GLOBALMARKETS": "GLOBAL_MARKETS",
        "KSA_TADAWUL": "KSA_TADAWUL",
        "TADAWUL": "KSA_TADAWUL",
        "KSA": "KSA_TADAWUL",
        "MUTUAL_FUNDS": "MUTUAL_FUNDS",
        "MUTUALFUNDS": "MUTUAL_FUNDS",
        "COMMODITIES_FX": "COMMODITIES_FX",
        "COMMODITIESFX": "COMMODITIES_FX",
        "MY_PORTFOLIO": "MY_PORTFOLIO",
        "PORTFOLIO": "MY_PORTFOLIO",
        "INSIGHTS_ANALYSIS": "INSIGHTS_ANALYSIS",
        "INSIGHTS": "INSIGHTS_ANALYSIS",
    }
    return aliases.get(k.replace("_", ""), aliases.get(k, k))


def resolve_page_spec(page_key: str) -> Dict[str, str]:
    k = _norm_page_key(page_key)
    return PAGE_SPECS.get(k, {"market": "mixed", "mode": "refresh_quotes"})


# =============================================================================
# Symbol normalization (KSA-focused + aligned fallback)
# =============================================================================
def _normalize_ksa_symbol(symbol: str) -> str:
    """
    KSA Normalizer:
    - Accepts: '1120', '1120.SR', 'TADAWUL:1120', 'SA:1120', '1120.TADAWUL'
    - Returns: '1120.SR' or "" if not KSA-shaped.
    """
    s = str(symbol or "").strip().upper()
    if not s:
        return ""

    for p in ("TADAWUL:", "TDWL:", "SA:", "KSA:", "TASI:"):
        if s.startswith(p):
            s = s[len(p) :].strip()

    if s.endswith(".TADAWUL"):
        s = s[: -len(".TADAWUL")].strip()

    # If it's numeric code (3-6 digits) => KSA
    if s.isdigit() and 3 <= len(s) <= 6:
        return f"{s}.SR"

    # If already .SR and numeric core
    if s.endswith(".SR"):
        core = s[:-3].strip()
        if core.isdigit() and 3 <= len(core) <= 6:
            return f"{core}.SR"

    return ""


def _fallback_normalize(symbol: str) -> str:
    return str(symbol or "").strip().upper()


# Try to use core normalizer if available
try:
    from core.symbols.normalize import normalize_symbol as _CORE_NORM  # type: ignore

    def _NORMALIZE_SYMBOL(x: str) -> str:
        try:
            return _CORE_NORM(x) or _fallback_normalize(x)
        except Exception:
            return _fallback_normalize(x)

except Exception:

    def _NORMALIZE_SYMBOL(x: str) -> str:
        return _fallback_normalize(x)


def split_tickers_by_market(tickers: Sequence[str]) -> Tuple[List[str], List[str]]:
    """
    Returns: (ksa_list, global_list) de-duplicated preserving order.
    KSA tickers are normalized to ####.SR.
    """
    seen_ksa, seen_glb = set(), set()
    ksa: List[str] = []
    glb: List[str] = []

    for t in tickers or []:
        raw = str(t or "").strip()
        if not raw:
            continue

        ksa_norm = _normalize_ksa_symbol(raw)
        if ksa_norm:
            if ksa_norm not in seen_ksa:
                seen_ksa.add(ksa_norm)
                ksa.append(ksa_norm)
            continue

        norm = _NORMALIZE_SYMBOL(raw)
        if norm and norm not in seen_glb:
            seen_glb.add(norm)
            glb.append(norm)

    return ksa, glb


# =============================================================================
# Payload hygiene
# =============================================================================
def _prune_none(obj: Any) -> Any:
    """
    Recursively removes None values from dicts/lists.
    Does NOT remove empty lists/dicts/strings (keeps intentional empties).
    """
    if obj is None:
        return None
    if isinstance(obj, dict):
        out: Dict[str, Any] = {}
        for k, v in obj.items():
            vv = _prune_none(v)
            if vv is None:
                continue
            out[k] = vv
        return out
    if isinstance(obj, list):
        out_list = []
        for x in obj:
            xx = _prune_none(x)
            if xx is None:
                continue
            out_list.append(xx)
        return out_list
    return obj


# =============================================================================
# Result structure
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
    used_backup: bool = False

    def as_dict(self) -> Dict[str, Any]:
        return {k: v for k, v in self.__dict__.items()}


# =============================================================================
# Client
# =============================================================================
class GoogleAppsScriptClient:
    def __init__(self, base_url: Optional[str] = None, token: Optional[str] = None):
        self.primary_url = (base_url or _PRIMARY_URL or "").rstrip("/")
        self.backup_url = (_BACKUP_URL or "").rstrip("/")
        self.base_url = self.primary_url or self.backup_url
        self.token = token or _DEFAULT_TOKEN
        self.timeout = float(_TIMEOUT)

        # Circuit breaker state
        self._fail_count = 0
        self._cb_open_until = 0.0
        self._auth_locked_until = 0.0

        # SSL Context
        self._ssl_ctx = ssl.create_default_context()
        if not _VERIFY_SSL:
            self._ssl_ctx.check_hostname = False
            self._ssl_ctx.verify_mode = ssl.CERT_NONE

        if not self.base_url:
            logger.warning("⚠️ No Apps Script URL configured (GOOGLE_APPS_SCRIPT_URL/GOOGLE_APPS_SCRIPT_BACKUP_URL).")

        # Environment tuning (optional)
        if _APP_ENV in {"dev", "local", "test"}:
            # Faster feedback in dev; retries still controlled by env
            self.timeout = max(10.0, min(self.timeout, 35.0))

    # ----------------------------
    # URL builder (safe even if base_url already has query)
    # ----------------------------
    def _build_url(self, base_url: str, mode: str, query: Optional[Dict[str, str]] = None) -> str:
        base_url = (base_url or "").strip()
        if not base_url:
            return ""

        parsed = urllib.parse.urlparse(base_url)
        existing = dict(urllib.parse.parse_qsl(parsed.query, keep_blank_values=True))

        # Always include mode
        existing["mode"] = str(mode or "").strip()

        # Token via query transport
        if self.token and "query" in _TOKEN_TRANSPORT:
            existing[_TOKEN_PARAM] = self.token

        if query:
            for k, v in query.items():
                if v is None:
                    continue
                existing[str(k)] = str(v)

        new_query = urllib.parse.urlencode(existing)
        rebuilt = parsed._replace(query=new_query)
        return urllib.parse.urlunparse(rebuilt)

    def _now(self) -> float:
        return time.time()

    def _cb_is_open(self) -> bool:
        return self._now() < self._cb_open_until

    def _auth_is_locked(self) -> bool:
        return self._now() < self._auth_locked_until

    def _open_cb(self, seconds: int) -> None:
        self._cb_open_until = max(self._cb_open_until, self._now() + float(max(1, seconds)))

    def _lock_auth(self, seconds: int) -> None:
        self._auth_locked_until = max(self._auth_locked_until, self._now() + float(max(10, seconds)))

    def _sleep_backoff(self, attempt: int, *, retry_after: Optional[float] = None) -> None:
        # Honor server retry-after when present
        if retry_after is not None and retry_after > 0:
            time.sleep(min(30.0, float(retry_after)))
            return

        # Jittered exponential backoff
        base = 1.2 ** max(0, attempt)
        jitter = random.uniform(0.05, 0.85)
        sleep_s = min(12.0, base + jitter)
        time.sleep(sleep_s)

    def _choose_urls(self) -> List[Tuple[str, bool]]:
        """
        Returns [(url, used_backup), ...] in preferred order.
        """
        urls: List[Tuple[str, bool]] = []
        if self.primary_url:
            urls.append((self.primary_url, False))
        if self.backup_url and self.backup_url != self.primary_url:
            urls.append((self.backup_url, True))
        # If only base_url is set (legacy)
        if not urls and self.base_url:
            urls.append((self.base_url, self.base_url == self.backup_url and bool(self.backup_url)))
        return urls

    # ----------------------------
    # Low-level call
    # ----------------------------
    def call_script(
        self,
        mode: str,
        payload: Optional[Dict[str, Any]] = None,
        *,
        method: Optional[str] = None,
        retries: Optional[int] = None,
        query: Optional[Dict[str, str]] = None,
        request_id: Optional[str] = None,
    ) -> AppsScriptResult:
        if not self.base_url and not self.primary_url and not self.backup_url:
            return AppsScriptResult(False, 0, None, "No URL Configured")

        if self._auth_is_locked():
            return AppsScriptResult(False, 0, None, "Auth locked (circuit breaker)")

        if self._cb_is_open():
            return AppsScriptResult(False, 0, None, "Circuit breaker open (temporary)")

        use_method = (method or _DEFAULT_HTTP_METHOD or "POST").upper().strip()
        max_retries = int(retries if retries is not None else _MAX_RETRIES)

        # Payload prune (None only)
        clean_payload = _prune_none(payload or {})

        # Auth injection (header/body)
        headers = {
            "Content-Type": "application/json",
            "User-Agent": f"TFB-AppsScriptClient/{CLIENT_VERSION}",
        }
        if request_id:
            headers["X-REQUEST-ID"] = str(request_id)

        if self.token and "header" in _TOKEN_TRANSPORT:
            headers["X-APP-TOKEN"] = self.token

        if self.token and "body" in _TOKEN_TRANSPORT:
            # keep this compatible with your GAS style
            auth = clean_payload.get("auth")
            if not isinstance(auth, dict):
                auth = {}
            auth.setdefault("token", self.token)
            clean_payload["auth"] = auth

        body_bytes: Optional[bytes] = None
        if use_method != "GET":
            try:
                body_bytes = json.dumps(clean_payload, ensure_ascii=False).encode("utf-8")
            except Exception as ex:
                return AppsScriptResult(False, 0, None, f"JSON encode error: {ex}")

        started = time.time()
        last_err: Optional[str] = None
        last_raw: Optional[str] = None
        last_code: int = 0
        last_url: str = ""

        urls_to_try = self._choose_urls()
        if not urls_to_try:
            return AppsScriptResult(False, 0, None, "No URL Configured")

        for base_url, used_backup in urls_to_try:
            url = self._build_url(base_url, mode, query=query)
            if not url:
                continue

            # retry loop per URL
            for attempt in range(max_retries + 1):
                try:
                    req = urllib.request.Request(url, data=body_bytes, headers=headers, method=use_method)
                    with urllib.request.urlopen(req, timeout=self.timeout, context=self._ssl_ctx) as resp:
                        raw = resp.read().decode("utf-8", errors="replace")
                        code = int(getattr(resp, "status", 200) or 200)

                        # Try JSON decode
                        try:
                            data = json.loads(raw) if raw else {}
                            ok = True
                            self._fail_count = 0
                            self._cb_open_until = 0.0
                            return AppsScriptResult(
                                ok=ok,
                                status_code=code,
                                data=data,
                                error=None,
                                raw_text=(raw[:_MAX_LOG_CHARS] if raw else None),
                                url=url,
                                elapsed_ms=int((time.time() - started) * 1000),
                                used_backup=used_backup,
                            )
                        except Exception:
                            # Non-JSON response (still a failure for our contract)
                            last_err = "Invalid JSON Response"
                            last_raw = raw[:_MAX_LOG_CHARS] if raw else None
                            last_code = code
                            last_url = url

                            # retry on invalid JSON (transient), unless last attempt
                            if attempt < max_retries:
                                self._sleep_backoff(attempt)
                                continue

                            self._fail_count += 1
                            if self._fail_count >= _CB_FAIL_THRESHOLD:
                                self._open_cb(_CB_OPEN_SEC)

                            return AppsScriptResult(
                                ok=False,
                                status_code=code,
                                data=None,
                                error=last_err,
                                raw_text=last_raw,
                                url=url,
                                elapsed_ms=int((time.time() - started) * 1000),
                                used_backup=used_backup,
                            )

                except urllib.error.HTTPError as e:
                    code = int(getattr(e, "code", 0) or 0)
                    last_code = code
                    last_url = url

                    # Read body if possible
                    try:
                        rawb = e.read()
                        raw = rawb.decode("utf-8", errors="replace") if rawb else ""
                    except Exception:
                        raw = ""
                    last_raw = raw[:_MAX_LOG_CHARS] if raw else None

                    # Auth circuit-breaker (do not retry)
                    if code in (401, 403):
                        self._lock_auth(_AUTH_LOCK_SEC)
                        self._fail_count += 1
                        return AppsScriptResult(
                            ok=False,
                            status_code=code,
                            data=None,
                            error=f"Auth Error: {getattr(e, 'reason', '')}".strip() or "Auth Error",
                            raw_text=last_raw,
                            url=url,
                            elapsed_ms=int((time.time() - started) * 1000),
                            used_backup=used_backup,
                        )

                    # Retryable codes
                    retry_after = None
                    try:
                        ra = e.headers.get("Retry-After") if hasattr(e, "headers") else None
                        if ra:
                            retry_after = float(ra)
                    except Exception:
                        retry_after = None

                    if code in (408, 409, 425, 429, 500, 502, 503, 504):
                        if attempt < max_retries:
                            logger.warning(f"[AppsScript] HTTP {code} retry {attempt+1}/{max_retries} (backup={used_backup})")
                            self._sleep_backoff(attempt, retry_after=retry_after)
                            continue

                    # Non-retry or exhausted
                    self._fail_count += 1
                    if self._fail_count >= _CB_FAIL_THRESHOLD:
                        self._open_cb(_CB_OPEN_SEC)

                    return AppsScriptResult(
                        ok=False,
                        status_code=code,
                        data=None,
                        error=f"HTTP Error: {getattr(e, 'reason', '')}".strip() or f"HTTP {code}",
                        raw_text=last_raw,
                        url=url,
                        elapsed_ms=int((time.time() - started) * 1000),
                        used_backup=used_backup,
                    )

                except Exception as ex:
                    last_err = f"Network Error: {ex.__class__.__name__}: {ex}"
                    last_code = 0
                    last_url = url

                    if attempt < max_retries:
                        self._sleep_backoff(attempt)
                        continue

                    self._fail_count += 1
                    if self._fail_count >= _CB_FAIL_THRESHOLD:
                        self._open_cb(_CB_OPEN_SEC)

                    return AppsScriptResult(
                        ok=False,
                        status_code=0,
                        data=None,
                        error=last_err,
                        raw_text=None,
                        url=url,
                        elapsed_ms=int((time.time() - started) * 1000),
                        used_backup=used_backup,
                    )

            # If we exit retry loop for this URL, try next URL (backup failover)
            # (We only reach here if url loop continues without returning.)
            # Keep last_* values for final fallback.
            continue

        # Final fallback if no URL succeeded
        return AppsScriptResult(
            ok=False,
            status_code=last_code,
            data=None,
            error=last_err or "Max attempts exceeded (all URLs)",
            raw_text=last_raw,
            url=last_url or None,
            elapsed_ms=int((time.time() - started) * 1000),
            used_backup=bool(self.backup_url and (self.base_url == self.backup_url)),
        )

    # ----------------------------
    # High-level methods (aligned payload contract)
    # ----------------------------
    def sync_page_quotes(
        self,
        page_key: str,
        *,
        sheet_id: str,
        sheet_name: str,
        tickers: Sequence[str],
        request_id: Optional[str] = None,
        extra_meta: Optional[Dict[str, Any]] = None,
        method: Optional[str] = None,
        retries: Optional[int] = None,
    ) -> AppsScriptResult:
        """
        Builds your standard payload and routes tickers by page spec market.
        """
        spec = resolve_page_spec(page_key)
        ksa, glb = split_tickers_by_market(list(tickers or []))

        payload: Dict[str, Any] = {
            "sheet": {"id": sheet_id, "name": sheet_name},
            "meta": {
                "client": "tfb_backend",
                "version": CLIENT_VERSION,
                "timestamp_utc": _utc_iso(),
                "page_key": _norm_page_key(page_key),
            },
            "backend": {"base_url": _BACKEND_URL} if _BACKEND_URL else {},
        }

        if extra_meta and isinstance(extra_meta, dict):
            payload["meta"].update(extra_meta)

        # Route by market
        market = spec.get("market", "mixed")
        if market == "ksa":
            payload["ksa_tickers"] = ksa
            payload["global_tickers"] = []
            payload["all_tickers"] = ksa
        elif market == "global":
            payload["ksa_tickers"] = []
            payload["global_tickers"] = glb
            payload["all_tickers"] = glb
        else:
            payload["ksa_tickers"] = ksa
            payload["global_tickers"] = glb
            payload["all_tickers"] = ksa + glb

        logger.info(f"[AppsScript] sync_page_quotes page={payload['meta'].get('page_key')} mode={spec.get('mode')} count={len(payload.get('all_tickers') or [])}")
        return self.call_script(
            mode=str(spec.get("mode") or "refresh_quotes"),
            payload=payload,
            method=method,
            retries=retries,
            request_id=request_id,
        )

    def ping(
        self,
        *,
        request_id: Optional[str] = None,
        method: Optional[str] = None,
        retries: Optional[int] = None,
    ) -> AppsScriptResult:
        """
        Optional health ping (depends on your GAS implementation supporting mode=health or mode=ping).
        """
        return self.call_script(
            mode="health",
            payload={"meta": {"client": "tfb_backend", "version": CLIENT_VERSION, "timestamp_utc": _utc_iso()}},
            method=method or "GET",
            retries=retries,
            request_id=request_id,
        )


# Singleton (kept for backward compatibility)
apps_script_client = GoogleAppsScriptClient()
get_apps_script_client = lambda: apps_script_client  # noqa: E731

__all__ = [
    "GoogleAppsScriptClient",
    "AppsScriptResult",
    "apps_script_client",
    "get_apps_script_client",
    "PAGE_SPECS",
    "resolve_page_spec",
    "split_tickers_by_market",
]
