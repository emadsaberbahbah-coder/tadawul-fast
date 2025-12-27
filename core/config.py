# core/config.py  (FULL REPLACEMENT)
"""
core/config.py
------------------------------------------------------------
Compatibility shim (PROD SAFE) – v1.6.0 (ENV-ALIAS HARDENED)

Many modules import settings via:
  from core.config import Settings, get_settings

Canonical source of truth is the repo-root `config.py`.
This module re-exports Settings/get_settings so older imports keep working.

What this version fixes for your Render env names
- ✅ Maps FINNHUB_API_KEY  -> finnhub_api_token (and finnhub_api_key alias)
- ✅ Maps EODHD_API_KEY    -> eodhd_api_token   (and eodhd_api_key alias)
- ✅ Reads ENABLED_PROVIDERS / PRIMARY_PROVIDER (as in your env list)
- ✅ Never rejects "0" values for ints/floats (no misleading defaults)

Design goals
- Never crash app boot if root config.py is missing or mid-refactor.
- Safe fallback Settings with common knobs used by routers.
- Supports BOTH naming styles:
    - lower_case: app_name, env, version, log_level, ...
    - UPPER_CASE: APP_NAME, ENV, VERSION, LOG_LEVEL, ...
"""

from __future__ import annotations

import json
import os
from functools import lru_cache
from typing import Any, Dict, List, Optional, Tuple

# =============================================================================
# Try importing root config.py (preferred)
# =============================================================================
_root_Settings = None
_root_get_settings = None

try:
    import config as _root_config  # type: ignore

    _root_Settings = getattr(_root_config, "Settings", None)
    _root_get_settings = getattr(_root_config, "get_settings", None)
except Exception:
    _root_Settings = None
    _root_get_settings = None


# =============================================================================
# Helpers (fallback-safe, no heavy imports)
# =============================================================================
_TRUTHY = {"1", "true", "yes", "y", "on", "t"}
_FALSY = {"0", "false", "no", "n", "off", "f"}


def _env_str(name: str, default: Optional[str] = None) -> Optional[str]:
    v = os.getenv(name)
    if v is None:
        return default
    s = str(v).strip()
    return s if s else default


def _env_first(names: List[str], default: Optional[str] = None) -> Optional[str]:
    for n in names:
        v = _env_str(n, None)
        if v is not None:
            return v
    return default


def _env_bool(name: str, default: bool = False) -> bool:
    v = _env_str(name, None)
    if v is None:
        return default
    s = v.strip().lower()
    if s in _TRUTHY:
        return True
    if s in _FALSY:
        return False
    return default


def _env_bool_first(names: List[str], default: bool = False) -> bool:
    for n in names:
        v = _env_str(n, None)
        if v is None:
            continue
        return _env_bool(n, default)
    return default


def _env_int_first(names: List[str], default: int) -> int:
    for n in names:
        v = _env_str(n, None)
        if v is None:
            continue
        try:
            return int(str(v).strip())
        except Exception:
            continue
    return default


def _env_float_first(names: List[str], default: float) -> float:
    for n in names:
        v = _env_str(n, None)
        if v is None:
            continue
        try:
            return float(str(v).strip())
        except Exception:
            continue
    return default


def _parse_list(value: Any) -> List[str]:
    """
    Accept:
      - python list
      - CSV string
      - JSON list string: '["eodhd","finnhub"]'
    Return: cleaned lower-case list (deduped, order preserved)
    """
    if value is None:
        return []
    items: List[str] = []

    if isinstance(value, list):
        items = [str(x).strip() for x in value if str(x).strip()]
    else:
        s = str(value).strip()
        if not s:
            return []
        if s.startswith("[") and s.endswith("]"):
            try:
                arr = json.loads(s)
                if isinstance(arr, list):
                    items = [str(x).strip() for x in arr if str(x).strip()]
                else:
                    items = [p.strip() for p in s.split(",") if p.strip()]
            except Exception:
                items = [p.strip() for p in s.split(",") if p.strip()]
        else:
            items = [p.strip() for p in s.split(",") if p.strip()]

    out: List[str] = []
    seen = set()
    for x in items:
        yl = x.strip().lower()
        if not yl or yl in seen:
            continue
        seen.add(yl)
        out.append(yl)
    return out


def _mask_tail(s: Optional[str], keep: int = 4) -> str:
    s2 = (s or "").strip()
    if not s2:
        return ""
    if len(s2) <= keep:
        return "•" * len(s2)
    return ("•" * (len(s2) - keep)) + s2[-keep:]


def _resolve_google_credentials(
    google_sheets_credentials: Optional[str],
    google_credentials: Optional[str],
) -> Tuple[Optional[str], str]:
    if google_sheets_credentials and google_sheets_credentials.strip():
        return google_sheets_credentials.strip(), "GOOGLE_SHEETS_CREDENTIALS"
    if google_credentials and google_credentials.strip():
        return google_credentials.strip(), "GOOGLE_CREDENTIALS"
    return None, "none"


def _resolve_spreadsheet_id(default_spreadsheet_id: Optional[str]) -> Tuple[Optional[str], str]:
    if default_spreadsheet_id and default_spreadsheet_id.strip():
        return default_spreadsheet_id.strip(), "DEFAULT_SPREADSHEET_ID"
    return None, "none"


def _apply_compat_aliases(s: Any) -> Any:
    """
    Ensure common legacy attribute names exist without requiring env renames.
    We only set attributes that are missing or empty.
    """
    # --- tokens (Render has *_API_KEY; some code expects *_API_TOKEN) ---
    finnhub = getattr(s, "finnhub_api_token", None) or getattr(s, "finnhub_api_key", None)
    eodhd = getattr(s, "eodhd_api_token", None) or getattr(s, "eodhd_api_key", None)

    finnhub = finnhub or _env_first(["FINNHUB_API_KEY", "FINNHUB_API_TOKEN", "FINNHUB_TOKEN"], None)
    eodhd = eodhd or _env_first(["EODHD_API_KEY", "EODHD_API_TOKEN", "EODHD_TOKEN"], None)

    for name, val in [
        ("finnhub_api_token", finnhub),
        ("finnhub_api_key", finnhub),
        ("FINNHUB_API_TOKEN", finnhub),
        ("FINNHUB_API_KEY", finnhub),
        ("eodhd_api_token", eodhd),
        ("eodhd_api_key", eodhd),
        ("EODHD_API_TOKEN", eodhd),
        ("EODHD_API_KEY", eodhd),
    ]:
        try:
            cur = getattr(s, name, None)
            if (cur is None or cur == "") and val:
                setattr(s, name, val)
        except Exception:
            pass

    # --- app meta common aliases ---
    try:
        if getattr(s, "app_name", None) in (None, ""):
            setattr(s, "app_name", getattr(s, "service_name", None) or _env_first(["SERVICE_NAME", "APP_NAME"], "Tadawul Stock Analysis API"))
    except Exception:
        pass

    try:
        if getattr(s, "env", None) in (None, ""):
            setattr(s, "env", getattr(s, "environment", None) or _env_first(["ENVIRONMENT", "APP_ENV", "ENV"], "production"))
    except Exception:
        pass

    try:
        if getattr(s, "version", None) in (None, ""):
            setattr(s, "version", getattr(s, "service_version", None) or _env_first(["SERVICE_VERSION", "APP_VERSION", "VERSION"], "0.0.0"))
    except Exception:
        pass

    # --- providers aliases ---
    try:
        if not getattr(s, "enabled_providers", None):
            raw = getattr(s, "enabled_providers_raw", None) or _env_first(["ENABLED_PROVIDERS", "PROVIDERS"], "eodhd,finnhub")
            setattr(s, "enabled_providers", _parse_list(raw))
    except Exception:
        pass

    try:
        if not getattr(s, "enabled_ksa_providers", None):
            raw = getattr(s, "ksa_providers_raw", None) or _env_first(["KSA_PROVIDERS"], "yahoo_chart,tadawul,argaam")
            setattr(s, "enabled_ksa_providers", _parse_list(raw))
    except Exception:
        pass

    # --- auth aliases ---
    try:
        if getattr(s, "app_token", None) in (None, ""):
            setattr(s, "app_token", _env_first(["APP_TOKEN", "TFB_APP_TOKEN", "X_APP_TOKEN"], None))
    except Exception:
        pass

    try:
        if getattr(s, "backup_app_token", None) in (None, ""):
            setattr(s, "backup_app_token", _env_first(["BACKUP_APP_TOKEN"], None))
    except Exception:
        pass

    try:
        if getattr(s, "require_auth", None) in (None, ""):
            setattr(s, "require_auth", _env_bool("REQUIRE_AUTH", False))
    except Exception:
        pass

    # --- sheet aliases ---
    try:
        if getattr(s, "default_spreadsheet_id", None) in (None, ""):
            setattr(s, "default_spreadsheet_id", _env_first(["DEFAULT_SPREADSHEET_ID", "SPREADSHEET_ID"], None))
    except Exception:
        pass

    try:
        if getattr(s, "spreadsheet_id", None) in (None, ""):
            setattr(s, "spreadsheet_id", getattr(s, "default_spreadsheet_id", None))
    except Exception:
        pass

    try:
        if getattr(s, "google_sheets_credentials", None) in (None, ""):
            setattr(s, "google_sheets_credentials", _env_first(["GOOGLE_SHEETS_CREDENTIALS", "GOOGLE_CREDENTIALS"], None))
    except Exception:
        pass

    # --- cache/timeouts common names ---
    try:
        if getattr(s, "cache_ttl_sec", None) in (None, ""):
            setattr(s, "cache_ttl_sec", _env_int_first(["CACHE_DEFAULT_TTL", "CACHE_TTL_SEC"], 10))
    except Exception:
        pass

    try:
        if getattr(s, "http_timeout_sec", None) in (None, ""):
            setattr(s, "http_timeout_sec", int(_env_float_first(["HTTP_TIMEOUT"], 30.0)))
    except Exception:
        pass

    return s


# =============================================================================
# Re-export logic (root config preferred)
# =============================================================================
Settings = None  # type: ignore
get_settings = None  # type: ignore

if _root_Settings is not None and callable(_root_get_settings):
    Settings = _root_Settings  # type: ignore

    @lru_cache(maxsize=1)
    def get_settings() -> Any:  # type: ignore
        s = _root_get_settings()  # type: ignore
        return _apply_compat_aliases(s)

elif _root_Settings is not None and not callable(_root_get_settings):
    Settings = _root_Settings  # type: ignore

    @lru_cache(maxsize=1)
    def get_settings() -> Any:  # type: ignore
        s = Settings()  # type: ignore
        return _apply_compat_aliases(s)

elif callable(_root_get_settings) and _root_Settings is None:
    @lru_cache(maxsize=1)
    def get_settings() -> Any:  # type: ignore
        s = _root_get_settings()  # type: ignore
        return _apply_compat_aliases(s)

    try:
        _tmp = get_settings()  # type: ignore
        Settings = _tmp.__class__ if _tmp is not None else None  # type: ignore
    except Exception:
        Settings = None  # type: ignore


# =============================================================================
# Fallback implementation (only if root config is unavailable)
# =============================================================================
if Settings is None or get_settings is None:

    class Settings:  # type: ignore
        """
        Minimal Settings fallback with env-alias alignment to your Render variables.
        """

        def __init__(self) -> None:
            # App meta
            self.app_name: str = _env_first(["SERVICE_NAME", "APP_NAME"], "Tadawul Stock Analysis API") or "Tadawul Stock Analysis API"
            self.env: str = _env_first(["ENVIRONMENT", "APP_ENV", "ENV"], "production") or "production"
            self.version: str = _env_first(["SERVICE_VERSION", "APP_VERSION", "VERSION"], "0.0.0") or "0.0.0"
            self.log_level: str = (_env_first(["LOG_LEVEL"], "info") or "info").lower()
            self.debug: bool = _env_bool("DEBUG", False)

            # Auth
            self.app_token: Optional[str] = _env_first(["APP_TOKEN", "TFB_APP_TOKEN", "X_APP_TOKEN"], None)
            self.backup_app_token: Optional[str] = _env_first(["BACKUP_APP_TOKEN"], None)
            self.require_auth: bool = _env_bool("REQUIRE_AUTH", False)

            # Providers
            self.enabled_providers: List[str] = _parse_list(_env_first(["ENABLED_PROVIDERS", "PROVIDERS"], "eodhd,finnhub"))
            self.enabled_ksa_providers: List[str] = _parse_list(_env_first(["KSA_PROVIDERS"], "yahoo_chart,tadawul,argaam"))
            self.primary_provider: str = (_env_first(["PRIMARY_PROVIDER"], "eodhd") or "eodhd").strip().lower()

            # Tokens (✅ align with Render names)
            self.eodhd_api_token: Optional[str] = _env_first(["EODHD_API_KEY", "EODHD_API_TOKEN", "EODHD_TOKEN"], None)
            self.finnhub_api_token: Optional[str] = _env_first(["FINNHUB_API_KEY", "FINNHUB_API_TOKEN", "FINNHUB_TOKEN"], None)

            # Legacy key names (some modules still read *_api_key)
            self.eodhd_api_key: Optional[str] = self.eodhd_api_token
            self.finnhub_api_key: Optional[str] = self.finnhub_api_token

            # HTTP / retry
            self.http_timeout_sec: int = int(_env_float_first(["HTTP_TIMEOUT"], 30.0))
            self.max_retries: int = _env_int_first(["MAX_RETRIES"], 2)
            self.retry_delay_sec: float = _env_float_first(["RETRY_DELAY"], 0.5)

            # Cache
            self.cache_ttl_sec: int = _env_int_first(["CACHE_DEFAULT_TTL", "CACHE_TTL_SEC"], 10)
            self.cache_max_size: int = _env_int_first(["CACHE_MAX_SIZE"], 5000)

            # Routers knobs (match your advanced routes)
            self.ai_batch_size: int = _env_int_first(["AI_BATCH_SIZE"], 20)
            self.ai_batch_timeout_sec: float = _env_float_first(["AI_BATCH_TIMEOUT_SEC"], 45.0)
            self.ai_batch_concurrency: int = _env_int_first(["AI_BATCH_CONCURRENCY"], 5)
            self.ai_max_tickers: int = _env_int_first(["AI_MAX_TICKERS"], 500)

            self.adv_batch_size: int = _env_int_first(["ADV_BATCH_SIZE"], 25)
            self.adv_batch_timeout_sec: float = _env_float_first(["ADV_BATCH_TIMEOUT_SEC"], 45.0)
            self.adv_batch_concurrency: int = _env_int_first(["ADV_BATCH_CONCURRENCY"], 6)
            self.adv_max_tickers: int = _env_int_first(["ADV_MAX_TICKERS"], 500)

            # Sheets / creds
            self.default_spreadsheet_id: Optional[str] = _env_first(["DEFAULT_SPREADSHEET_ID", "SPREADSHEET_ID"], None)
            self.spreadsheet_id: Optional[str] = self.default_spreadsheet_id
            self.google_sheets_credentials: Optional[str] = _env_first(["GOOGLE_SHEETS_CREDENTIALS", "GOOGLE_CREDENTIALS"], None)
            self.google_credentials: Optional[str] = _env_first(["GOOGLE_CREDENTIALS"], None)

            # KSA URLs (optional)
            self.tadawul_quote_url: Optional[str] = _env_str("TADAWUL_QUOTE_URL", None)
            self.tadawul_fundamentals_url: Optional[str] = _env_str("TADAWUL_FUNDAMENTALS_URL", None)
            self.argaam_quote_url: Optional[str] = _env_str("ARGAAM_QUOTE_URL", None)
            self.argaam_profile_url: Optional[str] = _env_str("ARGAAM_PROFILE_URL", None)

        # Safe diagnostics (no secrets)
        def safe_summary(self) -> Dict[str, Any]:
            creds, creds_src = _resolve_google_credentials(self.google_sheets_credentials, self.google_credentials)
            sid, sid_src = _resolve_spreadsheet_id(self.default_spreadsheet_id)
            return {
                "APP_NAME": self.app_name,
                "ENV": self.env,
                "VERSION": self.version,
                "LOG_LEVEL": self.log_level,
                "REQUIRE_AUTH": bool(self.require_auth),
                "APP_TOKEN_SET": bool(self.app_token),
                "APP_TOKEN_MASK": _mask_tail(self.app_token, 4),
                "PROVIDERS": list(self.enabled_providers),
                "KSA_PROVIDERS": list(self.enabled_ksa_providers),
                "PRIMARY_PROVIDER": self.primary_provider,
                "EODHD_TOKEN_SET": bool(self.eodhd_api_token),
                "FINNHUB_TOKEN_SET": bool(self.finnhub_api_token),
                "SPREADSHEET_ID_SET": bool(sid),
                "SPREADSHEET_ID_SOURCE": sid_src,
                "GOOGLE_CREDS_SET": bool(creds),
                "GOOGLE_CREDS_SOURCE": creds_src,
            }

        # Uppercase aliases (legacy compatibility)
        def __getattr__(self, name: str) -> Any:
            if name.isupper():
                low = name.lower()
                if hasattr(self, low):
                    return getattr(self, low)
            raise AttributeError(name)

        @property
        def APP_NAME(self) -> str:  # noqa: N802
            return self.app_name

        @property
        def ENV(self) -> str:  # noqa: N802
            return self.env

        @property
        def VERSION(self) -> str:  # noqa: N802
            return self.version

        @property
        def LOG_LEVEL(self) -> str:  # noqa: N802
            return self.log_level

        @property
        def EODHD_API_KEY(self) -> Optional[str]:  # noqa: N802
            return self.eodhd_api_key

        @property
        def FINNHUB_API_KEY(self) -> Optional[str]:  # noqa: N802
            return self.finnhub_api_key

    @lru_cache(maxsize=1)
    def get_settings() -> Settings:  # type: ignore
        return _apply_compat_aliases(Settings())


__all__ = ["Settings", "get_settings"]
