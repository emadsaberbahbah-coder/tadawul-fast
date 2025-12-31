# core/config.py  — FULL REPLACEMENT — v1.8.0
"""
core/config.py
------------------------------------------------------------
Compatibility shim (PROD SAFE) – v1.8.0 (RENDER-ENV LOCKED)

Many modules import settings via:
  from core.config import Settings, get_settings

Canonical source of truth is repo-root `config.py`.
This module re-exports Settings/get_settings so older imports keep working.

✅ HARD alignment with your Render env keys (you will NOT rename env vars):
- FINNHUB_API_KEY
- EODHD_API_KEY
- ENABLED_PROVIDERS
- PRIMARY_PROVIDER
- HTTP_TIMEOUT
- MAX_RETRIES
- RETRY_DELAY
- CACHE_DEFAULT_TTL / CACHE_MAX_SIZE / CACHE_BACKUP_ENABLED / CACHE_SAVE_INTERVAL
- CORS_ORIGINS / ENABLE_RATE_LIMITING / MAX_REQUESTS_PER_MINUTE
- DEFAULT_SPREADSHEET_ID / GOOGLE_* / GOOGLE_APPS_SCRIPT_*
- SERVICE_NAME / SERVICE_VERSION / ENVIRONMENT / TZ / DEBUG / LOG_LEVEL / LOG_FORMAT
- ADVANCED_ANALYSIS_ENABLED / TADAWUL_MARKET_ENABLED / ENABLE_SWAGGER / ENABLE_REDOC

Key guarantee:
- If providers/modules expect FINNHUB_API_TOKEN / EODHD_API_TOKEN (token-style),
  we export env aliases at runtime (inside get_settings), without requiring env renames.

No network at import-time. No heavy imports required.
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
    return s if s != "" else default


def _env_first(names: List[str], default: Optional[str] = None) -> Optional[str]:
    for n in names:
        v = _env_str(n, None)
        if v is not None:
            return v
    return default


def _to_bool(v: Any, default: bool = False) -> bool:
    if v is None:
        return default
    if isinstance(v, bool):
        return v
    s = str(v).strip().lower()
    if s in _TRUTHY:
        return True
    if s in _FALSY:
        return False
    return default


def _to_int(v: Any, default: int) -> int:
    try:
        if v is None:
            return default
        s = str(v).strip()
        if s == "":
            return default
        return int(s)
    except Exception:
        return default


def _to_float(v: Any, default: float) -> float:
    try:
        if v is None:
            return default
        s = str(v).strip()
        if s == "":
            return default
        return float(s)
    except Exception:
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
        y = x.strip().lower()
        if not y or y in seen:
            continue
        seen.add(y)
        out.append(y)
    return out


def _mask_tail(s: Optional[str], keep: int = 4) -> str:
    s2 = (s or "").strip()
    if not s2:
        return ""
    if len(s2) <= keep:
        return "•" * len(s2)
    return ("•" * (len(s2) - keep)) + s2[-keep:]


def _export_env_if_missing(key: str, value: Optional[str]) -> None:
    """
    Export env alias only if target is missing/blank.
    """
    if value is None:
        return
    v = str(value).strip()
    if not v:
        return
    cur = os.getenv(key)
    if cur is None or str(cur).strip() == "":
        os.environ[key] = v


def _to_mapping(obj: Any) -> Dict[str, Any]:
    """
    Best-effort conversion for root settings object to a dict.
    """
    if obj is None:
        return {}
    try:
        md = getattr(obj, "model_dump", None)
        if callable(md):
            return md()  # type: ignore
    except Exception:
        pass
    try:
        dct = getattr(obj, "dict", None)
        if callable(dct):
            return dct()  # type: ignore
    except Exception:
        pass
    try:
        return dict(getattr(obj, "__dict__", {}) or {})
    except Exception:
        return {}


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


def _apply_runtime_env_aliases_from_render(s: Any) -> None:
    """
    Your Render env uses *_API_KEY, but some provider modules read *_API_TOKEN.
    Export aliases safely (no overwrite if already set).
    """
    finnhub = (
        getattr(s, "finnhub_api_token", None)
        or getattr(s, "finnhub_api_key", None)
        or _env_first(["FINNHUB_API_KEY", "FINNHUB_API_TOKEN", "FINNHUB_TOKEN"], None)
    )
    eodhd = (
        getattr(s, "eodhd_api_token", None)
        or getattr(s, "eodhd_api_key", None)
        or _env_first(["EODHD_API_KEY", "EODHD_API_TOKEN", "EODHD_TOKEN"], None)
    )

    _export_env_if_missing("FINNHUB_API_TOKEN", finnhub)
    _export_env_if_missing("FINNHUB_TOKEN", finnhub)
    _export_env_if_missing("EODHD_API_TOKEN", eodhd)
    _export_env_if_missing("EODHD_TOKEN", eodhd)

    # Common timeout aliases used by mixed legacy code
    http_timeout = getattr(s, "http_timeout", None) or _env_first(["HTTP_TIMEOUT"], None)
    retry_delay = getattr(s, "retry_delay", None) or _env_first(["RETRY_DELAY"], None)
    if http_timeout is not None:
        _export_env_if_missing("HTTP_TIMEOUT_SEC", str(_to_float(http_timeout, 30.0)))
    if retry_delay is not None:
        _export_env_if_missing("RETRY_DELAY_SEC", str(_to_float(retry_delay, 0.5)))


# =============================================================================
# Preferred path: Root config exists -> expose a compat Settings class + wrapper get_settings
# =============================================================================
Settings = None  # type: ignore
get_settings = None  # type: ignore

if _root_Settings is not None:

    class Settings(_root_Settings):  # type: ignore
        """
        Compat Settings wrapper around repo-root Settings.
        Adds legacy properties used across older modules.
        """

        # --- Common legacy names used across older modules ---
        @property
        def app_name(self) -> str:
            return (
                getattr(self, "service_name", None)
                or _env_first(["SERVICE_NAME", "APP_NAME"], "Tadawul Stock Analysis API")
                or "Tadawul Stock Analysis API"
            )

        @property
        def env(self) -> str:
            return (
                getattr(self, "environment", None)
                or _env_first(["ENVIRONMENT", "APP_ENV", "ENV"], "production")
                or "production"
            )

        @property
        def version(self) -> str:
            return (
                getattr(self, "service_version", None)
                or _env_first(["SERVICE_VERSION", "APP_VERSION", "VERSION"], "0.0.0")
                or "0.0.0"
            )

        @property
        def enabled_providers(self) -> List[str]:
            # Prefer root's computed property if present
            try:
                v = getattr(super(), "enabled_providers")  # type: ignore[misc]
                if isinstance(v, list) and v:
                    return [str(x).strip().lower() for x in v if str(x).strip()]
            except Exception:
                pass

            raw = getattr(self, "enabled_providers_raw", None) or _env_first(["ENABLED_PROVIDERS"], "eodhd,finnhub")
            xs = _parse_list(raw)
            return xs or ["eodhd", "finnhub"]

        @property
        def ksa_providers(self) -> List[str]:
            try:
                v = getattr(super(), "ksa_providers")  # type: ignore[misc]
                if isinstance(v, list) and v:
                    return [str(x).strip().lower() for x in v if str(x).strip()]
            except Exception:
                pass

            raw = getattr(self, "ksa_providers_raw", None) or _env_first(["KSA_PROVIDERS"], "yahoo_chart,tadawul,argaam")
            xs = _parse_list(raw)
            return xs or ["yahoo_chart", "tadawul", "argaam"]

        # --- token-style properties (some code expects *_api_token) ---
        @property
        def eodhd_api_token(self) -> Optional[str]:
            v = getattr(self, "eodhd_api_key", None) or getattr(super(), "eodhd_api_token", None)  # type: ignore[attr-defined]
            v = v or _env_first(["EODHD_API_KEY", "EODHD_API_TOKEN", "EODHD_TOKEN"], None)
            return (str(v).strip() if v else None)

        @property
        def finnhub_api_token(self) -> Optional[str]:
            v = getattr(self, "finnhub_api_key", None) or getattr(super(), "finnhub_api_token", None)  # type: ignore[attr-defined]
            v = v or _env_first(["FINNHUB_API_KEY", "FINNHUB_API_TOKEN", "FINNHUB_TOKEN"], None)
            return (str(v).strip() if v else None)

        # --- safe summary (no secrets) ---
        def safe_summary(self) -> Dict[str, Any]:
            google_sheets_credentials = getattr(self, "google_sheets_credentials", None)
            google_credentials = getattr(self, "google_credentials", None)
            default_spreadsheet_id = getattr(self, "default_spreadsheet_id", None)

            creds, creds_src = _resolve_google_credentials(google_sheets_credentials, google_credentials)
            sid, sid_src = _resolve_spreadsheet_id(default_spreadsheet_id)

            return {
                "APP_NAME": self.app_name,
                "ENV": self.env,
                "VERSION": self.version,
                "LOG_LEVEL": (getattr(self, "log_level", None) or "info").strip().lower(),
                "REQUIRE_AUTH": bool(getattr(self, "require_auth", False)),
                "APP_TOKEN_SET": bool((getattr(self, "app_token", None) or "").strip()),
                "APP_TOKEN_MASK": _mask_tail(getattr(self, "app_token", None), 4),
                "ENABLED_PROVIDERS": list(self.enabled_providers),
                "PRIMARY_PROVIDER": (getattr(self, "primary_provider", None) or "eodhd").strip().lower(),
                "KSA_PROVIDERS": list(self.ksa_providers),
                "EODHD_TOKEN_SET": bool((self.eodhd_api_token or "").strip()),
                "FINNHUB_TOKEN_SET": bool((self.finnhub_api_token or "").strip()),
                "SPREADSHEET_ID_SET": bool(sid),
                "SPREADSHEET_ID_SOURCE": sid_src,
                "GOOGLE_CREDS_SET": bool(creds),
                "GOOGLE_CREDS_SOURCE": creds_src,
            }

        # --- uppercase aliases (legacy) ---
        def __getattr__(self, name: str) -> Any:
            if name.isupper():
                low = name.lower()
                if hasattr(self, low):
                    return getattr(self, low)
                if name == "APP_NAME":
                    return self.app_name
                if name == "ENV":
                    return self.env
                if name == "VERSION":
                    return self.version
                if name == "EODHD_API_TOKEN":
                    return self.eodhd_api_token
                if name == "FINNHUB_API_TOKEN":
                    return self.finnhub_api_token
            raise AttributeError(name)

        # Optional helper used by main.py if available
        def as_safe_dict(self) -> Dict[str, Any]:
            return self.safe_summary()

    @lru_cache(maxsize=1)
    def get_settings() -> Any:  # type: ignore
        # Use root get_settings if available; otherwise instantiate
        if callable(_root_get_settings):
            s0 = _root_get_settings()  # type: ignore[misc]
        else:
            s0 = Settings()  # type: ignore[call-arg]

        # Convert to our compat class without mutating root instance
        if isinstance(s0, Settings):
            s = s0
        else:
            data = _to_mapping(s0)
            try:
                s = Settings(**data)  # type: ignore[arg-type]
            except Exception:
                s = Settings()  # type: ignore[call-arg]

        _apply_runtime_env_aliases_from_render(s)
        return s


# =============================================================================
# Fallback implementation (only if root config is unavailable)
# =============================================================================
if Settings is None or get_settings is None:

    class Settings:  # type: ignore
        """
        Minimal Settings fallback aligned to your Render env variable names.
        """

        def __init__(self) -> None:
            # App meta
            self.service_name: str = _env_first(["SERVICE_NAME", "APP_NAME"], "Tadawul Stock Analysis API") or "Tadawul Stock Analysis API"
            self.service_version: str = _env_first(["SERVICE_VERSION", "APP_VERSION", "VERSION"], "0.0.0") or "0.0.0"
            self.environment: str = _env_first(["ENVIRONMENT", "APP_ENV", "ENV"], "production") or "production"
            self.tz: str = _env_first(["TZ", "TIMEZONE"], "Asia/Riyadh") or "Asia/Riyadh"

            self.debug: bool = _to_bool(_env_str("DEBUG", "false"), False)
            self.log_level: str = (_env_first(["LOG_LEVEL"], "info") or "info").lower()
            self.log_format: str = _env_first(
                ["LOG_FORMAT"],
                "%(asctime)s | %(levelname)s | %(name)s | %(message)s",
            ) or "%(asctime)s | %(levelname)s | %(name)s | %(message)s"

            # Auth
            self.require_auth: bool = _to_bool(_env_str("REQUIRE_AUTH", "false"), False)
            self.app_token: Optional[str] = _env_first(["APP_TOKEN", "TFB_APP_TOKEN"], None)
            self.backup_app_token: Optional[str] = _env_first(["BACKUP_APP_TOKEN"], None)

            # Providers
            self.enabled_providers_raw: str = _env_first(["ENABLED_PROVIDERS", "PROVIDERS"], "eodhd,finnhub,fmp") or "eodhd,finnhub,fmp"
            self.primary_provider: str = (_env_first(["PRIMARY_PROVIDER"], "eodhd") or "eodhd").strip().lower()
            self.ksa_providers_raw: str = _env_first(["KSA_PROVIDERS"], "yahoo_chart,tadawul,argaam") or "yahoo_chart,tadawul,argaam"

            # Tokens (Render keys)
            self.eodhd_api_key: Optional[str] = _env_first(["EODHD_API_KEY", "EODHD_API_TOKEN", "EODHD_TOKEN"], None)
            self.finnhub_api_key: Optional[str] = _env_first(["FINNHUB_API_KEY", "FINNHUB_API_TOKEN", "FINNHUB_TOKEN"], None)

            # HTTP/retry
            self.http_timeout: float = _to_float(_env_first(["HTTP_TIMEOUT", "HTTP_TIMEOUT_SEC"], "30"), 30.0)
            self.max_retries: int = _to_int(_env_first(["MAX_RETRIES"], "2"), 2)
            self.retry_delay: float = _to_float(_env_first(["RETRY_DELAY", "RETRY_DELAY_SEC"], "0.5"), 0.5)

            # Cache
            self.cache_default_ttl: int = _to_int(_env_first(["CACHE_DEFAULT_TTL"], "10"), 10)
            self.cache_max_size: int = _to_int(_env_first(["CACHE_MAX_SIZE"], "5000"), 5000)
            self.cache_backup_enabled: bool = _to_bool(_env_first(["CACHE_BACKUP_ENABLED"], "false"), False)
            self.cache_save_interval: int = _to_int(_env_first(["CACHE_SAVE_INTERVAL"], "300"), 300)

            # CORS / rate limit
            self.cors_origins: str = _env_first(["CORS_ORIGINS"], "*") or "*"
            self.enable_rate_limiting: bool = _to_bool(_env_first(["ENABLE_RATE_LIMITING"], "true"), True)
            self.max_requests_per_minute: int = _to_int(_env_first(["MAX_REQUESTS_PER_MINUTE"], "240"), 240)

            # Google
            self.default_spreadsheet_id: Optional[str] = _env_first(["DEFAULT_SPREADSHEET_ID"], None)
            self.google_sheets_credentials: Optional[str] = _env_first(["GOOGLE_SHEETS_CREDENTIALS"], None)
            self.google_credentials: Optional[str] = _env_first(["GOOGLE_CREDENTIALS"], None)
            self.google_apps_script_url: Optional[str] = _env_first(["GOOGLE_APPS_SCRIPT_URL"], None)
            self.google_apps_script_backup_url: Optional[str] = _env_first(["GOOGLE_APPS_SCRIPT_BACKUP_URL"], None)

            # Feature flags
            self.advanced_analysis_enabled: bool = _to_bool(_env_first(["ADVANCED_ANALYSIS_ENABLED"], "true"), True)
            self.tadawul_market_enabled: bool = _to_bool(_env_first(["TADAWUL_MARKET_ENABLED"], "true"), True)
            self.enable_swagger: bool = _to_bool(_env_first(["ENABLE_SWAGGER"], "true"), True)
            self.enable_redoc: bool = _to_bool(_env_first(["ENABLE_REDOC"], "true"), True)

        # Legacy names
        @property
        def app_name(self) -> str:
            return self.service_name

        @property
        def env(self) -> str:
            return self.environment

        @property
        def version(self) -> str:
            return self.service_version

        @property
        def enabled_providers(self) -> List[str]:
            xs = _parse_list(self.enabled_providers_raw)
            return xs or ["eodhd", "finnhub", "fmp"]

        @property
        def ksa_providers(self) -> List[str]:
            xs = _parse_list(self.ksa_providers_raw)
            return xs or ["yahoo_chart", "tadawul", "argaam"]

        @property
        def eodhd_api_token(self) -> Optional[str]:
            return (self.eodhd_api_key or "").strip() or None

        @property
        def finnhub_api_token(self) -> Optional[str]:
            return (self.finnhub_api_key or "").strip() or None

        def safe_summary(self) -> Dict[str, Any]:
            creds, creds_src = _resolve_google_credentials(self.google_sheets_credentials, self.google_credentials)
            sid, sid_src = _resolve_spreadsheet_id(self.default_spreadsheet_id)
            return {
                "APP_NAME": self.app_name,
                "ENV": self.env,
                "VERSION": self.version,
                "LOG_LEVEL": self.log_level,
                "REQUIRE_AUTH": bool(self.require_auth),
                "APP_TOKEN_SET": bool((self.app_token or "").strip()),
                "APP_TOKEN_MASK": _mask_tail(self.app_token, 4),
                "ENABLED_PROVIDERS": list(self.enabled_providers),
                "PRIMARY_PROVIDER": self.primary_provider,
                "KSA_PROVIDERS": list(self.ksa_providers),
                "EODHD_TOKEN_SET": bool((self.eodhd_api_token or "").strip()),
                "FINNHUB_TOKEN_SET": bool((self.finnhub_api_token or "").strip()),
                "SPREADSHEET_ID_SET": bool(sid),
                "SPREADSHEET_ID_SOURCE": sid_src,
                "GOOGLE_CREDS_SET": bool(creds),
                "GOOGLE_CREDS_SOURCE": creds_src,
            }

        def as_safe_dict(self) -> Dict[str, Any]:
            return self.safe_summary()

        def __getattr__(self, name: str) -> Any:
            if name.isupper():
                if name == "APP_NAME":
                    return self.app_name
                if name == "ENV":
                    return self.env
                if name == "VERSION":
                    return self.version
                if name == "EODHD_API_TOKEN":
                    return self.eodhd_api_token
                if name == "FINNHUB_API_TOKEN":
                    return self.finnhub_api_token
            raise AttributeError(name)

    @lru_cache(maxsize=1)
    def get_settings() -> Settings:  # type: ignore
        s = Settings()
        _apply_runtime_env_aliases_from_render(s)
        return s


__all__ = ["Settings", "get_settings"]
