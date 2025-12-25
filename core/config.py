# core/config.py  (FULL REPLACEMENT)
"""
core/config.py
------------------------------------------------------------
Compatibility shim (PROD SAFE) – v1.4.0

Many modules import settings via:
  from core.config import Settings, get_settings

Single source of truth is the repo-root `config.py`.
This module re-exports Settings/get_settings so old imports keep working.

Design goals:
- Never break app boot if root config.py is missing or mid-refactor.
- Provide a minimal, attribute-friendly Settings fallback.
- Support BOTH naming styles:
    - lower_case: app_name, env, version, log_level, ...
    - UPPER_CASE: APP_NAME, ENV, VERSION, LOG_LEVEL, ...

v1.4.0 updates
- Improved "partial root config" support:
    • If root exports Settings only -> build get_settings() around it
    • If root exports get_settings only -> infer Settings from it
- Adds knobs used by routers:
    • ai_batch_size / ai_batch_timeout_sec / ai_batch_concurrency / ai_max_tickers
    • adv_batch_size / adv_batch_timeout_sec / adv_batch_concurrency / adv_max_tickers
- Adds resolved helpers for Google credentials / Spreadsheet ID (no secrets leaked).
"""

from __future__ import annotations

import json
import os
from typing import Any, Dict, List, Optional, Tuple

# =============================================================================
# Root config prefer (repo-root config.py)
# =============================================================================
Settings = None  # type: ignore
get_settings = None  # type: ignore

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
# Fallback helpers
# =============================================================================
_TRUTHY = {"1", "true", "yes", "y", "on", "t"}


def _env_str(name: str, default: Optional[str] = None) -> Optional[str]:
    v = os.getenv(name)
    if v is None:
        return default
    v = v.strip()
    return v if v else default


def _env_str_any(names: List[str], default: Optional[str] = None) -> Optional[str]:
    for n in names:
        v = _env_str(n, None)
        if v is not None:
            return v
    return default


def _env_bool_any(names: List[str], default: bool = False) -> bool:
    for n in names:
        v = _env_str(n, None)
        if v is None:
            continue
        return v.strip().lower() in _TRUTHY
    return default


def _env_bool(name: str, default: bool = False) -> bool:
    v = _env_str(name, None)
    if v is None:
        return default
    return v.strip().lower() in _TRUTHY


def _env_int_any(names: List[str], default: int) -> int:
    for n in names:
        v = _env_str(n, None)
        if v is None:
            continue
        try:
            x = int(v)
            # allow zero if explicitly desired? (keep old behavior: >0 only)
            if x > 0:
                return x
        except Exception:
            continue
    return default


def _env_float_any(names: List[str], default: float) -> float:
    for n in names:
        v = _env_str(n, None)
        if v is None:
            continue
        try:
            x = float(v)
            if x > 0:
                return x
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
        y = x.strip()
        if not y:
            continue
        yl = y.lower()
        if yl in seen:
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
    google_sa_json: Optional[str],
) -> Tuple[Optional[str], str]:
    """
    Returns (value, source_label)
    """
    if google_sheets_credentials and google_sheets_credentials.strip():
        return google_sheets_credentials.strip(), "GOOGLE_SHEETS_CREDENTIALS"
    if google_credentials and google_credentials.strip():
        return google_credentials.strip(), "GOOGLE_CREDENTIALS"
    if google_sa_json and google_sa_json.strip():
        return google_sa_json.strip(), "GOOGLE_SA_JSON"
    return None, "none"


def _resolve_spreadsheet_id(spreadsheet_id: Optional[str], google_sheets_id: Optional[str]) -> Tuple[Optional[str], str]:
    if spreadsheet_id and spreadsheet_id.strip():
        return spreadsheet_id.strip(), "SPREADSHEET_ID"
    if google_sheets_id and google_sheets_id.strip():
        return google_sheets_id.strip(), "GOOGLE_SHEETS_ID"
    return None, "none"


# =============================================================================
# If root config exists (fully or partially), use it safely
# =============================================================================
if callable(_root_get_settings) and _root_Settings is not None:
    # Best case: root has both
    Settings = _root_Settings  # type: ignore
    get_settings = _root_get_settings  # type: ignore

elif _root_Settings is not None and not callable(_root_get_settings):
    # Root has Settings only -> build get_settings() around it
    Settings = _root_Settings  # type: ignore
    _settings_singleton: Optional[Any] = None

    def get_settings() -> Any:  # type: ignore
        global _settings_singleton
        if _settings_singleton is None:
            _settings_singleton = Settings()  # type: ignore
        return _settings_singleton

elif callable(_root_get_settings) and _root_Settings is None:
    # Root has get_settings only -> infer Settings type if possible
    get_settings = _root_get_settings  # type: ignore
    try:
        _tmp = get_settings()  # type: ignore
        Settings = _tmp.__class__ if _tmp is not None else None  # type: ignore
    except Exception:
        Settings = None  # type: ignore

# =============================================================================
# Fallback implementation (only used if root exports are unavailable)
# =============================================================================
if Settings is None or get_settings is None:

    class Settings:  # type: ignore
        """
        Minimal Settings fallback.

        Exposes BOTH naming styles for compatibility:
          - app_name + APP_NAME
          - env + ENV
          - version + VERSION
          - log_level + LOG_LEVEL

        And common runtime knobs used by this repo.
        """

        def __init__(self) -> None:
            # -----------------------------------------------------------------
            # Identity
            # -----------------------------------------------------------------
            self.app_name: str = (
                _env_str_any(["APP_NAME", "SERVICE_NAME"], "Tadawul Fast Bridge") or "Tadawul Fast Bridge"
            )

            self.env: str = (
                _env_str_any(["APP_ENV", "ENV", "ENVIRONMENT"], "production") or "production"
            )

            self.version: str = (
                _env_str_any(["SERVICE_VERSION", "APP_VERSION", "VERSION"], "dev") or "dev"
            )

            # -----------------------------------------------------------------
            # Logging
            # -----------------------------------------------------------------
            self.log_level: str = (_env_str_any(["LOG_LEVEL", "UVICORN_LOG_LEVEL"], "info") or "info").lower()
            self.log_format: str = (
                _env_str("LOG_FORMAT", "%(asctime)s | %(levelname)s | %(name)s | %(message)s")
                or "%(asctime)s | %(levelname)s | %(name)s | %(message)s"
            )

            # -----------------------------------------------------------------
            # Auth / Security
            # -----------------------------------------------------------------
            self.app_token: Optional[str] = _env_str_any(["APP_TOKEN", "TFB_APP_TOKEN"], None)
            self.backup_app_token: Optional[str] = _env_str_any(["BACKUP_APP_TOKEN"], None)

            # REQUIRE_AUTH: when true, router auth logic SHOULD enforce token even if none set.
            # (Routers must read this flag; config only exposes it.)
            self.require_auth: bool = _env_bool_any(["REQUIRE_AUTH"], False)

            # -----------------------------------------------------------------
            # Providers
            # -----------------------------------------------------------------
            self.enabled_providers: List[str] = _parse_list(
                _env_str_any(["ENABLED_PROVIDERS", "PROVIDERS"], "eodhd,finnhub,fmp,yahoo_chart")
            )
            self.enabled_ksa_providers: List[str] = _parse_list(_env_str("KSA_PROVIDERS", "tadawul,argaam,yahoo_chart"))

            self.primary_provider: str = (_env_str_any(["PRIMARY_PROVIDER"], "") or "").strip().lower()

            # KSA policy flags
            self.enable_yfinance_ksa: bool = _env_bool("ENABLE_YFINANCE_KSA", False)
            self.enable_yahoo_chart_ksa: bool = _env_bool("ENABLE_YAHOO_CHART_KSA", True)

            # -----------------------------------------------------------------
            # HTTP / retry controls
            # -----------------------------------------------------------------
            self.http_timeout_sec: int = _env_int_any(["HTTP_TIMEOUT_SEC", "HTTP_TIMEOUT"], 25)
            self.max_retries: int = _env_int_any(["MAX_RETRIES"], 2)
            self.retry_delay_sec: int = _env_int_any(["RETRY_DELAY_SEC", "RETRY_DELAY"], 1)

            # -----------------------------------------------------------------
            # Cache / TTL controls (seconds)
            # -----------------------------------------------------------------
            self.cache_ttl_sec: int = _env_int_any(["CACHE_TTL_SEC"], 20)
            self.quote_ttl_sec: int = _env_int_any(["QUOTE_TTL_SEC"], 30)
            self.fundamentals_ttl_sec: int = _env_int_any(["FUNDAMENTALS_TTL_SEC"], 21600)
            self.argaam_snapshot_ttl_sec: int = _env_int_any(["ARGAAM_SNAPSHOT_TTL_SEC"], 30)

            # -----------------------------------------------------------------
            # Feature flags
            # -----------------------------------------------------------------
            self.ai_analysis_enabled: bool = _env_bool("AI_ANALYSIS_ENABLED", True)
            self.advanced_analysis_enabled: bool = _env_bool("ADVANCED_ANALYSIS_ENABLED", True)

            # Router knobs (align with routes/ai_analysis.py and routes/advanced_analysis.py)
            self.ai_batch_size: int = _env_int_any(["AI_BATCH_SIZE"], 20)
            self.ai_batch_timeout_sec: float = _env_float_any(["AI_BATCH_TIMEOUT_SEC"], 45.0)
            self.ai_batch_concurrency: int = _env_int_any(["AI_BATCH_CONCURRENCY"], 5)
            self.ai_max_tickers: int = _env_int_any(["AI_MAX_TICKERS"], 500)

            self.adv_batch_size: int = _env_int_any(["ADV_BATCH_SIZE"], 25)
            self.adv_batch_timeout_sec: float = _env_float_any(["ADV_BATCH_TIMEOUT_SEC"], 45.0)
            self.adv_batch_concurrency: int = _env_int_any(["ADV_BATCH_CONCURRENCY"], 6)
            self.adv_max_tickers: int = _env_int_any(["ADV_MAX_TICKERS"], 500)

            # -----------------------------------------------------------------
            # CORS
            # -----------------------------------------------------------------
            self.enable_cors_all_origins: bool = _env_bool_any(["ENABLE_CORS_ALL_ORIGINS", "CORS_ALL_ORIGINS"], True)
            self.cors_origins_list: List[str] = (
                ["*"]
                if self.enable_cors_all_origins
                else _parse_list(_env_str_any(["CORS_ORIGINS", "CORS_ALLOW_ORIGINS"], "") or "")
            )

            # -----------------------------------------------------------------
            # Rate limiting (SlowAPI)
            # -----------------------------------------------------------------
            self.enable_rate_limiting: bool = _env_bool_any(["ENABLE_RATE_LIMITING"], True)
            self.rate_limit_per_minute: int = _env_int_any(["RATE_LIMIT_PER_MINUTE"], 240)

            # Keep old names too (legacy compatibility)
            self.slowapi_enabled: bool = _env_bool_any(["SLOWAPI_ENABLED"], True)
            self.slowapi_default_limit: str = _env_str("SLOWAPI_DEFAULT_LIMIT", f"{self.rate_limit_per_minute}/minute") or f"{self.rate_limit_per_minute}/minute"

            # -----------------------------------------------------------------
            # API keys
            # -----------------------------------------------------------------
            self.eodhd_api_key: Optional[str] = _env_str("EODHD_API_KEY", None)
            self.finnhub_api_key: Optional[str] = _env_str("FINNHUB_API_KEY", None)
            self.fmp_api_key: Optional[str] = _env_str("FMP_API_KEY", None)
            self.alpha_vantage_api_key: Optional[str] = _env_str("ALPHA_VANTAGE_API_KEY", None)
            self.twelvedata_api_key: Optional[str] = _env_str("TWELVEDATA_API_KEY", None)
            self.marketstack_api_key: Optional[str] = _env_str("MARKETSTACK_API_KEY", None)

            # -----------------------------------------------------------------
            # URLs / Integration
            # -----------------------------------------------------------------
            self.base_url: str = (_env_str_any(["BASE_URL", "BACKEND_BASE_URL"], "") or "").strip()
            self.backend_base_url: str = (_env_str_any(["BACKEND_BASE_URL", "BASE_URL"], "") or "").strip()

            self.google_apps_script_url: str = (_env_str("GOOGLE_APPS_SCRIPT_URL", "") or "").strip()
            self.google_apps_script_backup_url: str = (_env_str("GOOGLE_APPS_SCRIPT_BACKUP_URL", "") or "").strip()

            # -----------------------------------------------------------------
            # Google / Sheets (critical alignment)
            # -----------------------------------------------------------------
            self.google_sheets_credentials: Optional[str] = _env_str("GOOGLE_SHEETS_CREDENTIALS", None)
            self.google_credentials: Optional[str] = _env_str("GOOGLE_CREDENTIALS", None)
            self.google_sa_json: Optional[str] = _env_str("GOOGLE_SA_JSON", None)

            self.spreadsheet_id: Optional[str] = _env_str_any(["SPREADSHEET_ID", "DEFAULT_SPREADSHEET_ID"], None)
            self.google_sheets_id: Optional[str] = _env_str("GOOGLE_SHEETS_ID", None)

        # ---------------------------------------------------------------------
        # Helpful “safe” summary for diagnostics (no secret leaks)
        # ---------------------------------------------------------------------
        def safe_summary(self) -> Dict[str, Any]:
            creds, creds_src = _resolve_google_credentials(
                self.google_sheets_credentials,
                self.google_credentials,
                self.google_sa_json,
            )
            sid, sid_src = _resolve_spreadsheet_id(self.spreadsheet_id, self.google_sheets_id)

            return {
                "APP_ENV": self.env,
                "APP_NAME": self.app_name,
                "VERSION": self.version,
                "LOG_LEVEL": self.log_level,
                "REQUIRE_AUTH": bool(self.require_auth),
                "APP_TOKEN_SET": bool(self.app_token),
                "BACKUP_APP_TOKEN_SET": bool(self.backup_app_token),
                "APP_TOKEN_MASK": _mask_tail(self.app_token, keep=4),
                "ENABLED_PROVIDERS": list(self.enabled_providers),
                "KSA_PROVIDERS": list(self.enabled_ksa_providers),
                "PRIMARY_PROVIDER": self.primary_provider,
                "AI_ANALYSIS_ENABLED": bool(self.ai_analysis_enabled),
                "ADVANCED_ANALYSIS_ENABLED": bool(self.advanced_analysis_enabled),
                "AI_LIMITS": {
                    "batch_size": int(self.ai_batch_size),
                    "timeout_sec": float(self.ai_batch_timeout_sec),
                    "concurrency": int(self.ai_batch_concurrency),
                    "max_tickers": int(self.ai_max_tickers),
                },
                "ADV_LIMITS": {
                    "batch_size": int(self.adv_batch_size),
                    "timeout_sec": float(self.adv_batch_timeout_sec),
                    "concurrency": int(self.adv_batch_concurrency),
                    "max_tickers": int(self.adv_max_tickers),
                },
                "ENABLE_YFINANCE_KSA": bool(self.enable_yfinance_ksa),
                "ENABLE_YAHOO_CHART_KSA": bool(self.enable_yahoo_chart_ksa),
                "HTTP_TIMEOUT_SEC": int(self.http_timeout_sec),
                "MAX_RETRIES": int(self.max_retries),
                "RETRY_DELAY_SEC": int(self.retry_delay_sec),
                "CACHE_TTL_SEC": int(self.cache_ttl_sec),
                "QUOTE_TTL_SEC": int(self.quote_ttl_sec),
                "FUNDAMENTALS_TTL_SEC": int(self.fundamentals_ttl_sec),
                "ARGAAM_SNAPSHOT_TTL_SEC": int(self.argaam_snapshot_ttl_sec),
                "ENABLE_RATE_LIMITING": bool(self.enable_rate_limiting),
                "RATE_LIMIT_PER_MINUTE": int(self.rate_limit_per_minute),
                "CORS_ALL": bool(self.enable_cors_all_origins),
                "CORS_ORIGINS": ["*"] if self.enable_cors_all_origins else list(self.cors_origins_list),
                "SPREADSHEET_ID_SET": bool(sid),
                "SPREADSHEET_ID_SOURCE": sid_src,
                "GOOGLE_CREDENTIALS_SET": bool(creds),
                "GOOGLE_CREDENTIALS_SOURCE": creds_src,
                "GOOGLE_APPS_SCRIPT_BACKUP_URL_SET": bool(self.google_apps_script_backup_url),
            }

        # ---------------------------------------------------------------------
        # Resolved helpers (used by clients safely)
        # ---------------------------------------------------------------------
        def resolved_google_credentials(self) -> Tuple[Optional[str], str]:
            return _resolve_google_credentials(self.google_sheets_credentials, self.google_credentials, self.google_sa_json)

        def resolved_spreadsheet_id(self) -> Tuple[Optional[str], str]:
            return _resolve_spreadsheet_id(self.spreadsheet_id, self.google_sheets_id)

        # ---------------------------------------------------------------------
        # UPPERCASE aliases (legacy compatibility)
        # ---------------------------------------------------------------------
        def __getattr__(self, name: str) -> Any:
            # allow uppercase -> lowercase mapping automatically
            if name.isupper():
                low = name.lower()
                if low != name and hasattr(self, low):
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
        def PROVIDERS(self) -> List[str]:  # noqa: N802
            return list(self.enabled_providers)

        @property
        def KSA_PROVIDERS(self) -> List[str]:  # noqa: N802
            return list(self.enabled_ksa_providers)

        @property
        def CORS_ALLOW_ORIGINS(self) -> List[str]:  # noqa: N802
            return list(self.cors_origins_list)

        @property
        def EODHD_API_KEY(self) -> Optional[str]:  # noqa: N802
            return self.eodhd_api_key

        @property
        def FINNHUB_API_KEY(self) -> Optional[str]:  # noqa: N802
            return self.finnhub_api_key

        @property
        def FMP_API_KEY(self) -> Optional[str]:  # noqa: N802
            return self.fmp_api_key

        @property
        def SLOWAPI_ENABLED(self) -> bool:  # noqa: N802
            return self.slowapi_enabled

        @property
        def SLOWAPI_DEFAULT_LIMIT(self) -> str:  # noqa: N802
            return self.slowapi_default_limit

        @property
        def GOOGLE_SHEETS_CREDENTIALS(self) -> Optional[str]:  # noqa: N802
            return self.google_sheets_credentials

        @property
        def SPREADSHEET_ID(self) -> Optional[str]:  # noqa: N802
            sid, _ = self.resolved_spreadsheet_id()
            return sid

    _settings_singleton: Optional[Settings] = None

    def get_settings() -> Settings:  # type: ignore
        global _settings_singleton
        if _settings_singleton is None:
            _settings_singleton = Settings()
        return _settings_singleton


__all__ = ["Settings", "get_settings"]
