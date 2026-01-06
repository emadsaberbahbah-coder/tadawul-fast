# core/config.py  — FULL REPLACEMENT — v1.9.1
"""
core/config.py
------------------------------------------------------------
Compatibility shim (PROD SAFE) – v1.9.1 (RENDER-ENV LOCKED)

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

✅ New (non-breaking) env keys to enable the “per-page headers + forecasting plan”:
- SHEET_SCHEMAS_ENABLED                 (default: true)
- SHEET_SCHEMA_VERSION                  (default: "vNext")
- FORECAST_ENABLED                      (default: true)
- FORECAST_HORIZONS_DAYS                (default: "30,90,365")
- FORECAST_LOOKBACK_DAYS                (default: 365)
- FORECAST_MIN_POINTS                   (default: 90)
- FORECAST_METHOD                       (default: "ewma")  # engine decides
- FORECAST_CACHE_TTL                    (default: 180)
- RECOMMENDATION_MODE                   (default: "hybrid")  # fair_value | forecast | hybrid
- RECOMMENDATION_PRIMARY_HORIZON_DAYS   (default: 90)
- PERCENT_CHANGE_FORMAT                 (default: "ratio")   # ratio | percent (ratio recommended for Sheets)

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
        return int(float(s))
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


def _parse_int_list(value: Any, default: List[int]) -> List[int]:
    """
    Accept:
      - python list
      - CSV string "30,90,365"
      - JSON list string "[30,90,365]"
    Return: cleaned int list (deduped, order preserved). Falls back to default if empty.
    """
    xs_raw = _parse_list(value)
    if not xs_raw and isinstance(value, list):
        try:
            xs_raw = [str(x).strip() for x in value if str(x).strip()]
        except Exception:
            xs_raw = []

    out: List[int] = []
    seen = set()
    for x in xs_raw:
        n = _to_int(x, -1)
        if n <= 0 or n in seen:
            continue
        seen.add(n)
        out.append(n)

    return out or list(default)


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
        getattr(s, "finnhub_api_key", None)
        or _env_first(["FINNHUB_API_KEY", "FINNHUB_API_TOKEN", "FINNHUB_TOKEN"], None)
    )
    eodhd = (
        getattr(s, "eodhd_api_key", None)
        or _env_first(["EODHD_API_KEY", "EODHD_API_TOKEN", "EODHD_TOKEN"], None)
    )

    # If the wrapper exposes token-style properties, prefer them (but do not force)
    try:
        finnhub_tok = getattr(s, "finnhub_api_token", None)
        if finnhub_tok:
            finnhub = finnhub_tok
    except Exception:
        pass

    try:
        eodhd_tok = getattr(s, "eodhd_api_token", None)
        if eodhd_tok:
            eodhd = eodhd_tok
    except Exception:
        pass

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
        Adds legacy + new “schema + forecasting” properties used across modules.
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

        # --- provider lists ---
        @property
        def enabled_providers(self) -> List[str]:
            # Prefer root's computed property if present
            try:
                v = super().enabled_providers  # type: ignore[misc]
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
                v = super().ksa_providers  # type: ignore[misc]
                if isinstance(v, list) and v:
                    return [str(x).strip().lower() for x in v if str(x).strip()]
            except Exception:
                pass

            raw = getattr(self, "ksa_providers_raw", None) or _env_first(
                ["KSA_PROVIDERS"], "yahoo_chart,tadawul,argaam"
            )
            xs = _parse_list(raw)
            return xs or ["yahoo_chart", "tadawul", "argaam"]

        # --- token-style properties (some code expects *_api_token) ---
        @property
        def eodhd_api_token(self) -> Optional[str]:
            v = getattr(self, "eodhd_api_key", None)
            if not v:
                try:
                    v = super().eodhd_api_token  # type: ignore[attr-defined]
                except Exception:
                    v = None
            v = v or _env_first(["EODHD_API_KEY", "EODHD_API_TOKEN", "EODHD_TOKEN"], None)
            return (str(v).strip() if v else None)

        @property
        def finnhub_api_token(self) -> Optional[str]:
            v = getattr(self, "finnhub_api_key", None)
            if not v:
                try:
                    v = super().finnhub_api_token  # type: ignore[attr-defined]
                except Exception:
                    v = None
            v = v or _env_first(["FINNHUB_API_KEY", "FINNHUB_API_TOKEN", "FINNHUB_TOKEN"], None)
            return (str(v).strip() if v else None)

        # ---------------------------------------------------------------------
        # ✅ New: Sheet schemas (per-page headers) controls
        # ---------------------------------------------------------------------
        @property
        def sheet_schemas_enabled(self) -> bool:
            v = None
            try:
                v = super().sheet_schemas_enabled  # type: ignore[attr-defined]
            except Exception:
                v = None
            if v is None:
                v = _env_first(["SHEET_SCHEMAS_ENABLED"], "true")
            return _to_bool(v, True)

        @property
        def sheet_schema_version(self) -> str:
            v = None
            try:
                v = super().sheet_schema_version  # type: ignore[attr-defined]
            except Exception:
                v = None
            if not v:
                v = _env_first(["SHEET_SCHEMA_VERSION"], "vNext")
            return (v or "vNext").strip()

        # ---------------------------------------------------------------------
        # ✅ New: Forecasting & recommendation controls
        # ---------------------------------------------------------------------
        @property
        def forecast_enabled(self) -> bool:
            v = None
            try:
                v = super().forecast_enabled  # type: ignore[attr-defined]
            except Exception:
                v = None
            if v is None:
                v = _env_first(["FORECAST_ENABLED"], "true")
            return _to_bool(v, True)

        @property
        def forecast_horizons_days(self) -> List[int]:
            raw = None
            try:
                raw = super().forecast_horizons_days  # type: ignore[attr-defined]
            except Exception:
                raw = None

            if isinstance(raw, list) and raw:
                # already list[int]
                out: List[int] = []
                seen = set()
                for x in raw:
                    n = _to_int(x, -1)
                    if n > 0 and n not in seen:
                        seen.add(n)
                        out.append(n)
                if out:
                    return out

            raw2 = getattr(self, "forecast_horizons_days_raw", None) or _env_first(["FORECAST_HORIZONS_DAYS"], "30,90,365")
            return _parse_int_list(raw2, default=[30, 90, 365])

        @property
        def forecast_lookback_days(self) -> int:
            v = None
            try:
                v = super().forecast_lookback_days  # type: ignore[attr-defined]
            except Exception:
                v = None
            if v is None:
                v = _env_first(["FORECAST_LOOKBACK_DAYS"], "365")
            return _to_int(v, 365)

        @property
        def forecast_min_points(self) -> int:
            v = None
            try:
                v = super().forecast_min_points  # type: ignore[attr-defined]
            except Exception:
                v = None
            if v is None:
                v = _env_first(["FORECAST_MIN_POINTS"], "90")
            return _to_int(v, 90)

        @property
        def forecast_method(self) -> str:
            v = None
            try:
                v = super().forecast_method  # type: ignore[attr-defined]
            except Exception:
                v = None
            if not v:
                v = _env_first(["FORECAST_METHOD"], "ewma")
            return (v or "ewma").strip().lower()

        @property
        def forecast_cache_ttl(self) -> int:
            v = None
            try:
                v = super().forecast_cache_ttl  # type: ignore[attr-defined]
            except Exception:
                v = None
            if v is None:
                v = _env_first(["FORECAST_CACHE_TTL"], "180")
            return _to_int(v, 180)

        @property
        def recommendation_mode(self) -> str:
            v = None
            try:
                v = super().recommendation_mode  # type: ignore[attr-defined]
            except Exception:
                v = None
            if not v:
                v = _env_first(["RECOMMENDATION_MODE"], "hybrid")
            x = (v or "hybrid").strip().lower()
            return x if x in {"fair_value", "forecast", "hybrid"} else "hybrid"

        @property
        def recommendation_primary_horizon_days(self) -> int:
            v = None
            try:
                v = super().recommendation_primary_horizon_days  # type: ignore[attr-defined]
            except Exception:
                v = None
            if v is None:
                v = _env_first(["RECOMMENDATION_PRIMARY_HORIZON_DAYS"], "90")
            return _to_int(v, 90)

        @property
        def percent_change_format(self) -> str:
            """
            ratio  -> store as 0.0123 and format as % in Sheets (recommended)
            percent -> store as 1.23 (already percent units)
            """
            v = None
            try:
                v = super().percent_change_format  # type: ignore[attr-defined]
            except Exception:
                v = None
            if not v:
                v = _env_first(["PERCENT_CHANGE_FORMAT"], "ratio")
            x = (v or "ratio").strip().lower()
            return x if x in {"ratio", "percent"} else "ratio"

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
                "SHEET_SCHEMAS_ENABLED": bool(self.sheet_schemas_enabled),
                "SHEET_SCHEMA_VERSION": self.sheet_schema_version,
                "FORECAST_ENABLED": bool(self.forecast_enabled),
                "FORECAST_HORIZONS_DAYS": list(self.forecast_horizons_days),
                "FORECAST_LOOKBACK_DAYS": int(self.forecast_lookback_days),
                "FORECAST_MIN_POINTS": int(self.forecast_min_points),
                "FORECAST_METHOD": self.forecast_method,
                "FORECAST_CACHE_TTL": int(self.forecast_cache_ttl),
                "RECOMMENDATION_MODE": self.recommendation_mode,
                "RECOMMENDATION_PRIMARY_HORIZON_DAYS": int(self.recommendation_primary_horizon_days),
                "PERCENT_CHANGE_FORMAT": self.percent_change_format,
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
        Includes vNext schema + forecast controls (non-breaking).
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
            self.require_auth: bool = _to_bool(_env_first(["REQUIRE_AUTH"], "false"), False)
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

            # ✅ vNext schema + forecasting controls
            self._sheet_schemas_enabled_raw: str = _env_first(["SHEET_SCHEMAS_ENABLED"], "true") or "true"
            self._sheet_schema_version_raw: str = _env_first(["SHEET_SCHEMA_VERSION"], "vNext") or "vNext"

            self._forecast_enabled_raw: str = _env_first(["FORECAST_ENABLED"], "true") or "true"
            self._forecast_horizons_days_raw: str = _env_first(["FORECAST_HORIZONS_DAYS"], "30,90,365") or "30,90,365"
            self._forecast_lookback_days_raw: str = _env_first(["FORECAST_LOOKBACK_DAYS"], "365") or "365"
            self._forecast_min_points_raw: str = _env_first(["FORECAST_MIN_POINTS"], "90") or "90"
            self._forecast_method_raw: str = _env_first(["FORECAST_METHOD"], "ewma") or "ewma"
            self._forecast_cache_ttl_raw: str = _env_first(["FORECAST_CACHE_TTL"], "180") or "180"

            self._recommendation_mode_raw: str = _env_first(["RECOMMENDATION_MODE"], "hybrid") or "hybrid"
            self._recommendation_primary_horizon_days_raw: str = _env_first(["RECOMMENDATION_PRIMARY_HORIZON_DAYS"], "90") or "90"
            self._percent_change_format_raw: str = _env_first(["PERCENT_CHANGE_FORMAT"], "ratio") or "ratio"

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

        # ✅ vNext schema + forecasting properties
        @property
        def sheet_schemas_enabled(self) -> bool:
            return _to_bool(self._sheet_schemas_enabled_raw, True)

        @property
        def sheet_schema_version(self) -> str:
            return (self._sheet_schema_version_raw or "vNext").strip()

        @property
        def forecast_enabled(self) -> bool:
            return _to_bool(self._forecast_enabled_raw, True)

        @property
        def forecast_horizons_days(self) -> List[int]:
            return _parse_int_list(self._forecast_horizons_days_raw, default=[30, 90, 365])

        @property
        def forecast_lookback_days(self) -> int:
            return _to_int(self._forecast_lookback_days_raw, 365)

        @property
        def forecast_min_points(self) -> int:
            return _to_int(self._forecast_min_points_raw, 90)

        @property
        def forecast_method(self) -> str:
            return (self._forecast_method_raw or "ewma").strip().lower()

        @property
        def forecast_cache_ttl(self) -> int:
            return _to_int(self._forecast_cache_ttl_raw, 180)

        @property
        def recommendation_mode(self) -> str:
            x = (self._recommendation_mode_raw or "hybrid").strip().lower()
            return x if x in {"fair_value", "forecast", "hybrid"} else "hybrid"

        @property
        def recommendation_primary_horizon_days(self) -> int:
            return _to_int(self._recommendation_primary_horizon_days_raw, 90)

        @property
        def percent_change_format(self) -> str:
            x = (self._percent_change_format_raw or "ratio").strip().lower()
            return x if x in {"ratio", "percent"} else "ratio"

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
                "SHEET_SCHEMAS_ENABLED": bool(self.sheet_schemas_enabled),
                "SHEET_SCHEMA_VERSION": self.sheet_schema_version,
                "FORECAST_ENABLED": bool(self.forecast_enabled),
                "FORECAST_HORIZONS_DAYS": list(self.forecast_horizons_days),
                "FORECAST_LOOKBACK_DAYS": int(self.forecast_lookback_days),
                "FORECAST_MIN_POINTS": int(self.forecast_min_points),
                "FORECAST_METHOD": self.forecast_method,
                "FORECAST_CACHE_TTL": int(self.forecast_cache_ttl),
                "RECOMMENDATION_MODE": self.recommendation_mode,
                "RECOMMENDATION_PRIMARY_HORIZON_DAYS": int(self.recommendation_primary_horizon_days),
                "PERCENT_CHANGE_FORMAT": self.percent_change_format,
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
