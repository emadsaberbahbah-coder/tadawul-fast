# env.py — FULL REPLACEMENT (QUIET + PROD SAFE) — v5.0.5
"""
env.py
------------------------------------------------------------
Backward-compatible environment exports for Tadawul Fast Bridge (v5.0.5)

Key goals
- ✅ No noisy banner logs by default (quiet boot).
- ✅ Version resolves from ENV FIRST: APP_VERSION / SERVICE_VERSION / VERSION.
- ✅ Never crashes import-time (defensive).
- ✅ Provides legacy exports used by older modules/routes.
- ✅ Keeps ENGINE_CACHE_TTL_SEC + ENRICHED_BATCH_CONCURRENCY (and aligns with ENRICHED_CONCURRENCY).

Enable optional banner log:
- ENV_LOG_ON_BOOT=true
"""

from __future__ import annotations

import json
import logging
import os
from typing import Any, Dict, List, Optional


ENV_VERSION = "5.0.5"
logger = logging.getLogger("env")

_TRUTHY = {"1", "true", "yes", "y", "on", "t"}
_FALSY = {"0", "false", "no", "n", "off", "f"}


def _truthy(v: Any) -> bool:
    return str(v or "").strip().lower() in _TRUTHY


def _safe_bool(v: Any, default: bool = False) -> bool:
    if isinstance(v, bool):
        return v
    if v is None:
        return default
    s = str(v).strip().lower()
    if s in _TRUTHY:
        return True
    if s in _FALSY:
        return False
    return default


def _safe_int(v: Any, default: int) -> int:
    try:
        x = int(str(v).strip())
        return x if x > 0 else default
    except Exception:
        return default


def _safe_float(v: Any, default: float) -> float:
    try:
        x = float(str(v).strip())
        return x if x > 0 else default
    except Exception:
        return default


def _normalize_version(v: Any) -> str:
    s = str(v or "").strip()
    if not s:
        return ""
    if s.lower() in {"unknown", "none", "null"}:
        return ""
    return s


def _mask_tail(s: Optional[str], keep: int = 4) -> str:
    x = (s or "").strip()
    if not x:
        return ""
    if len(x) <= keep:
        return "•" * len(x)
    return ("•" * (len(x) - keep)) + x[-keep:]


def _safe_join(items: List[str]) -> str:
    return ",".join([str(x) for x in items if str(x).strip()])


def _as_list_lower(v: Any) -> List[str]:
    if v is None:
        return []
    if isinstance(v, list):
        raw = [str(x).strip().lower() for x in v if str(x).strip()]
    else:
        s = str(v).strip()
        if not s:
            return []
        if s.startswith("[") and s.endswith("]"):
            try:
                arr = json.loads(s)
                if isinstance(arr, list):
                    raw = [str(x).strip().lower() for x in arr if str(x).strip()]
                else:
                    raw = []
            except Exception:
                raw = []
        else:
            raw = [p.strip().lower() for p in s.split(",") if p.strip()]

    # de-dupe preserve order
    out: List[str] = []
    seen = set()
    for x in raw:
        if x and x not in seen:
            seen.add(x)
            out.append(x)
    return out


def _try_parse_json_dict(raw: Any) -> Optional[Dict[str, Any]]:
    if raw is None:
        return None
    if isinstance(raw, dict):
        return raw
    if not isinstance(raw, str):
        return None
    s = raw.strip()
    if not s:
        return None
    if (s.startswith('"') and s.endswith('"')) or (s.startswith("'") and s.endswith("'")):
        s = s[1:-1].strip()
    try:
        obj = json.loads(s)
        return obj if isinstance(obj, dict) else None
    except Exception:
        return None


# ---------------------------------------------------------------------
# Optional: try to load Settings (VERY DEFENSIVE)
# ---------------------------------------------------------------------
_SETTINGS_LOADED = False
_SETTINGS_OBJ: Optional[object] = None
_LOADING_SETTINGS = False


def _try_load_settings_once() -> Optional[object]:
    global _SETTINGS_LOADED, _SETTINGS_OBJ, _LOADING_SETTINGS
    if _SETTINGS_LOADED:
        return _SETTINGS_OBJ
    if _LOADING_SETTINGS:
        return None

    _LOADING_SETTINGS = True
    try:
        # Prefer config.py at repo root (your main.py prefers this too)
        try:
            from config import get_settings  # type: ignore

            _SETTINGS_OBJ = get_settings()
            _SETTINGS_LOADED = True
            return _SETTINGS_OBJ
        except Exception:
            pass

        # Fallback legacy
        try:
            from core.config import get_settings  # type: ignore

            _SETTINGS_OBJ = get_settings()
            _SETTINGS_LOADED = True
            return _SETTINGS_OBJ
        except Exception:
            pass

        _SETTINGS_LOADED = True
        _SETTINGS_OBJ = None
        return None
    finally:
        _LOADING_SETTINGS = False


def _get_attr(obj: Optional[object], name: str, default: Any = None) -> Any:
    if obj is None:
        return default
    try:
        if hasattr(obj, name):
            v = getattr(obj, name)
            if v is None:
                return default
            if isinstance(v, str) and not v.strip():
                return default
            return v
    except Exception:
        return default
    return default


def _get_first_env(*keys: str) -> Optional[str]:
    for k in keys:
        v = os.getenv(k)
        if v is not None and str(v).strip():
            return str(v).strip()
    return None


def _resolve_version(settings_obj: Optional[object]) -> str:
    # 1) ENV FIRST (canonical)
    for k in ("APP_VERSION", "SERVICE_VERSION", "VERSION", "RELEASE"):
        vv = _normalize_version(_get_first_env(k))
        if vv:
            return vv

    # 2) Render commit short hash
    commit = (os.getenv("RENDER_GIT_COMMIT") or os.getenv("GIT_COMMIT") or "").strip()
    if commit:
        return commit[:7]

    # 3) Settings fallback
    for k in ("version", "app_version", "APP_VERSION"):
        vv = _normalize_version(_get_attr(settings_obj, k, ""))
        if vv:
            return vv

    return "dev"


# ---------------------------------------------------------------------
# Build exports (prefer ENV, then settings, then defaults)
# ---------------------------------------------------------------------
_base_settings = _try_load_settings_once()

APP_NAME = (
    _get_first_env("APP_NAME", "SERVICE_NAME", "APP_TITLE")
    or str(_get_attr(_base_settings, "app_name", "") or _get_attr(_base_settings, "APP_NAME", "") or "").strip()
    or "Tadawul Fast Bridge"
)

APP_ENV = (
    _get_first_env("APP_ENV", "ENVIRONMENT")
    or str(_get_attr(_base_settings, "env", "") or _get_attr(_base_settings, "environment", "") or "").strip()
    or "production"
)

APP_VERSION = _resolve_version(_base_settings)

BACKEND_BASE_URL = (
    (_get_first_env("BACKEND_BASE_URL", "TFB_BASE_URL", "BASE_URL") or "").rstrip("/")
    or str(_get_attr(_base_settings, "backend_base_url", "") or _get_attr(_base_settings, "base_url", "") or "").rstrip("/")
)

APP_TOKEN = _get_first_env("APP_TOKEN", "TFB_APP_TOKEN") or str(_get_attr(_base_settings, "app_token", "") or "").strip() or None
BACKUP_APP_TOKEN = _get_first_env("BACKUP_APP_TOKEN") or str(_get_attr(_base_settings, "backup_app_token", "") or "").strip() or None

REQUIRE_AUTH = _safe_bool(_get_first_env("REQUIRE_AUTH"), False) or _safe_bool(_get_attr(_base_settings, "require_auth", None), False)

# Providers
ENABLED_PROVIDERS: List[str] = _as_list_lower(
    _get_first_env("ENABLED_PROVIDERS", "PROVIDERS")
    or _get_attr(_base_settings, "enabled_providers", None)
    or "eodhd,finnhub"
)

KSA_PROVIDERS: List[str] = _as_list_lower(
    _get_first_env("KSA_PROVIDERS")
    or _get_attr(_base_settings, "enabled_ksa_providers", None)
    or "yahoo_chart,tadawul,argaam"
)

PRIMARY_PROVIDER = (
    (_get_first_env("PRIMARY_PROVIDER") or "").strip().lower()
    or str(_get_attr(_base_settings, "primary_provider", "") or "").strip().lower()
    or (ENABLED_PROVIDERS[0] if ENABLED_PROVIDERS else "finnhub")
)

PROVIDERS = _safe_join(ENABLED_PROVIDERS)

# HTTP / cache
HTTP_TIMEOUT_SEC = _safe_float(_get_first_env("HTTP_TIMEOUT_SEC", "HTTP_TIMEOUT"), 25.0)
HTTP_TIMEOUT_SEC = max(5.0, float(HTTP_TIMEOUT_SEC or 25.0))
HTTP_TIMEOUT = _safe_int(_get_first_env("HTTP_TIMEOUT"), int(HTTP_TIMEOUT_SEC))

CACHE_TTL_SEC = _safe_float(_get_first_env("CACHE_TTL_SEC"), 20.0)

# Added (keep your legacy aliases)
ENGINE_CACHE_TTL_SEC = _safe_int(_get_first_env("ENGINE_CACHE_TTL_SEC", "ENGINE_TTL_SEC"), int(CACHE_TTL_SEC))
# Keep legacy name but also align with your blueprint "ENRICHED_CONCURRENCY"
ENRICHED_BATCH_CONCURRENCY = _safe_int(_get_first_env("ENRICHED_BATCH_CONCURRENCY", "ENRICHED_CONCURRENCY"), 8)

# Provider keys
EODHD_API_KEY = _get_first_env("EODHD_API_KEY") or _get_attr(_base_settings, "eodhd_api_key", None)
FINNHUB_API_KEY = _get_first_env("FINNHUB_API_KEY") or _get_attr(_base_settings, "finnhub_api_key", None)
FMP_API_KEY = _get_first_env("FMP_API_KEY") or _get_attr(_base_settings, "fmp_api_key", None)
ALPHA_VANTAGE_API_KEY = _get_first_env("ALPHA_VANTAGE_API_KEY") or _get_attr(_base_settings, "alpha_vantage_api_key", None)
ARGAAM_API_KEY = _get_first_env("ARGAAM_API_KEY") or _get_attr(_base_settings, "argaam_api_key", None)

# CORS
ENABLE_CORS_ALL_ORIGINS = _safe_bool(_get_first_env("ENABLE_CORS_ALL_ORIGINS", "CORS_ALL_ORIGINS"), True)
CORS_ALL_ORIGINS = ENABLE_CORS_ALL_ORIGINS
CORS_ORIGINS = _get_first_env("CORS_ORIGINS") or _get_attr(_base_settings, "cors_origins", None)
CORS_ORIGINS_LIST = ["*"] if ENABLE_CORS_ALL_ORIGINS else _as_list_lower(CORS_ORIGINS or "")

# Google Sheets
_GOOGLE_SHEETS_CREDENTIALS_RAW = (
    _get_first_env("GOOGLE_SHEETS_CREDENTIALS", "GOOGLE_CREDENTIALS", "GOOGLE_SA_JSON") or ""
).strip()
GOOGLE_SHEETS_CREDENTIALS: Optional[Dict[str, Any]] = _try_parse_json_dict(_GOOGLE_SHEETS_CREDENTIALS_RAW)

DEFAULT_SPREADSHEET_ID = (_get_first_env("DEFAULT_SPREADSHEET_ID", "SPREADSHEET_ID", "GOOGLE_SHEETS_ID") or "").strip()
SPREADSHEET_ID = DEFAULT_SPREADSHEET_ID
GOOGLE_SHEETS_ID = (_get_first_env("GOOGLE_SHEETS_ID") or "").strip()

GOOGLE_APPS_SCRIPT_URL = (_get_first_env("GOOGLE_APPS_SCRIPT_URL") or "").strip()
GOOGLE_APPS_SCRIPT_BACKUP_URL = (_get_first_env("GOOGLE_APPS_SCRIPT_BACKUP_URL") or "").strip()

# Sheet names (keep your legacy keys)
SHEET_KSA_TADAWUL = (_get_first_env("SHEET_KSA_TADAWUL") or "KSA_Tadawul_Market").strip()
SHEET_GLOBAL_MARKETS = (_get_first_env("SHEET_GLOBAL_MARKETS") or "Global_Markets").strip()
SHEET_MUTUAL_FUNDS = (_get_first_env("SHEET_MUTUAL_FUNDS") or "Mutual_Funds").strip()
SHEET_COMMODITIES_FX = (_get_first_env("SHEET_COMMODITIES_FX") or "Commodities_FX").strip()
SHEET_MARKET_LEADERS = (_get_first_env("SHEET_MARKET_LEADERS") or "Market_Leaders").strip()
SHEET_MY_PORTFOLIO = (_get_first_env("SHEET_MY_PORTFOLIO") or "My_Portfolio").strip()
SHEET_INSIGHTS_ANALYSIS = (_get_first_env("SHEET_INSIGHTS_ANALYSIS") or "Insights_Analysis").strip()
SHEET_INVESTMENT_ADVISOR = (_get_first_env("SHEET_INVESTMENT_ADVISOR") or "Investment_Advisor").strip()
SHEET_ECONOMIC_CALENDAR = (_get_first_env("SHEET_ECONOMIC_CALENDAR") or "Economic_Calendar").strip()
SHEET_INVESTMENT_INCOME = (_get_first_env("SHEET_INVESTMENT_INCOME") or "Investment_Income_Statement").strip()

# Logging flags
LOG_LEVEL = (_get_first_env("LOG_LEVEL") or "INFO").upper()
LOG_JSON = _safe_bool(_get_first_env("LOG_JSON"), False)
IS_PRODUCTION = str(APP_ENV).strip().lower() in {"prod", "production"}


# ---------------------------------------------------------------------
# settings compatibility object (very lightweight)
# ---------------------------------------------------------------------
class _Settings:
    app_name = APP_NAME
    env = APP_ENV
    version = APP_VERSION
    backend_base_url = BACKEND_BASE_URL
    base_url = BACKEND_BASE_URL
    app_token = APP_TOKEN or ""
    backup_app_token = BACKUP_APP_TOKEN or ""
    require_auth = REQUIRE_AUTH
    enabled_providers = ENABLED_PROVIDERS
    enabled_ksa_providers = KSA_PROVIDERS
    primary_provider = PRIMARY_PROVIDER
    http_timeout_sec = HTTP_TIMEOUT_SEC
    cache_ttl_sec = CACHE_TTL_SEC
    engine_cache_ttl_sec = ENGINE_CACHE_TTL_SEC
    enriched_batch_concurrency = ENRICHED_BATCH_CONCURRENCY
    google_credentials_dict = GOOGLE_SHEETS_CREDENTIALS
    default_spreadsheet_id = DEFAULT_SPREADSHEET_ID
    spreadsheet_id = DEFAULT_SPREADSHEET_ID
    has_google_sheets = bool(GOOGLE_SHEETS_CREDENTIALS) and bool(DEFAULT_SPREADSHEET_ID)


settings: object = _Settings()


def safe_env_summary() -> Dict[str, Any]:
    return {
        "app": APP_NAME,
        "env": APP_ENV,
        "version": APP_VERSION,
        "env_py_version": ENV_VERSION,
        "backend_base_url": BACKEND_BASE_URL,
        "providers": ENABLED_PROVIDERS,
        "ksa_providers": KSA_PROVIDERS,
        "primary": PRIMARY_PROVIDER,
        "require_auth": REQUIRE_AUTH,
        "cors_all": CORS_ALL_ORIGINS,
        "timeout_sec": HTTP_TIMEOUT_SEC,
        "cache_ttl_sec": CACHE_TTL_SEC,
        "engine_cache_ttl_sec": ENGINE_CACHE_TTL_SEC,
        "enriched_batch_concurrency": ENRICHED_BATCH_CONCURRENCY,
        "has_google_sheets": bool(GOOGLE_SHEETS_CREDENTIALS) and bool(DEFAULT_SPREADSHEET_ID),
        "default_spreadsheet_id_set": bool(DEFAULT_SPREADSHEET_ID),
        "apps_script_backup_url_set": bool(GOOGLE_APPS_SCRIPT_BACKUP_URL),
        "app_token_mask": _mask_tail(APP_TOKEN or "", keep=4),
        "keys_present": {
            "eodhd": bool(EODHD_API_KEY),
            "finnhub": bool(FINNHUB_API_KEY),
            "fmp": bool(FMP_API_KEY),
            "alpha_vantage": bool(ALPHA_VANTAGE_API_KEY),
            "argaam": bool(ARGAAM_API_KEY),
        },
    }


# Optional banner log (OFF by default)
if _truthy(os.getenv("ENV_LOG_ON_BOOT", "false")):
    try:
        logger.info("[env] App=%s | Env=%s | Version=%s | env.py=%s", APP_NAME, APP_ENV, APP_VERSION, ENV_VERSION)
        logger.info("[env] Providers=%s", _safe_join(ENABLED_PROVIDERS))
        logger.info("[env] KSA Providers=%s", _safe_join(KSA_PROVIDERS))
        logger.info(
            "[env] Sheets creds=%s | SpreadsheetId=%s",
            "SET" if bool(GOOGLE_SHEETS_CREDENTIALS) else "MISSING",
            "SET" if bool(DEFAULT_SPREADSHEET_ID) else "MISSING",
        )
    except Exception:
        pass


__all__ = [
    "settings",
    "safe_env_summary",
    "ENV_VERSION",
    "APP_NAME",
    "APP_ENV",
    "APP_VERSION",
    "BACKEND_BASE_URL",
    "APP_TOKEN",
    "BACKUP_APP_TOKEN",
    "REQUIRE_AUTH",
    "ENABLED_PROVIDERS",
    "KSA_PROVIDERS",
    "PRIMARY_PROVIDER",
    "PROVIDERS",
    "HTTP_TIMEOUT",
    "HTTP_TIMEOUT_SEC",
    "CACHE_TTL_SEC",
    "ENGINE_CACHE_TTL_SEC",
    "ENRICHED_BATCH_CONCURRENCY",
    "EODHD_API_KEY",
    "FINNHUB_API_KEY",
    "FMP_API_KEY",
    "ALPHA_VANTAGE_API_KEY",
    "ARGAAM_API_KEY",
    "ENABLE_CORS_ALL_ORIGINS",
    "CORS_ALL_ORIGINS",
    "CORS_ORIGINS",
    "CORS_ORIGINS_LIST",
    "GOOGLE_SHEETS_CREDENTIALS",
    "DEFAULT_SPREADSHEET_ID",
    "SPREADSHEET_ID",
    "GOOGLE_SHEETS_ID",
    "GOOGLE_APPS_SCRIPT_URL",
    "GOOGLE_APPS_SCRIPT_BACKUP_URL",
    "SHEET_KSA_TADAWUL",
    "SHEET_GLOBAL_MARKETS",
    "SHEET_MUTUAL_FUNDS",
    "SHEET_COMMODITIES_FX",
    "SHEET_MARKET_LEADERS",
    "SHEET_MY_PORTFOLIO",
    "SHEET_INSIGHTS_ANALYSIS",
    "SHEET_INVESTMENT_ADVISOR",
    "SHEET_ECONOMIC_CALENDAR",
    "SHEET_INVESTMENT_INCOME",
    "LOG_LEVEL",
    "LOG_JSON",
    "IS_PRODUCTION",
]
