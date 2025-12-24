# env.py  (FULL REPLACEMENT)
"""
env.py
------------------------------------------------------------
Backward-compatible environment exports for Tadawul Fast Bridge (v4.7.1+)

✅ What this file does:
- Uses SINGLE source of truth from config/get_settings() when available.
- Exposes backward-compatible constants used across legacy modules.
- Prints a clean boot banner (providers + KSA providers) WITHOUT leaking secrets.
- Keeps KSA-safe behavior (routing/blocks must remain in config.py / engines).

Important:
- Do NOT duplicate parsing logic here in normal mode.
- Fallback mode exists only to avoid crashing if config import fails.
"""

from __future__ import annotations

import json
import logging
import os
from typing import Any, Dict, List, Optional

logger = logging.getLogger("env")

_SETTINGS: Optional[object] = None
_BANNER_PRINTED = False
_WARNED_SETTINGS_IMPORT = False


# ------------------------------------------------------------
# Small helpers (safe)
# ------------------------------------------------------------
_TRUTHY = {"1", "true", "yes", "on", "y", "t"}
_FALSY = {"0", "false", "no", "off", "n", "f"}


def _mask(v: Optional[str]) -> Optional[str]:
    if not v:
        return None
    s = str(v)
    if len(s) <= 6:
        return "***"
    return s[:3] + "***" + s[-3:]


def _safe_join(items: List[str]) -> str:
    return ",".join([str(x) for x in items if x])


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


def _try_parse_json_dict(raw: Any) -> Optional[Dict[str, Any]]:
    """
    Fallback-only helper for GOOGLE_SHEETS_CREDENTIALS-like env JSON.
    """
    if raw is None:
        return None
    if isinstance(raw, dict):
        return raw
    if not isinstance(raw, str):
        return None
    s = raw.strip()
    if not s:
        return None
    try:
        obj = json.loads(s)
        return obj if isinstance(obj, dict) else None
    except Exception:
        return None


def _get_attr_any(obj: Any, names: List[str], default: Any = None) -> Any:
    for n in names:
        try:
            if hasattr(obj, n):
                v = getattr(obj, n)
                if v is not None:
                    return v
        except Exception:
            pass
    return default


def _normalize_version(v: Any) -> str:
    s = str(v or "").strip()
    if not s:
        return ""
    if s.lower() in {"unknown", "none", "null"}:
        return ""
    return s


def _resolve_version(settings_obj: Any) -> str:
    # settings.version -> env(APP_VERSION) -> render commit -> dev
    v = _normalize_version(_get_attr_any(settings_obj, ["version", "app_version", "APP_VERSION"], ""))
    if not v:
        v = _normalize_version(os.getenv("APP_VERSION", ""))
    if not v:
        commit = (os.getenv("RENDER_GIT_COMMIT") or os.getenv("GIT_COMMIT") or "").strip()
        if commit:
            v = commit[:7]
    return v or "dev"


def _as_list_lower(v: Any) -> List[str]:
    if v is None:
        return []
    if isinstance(v, list):
        return [str(x).strip().lower() for x in v if str(x).strip()]
    s = str(v).strip()
    if not s:
        return []
    # JSON list string?
    if s.startswith("[") and s.endswith("]"):
        try:
            arr = json.loads(s)
            if isinstance(arr, list):
                return [str(x).strip().lower() for x in arr if str(x).strip()]
        except Exception:
            pass
    return [x.strip().lower() for x in s.split(",") if x.strip()]


# ------------------------------------------------------------
# Settings loader (single source of truth)
# ------------------------------------------------------------
def _load_settings():
    """
    Load settings from config.py (preferred).
    Tries core.config first, then root config.
    If BOTH fail, fall back to a minimal env-only view.
    """
    global _SETTINGS, _WARNED_SETTINGS_IMPORT
    if _SETTINGS is not None:
        return _SETTINGS

    # Preferred: core.config
    try:
        from core.config import get_settings  # type: ignore

        _SETTINGS = get_settings()
        return _SETTINGS
    except Exception as exc:
        if not _WARNED_SETTINGS_IMPORT:
            _WARNED_SETTINGS_IMPORT = True
            logger.warning("[env] Cannot import core.config.get_settings(): %s", exc)

    # Secondary: root config.py
    try:
        from config import get_settings  # type: ignore

        _SETTINGS = get_settings()
        return _SETTINGS
    except Exception as exc:
        if not _WARNED_SETTINGS_IMPORT:
            _WARNED_SETTINGS_IMPORT = True
            logger.warning("[env] Cannot import config.get_settings(): %s", exc)

    # Minimal fallback (ONLY if settings import fails)
    class _Fallback:
        app_name = os.getenv("APP_NAME", "Tadawul Fast Bridge")
        env = os.getenv("APP_ENV", "production")
        version = os.getenv("APP_VERSION", "4.7.1")
        log_level = (os.getenv("LOG_LEVEL", "INFO") or "INFO").upper()
        log_json = _safe_bool(os.getenv("LOG_JSON"), False)

        _prov_csv = os.getenv("ENABLED_PROVIDERS") or os.getenv("PROVIDERS") or "finnhub,fmp,eodhd"
        enabled_providers = [x.strip().lower() for x in _prov_csv.split(",") if x.strip()]
        enabled_ksa_providers = [x.strip().lower() for x in (os.getenv("KSA_PROVIDERS", "tadawul,argaam") or "").split(",") if x.strip()]

        primary_provider = (
            (os.getenv("PRIMARY_PROVIDER") or "").strip().lower()
            or (enabled_providers[0] if enabled_providers else "finnhub")
        )

        cors_all_origins = _safe_bool(os.getenv("ENABLE_CORS_ALL_ORIGINS") or os.getenv("CORS_ALL_ORIGINS") or "true", True)
        cors_origins = os.getenv("CORS_ORIGINS")
        cors_origins_list = ["*"] if cors_all_origins else (
            [x.strip() for x in (cors_origins or "").split(",") if x.strip()] if cors_origins else []
        )

        http_timeout_sec = _safe_float(os.getenv("HTTP_TIMEOUT_SEC") or os.getenv("HTTP_TIMEOUT"), 25.0)
        cache_ttl_sec = _safe_float(os.getenv("CACHE_TTL_SEC"), 20.0)
        quote_ttl_sec = _safe_float(os.getenv("QUOTE_TTL_SEC"), 30.0)
        fundamentals_ttl_sec = _safe_float(os.getenv("FUNDAMENTALS_TTL_SEC"), 21600.0)
        argaam_snapshot_ttl_sec = _safe_float(os.getenv("ARGAAM_SNAPSHOT_TTL_SEC"), 30.0)

        app_token = os.getenv("APP_TOKEN")
        backup_app_token = os.getenv("BACKUP_APP_TOKEN")

        eodhd_api_key = os.getenv("EODHD_API_KEY")
        finnhub_api_key = os.getenv("FINNHUB_API_KEY")
        fmp_api_key = os.getenv("FMP_API_KEY")
        alpha_vantage_api_key = os.getenv("ALPHA_VANTAGE_API_KEY")
        argaam_api_key = os.getenv("ARGAAM_API_KEY")

        backend_base_url = (os.getenv("BACKEND_BASE_URL") or "https://tadawul-fast-bridge.onrender.com").rstrip("/")

        google_credentials_dict = _try_parse_json_dict(os.getenv("GOOGLE_SHEETS_CREDENTIALS"))
        default_spreadsheet_id = os.getenv("DEFAULT_SPREADSHEET_ID") or os.getenv("GOOGLE_SHEET_ID")
        google_sheet_id = os.getenv("GOOGLE_SHEET_ID")
        google_sheet_range = os.getenv("GOOGLE_SHEET_RANGE")
        has_google_sheets = bool(google_credentials_dict) and bool(default_spreadsheet_id or google_sheet_id)

        sheet_ksa_tadawul = os.getenv("SHEET_KSA_TADAWUL", "KSA_Tadawul_Market")
        sheet_global_markets = os.getenv("SHEET_GLOBAL_MARKETS", "Global_Markets")
        sheet_mutual_funds = os.getenv("SHEET_MUTUAL_FUNDS", "Mutual_Funds")
        sheet_commodities_fx = os.getenv("SHEET_COMMODITIES_FX", "Commodities_FX")
        sheet_market_leaders = os.getenv("SHEET_MARKET_LEADERS", "Market_Leaders")
        sheet_my_portfolio = os.getenv("SHEET_MY_PORTFOLIO", "My_Portfolio")
        sheet_insights_analysis = os.getenv("SHEET_INSIGHTS_ANALYSIS", "Insights_Analysis")
        sheet_investment_advisor = os.getenv("SHEET_INVESTMENT_ADVISOR", "Investment_Advisor")
        sheet_economic_calendar = os.getenv("SHEET_ECONOMIC_CALENDAR", "Economic_Calendar")
        sheet_investment_income = os.getenv("SHEET_INVESTMENT_INCOME", "Investment_Income_Statement")

    _SETTINGS = _Fallback()
    return _SETTINGS


def _print_banner_once(s) -> None:
    global _BANNER_PRINTED
    if _BANNER_PRINTED:
        return

    # Optional kill switch
    if _safe_bool(os.getenv("DISABLE_ENV_BANNER"), False):
        _BANNER_PRINTED = True
        return

    _BANNER_PRINTED = True

    try:
        app_name = _get_attr_any(s, ["app_name", "APP_NAME"], "Tadawul Fast Bridge")
        env_name = _get_attr_any(s, ["env", "environment", "APP_ENV"], "production")
        ver = _resolve_version(s)

        providers = list(_get_attr_any(s, ["enabled_providers"], []) or [])
        if not providers:
            providers = _as_list_lower(os.getenv("ENABLED_PROVIDERS") or os.getenv("PROVIDERS"))

        ksa_providers = list(_get_attr_any(s, ["enabled_ksa_providers"], []) or [])
        if not ksa_providers:
            ksa_providers = _as_list_lower(os.getenv("KSA_PROVIDERS"))

        logger.info("[env] App=%s | Env=%s | Version=%s", app_name, env_name, ver)
        logger.info("[env] Providers=%s", _safe_join([str(x) for x in providers]))
        logger.info("[env] KSA Providers=%s", _safe_join([str(x) for x in ksa_providers]))
    except Exception:
        pass


settings = _load_settings()
_print_banner_once(settings)


# ------------------------------------------------------------
# Backward compatible exports (DON'T BREAK IMPORTS)
# ------------------------------------------------------------
APP_NAME = _get_attr_any(settings, ["app_name", "APP_NAME"], "Tadawul Fast Bridge")
APP_ENV = _get_attr_any(settings, ["env", "environment", "APP_ENV"], "production")
APP_VERSION = _resolve_version(settings)

BACKEND_BASE_URL = str(
    _get_attr_any(
        settings,
        ["backend_base_url", "BACKEND_BASE_URL"],
        os.getenv("BACKEND_BASE_URL", "https://tadawul-fast-bridge.onrender.com"),
    )
).rstrip("/")

APP_TOKEN = _get_attr_any(settings, ["app_token", "APP_TOKEN"], None)
BACKUP_APP_TOKEN = _get_attr_any(settings, ["backup_app_token", "BACKUP_APP_TOKEN"], None)

# Providers
ENABLED_PROVIDERS: List[str] = list(_get_attr_any(settings, ["enabled_providers"], []) or [])
if not ENABLED_PROVIDERS:
    ENABLED_PROVIDERS = _as_list_lower(os.getenv("ENABLED_PROVIDERS") or os.getenv("PROVIDERS"))

KSA_PROVIDERS: List[str] = list(_get_attr_any(settings, ["enabled_ksa_providers"], []) or [])
if not KSA_PROVIDERS:
    KSA_PROVIDERS = _as_list_lower(os.getenv("KSA_PROVIDERS"))

PRIMARY_PROVIDER = (
    _get_attr_any(settings, ["primary_provider", "primary", "PRIMARY_PROVIDER"], None)
    or (ENABLED_PROVIDERS[0] if ENABLED_PROVIDERS else "finnhub")
)
PRIMARY_PROVIDER = str(PRIMARY_PROVIDER).strip().lower()

PROVIDERS = _safe_join(ENABLED_PROVIDERS)

# Timeouts / TTLs
HTTP_TIMEOUT_SEC = float(_get_attr_any(settings, ["http_timeout_sec", "HTTP_TIMEOUT_SEC"], os.getenv("HTTP_TIMEOUT_SEC") or 25.0))
HTTP_TIMEOUT = _safe_int(_get_attr_any(settings, ["http_timeout", "HTTP_TIMEOUT"], int(HTTP_TIMEOUT_SEC)), int(HTTP_TIMEOUT_SEC))

CACHE_TTL_SEC = float(_get_attr_any(settings, ["cache_ttl_sec", "CACHE_TTL_SEC"], os.getenv("CACHE_TTL_SEC") or 20.0))
QUOTE_TTL_SEC = float(_get_attr_any(settings, ["quote_ttl_sec", "QUOTE_TTL_SEC"], os.getenv("QUOTE_TTL_SEC") or 30.0))
FUNDAMENTALS_TTL_SEC = float(_get_attr_any(settings, ["fundamentals_ttl_sec", "FUNDAMENTALS_TTL_SEC"], os.getenv("FUNDAMENTALS_TTL_SEC") or 21600.0))
ARGAAM_SNAPSHOT_TTL_SEC = float(_get_attr_any(settings, ["argaam_snapshot_ttl_sec", "ARGAAM_SNAPSHOT_TTL_SEC"], os.getenv("ARGAAM_SNAPSHOT_TTL_SEC") or 30.0))

# API keys (do NOT log these)
EODHD_API_KEY = _get_attr_any(settings, ["eodhd_api_key", "EODHD_API_KEY"], os.getenv("EODHD_API_KEY"))
FINNHUB_API_KEY = _get_attr_any(settings, ["finnhub_api_key", "FINNHUB_API_KEY"], os.getenv("FINNHUB_API_KEY"))
FMP_API_KEY = _get_attr_any(settings, ["fmp_api_key", "FMP_API_KEY"], os.getenv("FMP_API_KEY"))
ALPHA_VANTAGE_API_KEY = _get_attr_any(settings, ["alpha_vantage_api_key", "ALPHA_VANTAGE_API_KEY"], os.getenv("ALPHA_VANTAGE_API_KEY"))
ARGAAM_API_KEY = _get_attr_any(settings, ["argaam_api_key", "ARGAAM_API_KEY"], os.getenv("ARGAAM_API_KEY"))

# CORS
ENABLE_CORS_ALL_ORIGINS = bool(_get_attr_any(settings, ["cors_all_origins", "ENABLE_CORS_ALL_ORIGINS"], _safe_bool(os.getenv("ENABLE_CORS_ALL_ORIGINS") or os.getenv("CORS_ALL_ORIGINS"), True)))
CORS_ALL_ORIGINS = ENABLE_CORS_ALL_ORIGINS
CORS_ORIGINS = _get_attr_any(settings, ["cors_origins", "CORS_ORIGINS"], os.getenv("CORS_ORIGINS"))
CORS_ORIGINS_LIST = list(_get_attr_any(settings, ["cors_origins_list", "CORS_ORIGINS_LIST"], ["*"] if ENABLE_CORS_ALL_ORIGINS else []))

# Google Sheets
GOOGLE_SHEETS_CREDENTIALS: Optional[Dict[str, Any]] = _get_attr_any(settings, ["google_credentials_dict", "google_sheets_credentials"], None)
if GOOGLE_SHEETS_CREDENTIALS is None:
    # fallback-only: allow env json
    GOOGLE_SHEETS_CREDENTIALS = _try_parse_json_dict(os.getenv("GOOGLE_SHEETS_CREDENTIALS"))

DEFAULT_SPREADSHEET_ID = _get_attr_any(settings, ["default_spreadsheet_id", "DEFAULT_SPREADSHEET_ID"], os.getenv("DEFAULT_SPREADSHEET_ID"))
GOOGLE_SHEET_ID = _get_attr_any(settings, ["google_sheet_id", "GOOGLE_SHEET_ID"], os.getenv("GOOGLE_SHEET_ID"))
GOOGLE_SHEET_RANGE = _get_attr_any(settings, ["google_sheet_range", "GOOGLE_SHEET_RANGE"], os.getenv("GOOGLE_SHEET_RANGE"))

HAS_GOOGLE_SHEETS = bool(
    _get_attr_any(
        settings,
        ["has_google_sheets", "HAS_GOOGLE_SHEETS"],
        bool(GOOGLE_SHEETS_CREDENTIALS) and bool(DEFAULT_SPREADSHEET_ID or GOOGLE_SHEET_ID),
    )
)

# Sheet names
SHEET_KSA_TADAWUL = _get_attr_any(settings, ["sheet_ksa_tadawul", "SHEET_KSA_TADAWUL"], "KSA_Tadawul_Market")
SHEET_GLOBAL_MARKETS = _get_attr_any(settings, ["sheet_global_markets", "SHEET_GLOBAL_MARKETS"], "Global_Markets")
SHEET_MUTUAL_FUNDS = _get_attr_any(settings, ["sheet_mutual_funds", "SHEET_MUTUAL_FUNDS"], "Mutual_Funds")
SHEET_COMMODITIES_FX = _get_attr_any(settings, ["sheet_commodities_fx", "SHEET_COMMODITIES_FX"], "Commodities_FX")
SHEET_MARKET_LEADERS = _get_attr_any(settings, ["sheet_market_leaders", "SHEET_MARKET_LEADERS"], "Market_Leaders")
SHEET_MY_PORTFOLIO = _get_attr_any(settings, ["sheet_my_portfolio", "SHEET_MY_PORTFOLIO"], "My_Portfolio")
SHEET_INSIGHTS_ANALYSIS = _get_attr_any(settings, ["sheet_insights_analysis", "SHEET_INSIGHTS_ANALYSIS"], "Insights_Analysis")
SHEET_INVESTMENT_ADVISOR = _get_attr_any(settings, ["sheet_investment_advisor", "SHEET_INVESTMENT_ADVISOR"], "Investment_Advisor")
SHEET_ECONOMIC_CALENDAR = _get_attr_any(settings, ["sheet_economic_calendar", "SHEET_ECONOMIC_CALENDAR"], "Economic_Calendar")
SHEET_INVESTMENT_INCOME = _get_attr_any(settings, ["sheet_investment_income", "SHEET_INVESTMENT_INCOME"], "Investment_Income_Statement")

# Logging flags
LOG_LEVEL = (str(_get_attr_any(settings, ["log_level", "LOG_LEVEL"], os.getenv("LOG_LEVEL", "INFO"))) or "INFO").upper()
LOG_JSON = bool(_get_attr_any(settings, ["log_json", "LOG_JSON"], _safe_bool(os.getenv("LOG_JSON"), False)))
IS_PRODUCTION = str(APP_ENV).strip().lower() in {"prod", "production"}


def safe_env_summary() -> Dict[str, Any]:
    """
    Safe summary for /system/settings (no secrets).
    """
    return {
        "app": APP_NAME,
        "env": APP_ENV,
        "version": APP_VERSION,
        "backend_base_url": BACKEND_BASE_URL,
        "providers": ENABLED_PROVIDERS,
        "ksa_providers": KSA_PROVIDERS,
        "primary": PRIMARY_PROVIDER,
        "cors_all": CORS_ALL_ORIGINS,
        "timeout_sec": HTTP_TIMEOUT_SEC,
        "cache_ttl_sec": CACHE_TTL_SEC,
        "has_google_sheets": HAS_GOOGLE_SHEETS,
        "default_spreadsheet_id_set": bool(DEFAULT_SPREADSHEET_ID),
        "google_sheet_id_set": bool(GOOGLE_SHEET_ID),
        "app_token": _mask(APP_TOKEN),
        # show presence only (not values)
        "keys_present": {
            "eodhd": bool(EODHD_API_KEY),
            "finnhub": bool(FINNHUB_API_KEY),
            "fmp": bool(FMP_API_KEY),
            "alpha_vantage": bool(ALPHA_VANTAGE_API_KEY),
            "argaam": bool(ARGAAM_API_KEY),
        },
    }


__all__ = [
    "settings",
    "safe_env_summary",
    "APP_NAME",
    "APP_ENV",
    "APP_VERSION",
    "BACKEND_BASE_URL",
    "APP_TOKEN",
    "BACKUP_APP_TOKEN",
    "ENABLED_PROVIDERS",
    "KSA_PROVIDERS",
    "PRIMARY_PROVIDER",
    "PROVIDERS",
    "HTTP_TIMEOUT",
    "HTTP_TIMEOUT_SEC",
    "CACHE_TTL_SEC",
    "QUOTE_TTL_SEC",
    "FUNDAMENTALS_TTL_SEC",
    "ARGAAM_SNAPSHOT_TTL_SEC",
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
    "GOOGLE_SHEET_ID",
    "GOOGLE_SHEET_RANGE",
    "HAS_GOOGLE_SHEETS",
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
