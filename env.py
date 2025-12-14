"""
env.py
------------------------------------------------------------
Backward-compatible environment exports for Tadawul Fast Bridge (v4.6.0+)

✅ What this file does now (improved):
- Uses the single source of truth from root `config.py` (get_settings()).
- Exposes backward-compatible constants used across legacy modules.
- Prints a clean boot banner (providers + KSA providers) without leaking secrets.
- Keeps KSA-safe behavior (hard-block global providers for KSA in config.py).

Important:
- Do NOT duplicate parsing logic here anymore.
- If you need to change env var names/behavior, change `config.py`.
"""

from __future__ import annotations

import logging
import os
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)

# ------------------------------------------------------------
# Settings loader (single source of truth)
# ------------------------------------------------------------
_SETTINGS = None
_BANNER_PRINTED = False


def _mask(v: Optional[str]) -> Optional[str]:
    if not v:
        return None
    s = str(v)
    if len(s) <= 6:
        return "***"
    return s[:3] + "***" + s[-3:]


def _safe_join(items: List[str]) -> str:
    return ",".join([x for x in items if x])


def _load_settings():
    """
    Load settings from root config.py (preferred).
    If import fails for any reason, fall back to a minimal env-only view
    to avoid crashing the service.
    """
    global _SETTINGS
    if _SETTINGS is not None:
        return _SETTINGS

    try:
        from config import get_settings  # root config.py
        _SETTINGS = get_settings()
        return _SETTINGS
    except Exception as exc:
        logger.error("[env] Failed to import config.get_settings(): %s", exc)

        # Minimal fallback (very defensive)
        class _Fallback:
            app_name = os.getenv("APP_NAME", "Tadawul Fast Bridge")
            env = os.getenv("APP_ENV", "production")
            version = os.getenv("APP_VERSION", "4.6.0")
            log_level = (os.getenv("LOG_LEVEL", "info") or "info").lower()

            # Providers (best-effort)
            providers_list = [x.strip().lower() for x in (os.getenv("ENABLED_PROVIDERS") or os.getenv("PROVIDERS") or "eodhd,finnhub").split(",") if x.strip()]
            enabled_providers = providers_list
            enabled_ksa_providers = ["tadawul", "argaam"]

            primary_provider = (os.getenv("PRIMARY_PROVIDER") or "").strip().lower() or (enabled_providers[0] if enabled_providers else "eodhd")

            cors_all_origins = (os.getenv("ENABLE_CORS_ALL_ORIGINS") or os.getenv("CORS_ALL_ORIGINS") or "true").strip().lower() in {"1", "true", "yes", "on"}
            cors_origins = os.getenv("CORS_ORIGINS")
            cors_origins_list = ["*"] if cors_all_origins else ([x.strip() for x in (cors_origins or "").split(",") if x.strip()] if cors_origins else [])

            http_timeout_sec = float(os.getenv("HTTP_TIMEOUT_SEC") or 25)
            cache_ttl_sec = int(os.getenv("CACHE_TTL_SEC") or 20)
            quote_ttl_sec = int(os.getenv("QUOTE_TTL_SEC") or 30)
            fundamentals_ttl_sec = int(os.getenv("FUNDAMENTALS_TTL_SEC") or 21600)
            argaam_snapshot_ttl_sec = int(os.getenv("ARGAAM_SNAPSHOT_TTL_SEC") or 30)

            app_token = os.getenv("APP_TOKEN")
            backup_app_token = os.getenv("BACKUP_APP_TOKEN")

            eodhd_api_key = os.getenv("EODHD_API_KEY")
            finnhub_api_key = os.getenv("FINNHUB_API_KEY")
            fmp_api_key = os.getenv("FMP_API_KEY")
            alpha_vantage_api_key = os.getenv("ALPHA_VANTAGE_API_KEY")
            argaam_api_key = os.getenv("ARGAAM_API_KEY")

            default_spreadsheet_id = os.getenv("DEFAULT_SPREADSHEET_ID") or os.getenv("GOOGLE_SHEET_ID")
            google_sheet_id = os.getenv("GOOGLE_SHEET_ID")
            google_sheet_range = os.getenv("GOOGLE_SHEET_RANGE")
            google_credentials_dict = None
            has_google_sheets = False

            # Sheets names
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
    _BANNER_PRINTED = True

    try:
        logger.info("[env] App=%s | Env=%s | Version=%s", getattr(s, "app_name", ""), getattr(s, "env", ""), getattr(s, "version", ""))
        logger.info("[env] Providers=%s", _safe_join(list(getattr(s, "enabled_providers", []))))
        logger.info("[env] KSA Providers=%s", _safe_join(list(getattr(s, "enabled_ksa_providers", []))))
    except Exception:
        # Never block app startup
        pass


settings = _load_settings()
_print_banner_once(settings)

# ------------------------------------------------------------
# Backward compatible exports (DON'T BREAK IMPORTS)
# ------------------------------------------------------------

# App identity
APP_NAME = getattr(settings, "app_name", "Tadawul Fast Bridge")
APP_ENV = getattr(settings, "env", "production")
APP_VERSION = getattr(settings, "version", "4.6.0")

# Base URL
BACKEND_BASE_URL = getattr(settings, "backend_base_url", None) or os.getenv("BACKEND_BASE_URL", "https://tadawul-fast-bridge.onrender.com").rstrip("/")

# Tokens
APP_TOKEN = getattr(settings, "app_token", None)
BACKUP_APP_TOKEN = getattr(settings, "backup_app_token", None)

# Providers
ENABLED_PROVIDERS: List[str] = list(getattr(settings, "enabled_providers", []))
KSA_PROVIDERS: List[str] = list(getattr(settings, "enabled_ksa_providers", []))
PRIMARY_PROVIDER = (getattr(settings, "primary_provider", None) or getattr(settings, "primary", None) or (ENABLED_PROVIDERS[0] if ENABLED_PROVIDERS else "eodhd")).strip().lower()

# A CSV alias some modules still expect
PROVIDERS = _safe_join(ENABLED_PROVIDERS)

# Timeouts / TTL
HTTP_TIMEOUT_SEC = float(getattr(settings, "http_timeout_sec", 25.0))
HTTP_TIMEOUT = int(HTTP_TIMEOUT_SEC)

CACHE_TTL_SEC = float(getattr(settings, "cache_ttl_sec", 20))
QUOTE_TTL_SEC = float(getattr(settings, "quote_ttl_sec", 30))
FUNDAMENTALS_TTL_SEC = float(getattr(settings, "fundamentals_ttl_sec", 21600))
ARGAAM_SNAPSHOT_TTL_SEC = float(getattr(settings, "argaam_snapshot_ttl_sec", 30))

# Provider keys
EODHD_API_KEY = getattr(settings, "eodhd_api_key", None)
FINNHUB_API_KEY = getattr(settings, "finnhub_api_key", None)
FMP_API_KEY = getattr(settings, "fmp_api_key", None)
ALPHA_VANTAGE_API_KEY = getattr(settings, "alpha_vantage_api_key", None)
ARGAAM_API_KEY = getattr(settings, "argaam_api_key", None)

# CORS
ENABLE_CORS_ALL_ORIGINS = bool(getattr(settings, "cors_all_origins", True))
CORS_ALL_ORIGINS = ENABLE_CORS_ALL_ORIGINS
CORS_ORIGINS = getattr(settings, "cors_origins", None)
CORS_ORIGINS_LIST = list(getattr(settings, "cors_origins_list", ["*"] if ENABLE_CORS_ALL_ORIGINS else []))

# Google Sheets
GOOGLE_SHEETS_CREDENTIALS: Optional[Dict[str, Any]] = getattr(settings, "google_credentials_dict", None)
DEFAULT_SPREADSHEET_ID = getattr(settings, "default_spreadsheet_id", None)
GOOGLE_SHEET_ID = getattr(settings, "google_sheet_id", None)
GOOGLE_SHEET_RANGE = getattr(settings, "google_sheet_range", None)
HAS_GOOGLE_SHEETS = bool(getattr(settings, "has_google_sheets", False))

# Sheet names (Apps Script alignment)
SHEET_KSA_TADAWUL = getattr(settings, "sheet_ksa_tadawul", "KSA_Tadawul_Market")
SHEET_GLOBAL_MARKETS = getattr(settings, "sheet_global_markets", "Global_Markets")
SHEET_MUTUAL_FUNDS = getattr(settings, "sheet_mutual_funds", "Mutual_Funds")
SHEET_COMMODITIES_FX = getattr(settings, "sheet_commodities_fx", "Commodities_FX")
SHEET_MARKET_LEADERS = getattr(settings, "sheet_market_leaders", "Market_Leaders")
SHEET_MY_PORTFOLIO = getattr(settings, "sheet_my_portfolio", "My_Portfolio")
SHEET_INSIGHTS_ANALYSIS = getattr(settings, "sheet_insights_analysis", "Insights_Analysis")
SHEET_INVESTMENT_ADVISOR = getattr(settings, "sheet_investment_advisor", "Investment_Advisor")
SHEET_ECONOMIC_CALENDAR = getattr(settings, "sheet_economic_calendar", "Economic_Calendar")
SHEET_INVESTMENT_INCOME = getattr(settings, "sheet_investment_income", "Investment_Income_Statement")

# Logging
LOG_LEVEL = (getattr(settings, "log_level", "info") or "info").upper()
LOG_JSON = bool(getattr(settings, "log_json", False))
IS_PRODUCTION = str(APP_ENV).strip().lower() in {"prod", "production"}

# Convenience: safe summary (optional)
def safe_env_summary() -> Dict[str, Any]:
    return {
        "app": APP_NAME,
        "env": APP_ENV,
        "version": APP_VERSION,
        "providers": ENABLED_PROVIDERS,
        "ksa_providers": KSA_PROVIDERS,
        "primary": PRIMARY_PROVIDER,
        "cors_all": CORS_ALL_ORIGINS,
        "timeout_sec": HTTP_TIMEOUT_SEC,
        "cache_ttl_sec": CACHE_TTL_SEC,
        "has_google_sheets": HAS_GOOGLE_SHEETS,
        "app_token": _mask(APP_TOKEN),
    }


__all__ = [
    "settings",
    "safe_env_summary",
    # Back-compat exports
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
