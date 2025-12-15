"""
env.py
------------------------------------------------------------
Backward-compatible environment exports for Tadawul Fast Bridge (v4.7.0+)

✅ What this file does:
- Uses SINGLE source of truth from config/get_settings() when available.
- Exposes backward-compatible constants used across legacy modules.
- Prints a clean boot banner (providers + KSA providers) without leaking secrets.
- Keeps KSA-safe behavior (actual routing/blocks must remain in config.py / engines).

Important:
- Do NOT duplicate parsing logic here in normal mode.
- Fallback mode exists only to avoid crashing if config import fails.
"""

from __future__ import annotations

import json
import logging
import os
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)

_SETTINGS = None
_BANNER_PRINTED = False


# ------------------------------------------------------------
# Small helpers (safe)
# ------------------------------------------------------------
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
    if s in {"1", "true", "yes", "on", "y", "t"}:
        return True
    if s in {"0", "false", "no", "off", "n", "f"}:
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
    (Normal mode parsing should live in config.py.)
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


# ------------------------------------------------------------
# Settings loader (single source of truth)
# ------------------------------------------------------------
def _load_settings():
    """
    Load settings from config.py (preferred).
    Tries core.config first, then root config.

    If BOTH fail for any reason, fall back to a minimal env-only view
    to avoid crashing the service.
    """
    global _SETTINGS
    if _SETTINGS is not None:
        return _SETTINGS

    # Preferred: core.config (matches imports used by core modules)
    try:
        from core.config import get_settings  # type: ignore
        _SETTINGS = get_settings()
        return _SETTINGS
    except Exception as exc:
        logger.error("[env] Failed to import core.config.get_settings(): %s", exc)

    # Secondary: root config.py (older layouts)
    try:
        from config import get_settings  # type: ignore
        _SETTINGS = get_settings()
        return _SETTINGS
    except Exception as exc:
        logger.error("[env] Failed to import config.get_settings(): %s", exc)

    # -------------------------
    # Minimal fallback (defensive)
    # -------------------------
    class _Fallback:
        # App identity
        app_name = os.getenv("APP_NAME", "Tadawul Fast Bridge")
        env = os.getenv("APP_ENV", "production")
        version = os.getenv("APP_VERSION", "4.7.0")
        log_level = (os.getenv("LOG_LEVEL", "info") or "info").lower()
        log_json = _safe_bool(os.getenv("LOG_JSON"), False)

        # Providers (best-effort)
        _prov_csv = os.getenv("ENABLED_PROVIDERS") or os.getenv("PROVIDERS") or "finnhub,fmp,eodhd"
        providers_list = [x.strip().lower() for x in _prov_csv.split(",") if x.strip()]
        enabled_providers = providers_list
        enabled_ksa_providers = ["tadawul", "argaam"]

        primary_provider = (
            (os.getenv("PRIMARY_PROVIDER") or "").strip().lower()
            or (enabled_providers[0] if enabled_providers else "finnhub")
        )

        cors_all_origins = _safe_bool(
            os.getenv("ENABLE_CORS_ALL_ORIGINS") or os.getenv("CORS_ALL_ORIGINS") or "true",
            True,
        )
        cors_origins = os.getenv("CORS_ORIGINS")
        cors_origins_list = ["*"] if cors_all_origins else (
            [x.strip() for x in (cors_origins or "").split(",") if x.strip()] if cors_origins else []
        )

        # Timeouts / TTLs
        http_timeout_sec = _safe_float(os.getenv("HTTP_TIMEOUT_SEC") or os.getenv("HTTP_TIMEOUT"), 25.0)
        cache_ttl_sec = _safe_float(os.getenv("CACHE_TTL_SEC"), 20.0)
        quote_ttl_sec = _safe_float(os.getenv("QUOTE_TTL_SEC"), 30.0)
        fundamentals_ttl_sec = _safe_float(os.getenv("FUNDAMENTALS_TTL_SEC"), 21600.0)
        argaam_snapshot_ttl_sec = _safe_float(os.getenv("ARGAAM_SNAPSHOT_TTL_SEC"), 30.0)

        # Tokens
        app_token = os.getenv("APP_TOKEN")
        backup_app_token = os.getenv("BACKUP_APP_TOKEN")

        # Provider keys
        eodhd_api_key = os.getenv("EODHD_API_KEY")
        finnhub_api_key = os.getenv("FINNHUB_API_KEY")
        fmp_api_key = os.getenv("FMP_API_KEY")
        alpha_vantage_api_key = os.getenv("ALPHA_VANTAGE_API_KEY")
        argaam_api_key = os.getenv("ARGAAM_API_KEY")

        # Backend base URL
        backend_base_url = (os.getenv("BACKEND_BASE_URL") or "https://tadawul-fast-bridge.onrender.com").rstrip("/")

        # Google Sheets (fallback parses only if config is unavailable)
        google_credentials_dict = _try_parse_json_dict(os.getenv("GOOGLE_SHEETS_CREDENTIALS"))
        default_spreadsheet_id = os.getenv("DEFAULT_SPREADSHEET_ID") or os.getenv("GOOGLE_SHEET_ID")
        google_sheet_id = os.getenv("GOOGLE_SHEET_ID")
        google_sheet_range = os.getenv("GOOGLE_SHEET_RANGE")
        has_google_sheets = bool(google_credentials_dict) and bool(default_spreadsheet_id or google_sheet_id)

        # Sheet names
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
        logger.info("[env] App=%s | Env=%s | Version=%s",
                    getattr(s, "app_name", ""), getattr(s, "env", ""), getattr(s, "version", ""))
        logger.info("[env] Providers=%s", _safe_join(list(getattr(s, "enabled_providers", []))))
        logger.info("[env] KSA Providers=%s", _safe_join(list(getattr(s, "enabled_ksa_providers", []))))
    except Exception:
        pass


settings = _load_settings()
_print_banner_once(settings)


# ------------------------------------------------------------
# Backward compatible exports (DON'T BREAK IMPORTS)
# ------------------------------------------------------------

# App identity
APP_NAME = getattr(settings, "app_name", "Tadawul Fast Bridge")
APP_ENV = getattr(settings, "env", "production")
APP_VERSION = getattr(settings, "version", "4.7.0")

# Base URL
BACKEND_BASE_URL = (getattr(settings, "backend_base_url", None) or os.getenv("BACKEND_BASE_URL") or "https://tadawul-fast-bridge.onrender.com").rstrip("/")

# Tokens
APP_TOKEN = getattr(settings, "app_token", None)
BACKUP_APP_TOKEN = getattr(settings, "backup_app_token", None)

# Providers
ENABLED_PROVIDERS: List[str] = list(getattr(settings, "enabled_providers", []))
KSA_PROVIDERS: List[str] = list(getattr(settings, "enabled_ksa_providers", []))
PRIMARY_PROVIDER = (
    getattr(settings, "primary_provider", None)
    or getattr(settings, "primary", None)
    or (ENABLED_PROVIDERS[0] if ENABLED_PROVIDERS else "finnhub")
)
PRIMARY_PROVIDER = str(PRIMARY_PROVIDER).strip().lower()

# CSV alias some modules still expect
PROVIDERS = _safe_join(ENABLED_PROVIDERS)

# Timeouts / TTL
HTTP_TIMEOUT_SEC = float(getattr(settings, "http_timeout_sec", 25.0))
HTTP_TIMEOUT = int(HTTP_TIMEOUT_SEC)

CACHE_TTL_SEC = float(getattr(settings, "cache_ttl_sec", 20.0))
QUOTE_TTL_SEC = float(getattr(settings, "quote_ttl_sec", 30.0))
FUNDAMENTALS_TTL_SEC = float(getattr(settings, "fundamentals_ttl_sec", 21600.0))
ARGAAM_SNAPSHOT_TTL_SEC = float(getattr(settings, "argaam_snapshot_ttl_sec", 30.0))

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


# Convenience: safe summary (no secrets)
def safe_env_summary() -> Dict[str, Any]:
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
