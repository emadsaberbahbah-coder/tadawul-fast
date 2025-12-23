# env.py  (FULL REPLACEMENT)
"""
env.py
------------------------------------------------------------
Backward-compatible environment exports for Tadawul Fast Bridge (v4.7.1)

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


# ------------------------------------------------------------
# Settings loader (single source of truth)
# ------------------------------------------------------------
def _load_settings():
    """
    Load settings from config.py (preferred).
    Tries core.config first, then root config.
    If BOTH fail, fall back to a minimal env-only view.
    """
    global _SETTINGS
    if _SETTINGS is not None:
        return _SETTINGS

    # Preferred: core.config
    try:
        from core.config import get_settings  # type: ignore
        _SETTINGS = get_settings()
        return _SETTINGS
    except Exception as exc:
        logger.error("[env] Failed to import core.config.get_settings(): %s", exc)

    # Secondary: root config.py
    try:
        from config import get_settings  # type: ignore
        _SETTINGS = get_settings()
        return _SETTINGS
    except Exception as exc:
        logger.error("[env] Failed to import config.get_settings(): %s", exc)

    # Minimal fallback
    class _Fallback:
        app_name = os.getenv("APP_NAME", "Tadawul Fast Bridge")
        env = os.getenv("APP_ENV", "production")
        version = os.getenv("APP_VERSION", "4.7.1")
        log_level = (os.getenv("LOG_LEVEL", "info") or "info").lower()
        log_json = _safe_bool(os.getenv("LOG_JSON"), False)

        _prov_csv = os.getenv("ENABLED_PROVIDERS") or os.getenv("PROVIDERS") or "finnhub,fmp,eodhd"
        providers_list = [x.strip().lower() for x in _prov_csv.split(",") if x.strip()]
        enabled_providers = providers_list
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
    _BANNER_PRINTED = True

    try:
        logger.info("[env] App=%s | Env=%s | Version=%s",
                    _get_attr_any(s, ["app_name", "APP_NAME"], ""),
                    _get_attr_any(s, ["env", "environment", "APP_ENV"], ""),
                    _get_attr_any(s, ["version", "app_version", "APP_VERSION"], ""))
        logger.info("[env] Providers=%s", _safe_join(list(_get_attr_any(s, ["enabled_providers"], []) or [])))
        logger.info("[env] KSA Providers=%s", _safe_join(list(_get_attr_any(s, ["enabled_ksa_providers"], []) or [])))
    except Exception:
        pass


settings = _load_settings()
_print_banner_once(settings)


# ------------------------------------------------------------
# Backward compatible exports (DON'T BREAK IMPORTS)
# ------------------------------------------------------------

APP_NAME = _get_attr_any(settings, ["app_name", "APP_NAME"], "Tadawul Fast Bridge")
APP_ENV = _get_attr_any(settings, ["env", "environment", "APP_ENV"], "production")
APP_VERSION = _get_attr_any(settings, ["version", "app_version", "APP_VERSION"], "4.7.1")

BACKEND_BASE_URL = (str(_get_attr_any(settings, ["backend_base_url", "BACKEND_BASE_URL"], os.getenv("BACKEND_BASE_URL", "https://tadawul-fast-bridge.onrender.com")))).rstrip("/")

APP_TOKEN = _get_attr_any(settings, ["app_token", "APP_TOKEN"], None)
BACKUP_APP_TOKEN = _get_attr_any(settings, ["backup_app_token", "BACKUP_APP_TOKEN"], None)

ENABLED_PROVIDERS: List[str] = list(_get_attr_any(settings, ["enabled_providers"], []) or [])
KSA_PROVIDERS: List[str] = list(_get_attr_any(settings, ["enabled_ksa_providers"], []) or [])
PRIMARY_PROVIDER = (
    _get_attr_any(settings, ["primary_provider", "primary", "PRIMARY_PROVIDER"], None)
    or (ENABLED_PROVIDERS[0] if ENABLED_PROVIDERS else "finnhub")
)
PRIMARY_PROVIDER = str(PRIMARY_PROVIDER).strip().lower()

PROVIDERS = _safe_join(ENABLED_PROVIDERS)

HTTP_TIMEOUT_SEC = float(_get_attr_any(settings, ["http_timeout_sec"], 25.0))
HTTP_TIMEOUT = int(HTTP_TIMEOUT_SEC)

CACHE_TTL_SEC = float(_get_attr_any(settings, ["cache_ttl_sec"], 20.0))
QUOTE_TTL_SEC = float(_get_attr_any(settings, ["quote_ttl_sec"], 30.0))
FUNDAMENTALS_TTL_SEC = float(_get_attr_any(settings, ["fundamentals_ttl_sec"], 21600.0))
ARGAAM_SNAPSHOT_TTL_SEC = float(_get_attr_any(settings, ["argaam_snapshot_ttl_sec"], 30.0))

EODHD_API_KEY = _get_attr_any(settings, ["eodhd_api_key", "EODHD_API_KEY"], None)
FINNHUB_API_KEY = _get_attr_any(settings, ["finnhub_api_key", "FINNHUB_API_KEY"], None)
FMP_API_KEY = _get_attr_any(settings, ["fmp_api_key", "FMP_API_KEY"], None)
ALPHA_VANTAGE_API_KEY = _get_attr_any(settings, ["alpha_vantage_api_key", "ALPHA_VANTAGE_API_KEY"], None)
ARGAAM_API_KEY = _get_attr_any(settings, ["argaam_api_key", "ARGAAM_API_KEY"], None)

ENABLE_CORS_ALL_ORIGINS = bool(_get_attr_any(settings, ["cors_all_origins", "ENABLE_CORS_ALL_ORIGINS"], True))
CORS_ALL_ORIGINS = ENABLE_CORS_ALL_ORIGINS
CORS_ORIGINS = _get_attr_any(settings, ["cors_origins", "CORS_ORIGINS"], None)
CORS_ORIGINS_LIST = list(_get_attr_any(settings, ["cors_origins_list"], ["*"] if ENABLE_CORS_ALL_ORIGINS else []))

GOOGLE_SHEETS_CREDENTIALS: Optional[Dict[str, Any]] = _get_attr_any(settings, ["google_credentials_dict"], None)
DEFAULT_SPREADSHEET_ID = _get_attr_any(settings, ["default_spreadsheet_id", "DEFAULT_SPREADSHEET_ID"], None)
GOOGLE_SHEET_ID = _get_attr_any(settings, ["google_sheet_id", "GOOGLE_SHEET_ID"], None)
GOOGLE_SHEET_RANGE = _get_attr_any(settings, ["google_sheet_range", "GOOGLE_SHEET_RANGE"], None)
HAS_GOOGLE_SHEETS = bool(_get_attr_any(settings, ["has_google_sheets"], False))

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

LOG_LEVEL = (str(_get_attr_any(settings, ["log_level", "LOG_LEVEL"], "info")) or "info").upper()
LOG_JSON = bool(_get_attr_any(settings, ["log_json", "LOG_JSON"], False))
IS_PRODUCTION = str(APP_ENV).strip().lower() in {"prod", "production"}


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
