"""
env.py
------------------------------------------------------------
Backward-compatible environment exports for Tadawul Fast Bridge (v4.8.0) – PROD SAFE

✅ What this file does:
- Uses SINGLE source of truth from core.config.get_settings() when available.
- Exposes backward-compatible constants used across legacy modules (routes + engines).
- Prints a clean boot banner (providers + KSA providers) without leaking secrets.
- Adds missing legacy limit exports used by older routers (ENRICHED_*, ARGAAM_*, AI_*, ADV_*).

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

ENV_MODULE_VERSION = "4.8.0"

_SETTINGS: Any = None
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


def _safe_join(items: List[Any]) -> str:
    return ",".join([str(x) for x in (items or []) if x is not None and str(x).strip()])


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


def _env(name: str, default: Any = None) -> Any:
    v = os.getenv(name)
    if v is None:
        return default
    v = str(v).strip()
    return v if v != "" else default


def _get_attr(obj: Any, *names: str, default: Any = None) -> Any:
    """
    Best-effort attribute getter across different Settings versions.
    """
    if obj is None:
        return default
    for n in names:
        try:
            v = getattr(obj, n, None)
            if v is not None:
                return v
        except Exception:
            pass
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


def _as_str_list(v: Any) -> List[str]:
    if v is None:
        return []
    if isinstance(v, list):
        out: List[str] = []
        for x in v:
            s = str(x).strip()
            if s:
                out.append(s)
        return out
    s = str(v).strip()
    if not s:
        return []
    if "," in s:
        return [p.strip() for p in s.split(",") if p.strip()]
    return [s]


def _dedupe_preserve_order(items: List[str]) -> List[str]:
    seen = set()
    out: List[str] = []
    for x in items or []:
        k = (x or "").strip()
        if not k:
            continue
        kk = k.lower()
        if kk in seen:
            continue
        seen.add(kk)
        out.append(k)
    return out


# ------------------------------------------------------------
# Settings loader (single source of truth)
# ------------------------------------------------------------
def _load_settings() -> Any:
    """
    Load settings from core.config (preferred), then root config.py.
    If BOTH fail, fall back to a minimal env-only view to avoid crashing.
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

    # -------------------------
    # Minimal fallback (defensive)
    # -------------------------
    class _Fallback:
        # App identity
        app_name = _env("APP_NAME", "Tadawul Fast Bridge")
        env = _env("APP_ENV", _env("ENVIRONMENT", "production"))
        version = _env("APP_VERSION", "4.8.0")

        log_level = (_env("LOG_LEVEL", "info") or "info").lower()
        log_json = _safe_bool(_env("LOG_JSON"), False)

        # Providers
        _prov_csv = _env("ENABLED_PROVIDERS", _env("PROVIDERS", "finnhub,fmp,eodhd"))
        enabled_providers = [x.strip().lower() for x in str(_prov_csv).split(",") if x.strip()]
        enabled_ksa_providers = [x.strip().lower() for x in str(_env("KSA_PROVIDERS", "tadawul,argaam")).split(",") if x.strip()]
        primary_provider = (_env("PRIMARY_PROVIDER", "") or (enabled_providers[0] if enabled_providers else "finnhub")).strip().lower()

        # CORS
        cors_all_origins = _safe_bool(_env("ENABLE_CORS_ALL_ORIGINS", _env("CORS_ALL_ORIGINS", "true")), True)
        cors_origins = _env("CORS_ORIGINS")
        cors_origins_list = ["*"] if cors_all_origins else (
            [x.strip() for x in str(cors_origins or "").split(",") if x.strip()] if cors_origins else []
        )

        # Timeouts / TTLs
        http_timeout_sec = _safe_float(_env("HTTP_TIMEOUT_SEC", _env("HTTP_TIMEOUT", 25.0)), 25.0)
        cache_ttl_sec = _safe_float(_env("CACHE_TTL_SEC", 20.0), 20.0)
        quote_ttl_sec = _safe_float(_env("QUOTE_TTL_SEC", 30.0), 30.0)
        fundamentals_ttl_sec = _safe_float(_env("FUNDAMENTALS_TTL_SEC", 21600.0), 21600.0)
        argaam_snapshot_ttl_sec = _safe_float(_env("ARGAAM_SNAPSHOT_TTL_SEC", 30.0), 30.0)

        # Tokens
        app_token = _env("APP_TOKEN")
        backup_app_token = _env("BACKUP_APP_TOKEN")

        # Provider keys
        eodhd_api_key = _env("EODHD_API_KEY")
        finnhub_api_key = _env("FINNHUB_API_KEY")
        fmp_api_key = _env("FMP_API_KEY")
        alpha_vantage_api_key = _env("ALPHA_VANTAGE_API_KEY")
        argaam_api_key = _env("ARGAAM_API_KEY")

        # Backend base URL
        backend_base_url = str(_env("BACKEND_BASE_URL", "https://tadawul-fast-bridge.onrender.com")).rstrip("/")

        # Google Sheets (fallback parses only if config unavailable)
        google_credentials_dict = _try_parse_json_dict(_env("GOOGLE_SHEETS_CREDENTIALS"))
        default_spreadsheet_id = _env("DEFAULT_SPREADSHEET_ID", _env("GOOGLE_SHEET_ID"))
        google_sheet_id = _env("GOOGLE_SHEET_ID")
        google_sheet_range = _env("GOOGLE_SHEET_RANGE")
        has_google_sheets = bool(google_credentials_dict) and bool(default_spreadsheet_id or google_sheet_id)

        # Sheet names
        sheet_ksa_tadawul = _env("SHEET_KSA_TADAWUL", "KSA_Tadawul_Market")
        sheet_global_markets = _env("SHEET_GLOBAL_MARKETS", "Global_Markets")
        sheet_mutual_funds = _env("SHEET_MUTUAL_FUNDS", "Mutual_Funds")
        sheet_commodities_fx = _env("SHEET_COMMODITIES_FX", "Commodities_FX")
        sheet_market_leaders = _env("SHEET_MARKET_LEADERS", "Market_Leaders")
        sheet_my_portfolio = _env("SHEET_MY_PORTFOLIO", "My_Portfolio")
        sheet_insights_analysis = _env("SHEET_INSIGHTS_ANALYSIS", "Insights_Analysis")
        sheet_investment_advisor = _env("SHEET_INVESTMENT_ADVISOR", "Investment_Advisor")
        sheet_economic_calendar = _env("SHEET_ECONOMIC_CALENDAR", "Economic_Calendar")
        sheet_investment_income = _env("SHEET_INVESTMENT_INCOME", "Investment_Income_Statement")

    _SETTINGS = _Fallback()
    return _SETTINGS


def refresh_settings() -> Any:
    """
    Force reload settings (rarely needed; useful in tests).
    """
    global _SETTINGS
    _SETTINGS = None
    return _load_settings()


def _print_banner_once(s: Any) -> None:
    global _BANNER_PRINTED
    if _BANNER_PRINTED:
        return

    # allow disabling banner (Render multi-workers can be noisy)
    banner_on = _safe_bool(_env("ENV_BANNER", "true"), True)
    if not banner_on:
        _BANNER_PRINTED = True
        return

    _BANNER_PRINTED = True
    try:
        app = _get_attr(s, "app_name", "name", default="Tadawul Fast Bridge")
        envv = _get_attr(s, "env", "environment", default="production")
        ver = _get_attr(s, "version", "app_version", default=ENV_MODULE_VERSION)

        providers = _dedupe_preserve_order([str(x).lower() for x in _as_str_list(_get_attr(s, "enabled_providers", "providers_list", default=[]))])
        ksa_prov = _dedupe_preserve_order([str(x).lower() for x in _as_str_list(_get_attr(s, "enabled_ksa_providers", default=[]))])

        logger.info("[env] App=%s | Env=%s | Version=%s", app, envv, ver)
        logger.info("[env] Providers=%s", _safe_join(providers))
        logger.info("[env] KSA Providers=%s", _safe_join(ksa_prov))
    except Exception:
        # never fail startup for banner
        pass


settings = _load_settings()
_print_banner_once(settings)


# ------------------------------------------------------------
# Backward compatible exports (DON'T BREAK IMPORTS)
# ------------------------------------------------------------

# App identity
APP_NAME = str(_get_attr(settings, "app_name", "name", default="Tadawul Fast Bridge"))
APP_ENV = str(_get_attr(settings, "env", "environment", default="production"))
APP_VERSION = str(_get_attr(settings, "version", "app_version", "version_name", default=ENV_MODULE_VERSION))

# Base URL
BACKEND_BASE_URL = str(_get_attr(settings, "backend_base_url", "base_url", default=_env("BACKEND_BASE_URL", "https://tadawul-fast-bridge.onrender.com"))).rstrip("/")

# Tokens
APP_TOKEN = _get_attr(settings, "app_token", "APP_TOKEN", default=_env("APP_TOKEN"))
BACKUP_APP_TOKEN = _get_attr(settings, "backup_app_token", "BACKUP_APP_TOKEN", default=_env("BACKUP_APP_TOKEN"))

# Providers
ENABLED_PROVIDERS: List[str] = _dedupe_preserve_order(
    [str(x).strip().lower() for x in _as_str_list(_get_attr(settings, "enabled_providers", "providers_list", default=[]))]
) or _dedupe_preserve_order([x.strip().lower() for x in str(_env("ENABLED_PROVIDERS", _env("PROVIDERS", "finnhub,fmp,eodhd"))).split(",") if x.strip()])

KSA_PROVIDERS: List[str] = _dedupe_preserve_order(
    [str(x).strip().lower() for x in _as_str_list(_get_attr(settings, "enabled_ksa_providers", default=[]))]
) or _dedupe_preserve_order([x.strip().lower() for x in str(_env("KSA_PROVIDERS", "tadawul,argaam")).split(",") if x.strip()])

PRIMARY_PROVIDER = (
    str(_get_attr(settings, "primary_provider", "primary", default=_env("PRIMARY_PROVIDER", "")) or "").strip().lower()
    or (ENABLED_PROVIDERS[0] if ENABLED_PROVIDERS else "finnhub")
)

# CSV alias some modules still expect
PROVIDERS = _safe_join(ENABLED_PROVIDERS)

# Timeouts / TTL
HTTP_TIMEOUT_SEC = float(_get_attr(settings, "http_timeout_sec", default=_safe_float(_env("HTTP_TIMEOUT_SEC", _env("HTTP_TIMEOUT", 25.0)), 25.0)))
HTTP_TIMEOUT = int(HTTP_TIMEOUT_SEC)

CACHE_TTL_SEC = float(_get_attr(settings, "cache_ttl_sec", default=_safe_float(_env("CACHE_TTL_SEC", 20.0), 20.0)))
QUOTE_TTL_SEC = float(_get_attr(settings, "quote_ttl_sec", default=_safe_float(_env("QUOTE_TTL_SEC", 30.0), 30.0)))
FUNDAMENTALS_TTL_SEC = float(_get_attr(settings, "fundamentals_ttl_sec", default=_safe_float(_env("FUNDAMENTALS_TTL_SEC", 21600.0), 21600.0)))
ARGAAM_SNAPSHOT_TTL_SEC = float(_get_attr(settings, "argaam_snapshot_ttl_sec", default=_safe_float(_env("ARGAAM_SNAPSHOT_TTL_SEC", 30.0), 30.0)))

# Provider keys (exported for backward compat — do NOT log)
EODHD_API_KEY = _get_attr(settings, "eodhd_api_key", default=_env("EODHD_API_KEY"))
FINNHUB_API_KEY = _get_attr(settings, "finnhub_api_key", default=_env("FINNHUB_API_KEY"))
FMP_API_KEY = _get_attr(settings, "fmp_api_key", default=_env("FMP_API_KEY"))
ALPHA_VANTAGE_API_KEY = _get_attr(settings, "alpha_vantage_api_key", default=_env("ALPHA_VANTAGE_API_KEY"))
ARGAAM_API_KEY = _get_attr(settings, "argaam_api_key", default=_env("ARGAAM_API_KEY"))

# CORS
ENABLE_CORS_ALL_ORIGINS = bool(_get_attr(settings, "cors_all_origins", default=_safe_bool(_env("ENABLE_CORS_ALL_ORIGINS", _env("CORS_ALL_ORIGINS", "true")), True)))
CORS_ALL_ORIGINS = ENABLE_CORS_ALL_ORIGINS
CORS_ORIGINS = _get_attr(settings, "cors_origins", default=_env("CORS_ORIGINS"))
CORS_ORIGINS_LIST = list(_get_attr(settings, "cors_origins_list", default=(["*"] if ENABLE_CORS_ALL_ORIGINS else [])))

# Google Sheets
GOOGLE_SHEETS_CREDENTIALS: Optional[Dict[str, Any]] = _get_attr(settings, "google_credentials_dict", default=None)
if GOOGLE_SHEETS_CREDENTIALS is None:
    # fallback-only (do not do this if config is healthy)
    GOOGLE_SHEETS_CREDENTIALS = _try_parse_json_dict(_env("GOOGLE_SHEETS_CREDENTIALS"))

DEFAULT_SPREADSHEET_ID = _get_attr(settings, "default_spreadsheet_id", default=_env("DEFAULT_SPREADSHEET_ID", _env("GOOGLE_SHEET_ID")))
GOOGLE_SHEET_ID = _get_attr(settings, "google_sheet_id", default=_env("GOOGLE_SHEET_ID"))
GOOGLE_SHEET_RANGE = _get_attr(settings, "google_sheet_range", default=_env("GOOGLE_SHEET_RANGE"))
HAS_GOOGLE_SHEETS = bool(_get_attr(settings, "has_google_sheets", default=bool(GOOGLE_SHEETS_CREDENTIALS) and bool(DEFAULT_SPREADSHEET_ID or GOOGLE_SHEET_ID)))

# Sheet names (Apps Script alignment)
SHEET_KSA_TADAWUL = str(_get_attr(settings, "sheet_ksa_tadawul", default=_env("SHEET_KSA_TADAWUL", "KSA_Tadawul_Market")))
SHEET_GLOBAL_MARKETS = str(_get_attr(settings, "sheet_global_markets", default=_env("SHEET_GLOBAL_MARKETS", "Global_Markets")))
SHEET_MUTUAL_FUNDS = str(_get_attr(settings, "sheet_mutual_funds", default=_env("SHEET_MUTUAL_FUNDS", "Mutual_Funds")))
SHEET_COMMODITIES_FX = str(_get_attr(settings, "sheet_commodities_fx", default=_env("SHEET_COMMODITIES_FX", "Commodities_FX")))
SHEET_MARKET_LEADERS = str(_get_attr(settings, "sheet_market_leaders", default=_env("SHEET_MARKET_LEADERS", "Market_Leaders")))
SHEET_MY_PORTFOLIO = str(_get_attr(settings, "sheet_my_portfolio", default=_env("SHEET_MY_PORTFOLIO", "My_Portfolio")))
SHEET_INSIGHTS_ANALYSIS = str(_get_attr(settings, "sheet_insights_analysis", default=_env("SHEET_INSIGHTS_ANALYSIS", "Insights_Analysis")))
SHEET_INVESTMENT_ADVISOR = str(_get_attr(settings, "sheet_investment_advisor", default=_env("SHEET_INVESTMENT_ADVISOR", "Investment_Advisor")))
SHEET_ECONOMIC_CALENDAR = str(_get_attr(settings, "sheet_economic_calendar", default=_env("SHEET_ECONOMIC_CALENDAR", "Economic_Calendar")))
SHEET_INVESTMENT_INCOME = str(_get_attr(settings, "sheet_investment_income", default=_env("SHEET_INVESTMENT_INCOME", "Investment_Income_Statement")))

# Logging flags
LOG_LEVEL = str(_get_attr(settings, "log_level", default=_env("LOG_LEVEL", "info")) or "info").upper()
LOG_JSON = bool(_get_attr(settings, "log_json", default=_safe_bool(_env("LOG_JSON"), False)))
IS_PRODUCTION = str(APP_ENV).strip().lower() in {"prod", "production"}


# ------------------------------------------------------------
# Legacy limits exports (used by older routes via `import env as env_mod`)
# ------------------------------------------------------------
# Enriched router
ENRICHED_MAX_TICKERS = _safe_int(_get_attr(settings, "enriched_max_tickers", default=_env("ENRICHED_MAX_TICKERS", 250)), 250)
ENRICHED_BATCH_SIZE = _safe_int(_get_attr(settings, "enriched_batch_size", default=_env("ENRICHED_BATCH_SIZE", 40)), 40)
ENRICHED_CONCURRENCY = _safe_int(_get_attr(settings, "enriched_concurrency", default=_env("ENRICHED_CONCURRENCY", 8)), 8)
ENRICHED_TIMEOUT_SEC = float(_safe_float(_get_attr(settings, "enriched_timeout_sec", default=_env("ENRICHED_TIMEOUT_SEC", 45.0)), 45.0))

# Argaam router
ARGAAM_MAX_TICKERS = _safe_int(_get_attr(settings, "argaam_max_tickers", default=_env("ARGAAM_MAX_TICKERS", 400)), 400)
ARGAAM_BATCH_SIZE = _safe_int(_get_attr(settings, "argaam_batch_size", default=_env("ARGAAM_BATCH_SIZE", 40)), 40)
ARGAAM_CONCURRENCY = _safe_int(_get_attr(settings, "argaam_concurrency", default=_env("ARGAAM_CONCURRENCY", 6)), 6)
ARGAAM_TIMEOUT_SEC = float(_safe_float(_get_attr(settings, "argaam_timeout_sec", default=_env("ARGAAM_TIMEOUT_SEC", 45.0)), 45.0))

# AI Analysis router
AI_BATCH_SIZE = _safe_int(_get_attr(settings, "ai_batch_size", default=_env("AI_BATCH_SIZE", 20)), 20)
AI_BATCH_TIMEOUT_SEC = float(_safe_float(_get_attr(settings, "ai_batch_timeout_sec", default=_env("AI_BATCH_TIMEOUT_SEC", 45.0)), 45.0))
AI_BATCH_CONCURRENCY = _safe_int(_get_attr(settings, "ai_batch_concurrency", default=_env("AI_BATCH_CONCURRENCY", 5)), 5)
AI_MAX_TICKERS = _safe_int(_get_attr(settings, "ai_max_tickers", default=_env("AI_MAX_TICKERS", 500)), 500)

# Advanced Analysis router
ADV_BATCH_SIZE = _safe_int(_get_attr(settings, "adv_batch_size", default=_env("ADV_BATCH_SIZE", 25)), 25)
ADV_BATCH_TIMEOUT_SEC = float(_safe_float(_get_attr(settings, "adv_batch_timeout_sec", default=_env("ADV_BATCH_TIMEOUT_SEC", 45.0)), 45.0))
ADV_BATCH_CONCURRENCY = _safe_int(_get_attr(settings, "adv_batch_concurrency", default=_env("ADV_BATCH_CONCURRENCY", 6)), 6)
ADV_MAX_TICKERS = _safe_int(_get_attr(settings, "adv_max_tickers", default=_env("ADV_MAX_TICKERS", 500)), 500)


# Convenience: safe summary (no secrets)
def safe_env_summary() -> Dict[str, Any]:
    return {
        "env_module_version": ENV_MODULE_VERSION,
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
    "ENV_MODULE_VERSION",
    "settings",
    "refresh_settings",
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
    # Legacy limits (important for older routers)
    "ENRICHED_MAX_TICKERS",
    "ENRICHED_BATCH_SIZE",
    "ENRICHED_CONCURRENCY",
    "ENRICHED_TIMEOUT_SEC",
    "ARGAAM_MAX_TICKERS",
    "ARGAAM_BATCH_SIZE",
    "ARGAAM_CONCURRENCY",
    "ARGAAM_TIMEOUT_SEC",
    "AI_BATCH_SIZE",
    "AI_BATCH_TIMEOUT_SEC",
    "AI_BATCH_CONCURRENCY",
    "AI_MAX_TICKERS",
    "ADV_BATCH_SIZE",
    "ADV_BATCH_TIMEOUT_SEC",
    "ADV_BATCH_CONCURRENCY",
    "ADV_MAX_TICKERS",
]
