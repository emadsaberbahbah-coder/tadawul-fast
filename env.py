# env.py  (FULL REPLACEMENT)
"""
env.py
------------------------------------------------------------
Backward-compatible environment exports for Tadawul Fast Bridge (v4.8.0)

✅ What this file does:
- Uses SINGLE source of truth from core.config/config.get_settings() when available.
- Exposes backward-compatible constants used across legacy modules.
- Prints a clean boot banner (providers + KSA providers) WITHOUT leaking secrets.
- Provides a settings "view" that adds a few computed/aliased attributes
  expected by other modules (google_sheets_service, symbols_reader, etc.).

Important:
- Do NOT duplicate business logic here (routing/blocks remain in engines).
- Fallback mode exists ONLY to avoid crashing if config import fails.
"""

from __future__ import annotations

import json
import logging
import os
from typing import Any, Dict, List, Optional

logger = logging.getLogger("env")

_SETTINGS_BASE: Optional[object] = None
settings: Optional[object] = None
_BANNER_PRINTED = False
_WARNED_SETTINGS_IMPORT = False

_TRUTHY = {"1", "true", "yes", "on", "y", "t"}
_FALSY = {"0", "false", "no", "off", "n", "f"}


# ------------------------------------------------------------
# Safe helpers
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
    For GOOGLE_SHEETS_CREDENTIALS-like env JSON.
    Accepts dict or JSON string.
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
    # Handle quoted JSON (sometimes stored as '"{...}"')
    if (s.startswith('"') and s.endswith('"')) or (s.startswith("'") and s.endswith("'")):
        s = s[1:-1].strip()
    try:
        obj = json.loads(s)
        return obj if isinstance(obj, dict) else None
    except Exception:
        return None


def _as_list_lower(v: Any) -> List[str]:
    if v is None:
        return []
    if isinstance(v, list):
        return [str(x).strip().lower() for x in v if str(x).strip()]
    s = str(v).strip()
    if not s:
        return []
    if s.startswith("[") and s.endswith("]"):
        try:
            arr = json.loads(s)
            if isinstance(arr, list):
                return [str(x).strip().lower() for x in arr if str(x).strip()]
        except Exception:
            pass
    return [x.strip().lower() for x in s.split(",") if x.strip()]


def _get_attr_any(obj: Any, names: List[str], default: Any = None) -> Any:
    for n in names:
        try:
            if hasattr(obj, n):
                v = getattr(obj, n)
                if v is not None and (not isinstance(v, str) or v.strip()):
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
    v = _normalize_version(_get_attr_any(settings_obj, ["version", "app_version", "APP_VERSION"], ""))
    if not v:
        v = _normalize_version(os.getenv("SERVICE_VERSION") or os.getenv("APP_VERSION") or "")
    if not v:
        commit = (os.getenv("RENDER_GIT_COMMIT") or os.getenv("GIT_COMMIT") or "").strip()
        if commit:
            v = commit[:7]
    return v or "dev"


# ------------------------------------------------------------
# Settings loader (single source of truth)
# ------------------------------------------------------------
def _load_settings_base() -> object:
    """
    Load settings from core.config (preferred), then root config.
    If BOTH fail, use a minimal env-only view.
    """
    global _SETTINGS_BASE, _WARNED_SETTINGS_IMPORT
    if _SETTINGS_BASE is not None:
        return _SETTINGS_BASE

    # Preferred: core.config
    try:
        from core.config import get_settings  # type: ignore

        _SETTINGS_BASE = get_settings()
        return _SETTINGS_BASE
    except Exception as exc:
        if not _WARNED_SETTINGS_IMPORT:
            _WARNED_SETTINGS_IMPORT = True
            logger.warning("[env] Cannot import core.config.get_settings(): %s", exc)

    # Secondary: root config.py
    try:
        from config import get_settings  # type: ignore

        _SETTINGS_BASE = get_settings()
        return _SETTINGS_BASE
    except Exception as exc:
        if not _WARNED_SETTINGS_IMPORT:
            _WARNED_SETTINGS_IMPORT = True
            logger.warning("[env] Cannot import config.get_settings(): %s", exc)

    # Minimal fallback (ONLY if settings import fails)
    class _Fallback:
        app_name = os.getenv("APP_NAME", "Tadawul Fast Bridge")
        env = os.getenv("APP_ENV") or os.getenv("ENVIRONMENT") or "production"
        version = os.getenv("SERVICE_VERSION") or os.getenv("APP_VERSION") or "dev"
        log_level = (os.getenv("LOG_LEVEL", "INFO") or "INFO").lower()

        enabled_providers = _as_list_lower(os.getenv("ENABLED_PROVIDERS") or os.getenv("PROVIDERS") or "finnhub,fmp,eodhd,yahoo_chart")
        enabled_ksa_providers = _as_list_lower(os.getenv("KSA_PROVIDERS") or "tadawul,argaam,yahoo_chart")
        primary_provider = (os.getenv("PRIMARY_PROVIDER") or "").strip().lower() or (enabled_providers[0] if enabled_providers else "finnhub")

        backend_base_url = (os.getenv("BACKEND_BASE_URL") or os.getenv("BASE_URL") or "").rstrip("/")
        app_token = os.getenv("APP_TOKEN") or os.getenv("TFB_APP_TOKEN")
        backup_app_token = os.getenv("BACKUP_APP_TOKEN")

        require_auth = _safe_bool(os.getenv("REQUIRE_AUTH"), False)

        http_timeout_sec = _safe_float(os.getenv("HTTP_TIMEOUT_SEC") or os.getenv("HTTP_TIMEOUT"), 25.0)

        google_sheets_credentials_raw = (os.getenv("GOOGLE_SHEETS_CREDENTIALS") or os.getenv("GOOGLE_CREDENTIALS") or "").strip()
        google_credentials_dict = _try_parse_json_dict(google_sheets_credentials_raw)

        default_spreadsheet_id = (os.getenv("DEFAULT_SPREADSHEET_ID") or os.getenv("SPREADSHEET_ID") or os.getenv("GOOGLE_SHEETS_ID") or "").strip()

        google_apps_script_url = (os.getenv("GOOGLE_APPS_SCRIPT_URL") or "").strip()
        google_apps_script_backup_url = (os.getenv("GOOGLE_APPS_SCRIPT_BACKUP_URL") or "").strip()

        # sheet names
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

    _SETTINGS_BASE = _Fallback()
    return _SETTINGS_BASE


class _SettingsView:
    """
    Lightweight wrapper around the base settings object.
    Adds a few computed/aliased attributes without mutating the base object
    (important if base is a Pydantic Settings model).
    """

    def __init__(self, base: object, extras: Dict[str, Any]):
        self._base = base
        for k, v in extras.items():
            setattr(self, k, v)

    def __getattr__(self, name: str) -> Any:
        return getattr(self._base, name)


def _build_settings_view(base: object) -> _SettingsView:
    # Identity
    app_name = str(_get_attr_any(base, ["app_name", "APP_NAME"], os.getenv("APP_NAME", "Tadawul Fast Bridge")) or "Tadawul Fast Bridge")
    env_name = str(_get_attr_any(base, ["env", "environment", "APP_ENV"], os.getenv("APP_ENV") or os.getenv("ENVIRONMENT") or "production") or "production")
    ver = _resolve_version(base)

    # URLs
    backend_base_url = str(
        _get_attr_any(
            base,
            ["backend_base_url", "BACKEND_BASE_URL", "base_url", "BASE_URL"],
            os.getenv("BACKEND_BASE_URL") or os.getenv("BASE_URL") or "",
        )
        or ""
    ).rstrip("/")

    # Auth tokens
    app_token = _get_attr_any(base, ["app_token", "APP_TOKEN"], os.getenv("APP_TOKEN") or os.getenv("TFB_APP_TOKEN"))
    backup_app_token = _get_attr_any(base, ["backup_app_token", "BACKUP_APP_TOKEN"], os.getenv("BACKUP_APP_TOKEN"))

    require_auth = _safe_bool(_get_attr_any(base, ["require_auth", "REQUIRE_AUTH"], os.getenv("REQUIRE_AUTH")), False)

    # Providers
    enabled = list(_get_attr_any(base, ["enabled_providers"], []) or [])
    if not enabled:
        enabled = _as_list_lower(os.getenv("ENABLED_PROVIDERS") or os.getenv("PROVIDERS"))

    ksa = list(_get_attr_any(base, ["enabled_ksa_providers"], []) or [])
    if not ksa:
        ksa = _as_list_lower(os.getenv("KSA_PROVIDERS"))

    primary = _get_attr_any(base, ["primary_provider", "PRIMARY_PROVIDER"], os.getenv("PRIMARY_PROVIDER"))
    primary = (str(primary).strip().lower() if primary else "") or (enabled[0] if enabled else "finnhub")

    # Timeouts / TTLs
    http_timeout_sec = float(_get_attr_any(base, ["http_timeout_sec", "HTTP_TIMEOUT_SEC"], os.getenv("HTTP_TIMEOUT_SEC") or os.getenv("HTTP_TIMEOUT") or 25.0))
    http_timeout_sec = max(5.0, float(http_timeout_sec or 25.0))

    cache_ttl_sec = float(_get_attr_any(base, ["cache_ttl_sec", "CACHE_TTL_SEC"], os.getenv("CACHE_TTL_SEC") or 20.0))
    quote_ttl_sec = float(_get_attr_any(base, ["quote_ttl_sec", "QUOTE_TTL_SEC"], os.getenv("QUOTE_TTL_SEC") or 30.0))
    fundamentals_ttl_sec = float(_get_attr_any(base, ["fundamentals_ttl_sec", "FUNDAMENTALS_TTL_SEC"], os.getenv("FUNDAMENTALS_TTL_SEC") or 21600.0))
    argaam_snapshot_ttl_sec = float(_get_attr_any(base, ["argaam_snapshot_ttl_sec", "ARGAAM_SNAPSHOT_TTL_SEC"], os.getenv("ARGAAM_SNAPSHOT_TTL_SEC") or 30.0))

    # Google / Sheets
    # Prefer a parsed dict if present; otherwise parse any raw JSON we can find.
    creds_dict = _get_attr_any(base, ["google_credentials_dict"], None)
    creds_raw = _get_attr_any(
        base,
        ["google_sheets_credentials_raw", "google_sheets_credentials", "google_credentials", "google_sa_json", "GOOGLE_SHEETS_CREDENTIALS"],
        None,
    )

    if creds_dict is None:
        # If base exposes google_sheets_credentials as dict already, pick it up
        if isinstance(_get_attr_any(base, ["google_sheets_credentials"], None), dict):
            creds_dict = _get_attr_any(base, ["google_sheets_credentials"], None)

    if creds_dict is None:
        # Try parse raw
        creds_dict = _try_parse_json_dict(creds_raw)

    if creds_dict is None:
        # Final fallback: env var JSON
        creds_dict = _try_parse_json_dict(os.getenv("GOOGLE_SHEETS_CREDENTIALS"))

    if isinstance(creds_raw, dict):
        creds_raw = json.dumps(creds_raw)

    if not isinstance(creds_raw, str) or not creds_raw.strip():
        creds_raw = (os.getenv("GOOGLE_SHEETS_CREDENTIALS") or os.getenv("GOOGLE_CREDENTIALS") or os.getenv("GOOGLE_SA_JSON") or "").strip()

    # Spreadsheet IDs (aliases used by different modules)
    spreadsheet_id = str(
        _get_attr_any(
            base,
            ["spreadsheet_id", "default_spreadsheet_id", "google_sheets_id", "SPREADSHEET_ID", "DEFAULT_SPREADSHEET_ID"],
            os.getenv("SPREADSHEET_ID") or os.getenv("DEFAULT_SPREADSHEET_ID") or os.getenv("GOOGLE_SHEETS_ID") or "",
        )
        or ""
    ).strip()

    default_spreadsheet_id = spreadsheet_id  # canonical alias for older modules

    has_google_sheets = bool(isinstance(creds_dict, dict) and creds_dict) and bool(default_spreadsheet_id)

    # GAS URLs
    gas_url = str(_get_attr_any(base, ["google_apps_script_url"], os.getenv("GOOGLE_APPS_SCRIPT_URL") or "") or "").strip()
    gas_backup = str(_get_attr_any(base, ["google_apps_script_backup_url"], os.getenv("GOOGLE_APPS_SCRIPT_BACKUP_URL") or "") or "").strip()

    # Sheet names (for symbols_reader / sync scripts)
    extras: Dict[str, Any] = {
        "app_name": app_name,
        "env": env_name,
        "version": ver,
        "backend_base_url": backend_base_url,
        "base_url": backend_base_url,  # convenience alias
        "app_token": app_token,
        "backup_app_token": backup_app_token,
        "require_auth": require_auth,
        "enabled_providers": [str(x).strip().lower() for x in enabled if str(x).strip()],
        "enabled_ksa_providers": [str(x).strip().lower() for x in ksa if str(x).strip()],
        "primary_provider": primary,
        "http_timeout_sec": http_timeout_sec,
        "cache_ttl_sec": float(cache_ttl_sec),
        "quote_ttl_sec": float(quote_ttl_sec),
        "fundamentals_ttl_sec": float(fundamentals_ttl_sec),
        "argaam_snapshot_ttl_sec": float(argaam_snapshot_ttl_sec),
        "google_credentials_dict": creds_dict,
        "google_sheets_credentials_raw": creds_raw,
        "spreadsheet_id": spreadsheet_id,
        "default_spreadsheet_id": default_spreadsheet_id,
        "has_google_sheets": has_google_sheets,
        "google_apps_script_url": gas_url,
        "google_apps_script_backup_url": gas_backup,
        # standard page names
        "sheet_ksa_tadawul": str(_get_attr_any(base, ["sheet_ksa_tadawul"], os.getenv("SHEET_KSA_TADAWUL", "KSA_Tadawul_Market"))),
        "sheet_global_markets": str(_get_attr_any(base, ["sheet_global_markets"], os.getenv("SHEET_GLOBAL_MARKETS", "Global_Markets"))),
        "sheet_mutual_funds": str(_get_attr_any(base, ["sheet_mutual_funds"], os.getenv("SHEET_MUTUAL_FUNDS", "Mutual_Funds"))),
        "sheet_commodities_fx": str(_get_attr_any(base, ["sheet_commodities_fx"], os.getenv("SHEET_COMMODITIES_FX", "Commodities_FX"))),
        "sheet_market_leaders": str(_get_attr_any(base, ["sheet_market_leaders"], os.getenv("SHEET_MARKET_LEADERS", "Market_Leaders"))),
        "sheet_my_portfolio": str(_get_attr_any(base, ["sheet_my_portfolio"], os.getenv("SHEET_MY_PORTFOLIO", "My_Portfolio"))),
        "sheet_insights_analysis": str(_get_attr_any(base, ["sheet_insights_analysis"], os.getenv("SHEET_INSIGHTS_ANALYSIS", "Insights_Analysis"))),
        "sheet_investment_advisor": str(_get_attr_any(base, ["sheet_investment_advisor"], os.getenv("SHEET_INVESTMENT_ADVISOR", "Investment_Advisor"))),
        "sheet_economic_calendar": str(_get_attr_any(base, ["sheet_economic_calendar"], os.getenv("SHEET_ECONOMIC_CALENDAR", "Economic_Calendar"))),
        "sheet_investment_income": str(_get_attr_any(base, ["sheet_investment_income"], os.getenv("SHEET_INVESTMENT_INCOME", "Investment_Income_Statement"))),
        # logging convenience
        "log_level": str(_get_attr_any(base, ["log_level", "LOG_LEVEL"], os.getenv("LOG_LEVEL", "info")) or "info").lower(),
    }
    return _SettingsView(base, extras)


def _print_banner_once(s: object) -> None:
    global _BANNER_PRINTED
    if _BANNER_PRINTED:
        return

    if _safe_bool(os.getenv("DISABLE_ENV_BANNER"), False):
        _BANNER_PRINTED = True
        return

    _BANNER_PRINTED = True
    try:
        app_name = _get_attr_any(s, ["app_name", "APP_NAME"], "Tadawul Fast Bridge")
        env_name = _get_attr_any(s, ["env", "environment", "APP_ENV"], "production")
        ver = _resolve_version(s)

        providers = list(_get_attr_any(s, ["enabled_providers"], []) or [])
        ksa_providers = list(_get_attr_any(s, ["enabled_ksa_providers"], []) or [])

        logger.info("[env] App=%s | Env=%s | Version=%s", app_name, env_name, ver)
        logger.info("[env] Providers=%s", _safe_join([str(x) for x in providers]))
        logger.info("[env] KSA Providers=%s", _safe_join([str(x) for x in ksa_providers]))
        # Presence only
        logger.info(
            "[env] Sheets creds=%s | SpreadsheetId=%s",
            "SET" if bool(_get_attr_any(s, ["google_credentials_dict"], None)) else "MISSING",
            "SET" if bool(_get_attr_any(s, ["default_spreadsheet_id", "spreadsheet_id"], "")) else "MISSING",
        )
    except Exception:
        pass


# Build final settings view
_SETTINGS_BASE = _load_settings_base()
settings = _build_settings_view(_SETTINGS_BASE)
_print_banner_once(settings)

# ------------------------------------------------------------
# Backward compatible exports (DON'T BREAK IMPORTS)
# ------------------------------------------------------------
APP_NAME = _get_attr_any(settings, ["app_name", "APP_NAME"], "Tadawul Fast Bridge")
APP_ENV = _get_attr_any(settings, ["env", "environment", "APP_ENV"], "production")
APP_VERSION = _resolve_version(settings)

BACKEND_BASE_URL = str(_get_attr_any(settings, ["backend_base_url", "BACKEND_BASE_URL", "base_url", "BASE_URL"], "") or "").rstrip("/")

APP_TOKEN = _get_attr_any(settings, ["app_token", "APP_TOKEN"], None)
BACKUP_APP_TOKEN = _get_attr_any(settings, ["backup_app_token", "BACKUP_APP_TOKEN"], None)
REQUIRE_AUTH = bool(_get_attr_any(settings, ["require_auth", "REQUIRE_AUTH"], _safe_bool(os.getenv("REQUIRE_AUTH"), False)))

# Providers
ENABLED_PROVIDERS: List[str] = list(_get_attr_any(settings, ["enabled_providers"], []) or [])
if not ENABLED_PROVIDERS:
    ENABLED_PROVIDERS = _as_list_lower(os.getenv("ENABLED_PROVIDERS") or os.getenv("PROVIDERS"))

KSA_PROVIDERS: List[str] = list(_get_attr_any(settings, ["enabled_ksa_providers"], []) or [])
if not KSA_PROVIDERS:
    KSA_PROVIDERS = _as_list_lower(os.getenv("KSA_PROVIDERS"))

PRIMARY_PROVIDER = (
    _get_attr_any(settings, ["primary_provider", "PRIMARY_PROVIDER"], None)
    or (ENABLED_PROVIDERS[0] if ENABLED_PROVIDERS else "finnhub")
)
PRIMARY_PROVIDER = str(PRIMARY_PROVIDER).strip().lower()

PROVIDERS = _safe_join(ENABLED_PROVIDERS)

# Timeouts / TTLs
HTTP_TIMEOUT_SEC = float(_get_attr_any(settings, ["http_timeout_sec", "HTTP_TIMEOUT_SEC"], os.getenv("HTTP_TIMEOUT_SEC") or os.getenv("HTTP_TIMEOUT") or 25.0))
HTTP_TIMEOUT_SEC = max(5.0, HTTP_TIMEOUT_SEC)
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
ENABLE_CORS_ALL_ORIGINS = bool(_get_attr_any(settings, ["enable_cors_all_origins", "cors_all_origins", "ENABLE_CORS_ALL_ORIGINS"], _safe_bool(os.getenv("ENABLE_CORS_ALL_ORIGINS") or os.getenv("CORS_ALL_ORIGINS"), True)))
CORS_ALL_ORIGINS = ENABLE_CORS_ALL_ORIGINS
CORS_ORIGINS = _get_attr_any(settings, ["cors_origins", "CORS_ORIGINS"], os.getenv("CORS_ORIGINS"))
CORS_ORIGINS_LIST = list(_get_attr_any(settings, ["cors_origins_list", "CORS_ORIGINS_LIST"], ["*"] if ENABLE_CORS_ALL_ORIGINS else []))

# Google Sheets (exports + compatibility)
GOOGLE_SHEETS_CREDENTIALS: Optional[Dict[str, Any]] = _get_attr_any(settings, ["google_credentials_dict", "google_sheets_credentials"], None)
if GOOGLE_SHEETS_CREDENTIALS is None:
    GOOGLE_SHEETS_CREDENTIALS = _try_parse_json_dict(os.getenv("GOOGLE_SHEETS_CREDENTIALS"))

DEFAULT_SPREADSHEET_ID = str(_get_attr_any(settings, ["default_spreadsheet_id", "DEFAULT_SPREADSHEET_ID"], os.getenv("DEFAULT_SPREADSHEET_ID") or os.getenv("SPREADSHEET_ID") or "") or "").strip()
SPREADSHEET_ID = DEFAULT_SPREADSHEET_ID  # alias (your Render env uses SPREADSHEET_ID)
GOOGLE_SHEETS_ID = str(_get_attr_any(settings, ["google_sheets_id", "GOOGLE_SHEETS_ID"], os.getenv("GOOGLE_SHEETS_ID") or "") or "").strip()

GOOGLE_SHEET_ID = str(_get_attr_any(settings, ["google_sheet_id", "GOOGLE_SHEET_ID"], os.getenv("GOOGLE_SHEET_ID") or "") or "").strip()
GOOGLE_SHEET_RANGE = _get_attr_any(settings, ["google_sheet_range", "GOOGLE_SHEET_RANGE"], os.getenv("GOOGLE_SHEET_RANGE"))

HAS_GOOGLE_SHEETS = bool(_get_attr_any(settings, ["has_google_sheets", "HAS_GOOGLE_SHEETS"], bool(GOOGLE_SHEETS_CREDENTIALS) and bool(DEFAULT_SPREADSHEET_ID)))

# GAS URLs
GOOGLE_APPS_SCRIPT_URL = str(_get_attr_any(settings, ["google_apps_script_url"], os.getenv("GOOGLE_APPS_SCRIPT_URL") or "") or "").strip()
GOOGLE_APPS_SCRIPT_BACKUP_URL = str(_get_attr_any(settings, ["google_apps_script_backup_url"], os.getenv("GOOGLE_APPS_SCRIPT_BACKUP_URL") or "") or "").strip()

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
        "require_auth": REQUIRE_AUTH,
        "cors_all": CORS_ALL_ORIGINS,
        "timeout_sec": HTTP_TIMEOUT_SEC,
        "cache_ttl_sec": CACHE_TTL_SEC,
        "has_google_sheets": HAS_GOOGLE_SHEETS,
        "default_spreadsheet_id_set": bool(DEFAULT_SPREADSHEET_ID),
        "google_sheet_id_set": bool(GOOGLE_SHEET_ID),
        "apps_script_backup_url_set": bool(GOOGLE_APPS_SCRIPT_BACKUP_URL),
        "app_token": _mask(APP_TOKEN),
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
    "REQUIRE_AUTH",
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
    "SPREADSHEET_ID",
    "GOOGLE_SHEETS_ID",
    "GOOGLE_SHEET_ID",
    "GOOGLE_SHEET_RANGE",
    "HAS_GOOGLE_SHEETS",
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
