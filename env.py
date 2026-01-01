# env.py — FULL REPLACEMENT (QUIET + PROD SAFE) — v5.0.10
"""
env.py
------------------------------------------------------------
Backward-compatible environment exports for Tadawul Fast Bridge (v5.0.10)

Key goals
- ✅ Quiet boot by default (no banner logs unless ENV_LOG_ON_BOOT=true)
- ✅ Version resolves from ENV FIRST: APP_VERSION / SERVICE_VERSION / VERSION / RELEASE
- ✅ Prefers repo-root config.get_settings() (canonical), but never crashes import-time
- ✅ Provides legacy exports used by older modules/routes
- ✅ Ensures token alias exports (FINNHUB_API_TOKEN / EODHD_API_TOKEN) for legacy modules
- ✅ Keeps ENGINE_CACHE_TTL_SEC + ENRICHED_BATCH_CONCURRENCY (aligns with ENRICHED_CONCURRENCY)
- ✅ Robust GOOGLE_SHEETS_CREDENTIALS parsing: supports:
    - minified JSON
    - pretty JSON
    - base64(JSON)
    - quoted JSON string (single/double)
- ✅ Exposes feature toggles used by DataEngineV2 (Yahoo chart/fundamentals/yfinance KSA)
- ✅ Exports APP_TOKEN / BACKUP_APP_TOKEN aliases when present (legacy routes read env directly)

Notes
- This file is intentionally defensive: it must never prevent app startup.
- It never overwrites existing os.environ keys (exports only if missing/blank).
"""

from __future__ import annotations

import base64
import json
import logging
import os
from typing import Any, Dict, List, Optional


ENV_VERSION = "5.0.10"
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
    low = s.lower()
    if low in {"unknown", "none", "null"}:
        return ""
    # treat config default "0.0.0" as "not set" so Render commit can win
    if low == "0.0.0":
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


def _strip_wrapping_quotes(s: str) -> str:
    t = (s or "").strip()
    if len(t) >= 2 and ((t[0] == t[-1] == '"') or (t[0] == t[-1] == "'")):
        return t[1:-1].strip()
    return t


def _looks_like_b64(s: str) -> bool:
    raw = (s or "").strip()
    if len(raw) < 80:
        return False
    # base64 is typically A-Z a-z 0-9 + / =
    allowed = set("ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/=\n\r")
    return all(c in allowed for c in raw)


def _maybe_b64_decode(s: str) -> str:
    """
    If s looks like base64 and decodes to JSON, return decoded.
    Otherwise return original.
    """
    raw = (s or "").strip()
    if not raw:
        return raw
    if raw.startswith("{"):
        return raw

    # avoid wasting time decoding short/non-b64 strings
    if not _looks_like_b64(raw):
        return raw

    try:
        dec = base64.b64decode(raw).decode("utf-8", errors="strict").strip()
        if dec.startswith("{") and ("private_key" in dec or '"type"' in dec):
            return dec
    except Exception:
        return raw
    return raw


def _try_parse_json_dict(raw: Any) -> Optional[Dict[str, Any]]:
    """
    Accepts:
    - dict
    - JSON string
    - base64(JSON string)
    - quoted JSON string
    Returns dict or None.
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

    s = _strip_wrapping_quotes(s)
    s = _maybe_b64_decode(s)
    s = _strip_wrapping_quotes(s).strip()
    if not s:
        return None
    if not s.startswith("{"):
        return None

    try:
        obj = json.loads(s)
        return obj if isinstance(obj, dict) else None
    except Exception:
        return None


def _export_env_if_missing(key: str, value: Any) -> None:
    if value is None:
        return
    v = str(value).strip()
    if not v:
        return
    cur = os.getenv(key)
    if cur is None or str(cur).strip() == "":
        os.environ[key] = v


def _get_first_env(*keys: str) -> Optional[str]:
    for k in keys:
        v = os.getenv(k)
        if v is not None and str(v).strip():
            return str(v).strip()
    return None


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

    out: List[str] = []
    seen = set()
    for x in raw:
        if x and x not in seen:
            seen.add(x)
            out.append(x)
    return out


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
        # Prefer repo-root config.py
        try:
            from config import get_settings  # type: ignore

            _SETTINGS_OBJ = get_settings()
            _SETTINGS_LOADED = True
            return _SETTINGS_OBJ
        except Exception:
            pass

        # Fallback legacy shim
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


def _env_first_bool(settings_obj: Optional[object], env_key: str, settings_attr: str, default: bool) -> bool:
    v = _get_first_env(env_key)
    if v is not None:
        return _safe_bool(v, default)
    return _safe_bool(_get_attr(settings_obj, settings_attr, None), default)


def _resolve_version(settings_obj: Optional[object]) -> str:
    # 1) ENV FIRST (canonical)
    for k in ("APP_VERSION", "SERVICE_VERSION", "VERSION", "RELEASE"):
        vv = _normalize_version(_get_first_env(k))
        if vv:
            return vv

    # 2) Settings (config.py)
    for k in ("service_version", "version", "app_version", "APP_VERSION"):
        vv = _normalize_version(_get_attr(settings_obj, k, ""))
        if vv:
            return vv

    # 3) Render commit short hash
    commit = (os.getenv("RENDER_GIT_COMMIT") or os.getenv("GIT_COMMIT") or "").strip()
    if commit:
        return commit[:7]

    return "dev"


# ---------------------------------------------------------------------
# Build exports (prefer ENV, then settings, then defaults)
# ---------------------------------------------------------------------
_base_settings = _try_load_settings_once()

# Title/name (config.py uses service_name)
APP_NAME = (
    _get_first_env("APP_NAME", "SERVICE_NAME", "APP_TITLE", "TITLE")
    or str(_get_attr(_base_settings, "service_name", "") or "").strip()
    or str(_get_attr(_base_settings, "app_name", "") or _get_attr(_base_settings, "APP_NAME", "") or "").strip()
    or "Tadawul Fast Bridge"
)

# Env (config.py uses environment)
APP_ENV = (
    _get_first_env("APP_ENV", "ENVIRONMENT", "ENV")
    or str(_get_attr(_base_settings, "environment", "") or "").strip()
    or str(_get_attr(_base_settings, "env", "") or "").strip()
    or "production"
)

APP_VERSION = _resolve_version(_base_settings)

BACKEND_BASE_URL = (
    (_get_first_env("BACKEND_BASE_URL", "TFB_BASE_URL", "BASE_URL") or "").rstrip("/")
    or str(_get_attr(_base_settings, "backend_base_url", "") or "").rstrip("/")
    or ""
)

# Auth tokens (ENV first, then settings)
APP_TOKEN = (
    _get_first_env("APP_TOKEN", "TFB_APP_TOKEN")
    or str(_get_attr(_base_settings, "app_token", "") or "").strip()
    or None
)
BACKUP_APP_TOKEN = (
    _get_first_env("BACKUP_APP_TOKEN")
    or str(_get_attr(_base_settings, "backup_app_token", "") or "").strip()
    or None
)

REQUIRE_AUTH = _env_first_bool(_base_settings, "REQUIRE_AUTH", "require_auth", False)

# Ensure token aliases exist for legacy modules that read env directly (no overwrite)
if APP_TOKEN:
    _export_env_if_missing("APP_TOKEN", APP_TOKEN)
    _export_env_if_missing("TFB_APP_TOKEN", APP_TOKEN)
if BACKUP_APP_TOKEN:
    _export_env_if_missing("BACKUP_APP_TOKEN", BACKUP_APP_TOKEN)

# Providers (config.py exposes enabled_providers + ksa_providers)
_enabled_from_env = _get_first_env("ENABLED_PROVIDERS", "PROVIDERS")
if _enabled_from_env is not None:
    ENABLED_PROVIDERS: List[str] = _as_list_lower(_enabled_from_env)
else:
    ENABLED_PROVIDERS = _as_list_lower(_get_attr(_base_settings, "enabled_providers", None)) or _as_list_lower(
        _get_attr(_base_settings, "enabled_providers_raw", None) or "eodhd,finnhub"
    )

_ksa_from_env = _get_first_env("KSA_PROVIDERS")
if _ksa_from_env is not None:
    KSA_PROVIDERS: List[str] = _as_list_lower(_ksa_from_env)
else:
    KSA_PROVIDERS = _as_list_lower(_get_attr(_base_settings, "ksa_providers", None)) or _as_list_lower(
        _get_attr(_base_settings, "ksa_providers_raw", None) or "yahoo_chart,tadawul,argaam"
    )

PRIMARY_PROVIDER = (
    (_get_first_env("PRIMARY_PROVIDER") or "").strip().lower()
    or str(_get_attr(_base_settings, "primary_provider", "") or "").strip().lower()
    or (ENABLED_PROVIDERS[0] if ENABLED_PROVIDERS else "finnhub")
)

PROVIDERS = _safe_join(ENABLED_PROVIDERS)

# Export provider strings for older modules that read env directly
_export_env_if_missing("ENABLED_PROVIDERS", _safe_join(ENABLED_PROVIDERS))
_export_env_if_missing("PROVIDERS", _safe_join(ENABLED_PROVIDERS))
_export_env_if_missing("KSA_PROVIDERS", _safe_join(KSA_PROVIDERS))
_export_env_if_missing("PRIMARY_PROVIDER", PRIMARY_PROVIDER)

# Feature toggles (used by DataEngineV2 / providers; env-first, fallback to settings)
ENABLE_YAHOO_CHART_KSA = _safe_bool(
    _get_first_env("ENABLE_YAHOO_CHART_KSA") or _get_attr(_base_settings, "enable_yahoo_chart_ksa", None),
    True,
)
ENABLE_YAHOO_CHART_SUPPLEMENT = _safe_bool(
    _get_first_env("ENABLE_YAHOO_CHART_SUPPLEMENT") or _get_attr(_base_settings, "enable_yahoo_chart_supplement", None),
    True,
)
ENABLE_YFINANCE_KSA = _safe_bool(
    _get_first_env("ENABLE_YFINANCE_KSA") or _get_attr(_base_settings, "enable_yfinance_ksa", None),
    False,
)

ENABLE_YAHOO_FUNDAMENTALS_KSA = _safe_bool(
    _get_first_env("ENABLE_YAHOO_FUNDAMENTALS_KSA") or _get_attr(_base_settings, "enable_yahoo_fundamentals_ksa", None),
    True,
)
ENABLE_YAHOO_FUNDAMENTALS_GLOBAL = _safe_bool(
    _get_first_env("ENABLE_YAHOO_FUNDAMENTALS_GLOBAL") or _get_attr(_base_settings, "enable_yahoo_fundamentals_global", None),
    False,
)

_export_env_if_missing("ENABLE_YAHOO_CHART_KSA", str(ENABLE_YAHOO_CHART_KSA).lower())
_export_env_if_missing("ENABLE_YAHOO_CHART_SUPPLEMENT", str(ENABLE_YAHOO_CHART_SUPPLEMENT).lower())
_export_env_if_missing("ENABLE_YFINANCE_KSA", str(ENABLE_YFINANCE_KSA).lower())
_export_env_if_missing("ENABLE_YAHOO_FUNDAMENTALS_KSA", str(ENABLE_YAHOO_FUNDAMENTALS_KSA).lower())
_export_env_if_missing("ENABLE_YAHOO_FUNDAMENTALS_GLOBAL", str(ENABLE_YAHOO_FUNDAMENTALS_GLOBAL).lower())

# HTTP / cache
HTTP_TIMEOUT_SEC = _safe_float(
    _get_first_env("HTTP_TIMEOUT_SEC", "HTTP_TIMEOUT") or _get_attr(_base_settings, "http_timeout_sec", None),
    25.0,
)
HTTP_TIMEOUT_SEC = max(5.0, float(HTTP_TIMEOUT_SEC or 25.0))
HTTP_TIMEOUT = _safe_int(_get_first_env("HTTP_TIMEOUT") or int(HTTP_TIMEOUT_SEC), int(HTTP_TIMEOUT_SEC))

CACHE_TTL_SEC = _safe_float(
    _get_first_env("CACHE_TTL_SEC", "CACHE_DEFAULT_TTL") or _get_attr(_base_settings, "cache_ttl_sec", None),
    20.0,
)

ENGINE_CACHE_TTL_SEC = _safe_int(
    _get_first_env("ENGINE_CACHE_TTL_SEC", "ENGINE_TTL_SEC") or _get_attr(_base_settings, "engine_cache_ttl_sec", None),
    int(CACHE_TTL_SEC) if float(CACHE_TTL_SEC) > 0 else 20,
)

ENRICHED_BATCH_CONCURRENCY = _safe_int(
    _get_first_env("ENRICHED_BATCH_CONCURRENCY", "ENRICHED_CONCURRENCY")
    or _get_attr(_base_settings, "enriched_batch_concurrency", None),
    8,
)

# Timeout aliases used by older code
_export_env_if_missing("HTTP_TIMEOUT_SEC", str(float(HTTP_TIMEOUT_SEC)))
_export_env_if_missing("HTTP_TIMEOUT", str(float(HTTP_TIMEOUT_SEC)))

# TTL aliases used by older code
_export_env_if_missing("CACHE_TTL_SEC", str(float(CACHE_TTL_SEC)))
_export_env_if_missing("CACHE_DEFAULT_TTL", str(float(CACHE_TTL_SEC)))
_export_env_if_missing("ENGINE_CACHE_TTL_SEC", str(int(ENGINE_CACHE_TTL_SEC)))
_export_env_if_missing("ENRICHED_BATCH_CONCURRENCY", str(int(ENRICHED_BATCH_CONCURRENCY)))
_export_env_if_missing("ENRICHED_CONCURRENCY", str(int(ENRICHED_BATCH_CONCURRENCY)))

# Provider keys
EODHD_API_KEY = (
    _get_first_env("EODHD_API_KEY", "EODHD_API_TOKEN", "EODHD_TOKEN")
    or _get_attr(_base_settings, "eodhd_api_key", None)
)
FINNHUB_API_KEY = (
    _get_first_env("FINNHUB_API_KEY", "FINNHUB_API_TOKEN", "FINNHUB_TOKEN")
    or _get_attr(_base_settings, "finnhub_api_key", None)
)
FMP_API_KEY = _get_first_env("FMP_API_KEY") or _get_attr(_base_settings, "fmp_api_key", None)
ALPHA_VANTAGE_API_KEY = _get_first_env("ALPHA_VANTAGE_API_KEY") or _get_attr(_base_settings, "alpha_vantage_api_key", None)
ARGAAM_API_KEY = _get_first_env("ARGAAM_API_KEY") or _get_attr(_base_settings, "argaam_api_key", None)

# Export token aliases for legacy provider modules (no overwrite)
if FINNHUB_API_KEY:
    _export_env_if_missing("FINNHUB_API_TOKEN", FINNHUB_API_KEY)
    _export_env_if_missing("FINNHUB_TOKEN", FINNHUB_API_KEY)

if EODHD_API_KEY:
    _export_env_if_missing("EODHD_API_TOKEN", EODHD_API_KEY)
    _export_env_if_missing("EODHD_TOKEN", EODHD_API_KEY)

# CORS
ENABLE_CORS_ALL_ORIGINS = _env_first_bool(_base_settings, "ENABLE_CORS_ALL_ORIGINS", "enable_cors_all_origins", True)
CORS_ALL_ORIGINS = ENABLE_CORS_ALL_ORIGINS
CORS_ORIGINS = (
    _get_first_env("CORS_ORIGINS")
    or str(_get_attr(_base_settings, "cors_origins", "") or "").strip()
    or "*"
)
if ENABLE_CORS_ALL_ORIGINS:
    CORS_ORIGINS_LIST = ["*"]
else:
    CORS_ORIGINS_LIST = [x.strip() for x in (CORS_ORIGINS or "").split(",") if x.strip()] or []

# Google Sheets (ENV first, then settings)
_google_creds_raw = (
    _get_first_env("GOOGLE_SHEETS_CREDENTIALS", "GOOGLE_CREDENTIALS", "GOOGLE_SA_JSON")
    or str(_get_attr(_base_settings, "google_sheets_credentials", "") or "").strip()
    or str(_get_attr(_base_settings, "google_credentials", "") or "").strip()
    or ""
).strip()
GOOGLE_SHEETS_CREDENTIALS: Optional[Dict[str, Any]] = _try_parse_json_dict(_google_creds_raw)

DEFAULT_SPREADSHEET_ID = (
    (_get_first_env("DEFAULT_SPREADSHEET_ID", "SPREADSHEET_ID", "GOOGLE_SHEETS_ID") or "").strip()
    or str(_get_attr(_base_settings, "default_spreadsheet_id", "") or "").strip()
    or ""
)
SPREADSHEET_ID = DEFAULT_SPREADSHEET_ID
GOOGLE_SHEETS_ID = (_get_first_env("GOOGLE_SHEETS_ID") or "").strip()

GOOGLE_APPS_SCRIPT_URL = (
    (_get_first_env("GOOGLE_APPS_SCRIPT_URL") or str(_get_attr(_base_settings, "google_apps_script_url", "") or "")).strip()
)
GOOGLE_APPS_SCRIPT_BACKUP_URL = (
    (_get_first_env("GOOGLE_APPS_SCRIPT_BACKUP_URL") or str(_get_attr(_base_settings, "google_apps_script_backup_url", "") or "")).strip()
)

# Export creds/spreadsheet env aliases for older modules (no overwrite)
if DEFAULT_SPREADSHEET_ID:
    _export_env_if_missing("DEFAULT_SPREADSHEET_ID", DEFAULT_SPREADSHEET_ID)
    _export_env_if_missing("SPREADSHEET_ID", DEFAULT_SPREADSHEET_ID)
    _export_env_if_missing("GOOGLE_SHEETS_ID", DEFAULT_SPREADSHEET_ID)

# Sheet names (defaults aligned to your current API usage)
SHEET_KSA_TADAWUL = (_get_first_env("SHEET_KSA_TADAWUL") or "KSA_Tadawul").strip()
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
LOG_LEVEL = (_get_first_env("LOG_LEVEL") or str(_get_attr(_base_settings, "log_level", "") or "") or "INFO").upper()
LOG_JSON = _safe_bool(_get_first_env("LOG_JSON"), False)
IS_PRODUCTION = str(APP_ENV).strip().lower() in {"prod", "production"}


# ---------------------------------------------------------------------
# settings compatibility object (very lightweight)
# ---------------------------------------------------------------------
class _Settings:
    # historical names
    app_name = APP_NAME
    env = APP_ENV
    environment = APP_ENV
    version = APP_VERSION
    service_version = APP_VERSION

    backend_base_url = BACKEND_BASE_URL
    base_url = BACKEND_BASE_URL

    app_token = APP_TOKEN or ""
    backup_app_token = BACKUP_APP_TOKEN or ""
    require_auth = REQUIRE_AUTH

    enabled_providers = ENABLED_PROVIDERS
    ksa_providers = KSA_PROVIDERS
    enabled_ksa_providers = KSA_PROVIDERS
    primary_provider = PRIMARY_PROVIDER

    http_timeout_sec = HTTP_TIMEOUT_SEC
    cache_ttl_sec = CACHE_TTL_SEC
    engine_cache_ttl_sec = ENGINE_CACHE_TTL_SEC
    enriched_batch_concurrency = ENRICHED_BATCH_CONCURRENCY

    enable_yahoo_chart_ksa = ENABLE_YAHOO_CHART_KSA
    enable_yahoo_chart_supplement = ENABLE_YAHOO_CHART_SUPPLEMENT
    enable_yfinance_ksa = ENABLE_YFINANCE_KSA
    enable_yahoo_fundamentals_ksa = ENABLE_YAHOO_FUNDAMENTALS_KSA
    enable_yahoo_fundamentals_global = ENABLE_YAHOO_FUNDAMENTALS_GLOBAL

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
        "features": {
            "yahoo_chart_ksa": ENABLE_YAHOO_CHART_KSA,
            "yahoo_chart_supplement": ENABLE_YAHOO_CHART_SUPPLEMENT,
            "yfinance_ksa": ENABLE_YFINANCE_KSA,
            "yahoo_fundamentals_ksa": ENABLE_YAHOO_FUNDAMENTALS_KSA,
            "yahoo_fundamentals_global": ENABLE_YAHOO_FUNDAMENTALS_GLOBAL,
        },
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
    "ENABLE_YAHOO_CHART_KSA",
    "ENABLE_YAHOO_CHART_SUPPLEMENT",
    "ENABLE_YFINANCE_KSA",
    "ENABLE_YAHOO_FUNDAMENTALS_KSA",
    "ENABLE_YAHOO_FUNDAMENTALS_GLOBAL",
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
