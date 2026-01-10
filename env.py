# env.py  (FULL REPLACEMENT) — v5.2.0
"""
env.py
------------------------------------------------------------
Backward-compatible environment exports for Tadawul Fast Bridge (v5.2.0)

Key goals
- ✅ Quiet boot by default (no banner logs unless ENV_LOG_ON_BOOT=true)
- ✅ Version resolves from ENV FIRST: APP_VERSION / SERVICE_VERSION / VERSION / RELEASE
- ✅ Prefers core.config.get_settings() (canonical), then repo-root config.get_settings()
- ✅ Provides legacy exports used by older modules/routes (settings object + aliases)
- ✅ Ensures token alias exports (FINNHUB_API_TOKEN / EODHD_API_TOKEN) for legacy modules
- ✅ Keeps ENGINE_CACHE_TTL_SEC + ENRICHED_BATCH_CONCURRENCY (aligns with ENRICHED_CONCURRENCY)
- ✅ Robust GOOGLE_SHEETS_CREDENTIALS parsing: supports:
    - minified JSON
    - pretty JSON
    - base64(JSON)
    - quoted JSON string (single/double)
- ✅ Exposes provider URLs used by KSA modules:
    - TADAWUL_QUOTE_URL / TADAWUL_FUNDAMENTALS_URL / ARGAAM_BASE_URL
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


ENV_VERSION = "5.2.0"
logger = logging.getLogger("env")

_TRUTHY = {"1", "true", "yes", "y", "on", "t", "enable", "enabled", "ok"}
_FALSY = {"0", "false", "no", "n", "off", "f", "disable", "disabled"}


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


def _looks_like_json_object(s: str) -> bool:
    ss = (s or "").strip()
    return ss.startswith("{") and ss.endswith("}") and len(ss) >= 2


def _looks_like_b64(s: str) -> bool:
    raw = (s or "").strip()
    if len(raw) < 40:
        return False
    allowed = set("ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/=\n\r")
    return all((c in allowed) for c in raw)


def _maybe_b64_decode_json(s: str) -> str:
    """
    If s looks like base64 and decodes to JSON, return decoded.
    Otherwise return original.
    """
    raw = (s or "").strip()
    if not raw:
        return raw
    if _looks_like_json_object(raw):
        return raw
    if not _looks_like_b64(raw):
        return raw

    try:
        dec = base64.b64decode(raw.encode("utf-8"), validate=False).decode("utf-8", errors="strict").strip()
        if _looks_like_json_object(dec):
            # must be valid JSON
            json.loads(dec)
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
    s = _maybe_b64_decode_json(s)
    s = _strip_wrapping_quotes(s).strip()

    if not _looks_like_json_object(s):
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
    """
    Accepts:
      - list
      - "a,b,c"
      - '["a","b"]'
      - "['a','b']" (best-effort)
    Returns: deduped lowercase list
    """
    if v is None:
        return []

    raw: List[str] = []
    if isinstance(v, list):
        raw = [str(x).strip().lower() for x in v if str(x).strip()]
    else:
        s = str(v).strip()
        if not s:
            return []
        if (s.startswith("[") and s.endswith("]")) or (s.startswith("(") and s.endswith(")")):
            try:
                arr = json.loads(s)
                if isinstance(arr, list):
                    raw = [str(x).strip().lower() for x in arr if str(x).strip()]
                else:
                    raw = []
            except Exception:
                try:
                    ss = s.strip()
                    if ss.startswith("(") and ss.endswith(")"):
                        ss = "[" + ss[1:-1] + "]"
                    ss = ss.replace("'", '"')
                    arr = json.loads(ss)
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
# Prefer core.config first (canonical), then repo-root config.
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
        # 1) canonical
        try:
            from core.config import get_settings  # type: ignore

            _SETTINGS_OBJ = get_settings()
            _SETTINGS_LOADED = True
            return _SETTINGS_OBJ
        except Exception:
            pass

        # 2) fallback
        try:
            from config import get_settings  # type: ignore

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
        v = getattr(obj, name, default)
        if v is None:
            return default
        if isinstance(v, str) and not v.strip():
            return default
        return v
    except Exception:
        return default


def _env_first_bool(settings_obj: Optional[object], env_key: str, settings_attr: str, default: bool) -> bool:
    v = _get_first_env(env_key)
    if v is not None:
        return _safe_bool(v, default)
    return _safe_bool(_get_attr(settings_obj, settings_attr, None), default)


def _resolve_version(settings_obj: Optional[object]) -> str:
    # 1) ENV FIRST
    for k in ("APP_VERSION", "SERVICE_VERSION", "VERSION", "RELEASE"):
        vv = _normalize_version(_get_first_env(k))
        if vv:
            return vv

    # 2) Settings (try common attribute names)
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

APP_NAME = (
    _get_first_env("APP_NAME", "SERVICE_NAME", "APP_TITLE", "TITLE")
    or str(_get_attr(_base_settings, "service_name", "") or "").strip()
    or str(_get_attr(_base_settings, "app_name", "") or _get_attr(_base_settings, "APP_NAME", "") or "").strip()
    or "Tadawul Fast Bridge"
)

APP_ENV = (
    _get_first_env("APP_ENV", "ENVIRONMENT", "ENV")
    or str(_get_attr(_base_settings, "environment", "") or "").strip()
    or str(_get_attr(_base_settings, "env", "") or "").strip()
    or "production"
).lower()

APP_VERSION = _resolve_version(_base_settings)

LOG_LEVEL = (
    _get_first_env("LOG_LEVEL")
    or str(_get_attr(_base_settings, "log_level", "") or "").strip()
    or "INFO"
).upper()

TIMEZONE_DEFAULT = (
    _get_first_env("TIMEZONE_DEFAULT", "TZ")
    or str(_get_attr(_base_settings, "timezone_default", "") or "").strip()
    or "Asia/Riyadh"
)

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

# Legacy flag (routes use "open if tokens missing"; keep for older code)
REQUIRE_AUTH = _env_first_bool(_base_settings, "REQUIRE_AUTH", "require_auth", False)

# Export token aliases for legacy modules (no overwrite)
if APP_TOKEN:
    _export_env_if_missing("APP_TOKEN", APP_TOKEN)
    _export_env_if_missing("TFB_APP_TOKEN", APP_TOKEN)
if BACKUP_APP_TOKEN:
    _export_env_if_missing("BACKUP_APP_TOKEN", BACKUP_APP_TOKEN)

# AI batching (env overrides settings)
AI_BATCH_SIZE = _safe_int(_get_first_env("AI_BATCH_SIZE") or _get_attr(_base_settings, "ai_batch_size", None), 20)
AI_BATCH_TIMEOUT_SEC = _safe_float(
    _get_first_env("AI_BATCH_TIMEOUT_SEC") or _get_attr(_base_settings, "ai_batch_timeout_sec", None),
    45.0,
)
AI_BATCH_CONCURRENCY = _safe_int(
    _get_first_env("AI_BATCH_CONCURRENCY") or _get_attr(_base_settings, "ai_batch_concurrency", None),
    5,
)
AI_MAX_TICKERS = _safe_int(_get_first_env("AI_MAX_TICKERS") or _get_attr(_base_settings, "ai_max_tickers", None), 500)

AI_BATCH_SIZE = max(5, min(250, AI_BATCH_SIZE))
AI_BATCH_TIMEOUT_SEC = max(5.0, min(180.0, float(AI_BATCH_TIMEOUT_SEC)))
AI_BATCH_CONCURRENCY = max(1, min(30, AI_BATCH_CONCURRENCY))
AI_MAX_TICKERS = max(10, min(3000, AI_MAX_TICKERS))

_export_env_if_missing("AI_BATCH_SIZE", str(int(AI_BATCH_SIZE)))
_export_env_if_missing("AI_BATCH_TIMEOUT_SEC", str(float(AI_BATCH_TIMEOUT_SEC)))
_export_env_if_missing("AI_BATCH_CONCURRENCY", str(int(AI_BATCH_CONCURRENCY)))
_export_env_if_missing("AI_MAX_TICKERS", str(int(AI_MAX_TICKERS)))

# Provider URLs (ENV first, then settings)
TADAWUL_QUOTE_URL = (
    _get_first_env("TADAWUL_QUOTE_URL") or str(_get_attr(_base_settings, "tadawul_quote_url", "") or "").strip() or ""
)
TADAWUL_FUNDAMENTALS_URL = (
    _get_first_env("TADAWUL_FUNDAMENTALS_URL")
    or str(_get_attr(_base_settings, "tadawul_fundamentals_url", "") or "").strip()
    or ""
)
ARGAAM_BASE_URL = (
    _get_first_env("ARGAAM_BASE_URL") or str(_get_attr(_base_settings, "argaam_base_url", "") or "").strip() or ""
)

_export_env_if_missing("TADAWUL_QUOTE_URL", TADAWUL_QUOTE_URL)
_export_env_if_missing("TADAWUL_FUNDAMENTALS_URL", TADAWUL_FUNDAMENTALS_URL)
_export_env_if_missing("ARGAAM_BASE_URL", ARGAAM_BASE_URL)

# Optional toggle (legacy). If config doesn't have it, default True.
ENABLE_YAHOO = _safe_bool(_get_first_env("ENABLE_YAHOO") or _get_attr(_base_settings, "enable_yahoo", None), True)
_export_env_if_missing("ENABLE_YAHOO", str(bool(ENABLE_YAHOO)).lower())

# Providers lists (env-first; fallbacks aligned with config default)
_enabled_from_env = _get_first_env("ENABLED_PROVIDERS", "PROVIDERS")
if _enabled_from_env is not None:
    ENABLED_PROVIDERS: List[str] = _as_list_lower(_enabled_from_env)
else:
    ENABLED_PROVIDERS = _as_list_lower(_get_attr(_base_settings, "enabled_providers", None)) or ["eodhd", "finnhub"]

_ksa_from_env = _get_first_env("KSA_PROVIDERS")
if _ksa_from_env is not None:
    KSA_PROVIDERS: List[str] = _as_list_lower(_ksa_from_env)
else:
    KSA_PROVIDERS = _as_list_lower(_get_attr(_base_settings, "ksa_providers", None)) or ["yahoo_chart"]

PRIMARY_PROVIDER = (
    (_get_first_env("PRIMARY_PROVIDER") or "").strip().lower()
    or str(_get_attr(_base_settings, "primary_provider", "") or "").strip().lower()
    or (ENABLED_PROVIDERS[0] if ENABLED_PROVIDERS else "eodhd")
)

PROVIDERS = _safe_join(ENABLED_PROVIDERS)

_export_env_if_missing("ENABLED_PROVIDERS", _safe_join(ENABLED_PROVIDERS))
_export_env_if_missing("PROVIDERS", _safe_join(ENABLED_PROVIDERS))
_export_env_if_missing("KSA_PROVIDERS", _safe_join(KSA_PROVIDERS))
_export_env_if_missing("PRIMARY_PROVIDER", PRIMARY_PROVIDER)

# Feature toggles (legacy provider modules; env-first, safe defaults)
ENABLE_YAHOO_CHART_KSA = _safe_bool(_get_first_env("ENABLE_YAHOO_CHART_KSA"), True)
ENABLE_YAHOO_CHART_SUPPLEMENT = _safe_bool(_get_first_env("ENABLE_YAHOO_CHART_SUPPLEMENT"), True)
ENABLE_YFINANCE_KSA = _safe_bool(_get_first_env("ENABLE_YFINANCE_KSA"), False)

ENABLE_YAHOO_FUNDAMENTALS_KSA = _safe_bool(_get_first_env("ENABLE_YAHOO_FUNDAMENTALS_KSA"), True)
ENABLE_YAHOO_FUNDAMENTALS_GLOBAL = _safe_bool(_get_first_env("ENABLE_YAHOO_FUNDAMENTALS_GLOBAL"), False)

_export_env_if_missing("ENABLE_YAHOO_CHART_KSA", str(bool(ENABLE_YAHOO_CHART_KSA)).lower())
_export_env_if_missing("ENABLE_YAHOO_CHART_SUPPLEMENT", str(bool(ENABLE_YAHOO_CHART_SUPPLEMENT)).lower())
_export_env_if_missing("ENABLE_YFINANCE_KSA", str(bool(ENABLE_YFINANCE_KSA)).lower())
_export_env_if_missing("ENABLE_YAHOO_FUNDAMENTALS_KSA", str(bool(ENABLE_YAHOO_FUNDAMENTALS_KSA)).lower())
_export_env_if_missing("ENABLE_YAHOO_FUNDAMENTALS_GLOBAL", str(bool(ENABLE_YAHOO_FUNDAMENTALS_GLOBAL)).lower())

# HTTP / cache (legacy)
HTTP_TIMEOUT_SEC = _safe_float(_get_first_env("HTTP_TIMEOUT_SEC", "HTTP_TIMEOUT"), 25.0)
HTTP_TIMEOUT_SEC = max(5.0, float(HTTP_TIMEOUT_SEC or 25.0))
HTTP_TIMEOUT = _safe_int(_get_first_env("HTTP_TIMEOUT") or int(HTTP_TIMEOUT_SEC), int(HTTP_TIMEOUT_SEC))

CACHE_TTL_SEC = _safe_float(_get_first_env("CACHE_TTL_SEC", "CACHE_DEFAULT_TTL"), 20.0)
ENGINE_CACHE_TTL_SEC = _safe_int(_get_first_env("ENGINE_CACHE_TTL_SEC", "ENGINE_TTL_SEC"), int(CACHE_TTL_SEC) or 20)

ENRICHED_BATCH_CONCURRENCY = _safe_int(_get_first_env("ENRICHED_BATCH_CONCURRENCY", "ENRICHED_CONCURRENCY"), 5)

_export_env_if_missing("HTTP_TIMEOUT_SEC", str(float(HTTP_TIMEOUT_SEC)))
_export_env_if_missing("HTTP_TIMEOUT", str(float(HTTP_TIMEOUT_SEC)))
_export_env_if_missing("CACHE_TTL_SEC", str(float(CACHE_TTL_SEC)))
_export_env_if_missing("CACHE_DEFAULT_TTL", str(float(CACHE_TTL_SEC)))
_export_env_if_missing("ENGINE_CACHE_TTL_SEC", str(int(ENGINE_CACHE_TTL_SEC)))
_export_env_if_missing("ENRICHED_BATCH_CONCURRENCY", str(int(ENRICHED_BATCH_CONCURRENCY)))
_export_env_if_missing("ENRICHED_CONCURRENCY", str(int(ENRICHED_BATCH_CONCURRENCY)))

# Provider keys (legacy; env-first)
EODHD_API_KEY = _get_first_env("EODHD_API_KEY", "EODHD_API_TOKEN", "EODHD_TOKEN")
FINNHUB_API_KEY = _get_first_env("FINNHUB_API_KEY", "FINNHUB_API_TOKEN", "FINNHUB_TOKEN")
FMP_API_KEY = _get_first_env("FMP_API_KEY")
ALPHA_VANTAGE_API_KEY = _get_first_env("ALPHA_VANTAGE_API_KEY")
ARGAAM_API_KEY = _get_first_env("ARGAAM_API_KEY")

# Export token aliases for legacy provider modules (no overwrite)
if FINNHUB_API_KEY:
    _export_env_if_missing("FINNHUB_API_TOKEN", FINNHUB_API_KEY)
    _export_env_if_missing("FINNHUB_TOKEN", FINNHUB_API_KEY)
if EODHD_API_KEY:
    _export_env_if_missing("EODHD_API_TOKEN", EODHD_API_KEY)
    _export_env_if_missing("EODHD_TOKEN", EODHD_API_KEY)

# CORS (env-first, fallback to config cors_origins if present)
ENABLE_CORS_ALL_ORIGINS = _env_first_bool(_base_settings, "ENABLE_CORS_ALL_ORIGINS", "enable_cors_all_origins", True)
CORS_ALL_ORIGINS = ENABLE_CORS_ALL_ORIGINS

_cors_from_env = _get_first_env("CORS_ORIGINS")
if _cors_from_env is not None:
    CORS_ORIGINS = _cors_from_env.strip()
else:
    cors_attr = _get_attr(_base_settings, "cors_origins", None)
    if isinstance(cors_attr, list):
        CORS_ORIGINS = _safe_join([str(x) for x in cors_attr if str(x).strip()]) or "*"
    else:
        CORS_ORIGINS = str(cors_attr or "*").strip() or "*"

if ENABLE_CORS_ALL_ORIGINS:
    CORS_ORIGINS_LIST = ["*"]
else:
    CORS_ORIGINS_LIST = [x.strip() for x in (CORS_ORIGINS or "").split(",") if x.strip()] or []

_export_env_if_missing("CORS_ORIGINS", CORS_ORIGINS)

# Google Sheets (ENV first, then settings)
_google_creds_raw = (
    _get_first_env("GOOGLE_SHEETS_CREDENTIALS", "GOOGLE_CREDENTIALS", "GOOGLE_SERVICE_ACCOUNT_JSON", "GOOGLE_SA_JSON")
    or str(_get_attr(_base_settings, "google_sheets_credentials", "") or "").strip()
    or ""
).strip()

GOOGLE_SHEETS_CREDENTIALS: Optional[Dict[str, Any]] = _try_parse_json_dict(_google_creds_raw)

DEFAULT_SPREADSHEET_ID = (
    (_get_first_env("DEFAULT_SPREADSHEET_ID", "SPREADSHEET_ID", "GOOGLE_SHEETS_ID") or "").strip()
    or str(_get_attr(_base_settings, "default_spreadsheet_id", "") or "").strip()
    or str(_get_attr(_base_settings, "tfb_spreadsheet_id", "") or "").strip()
    or ""
)

TFB_SPREADSHEET_ID = (
    (_get_first_env("TFB_SPREADSHEET_ID") or "").strip()
    or str(_get_attr(_base_settings, "tfb_spreadsheet_id", "") or "").strip()
    or DEFAULT_SPREADSHEET_ID
)

SPREADSHEET_ID = DEFAULT_SPREADSHEET_ID
GOOGLE_SHEETS_ID = (_get_first_env("GOOGLE_SHEETS_ID") or "").strip()

# Export for older modules (no overwrite)
if DEFAULT_SPREADSHEET_ID:
    _export_env_if_missing("DEFAULT_SPREADSHEET_ID", DEFAULT_SPREADSHEET_ID)
    _export_env_if_missing("SPREADSHEET_ID", DEFAULT_SPREADSHEET_ID)
    _export_env_if_missing("GOOGLE_SHEETS_ID", DEFAULT_SPREADSHEET_ID)
if TFB_SPREADSHEET_ID:
    _export_env_if_missing("TFB_SPREADSHEET_ID", TFB_SPREADSHEET_ID)

# Sheet names (defaults aligned with dashboard tabs)
SHEET_KSA_TADAWUL = (_get_first_env("SHEET_KSA_TADAWUL") or "KSA_Tadawul").strip()
SHEET_GLOBAL_MARKETS = (_get_first_env("SHEET_GLOBAL_MARKETS") or "Global_Markets").strip()
SHEET_MUTUAL_FUNDS = (_get_first_env("SHEET_MUTUAL_FUNDS") or "Mutual_Funds").strip()
SHEET_COMMODITIES_FX = (_get_first_env("SHEET_COMMODITIES_FX") or "Commodities_FX").strip()
SHEET_MARKET_LEADERS = (_get_first_env("SHEET_MARKET_LEADERS") or "Market_Leaders").strip()
SHEET_MY_PORTFOLIO = (_get_first_env("SHEET_MY_PORTFOLIO") or "My_Portfolio").strip()
SHEET_INSIGHTS_ANALYSIS = (_get_first_env("SHEET_INSIGHTS_ANALYSIS") or "Insights_Analysis").strip()

# Logging flags
LOG_JSON = _safe_bool(_get_first_env("LOG_JSON"), False)
IS_PRODUCTION = str(APP_ENV).strip().lower() in {"prod", "production"}


# ---------------------------------------------------------------------
# settings compatibility object (very lightweight)
# ---------------------------------------------------------------------
class _Settings:
    # identity
    app_name = APP_NAME
    service_name = APP_NAME
    env = APP_ENV
    environment = APP_ENV
    log_level = LOG_LEVEL
    version = APP_VERSION
    service_version = APP_VERSION
    timezone_default = TIMEZONE_DEFAULT

    # base url
    backend_base_url = BACKEND_BASE_URL
    base_url = BACKEND_BASE_URL

    # auth
    app_token = (APP_TOKEN or "")
    backup_app_token = (BACKUP_APP_TOKEN or "")
    require_auth = REQUIRE_AUTH

    # AI batch controls
    ai_batch_size = AI_BATCH_SIZE
    ai_batch_timeout_sec = AI_BATCH_TIMEOUT_SEC
    ai_batch_concurrency = AI_BATCH_CONCURRENCY
    ai_max_tickers = AI_MAX_TICKERS

    # providers
    enabled_providers = ENABLED_PROVIDERS
    ksa_providers = KSA_PROVIDERS
    enabled_ksa_providers = KSA_PROVIDERS
    primary_provider = PRIMARY_PROVIDER

    # timeouts/ttl
    http_timeout_sec = HTTP_TIMEOUT_SEC
    cache_ttl_sec = CACHE_TTL_SEC
    engine_cache_ttl_sec = ENGINE_CACHE_TTL_SEC
    enriched_batch_concurrency = ENRICHED_BATCH_CONCURRENCY

    # provider urls
    tadawul_quote_url = TADAWUL_QUOTE_URL
    tadawul_fundamentals_url = TADAWUL_FUNDAMENTALS_URL
    argaam_base_url = ARGAAM_BASE_URL
    enable_yahoo = ENABLE_YAHOO

    # legacy toggles
    enable_yahoo_chart_ksa = ENABLE_YAHOO_CHART_KSA
    enable_yahoo_chart_supplement = ENABLE_YAHOO_CHART_SUPPLEMENT
    enable_yfinance_ksa = ENABLE_YFINANCE_KSA
    enable_yahoo_fundamentals_ksa = ENABLE_YAHOO_FUNDAMENTALS_KSA
    enable_yahoo_fundamentals_global = ENABLE_YAHOO_FUNDAMENTALS_GLOBAL

    # sheets
    google_credentials_dict = GOOGLE_SHEETS_CREDENTIALS
    google_sheets_credentials = GOOGLE_SHEETS_CREDENTIALS  # legacy expects dict
    default_spreadsheet_id = DEFAULT_SPREADSHEET_ID
    tfb_spreadsheet_id = TFB_SPREADSHEET_ID
    spreadsheet_id = DEFAULT_SPREADSHEET_ID
    has_google_sheets = bool(GOOGLE_SHEETS_CREDENTIALS) and bool(DEFAULT_SPREADSHEET_ID)


settings: object = _Settings()


def safe_env_summary() -> Dict[str, Any]:
    return {
        "app": APP_NAME,
        "env": APP_ENV,
        "version": APP_VERSION,
        "env_py_version": ENV_VERSION,
        "log_level": LOG_LEVEL,
        "timezone_default": TIMEZONE_DEFAULT,
        "backend_base_url": BACKEND_BASE_URL,
        "auth": {"require_auth_flag": REQUIRE_AUTH, "app_token_mask": _mask_tail(APP_TOKEN or "", keep=4)},
        "ai_limits": {
            "ai_batch_size": AI_BATCH_SIZE,
            "ai_batch_timeout_sec": AI_BATCH_TIMEOUT_SEC,
            "ai_batch_concurrency": AI_BATCH_CONCURRENCY,
            "ai_max_tickers": AI_MAX_TICKERS,
        },
        "providers": {
            "enabled": ENABLED_PROVIDERS,
            "ksa": KSA_PROVIDERS,
            "primary": PRIMARY_PROVIDER,
            "enable_yahoo": ENABLE_YAHOO,
            "urls_set": {
                "tadawul_quote_url": bool(TADAWUL_QUOTE_URL),
                "tadawul_fundamentals_url": bool(TADAWUL_FUNDAMENTALS_URL),
                "argaam_base_url": bool(ARGAAM_BASE_URL),
            },
        },
        "timeouts": {
            "http_timeout_sec": HTTP_TIMEOUT_SEC,
            "cache_ttl_sec": CACHE_TTL_SEC,
            "engine_cache_ttl_sec": ENGINE_CACHE_TTL_SEC,
            "enriched_batch_concurrency": ENRICHED_BATCH_CONCURRENCY,
        },
        "sheets": {
            "has_google_sheets": bool(GOOGLE_SHEETS_CREDENTIALS) and bool(DEFAULT_SPREADSHEET_ID),
            "default_spreadsheet_id_set": bool(DEFAULT_SPREADSHEET_ID),
            "tfb_spreadsheet_id_set": bool(TFB_SPREADSHEET_ID),
        },
        "keys_present": {
            "eodhd": bool(EODHD_API_KEY),
            "finnhub": bool(FINNHUB_API_KEY),
            "fmp": bool(FMP_API_KEY),
            "alpha_vantage": bool(ALPHA_VANTAGE_API_KEY),
            "argaam": bool(ARGAAM_API_KEY),
        },
        "cors": {"all": CORS_ALL_ORIGINS, "origins": CORS_ORIGINS_LIST if not CORS_ALL_ORIGINS else ["*"]},
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
    "LOG_LEVEL",
    "TIMEZONE_DEFAULT",
    "BACKEND_BASE_URL",
    "APP_TOKEN",
    "BACKUP_APP_TOKEN",
    "REQUIRE_AUTH",
    "AI_BATCH_SIZE",
    "AI_BATCH_TIMEOUT_SEC",
    "AI_BATCH_CONCURRENCY",
    "AI_MAX_TICKERS",
    "TADAWUL_QUOTE_URL",
    "TADAWUL_FUNDAMENTALS_URL",
    "ARGAAM_BASE_URL",
    "ENABLE_YAHOO",
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
    "TFB_SPREADSHEET_ID",
    "SPREADSHEET_ID",
    "GOOGLE_SHEETS_ID",
    "SHEET_KSA_TADAWUL",
    "SHEET_GLOBAL_MARKETS",
    "SHEET_MUTUAL_FUNDS",
    "SHEET_COMMODITIES_FX",
    "SHEET_MARKET_LEADERS",
    "SHEET_MY_PORTFOLIO",
    "SHEET_INSIGHTS_ANALYSIS",
    "LOG_JSON",
    "IS_PRODUCTION",
]
