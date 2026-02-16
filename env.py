#!/usr/bin/env python3
# env.py
"""
env.py
------------------------------------------------------------
Backward-compatible environment exports for Tadawul Fast Bridge (v6.2.0)
Advanced Production Edition (Aligned + Enhanced)

Design goals
- ✅ Central Truth: OS env (highest) → config.get_settings() → defaults
- ✅ Backward-Compatible: keeps `settings` object + legacy module constants
- ✅ Deep Validation: validate_environment() returns warnings/errors (non-fatal)
- ✅ Provider Auto-Discovery: based on available API keys and enable flags
- ✅ Secret Hygiene: repairs malformed JSON credentials (Base64, escaped \\n)
- ✅ Riyadh Defaults: timezone defaults to Asia/Riyadh unless overridden
- ✅ Render-friendly: supports Render env groups and common key variants

IMPORTANT
- This module must NOT crash on import (CI/CD safe).
- If config.py exists, we read it; if not, we gracefully degrade.

Compatible with your Render Environment Variables list (examples):
APP_ENV / ENVIRONMENT, APP_NAME / SERVICE_NAME, APP_VERSION / VERSION,
BACKEND_BASE_URL / TFB_BASE_URL, APP_TOKEN / BACKUP_APP_TOKEN,
AI_ANALYSIS_ENABLED, ADVANCED_ANALYSIS_ENABLED, ENABLE_RATE_LIMITING,
ENABLED_PROVIDERS / PROVIDERS, KSA_PROVIDERS, PRIMARY_PROVIDER,
ENGINE_CACHE_TTL_SEC, HTTP_TIMEOUT_SEC / HTTP_TIMEOUT,
GOOGLE_SHEETS_CREDENTIALS, DEFAULT_SPREADSHEET_ID, etc.
"""

from __future__ import annotations

import base64
import json
import logging
import os
import re
import sys
from typing import Any, Dict, List, Optional, Tuple

ENV_VERSION = "6.2.0"
logger = logging.getLogger("env")

_TRUTHY = {"1", "true", "yes", "y", "on", "t", "enable", "enabled", "ok"}
_FALSY = {"0", "false", "no", "n", "off", "f", "disable", "disabled"}

# -----------------------------------------------------------------------------
# Internal helpers (safe + robust)
# -----------------------------------------------------------------------------
def _strip(v: Any) -> str:
    try:
        return str(v).strip()
    except Exception:
        return ""


def _strip_wrapping_quotes(s: str) -> str:
    t = _strip(s)
    if len(t) >= 2 and ((t[0] == t[-1] == '"') or (t[0] == t[-1] == "'")):
        return t[1:-1].strip()
    return t


def _safe_bool(v: Any, default: bool = False) -> bool:
    if isinstance(v, bool):
        return v
    s = _strip(v).lower()
    if not s:
        return default
    if s in _TRUTHY:
        return True
    if s in _FALSY:
        return False
    return default


def _safe_int(v: Any, default: int, *, lo: Optional[int] = None, hi: Optional[int] = None) -> int:
    try:
        x = int(float(_strip(v)))
    except Exception:
        x = default
    if lo is not None and x < lo:
        x = lo
    if hi is not None and x > hi:
        x = hi
    return x


def _safe_float(v: Any, default: float, *, lo: Optional[float] = None, hi: Optional[float] = None) -> float:
    try:
        x = float(_strip(v))
    except Exception:
        x = default
    if lo is not None and x < lo:
        x = lo
    if hi is not None and x > hi:
        x = hi
    return x


def _mask_secret(s: Optional[str], keep: int = 3) -> str:
    x = _strip(s)
    if not x:
        return "MISSING"
    if len(x) < 8:
        return "***"
    return f"{x[:keep]}...{x[-keep:]}"


def _get_first_env(*keys: str) -> Optional[str]:
    for k in keys:
        v = os.getenv(k)
        if v is not None and _strip(v):
            return _strip(v)
    return None


def _as_list_lower(v: Any) -> List[str]:
    """
    Robust parser for CSV, JSON arrays, newlines, and Python-style lists.
    Accepts separators: comma, semicolon, whitespace, newline, pipe.
    """
    if v is None:
        return []
    s = _strip(v)
    if not s:
        return []

    # JSON / Python-ish list
    if s.startswith(("[", "(")):
        try:
            s_clean = s.replace("'", '"')
            arr = json.loads(s_clean)
            if isinstance(arr, list):
                out: List[str] = []
                seen = set()
                for x in arr:
                    t = _strip(x).lower()
                    if t and t not in seen:
                        seen.add(t)
                        out.append(t)
                return out
        except Exception:
            pass

    parts = re.split(r"[\s,;|]+", s)
    out2: List[str] = []
    seen2 = set()
    for x in parts:
        t = _strip(x).lower()
        if t and t not in seen2:
            seen2.add(t)
            out2.append(t)
    return out2


def _repair_private_key(key: str) -> str:
    if "\\n" in key:
        return key.replace("\\n", "\n")
    return key


def _validate_json_creds_to_dict(raw: str) -> Optional[Dict[str, Any]]:
    """
    Validator/repair for Service Account JSON.
    Handles Base64, raw JSON, and double-escaped private keys.
    Returns dict if valid-ish, else None.
    """
    t = _strip_wrapping_quotes(raw or "")
    if not t:
        return None

    # Base64 decode attempt
    if not t.startswith("{") and len(t) > 50:
        try:
            decoded = base64.b64decode(t, validate=False).decode("utf-8", errors="replace").strip()
            if decoded.startswith("{"):
                t = decoded
        except Exception:
            pass

    try:
        obj = json.loads(t)
        if isinstance(obj, dict):
            pk = obj.get("private_key")
            if isinstance(pk, str) and pk:
                obj["private_key"] = _repair_private_key(pk)
            return obj
    except Exception:
        return None
    return None


def _validate_json_creds_to_string(raw: str) -> Optional[str]:
    d = _validate_json_creds_to_dict(raw)
    if not d:
        return None
    try:
        return json.dumps(d, separators=(",", ":"))
    except Exception:
        return None


# -----------------------------------------------------------------------------
# Load Canonical Settings (config.py) if present
# -----------------------------------------------------------------------------
_base_settings: Optional[object] = None


def _try_load_canonical_settings() -> Optional[object]:
    """
    Tries core.config then config (root).
    Must never raise.
    """
    global _base_settings
    if _base_settings is not None:
        return _base_settings

    for mod_path in ("core.config", "config"):
        try:
            mod = __import__(mod_path, fromlist=["get_settings"])
            if hasattr(mod, "get_settings"):
                _base_settings = mod.get_settings()
                return _base_settings
        except Exception:
            continue

    _base_settings = None
    return None


_s_obj = _try_load_canonical_settings()


def _get_setting(attr: str, env_keys: List[str], default: Any) -> Any:
    """
    Priority:
    1) OS Env
    2) canonical config.get_settings() object attribute
    3) default
    """
    v = _get_first_env(*env_keys)
    if v is not None:
        return v
    if _s_obj is not None:
        try:
            x = getattr(_s_obj, attr, None)
            if x is not None:
                return x
        except Exception:
            pass
    return default


# -----------------------------------------------------------------------------
# Static exported environment variables (module-level constants)
# -----------------------------------------------------------------------------
# App identity
APP_NAME = _get_setting("service_name", ["APP_NAME", "SERVICE_NAME", "APP_TITLE"], "Tadawul Fast Bridge")
APP_VERSION = _get_setting("app_version", ["APP_VERSION", "SERVICE_VERSION", "VERSION"], "dev")
APP_ENV = _strip(_get_setting("environment", ["APP_ENV", "ENVIRONMENT"], "production")).lower()

# Logging & localization
LOG_LEVEL = _strip(_get_first_env("LOG_LEVEL") or _get_setting("log_level", ["LOG_LEVEL"], "INFO")).upper()
LOG_JSON = _safe_bool(_get_first_env("LOG_JSON"), False)
LOG_FORMAT = _get_first_env("LOG_FORMAT") or ""  # optional
TIMEZONE_DEFAULT = _get_first_env("TIMEZONE_DEFAULT", "TZ") or _strip(
    _get_setting("timezone", ["APP_TIMEZONE", "TIMEZONE_DEFAULT", "TZ"], "Asia/Riyadh")
) or "Asia/Riyadh"

# Security / auth
AUTH_HEADER_NAME = _get_first_env("AUTH_HEADER_NAME", "TOKEN_HEADER_NAME") or _strip(
    _get_setting("auth_header_name", ["AUTH_HEADER_NAME"], "X-APP-TOKEN")
) or "X-APP-TOKEN"

APP_TOKEN = _get_first_env("APP_TOKEN", "TFB_APP_TOKEN")
BACKUP_APP_TOKEN = _get_first_env("BACKUP_APP_TOKEN")
REQUIRE_AUTH = _safe_bool(_get_first_env("REQUIRE_AUTH"), True)
ALLOW_QUERY_TOKEN = _safe_bool(_get_first_env("ALLOW_QUERY_TOKEN"), False)
OPEN_MODE = _safe_bool(_get_first_env("OPEN_MODE"), False)  # informational; canonical may override

# Feature flags
AI_ANALYSIS_ENABLED = _safe_bool(_get_first_env("AI_ANALYSIS_ENABLED"), True)
ADVANCED_ANALYSIS_ENABLED = _safe_bool(_get_first_env("ADVANCED_ANALYSIS_ENABLED", "ADVANCED_ENABLED"), True)
ADVISOR_ENABLED = _safe_bool(_get_first_env("ADVISOR_ENABLED"), True)
RATE_LIMIT_ENABLED = _safe_bool(_get_first_env("ENABLE_RATE_LIMITING"), True)

# Rate limits / tuning
MAX_REQUESTS_PER_MINUTE = _safe_int(_get_first_env("MAX_REQUESTS_PER_MINUTE"), 240, lo=30, hi=5000)
MAX_RETRIES = _safe_int(_get_first_env("MAX_RETRIES"), 2, lo=0, hi=10)
RETRY_DELAY = _safe_float(_get_first_env("RETRY_DELAY"), 0.6, lo=0.0, hi=30.0)

# Backend
BACKEND_BASE_URL = _strip(_get_first_env("BACKEND_BASE_URL", "TFB_BASE_URL") or _get_setting("backend_base_url", ["BACKEND_BASE_URL"], "http://127.0.0.1:8000")).rstrip("/")

# Cache knobs (for internal engines if referenced elsewhere)
ENGINE_CACHE_TTL_SEC = _safe_int(_get_first_env("ENGINE_CACHE_TTL_SEC", "CACHE_DEFAULT_TTL"), 20, lo=5, hi=3600)
CACHE_MAX_SIZE = _safe_int(_get_first_env("CACHE_MAX_SIZE"), 5000, lo=100, hi=500000)
CACHE_SAVE_INTERVAL = _safe_int(_get_first_env("CACHE_SAVE_INTERVAL"), 300, lo=30, hi=86400)
CACHE_BACKUP_ENABLED = _safe_bool(_get_first_env("CACHE_BACKUP_ENABLED"), True)

# HTTP timeouts
HTTP_TIMEOUT_SEC = _safe_float(_get_first_env("HTTP_TIMEOUT_SEC", "HTTP_TIMEOUT"), 45.0, lo=5.0, hi=180.0)

# Layout / Sheets row standard
TFB_SYMBOL_HEADER_ROW = _safe_int(_get_first_env("TFB_SYMBOL_HEADER_ROW"), 5, lo=1, hi=1000)
TFB_SYMBOL_START_ROW = _safe_int(_get_first_env("TFB_SYMBOL_START_ROW"), 6, lo=1, hi=1000)

# Google integration
DEFAULT_SPREADSHEET_ID = _get_first_env("DEFAULT_SPREADSHEET_ID", "SPREADSHEET_ID", "GOOGLE_SHEETS_ID") or _strip(
    _get_setting("spreadsheet_id", ["DEFAULT_SPREADSHEET_ID", "SPREADSHEET_ID"], "")
)

_GOOGLE_CREDS_RAW = os.getenv("GOOGLE_SHEETS_CREDENTIALS") or os.getenv("GOOGLE_CREDENTIALS") or ""
GOOGLE_SHEETS_CREDENTIALS = _validate_json_creds_to_string(_GOOGLE_CREDS_RAW)

GOOGLE_APPS_SCRIPT_URL = _get_first_env("GOOGLE_APPS_SCRIPT_URL")
GOOGLE_APPS_SCRIPT_BACKUP_URL = _get_first_env("GOOGLE_APPS_SCRIPT_BACKUP_URL")

# Providers - Keys / URLs
EODHD_API_KEY = _get_first_env("EODHD_API_KEY", "EODHD_API_TOKEN", "EODHD_TOKEN")
EODHD_BASE_URL = _get_first_env("EODHD_BASE_URL")

FINNHUB_API_KEY = _get_first_env("FINNHUB_API_KEY", "FINNHUB_TOKEN")
FMP_API_KEY = _get_first_env("FMP_API_KEY")
FMP_BASE_URL = _get_first_env("FMP_BASE_URL")

ALPHA_VANTAGE_API_KEY = _get_first_env("ALPHA_VANTAGE_API_KEY")
TWELVEDATA_API_KEY = _get_first_env("TWELVEDATA_API_KEY")
MARKETSTACK_API_KEY = _get_first_env("MARKETSTACK_API_KEY")

ARGAAM_QUOTE_URL = _get_first_env("ARGAAM_QUOTE_URL")
ARGAAM_API_KEY = _get_first_env("ARGAAM_API_KEY")

TADAWUL_QUOTE_URL = _get_first_env("TADAWUL_QUOTE_URL")
TADAWUL_MARKET_ENABLED = _safe_bool(_get_first_env("TADAWUL_MARKET_ENABLED"), True)
TADAWUL_MAX_SYMBOLS = _safe_int(_get_first_env("TADAWUL_MAX_SYMBOLS"), 1500, lo=10, hi=50000)
TADAWUL_REFRESH_INTERVAL = _safe_int(_get_first_env("TADAWUL_REFRESH_INTERVAL"), 60, lo=5, hi=3600)

KSA_DISALLOW_EODHD = _safe_bool(_get_first_env("KSA_DISALLOW_EODHD"), True)

# Providers lists (explicit or auto-detected)
_AUTO_GLOBAL: List[str] = []
if EODHD_API_KEY:
    _AUTO_GLOBAL.append("eodhd")
if FINNHUB_API_KEY:
    _AUTO_GLOBAL.append("finnhub")
if FMP_API_KEY:
    _AUTO_GLOBAL.append("fmp")
if ALPHA_VANTAGE_API_KEY:
    _AUTO_GLOBAL.append("alphavantage")
if TWELVEDATA_API_KEY:
    _AUTO_GLOBAL.append("twelvedata")
if MARKETSTACK_API_KEY:
    _AUTO_GLOBAL.append("marketstack")
if not _AUTO_GLOBAL:
    _AUTO_GLOBAL = ["eodhd", "finnhub"]

_AUTO_KSA: List[str] = []
if ARGAAM_QUOTE_URL:
    _AUTO_KSA.append("argaam")
if TADAWUL_QUOTE_URL:
    _AUTO_KSA.append("tadawul")
# Yahoo-chart is usually available even without an API key
if _safe_bool(_get_first_env("ENABLE_YAHOO_CHART_KSA", "ENABLE_YFINANCE_KSA"), True):
    _AUTO_KSA.insert(0, "yahoo_chart")
if not _AUTO_KSA:
    _AUTO_KSA = ["yahoo_chart", "argaam"]

ENABLED_PROVIDERS = _as_list_lower(_get_first_env("ENABLED_PROVIDERS", "PROVIDERS") or ",".join(_AUTO_GLOBAL))
KSA_PROVIDERS = _as_list_lower(_get_first_env("KSA_PROVIDERS") or ",".join(_AUTO_KSA))
PRIMARY_PROVIDER = _strip(_get_first_env("PRIMARY_PROVIDER") or (ENABLED_PROVIDERS[0] if ENABLED_PROVIDERS else "eodhd")).lower()

# Render (optional)
RENDER_API_KEY = _get_first_env("RENDER_API_KEY")
RENDER_SERVICE_ID = _get_first_env("RENDER_SERVICE_ID")
RENDER_SERVICE_NAME = _get_first_env("RENDER_SERVICE_NAME")

# CORS (optional)
ENABLE_CORS_ALL_ORIGINS = _safe_bool(_get_first_env("ENABLE_CORS_ALL_ORIGINS", "CORS_ALL_ORIGINS"), False)
CORS_ORIGINS = _get_first_env("CORS_ORIGINS") or ""


# -----------------------------------------------------------------------------
# Compatibility settings shim (what old code expects: env.settings)
# -----------------------------------------------------------------------------
class CompatibilitySettings:
    def __init__(self) -> None:
        # Keep legacy attribute names stable
        self.app_name = APP_NAME
        self.env = APP_ENV
        self.version = APP_VERSION
        self.log_level = LOG_LEVEL
        self.timezone = TIMEZONE_DEFAULT

        self.auth_header_name = AUTH_HEADER_NAME
        self.app_token = APP_TOKEN
        self.backup_app_token = BACKUP_APP_TOKEN
        self.require_auth = REQUIRE_AUTH
        self.allow_query_token = ALLOW_QUERY_TOKEN

        self.backend_base_url = BACKEND_BASE_URL

        self.tfb_layout = {"header_row": TFB_SYMBOL_HEADER_ROW, "start_row": TFB_SYMBOL_START_ROW}

        self.ai_analysis_enabled = AI_ANALYSIS_ENABLED
        self.advanced_analysis_enabled = ADVANCED_ANALYSIS_ENABLED
        self.advisor_enabled = ADVISOR_ENABLED
        self.rate_limit_enabled = RATE_LIMIT_ENABLED
        self.max_rpm = MAX_REQUESTS_PER_MINUTE

        self.ai_batch_size = _safe_int(_get_first_env("AI_BATCH_SIZE"), 20, lo=1, hi=200)
        self.ai_max_tickers = _safe_int(_get_first_env("AI_MAX_TICKERS"), 500, lo=1, hi=20000)
        self.adv_batch_size = _safe_int(_get_first_env("ADV_BATCH_SIZE"), 25, lo=1, hi=500)
        self.batch_concurrency = _safe_int(_get_first_env("BATCH_CONCURRENCY"), 5, lo=1, hi=50)

        self.http_timeout_sec = HTTP_TIMEOUT_SEC
        self.engine_cache_ttl_sec = ENGINE_CACHE_TTL_SEC

        self.default_spreadsheet_id = DEFAULT_SPREADSHEET_ID
        self.google_sheets_credentials = GOOGLE_SHEETS_CREDENTIALS

        self.enabled_providers = ENABLED_PROVIDERS
        self.ksa_providers = KSA_PROVIDERS
        self.primary_provider = PRIMARY_PROVIDER

        # Provider keys
        self.eodhd_api_key = EODHD_API_KEY
        self.finnhub_api_key = FINNHUB_API_KEY
        self.fmp_api_key = FMP_API_KEY
        self.alpha_vantage_api_key = ALPHA_VANTAGE_API_KEY
        self.twelvedata_api_key = TWELVEDATA_API_KEY
        self.marketstack_api_key = MARKETSTACK_API_KEY

        self.argaam_quote_url = ARGAAM_QUOTE_URL
        self.argaam_api_key = ARGAAM_API_KEY
        self.tadawul_quote_url = TADAWUL_QUOTE_URL

        self.ksa_disallow_eodhd = KSA_DISALLOW_EODHD


settings = CompatibilitySettings()


# -----------------------------------------------------------------------------
# Validation & Diagnostics
# -----------------------------------------------------------------------------
def validate_environment() -> Dict[str, List[str]]:
    """
    Performs logic checks on the loaded environment.
    Returns dict: {"errors": [...], "warnings": [...]}
    Non-fatal by design (PROD SAFE).
    """
    errors: List[str] = []
    warnings: List[str] = []

    # 1) Auth guard
    tokens_present = bool(APP_TOKEN or BACKUP_APP_TOKEN)
    if not tokens_present:
        warnings.append("Security: No APP_TOKEN configured. API may operate in OPEN MODE (depending on config.open_mode).")
    else:
        # If REQUIRE_AUTH is explicitly false, warn
        if not REQUIRE_AUTH:
            warnings.append("Security: REQUIRE_AUTH is false while tokens exist. Confirm this is intended.")

    # 2) Google Sheets readiness
    if ADVISOR_ENABLED or ADVANCED_ANALYSIS_ENABLED:
        if not GOOGLE_SHEETS_CREDENTIALS:
            warnings.append("Sheets: GOOGLE_SHEETS_CREDENTIALS missing or invalid (service account JSON not parsed).")
        if not DEFAULT_SPREADSHEET_ID:
            warnings.append("Sheets: DEFAULT_SPREADSHEET_ID missing. Sheet operations must provide explicit ID.")

    # 3) Provider sanity
    if "argaam" in KSA_PROVIDERS and not ARGAAM_QUOTE_URL:
        warnings.append("Provider: 'argaam' enabled but ARGAAM_QUOTE_URL is missing.")
    if "tadawul" in KSA_PROVIDERS and not TADAWUL_QUOTE_URL:
        warnings.append("Provider: 'tadawul' enabled but TADAWUL_QUOTE_URL is missing.")
    if "eodhd" in ENABLED_PROVIDERS and not EODHD_API_KEY:
        warnings.append("Provider: 'eodhd' enabled but EODHD_API_KEY missing.")
    if "finnhub" in ENABLED_PROVIDERS and not FINNHUB_API_KEY:
        warnings.append("Provider: 'finnhub' enabled but FINNHUB_API_KEY missing.")
    if PRIMARY_PROVIDER and ENABLED_PROVIDERS and PRIMARY_PROVIDER not in ENABLED_PROVIDERS:
        warnings.append("Provider: PRIMARY_PROVIDER not found in ENABLED_PROVIDERS. Routing may be inconsistent.")

    # 4) Backend URL sanity
    if not BACKEND_BASE_URL.lower().startswith(("http://", "https://")):
        errors.append("BACKEND_BASE_URL must start with http:// or https://")

    # 5) Rate limiting sanity
    if RATE_LIMIT_ENABLED and MAX_REQUESTS_PER_MINUTE < 30:
        warnings.append("RateLimit: MAX_REQUESTS_PER_MINUTE is very low (<30). You may throttle yourself too hard.")

    return {"errors": errors, "warnings": warnings}


def safe_env_summary() -> Dict[str, Any]:
    """
    Summary safe for logging/health endpoints.
    """
    diag = validate_environment()
    return {
        "env_version": ENV_VERSION,
        "app_identity": {
            "name": APP_NAME,
            "version": APP_VERSION,
            "env": APP_ENV,
            "python": sys.version.split(" ")[0],
        },
        "auth_config": {
            "header": AUTH_HEADER_NAME,
            "require_auth": REQUIRE_AUTH,
            "allow_query_token": ALLOW_QUERY_TOKEN,
            "tokens_present": bool(APP_TOKEN or BACKUP_APP_TOKEN),
            "app_token_masked": _mask_secret(APP_TOKEN),
            "backup_token_masked": _mask_secret(BACKUP_APP_TOKEN),
        },
        "dashboard_standard": {
            "header_row": TFB_SYMBOL_HEADER_ROW,
            "data_start_row": TFB_SYMBOL_START_ROW,
            "timezone_default": TIMEZONE_DEFAULT,
        },
        "backend": {
            "base_url": BACKEND_BASE_URL,
            "http_timeout_sec": HTTP_TIMEOUT_SEC,
            "max_retries": MAX_RETRIES,
            "retry_delay": RETRY_DELAY,
        },
        "providers": {
            "global": ENABLED_PROVIDERS,
            "ksa": KSA_PROVIDERS,
            "primary": PRIMARY_PROVIDER,
            "ksa_disallow_eodhd": KSA_DISALLOW_EODHD,
            "keys_detected": {
                "eodhd": bool(EODHD_API_KEY),
                "finnhub": bool(FINNHUB_API_KEY),
                "fmp": bool(FMP_API_KEY),
                "alphavantage": bool(ALPHA_VANTAGE_API_KEY),
                "twelvedata": bool(TWELVEDATA_API_KEY),
                "marketstack": bool(MARKETSTACK_API_KEY),
                "argaam_url": bool(ARGAAM_QUOTE_URL),
                "tadawul_url": bool(TADAWUL_QUOTE_URL),
            },
        },
        "integrations": {
            "google_sheets_ready": bool(GOOGLE_SHEETS_CREDENTIALS and DEFAULT_SPREADSHEET_ID),
            "default_spreadsheet_id_present": bool(DEFAULT_SPREADSHEET_ID),
            "advisor_enabled": ADVISOR_ENABLED,
            "ai_analysis_enabled": AI_ANALYSIS_ENABLED,
            "advanced_analysis_enabled": ADVANCED_ANALYSIS_ENABLED,
        },
        "cache": {
            "engine_cache_ttl_sec": ENGINE_CACHE_TTL_SEC,
            "cache_max_size": CACHE_MAX_SIZE,
            "cache_save_interval": CACHE_SAVE_INTERVAL,
            "cache_backup_enabled": CACHE_BACKUP_ENABLED,
        },
        "diagnostics": diag,
    }


__all__ = [
    "settings",
    "safe_env_summary",
    "validate_environment",
    "ENV_VERSION",
    # Common exports used across the repo
    "APP_NAME",
    "APP_VERSION",
    "APP_ENV",
    "LOG_LEVEL",
    "TIMEZONE_DEFAULT",
    "AUTH_HEADER_NAME",
    "TFB_SYMBOL_HEADER_ROW",
    "TFB_SYMBOL_START_ROW",
    "APP_TOKEN",
    "BACKUP_APP_TOKEN",
    "REQUIRE_AUTH",
    "ALLOW_QUERY_TOKEN",
    "BACKEND_BASE_URL",
    "HTTP_TIMEOUT_SEC",
    "ENGINE_CACHE_TTL_SEC",
    "AI_ANALYSIS_ENABLED",
    "ADVANCED_ANALYSIS_ENABLED",
    "ADVISOR_ENABLED",
    "RATE_LIMIT_ENABLED",
    "MAX_REQUESTS_PER_MINUTE",
    "DEFAULT_SPREADSHEET_ID",
    "GOOGLE_SHEETS_CREDENTIALS",
    "ENABLED_PROVIDERS",
    "KSA_PROVIDERS",
    "PRIMARY_PROVIDER",
]
