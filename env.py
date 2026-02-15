#!/usr/bin/env python3
# env.py
"""
env.py
------------------------------------------------------------
Backward-compatible environment exports for Tadawul Fast Bridge (v5.5.0)
Advanced Production Edition

Key goals:
- ✅ **Central Truth**: Reads from OS Env -> config.py -> Defaults.
- ✅ **Deep Validation**: Checks logical consistency of settings at boot.
- ✅ **Auto-Discovery**: Detects active providers based on API keys.
- ✅ **Secret Hygiene**: Auto-repairs malformed JSON credentials.
- ✅ **Riyadh Time**: Enforces UTC+3 defaults.

v5.5.0 changes:
- Added `validate_environment()` for startup health checks.
- Expanded provider detection (Tadawul, Argaam, EODHD, Finnhub).
- Robust multiline string parsing for list variables.
"""

from __future__ import annotations

import base64
import json
import logging
import os
import re
import sys
from typing import Any, Dict, List, Optional

ENV_VERSION = "5.5.0"
logger = logging.getLogger("env")

_TRUTHY = {"1", "true", "yes", "y", "on", "t", "enable", "enabled", "ok"}
_FALSY = {"0", "false", "no", "n", "off", "f", "disable", "disabled"}


# ---------------------------------------------------------------------
# Internal Helpers
# ---------------------------------------------------------------------
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
        x = int(float(str(v).strip()))
        return x if x >= 0 else default
    except Exception:
        return default


def _safe_float(v: Any, default: float) -> float:
    try:
        x = float(str(v).strip())
        return x if x >= 0 else default
    except Exception:
        return default


def _mask_secret(s: Optional[str], keep: int = 3) -> str:
    x = (s or "").strip()
    if not x: return "MISSING"
    if len(x) < 8: return "***"
    return f"{x[:keep]}...{x[-keep:]}"


def _strip_wrapping_quotes(s: str) -> str:
    t = (s or "").strip()
    if len(t) >= 2 and ((t[0] == t[-1] == '"') or (t[0] == t[-1] == "'")):
        return t[1:-1].strip()
    return t


def _validate_json_creds(raw: str) -> Optional[Dict[str, Any]]:
    """
    Advanced validator/repair tool for Service Account JSON.
    Handles Base64, raw JSON, and double-escaped private keys.
    """
    t = _strip_wrapping_quotes(raw or "").strip()
    if not t:
        return None
    
    # Base64 Decode Attempt
    if not t.startswith("{") and len(t) > 50:
        try:
            decoded = base64.b64decode(t, validate=False).decode("utf-8", errors="replace").strip()
            if decoded.startswith("{"):
                t = decoded
        except Exception:
            pass

    try:
        obj = json.loads(t)
        if isinstance(obj, dict) and "private_key" in obj:
            pk = obj["private_key"]
            # Fix Render/Shell literal newline escaping
            if isinstance(pk, str) and "\\n" in pk:
                obj["private_key"] = pk.replace("\\n", "\n")
            return obj
    except Exception:
        pass
    return None


def _get_first_env(*keys: str) -> Optional[str]:
    for k in keys:
        v = os.getenv(k)
        if v is not None and str(v).strip():
            return str(v).strip()
    return None


def _as_list_lower(v: Any) -> List[str]:
    """Robust parser for CSV, JSON arrays, newlines, and Python-style lists."""
    if v is None:
        return []

    s = str(v).strip()
    if not s:
        return []
        
    # Handle JSON/Python list syntax
    if s.startswith(("[", "(")):
        try:
            s_clean = s.replace("'", '"')
            arr = json.loads(s_clean)
            if isinstance(arr, list):
                return [str(x).strip().lower() for x in arr if str(x).strip()]
        except Exception:
            pass
    
    # Handle separators (comma, semicolon, newline, pipe)
    parts = re.split(r"[\s,;|]+", s)
    
    out: List[str] = []
    seen = set()
    for x in parts:
        clean = x.strip().lower()
        if clean and clean not in seen:
            seen.add(clean)
            out.append(clean)
    return out


# ---------------------------------------------------------------------
# Load Main Settings Object (Singleton Accessor)
# ---------------------------------------------------------------------
_base_settings: Optional[object] = None

def _try_load_canonical_settings() -> Optional[object]:
    """Tries core.config (newest) then config (legacy root)."""
    global _base_settings
    if _base_settings: return _base_settings
    for path in ("core.config", "config"):
        try:
            mod = __import__(path, fromlist=["get_settings"])
            if hasattr(mod, "get_settings"):
                _base_settings = mod.get_settings()
                return _base_settings
        except Exception:
            continue
    return None

_s_obj = _try_load_canonical_settings()

def _get_setting(name: str, env_keys: List[str], default: Any) -> Any:
    # 1. Direct Env (Highest Priority)
    val = _get_first_env(*env_keys)
    if val is not None: return val
    
    # 2. Canonical Settings Object
    if _s_obj:
        try: 
            val = getattr(_s_obj, name, None)
            if val is not None: return val
        except: pass
        
    return default

# ---------------------------------------------------------------------
# Static Environment Variables
# ---------------------------------------------------------------------
# App Identity
APP_NAME = _get_setting("service_name", ["APP_NAME", "SERVICE_NAME"], "Tadawul Fast Bridge")
APP_VERSION = _get_setting("app_version", ["APP_VERSION", "VERSION"], "dev")
APP_ENV = str(_get_setting("environment", ["APP_ENV", "ENVIRONMENT"], "production")).lower()

# Logging & Localization
LOG_LEVEL = str(_get_first_env("LOG_LEVEL") or "INFO").upper()
TIMEZONE_DEFAULT = _get_first_env("TIMEZONE_DEFAULT", "TZ") or "Asia/Riyadh"
AUTH_HEADER_NAME = _get_first_env("AUTH_HEADER_NAME", "TOKEN_HEADER_NAME") or "X-APP-TOKEN"

# Dashboard Constants (Row 5 Standard)
TFB_SYMBOL_HEADER_ROW = _safe_int(_get_first_env("TFB_SYMBOL_HEADER_ROW"), 5)
TFB_SYMBOL_START_ROW = _safe_int(_get_first_env("TFB_SYMBOL_START_ROW"), 6)

# Auth Secrets
APP_TOKEN = _get_first_env("APP_TOKEN", "TFB_APP_TOKEN")
BACKUP_APP_TOKEN = _get_first_env("BACKUP_APP_TOKEN")

# Providers - Credentials
ARGAAM_QUOTE_URL = _get_first_env("ARGAAM_QUOTE_URL")
ARGAAM_API_KEY = _get_first_env("ARGAAM_API_KEY")
EODHD_API_KEY = _get_first_env("EODHD_API_KEY", "EODHD_API_TOKEN", "EODHD_TOKEN")
FINNHUB_API_KEY = _get_first_env("FINNHUB_API_KEY", "FINNHUB_TOKEN")
TADAWUL_QUOTE_URL = _get_first_env("TADAWUL_QUOTE_URL")

# Feature Flags
ADVISOR_ENABLED = _safe_bool(_get_first_env("ADVISOR_ENABLED"), True)
AI_ANALYSIS_ENABLED = _safe_bool(_get_first_env("AI_ANALYSIS_ENABLED"), True)

# Batch Tuning
AI_BATCH_SIZE = _safe_int(_get_first_env("AI_BATCH_SIZE"), 20)
AI_MAX_TICKERS = _safe_int(_get_first_env("AI_MAX_TICKERS"), 500)
ADV_BATCH_SIZE = _safe_int(_get_first_env("ADV_BATCH_SIZE"), 25)

# Google Integration
DEFAULT_SPREADSHEET_ID = _get_first_env("DEFAULT_SPREADSHEET_ID", "SPREADSHEET_ID") or ""
# Convert the validated/dict credentials back to JSON string for clients expecting string
_creds_dict = _validate_json_creds(os.getenv("GOOGLE_SHEETS_CREDENTIALS") or os.getenv("GOOGLE_CREDENTIALS") or "")
GOOGLE_SHEETS_CREDENTIALS = json.dumps(_creds_dict) if _creds_dict else None

# Provider Auto-Detection (Smart Logic)
# If keys exist, add them to enabled lists automatically
_AUTO_GLOBAL = []
if EODHD_API_KEY: _AUTO_GLOBAL.append("eodhd")
if FINNHUB_API_KEY: _AUTO_GLOBAL.append("finnhub")
if not _AUTO_GLOBAL: _AUTO_GLOBAL = ["eodhd", "finnhub"] # defaults

_AUTO_KSA = []
if ARGAAM_QUOTE_URL: _AUTO_KSA.append("argaam")
if TADAWUL_QUOTE_URL: _AUTO_KSA.append("tadawul")
if not _AUTO_KSA: _AUTO_KSA = ["yahoo_chart", "argaam"] # defaults (yahoo usually free)

ENABLED_PROVIDERS = _as_list_lower(_get_first_env("ENABLED_PROVIDERS", "PROVIDERS") or ",".join(_AUTO_GLOBAL))
KSA_PROVIDERS = _as_list_lower(_get_first_env("KSA_PROVIDERS") or ",".join(_AUTO_KSA))

# ---------------------------------------------------------------------
# Compatibility Settings Shim
# ---------------------------------------------------------------------
class CompatibilitySettings:
    def __init__(self):
        self.app_name = APP_NAME
        self.env = APP_ENV
        self.version = APP_VERSION
        self.log_level = LOG_LEVEL
        self.auth_header_name = AUTH_HEADER_NAME
        self.app_token = APP_TOKEN
        self.backup_app_token = BACKUP_APP_TOKEN
        self.tfb_layout = {"header_row": TFB_SYMBOL_HEADER_ROW, "start_row": TFB_SYMBOL_START_ROW}
        self.advisor_enabled = ADVISOR_ENABLED
        self.ai_batch_size = AI_BATCH_SIZE
        self.ai_max_tickers = AI_MAX_TICKERS
        self.adv_batch_size = ADV_BATCH_SIZE
        self.google_sheets_credentials = GOOGLE_SHEETS_CREDENTIALS
        self.default_spreadsheet_id = DEFAULT_SPREADSHEET_ID
        self.enabled_providers = ENABLED_PROVIDERS
        self.ksa_providers = KSA_PROVIDERS

settings = CompatibilitySettings()

# ---------------------------------------------------------------------
# Validation & Diagnostics
# ---------------------------------------------------------------------
def validate_environment() -> List[str]:
    """
    Performs logic checks on the loaded environment.
    Returns a list of warning strings (empty if healthy).
    """
    warnings = []
    
    # 1. Auth Guard
    if not APP_TOKEN and not BACKUP_APP_TOKEN:
        warnings.append("Security Alert: No APP_TOKEN configured. API is in OPEN MODE.")
        
    # 2. Google Sheets
    if ADVISOR_ENABLED:
        if not GOOGLE_SHEETS_CREDENTIALS:
            warnings.append("Feature Conflict: ADVISOR_ENABLED=True but GOOGLE_SHEETS_CREDENTIALS missing.")
        if not DEFAULT_SPREADSHEET_ID:
            warnings.append("Configuration: DEFAULT_SPREADSHEET_ID is missing. Sheet operations require explicit ID.")

    # 3. Provider Health
    if "argaam" in KSA_PROVIDERS and not ARGAAM_QUOTE_URL:
        warnings.append("Provider: 'argaam' is enabled but ARGAAM_QUOTE_URL is missing.")
    if "eodhd" in ENABLED_PROVIDERS and not EODHD_API_KEY:
        warnings.append("Provider: 'eodhd' is enabled but EODHD_API_KEY is missing.")
    
    return warnings

def safe_env_summary() -> Dict[str, Any]:
    """Generates a summary of the environment state safe for logging."""
    return {
        "app_identity": {
            "name": APP_NAME,
            "version": APP_VERSION,
            "env": APP_ENV,
            "python": sys.version.split(" ")[0]
        },
        "auth_config": {
            "header": AUTH_HEADER_NAME,
            "tokens_present": bool(APP_TOKEN or BACKUP_APP_TOKEN),
            "app_token_masked": _mask_secret(APP_TOKEN)
        },
        "dashboard_standard": {
            "header_row": TFB_SYMBOL_HEADER_ROW,
            "data_start_row": TFB_SYMBOL_START_ROW
        },
        "providers": {
            "global": ENABLED_PROVIDERS,
            "ksa": KSA_PROVIDERS,
            "keys_detected": {
                "eodhd": bool(EODHD_API_KEY),
                "finnhub": bool(FINNHUB_API_KEY),
                "argaam": bool(ARGAAM_QUOTE_URL)
            }
        },
        "integrations": {
            "google_sheets_ready": bool(GOOGLE_SHEETS_CREDENTIALS and DEFAULT_SPREADSHEET_ID),
            "advisor_active": ADVISOR_ENABLED
        },
        "health_check": validate_environment()
    }

__all__ = [
    "settings",
    "safe_env_summary",
    "validate_environment",
    "ENV_VERSION",
    "APP_NAME",
    "APP_VERSION",
    "AUTH_HEADER_NAME",
    "TFB_SYMBOL_HEADER_ROW",
    "TFB_SYMBOL_START_ROW",
    "APP_TOKEN",
    "ADVISOR_ENABLED",
    "GOOGLE_SHEETS_CREDENTIALS",
    "DEFAULT_SPREADSHEET_ID",
    "ENABLED_PROVIDERS",
    "KSA_PROVIDERS"
]
