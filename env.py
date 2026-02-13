#!/usr/bin/env python3
# env.py
"""
env.py
------------------------------------------------------------
Backward-compatible environment exports for Tadawul Fast Bridge (v5.2.2)

Key goals
- ✅ Row 5 Standard: Exports TFB_SYMBOL_HEADER_ROW/START_ROW for dashboard alignment.
- ✅ Advisor Enabled: Syncs with main.py v5.4.4 feature flags.
- ✅ Version resolves from ENV FIRST: APP_VERSION / SERVICE_VERSION / VERSION / RELEASE
- ✅ Prefers core.config.get_settings() (canonical), then repo-root config.get_settings()
- ✅ Robust GOOGLE_SHEETS_CREDENTIALS parsing (JSON / base64 / quoted).

v5.2.2 changes
- ✅ Adds TFB_SYMBOL_HEADER_ROW=5 and TFB_SYMBOL_START_ROW=6 exports.
- ✅ Includes ADVISOR_ENABLED in compatibility settings.
- ✅ Adds ADV and ENRICHED batch constants for complete config mirroring.
"""

from __future__ import annotations

import base64
import json
import logging
import os
from typing import Any, Dict, List, Optional


ENV_VERSION = "5.2.2"
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
    allowed = set("ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/=_-\n\r")
    return all((c in allowed) for c in raw)


def _maybe_b64_decode_json(s: str) -> str:
    raw = (s or "").strip()
    if not raw:
        return raw
    if _looks_like_json_object(raw):
        return raw
    if not _looks_like_b64(raw):
        return raw

    try:
        dec = base64.b64decode(raw.encode("utf-8"), validate=False).decode("utf-8", errors="replace").strip()
        if _looks_like_json_object(dec):
            obj = json.loads(dec)
            if isinstance(obj, dict):
                return dec
    except Exception:
        return raw

    return raw


def _try_parse_json_dict(raw: Any) -> Optional[Dict[str, Any]]:
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
                    ss = s.strip().replace("'", '"')
                    arr = json.loads(ss)
                    if isinstance(arr, list):
                        raw = [str(x).strip().lower() for x in arr if str(x).strip()]
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
# Load Settings Object
# ---------------------------------------------------------------------
_SETTINGS_OBJ: Optional[object] = None

def _try_load_settings() -> Optional[object]:
    global _SETTINGS_OBJ
    if _SETTINGS_OBJ: return _SETTINGS_OBJ
    try:
        from core.config import get_settings
        _SETTINGS_OBJ = get_settings()
        return _SETTINGS_OBJ
    except Exception:
        try:
            from config import get_settings
            _SETTINGS_OBJ = get_settings()
            return _SETTINGS_OBJ
        except Exception:
            return None

def _get_attr(obj: Optional[object], name: str, default: Any = None) -> Any:
    if obj is None: return default
    return getattr(obj, name, default)

_base_settings = _try_load_settings()

# ---------------------------------------------------------------------
# Exports (v12.2 Plan Aligned)
# ---------------------------------------------------------------------
APP_NAME = _get_first_env("APP_NAME", "SERVICE_NAME") or _get_attr(_base_settings, "service_name", "Tadawul Fast Bridge")
APP_VERSION = _normalize_version(_get_first_env("APP_VERSION", "VERSION")) or _get_attr(_base_settings, "app_version", "dev")
APP_ENV = str(_get_first_env("APP_ENV", "ENV", "ENVIRONMENT") or "production").lower()

LOG_LEVEL = str(_get_first_env("LOG_LEVEL") or "INFO").upper()
TIMEZONE_DEFAULT = _get_first_env("TIMEZONE_DEFAULT", "TZ") or "Asia/Riyadh"
AUTH_HEADER_NAME = _get_first_env("AUTH_HEADER_NAME", "TOKEN_HEADER_NAME") or "X-APP-TOKEN"

# Row 5 Dashboard Standard
TFB_SYMBOL_HEADER_ROW = _safe_int(_get_first_env("TFB_SYMBOL_HEADER_ROW"), 5)
TFB_SYMBOL_START_ROW = _safe_int(_get_first_env("TFB_SYMBOL_START_ROW"), 6)

_export_env_if_missing("TFB_SYMBOL_HEADER_ROW", str(TFB_SYMBOL_HEADER_ROW))
_export_env_if_missing("TFB_SYMBOL_START_ROW", str(TFB_SYMBOL_START_ROW))

# Auth
APP_TOKEN = _get_first_env("APP_TOKEN", "TFB_APP_TOKEN") or _get_attr(_base_settings, "app_token")
BACKUP_APP_TOKEN = _get_first_env("BACKUP_APP_TOKEN") or _get_attr(_base_settings, "backup_app_token")

# Features
ADVISOR_ENABLED = _safe_bool(_get_first_env("ADVISOR_ENABLED"), True)

# Batch Limits (Mirroring config.py)
AI_BATCH_SIZE = _safe_int(_get_first_env("AI_BATCH_SIZE"), 20)
AI_MAX_TICKERS = _safe_int(_get_first_env("AI_MAX_TICKERS"), 500)
ADV_BATCH_SIZE = _safe_int(_get_first_env("ADV_BATCH_SIZE"), 25)
ENRICHED_BATCH_SIZE = _safe_int(_get_first_env("ENRICHED_BATCH_SIZE"), 40)

# Google Sheets
_creds_raw = _get_first_env("GOOGLE_SHEETS_CREDENTIALS", "GOOGLE_CREDENTIALS") or ""
GOOGLE_SHEETS_CREDENTIALS = _try_parse_json_dict(_creds_raw)
DEFAULT_SPREADSHEET_ID = _get_first_env("DEFAULT_SPREADSHEET_ID", "SPREADSHEET_ID") or ""

# ---------------------------------------------------------------------
# Compatibility Shim
# ---------------------------------------------------------------------
class _Settings:
    app_name = APP_NAME
    env = APP_ENV
    version = APP_VERSION
    log_level = LOG_LEVEL
    auth_header_name = AUTH_HEADER_NAME
    app_token = (APP_TOKEN or "")
    advisor_enabled = ADVISOR_ENABLED
    ai_batch_size = AI_BATCH_SIZE
    ai_max_tickers = AI_MAX_TICKERS
    adv_batch_size = ADV_BATCH_SIZE
    enriched_batch_size = ENRICHED_BATCH_SIZE
    google_sheets_credentials = GOOGLE_SHEETS_CREDENTIALS
    default_spreadsheet_id = DEFAULT_SPREADSHEET_ID

settings: object = _Settings()

__all__ = [
    "settings",
    "ENV_VERSION",
    "APP_NAME",
    "APP_VERSION",
    "AUTH_HEADER_NAME",
    "TFB_SYMBOL_HEADER_ROW",
    "TFB_SYMBOL_START_ROW",
    "APP_TOKEN",
    "ADVISOR_ENABLED",
    "GOOGLE_SHEETS_CREDENTIALS",
    "DEFAULT_SPREADSHEET_ID"
]
