# env.py — SAFE + QUIET (v5.0.5)
"""
env.py
------------------------------------------------------------
Legacy compatibility module (optional).

Goals:
- Never performs network calls at import-time
- Avoids noisy logs by default
- Provides legacy constants (if older modules import env.*)
- Keeps versions aligned to APP_VERSION/SERVICE_VERSION/VERSION
"""

from __future__ import annotations

import os
import logging
from typing import Any, Dict, Optional, List

logger = logging.getLogger("env")

_TRUTHY = {"1", "true", "yes", "y", "on", "t"}

def _truthy(v: Any) -> bool:
    return str(v or "").strip().lower() in _TRUTHY

def _get_first(*keys: str, default: str = "") -> str:
    for k in keys:
        v = os.getenv(k)
        if v is not None and str(v).strip():
            return str(v).strip()
    return default

def _mask_tail(s: Optional[str], keep: int = 4) -> str:
    x = (s or "").strip()
    if not x:
        return ""
    if len(x) <= keep:
        return "•" * len(x)
    return ("•" * (len(x) - keep)) + x[-keep:]

# ---------------------------------------------------------------------
# Canonical values (legacy exports)
# ---------------------------------------------------------------------
APP_NAME = _get_first("APP_NAME", "SERVICE_NAME", "APP_TITLE", default="Tadawul Fast Bridge")
APP_ENV = _get_first("APP_ENV", "ENVIRONMENT", "APP_ENVIRONMENT", default="production")

# Prefer env version keys first
APP_VERSION = _get_first("APP_VERSION", "SERVICE_VERSION", "VERSION", "RELEASE", default="dev")
ENV_PY_VERSION = APP_VERSION  # keep aligned (no separate “env.py=4.9.2”)

ENABLED_PROVIDERS = _get_first("ENABLED_PROVIDERS", "PROVIDERS", default="eodhd,finnhub")
KSA_PROVIDERS = _get_first("KSA_PROVIDERS", default="yahoo_chart,tadawul,argaam")

GOOGLE_SHEETS_CREDENTIALS = os.getenv("GOOGLE_SHEETS_CREDENTIALS") or os.getenv("GOOGLE_CREDENTIALS") or ""
DEFAULT_SPREADSHEET_ID = os.getenv("DEFAULT_SPREADSHEET_ID") or ""

# ---------------------------------------------------------------------
# Optional startup log (OFF by default)
# Turn on by setting ENV_LOG_ON_BOOT=true
# ---------------------------------------------------------------------
if _truthy(os.getenv("ENV_LOG_ON_BOOT", "false")):
    logger.info(
        "[env] App=%s | Env=%s | Version=%s | env.py=%s",
        APP_NAME, APP_ENV, APP_VERSION, ENV_PY_VERSION
    )
    logger.info("[env] Providers=%s", ENABLED_PROVIDERS)
    logger.info("[env] KSA Providers=%s", KSA_PROVIDERS)
    logger.info(
        "[env] Sheets creds=%s | SpreadsheetId=%s",
        "SET" if GOOGLE_SHEETS_CREDENTIALS.strip() else "MISSING",
        "SET" if DEFAULT_SPREADSHEET_ID.strip() else "MISSING",
    )

def get_env_snapshot() -> Dict[str, Any]:
    """Safe diagnostics (no secrets)."""
    return {
        "app_name": APP_NAME,
        "app_env": APP_ENV,
        "app_version": APP_VERSION,
        "providers": [p.strip() for p in ENABLED_PROVIDERS.split(",") if p.strip()],
        "ksa_providers": [p.strip() for p in KSA_PROVIDERS.split(",") if p.strip()],
        "sheets_creds_set": bool(GOOGLE_SHEETS_CREDENTIALS.strip()),
        "default_spreadsheet_id_set": bool(DEFAULT_SPREADSHEET_ID.strip()),
        "app_token_mask": _mask_tail(os.getenv("APP_TOKEN") or "", keep=4),
    }
