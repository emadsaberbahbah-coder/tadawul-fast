# core/config.py  (FULL REPLACEMENT)
"""
TADAWUL FAST BRIDGE – CORE CONFIG (v2.7.0) – PROD SAFE / NO HEAVY IMPORTS

Goals
- Single source of truth for settings used across routes and engine.
- Works without pydantic-settings (plain dataclass + env parsing).
- Token mode:
    - If APP_TOKEN (or BACKUP_APP_TOKEN) is set -> token-protected endpoints
    - If none are set -> OPEN mode (no auth)
- Includes AI batch controls used by routes/ai_analysis.py and routes/advanced_analysis.py

This module is intentionally lightweight and safe to import at startup.
"""

from __future__ import annotations

import base64
import json
import os
from dataclasses import asdict, dataclass, field
from functools import lru_cache
from typing import Any, Dict, List, Optional


CONFIG_VERSION = "2.7.0"


def _strip(v: Any) -> str:
    try:
        return str(v).strip()
    except Exception:
        return ""


def _coerce_int(v: Any, default: int) -> int:
    try:
        return int(str(v).strip())
    except Exception:
        return default


def _coerce_float(v: Any, default: float) -> float:
    try:
        return float(str(v).strip())
    except Exception:
        return default


def _coerce_bool(v: Any, default: bool) -> bool:
    s = _strip(v).lower()
    if not s:
        return default
    if s in ("1", "true", "yes", "y", "on", "enable", "enabled"):
        return True
    if s in ("0", "false", "no", "n", "off", "disable", "disabled"):
        return False
    return default


def _split_csv(v: Any) -> List[str]:
    s = _strip(v)
    if not s:
        return []
    parts = [p.strip() for p in s.split(",")]
    return [p for p in parts if p]


def _mask_secret(s: Optional[str]) -> Optional[str]:
    if not s:
        return None
    t = s.strip()
    if len(t) <= 6:
        return "***"
    return t[:3] + "***" + t[-3:]


def _maybe_decode_b64_json(s: str) -> Optional[str]:
    """
    If the value looks like base64 JSON, decode it.
    If it is plain JSON already, return as-is.
    If it is not JSON, return original string (still useful for some setups).
    Never raises.
    """
    raw = (s or "").strip()
    if not raw:
        return None

    # Plain JSON?
    if raw.startswith("{") and raw.endswith("}"):
        try:
            json.loads(raw)
            return raw
        except Exception:
            return raw

    # Try base64 decode -> JSON
    try:
        decoded = base64.b64decode(raw.encode("utf-8"), validate=True).decode("utf-8", errors="replace").strip()
        if decoded.startswith("{") and decoded.endswith("}"):
            json.loads(decoded)
            return decoded
    except Exception:
        pass

    return raw


@dataclass(frozen=True)
class Settings:
    # identity
    service_name: str = "tadawul-fast-bridge"
    environment: str = "prod"
    log_level: str = "INFO"

    # auth
    app_token: Optional[str] = None
    backup_app_token: Optional[str] = None

    # AI batching (used by /v1/analysis + /v1/advanced)
    ai_batch_size: int = 25
    ai_batch_timeout_sec: float = 45.0
    ai_batch_concurrency: int = 6
    ai_max_tickers: int = 800

    # CORS
    cors_origins: List[str] = field(default_factory=lambda: ["*"])

    # Google Sheets (optional)
    default_spreadsheet_id: Optional[str] = None
    tfb_spreadsheet_id: Optional[str] = None
    google_sheets_credentials: Optional[str] = None  # JSON or base64 JSON (decoded)
    google_application_credentials: Optional[str] = None  # file path (if used)

    # Providers (optional)
    tadawul_quote_url: Optional[str] = None
    tadawul_fundamentals_url: Optional[str] = None
    argaam_base_url: Optional[str] = None
    enable_yahoo: bool = True

    # misc
    timezone_default: str = "Asia/Riyadh"

    @staticmethod
    def from_env() -> "Settings":
        service_name = _strip(os.getenv("SERVICE_NAME") or os.getenv("APP_NAME") or "tadawul-fast-bridge")
        environment = _strip(os.getenv("ENVIRONMENT") or os.getenv("ENV") or "prod").lower()
        log_level = _strip(os.getenv("LOG_LEVEL") or "INFO").upper()

        app_token = _strip(os.getenv("APP_TOKEN")) or None
        backup_app_token = _strip(os.getenv("BACKUP_APP_TOKEN")) or None

        ai_batch_size = _coerce_int(os.getenv("AI_BATCH_SIZE"), 25)
        ai_batch_timeout_sec = _coerce_float(os.getenv("AI_BATCH_TIMEOUT_SEC"), 45.0)
        ai_batch_concurrency = _coerce_int(os.getenv("AI_BATCH_CONCURRENCY"), 6)
        ai_max_tickers = _coerce_int(os.getenv("AI_MAX_TICKERS"), 800)

        # guardrails
        ai_batch_size = max(5, min(250, ai_batch_size))
        ai_batch_timeout_sec = max(5.0, min(180.0, ai_batch_timeout_sec))
        ai_batch_concurrency = max(1, min(30, ai_batch_concurrency))
        ai_max_tickers = max(10, min(3000, ai_max_tickers))

        cors_origins = _split_csv(os.getenv("CORS_ORIGINS"))
        if not cors_origins:
            cors_origins = ["*"]

        default_spreadsheet_id = _strip(os.getenv("DEFAULT_SPREADSHEET_ID")) or None
        tfb_spreadsheet_id = _strip(os.getenv("TFB_SPREADSHEET_ID")) or None

        gsc_raw = _strip(os.getenv("GOOGLE_SHEETS_CREDENTIALS"))
        gsc = _maybe_decode_b64_json(gsc_raw) if gsc_raw else None
        gac = _strip(os.getenv("GOOGLE_APPLICATION_CREDENTIALS")) or None

        tadawul_quote_url = _strip(os.getenv("TADAWUL_QUOTE_URL")) or None
        tadawul_fundamentals_url = _strip(os.getenv("TADAWUL_FUNDAMENTALS_URL")) or None
        argaam_base_url = _strip(os.getenv("ARGAAM_BASE_URL")) or None

        enable_yahoo = _coerce_bool(os.getenv("ENABLE_YAHOO"), True)

        tz_default = _strip(os.getenv("TIMEZONE_DEFAULT") or "Asia/Riyadh")

        return Settings(
            service_name=service_name,
            environment=environment,
            log_level=log_level,
            app_token=app_token,
            backup_app_token=backup_app_token,
            ai_batch_size=ai_batch_size,
            ai_batch_timeout_sec=ai_batch_timeout_sec,
            ai_batch_concurrency=ai_batch_concurrency,
            ai_max_tickers=ai_max_tickers,
            cors_origins=cors_origins,
            default_spreadsheet_id=default_spreadsheet_id,
            tfb_spreadsheet_id=tfb_spreadsheet_id,
            google_sheets_credentials=gsc,
            google_application_credentials=gac,
            tadawul_quote_url=tadawul_quote_url,
            tadawul_fundamentals_url=tadawul_fundamentals_url,
            argaam_base_url=argaam_base_url,
            enable_yahoo=enable_yahoo,
            timezone_default=tz_default,
        )


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    return Settings.from_env()


def allowed_tokens() -> List[str]:
    s = get_settings()
    tokens: List[str] = []
    if s.app_token:
        tokens.append(s.app_token.strip())
    if s.backup_app_token:
        tokens.append(s.backup_app_token.strip())

    out: List[str] = []
    seen = set()
    for t in tokens:
        if t and t not in seen:
            seen.add(t)
            out.append(t)
    return out


def is_open_mode() -> bool:
    return len(allowed_tokens()) == 0


def auth_ok(x_app_token: Optional[str]) -> bool:
    toks = allowed_tokens()
    if not toks:
        return True
    return bool(x_app_token and x_app_token.strip() in toks)


def mask_settings_dict() -> Dict[str, Any]:
    """
    Safe dictionary for returning via API.
    Secrets are masked and heavy JSON blobs are not returned.
    """
    s = get_settings()
    d = asdict(s)

    d["app_token"] = _mask_secret(s.app_token)
    d["backup_app_token"] = _mask_secret(s.backup_app_token)

    # never return service-account JSON
    d["google_sheets_credentials"] = "***present***" if s.google_sheets_credentials else None
    d["google_application_credentials"] = "***present***" if s.google_application_credentials else None

    return d


__all__ = [
    "CONFIG_VERSION",
    "Settings",
    "get_settings",
    "allowed_tokens",
    "is_open_mode",
    "auth_ok",
    "mask_settings_dict",
]
