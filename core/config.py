"""
config.py
--------------------------------------------
Optional Pydantic Settings (v2) – SAFE PARSING (Render-friendly)

Goals:
- Never crash if env vars come as strings
- Robust bool/int/float parsing
- Robust list parsing for ENABLED_PROVIDERS
- Optional parsing for GOOGLE_SHEETS_CREDENTIALS (JSON string / escaped newlines / base64:)

Notes:
- This file is optional if you use env.py everywhere, but it is useful
  as the single typed source of truth across the project.
"""

from __future__ import annotations

import base64
import json
from functools import lru_cache
from typing import Any, Dict, List, Optional, Union

from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


def _parse_bool(v: Any, default: bool = False) -> bool:
    if isinstance(v, bool):
        return v
    if v is None:
        return default
    s = str(v).strip().lower()
    if s in ("1", "true", "yes", "y", "on"):
        return True
    if s in ("0", "false", "no", "n", "off"):
        return False
    return default


def _parse_int(v: Any, default: int) -> int:
    try:
        return int(float(str(v).strip()))
    except Exception:
        return default


def _parse_float(v: Any, default: float) -> float:
    try:
        return float(str(v).strip())
    except Exception:
        return default


class Settings(BaseSettings):
    # --- Application Info ---
    APP_NAME: str = "Tadawul Fast Bridge"
    APP_VERSION: str = "4.6.0"
    APP_ENV: str = "production"
    LOG_LEVEL: str = "INFO"
    PYTHON_VERSION: str = "3.11.9"

    # --- Connectivity / Auth ---
    BACKEND_BASE_URL: str = "https://tadawul-fast-bridge.onrender.com"
    APP_TOKEN: Optional[str] = None
    BACKUP_APP_TOKEN: Optional[str] = None
    ENABLE_CORS_ALL_ORIGINS: bool = True
    HTTP_TIMEOUT_SEC: float = 25.0

    # --- Providers ---
    # Keep as string or list in env; we normalize to list via validator
    ENABLED_PROVIDERS: Union[str, List[str]] = "fmp,yfinance"  # (KSA handled separately in engine routing)
    PRIMARY_PROVIDER: str = "fmp"
    EODHD_API_KEY: Optional[str] = None
    FMP_API_KEY: Optional[str] = None
    FINNHUB_API_KEY: Optional[str] = None
    ENABLE_YFINANCE: bool = True

    # KSA safety flags (engine should enforce)
    KSA_DISALLOW_EODHD: bool = True
    EODHD_FETCH_FUNDAMENTALS: bool = True

    # --- Google ---
    GOOGLE_SHEETS_CREDENTIALS: Optional[str] = None
    DEFAULT_SPREADSHEET_ID: Optional[str] = None

    # --- Batch / Performance ---
    ADV_BATCH_SIZE: int = 20
    ADV_BATCH_TIMEOUT_SEC: float = 45.0
    ADV_MAX_TICKERS: int = 500
    ADV_BATCH_CONCURRENCY: int = 5

    AI_BATCH_SIZE: int = 20
    AI_BATCH_TIMEOUT_SEC: float = 45.0
    AI_MAX_TICKERS: int = 500
    AI_BATCH_CONCURRENCY: int = 5

    ENRICHED_BATCH_SIZE: int = 40
    ENRICHED_BATCH_TIMEOUT_SEC: float = 45.0
    ENRICHED_MAX_TICKERS: int = 250
    ENRICHED_BATCH_CONCURRENCY: int = 5

    # -------------------------
    # Validators
    # -------------------------
    @field_validator("ENABLE_CORS_ALL_ORIGINS", "ENABLE_YFINANCE", "EODHD_FETCH_FUNDAMENTALS", "KSA_DISALLOW_EODHD", mode="before")
    @classmethod
    def _v_bool(cls, v: Any) -> bool:
        return _parse_bool(v, default=False)

    @field_validator("HTTP_TIMEOUT_SEC", "ADV_BATCH_TIMEOUT_SEC", "AI_BATCH_TIMEOUT_SEC", "ENRICHED_BATCH_TIMEOUT_SEC", mode="before")
    @classmethod
    def _v_float(cls, v: Any) -> float:
        return _parse_float(v, default=25.0)

    @field_validator(
        "ADV_BATCH_SIZE",
        "ADV_MAX_TICKERS",
        "ADV_BATCH_CONCURRENCY",
        "AI_BATCH_SIZE",
        "AI_MAX_TICKERS",
        "AI_BATCH_CONCURRENCY",
        "ENRICHED_BATCH_SIZE",
        "ENRICHED_MAX_TICKERS",
        "ENRICHED_BATCH_CONCURRENCY",
        mode="before",
    )
    @classmethod
    def _v_int(cls, v: Any) -> int:
        # keep defaults if parsing fails
        return _parse_int(v, default=0) or 0

    @field_validator("ENABLED_PROVIDERS", mode="before")
    @classmethod
    def parse_providers_list(cls, v: Union[str, List[str]]) -> List[str]:
        if isinstance(v, list):
            return [str(x).strip().lower() for x in v if str(x).strip()]

        if not isinstance(v, str):
            return []

        s = v.strip()
        if not s:
            return []

        # JSON array form
        if s.startswith("[") and s.endswith("]"):
            try:
                arr = json.loads(s)
                if isinstance(arr, list):
                    return [str(x).strip().lower() for x in arr if str(x).strip()]
            except Exception:
                pass

        # Comma-separated
        return [p.strip().lower() for p in s.split(",") if p.strip()]

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=True,
        extra="ignore",
    )

    # -------------------------
    # Helpers
    # -------------------------
    def google_credentials_info(self) -> Optional[Dict[str, Any]]:
        """
        Returns service account dict if GOOGLE_SHEETS_CREDENTIALS contains JSON.
        Supports:
        - Raw JSON string
        - JSON with escaped newlines (\\n)
        - base64:.... (optional)
        """
        raw = self.GOOGLE_SHEETS_CREDENTIALS
        if not raw:
            return None

        s = raw.strip()

        # Optional base64 prefix
        if s.lower().startswith("base64:"):
            try:
                b = base64.b64decode(s.split(":", 1)[1].strip())
                s = b.decode("utf-8", errors="replace").strip()
            except Exception:
                return None

        # Common Render/ENV escaping
        s = s.replace("\\n", "\n").strip()

        if s.startswith("{") and s.endswith("}"):
            try:
                obj = json.loads(s)
                return obj if isinstance(obj, dict) else None
            except Exception:
                return None

        return None


@lru_cache()
def get_settings() -> Settings:
    return Settings()
