"""
core/config.py
===========================================================
Central configuration for Tadawul Fast Bridge (Hardened)

Goals
- Zero-crash settings loading on Render / local .env
- Robust parsing for list-like env vars (JSON / comma-separated)
- Safe defaults + validation (timeouts, batch sizes, provider selection)
- Convenience helpers for Google credentials parsing

Pydantic: v2 + pydantic-settings
"""

from __future__ import annotations

import json
from functools import lru_cache
from typing import Any, Dict, List, Optional, Sequence, Union

from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


def _as_clean_str(v: Any) -> Optional[str]:
    if v is None:
        return None
    s = str(v).strip()
    return s if s else None


def _dedupe_preserve(items: Sequence[str]) -> List[str]:
    seen = set()
    out: List[str] = []
    for x in items:
        x = (x or "").strip()
        if not x:
            continue
        k = x.lower()
        if k in seen:
            continue
        seen.add(k)
        out.append(k)
    return out


def _parse_str_list(v: Union[str, List[str], None]) -> List[str]:
    """
    Accepts:
      - None
      - ["a","b"]
      - '["a","b"]'
      - "a,b,c"
      - "a b c"  (space separated fallback)
    Returns: lowercased, deduped list
    """
    if v is None:
        return []

    if isinstance(v, list):
        return _dedupe_preserve([str(x) for x in v])

    s = str(v).strip()
    if not s:
        return []

    # JSON list
    if s.startswith("[") and s.endswith("]"):
        try:
            raw = json.loads(s)
            if isinstance(raw, list):
                return _dedupe_preserve([str(x) for x in raw])
        except Exception:
            # fall through
            pass

    # Comma-separated
    if "," in s:
        return _dedupe_preserve([p.strip() for p in s.split(",")])

    # Space-separated fallback
    parts = [p.strip() for p in s.split()]
    return _dedupe_preserve(parts)


class Settings(BaseSettings):
    """
    Loads from env first, then .env (local).
    """

    # -------------------------
    # App identity
    # -------------------------
    APP_NAME: str = "Tadawul Fast Bridge"
    APP_VERSION: str = "4.0.0"
    APP_ENV: str = "production"  # production | staging | development
    LOG_LEVEL: str = "INFO"
    PYTHON_VERSION: str = "3.11.9"

    # -------------------------
    # Security & connectivity
    # -------------------------
    APP_TOKEN: Optional[str] = None
    ENABLE_CORS_ALL_ORIGINS: bool = True
    HTTP_TIMEOUT_SEC: float = 25.0

    # -------------------------
    # Providers
    # -------------------------
    ENABLED_PROVIDERS: Union[str, List[str]] = Field(default_factory=lambda: ["eodhd", "fmp", "yfinance"])
    PRIMARY_PROVIDER: str = "eodhd"

    EODHD_API_KEY: Optional[str] = None
    FMP_API_KEY: Optional[str] = None

    # Feature flags
    EODHD_FETCH_FUNDAMENTALS: bool = True
    ENABLE_YFINANCE: bool = True

    # -------------------------
    # Google integration (optional on backend)
    # -------------------------
    GOOGLE_SHEETS_CREDENTIALS: Optional[str] = None
    DEFAULT_SPREADSHEET_ID: Optional[str] = None
    GOOGLE_APPS_SCRIPT_BACKUP_URL: Optional[str] = None

    # -------------------------
    # Batch tuning
    # -------------------------
    ADV_BATCH_SIZE: int = 20
    ADV_BATCH_TIMEOUT_SEC: float = 60.0
    ADV_BATCH_CONCURRENCY: int = 5

    ENRICHED_BATCH_SIZE: int = 40
    ENRICHED_BATCH_TIMEOUT_SEC: float = 45.0
    ENRICHED_BATCH_CONCURRENCY: int = 5

    AI_BATCH_SIZE: int = 10
    AI_BATCH_TIMEOUT_SEC: float = 60.0
    AI_BATCH_CONCURRENCY: int = 5

    # -------------------------
    # Validators / normalizers
    # -------------------------
    @field_validator("APP_ENV", mode="before")
    @classmethod
    def _norm_env(cls, v: Any) -> str:
        s = (str(v).strip().lower() if v is not None else "production")
        if s in ("prod",):
            return "production"
        if s in ("dev",):
            return "development"
        return s or "production"

    @field_validator("LOG_LEVEL", mode="before")
    @classmethod
    def _norm_log_level(cls, v: Any) -> str:
        s = (str(v).strip().upper() if v is not None else "INFO")
        # keep permissive, but normalize common values
        mapping = {"WARN": "WARNING"}
        return mapping.get(s, s) or "INFO"

    @field_validator("APP_TOKEN", mode="before")
    @classmethod
    def _clean_token(cls, v: Any) -> Optional[str]:
        return _as_clean_str(v)

    @field_validator("HTTP_TIMEOUT_SEC", mode="before")
    @classmethod
    def _timeout_positive(cls, v: Any) -> float:
        try:
            x = float(v)
        except Exception:
            x = 25.0
        if x <= 0:
            x = 25.0
        return x

    @field_validator("ENABLED_PROVIDERS", mode="before")
    @classmethod
    def _parse_providers(cls, v: Union[str, List[str], None]) -> List[str]:
        lst = _parse_str_list(v)
        # Optional compatibility: if yfinance disabled explicitly
        # keep list as-is; feature flag will govern usage.
        return lst

    @field_validator("PRIMARY_PROVIDER", mode="before")
    @classmethod
    def _norm_primary_provider(cls, v: Any) -> str:
        s = (str(v).strip().lower() if v is not None else "")
        return s or "eodhd"

    @field_validator(
        "ADV_BATCH_SIZE",
        "ENRICHED_BATCH_SIZE",
        "AI_BATCH_SIZE",
        mode="before",
    )
    @classmethod
    def _batch_size_positive(cls, v: Any) -> int:
        try:
            x = int(v)
        except Exception:
            x = 20
        return max(1, x)

    @field_validator(
        "ADV_BATCH_TIMEOUT_SEC",
        "ENRICHED_BATCH_TIMEOUT_SEC",
        "AI_BATCH_TIMEOUT_SEC",
        mode="before",
    )
    @classmethod
    def _batch_timeout_positive(cls, v: Any) -> float:
        try:
            x = float(v)
        except Exception:
            x = 45.0
        return max(5.0, x)

    @field_validator(
        "ADV_BATCH_CONCURRENCY",
        "ENRICHED_BATCH_CONCURRENCY",
        "AI_BATCH_CONCURRENCY",
        mode="before",
    )
    @classmethod
    def _concurrency_positive(cls, v: Any) -> int:
        try:
            x = int(v)
        except Exception:
            x = 5
        return max(1, min(50, x))

    # -------------------------
    # Convenience computed helpers
    # -------------------------
    @property
    def enabled_providers_list(self) -> List[str]:
        # Ensure list type even if pydantic gave us a string (shouldn't happen with validator)
        return _parse_str_list(self.ENABLED_PROVIDERS)

    @property
    def primary_provider_resolved(self) -> str:
        """
        If PRIMARY_PROVIDER is not in enabled list, fall back to first enabled provider.
        """
        primary = (self.PRIMARY_PROVIDER or "").strip().lower()
        enabled = self.enabled_providers_list
        if primary and primary in enabled:
            return primary
        return enabled[0] if enabled else primary or "yfinance"

    def google_sheets_credentials_dict(self) -> Optional[Dict[str, Any]]:
        """
        Parse GOOGLE_SHEETS_CREDENTIALS JSON safely.
        Fixes Render newline escaping in private_key.
        """
        raw = _as_clean_str(self.GOOGLE_SHEETS_CREDENTIALS)
        if not raw:
            return None
        try:
            cleaned = raw
            if (cleaned.startswith("'") and cleaned.endswith("'")) or (cleaned.startswith('"') and cleaned.endswith('"')):
                cleaned = cleaned[1:-1]
            d = json.loads(cleaned)
            if isinstance(d, dict) and "private_key" in d and isinstance(d["private_key"], str):
                d["private_key"] = d["private_key"].replace("\\n", "\n")
            return d if isinstance(d, dict) else None
        except Exception:
            return None

    # -------------------------
    # Pydantic Settings config
    # -------------------------
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=True,
        extra="ignore",
    )


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    """
    Cached settings instance (safe for FastAPI dependency usage).
    """
    s = Settings()

    # Final guard: keep PRIMARY_PROVIDER consistent with ENABLED_PROVIDERS
    # (does not mutate env; just ensures runtime consistency)
    _ = s.primary_provider_resolved
    return s
