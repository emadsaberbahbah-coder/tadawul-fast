# config.py
from __future__ import annotations

import base64
import json
from functools import lru_cache
from typing import Any, Dict, List, Optional, Set

from pydantic import Field, field_validator, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


def _split_csv_lower(value: Optional[str]) -> List[str]:
    if not value:
        return []
    parts = []
    for x in str(value).split(","):
        x = x.strip().lower()
        if x:
            parts.append(x)
    # keep order, de-dup
    seen: Set[str] = set()
    out: List[str] = []
    for p in parts:
        if p not in seen:
            seen.add(p)
            out.append(p)
    return out


def _none_if_blank(v: Any) -> Any:
    if v is None:
        return None
    if isinstance(v, str):
        s = v.strip()
        return s if s else None
    return v


class Settings(BaseSettings):
    # =========================
    # App
    # =========================
    app_name: str = Field(default="Tadawul Fast Bridge", alias="APP_NAME")
    env: str = Field(default="production", alias="APP_ENV")
    version: str = Field(default="4.6.0", alias="APP_VERSION")

    # Providers CSV (GLOBAL defaults). Keep as string for backward compatibility.
    # Example: "eodhd,finnhub" or "fmp,yfinance"
    providers: str = Field(default="eodhd,finnhub", alias="PROVIDERS")

    # KSA-only providers CSV (optional).
    # If not provided, we auto-derive a KSA-safe list from enabled providers,
    # but ALWAYS exclude eodhd/finnhub/fmp for .SR unless your engine explicitly supports it.
    ksa_providers: Optional[str] = Field(default=None, alias="KSA_PROVIDERS")

    # Operational toggles
    cors_all_origins: bool = Field(default=True, alias="CORS_ALL_ORIGINS")
    cors_origins: Optional[str] = Field(default=None, alias="CORS_ORIGINS")  # optional CSV
    log_level: str = Field(default="INFO", alias="LOG_LEVEL")
    log_json: bool = Field(default=False, alias="LOG_JSON")

    # Rate limiting (SlowAPI)
    rate_limit_per_minute: int = Field(default=240, alias="RATE_LIMIT_PER_MINUTE")

    # HTTP behavior (used by engines/clients if they read settings)
    provider_timeout_seconds: float = Field(default=12.0, alias="PROVIDER_TIMEOUT_SECONDS")
    cache_ttl_seconds: int = Field(default=45, alias="CACHE_TTL_SECONDS")

    # =========================
    # Security
    # =========================
    app_token: Optional[str] = Field(default=None, alias="APP_TOKEN")

    # =========================
    # Provider API keys
    # =========================
    eodhd_api_key: Optional[str] = Field(default=None, alias="EODHD_API_KEY")
    finnhub_api_key: Optional[str] = Field(default=None, alias="FINNHUB_API_KEY")
    fmp_api_key: Optional[str] = Field(default=None, alias="FMP_API_KEY")

    # =========================
    # Google Sheets
    # =========================
    google_sheets_credentials: Optional[str] = Field(default=None, alias="GOOGLE_SHEETS_CREDENTIALS")
    google_sheet_id: Optional[str] = Field(default=None, alias="GOOGLE_SHEET_ID")
    google_sheet_range: Optional[str] = Field(default=None, alias="GOOGLE_SHEET_RANGE")  # optional e.g. "Market_Leaders!A:AZ"

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
        case_sensitive=False,
        populate_by_name=True,
    )

    # -------------------------
    # Normalizers
    # -------------------------
    @field_validator(
        "app_name",
        "env",
        "version",
        "providers",
        "ksa_providers",
        "cors_origins",
        "log_level",
        "app_token",
        "eodhd_api_key",
        "finnhub_api_key",
        "fmp_api_key",
        "google_sheets_credentials",
        "google_sheet_id",
        "google_sheet_range",
        mode="before",
    )
    @classmethod
    def strip_and_none_if_blank(cls, v):
        return _none_if_blank(v)

    @field_validator("providers", mode="before")
    @classmethod
    def normalize_providers_csv(cls, v):
        # Accept list-like values but store as CSV string for backward compatibility
        if v is None:
            return "eodhd,finnhub"
        if isinstance(v, (list, tuple, set)):
            items = [str(x).strip() for x in v if str(x).strip()]
            return ",".join(items)
        return str(v).strip()

    @field_validator("ksa_providers", mode="before")
    @classmethod
    def normalize_ksa_providers_csv(cls, v):
        if v is None:
            return None
        if isinstance(v, (list, tuple, set)):
            items = [str(x).strip() for x in v if str(x).strip()]
            return ",".join(items) if items else None
        s = str(v).strip()
        return s if s else None

    @model_validator(mode="after")
    def validate_settings(self):
        # If CORS is restricted but no origins provided => default to none (safe)
        if not self.cors_all_origins and not self.cors_origins:
            self.cors_origins = None
        # Hard safety: avoid negative/zero rate limits
        if self.rate_limit_per_minute <= 0:
            self.rate_limit_per_minute = 240
        # Reasonable bounds
        if self.provider_timeout_seconds <= 0:
            self.provider_timeout_seconds = 12.0
        if self.cache_ttl_seconds < 0:
            self.cache_ttl_seconds = 0
        return self

    # -------------------------
    # Computed lists
    # -------------------------
    @property
    def providers_list(self) -> List[str]:
        return _split_csv_lower(self.providers)

    @property
    def ksa_providers_list(self) -> List[str]:
        return _split_csv_lower(self.ksa_providers) if self.ksa_providers else []

    @property
    def cors_origins_list(self) -> List[str]:
        if self.cors_all_origins:
            return ["*"]
        if not self.cors_origins:
            return []
        return [x.strip() for x in self.cors_origins.split(",") if x.strip()]

    # -------------------------
    # Provider enablement logic
    # -------------------------
    def _has_key_for(self, provider: str) -> bool:
        p = (provider or "").strip().lower()
        if p == "eodhd":
            return bool(self.eodhd_api_key)
        if p == "finnhub":
            return bool(self.finnhub_api_key)
        if p == "fmp":
            return bool(self.fmp_api_key)
        # yfinance / argaam / tadawul may not require keys
        if p in {"yfinance", "yahoo", "argaam", "tadawul"}:
            return True
        # unknown providers => assume enabled, engine may handle
        return True

    @property
    def enabled_providers(self) -> List[str]:
        """Providers that are configured AND have required keys (if any)."""
        out: List[str] = []
        for p in self.providers_list:
            if self._has_key_for(p):
                out.append(p)
        return out

    @property
    def enabled_ksa_providers(self) -> List[str]:
        """
        KSA-safe provider list:
        - If KSA_PROVIDERS is set => use it (filtered for enablement + safety).
        - Else => derive from enabled_providers but ALWAYS exclude providers
          that are not KSA-safe by design.
        """
        # KSA-hard block list (keep backend safe even if env is misconfigured)
        hard_block = {"eodhd", "finnhub", "fmp"}  # these are global sources in your architecture
        base = self.ksa_providers_list if self.ksa_providers_list else self.enabled_providers

        out: List[str] = []
        for p in base:
            pl = p.strip().lower()
            if not pl:
                continue
            if pl in hard_block:
                continue
            if self._has_key_for(pl):
                out.append(pl)

        # If nothing left, default to your KSA gateway expectation
        # (routes_argaam / legacy engine should handle these without keys)
        if not out:
            out = ["tadawul", "argaam"]
        return out

    # -------------------------
    # Google credentials parsing (supports raw JSON or base64 JSON)
    # -------------------------
    @property
    def google_credentials_dict(self) -> Optional[Dict[str, Any]]:
        """
        Returns credentials JSON as dict.
        Accepts either:
          - raw JSON string
          - base64-encoded JSON string
        """
        raw = self.google_sheets_credentials
        if not raw:
            return None

        # 1) try raw JSON
        try:
            obj = json.loads(raw)
            return obj if isinstance(obj, dict) else None
        except Exception:
            pass

        # 2) try base64 JSON
        try:
            decoded = base64.b64decode(raw).decode("utf-8", errors="strict")
            obj = json.loads(decoded)
            return obj if isinstance(obj, dict) else None
        except Exception:
            return None

    @property
    def has_google_sheets(self) -> bool:
        return bool(self.google_sheet_id and self.google_credentials_dict)

    # -------------------------
    # Safe logging helper
    # -------------------------
    def safe_summary(self) -> Dict[str, Any]:
        """Small masked dict for logs/health endpoints."""
        def _mask(v: Optional[str]) -> Optional[str]:
            if not v:
                return None
            if len(v) <= 6:
                return "***"
            return v[:3] + "***" + v[-3:]

        return {
            "app_name": self.app_name,
            "env": self.env,
            "version": self.version,
            "providers": self.providers_list,
            "ksa_providers": self.enabled_ksa_providers,
            "cors_all_origins": self.cors_all_origins,
            "rate_limit_per_minute": self.rate_limit_per_minute,
            "timeout_s": self.provider_timeout_seconds,
            "cache_ttl_s": self.cache_ttl_seconds,
            "has_google_sheets": self.has_google_sheets,
            "app_token": _mask(self.app_token),
            "eodhd_key": _mask(self.eodhd_api_key),
            "finnhub_key": _mask(self.finnhub_api_key),
            "fmp_key": _mask(self.fmp_api_key),
        }


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    return Settings()
