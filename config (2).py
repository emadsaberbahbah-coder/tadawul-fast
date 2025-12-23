# config.py
from __future__ import annotations

import base64
import json
from functools import lru_cache
from typing import Any, Dict, List, Optional, Set

from pydantic import AliasChoices, Field, field_validator, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


def _split_csv_lower(value: Optional[str]) -> List[str]:
    if not value:
        return []
    parts: List[str] = []
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


def _to_bool(v: Any, default: bool = False) -> bool:
    if v is None:
        return default
    if isinstance(v, bool):
        return v
    s = str(v).strip().lower()
    if s in {"1", "true", "yes", "y", "on"}:
        return True
    if s in {"0", "false", "no", "n", "off"}:
        return False
    return default


class Settings(BaseSettings):
    """
    Single source of truth for environment configuration.

    Important compatibility:
    - Accepts BOTH legacy env names and new names via AliasChoices.
    - Keeps `providers` as CSV string for backwards compatibility across modules.
    """

    # =========================
    # App
    # =========================
    app_name: str = Field(
        default="Tadawul Fast Bridge",
        validation_alias=AliasChoices("APP_NAME", "APP"),
    )
    env: str = Field(
        default="production",
        validation_alias=AliasChoices("APP_ENV", "ENV"),
    )
    version: str = Field(
        default="4.6.0",
        validation_alias=AliasChoices("APP_VERSION", "VERSION"),
    )

    backend_base_url: Optional[str] = Field(
        default=None,
        validation_alias=AliasChoices("BACKEND_BASE_URL", "BASE_URL"),
    )

    # =========================
    # Providers policy (GLOBAL)
    # =========================
    # Prefer ENABLED_PROVIDERS, but accept legacy PROVIDERS.
    providers: str = Field(
        default="eodhd,finnhub",
        validation_alias=AliasChoices("ENABLED_PROVIDERS", "PROVIDERS"),
    )
    primary_provider: Optional[str] = Field(
        default=None,
        validation_alias=AliasChoices("PRIMARY_PROVIDER",),
    )
    enable_yfinance: bool = Field(
        default=True,
        validation_alias=AliasChoices("ENABLE_YFINANCE",),
    )

    # =========================
    # KSA policy
    # =========================
    ksa_providers: Optional[str] = Field(
        default=None,
        validation_alias=AliasChoices("KSA_PROVIDERS",),
    )
    ksa_disallow_eodhd: bool = Field(
        default=True,
        validation_alias=AliasChoices("KSA_DISALLOW_EODHD",),
    )

    # =========================
    # Operational toggles
    # =========================
    cors_all_origins: bool = Field(
        default=True,
        validation_alias=AliasChoices("ENABLE_CORS_ALL_ORIGINS", "CORS_ALL_ORIGINS"),
    )
    cors_origins: Optional[str] = Field(
        default=None,
        validation_alias=AliasChoices("CORS_ORIGINS",),
    )

    log_level: str = Field(
        default="info",
        validation_alias=AliasChoices("LOG_LEVEL",),
    )
    log_json: bool = Field(
        default=False,
        validation_alias=AliasChoices("LOG_JSON",),
    )

    # Rate limiting (SlowAPI)
    rate_limit_per_minute: int = Field(
        default=240,
        validation_alias=AliasChoices("RATE_LIMIT_PER_MINUTE",),
    )

    # =========================
    # HTTP + caching
    # =========================
    http_timeout_sec: float = Field(
        default=25.0,
        validation_alias=AliasChoices("HTTP_TIMEOUT_SEC", "PROVIDER_TIMEOUT_SECONDS"),
    )
    cache_ttl_sec: int = Field(
        default=20,
        validation_alias=AliasChoices("CACHE_TTL_SEC", "CACHE_TTL_SECONDS"),
    )
    quote_ttl_sec: int = Field(
        default=30,
        validation_alias=AliasChoices("QUOTE_TTL_SEC",),
    )
    fundamentals_ttl_sec: int = Field(
        default=21600,
        validation_alias=AliasChoices("FUNDAMENTALS_TTL_SEC",),
    )
    argaam_snapshot_ttl_sec: int = Field(
        default=30,
        validation_alias=AliasChoices("ARGAAM_SNAPSHOT_TTL_SEC",),
    )

    # =========================
    # Security
    # =========================
    app_token: Optional[str] = Field(default=None, validation_alias=AliasChoices("APP_TOKEN",))
    backup_app_token: Optional[str] = Field(default=None, validation_alias=AliasChoices("BACKUP_APP_TOKEN",))

    # =========================
    # Provider API keys
    # =========================
    eodhd_api_key: Optional[str] = Field(default=None, validation_alias=AliasChoices("EODHD_API_KEY",))
    finnhub_api_key: Optional[str] = Field(default=None, validation_alias=AliasChoices("FINNHUB_API_KEY",))
    fmp_api_key: Optional[str] = Field(default=None, validation_alias=AliasChoices("FMP_API_KEY",))
    alpha_vantage_api_key: Optional[str] = Field(default=None, validation_alias=AliasChoices("ALPHA_VANTAGE_API_KEY",))
    argaam_api_key: Optional[str] = Field(default=None, validation_alias=AliasChoices("ARGAAM_API_KEY",))

    # =========================
    # Batch controls
    # =========================
    enriched_batch_size: int = Field(default=40, validation_alias=AliasChoices("ENRICHED_BATCH_SIZE",))
    enriched_batch_timeout_sec: int = Field(default=45, validation_alias=AliasChoices("ENRICHED_BATCH_TIMEOUT_SEC",))
    enriched_max_tickers: int = Field(default=250, validation_alias=AliasChoices("ENRICHED_MAX_TICKERS",))
    enriched_batch_concurrency: int = Field(default=5, validation_alias=AliasChoices("ENRICHED_BATCH_CONCURRENCY",))

    ai_batch_size: int = Field(default=20, validation_alias=AliasChoices("AI_BATCH_SIZE",))
    ai_batch_timeout_sec: int = Field(default=45, validation_alias=AliasChoices("AI_BATCH_TIMEOUT_SEC",))
    ai_max_tickers: int = Field(default=500, validation_alias=AliasChoices("AI_MAX_TICKERS",))
    ai_batch_concurrency: int = Field(default=5, validation_alias=AliasChoices("AI_BATCH_CONCURRENCY",))

    adv_batch_size: int = Field(default=25, validation_alias=AliasChoices("ADV_BATCH_SIZE",))
    adv_batch_timeout_sec: int = Field(default=45, validation_alias=AliasChoices("ADV_BATCH_TIMEOUT_SEC",))
    adv_max_tickers: int = Field(default=500, validation_alias=AliasChoices("ADV_MAX_TICKERS",))
    adv_batch_concurrency: int = Field(default=6, validation_alias=AliasChoices("ADV_BATCH_CONCURRENCY",))

    # =========================
    # Google Sheets
    # =========================
    google_sheets_credentials: Optional[str] = Field(
        default=None,
        validation_alias=AliasChoices("GOOGLE_SHEETS_CREDENTIALS",),
    )
    default_spreadsheet_id: Optional[str] = Field(
        default=None,
        validation_alias=AliasChoices("DEFAULT_SPREADSHEET_ID", "GOOGLE_SHEET_ID"),
    )
    google_sheet_range: Optional[str] = Field(
        default=None,
        validation_alias=AliasChoices("GOOGLE_SHEET_RANGE",),
    )

    # Sheets client tuning
    sheets_backend_timeout_sec: int = Field(default=120, validation_alias=AliasChoices("SHEETS_BACKEND_TIMEOUT_SEC",))
    sheets_backend_retries: int = Field(default=2, validation_alias=AliasChoices("SHEETS_BACKEND_RETRIES",))
    sheets_backend_retry_sleep: float = Field(default=1.0, validation_alias=AliasChoices("SHEETS_BACKEND_RETRY_SLEEP",))
    sheets_api_retries: int = Field(default=3, validation_alias=AliasChoices("SHEETS_API_RETRIES",))
    sheets_api_retry_base_sleep: float = Field(default=1.0, validation_alias=AliasChoices("SHEETS_API_RETRY_BASE_SLEEP",))
    sheets_max_rows_per_write: int = Field(default=500, validation_alias=AliasChoices("SHEETS_MAX_ROWS_PER_WRITE",))

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
        "backend_base_url",
        "providers",
        "primary_provider",
        "ksa_providers",
        "cors_origins",
        "log_level",
        "app_token",
        "backup_app_token",
        "eodhd_api_key",
        "finnhub_api_key",
        "fmp_api_key",
        "alpha_vantage_api_key",
        "argaam_api_key",
        "google_sheets_credentials",
        "default_spreadsheet_id",
        "google_sheet_range",
        mode="before",
    )
    @classmethod
    def strip_and_none_if_blank(cls, v):
        return _none_if_blank(v)

    @field_validator("enable_yfinance", "cors_all_origins", "log_json", "ksa_disallow_eodhd", mode="before")
    @classmethod
    def normalize_bool(cls, v, info):
        default = getattr(cls, info.field_name, False)  # type: ignore[attr-defined]
        return _to_bool(v, default=bool(default))

    @field_validator("providers", mode="before")
    @classmethod
    def normalize_providers_csv(cls, v):
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

    @field_validator("log_level", mode="before")
    @classmethod
    def normalize_log_level(cls, v):
        if v is None:
            return "info"
        s = str(v).strip()
        return s.lower() if s else "info"

    @model_validator(mode="after")
    def validate_settings(self):
        # If CORS is restricted but no origins provided => safe default (none)
        if not self.cors_all_origins and not self.cors_origins:
            self.cors_origins = None

        # Hard safety: avoid invalid values
        if self.rate_limit_per_minute <= 0:
            self.rate_limit_per_minute = 240
        if self.http_timeout_sec <= 0:
            self.http_timeout_sec = 25.0
        if self.cache_ttl_sec < 0:
            self.cache_ttl_sec = 0

        # Keep yfinance optional globally
        if not self.enable_yfinance:
            plist = [p for p in self.providers_list if p not in {"yfinance", "yahoo"}]
            self.providers = ",".join(plist) if plist else self.providers

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
        if p == "alphavantage":
            return bool(self.alpha_vantage_api_key)
        # providers that do not require keys in our design
        if p in {"yfinance", "yahoo", "argaam", "tadawul"}:
            return True
        # unknown providers => allow; engine may handle safely
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
        - If KSA_PROVIDERS is set => use it (filtered for safety + key enablement).
        - Else => default to ["tadawul","argaam"] regardless of global providers.
        - Always hard-block non-KSA providers if ksa_disallow_eodhd is True.
        """
        base = self.ksa_providers_list if self.ksa_providers_list else ["tadawul", "argaam"]

        hard_block = {"eodhd", "finnhub", "fmp", "alphavantage"}
        out: List[str] = []

        for p in base:
            pl = (p or "").strip().lower()
            if not pl:
                continue
            if self.ksa_disallow_eodhd and pl in hard_block:
                continue
            if self._has_key_for(pl):
                out.append(pl)

        if not out:
            out = ["tadawul", "argaam"]
        return out

    # -------------------------
    # Google credentials parsing (raw JSON or base64 JSON)
    # -------------------------
    @property
    def google_credentials_dict(self) -> Optional[Dict[str, Any]]:
        raw = self.google_sheets_credentials
        if not raw:
            return None

        # 1) raw JSON
        try:
            obj = json.loads(raw)
            return obj if isinstance(obj, dict) else None
        except Exception:
            pass

        # 2) base64 JSON
        try:
            decoded = base64.b64decode(raw).decode("utf-8", errors="strict")
            obj = json.loads(decoded)
            return obj if isinstance(obj, dict) else None
        except Exception:
            return None

    @property
    def has_google_sheets(self) -> bool:
        return bool(self.default_spreadsheet_id and self.google_credentials_dict)

    # -------------------------
    # Safe logging helper
    # -------------------------
    def safe_summary(self) -> Dict[str, Any]:
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
            "backend_base_url": self.backend_base_url,
            "providers": self.enabled_providers,
            "ksa_providers": self.enabled_ksa_providers,
            "primary_provider": (self.primary_provider or "").strip().lower() or None,
            "cors_all_origins": self.cors_all_origins,
            "rate_limit_per_minute": self.rate_limit_per_minute,
            "http_timeout_sec": self.http_timeout_sec,
            "cache_ttl_sec": self.cache_ttl_sec,
            "has_google_sheets": self.has_google_sheets,
            "app_token": _mask(self.app_token),
            "backup_app_token": _mask(self.backup_app_token),
            "eodhd_key": _mask(self.eodhd_api_key),
            "finnhub_key": _mask(self.finnhub_api_key),
            "fmp_key": _mask(self.fmp_api_key),
        }


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    return Settings()
