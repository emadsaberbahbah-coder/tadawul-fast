"""
env.py
------------------------------------------------------------
Centralized environment configuration for Tadawul Fast Bridge (v4.6.0+)

Goals (enhanced)
- Strong, safe parsing for env vars (bool/int/float/csv/json/base64-json).
- Backward compatible exports used across the codebase.
- Aligns with config.py style + KSA-safe provider separation.
- Clear warnings when providers are enabled but keys are missing.
- Supports BOTH naming styles:
    * PROVIDERS (CSV)      + keys (EODHD_API_KEY, FINNHUB_API_KEY, FMP_API_KEY)
    * ENABLED_PROVIDERS    + PRIMARY_PROVIDER
    * HTTP_TIMEOUT / HTTP_TIMEOUT_SEC
    * CORS_ALL_ORIGINS / ENABLE_CORS_ALL_ORIGINS
- Google creds: accepts raw JSON or base64 JSON (Render-friendly).
"""

from __future__ import annotations

import base64
import json
import logging
import os
from typing import Any, Dict, List, Optional, Set

try:
    from pydantic import BaseModel, Field
except Exception:
    # Minimal fallback to keep app booting even if pydantic is unavailable (rare in prod)
    class BaseModel:  # type: ignore
        def __init__(self, **kwargs):
            self.__dict__.update(kwargs)

    def Field(default=None, **kwargs):  # type: ignore
        return default


try:
    from dotenv import load_dotenv  # type: ignore
    load_dotenv()
except Exception:
    pass


logger = logging.getLogger(__name__)

_TRUTHY = {"1", "true", "yes", "on", "y", "t"}
_FALSY = {"0", "false", "no", "off", "n", "f"}


# ------------------------------------------------------------
# Basic parsers
# ------------------------------------------------------------
def _get_raw(key: str) -> Optional[str]:
    v = os.getenv(key)
    return None if v is None else str(v)


def _get_str(key: str, default: str = "") -> str:
    v = _get_raw(key)
    if v is None:
        return default
    s = v.strip()
    return default if not s else s


def _get_int(key: str, default: int) -> int:
    raw = _get_raw(key)
    if raw is None:
        return default
    try:
        return int(str(raw).strip())
    except Exception:
        logger.warning("[env] Invalid int %s=%r, default=%s", key, raw, default)
        return default


def _get_float(key: str, default: float) -> float:
    raw = _get_raw(key)
    if raw is None:
        return default
    try:
        return float(str(raw).strip())
    except Exception:
        logger.warning("[env] Invalid float %s=%r, default=%s", key, raw, default)
        return default


def _get_bool(key: str, default: bool) -> bool:
    raw = _get_raw(key)
    if raw is None:
        return default
    s = str(raw).strip().lower()
    if s in _TRUTHY:
        return True
    if s in _FALSY:
        return False
    return default


def _split_csv(value: Optional[str]) -> List[str]:
    if not value:
        return []
    parts = [p.strip() for p in str(value).split(",")]
    parts = [p for p in parts if p]
    return parts


def _split_csv_lower_dedup(value: Optional[str]) -> List[str]:
    if not value:
        return []
    items = [x.strip().lower() for x in str(value).split(",") if x.strip()]
    seen: Set[str] = set()
    out: List[str] = []
    for it in items:
        if it not in seen:
            seen.add(it)
            out.append(it)
    return out


def _parse_list_or_csv(value: Optional[str]) -> List[str]:
    """
    Accepts:
      - JSON list: ["eodhd","fmp"]
      - CSV: eodhd,fmp
    Returns lowercase list de-duplicated (order preserved).
    """
    if not value:
        return []
    v = str(value).strip()
    if not v:
        return []
    # JSON list
    if v.startswith("[") and v.endswith("]"):
        try:
            arr = json.loads(v)
            if isinstance(arr, list):
                return _split_csv_lower_dedup(",".join(str(x) for x in arr))
        except Exception:
            pass
    # CSV
    return _split_csv_lower_dedup(v)


def _maybe_unquote(raw: str) -> str:
    s = raw.strip()
    if len(s) >= 2 and ((s[0] == s[-1] == '"') or (s[0] == s[-1] == "'")):
        return s[1:-1].strip()
    return s


def _load_json_or_b64json(value: Optional[str]) -> Optional[Dict[str, Any]]:
    """
    Accept:
      - raw JSON dict string
      - base64(JSON)
    Return dict or None.
    """
    if not value:
        return None
    raw = _maybe_unquote(str(value))

    # Try raw JSON
    try:
        obj = json.loads(raw)
        if isinstance(obj, dict):
            return obj
    except Exception:
        pass

    # Try base64 JSON
    try:
        decoded = base64.b64decode(raw).decode("utf-8", errors="strict")
        obj = json.loads(decoded)
        if isinstance(obj, dict):
            return obj
    except Exception:
        return None

    return None


# ------------------------------------------------------------
# Settings model (pydantic if available)
# ------------------------------------------------------------
class Settings(BaseModel):
    # App identity
    app_name: str = Field(default_factory=lambda: _get_str("APP_NAME", "Tadawul Fast Bridge"))
    app_env: str = Field(default_factory=lambda: _get_str("APP_ENV", "production"))
    app_version: str = Field(default_factory=lambda: _get_str("APP_VERSION", "4.6.0"))
    log_level: str = Field(default_factory=lambda: _get_str("LOG_LEVEL", "INFO").upper())
    log_json: bool = Field(default_factory=lambda: _get_bool("LOG_JSON", False))

    # Service base url (used by client/legacy modules)
    backend_base_url: str = Field(
        default_factory=lambda: _get_str("BACKEND_BASE_URL", "https://tadawul-fast-bridge.onrender.com").rstrip("/")
    )

    # Timeout: accept both
    http_timeout_sec: float = Field(
        default_factory=lambda: _get_float("HTTP_TIMEOUT_SEC", _get_float("HTTP_TIMEOUT", 25.0))
    )

    # Tokens
    app_token: Optional[str] = Field(default_factory=lambda: _get_raw("APP_TOKEN"))
    backup_app_token: Optional[str] = Field(default_factory=lambda: _get_raw("BACKUP_APP_TOKEN"))

    # Provider configuration (support BOTH naming styles)
    # 1) New style: PROVIDERS="eodhd,finnhub" (CSV)
    providers_csv: str = Field(default_factory=lambda: _get_str("PROVIDERS", ""))

    # 2) Legacy style: ENABLED_PROVIDERS="eodhd,fmp,yfinance" or JSON
    enabled_providers: List[str] = Field(
        default_factory=lambda: _parse_list_or_csv(_get_str("ENABLED_PROVIDERS", ""))  # may be empty
    )

    primary_provider: str = Field(default_factory=lambda: _get_str("PRIMARY_PROVIDER", "").lower())

    # KSA-safe providers override (optional)
    ksa_providers: List[str] = Field(
        default_factory=lambda: _parse_list_or_csv(_get_str("KSA_PROVIDERS", ""))  # may be empty
    )

    # Provider keys + base urls
    eodhd_api_key: Optional[str] = Field(default_factory=lambda: _get_raw("EODHD_API_KEY"))
    eodhd_base_url: str = Field(default_factory=lambda: _get_str("EODHD_BASE_URL", "https://eodhd.com/api").rstrip("/"))

    fmp_api_key: Optional[str] = Field(default_factory=lambda: _get_raw("FMP_API_KEY"))
    fmp_base_url: str = Field(
        default_factory=lambda: _get_str("FMP_BASE_URL", "https://financialmodelingprep.com/api/v3").rstrip("/")
    )

    finnhub_api_key: Optional[str] = Field(default_factory=lambda: _get_raw("FINNHUB_API_KEY"))
    alpha_vantage_api_key: Optional[str] = Field(default_factory=lambda: _get_raw("ALPHA_VANTAGE_API_KEY"))

    # Argaam gateway config (optional)
    argaam_gateway_url: Optional[str] = Field(default_factory=lambda: _get_raw("ARGAAM_GATEWAY_URL"))
    argaam_api_key: Optional[str] = Field(default_factory=lambda: _get_raw("ARGAAM_API_KEY"))

    # Feature toggles
    enable_yfinance: bool = Field(default_factory=lambda: _get_bool("ENABLE_YFINANCE", True))

    # Cache/TTL (align with render.yaml naming too)
    cache_ttl_sec: float = Field(default_factory=lambda: _get_float("CACHE_TTL_SEC", _get_float("ENGINE_CACHE_TTL_SECONDS", 20.0)))
    quote_ttl_sec: float = Field(default_factory=lambda: _get_float("QUOTE_TTL_SEC", 30.0))
    fundamentals_ttl_sec: float = Field(default_factory=lambda: _get_float("FUNDAMENTALS_TTL_SEC", 21600.0))
    argaam_snapshot_ttl_sec: float = Field(default_factory=lambda: _get_float("ARGAAM_SNAPSHOT_TTL_SEC", 30.0))

    # Batch limits
    adv_batch_size: int = Field(default_factory=lambda: _get_int("ADV_BATCH_SIZE", 20))
    adv_batch_timeout_sec: float = Field(default_factory=lambda: _get_float("ADV_BATCH_TIMEOUT_SEC", 45.0))
    adv_max_tickers: int = Field(default_factory=lambda: _get_int("ADV_MAX_TICKERS", 500))
    adv_batch_concurrency: int = Field(default_factory=lambda: _get_int("ADV_BATCH_CONCURRENCY", 5))

    ai_batch_size: int = Field(default_factory=lambda: _get_int("AI_BATCH_SIZE", 20))
    ai_batch_timeout_sec: float = Field(default_factory=lambda: _get_float("AI_BATCH_TIMEOUT_SEC", 45.0))
    ai_max_tickers: int = Field(default_factory=lambda: _get_int("AI_MAX_TICKERS", 500))
    ai_batch_concurrency: int = Field(default_factory=lambda: _get_int("AI_BATCH_CONCURRENCY", 5))

    enriched_batch_size: int = Field(default_factory=lambda: _get_int("ENRICHED_BATCH_SIZE", 40))
    enriched_batch_timeout_sec: float = Field(default_factory=lambda: _get_float("ENRICHED_BATCH_TIMEOUT_SEC", 45.0))
    enriched_max_tickers: int = Field(default_factory=lambda: _get_int("ENRICHED_MAX_TICKERS", 250))
    enriched_batch_concurrency: int = Field(default_factory=lambda: _get_int("ENRICHED_BATCH_CONCURRENCY", 5))

    # Sheet names
    sheet_ksa_tadawul: str = Field(default_factory=lambda: _get_str("SHEET_KSA_TADAWUL", "KSA_Tadawul_Market"))
    sheet_global_markets: str = Field(default_factory=lambda: _get_str("SHEET_GLOBAL_MARKETS", "Global_Markets"))
    sheet_mutual_funds: str = Field(default_factory=lambda: _get_str("SHEET_MUTUAL_FUNDS", "Mutual_Funds"))
    sheet_commodities_fx: str = Field(default_factory=lambda: _get_str("SHEET_COMMODITIES_FX", "Commodities_FX"))
    sheet_market_leaders: str = Field(default_factory=lambda: _get_str("SHEET_MARKET_LEADERS", "Market_Leaders"))
    sheet_my_portfolio: str = Field(default_factory=lambda: _get_str("SHEET_MY_PORTFOLIO", "My_Portfolio"))
    sheet_insights_analysis: str = Field(default_factory=lambda: _get_str("SHEET_INSIGHTS_ANALYSIS", "Insights_Analysis"))
    sheet_investment_advisor: str = Field(default_factory=lambda: _get_str("SHEET_INVESTMENT_ADVISOR", "Investment_Advisor"))
    sheet_economic_calendar: str = Field(default_factory=lambda: _get_str("SHEET_ECONOMIC_CALENDAR", "Economic_Calendar"))
    sheet_investment_income: str = Field(default_factory=lambda: _get_str("SHEET_INVESTMENT_INCOME", "Investment_Income_Statement"))

    # Google integration
    google_sheets_credentials_raw: Optional[str] = Field(default_factory=lambda: _get_raw("GOOGLE_SHEETS_CREDENTIALS"))
    google_apps_script_backup_url: Optional[str] = Field(default_factory=lambda: _get_raw("GOOGLE_APPS_SCRIPT_BACKUP_URL"))
    default_spreadsheet_id: Optional[str] = Field(default_factory=lambda: _get_raw("DEFAULT_SPREADSHEET_ID"))
    google_sheet_id: Optional[str] = Field(default_factory=lambda: _get_raw("GOOGLE_SHEET_ID"))
    google_sheet_range: Optional[str] = Field(default_factory=lambda: _get_raw("GOOGLE_SHEET_RANGE"))

    # CORS naming variants
    enable_cors_all_origins: bool = Field(default_factory=lambda: _get_bool("ENABLE_CORS_ALL_ORIGINS", _get_bool("CORS_ALL_ORIGINS", True)))
    cors_all_origins: bool = Field(default_factory=lambda: _get_bool("CORS_ALL_ORIGINS", True))
    cors_origins: Optional[str] = Field(default_factory=lambda: _get_raw("CORS_ORIGINS"))

    # -------------------------
    # Derived/computed
    # -------------------------
    @property
    def providers(self) -> List[str]:
        """
        Final enabled providers list (lowercase, dedup, order preserved):
        Priority:
          1) PROVIDERS if set
          2) ENABLED_PROVIDERS if set
          3) default fallback
        """
        if self.providers_csv:
            lst = _parse_list_or_csv(self.providers_csv)
            return lst if lst else []
        if self.enabled_providers:
            return self.enabled_providers
        return ["eodhd", "finnhub"]

    @property
    def primary(self) -> str:
        if self.primary_provider:
            return self.primary_provider
        # choose first provider if available
        return (self.providers[0] if self.providers else "eodhd").lower()

    @property
    def google_sheets_credentials(self) -> Optional[Dict[str, Any]]:
        return _load_json_or_b64json(self.google_sheets_credentials_raw)

    @property
    def has_google_sheets(self) -> bool:
        return bool((self.google_sheet_id or self.default_spreadsheet_id) and self.google_sheets_credentials)

    @property
    def is_production(self) -> bool:
        return self.app_env.strip().lower() in {"prod", "production"}

    # KSA safety: hard-block providers not intended for KSA in your architecture
    @property
    def enabled_ksa_providers(self) -> List[str]:
        hard_block = {"eodhd", "finnhub", "fmp"}  # KSA should route via tadawul/argaam in your design
        base = self.ksa_providers if self.ksa_providers else self.providers
        out: List[str] = []
        for p in base:
            pl = (p or "").strip().lower()
            if not pl:
                continue
            if pl in hard_block:
                continue
            out.append(pl)
        if not out:
            out = ["tadawul", "argaam"]
        return out

    def _has_key(self, provider: str) -> bool:
        p = (provider or "").strip().lower()
        if p == "eodhd":
            return bool(self.eodhd_api_key)
        if p == "finnhub":
            return bool(self.finnhub_api_key)
        if p == "fmp":
            return bool(self.fmp_api_key)
        if p in {"yfinance", "yahoo", "argaam", "tadawul"}:
            return True
        return True

    def post_init_warnings(self) -> None:
        logger.info("[env] App=%s | Env=%s | Version=%s", self.app_name, self.app_env, self.app_version)
        logger.info("[env] Providers=%s", ",".join(self.providers or []))
        logger.info("[env] KSA Providers=%s", ",".join(self.enabled_ksa_providers or []))

        # Warn missing keys for enabled providers
        for p in self.providers:
            if p in {"eodhd", "finnhub", "fmp"} and not self._has_key(p):
                logger.warning("[env] %s enabled but required API key missing.", p.upper())

        # Warn if google creds look missing
        if (self.google_sheet_id or self.default_spreadsheet_id) and not self.google_sheets_credentials:
            logger.warning("[env] Google Sheet ID is set but GOOGLE_SHEETS_CREDENTIALS is missing/invalid JSON.")

        # Warn about provider timeout bounds
        if self.http_timeout_sec <= 0:
            logger.warning("[env] HTTP timeout <= 0; using safe default 25 seconds.")


def _init_settings() -> Settings:
    try:
        s = Settings()
        s.post_init_warnings()
        return s
    except Exception as exc:
        logger.error("[env] Failed to initialize Settings: %s", exc)
        # Try minimum safe settings (should still boot)
        return Settings()  # type: ignore


settings: Settings = _init_settings()

# ------------------------------------------------------------
# Backward compatible exports (DON'T BREAK IMPORTS)
# ------------------------------------------------------------
APP_ENV = settings.app_env
APP_NAME = settings.app_name
APP_VERSION = settings.app_version
BACKEND_BASE_URL = settings.backend_base_url

APP_TOKEN = settings.app_token
BACKUP_APP_TOKEN = settings.backup_app_token

# Provider exports
ENABLED_PROVIDERS = settings.providers
PRIMARY_PROVIDER = settings.primary

# Timeouts
HTTP_TIMEOUT = int(settings.http_timeout_sec)
HTTP_TIMEOUT_SEC = float(settings.http_timeout_sec)

# Provider keys & urls
EODHD_API_KEY = settings.eodhd_api_key
EODHD_BASE_URL = settings.eodhd_base_url
FMP_API_KEY = settings.fmp_api_key
FMP_BASE_URL = settings.fmp_base_url
FINNHUB_API_KEY = settings.finnhub_api_key
ALPHA_VANTAGE_API_KEY = settings.alpha_vantage_api_key

ARGAAM_GATEWAY_URL = settings.argaam_gateway_url
ARGAAM_API_KEY = settings.argaam_api_key

# CORS
ENABLE_CORS_ALL_ORIGINS = settings.enable_cors_all_origins or settings.cors_all_origins
CORS_ALL_ORIGINS = settings.cors_all_origins
CORS_ORIGINS = settings.cors_origins

# Cache / TTL
CACHE_TTL_SEC = float(settings.cache_ttl_sec)
QUOTE_TTL_SEC = float(settings.quote_ttl_sec)
FUNDAMENTALS_TTL_SEC = float(settings.fundamentals_ttl_sec)
ARGAAM_SNAPSHOT_TTL_SEC = float(settings.argaam_snapshot_ttl_sec)

# Batch limits
ADV_BATCH_SIZE = int(settings.adv_batch_size)
ADV_BATCH_TIMEOUT_SEC = float(settings.adv_batch_timeout_sec)
ADV_MAX_TICKERS = int(settings.adv_max_tickers)
ADV_BATCH_CONCURRENCY = int(settings.adv_batch_concurrency)

AI_BATCH_SIZE = int(settings.ai_batch_size)
AI_BATCH_TIMEOUT_SEC = float(settings.ai_batch_timeout_sec)
AI_MAX_TICKERS = int(settings.ai_max_tickers)
AI_BATCH_CONCURRENCY = int(settings.ai_batch_concurrency)

ENRICHED_BATCH_SIZE = int(settings.enriched_batch_size)
ENRICHED_BATCH_TIMEOUT_SEC = float(settings.enriched_batch_timeout_sec)
ENRICHED_MAX_TICKERS = int(settings.enriched_max_tickers)
ENRICHED_BATCH_CONCURRENCY = int(settings.enriched_batch_concurrency)

# Sheets
SHEET_KSA_TADAWUL = settings.sheet_ksa_tadawul
SHEET_GLOBAL_MARKETS = settings.sheet_global_markets
SHEET_MUTUAL_FUNDS = settings.sheet_mutual_funds
SHEET_COMMODITIES_FX = settings.sheet_commodities_fx
SHEET_MARKET_LEADERS = settings.sheet_market_leaders
SHEET_MY_PORTFOLIO = settings.sheet_my_portfolio
SHEET_INSIGHTS_ANALYSIS = settings.sheet_insights_analysis
SHEET_INVESTMENT_ADVISOR = settings.sheet_investment_advisor
SHEET_ECONOMIC_CALENDAR = settings.sheet_economic_calendar
SHEET_INVESTMENT_INCOME = settings.sheet_investment_income

# Google
GOOGLE_SHEETS_CREDENTIALS = settings.google_sheets_credentials  # dict or None
DEFAULT_SPREADSHEET_ID = settings.default_spreadsheet_id
GOOGLE_SHEET_ID = settings.google_sheet_id
GOOGLE_SHEET_RANGE = settings.google_sheet_range
GOOGLE_APPS_SCRIPT_BACKUP_URL = settings.google_apps_script_backup_url
HAS_GOOGLE_SHEETS = settings.has_google_sheets

# Logging
LOG_LEVEL = settings.log_level
LOG_JSON = settings.log_json
IS_PRODUCTION = settings.is_production

# KSA Providers (for engines/routers that want explicit KSA-safe list)
KSA_PROVIDERS = settings.enabled_ksa_providers

__all__ = ["Settings", "settings"]
