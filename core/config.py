# core/config.py
"""
core/config.py
------------------------------------------------------------
Compatibility shim (stable) – v2.2.0 (HARDENED)

Many modules import settings via:
  from core.config import Settings, get_settings

Single source of truth is the repo-root `config.py`.
This module re-exports Settings/get_settings so old imports keep working.

Rules:
- Prefer root config.py always.
- Never crash the app if import graph is broken: provide a minimal fallback.
- Fallback reads common env vars so production can still boot.
"""

from __future__ import annotations

import json
import os
from typing import Any, Dict, List, Optional


# =============================================================================
# Preferred: repo root config.py
# =============================================================================
try:
    # ✅ Preferred (single source of truth)
    from config import Settings as Settings  # type: ignore
    from config import get_settings as get_settings  # type: ignore

except Exception:  # pragma: no cover
    # =============================================================================
    # Defensive fallback: minimal Settings + env parsing (NO pydantic dependency)
    # =============================================================================

    _TRUTHY = {"1", "true", "yes", "y", "on", "t"}
    _FALSY = {"0", "false", "no", "n", "off", "f"}

    def _env_raw(name: str) -> Optional[str]:
        v = os.getenv(name)
        return None if v is None else str(v)

    def _env_str(name: str, default: Optional[str] = None) -> Optional[str]:
        v = _env_raw(name)
        if v is None:
            return default
        v = v.strip()
        return v if v else default

    def _env_int(name: str, default: int) -> int:
        v = _env_raw(name)
        if v is None:
            return default
        try:
            n = int(str(v).strip())
            return n if n > 0 else default
        except Exception:
            return default

    def _env_float(name: str, default: float) -> float:
        v = _env_raw(name)
        if v is None:
            return default
        try:
            n = float(str(v).strip())
            return n if n > 0 else default
        except Exception:
            return default

    def _env_bool(name: str, default: bool) -> bool:
        v = _env_raw(name)
        if v is None:
            return default
        s = str(v).strip().lower()
        if s in _TRUTHY:
            return True
        if s in _FALSY:
            return False
        return default

    def _env_list(name: str, default: List[str]) -> List[str]:
        v = _env_str(name, None)
        if not v:
            return list(default)
        parts = [p.strip() for p in v.replace(";", ",").split(",") if p.strip()]
        return parts if parts else list(default)

    def _safe_json_loads(s: str) -> Optional[Any]:
        try:
            return json.loads(s)
        except Exception:
            return None

    def _coerce_private_key(creds_info: Dict[str, Any]) -> Dict[str, Any]:
        try:
            pk = creds_info.get("private_key")
            if isinstance(pk, str) and "\\n" in pk:
                creds_info["private_key"] = pk.replace("\\n", "\n")
        except Exception:
            pass
        return creds_info

    def _parse_json_dict_env(name: str) -> Optional[Dict[str, Any]]:
        """
        Accept JSON dict strings, including quoted JSON.
        Returns dict or None.
        """
        raw = _env_str(name, None)
        if not raw:
            return None
        cleaned = raw.strip()
        if (cleaned.startswith("'") and cleaned.endswith("'")) or (cleaned.startswith('"') and cleaned.endswith('"')):
            cleaned = cleaned[1:-1].strip()
        if not cleaned.startswith("{"):
            return None
        obj = _safe_json_loads(cleaned)
        if isinstance(obj, dict) and obj:
            return _coerce_private_key(obj)
        return None

    def _providers_list_from_env() -> List[str]:
        # Prefer ENABLED_PROVIDERS, then PROVIDERS
        raw = _env_str("ENABLED_PROVIDERS", None) or _env_str("PROVIDERS", None) or ""
        raw = raw.strip()
        if not raw:
            return []
        # allow JSON list
        if raw.startswith("[") and raw.endswith("]"):
            arr = _safe_json_loads(raw)
            if isinstance(arr, list):
                return [str(x).strip().lower() for x in arr if str(x).strip()]
        return [p.strip().lower() for p in raw.split(",") if p.strip()]

    def _ksa_providers_list_from_env() -> List[str]:
        raw = _env_str("KSA_PROVIDERS", None) or _env_str("ENABLED_KSA_PROVIDERS", None) or ""
        raw = raw.strip()
        if not raw:
            return []
        if raw.startswith("[") and raw.endswith("]"):
            arr = _safe_json_loads(raw)
            if isinstance(arr, list):
                return [str(x).strip().lower() for x in arr if str(x).strip()]
        return [p.strip().lower() for p in raw.split(",") if p.strip()]

    def _cors_origins_list_from_env() -> List[str]:
        if _env_bool("ENABLE_CORS_ALL_ORIGINS", _env_bool("CORS_ALL_ORIGINS", True)):
            return ["*"]
        raw = _env_str("CORS_ORIGINS", "") or ""
        out = [o.strip() for o in raw.split(",") if o.strip()]
        return out

    class Settings:  # type: ignore
        """
        Minimal fallback Settings.

        Goals:
        - Keep attribute names compatible with the existing codebase
          (routes, engines, google_sheets_service, main.py).
        - Provide provider list aliases used by DataEngine_v2:
            settings.providers_list (list) OR settings.providers (csv)
        - Provide auth token aliases used by routes:
            settings.app_token / settings.backup_app_token
            settings.APP_TOKEN / settings.BACKUP_APP_TOKEN
        """

        # -------------------------
        # App meta
        # -------------------------
        app_name: str = _env_str("APP_NAME", "Tadawul Fast Bridge") or "Tadawul Fast Bridge"
        env: str = _env_str("APP_ENV", _env_str("ENV", "production")) or "production"
        version: str = _env_str("APP_VERSION", _env_str("VERSION", "4.6.0")) or "4.6.0"
        log_level: str = _env_str("LOG_LEVEL", "info") or "info"

        # -------------------------
        # Backend identity
        # -------------------------
        backend_base_url: str = (_env_str("BACKEND_BASE_URL", "") or "").rstrip("/")
        default_spreadsheet_id: Optional[str] = _env_str("DEFAULT_SPREADSHEET_ID", None)

        # -------------------------
        # Auth tokens (routes guard)
        # -------------------------
        app_token: Optional[str] = _env_str("APP_TOKEN", None)
        backup_app_token: Optional[str] = _env_str("BACKUP_APP_TOKEN", None)

        # Uppercase aliases (some modules try getattr(settings, "APP_TOKEN"))
        APP_TOKEN: Optional[str] = app_token
        BACKUP_APP_TOKEN: Optional[str] = backup_app_token

        # -------------------------
        # Providers policy
        # -------------------------
        providers_list: List[str] = _providers_list_from_env() or ["finnhub", "fmp"]
        providers: str = ",".join(providers_list)

        enabled_providers: List[str] = providers_list  # legacy/alternate naming

        enabled_ksa_providers: List[str] = _ksa_providers_list_from_env() or ["tadawul", "argaam"]
        ksa_providers: str = ",".join(enabled_ksa_providers)

        enable_yfinance: bool = _env_bool("ENABLE_YFINANCE", True)
        argaam_detail_enabled: bool = _env_bool("ARGAAM_DETAIL_ENABLED", True)

        # -------------------------
        # HTTP + caching knobs (used by engines/services)
        # -------------------------
        http_timeout_sec: float = _env_float("HTTP_TIMEOUT_SEC", _env_float("HTTP_TIMEOUT", 25.0))
        cache_ttl_sec: float = _env_float("CACHE_TTL_SEC", 20.0)
        quote_ttl_sec: float = _env_float("QUOTE_TTL_SEC", 30.0)
        fundamentals_ttl_sec: float = _env_float("FUNDAMENTALS_TTL_SEC", 21600.0)
        argaam_snapshot_ttl_sec: float = _env_float("ARGAAM_SNAPSHOT_TTL_SEC", 30.0)
        argaam_detail_ttl_sec: float = _env_float("ARGAAM_DETAIL_TTL_SEC", 20.0)

        # -------------------------
        # Batch tuning used by routes
        # -------------------------
        ENRICHED_MAX_TICKERS: int = _env_int("ENRICHED_MAX_TICKERS", 250)
        ENRICHED_BATCH_SIZE: int = _env_int("ENRICHED_BATCH_SIZE", 40)
        ENRICHED_BATCH_TIMEOUT_SEC: float = _env_float("ENRICHED_BATCH_TIMEOUT_SEC", 45.0)
        ENRICHED_SINGLE_TIMEOUT_SEC: float = _env_float("ENRICHED_SINGLE_TIMEOUT_SEC", 12.0)
        ENRICHED_BATCH_CONCURRENCY: int = _env_int("ENRICHED_BATCH_CONCURRENCY", 5)

        AI_BATCH_SIZE: int = _env_int("AI_BATCH_SIZE", 20)
        AI_BATCH_TIMEOUT_SEC: float = _env_float("AI_BATCH_TIMEOUT_SEC", 45.0)
        AI_BATCH_CONCURRENCY: int = _env_int("AI_BATCH_CONCURRENCY", 5)
        AI_MAX_TICKERS: int = _env_int("AI_MAX_TICKERS", 500)

        ADV_BATCH_SIZE: int = _env_int("ADV_BATCH_SIZE", 25)
        ADV_BATCH_TIMEOUT_SEC: float = _env_float("ADV_BATCH_TIMEOUT_SEC", 45.0)
        ADV_BATCH_CONCURRENCY: int = _env_int("ADV_BATCH_CONCURRENCY", 6)
        ADV_MAX_TICKERS: int = _env_int("ADV_MAX_TICKERS", 500)

        # KSA timeouts (routes/enriched_quote.py reads these by env fallback too)
        KSA_SINGLE_TIMEOUT_SEC: float = _env_float("KSA_SINGLE_TIMEOUT_SEC", 10.0)
        KSA_BATCH_TIMEOUT_SEC: float = _env_float("KSA_BATCH_TIMEOUT_SEC", 20.0)
        KSA_FALLBACK_ENABLED: bool = _env_bool("KSA_FALLBACK_ENABLED", True)
        KSA_FALLBACK_ROUTE: str = _env_str("KSA_FALLBACK_ROUTE", "/v1/argaam/quote") or "/v1/argaam/quote"
        KSA_FALLBACK_TIMEOUT_SEC: float = _env_float("KSA_FALLBACK_TIMEOUT_SEC", 7.0)

        # -------------------------
        # Provider keys (optional)
        # -------------------------
        fmp_api_key: Optional[str] = _env_str("FMP_API_KEY", None)
        finnhub_api_key: Optional[str] = _env_str("FINNHUB_API_KEY", None)
        eodhd_api_key: Optional[str] = _env_str("EODHD_API_KEY", None)
        alpha_vantage_api_key: Optional[str] = _env_str("ALPHA_VANTAGE_API_KEY", None)

        # -------------------------
        # CORS (optional)
        # -------------------------
        cors_origins_list: List[str] = _cors_origins_list_from_env()

        # -------------------------
        # Google Sheets creds (optional; google_sheets_service.py expects these names)
        # -------------------------
        google_sheets_credentials_raw: Optional[str] = _env_str("GOOGLE_SHEETS_CREDENTIALS", None)
        google_sheets_credentials: Optional[str] = google_sheets_credentials_raw  # legacy alias
        google_credentials_dict: Optional[Dict[str, Any]] = _parse_json_dict_env("GOOGLE_SHEETS_CREDENTIALS")

        # -------------------------
        # Rate limiting (optional)
        # -------------------------
        rate_limit_per_minute: int = _env_int("RATE_LIMIT_PER_MINUTE", 240)

        def __init__(self, **kwargs: Any):
            # allow overrides (tests / local)
            for k, v in (kwargs or {}).items():
                setattr(self, k, v)

            # keep aliases consistent if overridden
            try:
                self.APP_TOKEN = getattr(self, "app_token", None)
                self.BACKUP_APP_TOKEN = getattr(self, "backup_app_token", None)
            except Exception:
                pass

            try:
                pl = getattr(self, "providers_list", None)
                if isinstance(pl, list):
                    self.providers = ",".join([str(x).strip().lower() for x in pl if str(x).strip()])
                    self.enabled_providers = pl
            except Exception:
                pass

    _CACHED: Optional[Settings] = None

    def get_settings() -> Settings:  # type: ignore
        global _CACHED
        if _CACHED is None:
            _CACHED = Settings()
        return _CACHED


__all__ = ["Settings", "get_settings"]

