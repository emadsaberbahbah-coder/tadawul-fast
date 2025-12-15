# core/config.py
"""
core/config.py
------------------------------------------------------------
Compatibility shim (stable) – v2.0.0 (HARDENED)

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

import os
from typing import Any, Optional, List


# =============================================================================
# Preferred: repo root config.py
# =============================================================================
try:
    # ✅ Preferred
    from config import Settings as Settings  # type: ignore
    from config import get_settings as get_settings  # type: ignore

except Exception:  # pragma: no cover
    # =============================================================================
    # Defensive fallback: minimal Settings + env parsing
    # =============================================================================

    def _env_str(name: str, default: Optional[str] = None) -> Optional[str]:
        v = os.getenv(name)
        if v is None:
            return default
        v = str(v).strip()
        return v if v else default

    def _env_int(name: str, default: int) -> int:
        try:
            v = os.getenv(name)
            if v is None:
                return default
            n = int(str(v).strip())
            return n if n > 0 else default
        except Exception:
            return default

    def _env_float(name: str, default: float) -> float:
        try:
            v = os.getenv(name)
            if v is None:
                return default
            n = float(str(v).strip())
            return n if n > 0 else default
        except Exception:
            return default

    def _env_list(name: str, default: List[str]) -> List[str]:
        v = _env_str(name, None)
        if not v:
            return list(default)
        parts = [p.strip() for p in v.replace(";", ",").split(",") if p.strip()]
        return parts if parts else list(default)

    class Settings:  # type: ignore
        """
        Minimal fallback Settings.

        NOTE:
        - Keep attribute names compatible with routers:
            app_token / backup_app_token
            ai_batch_* / adv_batch_* / enriched_*
        - Keep provider lists compatible with DataEngine expectations.
        """

        # App meta
        app_name: str = _env_str("APP_NAME", "Tadawul Fast Bridge") or "Tadawul Fast Bridge"
        env: str = _env_str("ENV", _env_str("APP_ENV", "production")) or "production"
        version: str = _env_str("VERSION", "4.6.0") or "4.6.0"
        log_level: str = _env_str("LOG_LEVEL", "info") or "info"

        # Providers (non-KSA + KSA-safe)
        enabled_providers: List[str] = _env_list("ENABLED_PROVIDERS", ["fmp", "yahoo"])
        enabled_ksa_providers: List[str] = _env_list("ENABLED_KSA_PROVIDERS", ["tadawul", "argaam"])

        # Secrets / tokens (auth guard)
        app_token: Optional[str] = _env_str("APP_TOKEN", None)
        backup_app_token: Optional[str] = _env_str("BACKUP_APP_TOKEN", None)

        # Batch tuning used by routes (safe defaults)
        ai_batch_size: int = _env_int("AI_BATCH_SIZE", 20)
        ai_batch_timeout_sec: float = _env_float("AI_BATCH_TIMEOUT_SEC", 45.0)
        ai_batch_concurrency: int = _env_int("AI_BATCH_CONCURRENCY", 5)
        ai_max_tickers: int = _env_int("AI_MAX_TICKERS", 500)

        adv_batch_size: int = _env_int("ADV_BATCH_SIZE", 25)
        adv_batch_timeout_sec: float = _env_float("ADV_BATCH_TIMEOUT_SEC", 45.0)
        adv_batch_concurrency: int = _env_int("ADV_BATCH_CONCURRENCY", 6)
        adv_max_tickers: int = _env_int("ADV_MAX_TICKERS", 500)

        ENRICHED_MAX_TICKERS: int = _env_int("ENRICHED_MAX_TICKERS", 250)
        ENRICHED_BATCH_SIZE: int = _env_int("ENRICHED_BATCH_SIZE", 40)

        # Optional provider keys (kept for compatibility; may be unused)
        fmp_api_key: Optional[str] = _env_str("FMP_API_KEY", None)
        finnhub_api_key: Optional[str] = _env_str("FINNHUB_API_KEY", None)
        eodhd_api_key: Optional[str] = _env_str("EODHD_API_KEY", None)

        # Google Sheets (optional; kept minimal)
        google_sheets_credentials: Optional[str] = _env_str("GOOGLE_SHEETS_CREDENTIALS", None)

        def __init__(self, **kwargs: Any):
            for k, v in (kwargs or {}).items():
                setattr(self, k, v)

    _CACHED: Optional[Settings] = None

    def get_settings() -> Settings:  # type: ignore
        global _CACHED
        if _CACHED is None:
            _CACHED = Settings()
        return _CACHED


__all__ = ["Settings", "get_settings"]
