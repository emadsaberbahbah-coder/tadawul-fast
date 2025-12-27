# routes/config.py  (FULL REPLACEMENT)
"""
routes/config.py
------------------------------------------------------------
Compatibility shim for older route modules that do:
  from routes.config import Settings, get_settings

Canonical source of truth is repo-root `config.py`.
Preferred import path:
  routes.config -> core.config -> (root) config.py

This module MUST:
- Never crash at import-time.
- Avoid heavy imports / side effects.
- Ensure Render env naming works (FINNHUB_API_KEY / EODHD_API_KEY etc.)
  by relying on core.config.get_settings() which exports runtime aliases.

Version: v1.3.0
"""

from __future__ import annotations

from typing import Any


# =============================================================================
# Preferred: import from core.config (which re-exports root config safely)
# =============================================================================
try:
    from core.config import Settings as Settings  # type: ignore
    from core.config import get_settings as get_settings  # type: ignore

except Exception:  # pragma: no cover
    # =============================================================================
    # Fallback: import directly from root config.py
    # =============================================================================
    try:
        from config import Settings as Settings  # type: ignore
        from config import get_settings as get_settings  # type: ignore

    except Exception:  # pragma: no cover
        # =============================================================================
        # Last-resort fallback: ultra-minimal env reader
        # (Keep it tiny; core.config should normally handle everything.)
        # =============================================================================
        import os
        from functools import lru_cache
        from typing import Optional, List, Dict

        _TRUTHY = {"1", "true", "yes", "y", "on", "t"}
        _FALSY = {"0", "false", "no", "n", "off", "f"}

        def _env_str(name: str, default: Optional[str] = None) -> Optional[str]:
            v = os.getenv(name)
            if v is None:
                return default
            s = str(v).strip()
            return s if s != "" else default

        def _env_first(names: List[str], default: Optional[str] = None) -> Optional[str]:
            for n in names:
                v = _env_str(n, None)
                if v is not None:
                    return v
            return default

        def _to_bool(v: Optional[str], default: bool = False) -> bool:
            if v is None:
                return default
            s = str(v).strip().lower()
            if s in _TRUTHY:
                return True
            if s in _FALSY:
                return False
            return default

        def _to_int(v: Optional[str], default: int) -> int:
            try:
                if v is None:
                    return default
                s = str(v).strip()
                if s == "":
                    return default
                return int(s)
            except Exception:
                return default

        def _to_float(v: Optional[str], default: float) -> float:
            try:
                if v is None:
                    return default
                s = str(v).strip()
                if s == "":
                    return default
                return float(s)
            except Exception:
                return default

        def _parse_list(raw: Optional[str]) -> List[str]:
            if raw is None:
                return []
            s = str(raw).strip()
            if not s:
                return []
            parts = [p.strip().lower() for p in s.split(",") if p.strip()]
            out: List[str] = []
            seen = set()
            for p in parts:
                if p in seen:
                    continue
                seen.add(p)
                out.append(p)
            return out

        def _export_env_if_missing(key: str, value: Optional[str]) -> None:
            if value is None:
                return
            v = str(value).strip()
            if not v:
                return
            cur = os.getenv(key)
            if cur is None or str(cur).strip() == "":
                os.environ[key] = v

        class Settings:  # type: ignore
            def __init__(self) -> None:
                self.service_name: str = _env_first(["SERVICE_NAME", "APP_NAME"], "Tadawul Stock Analysis API") or "Tadawul Stock Analysis API"
                self.service_version: str = _env_first(["SERVICE_VERSION", "APP_VERSION", "VERSION"], "0.0.0") or "0.0.0"
                self.environment: str = _env_first(["ENVIRONMENT", "APP_ENV", "ENV"], "production") or "production"
                self.tz: str = _env_first(["TZ", "TIMEZONE"], "Asia/Riyadh") or "Asia/Riyadh"

                self.debug: bool = _to_bool(_env_str("DEBUG", "false"), False)
                self.log_level: str = (_env_first(["LOG_LEVEL"], "info") or "info").lower()

                self.require_auth: bool = _to_bool(_env_first(["REQUIRE_AUTH"], "false"), False)
                self.app_token: Optional[str] = _env_first(["APP_TOKEN"], None)
                self.backup_app_token: Optional[str] = _env_first(["BACKUP_APP_TOKEN"], None)

                self.enabled_providers_raw: str = _env_first(["ENABLED_PROVIDERS"], "eodhd,finnhub") or "eodhd,finnhub"
                self.primary_provider: str = (_env_first(["PRIMARY_PROVIDER"], "eodhd") or "eodhd").strip().lower()

                # Tokens in Render are *_API_KEY; export *_API_TOKEN for providers that expect it
                self.eodhd_api_key: Optional[str] = _env_first(["EODHD_API_KEY", "EODHD_API_TOKEN", "EODHD_TOKEN"], None)
                self.finnhub_api_key: Optional[str] = _env_first(["FINNHUB_API_KEY", "FINNHUB_API_TOKEN", "FINNHUB_TOKEN"], None)

                _export_env_if_missing("EODHD_API_TOKEN", self.eodhd_api_key)
                _export_env_if_missing("FINNHUB_API_TOKEN", self.finnhub_api_key)

                self.http_timeout: float = _to_float(_env_first(["HTTP_TIMEOUT"], "30"), 30.0)
                self.max_retries: int = _to_int(_env_first(["MAX_RETRIES"], "2"), 2)
                self.retry_delay: float = _to_float(_env_first(["RETRY_DELAY"], "0.5"), 0.5)

            @property
            def app_name(self) -> str:
                return self.service_name

            @property
            def env(self) -> str:
                return self.environment

            @property
            def version(self) -> str:
                return self.service_version

            @property
            def enabled_providers(self) -> List[str]:
                xs = _parse_list(self.enabled_providers_raw)
                return xs or ["eodhd", "finnhub"]

            @property
            def eodhd_api_token(self) -> Optional[str]:
                return (self.eodhd_api_key or "").strip() or None

            @property
            def finnhub_api_token(self) -> Optional[str]:
                return (self.finnhub_api_key or "").strip() or None

            def safe_summary(self) -> Dict[str, Any]:
                return {
                    "APP_NAME": self.app_name,
                    "ENV": self.env,
                    "VERSION": self.version,
                    "LOG_LEVEL": self.log_level,
                    "REQUIRE_AUTH": bool(self.require_auth),
                    "APP_TOKEN_SET": bool((self.app_token or "").strip()),
                    "ENABLED_PROVIDERS": list(self.enabled_providers),
                    "PRIMARY_PROVIDER": self.primary_provider,
                    "EODHD_TOKEN_SET": bool((self.eodhd_api_token or "").strip()),
                    "FINNHUB_TOKEN_SET": bool((self.finnhub_api_token or "").strip()),
                }

            def __getattr__(self, name: str) -> Any:
                # Uppercase legacy
                if name.isupper():
                    if name == "APP_NAME":
                        return self.app_name
                    if name == "ENV":
                        return self.env
                    if name == "VERSION":
                        return self.version
                    if name == "EODHD_API_TOKEN":
                        return self.eodhd_api_token
                    if name == "FINNHUB_API_TOKEN":
                        return self.finnhub_api_token
                raise AttributeError(name)

        @lru_cache(maxsize=1)
        def get_settings() -> Settings:  # type: ignore
            return Settings()


__all__ = ["Settings", "get_settings"]
