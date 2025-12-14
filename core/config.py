from __future__ import annotations

import base64
import json
from functools import lru_cache
from typing import Any, Dict, List, Optional, Union

from pydantic import field_validator
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


class Settings(BaseSettings):
    APP_NAME: str = "Tadawul Fast Bridge"
    APP_VERSION: str = "4.6.0"
    APP_ENV: str = "production"
    LOG_LEVEL: str = "INFO"

    ENABLE_CORS_ALL_ORIGINS: bool = True

    # Providers (informational / routing)
    ENABLED_PROVIDERS: Union[str, List[str]] = "eodhd,finnhub"
    PRIMARY_PROVIDER: str = "fmp"
    KSA_DISALLOW_EODHD: bool = True

    # Google Sheets
    GOOGLE_SHEETS_CREDENTIALS: Optional[str] = None
    DEFAULT_SPREADSHEET_ID: Optional[str] = None

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=True,
        extra="ignore",
    )

    @field_validator("ENABLE_CORS_ALL_ORIGINS", "KSA_DISALLOW_EODHD", mode="before")
    @classmethod
    def _v_bool(cls, v: Any) -> bool:
        return _parse_bool(v, default=False)

    @field_validator("ENABLED_PROVIDERS", mode="before")
    @classmethod
    def _v_providers(cls, v: Any) -> List[str]:
        if isinstance(v, list):
            return [str(x).strip().lower() for x in v if str(x).strip()]
        s = str(v or "").strip()
        if not s:
            return []
        if s.startswith("[") and s.endswith("]"):
            try:
                arr = json.loads(s)
                if isinstance(arr, list):
                    return [str(x).strip().lower() for x in arr if str(x).strip()]
            except Exception:
                pass
        return [p.strip().lower() for p in s.split(",") if p.strip()]

    def google_credentials_info(self) -> Optional[Dict[str, Any]]:
        raw = self.GOOGLE_SHEETS_CREDENTIALS
        if not raw:
            return None
        s = raw.strip()
        if s.lower().startswith("base64:"):
            try:
                b = base64.b64decode(s.split(":", 1)[1].strip())
                s = b.decode("utf-8", errors="replace").strip()
            except Exception:
                return None
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
