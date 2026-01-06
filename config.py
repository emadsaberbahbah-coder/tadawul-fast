# config.py  (FULL REPLACEMENT)
"""
TADAWUL FAST BRIDGE – MAIN CONFIG (v2.8.0) – PROD SAFE / NO HEAVY IMPORTS

Goals
- ✅ Single canonical source of truth for settings used across routes + engine.
- ✅ Works without pydantic-settings (plain dataclass + env parsing).
- ✅ Token mode:
    - If APP_TOKEN (or BACKUP_APP_TOKEN) is set -> token-protected endpoints
    - If none are set -> OPEN mode (no auth)
- ✅ Aligns with latest env naming used by:
    - main.py  (DEFER_ROUTER_MOUNT / INIT_ENGINE_ON_BOOT / providers lists)
    - env.py   (enabled_providers / ksa_providers compatibility)
    - render.yaml (AI_TIMEOUT_SEC / AI_CONCURRENCY aliases, etc.)
- ✅ Never raises at import-time; safe to import at startup.

Notes
- This module intentionally avoids any heavy imports (engine/providers/etc.).
"""

from __future__ import annotations

import base64
import json
import os
from dataclasses import asdict, dataclass, field
from functools import lru_cache
from typing import Any, Dict, List, Optional

CONFIG_VERSION = "2.8.0"

_TRUTHY = {"1", "true", "yes", "y", "on", "t", "enable", "enabled"}
_FALSY = {"0", "false", "no", "n", "off", "f", "disable", "disabled"}


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
    if s in _TRUTHY:
        return True
    if s in _FALSY:
        return False
    return default


def _split_csv(v: Any) -> List[str]:
    s = _strip(v)
    if not s:
        return []
    parts = [p.strip() for p in s.split(",")]
    return [p for p in parts if p]


def _as_list_lower(v: Any) -> List[str]:
    """
    Accepts:
      - list
      - "a,b,c"
      - '["a","b"]'
    Returns: deduped lowercase list
    """
    if v is None:
        return []
    if isinstance(v, list):
        raw = [str(x).strip().lower() for x in v if str(x).strip()]
    else:
        s = _strip(v)
        if not s:
            return []
        if s.startswith("[") and s.endswith("]"):
            try:
                arr = json.loads(s)
                if isinstance(arr, list):
                    raw = [str(x).strip().lower() for x in arr if str(x).strip()]
                else:
                    raw = []
            except Exception:
                raw = []
        else:
            raw = [p.strip().lower() for p in s.split(",") if p.strip()]

    out: List[str] = []
    seen = set()
    for x in raw:
        if x and x not in seen:
            seen.add(x)
            out.append(x)
    return out


def _mask_secret(s: Optional[str]) -> Optional[str]:
    if not s:
        return None
    t = s.strip()
    if len(t) <= 6:
        return "***"
    return t[:3] + "***" + t[-3:]


def _maybe_decode_b64_json(s: str) -> Optional[str]:
    """
    - If value is JSON -> return it
    - If value is base64(JSON) -> decode+return JSON
    - Else return original string (still useful for some setups)
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


def _env_first(*keys: str) -> Optional[str]:
    for k in keys:
        v = os.getenv(k)
        if v is not None and str(v).strip():
            return str(v).strip()
    return None


@dataclass(frozen=True)
class Settings:
    # ------------------------------------------------------------------
    # Identity
    # ------------------------------------------------------------------
    service_name: str = "tadawul-fast-bridge"
    environment: str = "prod"
    log_level: str = "INFO"
    app_version: str = ""  # optional (ENV: APP_VERSION / SERVICE_VERSION / VERSION / RELEASE)

    # ------------------------------------------------------------------
    # Fast boot controls (main.py reads these from settings if present)
    # ------------------------------------------------------------------
    defer_router_mount: bool = True          # ENV: DEFER_ROUTER_MOUNT
    init_engine_on_boot: bool = True         # ENV: INIT_ENGINE_ON_BOOT

    # Feature flags (optional)
    ai_analysis_enabled: bool = True         # ENV: AI_ANALYSIS_ENABLED
    advanced_analysis_enabled: bool = True   # ENV: ADVANCED_ANALYSIS_ENABLED

    # ------------------------------------------------------------------
    # Auth
    # ------------------------------------------------------------------
    app_token: Optional[str] = None
    backup_app_token: Optional[str] = None
    require_auth: bool = False              # ENV: REQUIRE_AUTH (still OPEN if no tokens)

    # ------------------------------------------------------------------
    # Providers policy (settings-based compatibility for env.py/main.py)
    # ------------------------------------------------------------------
    enabled_providers: List[str] = field(default_factory=lambda: ["eodhd", "finnhub"])
    ksa_providers: List[str] = field(default_factory=lambda: ["yahoo_chart", "tadawul", "argaam"])
    primary_provider: str = "eodhd"
    ksa_disallow_eodhd: bool = True         # ENV: KSA_DISALLOW_EODHD

    # ------------------------------------------------------------------
    # HTTP + caching
    # ------------------------------------------------------------------
    http_timeout_sec: float = 25.0          # ENV: HTTP_TIMEOUT_SEC / HTTP_TIMEOUT
    cache_ttl_sec: float = 20.0             # ENV: CACHE_TTL_SEC / CACHE_DEFAULT_TTL
    engine_cache_ttl_sec: int = 20          # ENV: ENGINE_CACHE_TTL_SEC / ENGINE_TTL_SEC

    # ------------------------------------------------------------------
    # Batching controls (routes + engine)
    # - AI: used by routes/ai_analysis.py
    # - ADV: used by routes/advanced_analysis.py (if separate)
    # - ENRICHED: used by enriched endpoints
    # ------------------------------------------------------------------
    ai_batch_size: int = 25
    ai_batch_timeout_sec: float = 45.0
    ai_batch_concurrency: int = 6
    ai_max_tickers: int = 800

    adv_batch_size: int = 25
    adv_batch_timeout_sec: float = 45.0
    adv_batch_concurrency: int = 6
    adv_max_tickers: int = 800

    enriched_batch_size: int = 40
    enriched_timeout_sec: float = 45.0
    enriched_batch_concurrency: int = 5
    enriched_max_tickers: int = 250

    # ------------------------------------------------------------------
    # CORS
    # ------------------------------------------------------------------
    enable_cors_all_origins: bool = True
    cors_origins: List[str] = field(default_factory=lambda: ["*"])

    # ------------------------------------------------------------------
    # Google Sheets (optional)
    # ------------------------------------------------------------------
    default_spreadsheet_id: Optional[str] = None
    tfb_spreadsheet_id: Optional[str] = None
    google_sheets_credentials: Optional[str] = None  # JSON or base64(JSON) decoded
    google_application_credentials: Optional[str] = None  # file path (if used)

    # Apps Script (optional)
    google_apps_script_url: Optional[str] = None
    google_apps_script_backup_url: Optional[str] = None

    # ------------------------------------------------------------------
    # Optional direct provider URLs (optional)
    # ------------------------------------------------------------------
    tadawul_quote_url: Optional[str] = None
    tadawul_fundamentals_url: Optional[str] = None
    argaam_base_url: Optional[str] = None

    # ------------------------------------------------------------------
    # Misc
    # ------------------------------------------------------------------
    timezone_default: str = "Asia/Riyadh"

    @staticmethod
    def from_env() -> "Settings":
        service_name = _strip(os.getenv("SERVICE_NAME") or os.getenv("APP_NAME") or "tadawul-fast-bridge")
        environment = _strip(os.getenv("ENVIRONMENT") or os.getenv("APP_ENV") or os.getenv("ENV") or "prod").lower()
        log_level = _strip(os.getenv("LOG_LEVEL") or "INFO").upper()

        # Version: ENV first (align with env.py)
        app_version = (
            _env_first("APP_VERSION", "SERVICE_VERSION", "VERSION", "RELEASE") or ""
        ).strip()

        defer_router_mount = _coerce_bool(os.getenv("DEFER_ROUTER_MOUNT"), True)
        init_engine_on_boot = _coerce_bool(os.getenv("INIT_ENGINE_ON_BOOT"), True)

        ai_analysis_enabled = _coerce_bool(os.getenv("AI_ANALYSIS_ENABLED"), True)
        advanced_analysis_enabled = _coerce_bool(os.getenv("ADVANCED_ANALYSIS_ENABLED"), True)

        app_token = _strip(os.getenv("APP_TOKEN")) or _strip(os.getenv("TFB_APP_TOKEN")) or None
        backup_app_token = _strip(os.getenv("BACKUP_APP_TOKEN")) or None
        require_auth = _coerce_bool(os.getenv("REQUIRE_AUTH"), False)

        enabled_providers = _as_list_lower(os.getenv("ENABLED_PROVIDERS") or os.getenv("PROVIDERS") or "")
        if not enabled_providers:
            enabled_providers = ["eodhd", "finnhub"]

        ksa_providers = _as_list_lower(os.getenv("KSA_PROVIDERS") or "")
        if not ksa_providers:
            ksa_providers = ["yahoo_chart", "tadawul", "argaam"]

        primary_provider = (_strip(os.getenv("PRIMARY_PROVIDER")) or (enabled_providers[0] if enabled_providers else "finnhub")).lower()
        ksa_disallow_eodhd = _coerce_bool(os.getenv("KSA_DISALLOW_EODHD"), True)

        # HTTP/caching (support legacy aliases)
        http_timeout_sec = _coerce_float(os.getenv("HTTP_TIMEOUT_SEC") or os.getenv("HTTP_TIMEOUT"), 25.0)
        http_timeout_sec = max(5.0, min(180.0, float(http_timeout_sec or 25.0)))

        cache_ttl_sec = _coerce_float(os.getenv("CACHE_TTL_SEC") or os.getenv("CACHE_DEFAULT_TTL"), 20.0)
        cache_ttl_sec = max(1.0, float(cache_ttl_sec or 20.0))

        engine_cache_ttl_sec = _coerce_int(os.getenv("ENGINE_CACHE_TTL_SEC") or os.getenv("ENGINE_TTL_SEC"), int(cache_ttl_sec))
        engine_cache_ttl_sec = max(1, min(3600, engine_cache_ttl_sec))

        # AI batching (support AI_* aliases in render.yaml)
        ai_batch_size = _coerce_int(os.getenv("AI_BATCH_SIZE"), _coerce_int(os.getenv("AI_BATCH"), 25))
        ai_batch_timeout_sec = _coerce_float(os.getenv("AI_BATCH_TIMEOUT_SEC"), _coerce_float(os.getenv("AI_TIMEOUT_SEC"), 45.0))
        ai_batch_concurrency = _coerce_int(os.getenv("AI_BATCH_CONCURRENCY"), _coerce_int(os.getenv("AI_CONCURRENCY"), 6))
        ai_max_tickers = _coerce_int(os.getenv("AI_MAX_TICKERS"), 800)

        # ADV batching (support ADV_* aliases in render.yaml)
        adv_batch_size = _coerce_int(os.getenv("ADV_BATCH_SIZE"), 25)
        adv_batch_timeout_sec = _coerce_float(os.getenv("ADV_BATCH_TIMEOUT_SEC"), _coerce_float(os.getenv("ADV_TIMEOUT_SEC"), 45.0))
        adv_batch_concurrency = _coerce_int(os.getenv("ADV_BATCH_CONCURRENCY"), _coerce_int(os.getenv("ADV_CONCURRENCY"), 6))
        adv_max_tickers = _coerce_int(os.getenv("ADV_MAX_TICKERS"), 800)

        # ENRICHED batching
        enriched_batch_size = _coerce_int(os.getenv("ENRICHED_BATCH_SIZE"), 40)
        enriched_timeout_sec = _coerce_float(os.getenv("ENRICHED_TIMEOUT_SEC"), 45.0)
        enriched_batch_concurrency = _coerce_int(os.getenv("ENRICHED_BATCH_CONCURRENCY") or os.getenv("ENRICHED_CONCURRENCY"), 5)
        enriched_max_tickers = _coerce_int(os.getenv("ENRICHED_MAX_TICKERS"), 250)

        # guardrails
        ai_batch_size = max(5, min(250, ai_batch_size))
        ai_batch_timeout_sec = max(5.0, min(180.0, ai_batch_timeout_sec))
        ai_batch_concurrency = max(1, min(30, ai_batch_concurrency))
        ai_max_tickers = max(10, min(3000, ai_max_tickers))

        adv_batch_size = max(5, min(250, adv_batch_size))
        adv_batch_timeout_sec = max(5.0, min(180.0, adv_batch_timeout_sec))
        adv_batch_concurrency = max(1, min(30, adv_batch_concurrency))
        adv_max_tickers = max(10, min(3000, adv_max_tickers))

        enriched_batch_size = max(5, min(300, enriched_batch_size))
        enriched_timeout_sec = max(5.0, min(180.0, enriched_timeout_sec))
        enriched_batch_concurrency = max(1, min(30, enriched_batch_concurrency))
        enriched_max_tickers = max(10, min(5000, enriched_max_tickers))

        enable_cors_all_origins = _coerce_bool(os.getenv("ENABLE_CORS_ALL_ORIGINS") or os.getenv("CORS_ALL_ORIGINS"), True)
        cors_origins = _split_csv(os.getenv("CORS_ORIGINS"))
        if not cors_origins:
            cors_origins = ["*"] if enable_cors_all_origins else []

        default_spreadsheet_id = _strip(os.getenv("DEFAULT_SPREADSHEET_ID")) or None
        tfb_spreadsheet_id = _strip(os.getenv("TFB_SPREADSHEET_ID")) or None

        gsc_raw = _strip(os.getenv("GOOGLE_SHEETS_CREDENTIALS"))
        google_sheets_credentials = _maybe_decode_b64_json(gsc_raw) if gsc_raw else None
        google_application_credentials = _strip(os.getenv("GOOGLE_APPLICATION_CREDENTIALS")) or None

        google_apps_script_url = _strip(os.getenv("GOOGLE_APPS_SCRIPT_URL")) or None
        google_apps_script_backup_url = _strip(os.getenv("GOOGLE_APPS_SCRIPT_BACKUP_URL")) or None

        tadawul_quote_url = _strip(os.getenv("TADAWUL_QUOTE_URL")) or None
        tadawul_fundamentals_url = _strip(os.getenv("TADAWUL_FUNDAMENTALS_URL")) or None
        argaam_base_url = _strip(os.getenv("ARGAAM_BASE_URL")) or None

        timezone_default = _strip(os.getenv("TIMEZONE_DEFAULT") or os.getenv("TZ") or "Asia/Riyadh")

        return Settings(
            service_name=service_name,
            environment=environment,
            log_level=log_level,
            app_version=app_version,
            defer_router_mount=defer_router_mount,
            init_engine_on_boot=init_engine_on_boot,
            ai_analysis_enabled=ai_analysis_enabled,
            advanced_analysis_enabled=advanced_analysis_enabled,
            app_token=app_token,
            backup_app_token=backup_app_token,
            require_auth=require_auth,
            enabled_providers=enabled_providers,
            ksa_providers=ksa_providers,
            primary_provider=primary_provider,
            ksa_disallow_eodhd=ksa_disallow_eodhd,
            http_timeout_sec=http_timeout_sec,
            cache_ttl_sec=cache_ttl_sec,
            engine_cache_ttl_sec=engine_cache_ttl_sec,
            ai_batch_size=ai_batch_size,
            ai_batch_timeout_sec=ai_batch_timeout_sec,
            ai_batch_concurrency=ai_batch_concurrency,
            ai_max_tickers=ai_max_tickers,
            adv_batch_size=adv_batch_size,
            adv_batch_timeout_sec=adv_batch_timeout_sec,
            adv_batch_concurrency=adv_batch_concurrency,
            adv_max_tickers=adv_max_tickers,
            enriched_batch_size=enriched_batch_size,
            enriched_timeout_sec=enriched_timeout_sec,
            enriched_batch_concurrency=enriched_batch_concurrency,
            enriched_max_tickers=enriched_max_tickers,
            enable_cors_all_origins=enable_cors_all_origins,
            cors_origins=cors_origins,
            default_spreadsheet_id=default_spreadsheet_id,
            tfb_spreadsheet_id=tfb_spreadsheet_id,
            google_sheets_credentials=google_sheets_credentials,
            google_application_credentials=google_application_credentials,
            google_apps_script_url=google_apps_script_url,
            google_apps_script_backup_url=google_apps_script_backup_url,
            tadawul_quote_url=tadawul_quote_url,
            tadawul_fundamentals_url=tadawul_fundamentals_url,
            argaam_base_url=argaam_base_url,
            timezone_default=timezone_default,
        )

    def as_safe_dict(self) -> Dict[str, Any]:
        return mask_settings_dict()


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    return Settings.from_env()


def allowed_tokens() -> List[str]:
    s = get_settings()
    toks: List[str] = []
    if s.app_token:
        toks.append(s.app_token.strip())
    if s.backup_app_token:
        toks.append(s.backup_app_token.strip())

    out: List[str] = []
    seen = set()
    for t in toks:
        if t and t not in seen:
            seen.add(t)
            out.append(t)
    return out


def is_open_mode() -> bool:
    # Policy: no tokens => open, regardless of REQUIRE_AUTH flag
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
