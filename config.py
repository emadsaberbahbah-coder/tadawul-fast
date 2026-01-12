# config.py  (FULL REPLACEMENT) — v3.0.1
"""
TADAWUL FAST BRIDGE – MAIN CONFIG (v3.0.1) – PROD SAFE / NO HEAVY IMPORTS

✅ Goals
- Never raises at import-time; safe at startup
- Default ENV name = "production"
- Default KSA providers = ["yahoo_chart"] (Option A)
- Google Sheets credentials: supports JSON or base64(JSON) + common alias keys
- Optional rate-limit tunables (ENABLE_RATE_LIMITING / MAX_REQUESTS_PER_MINUTE)
- Back-compat helpers: allowed_tokens(), is_open_mode(), auth_ok(), mask_settings_dict()

✅ v3.0.1 improvements (vs v3.0.0)
- Stronger base64(JSON) validation (rejects obvious non-JSON decodes)
- Cleaner ENV parsing + clamps (never negative)
- Adds token header customization: AUTH_HEADER_NAME (default: X-APP-TOKEN)
- Adds optional open-mode override: OPEN_MODE (default: auto by tokens)
  (Policy still: no tokens => open, unless OPEN_MODE explicitly set false AND tokens exist)
"""

from __future__ import annotations

import base64
import json
import os
from dataclasses import asdict, dataclass, field
from functools import lru_cache
from typing import Any, Dict, List, Optional

CONFIG_VERSION = "3.0.1"

_TRUTHY = {
    "1",
    "true",
    "yes",
    "y",
    "on",
    "t",
    "enable",
    "enabled",
    "ok",
}
_FALSY = {
    "0",
    "false",
    "no",
    "n",
    "off",
    "f",
    "disable",
    "disabled",
}


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
      - CSV: "a,b,c"
      - JSON list: '["a","b"]'
      - Python-ish list: "['a','b']" (best-effort)
    Returns: deduped lowercase list
    """
    if v is None:
        return []

    raw: List[str] = []
    if isinstance(v, list):
        raw = [str(x).strip().lower() for x in v if _strip(x)]
    else:
        s = _strip(v)
        if not s:
            return []
        if (s.startswith("[") and s.endswith("]")) or (s.startswith("(") and s.endswith(")")):
            # Try JSON first
            try:
                arr = json.loads(s)
                if isinstance(arr, list):
                    raw = [str(x).strip().lower() for x in arr if _strip(x)]
                else:
                    raw = []
            except Exception:
                # Try to normalize python-ish list to JSON list
                try:
                    ss = s.strip()
                    if ss.startswith("(") and ss.endswith(")"):
                        ss = "[" + ss[1:-1] + "]"
                    ss = ss.replace("'", '"')
                    arr = json.loads(ss)
                    if isinstance(arr, list):
                        raw = [str(x).strip().lower() for x in arr if _strip(x)]
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


def _looks_like_json_object(s: str) -> bool:
    ss = (s or "").strip()
    return ss.startswith("{") and ss.endswith("}") and len(ss) >= 2


def _is_probably_base64(s: str) -> bool:
    raw = (s or "").strip()
    if len(raw) < 40:
        return False
    # base64-ish chars; allow newlines
    for c in raw:
        if c.isalnum() or c in "+/=\n\r":
            continue
        return False
    return True


def _maybe_decode_b64_json(s: str) -> Optional[str]:
    """
    - If value is JSON -> return it
    - If value is base64(JSON) -> decode+return JSON
    - Else return original string
    Never raises.
    """
    raw = (s or "").strip()
    if not raw:
        return None

    # Plain JSON?
    if _looks_like_json_object(raw):
        try:
            json.loads(raw)
            return raw
        except Exception:
            return raw

    # Base64(JSON) best-effort (defensive)
    if not _is_probably_base64(raw):
        return raw

    try:
        decoded = base64.b64decode(raw.encode("utf-8"), validate=False).decode("utf-8", errors="replace").strip()
        if _looks_like_json_object(decoded):
            obj = json.loads(decoded)
            # sanity check: looks like service-account / credentials JSON
            if isinstance(obj, dict) and (obj.get("client_email") or obj.get("type") or obj.get("private_key")):
                return decoded
            # Still valid JSON object; keep it (some setups omit fields)
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
    service_name: str = "Tadawul Fast Bridge"
    environment: str = "production"
    log_level: str = "INFO"
    app_version: str = ""

    # ------------------------------------------------------------------
    # Fast boot controls (main.py reads these if present)
    # ------------------------------------------------------------------
    defer_router_mount: bool = True
    init_engine_on_boot: bool = True

    # Feature flags
    ai_analysis_enabled: bool = True
    advanced_analysis_enabled: bool = True

    # Rate limiting (optional)
    enable_rate_limiting: bool = True
    max_requests_per_minute: int = 240

    # ------------------------------------------------------------------
    # Auth
    # ------------------------------------------------------------------
    auth_header_name: str = "X-APP-TOKEN"
    app_token: Optional[str] = None
    backup_app_token: Optional[str] = None
    require_auth: bool = False  # NOTE: policy still OPEN if no tokens exist
    open_mode_override: Optional[bool] = None  # from OPEN_MODE if explicitly set

    # Derived flags (computed in from_env)
    auth_enabled: bool = False  # true when tokens exist
    auth_effective_required: bool = False  # true when tokens exist OR require_auth=true (still open if no tokens)

    # ------------------------------------------------------------------
    # Providers policy
    # ------------------------------------------------------------------
    enabled_providers: List[str] = field(default_factory=lambda: ["eodhd", "finnhub"])
    ksa_providers: List[str] = field(default_factory=lambda: ["yahoo_chart"])  # Option A default
    primary_provider: str = "eodhd"
    ksa_disallow_eodhd: bool = True

    # ------------------------------------------------------------------
    # HTTP + caching
    # ------------------------------------------------------------------
    http_timeout_sec: float = 25.0
    cache_ttl_sec: float = 20.0
    engine_cache_ttl_sec: int = 20

    # ------------------------------------------------------------------
    # Batch controls (routes + engine)
    # ------------------------------------------------------------------
    ai_batch_size: int = 20
    ai_batch_timeout_sec: float = 45.0
    ai_batch_concurrency: int = 5
    ai_max_tickers: int = 500

    adv_batch_size: int = 25
    adv_batch_timeout_sec: float = 45.0
    adv_batch_concurrency: int = 6
    adv_max_tickers: int = 500

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
    google_application_credentials: Optional[str] = None  # file path

    # Apps Script (optional)
    google_apps_script_url: Optional[str] = None
    google_apps_script_backup_url: Optional[str] = None

    # ------------------------------------------------------------------
    # Optional direct provider URLs
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
        service_name = _strip(os.getenv("SERVICE_NAME") or os.getenv("APP_NAME") or os.getenv("APP_TITLE") or "Tadawul Fast Bridge")
        environment = _strip(os.getenv("ENVIRONMENT") or os.getenv("APP_ENV") or os.getenv("ENV") or "production").lower()
        log_level = _strip(os.getenv("LOG_LEVEL") or "INFO").upper()

        app_version = (_env_first("APP_VERSION", "SERVICE_VERSION", "VERSION", "RELEASE") or "").strip()

        defer_router_mount = _coerce_bool(os.getenv("DEFER_ROUTER_MOUNT"), True)
        init_engine_on_boot = _coerce_bool(os.getenv("INIT_ENGINE_ON_BOOT"), True)

        ai_analysis_enabled = _coerce_bool(os.getenv("AI_ANALYSIS_ENABLED"), True)
        advanced_analysis_enabled = _coerce_bool(os.getenv("ADVANCED_ANALYSIS_ENABLED"), True)

        enable_rate_limiting = _coerce_bool(os.getenv("ENABLE_RATE_LIMITING"), True)
        max_requests_per_minute = _coerce_int(os.getenv("MAX_REQUESTS_PER_MINUTE"), 240)
        max_requests_per_minute = max(10, min(5000, max_requests_per_minute))

        auth_header_name = _strip(os.getenv("AUTH_HEADER_NAME") or os.getenv("TOKEN_HEADER_NAME") or "X-APP-TOKEN") or "X-APP-TOKEN"

        app_token = _strip(os.getenv("APP_TOKEN")) or _strip(os.getenv("TFB_APP_TOKEN")) or None
        backup_app_token = _strip(os.getenv("BACKUP_APP_TOKEN")) or None
        require_auth = _coerce_bool(os.getenv("REQUIRE_AUTH"), False)

        # Optional override knob (rarely needed)
        open_mode_env = os.getenv("OPEN_MODE")
        open_mode_override: Optional[bool] = None
        if open_mode_env is not None and str(open_mode_env).strip() != "":
            open_mode_override = _coerce_bool(open_mode_env, False)

        enabled_providers = _as_list_lower(os.getenv("ENABLED_PROVIDERS") or os.getenv("PROVIDERS") or "")
        if not enabled_providers:
            enabled_providers = ["eodhd", "finnhub"]

        # IMPORTANT: Default KSA = yahoo_chart ONLY (unless env explicitly sets otherwise)
        ksa_providers = _as_list_lower(os.getenv("KSA_PROVIDERS") or "")
        if not ksa_providers:
            ksa_providers = ["yahoo_chart"]

        primary_provider = (_strip(os.getenv("PRIMARY_PROVIDER")) or (enabled_providers[0] if enabled_providers else "eodhd")).lower()
        ksa_disallow_eodhd = _coerce_bool(os.getenv("KSA_DISALLOW_EODHD"), True)

        http_timeout_sec = _coerce_float(os.getenv("HTTP_TIMEOUT_SEC") or os.getenv("HTTP_TIMEOUT"), 25.0)
        http_timeout_sec = max(5.0, min(180.0, float(http_timeout_sec or 25.0)))

        cache_ttl_sec = _coerce_float(os.getenv("CACHE_TTL_SEC") or os.getenv("CACHE_DEFAULT_TTL"), 20.0)
        cache_ttl_sec = max(1.0, float(cache_ttl_sec or 20.0))

        engine_cache_ttl_sec = _coerce_int(os.getenv("ENGINE_CACHE_TTL_SEC") or os.getenv("ENGINE_TTL_SEC"), int(cache_ttl_sec))
        engine_cache_ttl_sec = max(1, min(3600, engine_cache_ttl_sec))

        # AI batching
        ai_batch_size = _coerce_int(os.getenv("AI_BATCH_SIZE"), 20)
        ai_batch_timeout_sec = _coerce_float(os.getenv("AI_BATCH_TIMEOUT_SEC") or os.getenv("AI_TIMEOUT_SEC"), 45.0)
        ai_batch_concurrency = _coerce_int(os.getenv("AI_BATCH_CONCURRENCY") or os.getenv("AI_CONCURRENCY"), 5)
        ai_max_tickers = _coerce_int(os.getenv("AI_MAX_TICKERS"), 500)

        # ADV batching
        adv_batch_size = _coerce_int(os.getenv("ADV_BATCH_SIZE"), 25)
        adv_batch_timeout_sec = _coerce_float(os.getenv("ADV_BATCH_TIMEOUT_SEC") or os.getenv("ADV_TIMEOUT_SEC"), 45.0)
        adv_batch_concurrency = _coerce_int(os.getenv("ADV_BATCH_CONCURRENCY") or os.getenv("ADV_CONCURRENCY"), 6)
        adv_max_tickers = _coerce_int(os.getenv("ADV_MAX_TICKERS"), 500)

        # ENRICHED batching
        enriched_batch_size = _coerce_int(os.getenv("ENRICHED_BATCH_SIZE"), 40)
        enriched_timeout_sec = _coerce_float(os.getenv("ENRICHED_TIMEOUT_SEC"), 45.0)
        enriched_batch_concurrency = _coerce_int(os.getenv("ENRICHED_BATCH_CONCURRENCY") or os.getenv("ENRICHED_CONCURRENCY"), 5)
        enriched_max_tickers = _coerce_int(os.getenv("ENRICHED_MAX_TICKERS"), 250)

        # Guardrails
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

        # Credentials: support common aliases
        gsc_raw = _strip(_env_first("GOOGLE_SHEETS_CREDENTIALS", "GOOGLE_CREDENTIALS", "GOOGLE_SERVICE_ACCOUNT_JSON") or "")
        google_sheets_credentials = _maybe_decode_b64_json(gsc_raw) if gsc_raw else None
        google_application_credentials = _strip(os.getenv("GOOGLE_APPLICATION_CREDENTIALS")) or None

        google_apps_script_url = _strip(os.getenv("GOOGLE_APPS_SCRIPT_URL")) or None
        google_apps_script_backup_url = _strip(os.getenv("GOOGLE_APPS_SCRIPT_BACKUP_URL")) or None

        tadawul_quote_url = _strip(os.getenv("TADAWUL_QUOTE_URL")) or None
        tadawul_fundamentals_url = _strip(os.getenv("TADAWUL_FUNDAMENTALS_URL")) or None
        argaam_base_url = _strip(os.getenv("ARGAAM_BASE_URL")) or None

        timezone_default = _strip(os.getenv("TIMEZONE_DEFAULT") or os.getenv("TZ") or "Asia/Riyadh")

        # Derived flags
        auth_enabled = bool((app_token or "").strip() or (backup_app_token or "").strip())
        auth_effective_required = bool(auth_enabled or require_auth)

        return Settings(
            service_name=service_name,
            environment=environment,
            log_level=log_level,
            app_version=app_version,
            defer_router_mount=defer_router_mount,
            init_engine_on_boot=init_engine_on_boot,
            ai_analysis_enabled=ai_analysis_enabled,
            advanced_analysis_enabled=advanced_analysis_enabled,
            enable_rate_limiting=enable_rate_limiting,
            max_requests_per_minute=max_requests_per_minute,
            auth_header_name=auth_header_name,
            app_token=app_token,
            backup_app_token=backup_app_token,
            require_auth=require_auth,
            open_mode_override=open_mode_override,
            auth_enabled=auth_enabled,
            auth_effective_required=auth_effective_required,
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
        d = asdict(self)
        d["app_token"] = _mask_secret(self.app_token)
        d["backup_app_token"] = _mask_secret(self.backup_app_token)
        d["google_sheets_credentials"] = "***present***" if self.google_sheets_credentials else None
        d["google_application_credentials"] = "***present***" if self.google_application_credentials else None
        return d


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
    """
    Policy:
    - No tokens => OPEN
    - Tokens exist => CLOSED unless OPEN_MODE explicitly true
    (This lets you keep old behavior but still force-open if desired.)
    """
    s = get_settings()
    toks = allowed_tokens()
    if not toks:
        return True
    if s.open_mode_override is True:
        return True
    if s.open_mode_override is False:
        return False
    return False


def auth_ok(x_app_token: Optional[str]) -> bool:
    toks = allowed_tokens()
    if not toks:
        return True
    return bool(x_app_token and x_app_token.strip() in toks)


def mask_settings_dict() -> Dict[str, Any]:
    return get_settings().as_safe_dict()


__all__ = [
    "CONFIG_VERSION",
    "Settings",
    "get_settings",
    "allowed_tokens",
    "is_open_mode",
    "auth_ok",
    "mask_settings_dict",
]
