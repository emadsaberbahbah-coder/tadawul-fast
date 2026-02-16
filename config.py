"""
TADAWUL FAST BRIDGE – MAIN CONFIG (v3.4.0) – PROD SAFE
===========================================================
Advanced Production Edition (Enhanced)

Goals
- Ultra-fast boot (stdlib only; no FastAPI/Pydantic imports at top-level)
- Secure-by-default "Open Mode" policy with explicit override controls
- Smart provider discovery + strict ENV coercion
- Render-friendly (ephemeral envs + external URL awareness)
- Boot validation guard (URLs, secrets, required integration fields)

Notes
- This module should remain import-safe in any environment (CI/CD included).
- Never raise on import. Validation errors are exposed via boot_diagnostics().
"""

from __future__ import annotations

import base64
import json
import os
import re
from dataclasses import asdict, dataclass, field
from functools import lru_cache
from typing import Any, Dict, List, Optional, Tuple

CONFIG_VERSION = "3.4.0"

_TRUTHY = {"1", "true", "yes", "y", "on", "t", "enable", "enabled", "ok"}
_FALSY = {"0", "false", "no", "n", "off", "f", "disable", "disabled"}

_URL_RE = re.compile(r"^https?://", re.IGNORECASE)
_HOSTPORT_RE = re.compile(r"^[a-z0-9.\-]+(:\d+)?$", re.IGNORECASE)

# -----------------------------
# Coercion / Parsing Helpers
# -----------------------------
def _strip(v: Any) -> str:
    try:
        return str(v).strip()
    except Exception:
        return ""


def _coerce_int(v: Any, default: int, *, lo: Optional[int] = None, hi: Optional[int] = None) -> int:
    try:
        x = int(float(_strip(v)))
    except Exception:
        x = default
    if lo is not None and x < lo:
        x = lo
    if hi is not None and x > hi:
        x = hi
    return x


def _coerce_float(v: Any, default: float, *, lo: Optional[float] = None, hi: Optional[float] = None) -> float:
    try:
        x = float(_strip(v))
    except Exception:
        x = default
    if lo is not None and x < lo:
        x = lo
    if hi is not None and x > hi:
        x = hi
    return x


def _coerce_bool(v: Any, default: bool) -> bool:
    s = _strip(v).lower()
    if not s:
        return default
    if s in _TRUTHY:
        return True
    if s in _FALSY:
        return False
    return default


def _safe_json_loads(s: str) -> Any:
    try:
        return json.loads(s)
    except Exception:
        return None


def _as_list_lower(v: Any) -> List[str]:
    """
    Handles:
    - CSV: "eodhd,finnhub"
    - JSON array: ["eodhd","finnhub"]
    - Python-ish list: ['eodhd','finnhub'] (best effort)
    """
    if not v:
        return []
    s = _strip(v)
    if not s:
        return []

    if s[0] in ("[", "("):
        # Convert single quotes to double quotes if present (best effort)
        s2 = s.replace("'", '"')
        obj = _safe_json_loads(s2)
        if isinstance(obj, list):
            out = []
            for x in obj:
                t = _strip(x).lower()
                if t:
                    out.append(t)
            return out

    return [p.strip().lower() for p in s.split(",") if p.strip()]


def _as_list(v: Any) -> List[str]:
    if not v:
        return []
    s = _strip(v)
    if not s:
        return []
    if s[0] in ("[", "("):
        s2 = s.replace("'", '"')
        obj = _safe_json_loads(s2)
        if isinstance(obj, list):
            return [str(x).strip() for x in obj if str(x).strip()]
    return [p.strip() for p in s.split(",") if p.strip()]


# -----------------------------
# Secret Handling
# -----------------------------
def _mask_secret(s: Optional[str]) -> Optional[str]:
    if not s:
        return None
    s = _strip(s)
    if len(s) <= 8:
        return "***"
    return f"{s[:3]}...{s[-3:]}"


def _looks_like_jwt(s: str) -> bool:
    # loose heuristic: header.payload.signature (base64url-ish)
    if not s:
        return False
    return s.count(".") == 2 and len(s) > 30


def _secret_strength_hint(s: str) -> Optional[str]:
    """
    Non-blocking hints (never prevent boot).
    """
    t = _strip(s)
    if not t:
        return None
    if _looks_like_jwt(t):
        return None
    if len(t) < 16:
        return "Token is short (<16 chars). Consider using a longer random secret."
    if t.lower() in {"token", "password", "secret", "1234", "123456", "admin"}:
        return "Token looks weak/common. Use a strong random secret."
    return None


# -----------------------------
# Credentials / JSON Handling
# -----------------------------
def _repair_private_key(key: str) -> str:
    # Fix common escaped newline issue from ENV vars
    if "\\n" in key:
        return key.replace("\\n", "\n")
    return key


def _validate_json_creds(raw: str) -> Optional[str]:
    """
    Validates and decodes potential Base64 or raw JSON credentials.
    Returns normalized JSON string if valid-ish, else best-effort raw.
    """
    t = _strip(raw)
    if not t:
        return None

    # Handle wrapping quotes common in shell exports
    if (t.startswith('"') and t.endswith('"')) or (t.startswith("'") and t.endswith("'")):
        t = t[1:-1].strip()

    # Base64 decode attempt (best effort)
    if not t.startswith("{") and len(t) > 50:
        try:
            decoded = base64.b64decode(t, validate=False).decode("utf-8", errors="replace").strip()
            if decoded.startswith("{"):
                t = decoded
        except Exception:
            pass

    obj = _safe_json_loads(t)
    if isinstance(obj, dict):
        pk = obj.get("private_key")
        if isinstance(pk, str) and pk:
            obj["private_key"] = _repair_private_key(pk)
        # Keep as compact JSON string
        try:
            return json.dumps(obj, separators=(",", ":"))
        except Exception:
            return t

    # Not parseable JSON: return as-is (some deployments store a path)
    return t


# -----------------------------
# URL / Host Validation
# -----------------------------
def _normalize_base_url(url: str) -> str:
    u = _strip(url).rstrip("/")
    return u


def _is_valid_http_url(url: str) -> bool:
    u = _strip(url)
    if not u:
        return False
    return bool(_URL_RE.match(u))


def _is_valid_hostport(v: str) -> bool:
    s = _strip(v)
    if not s:
        return False
    return bool(_HOSTPORT_RE.match(s))


# -----------------------------
# Settings Dataclass
# -----------------------------
@dataclass(frozen=True)
class Settings:
    # --- Identity & Environment ---
    service_name: str = "Tadawul Fast Bridge"
    environment: str = "production"  # production | staging | dev | local
    app_version: str = "dev"
    log_level: str = "INFO"
    timezone: str = "Asia/Riyadh"

    # --- Render / Deployment Awareness ---
    render_service_name: Optional[str] = None
    render_external_url: Optional[str] = None
    public_base_url: Optional[str] = None  # computed (best effort)

    # --- Feature Flags ---
    ai_analysis_enabled: bool = True
    advisor_enabled: bool = True
    advanced_enabled: bool = True
    rate_limit_enabled: bool = True

    # --- Rate Limits ---
    max_rpm: int = 240

    # --- Authentication / Security ---
    auth_header_name: str = "X-APP-TOKEN"
    app_token: Optional[str] = None
    backup_app_token: Optional[str] = None
    allow_query_token: bool = False
    open_mode: bool = False  # resolved in from_env

    # --- Providers ---
    enabled_providers: List[str] = field(default_factory=lambda: ["eodhd", "finnhub"])
    ksa_providers: List[str] = field(default_factory=lambda: ["yahoo_chart", "argaam"])
    primary_provider: str = "eodhd"

    # Optional provider URLs / keys (stored masked in mask_settings())
    backend_base_url: str = "http://127.0.0.1:8000"
    argaam_quote_url: Optional[str] = None

    # --- Network & Performance ---
    http_timeout_sec: float = 45.0
    cache_ttl_sec: int = 20
    batch_concurrency: int = 5

    # Batch sizes
    ai_batch_size: int = 20
    adv_batch_size: int = 25
    quote_batch_size: int = 50

    # --- Integrations ---
    spreadsheet_id: Optional[str] = None
    google_creds: Optional[str] = None  # normalized JSON string (or path)

    # --- Diagnostics ---
    boot_warnings: Tuple[str, ...] = ()
    boot_errors: Tuple[str, ...] = ()

    # -------------------------
    # Builders
    # -------------------------
    @staticmethod
    def from_env() -> "Settings":
        env_name = _strip(os.getenv("APP_ENV") or os.getenv("ENVIRONMENT") or "production").lower()

        # Tokens
        token1 = _strip(os.getenv("APP_TOKEN") or os.getenv("TFB_APP_TOKEN"))
        token2 = _strip(os.getenv("BACKUP_APP_TOKEN"))

        # Query-token support (must be explicit)
        allow_qs = _coerce_bool(os.getenv("ALLOW_QUERY_TOKEN"), False)

        tokens_exist = bool(token1 or token2)

        # Open mode policy:
        # - Default: OPEN only when no tokens exist.
        # - If OPEN_MODE is set: respect it, but if tokens exist and OPEN_MODE=true,
        #   we add a boot warning (to prevent accidental exposure).
        open_override = os.getenv("OPEN_MODE")
        if open_override is not None:
            is_open = _coerce_bool(open_override, not tokens_exist)
        else:
            is_open = not tokens_exist

        # Dynamic timeouts by environment (overrideable)
        is_prod = env_name in {"production", "prod"}
        base_timeout = 55.0 if is_prod else 30.0
        http_timeout = _coerce_float(os.getenv("HTTP_TIMEOUT_SEC"), base_timeout, lo=5.0, hi=180.0)

        # Providers: explicit list OR auto-discovery
        providers_env = _as_list_lower(os.getenv("ENABLED_PROVIDERS") or os.getenv("PROVIDERS"))
        if not providers_env:
            auto: List[str] = []
            if _strip(os.getenv("EODHD_API_TOKEN")):
                auto.append("eodhd")
            if _strip(os.getenv("FINNHUB_API_KEY")):
                auto.append("finnhub")
            if _strip(os.getenv("ALPHAVANTAGE_API_KEY")):
                auto.append("alphavantage")
            # fallback default
            providers_env = auto or ["eodhd", "finnhub"]

        ksa_env = _as_list_lower(os.getenv("KSA_PROVIDERS"))
        if not ksa_env:
            # default KSA stack
            ksa_env = ["yahoo_chart", "argaam"]

        # Primary provider: explicit OR first enabled
        primary = _strip(os.getenv("PRIMARY_PROVIDER")).lower()
        if not primary:
            primary = providers_env[0] if providers_env else "eodhd"

        # Backend base URL
        backend = _normalize_base_url(
            os.getenv("BACKEND_BASE_URL")
            or os.getenv("TFB_BASE_URL")
            or "http://127.0.0.1:8000"
        )

        # Render awareness
        render_name = _strip(os.getenv("RENDER_SERVICE_NAME")) or None
        render_external = _normalize_base_url(_strip(os.getenv("RENDER_EXTERNAL_URL"))) or None
        public_base = None
        if render_external and _is_valid_http_url(render_external):
            public_base = render_external
        else:
            # best-effort: if backend_base_url looks public
            if _is_valid_http_url(backend):
                public_base = backend

        # Integrations
        spreadsheet_id = _strip(
            os.getenv("DEFAULT_SPREADSHEET_ID")
            or os.getenv("SPREADSHEET_ID")
            or os.getenv("TFB_SPREADSHEET_ID")
        ) or None

        google_creds = _validate_json_creds(
            os.getenv("GOOGLE_SHEETS_CREDENTIALS") or os.getenv("GOOGLE_CREDENTIALS") or ""
        )

        # Optional Argaam URL
        argaam_url = _strip(os.getenv("ARGAAM_QUOTE_URL")) or None
        if argaam_url:
            argaam_url = _normalize_base_url(argaam_url)

        s = Settings(
            service_name=_strip(os.getenv("APP_NAME") or "Tadawul Fast Bridge"),
            environment=env_name,
            app_version=_strip(os.getenv("APP_VERSION") or CONFIG_VERSION),
            log_level=_strip(os.getenv("LOG_LEVEL") or "INFO").upper(),
            timezone=_strip(os.getenv("APP_TIMEZONE") or "Asia/Riyadh") or "Asia/Riyadh",
            render_service_name=render_name,
            render_external_url=render_external,
            public_base_url=public_base,
            ai_analysis_enabled=_coerce_bool(os.getenv("AI_ANALYSIS_ENABLED"), True),
            advisor_enabled=_coerce_bool(os.getenv("ADVISOR_ENABLED"), True),
            advanced_enabled=_coerce_bool(os.getenv("ADVANCED_ENABLED"), True),
            rate_limit_enabled=_coerce_bool(os.getenv("ENABLE_RATE_LIMITING"), True),
            max_rpm=_coerce_int(os.getenv("MAX_REQUESTS_PER_MINUTE"), 240, lo=30, hi=5000),
            auth_header_name=_strip(os.getenv("AUTH_HEADER_NAME") or "X-APP-TOKEN"),
            app_token=token1 or None,
            backup_app_token=token2 or None,
            allow_query_token=allow_qs,
            open_mode=is_open,
            enabled_providers=providers_env,
            ksa_providers=ksa_env,
            primary_provider=primary,
            backend_base_url=backend,
            argaam_quote_url=argaam_url,
            http_timeout_sec=http_timeout,
            cache_ttl_sec=_coerce_int(os.getenv("ENGINE_CACHE_TTL_SEC"), 20, lo=5, hi=3600),
            batch_concurrency=_coerce_int(os.getenv("BATCH_CONCURRENCY"), 5, lo=1, hi=50),
            ai_batch_size=_coerce_int(os.getenv("AI_BATCH_SIZE"), 20, lo=1, hi=200),
            adv_batch_size=_coerce_int(os.getenv("ADV_BATCH_SIZE"), 25, lo=1, hi=300),
            quote_batch_size=_coerce_int(os.getenv("QUOTE_BATCH_SIZE"), 50, lo=1, hi=1000),
            spreadsheet_id=spreadsheet_id,
            google_creds=google_creds,
        )

        # attach diagnostics (never raises)
        errs, warns = _validate_settings(s)
        return Settings(
            **{**asdict(s), "boot_errors": tuple(errs), "boot_warnings": tuple(warns)}
        )

    def mask_settings(self) -> Dict[str, Any]:
        """
        Safe for public health endpoints.
        """
        d = asdict(self)
        d["app_token"] = _mask_secret(self.app_token)
        d["backup_app_token"] = _mask_secret(self.backup_app_token)
        d["google_creds"] = "PRESENT" if self.google_creds else "MISSING"
        # Keep URLs but avoid exposing internal-only endpoints if desired
        return d


# -----------------------------
# Validation Guard
# -----------------------------
def _validate_settings(s: Settings) -> Tuple[List[str], List[str]]:
    errors: List[str] = []
    warnings: List[str] = []

    # Backend URL sanity
    if s.backend_base_url and not _is_valid_http_url(s.backend_base_url):
        errors.append(f"BACKEND_BASE_URL invalid: {s.backend_base_url!r} (must start with http:// or https://)")

    # Public base URL (optional)
    if s.public_base_url and not _is_valid_http_url(s.public_base_url):
        warnings.append(f"public_base_url not a valid http(s) URL: {s.public_base_url!r}")

    # Providers sanity
    if not s.enabled_providers:
        warnings.append("enabled_providers is empty; default routing may be degraded.")
    if s.primary_provider and s.enabled_providers and s.primary_provider not in s.enabled_providers:
        warnings.append(
            f"PRIMARY_PROVIDER={s.primary_provider!r} not in enabled_providers={s.enabled_providers!r} "
            "(will still run, but routing may be inconsistent)."
        )

    # Open mode safety warnings
    if s.open_mode and (s.app_token or s.backup_app_token):
        warnings.append("OPEN_MODE is enabled while tokens exist. This can expose endpoints publicly.")

    # Token hints (non-blocking)
    for name, tok in (("APP_TOKEN", s.app_token), ("BACKUP_APP_TOKEN", s.backup_app_token)):
        if tok:
            hint = _secret_strength_hint(tok)
            if hint:
                warnings.append(f"{name}: {hint}")

    # Google creds sanity (non-blocking)
    if s.google_creds:
        # if it's JSON, ensure it's dict-ish
        obj = _safe_json_loads(s.google_creds) if isinstance(s.google_creds, str) else None
        if isinstance(obj, dict):
            for k in ("client_email", "private_key", "project_id"):
                if k not in obj:
                    warnings.append(f"GOOGLE_SHEETS_CREDENTIALS missing field: {k}")
        else:
            # could be a file path; not necessarily an error
            pass
    else:
        warnings.append("Google Sheets credentials missing (GOOGLE_SHEETS_CREDENTIALS/GOOGLE_CREDENTIALS). Sheets features may fail.")

    # Argaam URL sanity (optional)
    if s.argaam_quote_url and not _is_valid_http_url(s.argaam_quote_url):
        warnings.append(f"ARGAAM_QUOTE_URL invalid: {s.argaam_quote_url!r}")

    # Spreadsheet id presence (optional but recommended)
    if not s.spreadsheet_id:
        warnings.append("Spreadsheet ID missing (DEFAULT_SPREADSHEET_ID / SPREADSHEET_ID). Scripts may require it.")

    return errors, warnings


# -----------------------------
# Public Accessors (Singleton)
# -----------------------------
@lru_cache(maxsize=1)
def get_settings() -> Settings:
    return Settings.from_env()


def boot_diagnostics() -> Dict[str, Any]:
    """
    Returns validation outcomes. Never raises.
    Useful for /health or /ready endpoints.
    """
    s = get_settings()
    return {
        "status": "ok" if not s.boot_errors else "error",
        "config_version": CONFIG_VERSION,
        "environment": s.environment,
        "open_mode": s.open_mode,
        "errors": list(s.boot_errors),
        "warnings": list(s.boot_warnings),
        "masked": s.mask_settings(),
    }


def allowed_tokens() -> List[str]:
    s = get_settings()
    out: List[str] = []
    for t in (s.app_token, s.backup_app_token):
        if t and t not in out:
            out.append(t)
    return out


def is_open_mode() -> bool:
    return get_settings().open_mode


def auth_ok(token: Optional[str]) -> bool:
    """
    Central auth policy:
    - If open mode => always OK
    - Else token must match one of allowed tokens
    """
    if is_open_mode():
        return True
    if not token:
        return False
    t = _strip(token)
    return bool(t and t in allowed_tokens())


def mask_settings_dict() -> Dict[str, Any]:
    return get_settings().mask_settings()


__all__ = [
    "CONFIG_VERSION",
    "Settings",
    "get_settings",
    "boot_diagnostics",
    "allowed_tokens",
    "is_open_mode",
    "auth_ok",
    "mask_settings_dict",
]
