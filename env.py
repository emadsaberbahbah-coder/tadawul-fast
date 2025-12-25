# env.py  (FULL REPLACEMENT)
"""
env.py
------------------------------------------------------------
Backward-compatible environment exports for Tadawul Fast Bridge (v5.0.0)

✅ What this file does:
- Uses SINGLE source of truth from core.config.get_settings() (preferred) or root config.get_settings().
- Exposes backward-compatible constants used across legacy modules.
- Prints a clean boot banner (providers + KSA providers) WITHOUT leaking secrets.
- Provides a settings "view" with computed/aliased attributes expected by legacy modules
  (google_sheets_service, symbols_reader, sync scripts, etc.).
- Robust Google credentials parsing:
    • JSON dict
    • JSON string (including quoted JSON)
    • base64-encoded JSON
    • file path JSON (incl. GOOGLE_APPLICATION_CREDENTIALS)

Design rules:
- Do NOT duplicate business logic here (routing/blocks remain in engines).
- Fallback mode exists ONLY to avoid crashing if config import fails.
- Robust against circular imports: if settings load recurses, fallback is used.

Env aliases supported (common):
- Base URL: TFB_BASE_URL, BACKEND_BASE_URL, BASE_URL, RENDER_EXTERNAL_URL
- Token:    TFB_APP_TOKEN, APP_TOKEN, BACKUP_APP_TOKEN
- Providers: ENABLED_PROVIDERS, PROVIDERS, TFB_PROVIDERS
- KSA providers: KSA_PROVIDERS, TFB_KSA_PROVIDERS
- Sheets creds: GOOGLE_SHEETS_CREDENTIALS, GOOGLE_SA_JSON, GOOGLE_CREDENTIALS,
               GOOGLE_APPLICATION_CREDENTIALS (file path)
- Spreadsheet ID: DEFAULT_SPREADSHEET_ID, SPREADSHEET_ID, GOOGLE_SHEETS_ID
"""

from __future__ import annotations

import base64
import json
import logging
import os
from typing import Any, Dict, List, Optional, Tuple

ENV_VERSION = "5.0.0"
logger = logging.getLogger("env")

_SETTINGS_BASE: Optional[object] = None
settings: Optional[object] = None

_SETTINGS_SOURCE = "unknown"  # core.config | config | fallback | recursion_fallback
_BANNER_PRINTED = False
_WARNED_SETTINGS_IMPORT = False
_LOADING_SETTINGS = False  # protects against circular imports

_TRUTHY = {"1", "true", "yes", "on", "y", "t"}
_FALSY = {"0", "false", "no", "off", "n", "f"}


# -----------------------------------------------------------------------------
# Safe helpers
# -----------------------------------------------------------------------------
def _mask(v: Optional[str]) -> Optional[str]:
    if not v:
        return None
    s = str(v).strip()
    if not s:
        return None
    if len(s) <= 6:
        return "***"
    return s[:3] + "***" + s[-3:]


def _safe_join(items: List[str]) -> str:
    return ",".join([str(x) for x in items if x])


def _safe_bool(v: Any, default: bool = False) -> bool:
    if isinstance(v, bool):
        return v
    if v is None:
        return default
    s = str(v).strip().lower()
    if s in _TRUTHY:
        return True
    if s in _FALSY:
        return False
    return default


def _safe_int(v: Any, default: int) -> int:
    try:
        x = int(str(v).strip())
        return x if x > 0 else default
    except Exception:
        return default


def _safe_float(v: Any, default: float) -> float:
    try:
        x = float(str(v).strip())
        return x if x > 0 else default
    except Exception:
        return default


def _strip_outer_quotes(s: str) -> str:
    x = (s or "").strip()
    if len(x) >= 2 and ((x.startswith('"') and x.endswith('"')) or (x.startswith("'") and x.endswith("'"))):
        return x[1:-1].strip()
    return x


def _canonical_base_url(url: str) -> str:
    u = (url or "").strip()
    if not u:
        return ""
    return u.rstrip("/")


def _get_attr_any(obj: Any, names: List[str], default: Any = None) -> Any:
    for n in names:
        try:
            if hasattr(obj, n):
                v = getattr(obj, n)
                if v is not None and (not isinstance(v, str) or v.strip()):
                    return v
        except Exception:
            pass
    return default


def _normalize_version(v: Any) -> str:
    s = str(v or "").strip()
    if not s:
        return ""
    if s.lower() in {"unknown", "none", "null"}:
        return ""
    return s


def _resolve_version(settings_obj: Any) -> str:
    v = _normalize_version(_get_attr_any(settings_obj, ["version", "app_version", "APP_VERSION"], ""))
    if not v:
        v = _normalize_version(os.getenv("SERVICE_VERSION") or os.getenv("APP_VERSION") or "")
    if not v:
        commit = (os.getenv("RENDER_GIT_COMMIT") or os.getenv("GIT_COMMIT") or "").strip()
        if commit:
            v = commit[:7]
    return v or "dev"


def _as_list_lower(v: Any) -> List[str]:
    """
    Accepts:
      - list/tuple/set
      - comma-separated string
      - JSON list string
    Returns: de-duplicated list (order preserved), lowercased.
    """
    if v is None:
        return []

    if isinstance(v, (tuple, set)):
        v = list(v)

    items: List[str] = []
    if isinstance(v, list):
        items = [str(x).strip().lower() for x in v if str(x).strip()]
    else:
        s = str(v).strip()
        if not s:
            return []
        if s.startswith("[") and s.endswith("]"):
            try:
                arr = json.loads(s)
                if isinstance(arr, list):
                    items = [str(x).strip().lower() for x in arr if str(x).strip()]
            except Exception:
                items = []
        else:
            items = [x.strip().lower() for x in s.split(",") if x.strip()]

    out: List[str] = []
    seen = set()
    for x in items:
        if not x or x in seen:
            continue
        seen.add(x)
        out.append(x)
    return out


def _try_parse_json_dict(raw: Any) -> Optional[Dict[str, Any]]:
    """
    Parse credentials/config JSON from:
      - dict
      - JSON string (optionally quoted)
    """
    if raw is None:
        return None
    if isinstance(raw, dict):
        return raw
    if not isinstance(raw, str):
        return None

    s = _strip_outer_quotes(raw)
    if not s:
        return None

    # Some env stores JSON with escaped newlines; json.loads handles it if valid.
    try:
        obj = json.loads(s)
        return obj if isinstance(obj, dict) else None
    except Exception:
        return None


def _looks_like_base64(s: str) -> bool:
    x = (s or "").strip()
    if len(x) < 40:
        return False
    # rough heuristic: base64 strings are typically A-Z a-z 0-9 + / =
    allowed = set("ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/=\n\r")
    return all((c in allowed) for c in x)


def _try_parse_b64_json_dict(raw: Any) -> Optional[Dict[str, Any]]:
    if raw is None or not isinstance(raw, str):
        return None
    s = _strip_outer_quotes(raw)
    if not s or not _looks_like_base64(s):
        return None
    try:
        decoded = base64.b64decode(s).decode("utf-8", errors="strict")
    except Exception:
        return None
    return _try_parse_json_dict(decoded)


def _try_load_json_file(path: Any) -> Optional[Dict[str, Any]]:
    if path is None:
        return None
    if isinstance(path, dict):
        return path
    p = str(path).strip()
    if not p:
        return None
    # common case: GOOGLE_APPLICATION_CREDENTIALS points to a file
    if not (p.endswith(".json") or os.path.sep in p or p.startswith(".")):
        return None
    try:
        if not os.path.isfile(p):
            return None
        with open(p, "r", encoding="utf-8") as f:
            obj = json.load(f)
        return obj if isinstance(obj, dict) else None
    except Exception:
        return None


def _load_google_creds_dict(base: object) -> Tuple[Optional[Dict[str, Any]], str, str]:
    """
    Returns: (creds_dict, creds_raw_str, source_tag)
    source_tag: base_dict | base_json | base_b64 | env_json | env_b64 | file | missing
    """
    # 1) base dict
    for attr in ["google_credentials_dict", "google_sheets_credentials", "google_credentials"]:
        v = _get_attr_any(base, [attr], None)
        if isinstance(v, dict) and v:
            return v, "", "base_dict"

    # 2) base raw string
    raw_candidates = [
        _get_attr_any(base, ["google_sheets_credentials_raw"], None),
        _get_attr_any(base, ["google_sheets_credentials"], None),
        _get_attr_any(base, ["google_sa_json"], None),
        _get_attr_any(base, ["GOOGLE_SHEETS_CREDENTIALS"], None),
    ]
    for raw in raw_candidates:
        d = _try_parse_json_dict(raw)
        if isinstance(d, dict) and d:
            return d, str(raw) if isinstance(raw, str) else json.dumps(raw), "base_json"
        d = _try_parse_b64_json_dict(raw)
        if isinstance(d, dict) and d:
            return d, str(raw) if isinstance(raw, str) else "", "base_b64"

    # 3) env JSON / B64
    env_raw = (
        os.getenv("GOOGLE_SHEETS_CREDENTIALS")
        or os.getenv("GOOGLE_SA_JSON")
        or os.getenv("GOOGLE_CREDENTIALS")
        or ""
    ).strip()
    if env_raw:
        d = _try_parse_json_dict(env_raw)
        if isinstance(d, dict) and d:
            return d, env_raw, "env_json"
        d = _try_parse_b64_json_dict(env_raw)
        if isinstance(d, dict) and d:
            return d, env_raw, "env_b64"

    # 4) file path (either direct env or GOOGLE_APPLICATION_CREDENTIALS)
    file_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS") or ""
    if file_path:
        d = _try_load_json_file(file_path)
        if isinstance(d, dict) and d:
            return d, file_path, "file"

    return None, env_raw, "missing"


def _maybe_configure_logging() -> None:
    """
    Avoid double-configuring logging if app already set handlers.
    Useful for standalone scripts that import env.py directly.
    """
    try:
        root = logging.getLogger()
        if root.handlers:
            return
        level = (os.getenv("LOG_LEVEL", "INFO") or "INFO").upper()
        logging.basicConfig(
            level=getattr(logging, level, logging.INFO),
            format="%(asctime)s | %(levelname)s | %(message)s",
        )
    except Exception:
        pass


_maybe_configure_logging()


# -----------------------------------------------------------------------------
# Settings loader (single source of truth)
# -----------------------------------------------------------------------------
def _build_fallback_settings() -> object:
    class _Fallback:
        app_name = os.getenv("APP_NAME", "Tadawul Fast Bridge")
        env = os.getenv("APP_ENV") or os.getenv("ENVIRONMENT") or "production"
        version = os.getenv("SERVICE_VERSION") or os.getenv("APP_VERSION") or "dev"
        log_level = (os.getenv("LOG_LEVEL", "INFO") or "INFO").lower()

        enabled_providers = _as_list_lower(os.getenv("TFB_PROVIDERS") or os.getenv("ENABLED_PROVIDERS") or os.getenv("PROVIDERS") or "finnhub,fmp,eodhd,yahoo_chart")
        enabled_ksa_providers = _as_list_lower(os.getenv("TFB_KSA_PROVIDERS") or os.getenv("KSA_PROVIDERS") or "tadawul,argaam,yahoo_chart")
        primary_provider = (os.getenv("PRIMARY_PROVIDER") or "").strip().lower() or (enabled_providers[0] if enabled_providers else "finnhub")

        backend_base_url = _canonical_base_url(
            os.getenv("TFB_BASE_URL")
            or os.getenv("BACKEND_BASE_URL")
            or os.getenv("BASE_URL")
            or os.getenv("RENDER_EXTERNAL_URL")
            or ""
        )

        app_token = (os.getenv("TFB_APP_TOKEN") or os.getenv("APP_TOKEN") or "").strip()
        backup_app_token = (os.getenv("BACKUP_APP_TOKEN") or "").strip()

        require_auth = _safe_bool(os.getenv("REQUIRE_AUTH") or os.getenv("AUTH_REQUIRED"), False)
        http_timeout_sec = _safe_float(os.getenv("HTTP_TIMEOUT_SEC") or os.getenv("HTTP_TIMEOUT"), 25.0)

        default_spreadsheet_id = (os.getenv("DEFAULT_SPREADSHEET_ID") or os.getenv("SPREADSHEET_ID") or os.getenv("GOOGLE_SHEETS_ID") or "").strip()

        google_apps_script_url = (os.getenv("GOOGLE_APPS_SCRIPT_URL") or "").strip()
        google_apps_script_backup_url = (os.getenv("GOOGLE_APPS_SCRIPT_BACKUP_URL") or "").strip()

        sheet_ksa_tadawul = os.getenv("SHEET_KSA_TADAWUL", "KSA_Tadawul_Market")
        sheet_global_markets = os.getenv("SHEET_GLOBAL_MARKETS", "Global_Markets")
        sheet_mutual_funds = os.getenv("SHEET_MUTUAL_FUNDS", "Mutual_Funds")
        sheet_commodities_fx = os.getenv("SHEET_COMMODITIES_FX", "Commodities_FX")
        sheet_market_leaders = os.getenv("SHEET_MARKET_LEADERS", "Market_Leaders")
        sheet_my_portfolio = os.getenv("SHEET_MY_PORTFOLIO", "My_Portfolio")
        sheet_insights_analysis = os.getenv("SHEET_INSIGHTS_ANALYSIS", "Insights_Analysis")
        sheet_investment_advisor = os.getenv("SHEET_INVESTMENT_ADVISOR", "Investment_Advisor")
        sheet_economic_calendar = os.getenv("SHEET_ECONOMIC_CALENDAR", "Economic_Calendar")
        sheet_investment_income = os.getenv("SHEET_INVESTMENT_INCOME", "Investment_Income_Statement")

    return _Fallback()


def _load_settings_base() -> object:
    """
    Load settings from core.config (preferred), then root config.
    If BOTH fail (or circular import detected), use a minimal env-only view.
    """
    global _SETTINGS_BASE, _WARNED_SETTINGS_IMPORT, _LOADING_SETTINGS, _SETTINGS_SOURCE

    if _SETTINGS_BASE is not None:
        return _SETTINGS_BASE

    if _LOADING_SETTINGS:
        _SETTINGS_SOURCE = "recursion_fallback"
        _SETTINGS_BASE = _build_fallback_settings()
        return _SETTINGS_BASE

    _LOADING_SETTINGS = True
    try:
        try:
            from core.config import get_settings  # type: ignore

            _SETTINGS_BASE = get_settings()
            _SETTINGS_SOURCE = "core.config"
            return _SETTINGS_BASE
        except Exception as exc:
            if not _WARNED_SETTINGS_IMPORT:
                _WARNED_SETTINGS_IMPORT = True
                logger.warning("[env] Cannot import core.config.get_settings(): %s", exc)

        try:
            from config import get_settings  # type: ignore

            _SETTINGS_BASE = get_settings()
            _SETTINGS_SOURCE = "config"
            return _SETTINGS_BASE
        except Exception as exc:
            if not _WARNED_SETTINGS_IMPORT:
                _WARNED_SETTINGS_IMPORT = True
                logger.warning("[env] Cannot import config.get_settings(): %s", exc)

        _SETTINGS_BASE = _build_fallback_settings()
        _SETTINGS_SOURCE = "fallback"
        return _SETTINGS_BASE

    finally:
        _LOADING_SETTINGS = False


class _SettingsView:
    """
    Lightweight wrapper around the base settings object.
    Adds computed/aliased attributes without mutating the base object.
    """

    def __init__(self, base: object, extras: Dict[str, Any]):
        self._base = base
        for k, v in extras.items():
            setattr(self, k, v)

    def __getattr__(self, name: str) -> Any:
        return getattr(self._base, name)


def _build_settings_view(base: object) -> _SettingsView:
    # Identity
    app_name = str(_get_attr_any(base, ["app_name", "APP_NAME"], os.getenv("APP_NAME", "Tadawul Fast Bridge")) or "Tadawul Fast Bridge")
    env_name = str(_get_attr_any(base, ["env", "environment", "APP_ENV"], os.getenv("APP_ENV") or os.getenv("ENVIRONMENT") or "production") or "production")
    ver = _resolve_version(base)

    # Base URL (support Render external)
    backend_base_url = _canonical_base_url(
        str(
            _get_attr_any(
                base,
                ["backend_base_url", "BACKEND_BASE_URL", "base_url", "BASE_URL"],
                os.getenv("TFB_BASE_URL") or os.getenv("BACKEND_BASE_URL") or os.getenv("BASE_URL") or os.getenv("RENDER_EXTERNAL_URL") or "",
            )
            or ""
        )
    )

    # Tokens
    app_token = str(_get_attr_any(base, ["app_token", "APP_TOKEN"], os.getenv("TFB_APP_TOKEN") or os.getenv("APP_TOKEN") or "") or "").strip()
    backup_app_token = str(_get_attr_any(base, ["backup_app_token", "BACKUP_APP_TOKEN"], os.getenv("BACKUP_APP_TOKEN") or "") or "").strip()

    require_auth = _safe_bool(_get_attr_any(base, ["require_auth", "REQUIRE_AUTH"], os.getenv("REQUIRE_AUTH") or os.getenv("AUTH_REQUIRED")), False)

    # Providers
    enabled = list(_get_attr_any(base, ["enabled_providers"], []) or [])
    if not enabled:
        enabled = _as_list_lower(os.getenv("TFB_PROVIDERS") or os.getenv("ENABLED_PROVIDERS") or os.getenv("PROVIDERS"))
    enabled_norm = _as_list_lower(enabled)

    ksa = list(_get_attr_any(base, ["enabled_ksa_providers"], []) or [])
    if not ksa:
        ksa = _as_list_lower(os.getenv("TFB_KSA_PROVIDERS") or os.getenv("KSA_PROVIDERS"))
    ksa_norm = _as_list_lower(ksa)

    primary = _get_attr_any(base, ["primary_provider", "PRIMARY_PROVIDER"], os.getenv("PRIMARY_PROVIDER"))
    primary = (str(primary).strip().lower() if primary else "") or (enabled_norm[0] if enabled_norm else "finnhub")

    # Timeouts / TTLs
    http_timeout_sec = float(_get_attr_any(base, ["http_timeout_sec", "HTTP_TIMEOUT_SEC"], os.getenv("HTTP_TIMEOUT_SEC") or os.getenv("HTTP_TIMEOUT") or 25.0))
    http_timeout_sec = max(5.0, float(http_timeout_sec or 25.0))

    cache_ttl_sec = float(_get_attr_any(base, ["cache_ttl_sec", "CACHE_TTL_SEC"], os.getenv("CACHE_TTL_SEC") or 20.0))
    quote_ttl_sec = float(_get_attr_any(base, ["quote_ttl_sec", "QUOTE_TTL_SEC"], os.getenv("QUOTE_TTL_SEC") or 30.0))
    fundamentals_ttl_sec = float(_get_attr_any(base, ["fundamentals_ttl_sec", "FUNDAMENTALS_TTL_SEC"], os.getenv("FUNDAMENTALS_TTL_SEC") or 21600.0))
    argaam_snapshot_ttl_sec = float(_get_attr_any(base, ["argaam_snapshot_ttl_sec", "ARGAAM_SNAPSHOT_TTL_SEC"], os.getenv("ARGAAM_SNAPSHOT_TTL_SEC") or 30.0))

    # Google credentials
    creds_dict, creds_raw, creds_src = _load_google_creds_dict(base)

    # Spreadsheet IDs
    spreadsheet_id = str(
        _get_attr_any(
            base,
            ["spreadsheet_id", "default_spreadsheet_id", "google_sheets_id", "SPREADSHEET_ID", "DEFAULT_SPREADSHEET_ID"],
            os.getenv("SPREADSHEET_ID") or os.getenv("DEFAULT_SPREADSHEET_ID") or os.getenv("GOOGLE_SHEETS_ID") or "",
        )
        or ""
    ).strip()

    default_spreadsheet_id = spreadsheet_id
    has_google_sheets = bool(isinstance(creds_dict, dict) and creds_dict) and bool(default_spreadsheet_id)

    # GAS URLs
    gas_url = str(_get_attr_any(base, ["google_apps_script_url"], os.getenv("GOOGLE_APPS_SCRIPT_URL") or "") or "").strip()
    gas_backup = str(_get_attr_any(base, ["google_apps_script_backup_url"], os.getenv("GOOGLE_APPS_SCRIPT_BACKUP_URL") or "") or "").strip()

    log_level = str(_get_attr_any(base, ["log_level", "LOG_LEVEL"], os.getenv("LOG_LEVEL", "info")) or "info").lower()

    extras: Dict[str, Any] = {
        "app_name": app_name,
        "env": env_name,
        "version": ver,
        "settings_source": _SETTINGS_SOURCE,
        "backend_base_url": backend_base_url,
        "base_url": backend_base_url,  # alias
        "app_token": app_token,
        "backup_app_token": backup_app_token,
        "require_auth": require_auth,
        "enabled_providers": enabled_norm,
        "enabled_ksa_providers": ksa_norm,
        "primary_provider": primary,
        "http_timeout_sec": http_timeout_sec,
        "cache_ttl_sec": float(cache_ttl_sec),
        "quote_ttl_sec": float(quote_ttl_sec),
        "fundamentals_ttl_sec": float(fundamentals_ttl_sec),
        "argaam_snapshot_ttl_sec": float(argaam_snapshot_ttl_sec),
        "google_credentials_dict": creds_dict,
        "google_sheets_credentials_raw": creds_raw,
        "google_creds_source": creds_src,
        "spreadsheet_id": spreadsheet_id,
        "default_spreadsheet_id": default_spreadsheet_id,
        "has_google_sheets": has_google_sheets,
        "google_apps_script_url": gas_url,
        "google_apps_script_backup_url": gas_backup,
        # standard page names
        "sheet_ksa_tadawul": str(_get_attr_any(base, ["sheet_ksa_tadawul"], os.getenv("SHEET_KSA_TADAWUL", "KSA_Tadawul_Market"))),
        "sheet_global_markets": str(_get_attr_any(base, ["sheet_global_markets"], os.getenv("SHEET_GLOBAL_MARKETS", "Global_Markets"))),
        "sheet_mutual_funds": str(_get_attr_any(base, ["sheet_mutual_funds"], os.getenv("SHEET_MUTUAL_FUNDS", "Mutual_Funds"))),
        "sheet_commodities_fx": str(_get_attr_any(base, ["sheet_commodities_fx"], os.getenv("SHEET_COMMODITIES_FX", "Commodities_FX"))),
        "sheet_market_leaders": str(_get_attr_any(base, ["sheet_market_leaders"], os.getenv("SHEET_MARKET_LEADERS", "Market_Leaders"))),
        "sheet_my_portfolio": str(_get_attr_any(base, ["sheet_my_portfolio"], os.getenv("SHEET_MY_PORTFOLIO", "My_Portfolio"))),
        "sheet_insights_analysis": str(_get_attr_any(base, ["sheet_insights_analysis"], os.getenv("SHEET_INSIGHTS_ANALYSIS", "Insights_Analysis"))),
        "sheet_investment_advisor": str(_get_attr_any(base, ["sheet_investment_advisor"], os.getenv("SHEET_INVESTMENT_ADVISOR", "Investment_Advisor"))),
        "sheet_economic_calendar": str(_get_attr_any(base, ["sheet_economic_calendar"], os.getenv("SHEET_ECONOMIC_CALENDAR", "Economic_Calendar"))),
        "sheet_investment_income": str(_get_attr_any(base, ["sheet_investment_income"], os.getenv("SHEET_INVESTMENT_INCOME", "Investment_Income_Statement"))),
        "log_level": log_level,
    }

    return _SettingsView(base, extras)


def _print_banner_once(s: object) -> None:
    global _BANNER_PRINTED
    if _BANNER_PRINTED:
        return

    # Any of these disables the banner
    if _safe_bool(os.getenv("DISABLE_ENV_BANNER") or os.getenv("ENV_BANNER_OFF"), False):
        _BANNER_PRINTED = True
        return

    _BANNER_PRINTED = True
    try:
        app_name = _get_attr_any(s, ["app_name", "APP_NAME"], "Tadawul Fast Bridge")
        env_name = _get_attr_any(s, ["env", "environment", "APP_ENV"], "production")
        ver = _resolve_version(s)

        providers = list(_get_attr_any(s, ["enabled_providers"], []) or [])
        ksa_providers = list(_get_attr_any(s, ["enabled_ksa_providers"], []) or [])

        logger.info("[env] App=%s | Env=%s | Version=%s | env.py=%s | source=%s", app_name, env_name, ver, ENV_VERSION, _SETTINGS_SOURCE)
        logger.info("[env] Providers=%s", _safe_join([str(x) for x in providers]))
        logger.info("[env] KSA Providers=%s", _safe_join([str(x) for x in ksa_providers]))
        logger.info(
            "[env] Sheets creds=%s (%s) | SpreadsheetId=%s",
            "SET" if bool(_get_attr_any(s, ["google_credentials_dict"], None)) else "MISSING",
            str(_get_attr_any(s, ["google_creds_source"], "")) or "unknown",
            "SET" if bool(_get_attr_any(s, ["default_spreadsheet_id", "spreadsheet_id"], "")) else "MISSING",
        )
    except Exception:
        pass


# -----------------------------------------------------------------------------
# Build final settings view
# -----------------------------------------------------------------------------
_SETTINGS_BASE = _load_settings_base()
settings = _build_settings_view(_SETTINGS_BASE)
_print_banner_once(settings)


def get_settings() -> object:
    """
    Backward-friendly accessor for modules that prefer a function.
    Returns the wrapped settings view.
    """
    return settings  # type: ignore[return-value]


# -----------------------------------------------------------------------------
# Backward compatible exports (DON'T BREAK IMPORTS)
# -----------------------------------------------------------------------------
APP_NAME = _get_attr_any(settings, ["app_name", "APP_NAME"], "Tadawul Fast Bridge")
APP_ENV = _get_attr_any(settings, ["env", "environment", "APP_ENV"], "production")
APP_VERSION = _resolve_version(settings)

SETTINGS_SOURCE = str(_get_attr_any(settings, ["settings_source"], _SETTINGS_SOURCE) or _SETTINGS_SOURCE)

BACKEND_BASE_URL = _canonical_base_url(str(_get_attr_any(settings, ["backend_base_url", "BACKEND_BASE_URL", "base_url", "BASE_URL"], "") or ""))

APP_TOKEN = _get_attr_any(settings, ["app_token", "APP_TOKEN"], None)
BACKUP_APP_TOKEN = _get_attr_any(settings, ["backup_app_token", "BACKUP_APP_TOKEN"], None)
REQUIRE_AUTH = bool(_get_attr_any(settings, ["require_auth", "REQUIRE_AUTH"], _safe_bool(os.getenv("REQUIRE_AUTH") or os.getenv("AUTH_REQUIRED"), False)))

# Providers
ENABLED_PROVIDERS: List[str] = list(_get_attr_any(settings, ["enabled_providers"], []) or [])
if not ENABLED_PROVIDERS:
    ENABLED_PROVIDERS = _as_list_lower(os.getenv("TFB_PROVIDERS") or os.getenv("ENABLED_PROVIDERS") or os.getenv("PROVIDERS"))

KSA_PROVIDERS: List[str] = list(_get_attr_any(settings, ["enabled_ksa_providers"], []) or [])
if not KSA_PROVIDERS:
    KSA_PROVIDERS = _as_list_lower(os.getenv("TFB_KSA_PROVIDERS") or os.getenv("KSA_PROVIDERS"))

PRIMARY_PROVIDER = (
    _get_attr_any(settings, ["primary_provider", "PRIMARY_PROVIDER"], None)
    or (ENABLED_PROVIDERS[0] if ENABLED_PROVIDERS else "finnhub")
)
PRIMARY_PROVIDER = str(PRIMARY_PROVIDER).strip().lower()

PROVIDERS = _safe_join(ENABLED_PROVIDERS)

# Timeouts / TTLs
HTTP_TIMEOUT_SEC = float(_get_attr_any(settings, ["http_timeout_sec", "HTTP_TIMEOUT_SEC"], os.getenv("HTTP_TIMEOUT_SEC") or os.getenv("HTTP_TIMEOUT") or 25.0))
HTTP_TIMEOUT_SEC = max(5.0, float(HTTP_TIMEOUT_SEC or 25.0))
HTTP_TIMEOUT = _safe_int(_get_attr_any(settings, ["http_timeout", "HTTP_TIMEOUT"], int(HTTP_TIMEOUT_SEC)), int(HTTP_TIMEOUT_SEC))

CACHE_TTL_SEC = float(_get_attr_any(settings, ["cache_ttl_sec", "CACHE_TTL_SEC"], os.getenv("CACHE_TTL_SEC") or 20.0))
QUOTE_TTL_SEC = float(_get_attr_any(settings, ["quote_ttl_sec", "QUOTE_TTL_SEC"], os.getenv("QUOTE_TTL_SEC") or 30.0))
FUNDAMENTALS_TTL_SEC = float(_get_attr_any(settings, ["fundamentals_ttl_sec", "FUNDAMENTALS_TTL_SEC"], os.getenv("FUNDAMENTALS_TTL_SEC") or 21600.0))
ARGAAM_SNAPSHOT_TTL_SEC = float(_get_attr_any(settings, ["argaam_snapshot_ttl_sec", "ARGAAM_SNAPSHOT_TTL_SEC"], os.getenv("ARGAAM_SNAPSHOT_TTL_SEC") or 30.0))

# API keys (do NOT log these)
EODHD_API_KEY = _get_attr_any(settings, ["eodhd_api_key", "EODHD_API_KEY"], os.getenv("EODHD_API_KEY"))
FINNHUB_API_KEY = _get_attr_any(settings, ["finnhub_api_key", "FINNHUB_API_KEY"], os.getenv("FINNHUB_API_KEY"))
FMP_API_KEY = _get_attr_any(settings, ["fmp_api_key", "FMP_API_KEY"], os.getenv("FMP_API_KEY"))
ALPHA_VANTAGE_API_KEY = _get_attr_any(settings, ["alpha_vantage_api_key", "ALPHA_VANTAGE_API_KEY"], os.getenv("ALPHA_VANTAGE_API_KEY"))
ARGAAM_API_KEY = _get_attr_any(settings, ["argaam_api_key", "ARGAAM_API_KEY"], os.getenv("ARGAAM_API_KEY"))

# CORS
ENABLE_CORS_ALL_ORIGINS = bool(
    _get_attr_any(
        settings,
        ["enable_cors_all_origins", "cors_all_origins", "ENABLE_CORS_ALL_ORIGINS"],
        _safe_bool(os.getenv("ENABLE_CORS_ALL_ORIGINS") or os.getenv("CORS_ALL_ORIGINS"), True),
    )
)
CORS_ALL_ORIGINS = ENABLE_CORS_ALL_ORIGINS
CORS_ORIGINS = _get_attr_any(settings, ["cors_origins", "CORS_ORIGINS"], os.getenv("CORS_ORIGINS"))
CORS_ORIGINS_LIST = list(_get_attr_any(settings, ["cors_origins_list", "CORS_ORIGINS_LIST"], ["*"] if ENABLE_CORS_ALL_ORIGINS else []))

# Google Sheets (exports + compatibility)
GOOGLE_SHEETS_CREDENTIALS: Optional[Dict[str, Any]] = _get_attr_any(settings, ["google_credentials_dict", "google_sheets_credentials"], None)
if GOOGLE_SHEETS_CREDENTIALS is None:
    GOOGLE_SHEETS_CREDENTIALS = _try_parse_json_dict(os.getenv("GOOGLE_SHEETS_CREDENTIALS") or os.getenv("GOOGLE_SA_JSON")) or _try_parse_b64_json_dict(
        os.getenv("GOOGLE_SHEETS_CREDENTIALS") or os.getenv("GOOGLE_SA_JSON") or ""
    )

DEFAULT_SPREADSHEET_ID = str(_get_attr_any(settings, ["default_spreadsheet_id", "DEFAULT_SPREADSHEET_ID"], os.getenv("DEFAULT_SPREADSHEET_ID") or os.getenv("SPREADSHEET_ID") or "") or "").strip()
SPREADSHEET_ID = DEFAULT_SPREADSHEET_ID  # alias (Render commonly uses SPREADSHEET_ID)
GOOGLE_SHEETS_ID = str(_get_attr_any(settings, ["google_sheets_id", "GOOGLE_SHEETS_ID"], os.getenv("GOOGLE_SHEETS_ID") or "") or "").strip()

GOOGLE_SHEET_ID = str(_get_attr_any(settings, ["google_sheet_id", "GOOGLE_SHEET_ID"], os.getenv("GOOGLE_SHEET_ID") or "") or "").strip()
GOOGLE_SHEET_RANGE = _get_attr_any(settings, ["google_sheet_range", "GOOGLE_SHEET_RANGE"], os.getenv("GOOGLE_SHEET_RANGE"))

HAS_GOOGLE_SHEETS = bool(_get_attr_any(settings, ["has_google_sheets", "HAS_GOOGLE_SHEETS"], bool(GOOGLE_SHEETS_CREDENTIALS) and bool(DEFAULT_SPREADSHEET_ID)))

# GAS URLs
GOOGLE_APPS_SCRIPT_URL = str(_get_attr_any(settings, ["google_apps_script_url"], os.getenv("GOOGLE_APPS_SCRIPT_URL") or "") or "").strip()
GOOGLE_APPS_SCRIPT_BACKUP_URL = str(_get_attr_any(settings, ["google_apps_script_backup_url"], os.getenv("GOOGLE_APPS_SCRIPT_BACKUP_URL") or "") or "").strip()

# Sheet names
SHEET_KSA_TADAWUL = _get_attr_any(settings, ["sheet_ksa_tadawul", "SHEET_KSA_TADAWUL"], "KSA_Tadawul_Market")
SHEET_GLOBAL_MARKETS = _get_attr_any(settings, ["sheet_global_markets", "SHEET_GLOBAL_MARKETS"], "Global_Markets")
SHEET_MUTUAL_FUNDS = _get_attr_any(settings, ["sheet_mutual_funds", "SHEET_MUTUAL_FUNDS"], "Mutual_Funds")
SHEET_COMMODITIES_FX = _get_attr_any(settings, ["sheet_commodities_fx", "SHEET_COMMODITIES_FX"], "Commodities_FX")
SHEET_MARKET_LEADERS = _get_attr_any(settings, ["sheet_market_leaders", "SHEET_MARKET_LEADERS"], "Market_Leaders")
SHEET_MY_PORTFOLIO = _get_attr_any(settings, ["sheet_my_portfolio", "SHEET_MY_PORTFOLIO"], "My_Portfolio")
SHEET_INSIGHTS_ANALYSIS = _get_attr_any(settings, ["sheet_insights_analysis", "SHEET_INSIGHTS_ANALYSIS"], "Insights_Analysis")
SHEET_INVESTMENT_ADVISOR = _get_attr_any(settings, ["sheet_investment_advisor", "SHEET_INVESTMENT_ADVISOR"], "Investment_Advisor")
SHEET_ECONOMIC_CALENDAR = _get_attr_any(settings, ["sheet_economic_calendar", "SHEET_ECONOMIC_CALENDAR"], "Economic_Calendar")
SHEET_INVESTMENT_INCOME = _get_attr_any(settings, ["sheet_investment_income", "SHEET_INVESTMENT_INCOME"], "Investment_Income_Statement")

# Logging flags
LOG_LEVEL = (str(_get_attr_any(settings, ["log_level", "LOG_LEVEL"], os.getenv("LOG_LEVEL", "INFO"))) or "INFO").upper()
LOG_JSON = bool(_get_attr_any(settings, ["log_json", "LOG_JSON"], _safe_bool(os.getenv("LOG_JSON"), False)))
IS_PRODUCTION = str(APP_ENV).strip().lower() in {"prod", "production"}


def safe_env_summary() -> Dict[str, Any]:
    """
    Safe summary for /system/settings (no secrets).
    """
    return {
        "app": APP_NAME,
        "env": APP_ENV,
        "version": APP_VERSION,
        "env_py_version": ENV_VERSION,
        "settings_source": SETTINGS_SOURCE,
        "backend_base_url": BACKEND_BASE_URL,
        "providers": ENABLED_PROVIDERS,
        "ksa_providers": KSA_PROVIDERS,
        "primary": PRIMARY_PROVIDER,
        "require_auth": REQUIRE_AUTH,
        "cors_all": CORS_ALL_ORIGINS,
        "timeout_sec": HTTP_TIMEOUT_SEC,
        "cache_ttl_sec": CACHE_TTL_SEC,
        "has_google_sheets": HAS_GOOGLE_SHEETS,
        "default_spreadsheet_id_set": bool(DEFAULT_SPREADSHEET_ID),
        "google_sheet_id_set": bool(GOOGLE_SHEET_ID),
        "apps_script_backup_url_set": bool(GOOGLE_APPS_SCRIPT_BACKUP_URL),
        "app_token": _mask(str(APP_TOKEN) if APP_TOKEN else None),
        "google_creds_source": str(_get_attr_any(settings, ["google_creds_source"], "")) or "unknown",
        "keys_present": {
            "eodhd": bool(EODHD_API_KEY),
            "finnhub": bool(FINNHUB_API_KEY),
            "fmp": bool(FMP_API_KEY),
            "alpha_vantage": bool(ALPHA_VANTAGE_API_KEY),
            "argaam": bool(ARGAAM_API_KEY),
        },
    }


__all__ = [
    # main view + accessor
    "settings",
    "get_settings",
    "safe_env_summary",
    # identity
    "ENV_VERSION",
    "SETTINGS_SOURCE",
    "APP_NAME",
    "APP_ENV",
    "APP_VERSION",
    # core URL/auth
    "BACKEND_BASE_URL",
    "APP_TOKEN",
    "BACKUP_APP_TOKEN",
    "REQUIRE_AUTH",
    # providers
    "ENABLED_PROVIDERS",
    "KSA_PROVIDERS",
    "PRIMARY_PROVIDER",
    "PROVIDERS",
    # timeouts/ttl
    "HTTP_TIMEOUT",
    "HTTP_TIMEOUT_SEC",
    "CACHE_TTL_SEC",
    "QUOTE_TTL_SEC",
    "FUNDAMENTALS_TTL_SEC",
    "ARGAAM_SNAPSHOT_TTL_SEC",
    # api keys
    "EODHD_API_KEY",
    "FINNHUB_API_KEY",
    "FMP_API_KEY",
    "ALPHA_VANTAGE_API_KEY",
    "ARGAAM_API_KEY",
    # cors
    "ENABLE_CORS_ALL_ORIGINS",
    "CORS_ALL_ORIGINS",
    "CORS_ORIGINS",
    "CORS_ORIGINS_LIST",
    # google sheets
    "GOOGLE_SHEETS_CREDENTIALS",
    "DEFAULT_SPREADSHEET_ID",
    "SPREADSHEET_ID",
    "GOOGLE_SHEETS_ID",
    "GOOGLE_SHEET_ID",
    "GOOGLE_SHEET_RANGE",
    "HAS_GOOGLE_SHEETS",
    # GAS
    "GOOGLE_APPS_SCRIPT_URL",
    "GOOGLE_APPS_SCRIPT_BACKUP_URL",
    # sheet names
    "SHEET_KSA_TADAWUL",
    "SHEET_GLOBAL_MARKETS",
    "SHEET_MUTUAL_FUNDS",
    "SHEET_COMMODITIES_FX",
    "SHEET_MARKET_LEADERS",
    "SHEET_MY_PORTFOLIO",
    "SHEET_INSIGHTS_ANALYSIS",
    "SHEET_INVESTMENT_ADVISOR",
    "SHEET_ECONOMIC_CALENDAR",
    "SHEET_INVESTMENT_INCOME",
    # logging flags
    "LOG_LEVEL",
    "LOG_JSON",
    "IS_PRODUCTION",
]
