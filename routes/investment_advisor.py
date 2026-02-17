# routes/config.py
"""
ROUTES CONFIG SHIM — PROD SAFE (v3.0.0)
------------------------------------------------------------

Purpose
- Backward compatibility for older imports like:
    from routes.config import get_settings, auth_ok, allowed_tokens, Settings, AUTH_HEADER_NAME
- Single source of truth ideally lives in repo-root `config.py`
- NEVER crashes app startup (defensive import + safe fallbacks)
- Supports BOTH:
    - X-APP-TOKEN header (or configurable AUTH_HEADER_NAME)
    - Authorization: Bearer <token>
    - optional ?token=... when ALLOW_QUERY_TOKEN=1

What’s improved vs v2.1.0
- ✅ Cached token/header resolution (fast, stable), with explicit refresh hook
- ✅ Dynamic header name resolution priority:
      env AUTH_HEADER_NAME -> root config -> settings -> default
- ✅ auth_ok_request() supports any header name (pass token already extracted)
- ✅ Stronger root-config proxying without breaking open-mode behavior
- ✅ Safer mask_settings_dict() (never exposes secrets, includes resolved runtime info)
- ✅ Allows settings to be pydantic v1/v2 or plain objects
- ✅ No heavy imports, no IO, no network

Environment
- APP_TOKEN / BACKUP_APP_TOKEN / TFB_APP_TOKEN
- AUTH_HEADER_NAME (optional, default X-APP-TOKEN)
- ALLOW_QUERY_TOKEN=1 (optional)

Notes
- If root config exists but is missing some helpers, this shim provides safe defaults.
"""

from __future__ import annotations

import importlib
import os
from functools import lru_cache
from typing import Any, Dict, List, Optional, Sequence

SHIM_VERSION = "3.0.0"
CONFIG_VERSION = f"{SHIM_VERSION}-shim"

_TRUTHY = {"1", "true", "yes", "y", "on", "t", "enable", "enabled"}
_FALSY = {"0", "false", "no", "n", "off", "f", "disable", "disabled"}


# -----------------------------------------------------------------------------
# Fallback Settings (used if root config can't be imported OR missing fields)
# -----------------------------------------------------------------------------
class _FallbackSettings:
    # Auth
    auth_header_name: str = "X-APP-TOKEN"
    allow_query_token: bool = False

    # AI batch defaults (commonly used by analysis routes)
    ai_batch_size: int = 25
    ai_batch_timeout_sec: float = 60.0
    ai_batch_concurrency: int = 6
    ai_max_tickers: int = 1200

    # Optional meta
    app_env: str = (os.getenv("APP_ENV") or "production").strip().lower()


Settings = _FallbackSettings  # alias for callers expecting Settings


# -----------------------------------------------------------------------------
# Helpers
# -----------------------------------------------------------------------------
def _strip(x: Any) -> str:
    try:
        return str(x or "").strip()
    except Exception:
        return ""


def _safe_getattr(obj: Any, name: str, default: Any = None) -> Any:
    try:
        return getattr(obj, name, default)
    except Exception:
        return default


def _env_bool(name: str, default: bool = False) -> bool:
    v = _strip(os.getenv(name))
    if not v:
        return default
    vv = v.lower()
    if vv in _TRUTHY:
        return True
    if vv in _FALSY:
        return False
    return default


def _dedupe_keep_order(items: Sequence[str]) -> List[str]:
    out: List[str] = []
    seen = set()
    for s in items:
        if s and s not in seen:
            seen.add(s)
            out.append(s)
    return out


def _load_root_config() -> Optional[Any]:
    try:
        return importlib.import_module("config")
    except Exception:
        return None


def _pydantic_dump(obj: Any) -> Dict[str, Any]:
    """
    Best-effort dump for pydantic v2/v1 objects; returns {} if not possible.
    """
    try:
        md = getattr(obj, "model_dump", None)
        if callable(md):
            d = md()
            return d if isinstance(d, dict) else {}
    except Exception:
        pass
    try:
        dd = getattr(obj, "dict", None)
        if callable(dd):
            d = dd()
            return d if isinstance(d, dict) else {}
    except Exception:
        pass
    return {}


def _extract_bearer(authorization: Optional[str]) -> str:
    a = _strip(authorization)
    if not a:
        return ""
    parts = a.split()
    if len(parts) == 2 and parts[0].lower() == "bearer":
        return parts[1].strip()
    return ""


# -----------------------------------------------------------------------------
# Defaults (overridden if root config provides them)
# -----------------------------------------------------------------------------
def get_settings() -> object:
    return _FallbackSettings()


@lru_cache(maxsize=1)
def _root() -> Optional[Any]:
    return _load_root_config()


def refresh_config_cache() -> None:
    """
    Optional utility for long-running processes/tests that change env vars.
    Safe to call; does not crash.
    """
    try:
        _root.cache_clear()  # type: ignore[attr-defined]
    except Exception:
        pass
    try:
        _allowed_tokens_cached.cache_clear()  # type: ignore[attr-defined]
    except Exception:
        pass
    try:
        _auth_header_name_cached.cache_clear()  # type: ignore[attr-defined]
    except Exception:
        pass


@lru_cache(maxsize=1)
def _settings_cached() -> object:
    try:
        return get_settings()
    except Exception:
        return _FallbackSettings()


def _resolve_auth_header_name_from_settings(s: Any) -> str:
    # Try multiple common names
    for attr in ("auth_header_name", "AUTH_HEADER_NAME", "authHeaderName", "auth_header"):
        v = _strip(_safe_getattr(s, attr))
        if v:
            return v
    # Try pydantic dicts too
    d = _pydantic_dump(s)
    for k in ("auth_header_name", "AUTH_HEADER_NAME", "authHeaderName", "auth_header"):
        v = _strip(d.get(k))
        if v:
            return v
    return ""


@lru_cache(maxsize=1)
def _auth_header_name_cached() -> str:
    # 1) env override wins
    env_v = _strip(os.getenv("AUTH_HEADER_NAME"))
    if env_v:
        return env_v

    r = _root()
    # 2) root constant
    if r is not None:
        root_v = _strip(_safe_getattr(r, "AUTH_HEADER_NAME"))
        if root_v:
            return root_v

    # 3) settings object
    try:
        s = _settings_cached()
        sv = _resolve_auth_header_name_from_settings(s)
        if sv:
            return sv
    except Exception:
        pass

    # 4) default
    return "X-APP-TOKEN"


AUTH_HEADER_NAME = _auth_header_name_cached()


def _tokens_from_settings(s: Any) -> List[str]:
    toks: List[str] = []
    # object attributes
    for attr in (
        "app_token",
        "backup_app_token",
        "tfb_app_token",
        "APP_TOKEN",
        "BACKUP_APP_TOKEN",
        "TFB_APP_TOKEN",
        "token",
        "auth_token",
    ):
        v = _strip(_safe_getattr(s, attr))
        if v:
            toks.append(v)

    # pydantic dumps
    d = _pydantic_dump(s)
    for k in (
        "app_token",
        "backup_app_token",
        "tfb_app_token",
        "APP_TOKEN",
        "BACKUP_APP_TOKEN",
        "TFB_APP_TOKEN",
        "token",
        "auth_token",
    ):
        v = _strip(d.get(k))
        if v:
            toks.append(v)

    return toks


@lru_cache(maxsize=1)
def _allowed_tokens_cached() -> List[str]:
    toks: List[str] = []

    # 1) root helper if present
    r = _root()
    if r is not None:
        fn = _safe_getattr(r, "allowed_tokens")
        if callable(fn):
            try:
                out = fn()
                if isinstance(out, list):
                    toks.extend([_strip(x) for x in out if _strip(x)])
            except Exception:
                pass

    # 2) settings fields
    try:
        s = _settings_cached()
        toks.extend(_tokens_from_settings(s))
    except Exception:
        pass

    # 3) env safety net
    for k in ("APP_TOKEN", "BACKUP_APP_TOKEN", "TFB_APP_TOKEN"):
        v = _strip(os.getenv(k))
        if v:
            toks.append(v)

    return _dedupe_keep_order([t for t in toks if t])


def allowed_tokens() -> List[str]:
    return list(_allowed_tokens_cached())


def is_open_mode() -> bool:
    # root override if present
    r = _root()
    if r is not None:
        fn = _safe_getattr(r, "is_open_mode")
        if callable(fn):
            try:
                return bool(fn())
            except Exception:
                pass
    return len(_allowed_tokens_cached()) == 0


def auth_ok(x_app_token: Optional[str]) -> bool:
    """
    Backward compatible:
      - If tokens NOT set => open mode => True
      - Else checks the configured auth header token value
    """
    toks = _allowed_tokens_cached()
    if not toks:
        return True
    return bool(x_app_token and _strip(x_app_token) in toks)


def auth_ok_request(
    *,
    x_app_token: Optional[str] = None,
    authorization: Optional[str] = None,
    query_token: Optional[str] = None,
) -> bool:
    """
    Modern helper:
    - Accepts token already extracted from your chosen header (AUTH_HEADER_NAME)
      plus Authorization Bearer, optional ?token when allowed.
    """
    toks = _allowed_tokens_cached()
    if not toks:
        return True

    tok = _strip(x_app_token)
    if tok and tok in toks:
        return True

    b = _extract_bearer(authorization)
    if b and b in toks:
        return True

    # allow query token via env OR settings OR root flag
    allow_q = _env_bool("ALLOW_QUERY_TOKEN", False)

    # root override
    r = _root()
    if r is not None:
        try:
            rv = _safe_getattr(r, "ALLOW_QUERY_TOKEN", None)
            if rv is not None:
                allow_q = bool(rv)
        except Exception:
            pass

    # settings override
    try:
        s = _settings_cached()
        sv = _safe_getattr(s, "allow_query_token", None)
        if sv is not None:
            allow_q = bool(sv)
        d = _pydantic_dump(s)
        if "allow_query_token" in d and d["allow_query_token"] is not None:
            allow_q = bool(d["allow_query_token"])
    except Exception:
        pass

    if allow_q:
        qt = _strip(query_token)
        if qt and qt in toks:
            return True

    return False


def mask_settings_dict() -> Dict[str, Any]:
    """
    Safe snapshot for logs/health.
    NEVER returns secrets.
    """
    try:
        s = _settings_cached()
        exposed: Dict[str, Any] = {}

        # common AI fields (safe)
        for k in ("ai_batch_size", "ai_batch_timeout_sec", "ai_batch_concurrency", "ai_max_tickers", "app_env"):
            v = _safe_getattr(s, k, None)
            if v is None:
                v = _pydantic_dump(s).get(k)
            if v is not None:
                exposed[k] = v

        # auth info (safe)
        exposed["auth_header_name"] = _resolve_auth_header_name_from_settings(s) or _auth_header_name_cached()
        exposed["allow_query_token"] = _env_bool("ALLOW_QUERY_TOKEN", False)

        # root config version info if exists
        r = _root()
        root_version = _strip(_safe_getattr(r, "CONFIG_VERSION")) if r is not None else ""
        cfg_version = root_version or CONFIG_VERSION

        return {
            "status": "ok",
            "open_mode": is_open_mode(),
            "config_version": cfg_version,
            "shim_version": SHIM_VERSION,
            "auth_header": _auth_header_name_cached(),
            "tokens_set": len(_allowed_tokens_cached()),
            "exposed": exposed,
        }
    except Exception:
        return {
            "status": "fallback",
            "open_mode": True,
            "config_version": CONFIG_VERSION,
            "shim_version": SHIM_VERSION,
            "auth_header": _auth_header_name_cached(),
            "tokens_set": len(_allowed_tokens_cached()),
        }


# -----------------------------------------------------------------------------
# Dynamic Proxy Logic (Prefer root config, fallback to shim)
# -----------------------------------------------------------------------------
r = _root()

if r is not None:
    # Prefer root CONFIG_VERSION if present
    rv = _safe_getattr(r, "CONFIG_VERSION", None)
    if rv is not None:
        try:
            CONFIG_VERSION = str(rv)
        except Exception:
            pass

    # Prefer root Settings if present
    rs = _safe_getattr(r, "Settings", None)
    if rs is not None:
        Settings = rs  # type: ignore[assignment]

    # Prefer root get_settings if present
    rgs = _safe_getattr(r, "get_settings", None)
    if callable(rgs):
        get_settings = rgs  # type: ignore[assignment]
        # refresh cached settings object on import swap
        try:
            _settings_cached.cache_clear()  # type: ignore[attr-defined]
        except Exception:
            pass

    # Prefer root auth_ok if present (keeps backward compat)
    rauth = _safe_getattr(r, "auth_ok", None)
    if callable(rauth):
        auth_ok = rauth  # type: ignore[assignment]

    # Prefer root mask_settings_dict if present
    rmsk = _safe_getattr(r, "mask_settings_dict", None)
    if callable(rmsk):
        mask_settings_dict = rmsk  # type: ignore[assignment]

    # AUTH_HEADER_NAME: prefer root constant, else cached resolver
    root_ahn = _strip(_safe_getattr(r, "AUTH_HEADER_NAME", ""))
    if root_ahn:
        AUTH_HEADER_NAME = root_ahn


__all__ = [
    "CONFIG_VERSION",
    "Settings",
    "AUTH_HEADER_NAME",
    "get_settings",
    "allowed_tokens",
    "is_open_mode",
    "auth_ok",
    "auth_ok_request",
    "mask_settings_dict",
    "refresh_config_cache",
]
