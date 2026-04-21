#!/usr/bin/env python3
"""
routes/top10_investments.py
===============================================================================
Top 10 Investments Routes — v4.2.0
===============================================================================
PREFIX-ISOLATED • AUTH-OPTIONAL (ENV-GATED) • SCHEMA-REGISTRY SHIM • CORE-
CONFIG-ALIGNED • ROUTE-OWNER ATTRIBUTED • STARTUP-SAFE

Canonical-plan note (read first)
--------------------------------
This module provides a dedicated Top_10_Investments route family under the
prefix `/v1/top10`. Per `main._CONTROLLED_CANONICAL_OWNER_MAP`, the
authoritative Top_10_Investments sheet-rows owner remains
`routes.advanced_analysis` at `/sheet-rows` and
`routes.investment_advisor` at `/v1/advanced/sheet-rows`. This router is a
STANDALONE Top10-focused diagnostic/utility surface that does NOT attempt
to take ownership of those canonical paths.

Where does this router fit in main.py?
-------------------------------------
- It is NOT currently listed in `main._CONTROLLED_ROUTE_PLAN` (which owns
  config / schema / analysis / advanced / advisor / enriched families).
- It is NOT currently listed in `main._OPTIONAL_ROUTE_MODULES` either.
- None of its paths appear in `main._CONTROLLED_CANONICAL_OWNER_MAP`.
- No entry exists in `main._allowed_prefixes_for_key` for "top10_investments"
  — meaning if a future main revision adds this router to the controlled
  plan, `_clone_filtered_router` would see an empty allowed-prefixes tuple
  and pass every path through unfiltered. That is still safe because the
  `/v1/top10` prefix used here does not collide with any canonical-owner
  path.

To enable this router in main.py, add one line to `_OPTIONAL_ROUTE_MODULES`:
    "routes.top10_investments"
And optionally, to `_allowed_prefixes_for_key`:
    "top10_investments": ("/v1/top10",)

Why this revision (v4.2.0 vs v4.1.0)
-------------------------------------
- FIX: adds the `/v1/top10` router prefix. v4.1.0 declared paths like
       `/sheet-rows`, `/schema`, `/health`, `/` directly — these collide
       with `main._CONTROLLED_CANONICAL_OWNER_MAP` entries (owned by
       `routes.advanced_analysis`) and with the main app's root aliases.
       With the `/v1/top10` prefix, every path becomes
       `/v1/top10/<whatever>` and there is zero collision.
- FIX: adds env-gated optional auth via the project-wide flexible 6-level
       `core.config.auth_ok` dispatch pattern. Default is OFF (preserves
       v4.1.0 behavior — no breaking change). Enable by setting
       `TOP10_REQUIRE_AUTH=true`. When enabled, `/top10`, `/run`,
       `/recommendations`, `/sheet-rows`, and `/sheet_rows` require a valid
       token via X-APP-TOKEN / X-API-Key / Bearer / query. `/health`,
       `/schema`, `/sheet-spec`, `/` remain unauthenticated to support
       diagnostics. Matches `routes.investment_advisor v5.2.0`,
       `routes.enriched_quote v8.5.0`, `routes.routes_argaam v2.1.0`.
- FIX: every response now includes `route_owner`, `route_family`,
       `timestamp_utc`, `request_id` — consistent with other revised
       routers and aids `main`'s `canonical_path_owners` diagnostics.
- FIX: `/health` now includes a `capabilities` dict (engine_resolvable,
       auth_ok_callable, is_open_mode_callable, get_settings_cached_callable,
       schema_registry_importable, auth_required), `route_owner`,
       `route_family`, `owned_paths` list, `timestamp_utc`, and
       `request_id`. v4.1.0 had 8 fields; v4.2.0 makes operator diagnosis
       self-describing.
- FIX: adds project-wide `_TRUTHY` / `_FALSY` env parser (matches
       `main._TRUTHY` / `_FALSY`).
- FIX: adds `get_router()` and `mount(app)` helpers matching the dynamic
       loader contract used by `routes.routes_argaam` / `routes.config`.
- FIX: FastAPI `Query` now uses the `default=...` keyword style consistent
       with the rest of the project (cosmetic; both styles work).
- KEEP: the v4.1.0 schema_registry shim layer is preserved intact — that
       was the critical module-startup fix in v4.1.0. All shim helpers
       (`get_display_headers`, `get_keys`, `get_required_keys`,
       `get_sheet_spec`, `get_supported_pages`, `project_row_to_schema`,
       `project_rows_to_schema`, `rows_to_matrix`,
       `validate_row_against_schema`) continue to try the canonical
       registry function first and fall back to inline implementations.
- KEEP: `Top10Request` model, blended scoring, deduplication, filter
       logic, engine resolution, all route endpoints, envelope shape,
       direct input rows handling, fallback source-pages pipeline.

Endpoints (all under `/v1/top10` prefix)
----------------------------------------
- GET  /v1/top10/            — root landing
- GET  /v1/top10/health      — diagnostic
- GET  /v1/top10/schema      — schema spec (alias of /sheet-spec)
- GET  /v1/top10/sheet-spec  — schema spec
- GET  /v1/top10/sheet_spec  — schema spec (underscore alias)
- GET  /v1/top10/page        — Top10 via query params
- GET  /v1/top10/top10       — Top10 via query params
- POST /v1/top10/top10       — Top10 via JSON body
- POST /v1/top10/run         — Top10 via JSON body (alias)
- POST /v1/top10/recommendations  — Top10 via JSON body (alias)
- POST /v1/top10/sheet-rows  — sheet-rows output
- POST /v1/top10/sheet_rows  — sheet-rows output (underscore alias)
- GET  /v1/top10/sheet-rows  — sheet-rows with schema_only / headers_only
- GET  /v1/top10/sheet_rows  — sheet-rows (underscore alias)
"""

from dataclasses import dataclass
from datetime import datetime, timezone
import importlib
import inspect
import logging
import os
import uuid
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple, Union

from fastapi import APIRouter, Body, Header, HTTPException, Query, Request, status
from fastapi.concurrency import run_in_threadpool
from pydantic import BaseModel, Field

logger = logging.getLogger(__name__)

VERSION = "4.2.0"
ROUTE_OWNER_NAME = "top10_investments"
ROUTE_FAMILY_NAME = "top10"
PAGE_NAME = "Top_10_Investments"

DEFAULT_TOP_N = 10
DEFAULT_PER_SOURCE_LIMIT = 80
DEFAULT_SOURCE_PAGES = [
    "Market_Leaders",
    "My_Portfolio",
    "Global_Markets",
    "Mutual_Funds",
    "Commodities_FX",
]

# Project-wide truthy/falsy vocabulary — matches main._TRUTHY / main._FALSY.
_TRUTHY = {"1", "true", "yes", "y", "on", "t", "enabled", "enable"}
_FALSY = {"0", "false", "no", "n", "off", "f", "disabled", "disable"}

# v4.2.0: prefix-isolated router. Paths become /v1/top10/*.
router = APIRouter(prefix="/v1/top10", tags=["top10_investments"])


# =============================================================================
# Generic helpers (project-aligned)
# =============================================================================
def _now_utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _env_bool(name: str, default: bool = False) -> bool:
    try:
        raw = (os.getenv(name, "") or "").strip().lower()
    except Exception:
        return bool(default)
    if not raw:
        return bool(default)
    if raw in _TRUTHY:
        return True
    if raw in _FALSY:
        return False
    return bool(default)


def _request_id(request: Optional[Request], x_request_id: Optional[str] = None) -> str:
    # Prefer RequestIDMiddleware-set value, then explicit header, then a UUID12.
    if request is not None:
        try:
            rid = getattr(request.state, "request_id", None)
            if rid:
                return str(rid)
        except Exception:
            pass
    if x_request_id:
        return str(x_request_id)
    if request is not None:
        try:
            hdr = request.headers.get("X-Request-ID")
            if hdr:
                return str(hdr)
        except Exception:
            pass
    return uuid.uuid4().hex[:12]


# =============================================================================
# Module-level core.config capture (mirrors other routers)
# =============================================================================
try:
    from core.config import auth_ok as _core_auth_ok  # type: ignore
except Exception:
    _core_auth_ok = None  # type: ignore

try:
    from core.config import is_open_mode as _core_is_open_mode  # type: ignore
except Exception:
    _core_is_open_mode = None  # type: ignore

try:
    from core.config import get_settings_cached as _core_get_settings_cached  # type: ignore
except Exception:
    def _core_get_settings_cached(*args: Any, **kwargs: Any) -> Any:  # type: ignore
        return None


# =============================================================================
# Auth (project-wide flexible 6-level dispatch — env-gated)
# =============================================================================
def _auth_require_enabled() -> bool:
    """
    v4.2.0 auth gating. Default is FALSE (preserves v4.1.0 behavior where
    this router had no auth at all). Flip `TOP10_REQUIRE_AUTH=true` to
    engage the project-wide flexible auth dispatch on data endpoints.
    """
    return _env_bool("TOP10_REQUIRE_AUTH", False)


def _extract_token_for_auth(
    request: Request,
    *,
    token_q: Optional[str],
    x_app_token: Optional[str],
    x_api_key: Optional[str],
    authorization: Optional[str],
) -> Tuple[Optional[str], Optional[str]]:
    try:
        authz = authorization or request.headers.get("Authorization")
    except Exception:
        authz = authorization

    token = (x_app_token or "").strip() or (x_api_key or "").strip()

    try:
        if not token:
            hdr_app = request.headers.get("X-APP-TOKEN") or ""
            if hdr_app:
                token = hdr_app.strip()
        if not token:
            hdr_key = request.headers.get("X-API-Key") or request.headers.get("X-API-KEY") or ""
            if hdr_key:
                token = hdr_key.strip()
    except Exception:
        pass

    if authz and isinstance(authz, str) and authz.strip().lower().startswith("bearer "):
        token = authz.strip().split(" ", 1)[1].strip()

    if (not token) and token_q:
        try:
            settings = _core_get_settings_cached()
            allow_query = bool(
                getattr(settings, "allow_query_token", False)
                or getattr(settings, "ALLOW_QUERY_TOKEN", False)
            )
        except Exception:
            allow_query = _env_bool("ALLOW_QUERY_TOKEN", False)
        if allow_query:
            token = token_q.strip()

    return (token or None), authz


def _auth_ok_flexible(
    request: Request,
    *,
    token_q: Optional[str],
    x_app_token: Optional[str],
    x_api_key: Optional[str],
    authorization: Optional[str],
) -> bool:
    """
    Flexible 6-level signature dispatch for core.config.auth_ok — matches
    main._call_auth_ok_flexible / investment_advisor v5.2.0 / enriched_quote
    v8.5.0 / routes_argaam v2.1.0.
    """
    # Open-mode short-circuit
    try:
        if callable(_core_is_open_mode) and bool(_core_is_open_mode()):
            return True
    except Exception:
        pass

    # core.config unavailable → fail open (preserves v4.1.0 no-auth default)
    if _core_auth_ok is None or not callable(_core_auth_ok):
        return True

    tok, authz = _extract_token_for_auth(
        request,
        token_q=token_q,
        x_app_token=x_app_token,
        x_api_key=x_api_key,
        authorization=authorization,
    )

    try:
        headers_dict = dict(request.headers)
    except Exception:
        headers_dict = {}
    if x_app_token:
        headers_dict.setdefault("X-APP-TOKEN", x_app_token)
    if x_api_key:
        headers_dict.setdefault("X-API-Key", x_api_key)
    if authorization:
        headers_dict.setdefault("Authorization", authorization)

    try:
        path = str(getattr(getattr(request, "url", None), "path", "") or "")
    except Exception:
        path = ""

    try:
        settings = _core_get_settings_cached()
    except Exception:
        settings = None

    attempts: Tuple[Dict[str, Any], ...] = (
        {
            "token": tok,
            "authorization": authz,
            "headers": headers_dict,
            "api_key": x_api_key,
            "path": path,
            "request": request,
            "settings": settings,
        },
        {
            "token": tok,
            "authorization": authz,
            "headers": headers_dict,
            "path": path,
            "request": request,
        },
        {
            "token": tok,
            "authorization": authz,
            "headers": headers_dict,
            "path": path,
        },
        {
            "token": tok,
            "authorization": authz,
            "headers": headers_dict,
        },
        {
            "token": tok,
            "authorization": authz,
        },
        {
            "token": tok,
        },
    )

    for kwargs in attempts:
        try:
            return bool(_core_auth_ok(**kwargs))
        except TypeError:
            continue
        except Exception:
            return False
    return False


def _require_auth_or_401(
    request: Request,
    *,
    token_q: Optional[str],
    x_app_token: Optional[str],
    x_api_key: Optional[str],
    authorization: Optional[str],
) -> None:
    """
    Enforces auth only if `TOP10_REQUIRE_AUTH=true`. Otherwise is a no-op.
    Matches every other router's `_require_auth_or_401` signature.
    """
    if not _auth_require_enabled():
        return
    if not _auth_ok_flexible(
        request,
        token_q=token_q,
        x_app_token=x_app_token,
        x_api_key=x_api_key,
        authorization=authorization,
    ):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid token",
        )


# =============================================================================
# Import-safe schema_registry shim layer (v4.1.0 — PRESERVED INTACT)
# Each helper tries the preferred schema_registry v3.4.0 function first,
# then falls back to a deterministic inline implementation.
# =============================================================================
def _load_sr() -> Any:
    """Return schema_registry module or None."""
    for path in ("core.sheets.schema_registry", "core.schema_registry", "schema_registry"):
        try:
            return importlib.import_module(path)
        except Exception:
            continue
    return None


_SR = _load_sr()


def get_display_headers(page: str) -> List[str]:
    """Return display headers for the given page."""
    if _SR is not None:
        # v3.4.0 canonical: get_display_headers or get_sheet_headers
        for fn_name in ("get_display_headers", "get_sheet_headers"):
            fn = getattr(_SR, fn_name, None)
            if callable(fn):
                try:
                    return list(fn(page))
                except Exception:
                    pass
    return []


def get_keys(page: str) -> List[str]:
    """Return schema keys for the given page."""
    if _SR is not None:
        for fn_name in ("get_keys", "get_sheet_keys"):
            fn = getattr(_SR, fn_name, None)
            if callable(fn):
                try:
                    return list(fn(page))
                except Exception:
                    pass
    return []


def get_required_keys(page: str) -> List[str]:
    """Return required keys. Falls back to top10-specific required set."""
    if _SR is not None:
        fn = getattr(_SR, "get_required_keys", None)
        if callable(fn):
            try:
                return list(fn(page))
            except Exception:
                pass
        # Derive from spec columns where required=True
        spec_fn = getattr(_SR, "get_sheet_spec", None)
        if callable(spec_fn):
            try:
                spec = spec_fn(page)
                cols = getattr(spec, "columns", None) or []
                req = []
                for c in cols:
                    d = c if isinstance(c, dict) else (vars(c) if hasattr(c, "__dict__") else {})
                    if d.get("required"):
                        k = d.get("key") or getattr(c, "key", None)
                        if k:
                            req.append(str(k))
                if req:
                    return req
            except Exception:
                pass
    # Deterministic fallback for Top_10_Investments
    if page == PAGE_NAME:
        return ["symbol", "top10_rank", "selection_reason"]
    return ["symbol"]


def get_sheet_spec(page: str, *, include_fields: bool = False) -> Any:
    """Return sheet spec object."""
    if _SR is not None:
        fn = getattr(_SR, "get_sheet_spec", None)
        if callable(fn):
            try:
                # Try with include_fields kwarg (newer API)
                try:
                    return fn(page, include_fields=include_fields)
                except TypeError:
                    return fn(page)
            except Exception:
                pass
    return {"page": page, "keys": get_keys(page), "headers": get_display_headers(page)}


def get_supported_pages() -> List[str]:
    """Return all registered page names."""
    if _SR is not None:
        for fn_name in ("get_supported_pages", "list_sheets", "list_pages"):
            fn = getattr(_SR, fn_name, None)
            if callable(fn):
                try:
                    return list(fn())
                except Exception:
                    pass
    return list(DEFAULT_SOURCE_PAGES) + [PAGE_NAME, "Insights_Analysis", "Data_Dictionary"]


def project_row_to_schema(page: str, row: Dict[str, Any]) -> Dict[str, Any]:
    """Project a row dict to the canonical schema keys for the given page."""
    if _SR is not None:
        fn = getattr(_SR, "project_row_to_schema", None)
        if callable(fn):
            try:
                return dict(fn(page, row))
            except Exception:
                pass
    # Inline shim: return row keyed to page's canonical keys; None for missing
    keys = get_keys(page)
    if not keys:
        return dict(row)
    out: Dict[str, Any] = {}
    for k in keys:
        out[k] = row.get(k)
    # Carry through any extra keys not in schema (e.g. _blended_score scratch field)
    for k, v in row.items():
        if k not in out:
            out[k] = v
    return out


def project_rows_to_schema(page: str, rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Project multiple rows to the canonical schema."""
    if _SR is not None:
        fn = getattr(_SR, "project_rows_to_schema", None)
        if callable(fn):
            try:
                return list(fn(page, rows))
            except Exception:
                pass
    return [project_row_to_schema(page, r) for r in rows]


def rows_to_matrix(page: str, rows: List[Dict[str, Any]]) -> List[List[Any]]:
    """Convert row dicts to a 2D list aligned to the page's canonical keys."""
    if _SR is not None:
        fn = getattr(_SR, "rows_to_matrix", None)
        if callable(fn):
            try:
                return [list(r) for r in fn(page, rows)]
            except Exception:
                pass
    keys = get_keys(page)
    if not keys:
        return []
    return [[r.get(k) for k in keys] for r in rows]


def validate_row_against_schema(
    page: str, row: Dict[str, Any], *, strict: bool = False
) -> Dict[str, Any]:
    """Validate a row against the schema. Returns a validation summary dict."""
    if _SR is not None:
        fn = getattr(_SR, "validate_row_against_schema", None)
        if callable(fn):
            try:
                return dict(fn(page, row, strict=strict))
            except Exception:
                pass
    required = get_required_keys(page)
    missing = [k for k in required if row.get(k) in (None, "")]
    return {
        "valid": len(missing) == 0,
        "missing": missing,
        "page": page,
    }


# =============================================================================
# Request model
# =============================================================================
class Top10Request(BaseModel):
    page: str = Field(default=PAGE_NAME)
    source_pages: Optional[Union[List[str], str]] = Field(
        default=None,
        description="Optional source pages used for fallback ranking.",
    )
    limit: int = Field(default=DEFAULT_TOP_N, ge=1, le=100)
    top_n: Optional[int] = Field(default=None, ge=1, le=100)
    per_source_limit: int = Field(default=DEFAULT_PER_SOURCE_LIMIT, ge=1, le=500)
    symbols: Optional[Union[List[str], str]] = None
    mode: Optional[str] = None
    rows: Optional[List[Dict[str, Any]]] = None
    data: Optional[List[Dict[str, Any]]] = None
    include_meta: bool = True
    include_validation: bool = False
    min_confidence: Optional[float] = None
    max_risk_score: Optional[float] = None
    force_fallback: bool = False


@dataclass
class EngineResolver:
    source: str
    callable_obj: Any


# =============================================================================
# Generic list/string helpers
# =============================================================================
def _as_list(value: Any) -> List[Any]:
    if value is None:
        return []
    if isinstance(value, list):
        return value
    if isinstance(value, tuple):
        return list(value)
    if isinstance(value, set):
        return list(value)
    if isinstance(value, str):
        parts = [x.strip() for x in value.replace(";", ",").split(",")]
        return [x for x in parts if x]
    return [value]


def _clean_pages(value: Any) -> List[str]:
    pages = [str(x).strip() for x in _as_list(value) if str(x).strip()]
    if not pages:
        pages = list(DEFAULT_SOURCE_PAGES)
    valid = set(get_supported_pages())
    out: List[str] = []
    for page in pages:
        if page == PAGE_NAME:
            continue
        if page in valid and page not in out:
            out.append(page)
    return out


def _clean_symbols(value: Any) -> List[str]:
    out: List[str] = []
    for item in _as_list(value):
        text = str(item).strip()
        if text and text not in out:
            out.append(text)
    return out


def _to_float(value: Any) -> Optional[float]:
    if value is None:
        return None
    if isinstance(value, bool):
        return float(value)
    if isinstance(value, (int, float)):
        return float(value)
    text = str(value).strip().replace(",", "").replace("%", "")
    if not text:
        return None
    try:
        return float(text)
    except Exception:
        return None


def _percent_points(value: Any) -> Optional[float]:
    num = _to_float(value)
    if num is None:
        return None
    if -1.5 <= num <= 1.5:
        return num * 100.0
    return num


def _safe_text(value: Any) -> str:
    return str(value or "").strip()


def _extract_rows(payload: Any) -> List[Dict[str, Any]]:
    if payload is None:
        return []

    if isinstance(payload, list):
        return [dict(item) for item in payload if isinstance(item, Mapping)]

    if not isinstance(payload, Mapping):
        return []

    keys = payload.get("keys") or payload.get("headers") or payload.get("display_headers") or []
    rows = payload.get("rows")
    if isinstance(rows, list):
        if rows and isinstance(rows[0], Mapping):
            return [dict(x) for x in rows if isinstance(x, Mapping)]
        if rows and isinstance(rows[0], (list, tuple)) and keys:
            out: List[Dict[str, Any]] = []
            for row in rows:
                row_list = list(row)
                out.append({str(keys[i]): row_list[i] if i < len(row_list) else None for i in range(len(keys))})
            return out

    rows_matrix = payload.get("rows_matrix")
    if isinstance(rows_matrix, list) and keys:
        out = []
        for row in rows_matrix:
            if isinstance(row, (list, tuple)):
                row_list = list(row)
                out.append({str(keys[i]): row_list[i] if i < len(row_list) else None for i in range(len(keys))})
        if out:
            return out

    for candidate_key in ("data", "items", "results", "quotes"):
        candidate = payload.get(candidate_key)
        if isinstance(candidate, list):
            extracted = _extract_rows(candidate)
            if extracted:
                return extracted
        if isinstance(candidate, Mapping):
            extracted = _extract_rows(candidate)
            if extracted:
                return extracted

    return []


def _resolve_engine() -> Optional[EngineResolver]:
    candidates: List[Tuple[str, str]] = [
        ("core.data_engine_v2", "get_sheet_rows_sync"),
        ("core.data_engine_v2", "get_sheet_rows"),
        ("core.data_engine", "get_sheet_rows_sync"),
        ("core.data_engine", "get_sheet_rows"),
    ]
    for module_name, attr_name in candidates:
        try:
            module = importlib.import_module(module_name)
            func = getattr(module, attr_name, None)
            if callable(func):
                return EngineResolver(source=f"{module_name}.{attr_name}", callable_obj=func)
        except Exception:
            continue

    for module_name in ("core.data_engine_v2", "core.data_engine"):
        try:
            module = importlib.import_module(module_name)
            engine = getattr(module, "engine", None)
            if engine is not None and hasattr(engine, "get_sheet_rows"):
                return EngineResolver(
                    source=f"{module_name}.engine.get_sheet_rows",
                    callable_obj=getattr(engine, "get_sheet_rows"),
                )
        except Exception:
            continue

    return None


async def _call_maybe_async(func: Any, *args: Any, **kwargs: Any) -> Any:
    if inspect.iscoroutinefunction(func):
        return await func(*args, **kwargs)
    result = await run_in_threadpool(lambda: func(*args, **kwargs))
    if inspect.isawaitable(result):
        return await result
    return result


async def _engine_fetch_page_rows(
    page: str,
    *,
    limit: int,
    symbols: Optional[List[str]] = None,
    mode: Optional[str] = None,
) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    resolver = _resolve_engine()
    if resolver is None:
        return [], {"engine_available": False, "engine_source": None}

    kwargs: Dict[str, Any] = {
        "page": page, "sheet": page, "sheet_name": page,
        "name": page, "tab": page, "limit": limit,
        "headers_only": False, "schema_only": False,
    }
    if symbols:
        kwargs["symbols"] = symbols
        kwargs["tickers"] = symbols
        kwargs["direct_symbols"] = symbols
    if mode:
        kwargs["mode"] = mode

    try:
        payload = await _call_maybe_async(resolver.callable_obj, **kwargs)
        rows = _extract_rows(payload)
        return rows, {
            "engine_available": True,
            "engine_source": resolver.source,
            "raw_payload_type": type(payload).__name__,
        }
    except TypeError:
        narrowed: Dict[str, Any] = {"page": page, "limit": limit}
        if symbols:
            narrowed["symbols"] = symbols
        try:
            payload = await _call_maybe_async(resolver.callable_obj, **narrowed)
            rows = _extract_rows(payload)
            return rows, {
                "engine_available": True,
                "engine_source": resolver.source,
                "raw_payload_type": type(payload).__name__,
                "signature_mode": "narrowed",
            }
        except Exception as exc:
            logger.warning("Engine fetch failed for page=%s via narrowed signature: %s", page, exc)
            return [], {"engine_available": True, "engine_source": resolver.source, "engine_error": str(exc)}
    except Exception as exc:
        logger.warning("Engine fetch failed for page=%s: %s", page, exc)
        return [], {"engine_available": True, "engine_source": resolver.source, "engine_error": str(exc)}


# =============================================================================
# Scoring helpers (v4.1.0 — PRESERVED INTACT)
# =============================================================================
def _bucket_penalty(bucket: str) -> float:
    text = _safe_text(bucket).lower()
    if "very high" in text: return 22.0
    if "high" in text: return 15.0
    if "moderate" in text or "medium" in text: return 7.0
    if "low" in text: return 0.0
    return 4.0


def _rating_boost(text: str) -> float:
    t = _safe_text(text).lower()
    if not t: return 0.0
    if "strong buy" in t: return 10.0
    if t == "buy" or " buy" in t: return 7.0
    if "outperform" in t or "overweight" in t: return 6.0
    if "hold" in t or "neutral" in t or "market perform" in t: return 2.0
    if "underperform" in t or "underweight" in t: return -4.0
    if "sell" in t: return -8.0
    return 0.0


def _signal_boost(text: str) -> float:
    t = _safe_text(text).lower()
    if not t: return 0.0
    if any(x in t for x in ("bullish", "positive", "uptrend", "up trend", "strong")): return 5.0
    if any(x in t for x in ("neutral", "sideways", "mixed")): return 1.0
    if any(x in t for x in ("bearish", "negative", "downtrend", "down trend", "weak")): return -4.0
    return 0.0


def _avg(values: Iterable[Optional[float]]) -> Optional[float]:
    nums = [float(x) for x in values if x is not None]
    if not nums: return None
    return sum(nums) / len(nums)


def _blended_score(projected: Mapping[str, Any]) -> float:
    overall = _to_float(projected.get("overall_score"))
    opportunity = _to_float(projected.get("opportunity_score"))
    quality = _to_float(projected.get("quality_score"))
    growth = _to_float(projected.get("growth_score"))
    value = _to_float(projected.get("value_score"))
    momentum = _to_float(projected.get("momentum_score"))
    valuation = _to_float(projected.get("valuation_score"))

    roi_1m = _percent_points(projected.get("expected_roi_1m"))
    roi_3m = _percent_points(projected.get("expected_roi_3m"))
    roi_12m = _percent_points(projected.get("expected_roi_12m"))

    conf = _avg([
        _to_float(projected.get("forecast_confidence")),
        _to_float(projected.get("confidence")),
        _to_float(projected.get("data_quality_score")),
    ])

    risk_score = _to_float(projected.get("risk_score"))
    risk_penalty = (risk_score * 0.30) if risk_score is not None else 0.0
    risk_penalty += _bucket_penalty(_safe_text(projected.get("risk_bucket")))

    rating_bonus = (
        _rating_boost(_safe_text(projected.get("recommendation")))
        + _rating_boost(_safe_text(projected.get("analyst_rating")))
    )
    signal_bonus = (
        _signal_boost(_safe_text(projected.get("signal")))
        + _signal_boost(_safe_text(projected.get("trend_1m")))
        + _signal_boost(_safe_text(projected.get("trend_3m")))
        + _signal_boost(_safe_text(projected.get("trend_12m")))
    )

    numeric_components = [
        (overall, 0.30),
        (opportunity, 0.22),
        (valuation, 0.10),
        (quality, 0.08),
        (growth, 0.08),
        (value, 0.06),
        (momentum, 0.06),
        (roi_1m, 0.02),
        (roi_3m, 0.05),
        (roi_12m, 0.03),
        (conf, 0.10),
    ]

    score = 0.0
    for value_num, weight in numeric_components:
        if value_num is not None:
            score += value_num * weight

    score += rating_bonus + signal_bonus
    score -= risk_penalty
    return round(score, 4)


def _criteria_snapshot(projected: Mapping[str, Any], score: float) -> str:
    parts: List[str] = [f"Blend={score:.2f}"]
    for key, label in [
        ("overall_score", "Overall"),
        ("opportunity_score", "Opp"),
        ("expected_roi_3m", "ROI3M"),
        ("expected_roi_12m", "ROI12M"),
        ("risk_score", "Risk"),
        ("forecast_confidence", "Conf"),
    ]:
        val = projected.get(key)
        if val not in (None, ""):
            parts.append(f"{label}={val}")

    bucket = _safe_text(projected.get("risk_bucket"))
    if bucket:
        parts.append(f"Bucket={bucket}")

    rec = _safe_text(projected.get("recommendation")) or _safe_text(projected.get("analyst_rating"))
    if rec:
        parts.append(f"Rec={rec}")

    return " | ".join(parts)


def _selection_reason(projected: Mapping[str, Any], score: float) -> str:
    reasons: List[str] = []

    overall = _to_float(projected.get("overall_score"))
    opportunity = _to_float(projected.get("opportunity_score"))
    roi_3m = _percent_points(projected.get("expected_roi_3m"))
    conf = _avg([
        _to_float(projected.get("forecast_confidence")),
        _to_float(projected.get("confidence")),
    ])
    risk_bucket = _safe_text(projected.get("risk_bucket"))
    rec = _safe_text(projected.get("recommendation")) or _safe_text(projected.get("analyst_rating"))

    if overall is not None and overall >= 70: reasons.append("strong overall score")
    if opportunity is not None and opportunity >= 70: reasons.append("high opportunity profile")
    if roi_3m is not None and roi_3m >= 8: reasons.append("supportive 3M return outlook")
    if conf is not None and conf >= 65: reasons.append("good forecast confidence")
    if risk_bucket and risk_bucket.lower().startswith("low"):
        reasons.append("contained risk bucket")
    elif risk_bucket and "moderate" in risk_bucket.lower():
        reasons.append("balanced risk profile")
    if rec:
        reasons.append(f"positive signal ({rec})")

    if not reasons:
        reasons.append("best blended score after risk-adjusted ranking")

    sentence = ", ".join(reasons[:4])
    return f"Selected due to {sentence}. Final blended score: {score:.2f}."


def _dedupe_by_symbol_best(rows: Iterable[Dict[str, Any]]) -> List[Dict[str, Any]]:
    best: Dict[str, Dict[str, Any]] = {}
    for row in rows:
        symbol = _safe_text(row.get("symbol")) or _safe_text(row.get("name"))
        if not symbol:
            continue
        current = best.get(symbol)
        current_score = _to_float(current.get("_blended_score")) if current else None
        row_score = _to_float(row.get("_blended_score"))
        if current is None or (
            (row_score if row_score is not None else float("-inf"))
            > (current_score if current_score is not None else float("-inf"))
        ):
            best[symbol] = row
    return list(best.values())


def _apply_filters(
    rows: Iterable[Dict[str, Any]],
    *,
    min_confidence: Optional[float],
    max_risk_score: Optional[float],
) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for row in rows:
        if min_confidence is not None:
            conf = _avg([
                _to_float(row.get("forecast_confidence")),
                _to_float(row.get("confidence")),
            ])
            if conf is None or conf < float(min_confidence):
                continue
        if max_risk_score is not None:
            risk = _to_float(row.get("risk_score"))
            if risk is not None and risk > float(max_risk_score):
                continue
        out.append(row)
    return out


def _rank_top10_rows(
    candidates: Sequence[Mapping[str, Any]], *, limit: int
) -> List[Dict[str, Any]]:
    ranked: List[Dict[str, Any]] = []

    for candidate in candidates:
        projected = project_row_to_schema(PAGE_NAME, dict(candidate))
        score = _blended_score(projected)
        projected["_blended_score"] = score
        projected["criteria_snapshot"] = _criteria_snapshot(projected, score)
        projected["selection_reason"] = _selection_reason(projected, score)
        ranked.append(projected)

    ranked = _dedupe_by_symbol_best(ranked)
    ranked.sort(
        key=lambda x: (
            _to_float(x.get("_blended_score")) if _to_float(x.get("_blended_score")) is not None else float("-inf"),
            _percent_points(x.get("expected_roi_3m")) if _percent_points(x.get("expected_roi_3m")) is not None else float("-inf"),
            _to_float(x.get("overall_score")) if _to_float(x.get("overall_score")) is not None else float("-inf"),
        ),
        reverse=True,
    )

    final_rows: List[Dict[str, Any]] = []
    for idx, row in enumerate(ranked[:limit], start=1):
        row["top10_rank"] = idx
        final_rows.append(project_row_to_schema(PAGE_NAME, row))
    return final_rows


# =============================================================================
# Envelope builder
# =============================================================================
def _build_envelope(
    rows: List[Dict[str, Any]],
    *,
    meta: Optional[Dict[str, Any]] = None,
    include_validation: bool = False,
    request_id: Optional[str] = None,
) -> Dict[str, Any]:
    keys = get_keys(PAGE_NAME)
    headers = get_display_headers(PAGE_NAME)
    projected = project_rows_to_schema(PAGE_NAME, rows)
    matrix = rows_to_matrix(PAGE_NAME, projected)

    envelope: Dict[str, Any] = {
        "ok": True,
        "page": PAGE_NAME,
        "sheet_name": PAGE_NAME,
        "keys": keys,
        "headers": headers,
        "display_headers": headers,
        "required_keys": get_required_keys(PAGE_NAME),
        "rows": projected,
        "rows_matrix": matrix,
        "data": projected,
        "count": len(projected),
        "version": VERSION,
        "route_owner": ROUTE_OWNER_NAME,
        "route_family": ROUTE_FAMILY_NAME,
        "request_id": request_id or uuid.uuid4().hex[:12],
        "timestamp_utc": _now_utc_iso(),
        "meta": {
            "route": "routes.top10_investments",
            "version": VERSION,
            "route_owner": ROUTE_OWNER_NAME,
            "route_family": ROUTE_FAMILY_NAME,
            "page": PAGE_NAME,
            "column_count": len(keys),
            "generated_at_utc": _now_utc_iso(),
            **(meta or {}),
        },
    }

    if include_validation:
        envelope["validation"] = [
            validate_row_against_schema(PAGE_NAME, row, strict=False)
            for row in projected
        ]

    return envelope


# =============================================================================
# Main builder
# =============================================================================
async def _build_top10_from_request(req: Top10Request, *, request_id: str) -> Dict[str, Any]:
    requested_limit = int(req.top_n or req.limit or DEFAULT_TOP_N)
    source_pages = _clean_pages(req.source_pages)
    symbols = _clean_symbols(req.symbols)

    direct_input_rows: List[Dict[str, Any]] = []
    if req.rows: direct_input_rows.extend(req.rows)
    if req.data: direct_input_rows.extend(req.data)

    meta: Dict[str, Any] = {
        "engine_used": False,
        "engine_source": None,
        "force_fallback": bool(req.force_fallback),
        "source_pages": source_pages,
        "requested_limit": requested_limit,
        "per_source_limit": int(req.per_source_limit),
        "input_rows_count": len(direct_input_rows),
    }

    if direct_input_rows:
        filtered = _apply_filters(
            project_rows_to_schema(PAGE_NAME, direct_input_rows),
            min_confidence=req.min_confidence,
            max_risk_score=req.max_risk_score,
        )
        top_rows = _rank_top10_rows(filtered, limit=requested_limit)
        meta["ranking_source"] = "direct_input_rows"
        meta["candidate_rows"] = len(filtered)
        return _build_envelope(top_rows, meta=meta, include_validation=req.include_validation, request_id=request_id)

    if not req.force_fallback:
        engine_rows, engine_meta = await _engine_fetch_page_rows(
            PAGE_NAME,
            limit=max(requested_limit, 10),
            symbols=symbols or None,
            mode=req.mode,
        )
        meta.update(engine_meta)
        if engine_rows:
            projected_engine_rows = project_rows_to_schema(PAGE_NAME, engine_rows)
            filtered_engine_rows = _apply_filters(
                projected_engine_rows,
                min_confidence=req.min_confidence,
                max_risk_score=req.max_risk_score,
            )
            top_rows = _rank_top10_rows(filtered_engine_rows, limit=requested_limit)
            meta["engine_used"] = True
            meta["ranking_source"] = "engine_top10_page"
            meta["candidate_rows"] = len(filtered_engine_rows)
            return _build_envelope(top_rows, meta=meta, include_validation=req.include_validation, request_id=request_id)

    candidate_rows: List[Dict[str, Any]] = []
    per_page_fetch_meta: Dict[str, Any] = {}

    for page in source_pages:
        page_rows, page_meta = await _engine_fetch_page_rows(
            page,
            limit=int(req.per_source_limit),
            symbols=symbols or None,
            mode=req.mode,
        )
        per_page_fetch_meta[page] = {**page_meta, "rows_count": len(page_rows)}
        if not page_rows:
            continue
        candidate_rows.extend(project_rows_to_schema(PAGE_NAME, page_rows))

    filtered_candidates = _apply_filters(
        candidate_rows,
        min_confidence=req.min_confidence,
        max_risk_score=req.max_risk_score,
    )
    top_rows = _rank_top10_rows(filtered_candidates, limit=requested_limit)

    meta["ranking_source"] = "fallback_source_pages"
    meta["candidate_rows"] = len(filtered_candidates)
    meta["engine_used"] = any(v.get("engine_available") for v in per_page_fetch_meta.values())
    meta["source_page_fetch"] = per_page_fetch_meta

    return _build_envelope(top_rows, meta=meta, include_validation=req.include_validation, request_id=request_id)


# =============================================================================
# Routes (all under /v1/top10 prefix)
# =============================================================================
_OWNED_PATHS: Tuple[str, ...] = (
    "/v1/top10/",
    "/v1/top10/health",
    "/v1/top10/schema",
    "/v1/top10/sheet-spec",
    "/v1/top10/sheet_spec",
    "/v1/top10/page",
    "/v1/top10/top10",
    "/v1/top10/run",
    "/v1/top10/recommendations",
    "/v1/top10/sheet-rows",
    "/v1/top10/sheet_rows",
)


@router.get("/health")
async def health(
    request: Request,
    x_request_id: Optional[str] = Header(default=None, alias="X-Request-ID"),
) -> Dict[str, Any]:
    rid = _request_id(request, x_request_id)
    resolver = _resolve_engine()
    capabilities = {
        "engine_resolvable": resolver is not None,
        "auth_ok_callable": callable(_core_auth_ok),
        "is_open_mode_callable": callable(_core_is_open_mode),
        "get_settings_cached_callable": callable(_core_get_settings_cached),
        "schema_registry_importable": _SR is not None,
        "auth_required": _auth_require_enabled(),
    }
    return {
        "ok": True,
        "status": "ok" if resolver is not None else "degraded",
        "service": "top10_investments",
        "route_module": "routes.top10_investments",
        "route_owner": ROUTE_OWNER_NAME,
        "route_family": ROUTE_FAMILY_NAME,
        "version": VERSION,
        "page": PAGE_NAME,
        "engine_available": resolver is not None,
        "engine_source": resolver.source if resolver else None,
        "supported_pages": get_supported_pages(),
        "capabilities": capabilities,
        "owned_paths": list(_OWNED_PATHS),
        "request_id": rid,
        "timestamp_utc": _now_utc_iso(),
    }


@router.get("/sheet-spec")
@router.get("/sheet_spec")
@router.get("/schema")
async def sheet_spec_endpoint(
    request: Request,
    x_request_id: Optional[str] = Header(default=None, alias="X-Request-ID"),
) -> Dict[str, Any]:
    rid = _request_id(request, x_request_id)
    return {
        "ok": True,
        "version": VERSION,
        "route_owner": ROUTE_OWNER_NAME,
        "route_family": ROUTE_FAMILY_NAME,
        "page": PAGE_NAME,
        "spec": get_sheet_spec(PAGE_NAME, include_fields=True),
        "request_id": rid,
        "timestamp_utc": _now_utc_iso(),
    }


@router.get("/page")
@router.get("/top10")
async def top10_get(
    request: Request,
    limit: int = Query(default=DEFAULT_TOP_N, ge=1, le=100),
    top_n: Optional[int] = Query(default=None, ge=1, le=100),
    source_pages: Optional[str] = Query(default=None),
    symbols: Optional[str] = Query(default=None),
    mode: Optional[str] = Query(default=None),
    per_source_limit: int = Query(default=DEFAULT_PER_SOURCE_LIMIT, ge=1, le=500),
    min_confidence: Optional[float] = Query(default=None),
    max_risk_score: Optional[float] = Query(default=None),
    include_validation: bool = Query(default=False),
    force_fallback: bool = Query(default=False),
    token: Optional[str] = Query(default=None),
    x_app_token: Optional[str] = Header(default=None, alias="X-APP-TOKEN"),
    x_api_key: Optional[str] = Header(default=None, alias="X-API-Key"),
    authorization: Optional[str] = Header(default=None, alias="Authorization"),
    x_request_id: Optional[str] = Header(default=None, alias="X-Request-ID"),
) -> Dict[str, Any]:
    _require_auth_or_401(
        request,
        token_q=token,
        x_app_token=x_app_token,
        x_api_key=x_api_key,
        authorization=authorization,
    )
    rid = _request_id(request, x_request_id)
    req = Top10Request(
        limit=limit, top_n=top_n, source_pages=source_pages,
        symbols=symbols, mode=mode, per_source_limit=per_source_limit,
        min_confidence=min_confidence, max_risk_score=max_risk_score,
        include_validation=include_validation, force_fallback=force_fallback,
    )
    return await _build_top10_from_request(req, request_id=rid)


@router.post("/top10")
@router.post("/run")
@router.post("/recommendations")
@router.post("/sheet-rows")
@router.post("/sheet_rows")
async def top10_post(
    request: Request,
    payload: Top10Request = Body(default_factory=Top10Request),
    token: Optional[str] = Query(default=None),
    x_app_token: Optional[str] = Header(default=None, alias="X-APP-TOKEN"),
    x_api_key: Optional[str] = Header(default=None, alias="X-API-Key"),
    authorization: Optional[str] = Header(default=None, alias="Authorization"),
    x_request_id: Optional[str] = Header(default=None, alias="X-Request-ID"),
) -> Dict[str, Any]:
    _require_auth_or_401(
        request,
        token_q=token,
        x_app_token=x_app_token,
        x_api_key=x_api_key,
        authorization=authorization,
    )
    rid = _request_id(request, x_request_id)
    try:
        return await _build_top10_from_request(payload, request_id=rid)
    except HTTPException:
        raise
    except Exception as exc:
        logger.exception("Top10 POST failed: %s", exc)
        raise HTTPException(status_code=500, detail=f"Top10 route failed: {exc}") from exc


@router.get("/sheet-rows")
@router.get("/sheet_rows")
async def sheet_rows_get(
    request: Request,
    page: str = Query(default=PAGE_NAME),
    limit: int = Query(default=DEFAULT_TOP_N, ge=1, le=100),
    top_n: Optional[int] = Query(default=None, ge=1, le=100),
    source_pages: Optional[str] = Query(default=None),
    symbols: Optional[str] = Query(default=None),
    mode: Optional[str] = Query(default=None),
    per_source_limit: int = Query(default=DEFAULT_PER_SOURCE_LIMIT, ge=1, le=500),
    schema_only: bool = Query(default=False),
    headers_only: bool = Query(default=False),
    include_validation: bool = Query(default=False),
    min_confidence: Optional[float] = Query(default=None),
    max_risk_score: Optional[float] = Query(default=None),
    force_fallback: bool = Query(default=False),
    token: Optional[str] = Query(default=None),
    x_app_token: Optional[str] = Header(default=None, alias="X-APP-TOKEN"),
    x_api_key: Optional[str] = Header(default=None, alias="X-API-Key"),
    authorization: Optional[str] = Header(default=None, alias="Authorization"),
    x_request_id: Optional[str] = Header(default=None, alias="X-Request-ID"),
) -> Dict[str, Any]:
    _require_auth_or_401(
        request,
        token_q=token,
        x_app_token=x_app_token,
        x_api_key=x_api_key,
        authorization=authorization,
    )
    rid = _request_id(request, x_request_id)

    if page and page != PAGE_NAME:
        raise HTTPException(
            status_code=400,
            detail=f"This route serves only {PAGE_NAME}. Received page={page!r}.",
        )

    if schema_only:
        return {
            "ok": True,
            "page": PAGE_NAME,
            "sheet_name": PAGE_NAME,
            "keys": get_keys(PAGE_NAME),
            "headers": get_display_headers(PAGE_NAME),
            "display_headers": get_display_headers(PAGE_NAME),
            "required_keys": get_required_keys(PAGE_NAME),
            "version": VERSION,
            "route_owner": ROUTE_OWNER_NAME,
            "route_family": ROUTE_FAMILY_NAME,
            "request_id": rid,
            "timestamp_utc": _now_utc_iso(),
            "meta": {
                "route": "routes.top10_investments",
                "version": VERSION,
                "route_owner": ROUTE_OWNER_NAME,
                "route_family": ROUTE_FAMILY_NAME,
                "schema_only": True,
                "generated_at_utc": _now_utc_iso(),
            },
        }

    if headers_only:
        return {
            "ok": True,
            "page": PAGE_NAME,
            "sheet_name": PAGE_NAME,
            "headers": get_display_headers(PAGE_NAME),
            "display_headers": get_display_headers(PAGE_NAME),
            "keys": get_keys(PAGE_NAME),
            "required_keys": get_required_keys(PAGE_NAME),
            "version": VERSION,
            "route_owner": ROUTE_OWNER_NAME,
            "route_family": ROUTE_FAMILY_NAME,
            "request_id": rid,
            "timestamp_utc": _now_utc_iso(),
            "meta": {
                "route": "routes.top10_investments",
                "version": VERSION,
                "route_owner": ROUTE_OWNER_NAME,
                "route_family": ROUTE_FAMILY_NAME,
                "headers_only": True,
                "generated_at_utc": _now_utc_iso(),
            },
        }

    req = Top10Request(
        page=PAGE_NAME, limit=limit, top_n=top_n,
        source_pages=source_pages, symbols=symbols, mode=mode,
        per_source_limit=per_source_limit, include_validation=include_validation,
        min_confidence=min_confidence, max_risk_score=max_risk_score,
        force_fallback=force_fallback,
    )
    try:
        return await _build_top10_from_request(req, request_id=rid)
    except HTTPException:
        raise
    except Exception as exc:
        logger.exception("Top10 sheet-rows GET failed: %s", exc)
        raise HTTPException(status_code=500, detail=f"Top10 sheet-rows failed: {exc}") from exc


@router.get("/")
async def root(
    request: Request,
    x_request_id: Optional[str] = Header(default=None, alias="X-Request-ID"),
) -> Dict[str, Any]:
    rid = _request_id(request, x_request_id)
    return {
        "ok": True,
        "message": "Top 10 Investments route is live.",
        "version": VERSION,
        "route_owner": ROUTE_OWNER_NAME,
        "route_family": ROUTE_FAMILY_NAME,
        "page": PAGE_NAME,
        "endpoints": [
            "/v1/top10/health",
            "/v1/top10/schema",
            "/v1/top10/sheet-spec",
            "/v1/top10/sheet_spec",
            "/v1/top10/page",
            "/v1/top10/top10",
            "/v1/top10/run",
            "/v1/top10/recommendations",
            "/v1/top10/sheet-rows",
            "/v1/top10/sheet_rows",
        ],
        "request_id": rid,
        "timestamp_utc": _now_utc_iso(),
    }


# =============================================================================
# Dynamic loader hooks (matches routes.routes_argaam / routes.config pattern)
# =============================================================================
def mount(app: Any) -> None:
    try:
        if app is not None and hasattr(app, "include_router"):
            app.include_router(router)
    except Exception:
        pass


def get_router() -> APIRouter:
    return router


__all__ = [
    "router",
    "mount",
    "get_router",
    "VERSION",
    "ROUTE_OWNER_NAME",
    "ROUTE_FAMILY_NAME",
    "PAGE_NAME",
    "Top10Request",
]
