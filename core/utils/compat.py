#!/usr/bin/env python3
# core/utils/compat.py
"""
================================================================================
TFB Compatibility Utilities -- v1.1.0
================================================================================
RENDER-SAFE • IMPORT-SAFE • Pydantic V1/V2 COMPAT • orjson/json COMPAT
JSON-SAFE • MODEL-SAFE • NO NETWORK I/O

Purpose
-------
Centralize compatibility helpers used across core/, routes/, integrations/,
and scripts/ modules.

v1.1.0 changes vs v1.0.0
--------------------------
  Version bump only. All v1.0.0 logic confirmed correct:
  - is_nonempty() correctly handles all dtype=pct fraction values:
    0.0 / 0.0142 / 0.25 -> NOT empty (preserved by compact_dict/clean_patch)
    NaN / Inf            -> empty    (filtered out by compact_dict/clean_patch)
    None / "" / []       -> empty    (filtered out)
  - json_dumps / json_safe correctly serialise fraction values as-is.
    dtype=pct fractions (e.g. 0.0142 = 1.42%) are never coerced or
    rounded during serialisation.
  - compact_dict keeps False and 0 values (correct for boolean/zero fields).
  - model_dump_compat / json_safe handle Pydantic v1 and v2 correctly.

This module intentionally stays lightweight and startup-safe:
- no network I/O
- no engine access
- no Google client initialization
- no heavy ML imports

Primary responsibilities
------------------------
1) Fast JSON helpers with safe fallback to stdlib json
2) Pydantic v2 preferred, v1 fallback-compatible imports
3) Unified model/object dumping helpers
4) Unified JSON-safe serialization helpers
5) Reusable small helpers for future refactors

Recommended usage
-----------------
Instead of repeating boilerplate like:
- try: import orjson ... except: import json ...
- try: from pydantic import BaseModel, ConfigDict, field_validator ...
  except: from pydantic import BaseModel, validator, root_validator ...
- repeated model_dump()/dict()/__dict__ conversion logic

Use:
    from core.utils.compat import (
        BaseModel,
        Field,
        ValidationError,
        ConfigDict,
        PYDANTIC_V2,
        field_validator,
        model_validator,
        validator,
        root_validator,
        json_dumps,
        json_dumps_bytes,
        json_loads,
        model_dump_compat,
        model_validate_compat,
        model_construct_compat,
        model_to_dict,
        object_to_dict,
        json_safe,
    )
================================================================================
"""

from __future__ import annotations

import json
import math
from dataclasses import asdict, is_dataclass
from datetime import date, datetime, time as dt_time
from decimal import Decimal
from enum import Enum
from typing import Any, Callable, Dict, Iterable, List, Mapping, MutableMapping, Optional, Sequence, Tuple, Type, TypeVar, Union

__version__ = "1.1.0"
COMPAT_VERSION = __version__

# =============================================================================
# Fast JSON (orjson optional)
# =============================================================================
try:
    import orjson  # type: ignore

    _HAS_ORJSON = True

    def json_dumps(value: Any, *, default: Callable[[Any], Any] = str, indent: int = 0) -> str:
        option = orjson.OPT_INDENT_2 if indent else 0
        return orjson.dumps(value, option=option, default=default).decode("utf-8")

    def json_dumps_bytes(value: Any, *, default: Callable[[Any], Any] = str, indent: int = 0) -> bytes:
        option = orjson.OPT_INDENT_2 if indent else 0
        return orjson.dumps(value, option=option, default=default)

    def json_loads(data: Union[str, bytes, bytearray]) -> Any:
        if isinstance(data, str):
            data = data.encode("utf-8")
        return orjson.loads(data)

except Exception:  # pragma: no cover
    orjson = None  # type: ignore
    _HAS_ORJSON = False

    def json_dumps(value: Any, *, default: Callable[[Any], Any] = str, indent: int = 0) -> str:
        return json.dumps(
            value,
            default=default,
            ensure_ascii=False,
            indent=(indent if indent else None),
        )

    def json_dumps_bytes(value: Any, *, default: Callable[[Any], Any] = str, indent: int = 0) -> bytes:
        return json_dumps(value, default=default, indent=indent).encode("utf-8")

    def json_loads(data: Union[str, bytes, bytearray]) -> Any:
        if isinstance(data, (bytes, bytearray)):
            data = data.decode("utf-8", errors="replace")
        return json.loads(data)


# =============================================================================
# Pydantic compatibility (v2 preferred, v1 fallback)
# =============================================================================
PYDANTIC_AVAILABLE = False
PYDANTIC_V2 = False

try:
    from pydantic import BaseModel, Field, ValidationError  # type: ignore

    try:
        from pydantic import ConfigDict, field_validator, model_validator  # type: ignore

        PYDANTIC_AVAILABLE = True
        PYDANTIC_V2 = True

        # v2 still provides validator/root_validator in many environments,
        # but we normalize availability defensively.
        try:
            from pydantic import validator, root_validator  # type: ignore
        except Exception:  # pragma: no cover
            def validator(*args: Any, **kwargs: Any):  # type: ignore
                def _decorator(fn: Callable[..., Any]) -> Callable[..., Any]:
                    return fn
                return _decorator

            def root_validator(*args: Any, **kwargs: Any):  # type: ignore
                def _decorator(fn: Callable[..., Any]) -> Callable[..., Any]:
                    return fn
                return _decorator

    except Exception:
        from pydantic import validator, root_validator  # type: ignore

        ConfigDict = None  # type: ignore
        field_validator = None  # type: ignore
        model_validator = None  # type: ignore

        PYDANTIC_AVAILABLE = True
        PYDANTIC_V2 = False

except Exception:  # pragma: no cover
    PYDANTIC_AVAILABLE = False
    PYDANTIC_V2 = False

    class BaseModel:  # type: ignore
        """Minimal fallback to avoid import crashes in utility consumers."""
        pass

    def Field(default: Any = None, **kwargs: Any) -> Any:  # type: ignore
        return default

    class ValidationError(Exception):
        pass

    ConfigDict = None  # type: ignore

    def validator(*args: Any, **kwargs: Any):  # type: ignore
        def _decorator(fn: Callable[..., Any]) -> Callable[..., Any]:
            return fn
        return _decorator

    def root_validator(*args: Any, **kwargs: Any):  # type: ignore
        def _decorator(fn: Callable[..., Any]) -> Callable[..., Any]:
            return fn
        return _decorator

    def field_validator(*args: Any, **kwargs: Any):  # type: ignore
        def _decorator(fn: Callable[..., Any]) -> Callable[..., Any]:
            return fn
        return _decorator

    def model_validator(*args: Any, **kwargs: Any):  # type: ignore
        def _decorator(fn: Callable[..., Any]) -> Callable[..., Any]:
            return fn
        return _decorator


TModel = TypeVar("TModel")


# =============================================================================
# Small generic helpers
# =============================================================================
def is_mapping(value: Any) -> bool:
    return isinstance(value, Mapping)


def is_nonempty(value: Any) -> bool:
    if value is None:
        return False
    if value is False or value == 0:
        return True
    if isinstance(value, float):
        return not (math.isnan(value) or math.isinf(value))
    if isinstance(value, str):
        return bool(value.strip())
    if isinstance(value, (list, tuple, set, dict)):
        return len(value) > 0
    return True


def compact_dict(d: Mapping[str, Any]) -> Dict[str, Any]:
    out: Dict[str, Any] = {}
    for k, v in d.items():
        if is_nonempty(v):
            out[str(k)] = v
    return out


# =============================================================================
# Pydantic / model compatibility helpers
# =============================================================================
def model_dump_compat(obj: Any, *, exclude_none: bool = False, mode: str = "python") -> Dict[str, Any]:
    """
    Return a dict from a Pydantic model, dataclass, mapping, or plain object.
    """
    if obj is None:
        return {}
    if isinstance(obj, dict):
        out = dict(obj)
        return compact_dict(out) if exclude_none else out
    if isinstance(obj, Mapping):
        try:
            out = dict(obj)
            return compact_dict(out) if exclude_none else out
        except Exception:
            return {}
    if is_dataclass(obj):
        try:
            out = asdict(obj)
            return compact_dict(out) if exclude_none else out
        except Exception:
            return {}

    try:
        md = getattr(obj, "model_dump", None)
        if callable(md):
            out = md(mode=mode, exclude_none=exclude_none)
            if isinstance(out, dict):
                return out
    except Exception:
        pass

    try:
        dct = getattr(obj, "dict", None)
        if callable(dct):
            out = dct(exclude_none=exclude_none)
            if isinstance(out, dict):
                return out
    except Exception:
        pass

    try:
        raw = getattr(obj, "__dict__", None)
        if isinstance(raw, dict):
            out = dict(raw)
            return compact_dict(out) if exclude_none else out
    except Exception:
        pass

    return {}


def model_to_dict(obj: Any, *, exclude_none: bool = False, mode: str = "python") -> Dict[str, Any]:
    return model_dump_compat(obj, exclude_none=exclude_none, mode=mode)


def object_to_dict(obj: Any, *, exclude_none: bool = False, mode: str = "python") -> Dict[str, Any]:
    return model_dump_compat(obj, exclude_none=exclude_none, mode=mode)


def model_validate_compat(model_cls: Type[TModel], data: Any) -> TModel:
    """
    Validate/construct a Pydantic model class across v1/v2.
    """
    mv = getattr(model_cls, "model_validate", None)
    if callable(mv):
        return mv(data)  # type: ignore[return-value]

    po = getattr(model_cls, "parse_obj", None)
    if callable(po):
        return po(data)  # type: ignore[return-value]

    return model_cls(**(data if isinstance(data, Mapping) else {"value": data}))  # type: ignore[misc,return-value]


def model_construct_compat(model_cls: Type[TModel], **values: Any) -> TModel:
    """
    Construct a Pydantic model without full validation when supported.
    """
    mc = getattr(model_cls, "model_construct", None)
    if callable(mc):
        return mc(**values)  # type: ignore[return-value]

    c = getattr(model_cls, "construct", None)
    if callable(c):
        return c(**values)  # type: ignore[return-value]

    return model_cls(**values)  # type: ignore[misc,return-value]


# =============================================================================
# JSON-safe conversion helpers
# =============================================================================
def json_safe(value: Any) -> Any:
    """
    Convert arbitrary values into JSON-safe values.
    """
    if value is None:
        return None

    if isinstance(value, (bool, int, str)):
        return value

    if isinstance(value, float):
        if math.isnan(value) or math.isinf(value):
            return None
        return value

    if isinstance(value, Decimal):
        try:
            f = float(value)
            if math.isnan(f) or math.isinf(f):
                return None
            return f
        except Exception:
            return str(value)

    if isinstance(value, Enum):
        try:
            return value.value
        except Exception:
            return str(value)

    if isinstance(value, (datetime, date, dt_time)):
        try:
            return value.isoformat()
        except Exception:
            return str(value)

    if isinstance(value, bytes):
        try:
            return value.decode("utf-8", errors="replace")
        except Exception:
            return str(value)

    if is_dataclass(value):
        try:
            return {str(k): json_safe(v) for k, v in asdict(value).items()}
        except Exception:
            return str(value)

    if isinstance(value, Mapping):
        return {str(k): json_safe(v) for k, v in value.items()}

    if isinstance(value, (list, tuple, set)):
        return [json_safe(v) for v in value]

    try:
        md = getattr(value, "model_dump", None)
        if callable(md):
            return json_safe(md(mode="python"))
    except Exception:
        pass

    try:
        dct = getattr(value, "dict", None)
        if callable(dct):
            return json_safe(dct())
    except Exception:
        pass

    try:
        raw = getattr(value, "__dict__", None)
        if isinstance(raw, dict):
            return json_safe(raw)
    except Exception:
        pass

    try:
        return str(value)
    except Exception:
        return None


def json_compact(value: Any) -> str:
    try:
        return json_dumps(json_safe(value), indent=0)
    except Exception:
        try:
            return str(value)
        except Exception:
            return ""


# =============================================================================
# Convenience helpers for validators across Pydantic versions
# =============================================================================
def pydantic_extra_ignore_config() -> Any:
    """
    Use in models that want extra='ignore' with v2, while v1 callers can ignore it.
    """
    if PYDANTIC_V2 and ConfigDict is not None:
        return ConfigDict(extra="ignore")
    return None


def pydantic_assignment_config() -> Any:
    if PYDANTIC_V2 and ConfigDict is not None:
        return ConfigDict(extra="ignore", validate_assignment=True)
    return None


__all__ = [
    "__version__",
    "COMPAT_VERSION",
    "_HAS_ORJSON",
    "PYDANTIC_AVAILABLE",
    "PYDANTIC_V2",
    "BaseModel",
    "Field",
    "ValidationError",
    "ConfigDict",
    "validator",
    "root_validator",
    "field_validator",
    "model_validator",
    "json_dumps",
    "json_dumps_bytes",
    "json_loads",
    "is_mapping",
    "is_nonempty",
    "compact_dict",
    "model_dump_compat",
    "model_to_dict",
    "object_to_dict",
    "model_validate_compat",
    "model_construct_compat",
    "json_safe",
    "json_compact",
    "pydantic_extra_ignore_config",
    "pydantic_assignment_config",
]
