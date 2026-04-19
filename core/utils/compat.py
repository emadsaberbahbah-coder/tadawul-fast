#!/usr/bin/env python3
# core/utils/compat.py
"""
================================================================================
TFB Compatibility Utilities -- v3.0.0
================================================================================
RENDER-SAFE • IMPORT-SAFE • Pydantic V1/V2 COMPAT • orjson/json COMPAT
JSON-SAFE • MODEL-SAFE • NO NETWORK I/O

Purpose
-------
Centralize compatibility helpers used across core/, routes/, integrations/,
and scripts/ modules.

v3.0.0 Changes (from v2.0.0)
----------------------------
Bug fixes:
  - `is_nonempty(value)` no longer uses unguarded `value == 0` comparison,
    which crashed on numpy arrays (ambiguous truth value) and any type
    with a non-boolean __eq__. Now uses explicit isinstance checks.
  - `is_nonempty(Decimal('NaN'))` now returns False, matching the behavior
    for `float('nan')`. Previously returned True (fell through all
    isinstance checks to the default).
  - `json_safe` and `model_dump_compat` now guard `is_dataclass(obj)` with
    `not isinstance(obj, type)`, so passing a dataclass CLASS (not
    instance) no longer triggers the TypeError + silent fallback path
    (returned "<class ...>" for json_safe, empty dict for model_dump_compat).
    Dataclass classes now fall through to the plain-object path.
  - `json_safe` now handles `frozenset` correctly (returns list), matching
    the behavior for set/list/tuple. Previously fell through to str().
  - In the Pydantic v1 fallback branch, `field_validator` and
    `model_validator` were assigned None — but they are exported in
    __all__ and used as decorators in downstream modules. Calling them
    raised TypeError ('NoneType' object is not callable). They now
    provide no-op decorators consistent with the "no Pydantic" branch.
  - `json_compact` docstring example was wrong: claimed
    `json_compact({'a':1,'b':None}) == '{"a":1}'` but the function does
    not strip None values — it only produces compact (no-indent) JSON.
    Docstring corrected to reflect actual behavior.

Cleanup:
  - Removed dead `orjson = None` assignment in the except ImportError
    branch (never referenced).
  - Removed unused `JSONConfig` dataclass (defined but never used, not
    in __all__).
  - Removed unused typing imports: `Iterable`, `Sequence`, `Set`, `Tuple`,
    `cast`; removed unused dataclasses import `field`.
  - Removed stale type-annotation claim from header docstring (the
    reference to a typed-dict helper was never actually implemented).

Redundancy:
  - `is_nonempty`: `value is False or value == 0` collapsed to a single
    explicit `isinstance(value, bool)` check (False is meaningful) +
    isinstance(int/Decimal) branch (0 is meaningful).

Preserved:
  - Full `__all__` surface (31 names, identical order).
  - Every public function signature and behavior for valid inputs.
  - orjson fast-path fallback to stdlib json.
  - Pydantic v2-first, v1-fallback, no-Pydantic-fallback import chain.
  - Exception classes (CompatError / SerializationError / ModelError).
================================================================================
"""

from __future__ import annotations

import json
import math
import logging
from dataclasses import asdict, is_dataclass
from datetime import date, datetime, time as dt_time
from decimal import Decimal
from enum import Enum
from typing import (
    Any,
    Callable,
    Dict,
    Mapping,
    Optional,
    Type,
    TypeVar,
    Union,
)

# ---------------------------------------------------------------------------
# Logging Setup
# ---------------------------------------------------------------------------

logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())

# ---------------------------------------------------------------------------
# Version
# ---------------------------------------------------------------------------

__version__ = "3.0.0"
COMPAT_VERSION = __version__

# ---------------------------------------------------------------------------
# Custom Exceptions
# ---------------------------------------------------------------------------

class CompatError(Exception):
    """Base exception for compatibility utilities."""
    pass


class SerializationError(CompatError):
    """Raised when JSON serialization fails."""
    pass


class ModelError(CompatError):
    """Raised when model operations fail."""
    pass


# ---------------------------------------------------------------------------
# Fast JSON (orjson optional)
# ---------------------------------------------------------------------------

_HAS_ORJSON = False
_ORJSON_AVAILABLE = False

try:
    import orjson  # type: ignore

    _HAS_ORJSON = True
    _ORJSON_AVAILABLE = True

    def _orjson_dumps(value: Any, *, default: Callable[[Any], Any] = str, indent: int = 0) -> str:
        """Serialize to JSON string using orjson."""
        option = orjson.OPT_INDENT_2 if indent else 0
        return orjson.dumps(value, option=option, default=default).decode("utf-8")

    def _orjson_dumps_bytes(value: Any, *, default: Callable[[Any], Any] = str, indent: int = 0) -> bytes:
        """Serialize to JSON bytes using orjson."""
        option = orjson.OPT_INDENT_2 if indent else 0
        return orjson.dumps(value, option=option, default=default)

    def _orjson_loads(data: Union[str, bytes, bytearray]) -> Any:
        """Deserialize JSON using orjson."""
        if isinstance(data, str):
            data = data.encode("utf-8")
        return orjson.loads(data)

except ImportError:
    # v3.0.0: removed dead `orjson = None` assignment (never referenced).

    def _stdlib_dumps(value: Any, *, default: Callable[[Any], Any] = str, indent: int = 0) -> str:
        """Serialize to JSON string using standard library."""
        return json.dumps(
            value,
            default=default,
            ensure_ascii=False,
            indent=indent if indent else None,
        )

    def _stdlib_dumps_bytes(value: Any, *, default: Callable[[Any], Any] = str, indent: int = 0) -> bytes:
        """Serialize to JSON bytes using standard library."""
        return _stdlib_dumps(value, default=default, indent=indent).encode("utf-8")

    def _stdlib_loads(data: Union[str, bytes, bytearray]) -> Any:
        """Deserialize JSON using standard library."""
        if isinstance(data, (bytes, bytearray)):
            data = data.decode("utf-8", errors="replace")
        return json.loads(data)


def json_dumps(value: Any, *, default: Callable[[Any], Any] = str, indent: int = 0) -> str:
    """
    Serialize value to JSON string.

    Uses orjson if available, falls back to standard library.

    Examples:
        >>> json_dumps({"key": "value"})
        '{"key":"value"}'
    """
    if _ORJSON_AVAILABLE:
        return _orjson_dumps(value, default=default, indent=indent)
    return _stdlib_dumps(value, default=default, indent=indent)


def json_dumps_bytes(value: Any, *, default: Callable[[Any], Any] = str, indent: int = 0) -> bytes:
    """
    Serialize value to JSON bytes.

    Uses orjson if available, falls back to standard library.

    Examples:
        >>> json_dumps_bytes({"key": "value"})
        b'{"key":"value"}'
    """
    if _ORJSON_AVAILABLE:
        return _orjson_dumps_bytes(value, default=default, indent=indent)
    return _stdlib_dumps_bytes(value, default=default, indent=indent)


def json_loads(data: Union[str, bytes, bytearray]) -> Any:
    """
    Deserialize JSON string or bytes.

    Uses orjson if available, falls back to standard library.

    Examples:
        >>> json_loads('{"key":"value"}')
        {'key': 'value'}
    """
    if _ORJSON_AVAILABLE:
        return _orjson_loads(data)
    return _stdlib_loads(data)


# ---------------------------------------------------------------------------
# Pydantic Compatibility (v2 preferred, v1 fallback)
# ---------------------------------------------------------------------------

PYDANTIC_AVAILABLE = False
PYDANTIC_V2 = False


def _noop_decorator(*args: Any, **kwargs: Any) -> Callable[..., Any]:
    """No-op decorator factory for missing Pydantic shims.

    Returned decorator yields the wrapped callable unchanged, so it can
    stand in for `validator`, `field_validator`, etc. in environments
    where those symbols are absent.
    """
    def decorator(fn: Callable[..., Any]) -> Callable[..., Any]:
        return fn
    return decorator


try:
    from pydantic import BaseModel, Field, ValidationError

    PYDANTIC_AVAILABLE = True

    # Try v2 imports
    try:
        from pydantic import ConfigDict, field_validator, model_validator

        PYDANTIC_V2 = True

        # v2 still provides validator/root_validator in some environments
        try:
            from pydantic import validator, root_validator
        except ImportError:
            # Provide no-op decorators for backward compatibility
            validator = _noop_decorator
            root_validator = _noop_decorator

    except ImportError:
        # v1 fallback
        from pydantic import validator, root_validator

        ConfigDict = None
        # v3.0.0 fix: v1 lacks field_validator/model_validator. v2 pasted
        # assigned them to None, but they are exported in __all__ and used
        # as decorators (@field_validator('x')) in downstream code. None is
        # not callable -> TypeError at decoration time. Provide no-op
        # decorator stubs instead so v1 environments at least import cleanly.
        field_validator = _noop_decorator
        model_validator = _noop_decorator
        PYDANTIC_V2 = False

except ImportError:
    # No Pydantic available - provide minimal fallbacks
    PYDANTIC_AVAILABLE = False
    PYDANTIC_V2 = False

    class BaseModel:  # type: ignore
        """Minimal fallback BaseModel to avoid import crashes."""
        pass

    def Field(default: Any = None, **kwargs: Any) -> Any:
        """Minimal fallback Field."""
        return default

    class ValidationError(Exception):
        """Fallback ValidationError."""
        pass

    ConfigDict = None

    validator = _noop_decorator
    root_validator = _noop_decorator
    field_validator = _noop_decorator
    model_validator = _noop_decorator


TModel = TypeVar("TModel", bound=Any)


# ---------------------------------------------------------------------------
# Type Guards and Value Helpers
# ---------------------------------------------------------------------------

def is_mapping(value: Any) -> bool:
    """
    Check if value is a Mapping (dict-like).

    Examples:
        >>> is_mapping({"key": "value"})
        True
        >>> is_mapping([1, 2, 3])
        False
    """
    return isinstance(value, Mapping)


def is_nonempty(value: Any) -> bool:
    """
    Check if value is non-empty / meaningful.

    Returns True for:
        - False (boolean False is meaningful)
        - 0, 0.0, Decimal('0') (zero is meaningful)
        - Non-NaN / non-Inf floats and Decimals
        - Non-empty strings (after strip), lists, tuples, sets, frozensets,
          dicts, and other Mappings

    Returns False for:
        - None
        - NaN / Inf floats
        - NaN Decimals
        - Empty strings (whitespace-only treated as empty)
        - Empty collections

    v3.0.0: uses explicit isinstance guards instead of `value == 0`, which
    previously crashed on numpy arrays and other types with non-boolean
    __eq__ results.

    Examples:
        >>> is_nonempty(0)
        True
        >>> is_nonempty(False)
        True
        >>> is_nonempty(float('nan'))
        False
        >>> is_nonempty("")
        False
        >>> is_nonempty("   ")
        False
    """
    if value is None:
        return False
    # bool is checked BEFORE int (since bool subclasses int); False is meaningful.
    if isinstance(value, bool):
        return True
    if isinstance(value, int):
        return True
    if isinstance(value, float):
        return not (math.isnan(value) or math.isinf(value))
    if isinstance(value, Decimal):
        try:
            if value.is_nan() or value.is_infinite():
                return False
        except Exception:
            # Very old/weird Decimal subclass — fall back to safe default
            return True
        return True
    if isinstance(value, str):
        return bool(value.strip())
    if isinstance(value, (list, tuple, set, frozenset, dict)):
        return len(value) > 0
    if isinstance(value, Mapping):
        try:
            return len(value) > 0
        except Exception:
            return True
    # Everything else (custom objects, etc.) treated as non-empty by default.
    return True


def compact_dict(d: Mapping[str, Any]) -> Dict[str, Any]:
    """
    Remove empty values from dictionary.

    Uses is_nonempty() to determine which values to keep. Keys are coerced
    to strings for JSON-friendly output.

    Examples:
        >>> compact_dict({"a": 1, "b": None, "c": "", "d": 0})
        {'a': 1, 'd': 0}
    """
    result: Dict[str, Any] = {}
    for key, value in d.items():
        if is_nonempty(value):
            result[str(key)] = value
    return result


# ---------------------------------------------------------------------------
# Pydantic / Model Compatibility Helpers
# ---------------------------------------------------------------------------

def _is_dataclass_instance(obj: Any) -> bool:
    """Return True only for dataclass INSTANCES, not the class itself.

    v3.0.0: guards against the common gotcha where `is_dataclass(SomeDC)`
    returns True for the class object as well; `asdict(cls)` then raises.
    """
    return is_dataclass(obj) and not isinstance(obj, type)


def model_dump_compat(obj: Any, *, exclude_none: bool = False, mode: str = "python") -> Dict[str, Any]:
    """
    Convert object to dictionary.

    Handles:
        - Pydantic models (v1 and v2)
        - Dataclass instances
        - Mappings (dict-like)
        - Plain objects with __dict__

    Args:
        obj: Object to convert
        exclude_none: Whether to exclude None values
        mode: Mode for Pydantic v2 (python or json)

    Returns:
        Dictionary representation; empty dict if conversion fails

    Examples:
        >>> from dataclasses import dataclass
        >>> @dataclass
        ... class Point:
        ...     x: int
        ...     y: int
        >>> model_dump_compat(Point(1, 2))
        {'x': 1, 'y': 2}
    """
    if obj is None:
        return {}

    # Direct mapping
    if isinstance(obj, dict):
        result = dict(obj)
        return compact_dict(result) if exclude_none else result

    if isinstance(obj, Mapping):
        try:
            result = dict(obj)
            return compact_dict(result) if exclude_none else result
        except Exception as e:
            logger.debug("Failed to convert mapping to dict: %s", e)
            return {}

    # Dataclass instance (v3.0.0: skip class objects)
    if _is_dataclass_instance(obj):
        try:
            result = asdict(obj)
            return compact_dict(result) if exclude_none else result
        except Exception as e:
            logger.debug("Failed to convert dataclass to dict: %s", e)
            return {}

    # Pydantic v2
    try:
        model_dump = getattr(obj, "model_dump", None)
        if callable(model_dump):
            result = model_dump(mode=mode, exclude_none=exclude_none)
            if isinstance(result, dict):
                return result
    except Exception as e:
        logger.debug("Failed to call model_dump: %s", e)

    # Pydantic v1
    try:
        dict_method = getattr(obj, "dict", None)
        if callable(dict_method):
            result = dict_method(exclude_none=exclude_none)
            if isinstance(result, dict):
                return result
    except Exception as e:
        logger.debug("Failed to call dict(): %s", e)

    # Plain object
    try:
        raw = getattr(obj, "__dict__", None)
        if isinstance(raw, dict):
            result = dict(raw)
            return compact_dict(result) if exclude_none else result
    except Exception as e:
        logger.debug("Failed to get __dict__: %s", e)

    return {}


def model_to_dict(obj: Any, *, exclude_none: bool = False, mode: str = "python") -> Dict[str, Any]:
    """Alias for model_dump_compat."""
    return model_dump_compat(obj, exclude_none=exclude_none, mode=mode)


def object_to_dict(obj: Any, *, exclude_none: bool = False, mode: str = "python") -> Dict[str, Any]:
    """Alias for model_dump_compat."""
    return model_dump_compat(obj, exclude_none=exclude_none, mode=mode)


def model_validate_compat(model_cls: Type[TModel], data: Any) -> TModel:
    """
    Validate/construct a Pydantic model across v1/v2.

    Args:
        model_cls: Pydantic model class
        data: Data to validate

    Returns:
        Validated model instance

    Raises:
        ModelError: If validation fails

    Examples:
        >>> from pydantic import BaseModel
        >>> class User(BaseModel):
        ...     name: str
        ...     age: int
        >>> user = model_validate_compat(User, {"name": "Alice", "age": 30})
        >>> user.name
        'Alice'
    """
    if not PYDANTIC_AVAILABLE:
        raise ModelError("Pydantic not available")

    # Pydantic v2
    model_validate = getattr(model_cls, "model_validate", None)
    if callable(model_validate):
        try:
            return model_validate(data)
        except Exception as e:
            raise ModelError(f"Model validation failed: {e}") from e

    # Pydantic v1
    parse_obj = getattr(model_cls, "parse_obj", None)
    if callable(parse_obj):
        try:
            return parse_obj(data)
        except Exception as e:
            raise ModelError(f"Model parse failed: {e}") from e

    # Fallback
    if isinstance(data, Mapping):
        try:
            return model_cls(**data)
        except Exception as e:
            raise ModelError(f"Model construction failed: {e}") from e

    raise ModelError(f"Cannot validate data of type {type(data).__name__}")


def model_construct_compat(model_cls: Type[TModel], **values: Any) -> TModel:
    """
    Construct a Pydantic model without full validation (when supported).

    Args:
        model_cls: Pydantic model class
        **values: Field values

    Returns:
        Model instance

    Examples:
        >>> from pydantic import BaseModel
        >>> class User(BaseModel):
        ...     name: str
        ...     age: int
        >>> user = model_construct_compat(User, name="Bob", age=25)
    """
    if not PYDANTIC_AVAILABLE:
        raise ModelError("Pydantic not available")

    # Pydantic v2
    model_construct = getattr(model_cls, "model_construct", None)
    if callable(model_construct):
        try:
            return model_construct(**values)
        except Exception as e:
            logger.debug("model_construct failed: %s", e)

    # Pydantic v1
    construct = getattr(model_cls, "construct", None)
    if callable(construct):
        try:
            return construct(**values)
        except Exception as e:
            logger.debug("construct failed: %s", e)

    # Fallback to normal construction
    return model_cls(**values)


# ---------------------------------------------------------------------------
# JSON-Safe Conversion Helpers
# ---------------------------------------------------------------------------

def json_safe(value: Any) -> Any:
    """
    Convert arbitrary values to JSON-safe values.

    Handles:
        - None, bool, int, str (return as-is)
        - float (NaN/Inf -> None)
        - Decimal -> float or str (NaN -> None)
        - Enum -> value
        - datetime/date/time -> ISO format string
        - bytes -> UTF-8 decoded string
        - Dataclass instances -> dict
        - Mappings -> dict
        - Sequences (list/tuple/set/frozenset) -> list
        - Pydantic models -> dict via model_dump
        - Other objects -> str

    v3.0.0: dataclass path now only matches INSTANCES (not classes);
    frozenset handled correctly.

    Examples:
        >>> json_safe(float('nan'))
        None
        >>> json_safe(Decimal('10.5'))
        10.5
        >>> from datetime import datetime
        >>> json_safe(datetime(2024, 1, 1))
        '2024-01-01T00:00:00'
    """
    if value is None:
        return None

    # Primitive types
    if isinstance(value, (bool, int, str)):
        return value

    # Float handling
    if isinstance(value, float):
        if math.isnan(value) or math.isinf(value):
            return None
        return value

    # Decimal
    if isinstance(value, Decimal):
        try:
            if value.is_nan() or value.is_infinite():
                return None
        except Exception:
            pass
        try:
            f = float(value)
            if math.isnan(f) or math.isinf(f):
                return None
            return f
        except Exception:
            return str(value)

    # Enum
    if isinstance(value, Enum):
        try:
            return value.value
        except Exception:
            return str(value)

    # DateTime
    if isinstance(value, (datetime, date, dt_time)):
        try:
            return value.isoformat()
        except Exception:
            return str(value)

    # Bytes
    if isinstance(value, bytes):
        try:
            return value.decode("utf-8", errors="replace")
        except Exception:
            return str(value)

    # Dataclass INSTANCE only (v3.0.0 fix)
    if _is_dataclass_instance(value):
        try:
            return {str(k): json_safe(v) for k, v in asdict(value).items()}
        except Exception:
            return str(value)

    # Mapping
    if isinstance(value, Mapping):
        return {str(k): json_safe(v) for k, v in value.items()}

    # Sequence (but not string). v3.0.0: include frozenset.
    if isinstance(value, (list, tuple, set, frozenset)):
        return [json_safe(v) for v in value]

    # Pydantic v2
    try:
        model_dump = getattr(value, "model_dump", None)
        if callable(model_dump):
            dumped = model_dump(mode="python")
            if isinstance(dumped, dict):
                return json_safe(dumped)
    except Exception:
        pass

    # Pydantic v1
    try:
        dict_method = getattr(value, "dict", None)
        if callable(dict_method):
            dumped = dict_method()
            if isinstance(dumped, dict):
                return json_safe(dumped)
    except Exception:
        pass

    # Plain object
    try:
        raw = getattr(value, "__dict__", None)
        if isinstance(raw, dict):
            return json_safe(raw)
    except Exception:
        pass

    # Fallback
    try:
        return str(value)
    except Exception:
        return None


def json_compact(value: Any) -> str:
    """
    Serialize value to compact (no-indent) JSON string.

    Runs the value through ``json_safe`` first to handle NaN/datetime/
    Decimal/etc., then emits a single-line JSON string via ``json_dumps``.
    This does NOT strip None/empty values — use ``compact_dict`` first if
    you want that behavior.

    Examples:
        >>> # Note: orjson returns '{"a":1,"b":null}' (no spaces);
        >>> # stdlib returns '{"a": 1, "b": null}' (spaces after separators).
        >>> # Both are valid compact-ish JSON; exact whitespace depends on backend.
        >>> 'null' in json_compact({"a": 1, "b": None})
        True
    """
    try:
        safe_value = json_safe(value)
        return json_dumps(safe_value, indent=0)
    except Exception as e:
        logger.warning("json_compact failed: %s", e)
        try:
            return str(value)
        except Exception:
            return ""


# ---------------------------------------------------------------------------
# Pydantic Configuration Helpers
# ---------------------------------------------------------------------------

def pydantic_extra_ignore_config() -> Optional[Any]:
    """
    Get ConfigDict for models that want extra='ignore'.

    Returns:
        ConfigDict for v2, None for v1

    Examples:
        >>> class MyModel(BaseModel):
        ...     if pydantic_extra_ignore_config():
        ...         model_config = pydantic_extra_ignore_config()
    """
    if PYDANTIC_V2 and ConfigDict is not None:
        return ConfigDict(extra="ignore")
    return None


def pydantic_assignment_config() -> Optional[Any]:
    """
    Get ConfigDict for models that want validate_assignment=True.

    Returns:
        ConfigDict for v2, None for v1

    Examples:
        >>> class MyModel(BaseModel):
        ...     if pydantic_assignment_config():
        ...         model_config = pydantic_assignment_config()
    """
    if PYDANTIC_V2 and ConfigDict is not None:
        return ConfigDict(extra="ignore", validate_assignment=True)
    return None


# ---------------------------------------------------------------------------
# Module Exports
# ---------------------------------------------------------------------------

__all__ = [
    "__version__",
    "COMPAT_VERSION",
    "_HAS_ORJSON",
    "PYDANTIC_AVAILABLE",
    "PYDANTIC_V2",
    # Types
    "BaseModel",
    "Field",
    "ValidationError",
    "ConfigDict",
    # Decorators
    "validator",
    "root_validator",
    "field_validator",
    "model_validator",
    # JSON
    "json_dumps",
    "json_dumps_bytes",
    "json_loads",
    # Type guards
    "is_mapping",
    "is_nonempty",
    "compact_dict",
    # Model helpers
    "model_dump_compat",
    "model_to_dict",
    "object_to_dict",
    "model_validate_compat",
    "model_construct_compat",
    # JSON safe
    "json_safe",
    "json_compact",
    # Config helpers
    "pydantic_extra_ignore_config",
    "pydantic_assignment_config",
    # Exceptions
    "CompatError",
    "SerializationError",
    "ModelError",
]
