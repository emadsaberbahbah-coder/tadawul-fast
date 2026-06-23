#!/usr/bin/env python3
# core/utils/__init__.py
"""
core.utils — Small shared utilities (v1.0.1)

    - compat  : Pydantic v1/v2 compatibility shim + JSON serialization
                helpers. Re-exports BaseModel / Field / validators /
                ConfigDict with version-appropriate shims (PYDANTIC_V2
                detection); provides json_dumps / json_loads / json_safe /
                json_compact (orjson fast path with a stdlib-json fallback)
                and model_dump_compat / model_validate_compat /
                model_to_dict / object_to_dict that work across Pydantic
                v1 and v2. json_safe is nan/inf-safe (non-finite floats ->
                null) and preserves 0 as a real value (only None / "" / []
                are treated as blank). Import-safe: if pydantic / orjson
                are absent it degrades to stub models + stdlib json without
                raising at import.

This __init__ is intentionally empty of runtime imports. Consumers
import directly:

    from core.utils.compat import json_dumps, json_safe, BaseModel
    from core.utils.compat import model_dump_compat, model_validate_compat

--------------------------------------------------------------------------
CHANGELOG
--------------------------------------------------------------------------
v1.0.1
    - DOC: corrected the compat descriptor. The prior text read
      "cross-module compatibility helpers (type coercion, safe attribute
      access, import-safe fallbacks)", which did not match the module.
      compat.py is a Pydantic v1/v2 + JSON-serialization compatibility
      layer (version-shimmed model re-exports, orjson/stdlib JSON, and
      cross-version model dump/validate), not a type-coercion or
      attribute-access helper. Docstring-only change — no runtime import,
      behavior, or public API impact (__version__ bumped; __all__
      unchanged).
v1.0.0
    - Initial package initializer (empty-by-design; lazy consumer imports
      so a heavy/optional dependency never blocks boot).
"""

from __future__ import annotations

__version__ = "1.0.1"
__all__: list[str] = []
